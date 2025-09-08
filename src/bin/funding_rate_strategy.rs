use anyhow::Result;
use lazy_static::lazy_static;
use log::info;
use mkt_signal::common::iceoryx_subscriber::{MultiChannelSubscriber, SubscribeParams, ChannelType};
use mkt_signal::common::iceoryx_publisher::SignalPublisher;
use mkt_signal::exchange::Exchange;
use mkt_signal::mkt_msg::{self, MktMsgType, AskBidSpreadMsg, FundingRateMsg};
use std::collections::{HashMap, HashSet};
use std::fs;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::select;
use tokio::signal;
use tokio::time::{interval, Instant};
use bincode;
/// 套利策略配置 - 每个交易所包含多个[现货符号, 期货符号, 价差阈值]数组
#[derive(Debug, Deserialize)]
struct ArbitrageConfig {
    binance: Vec<(String, String, f64)>,
    okex: Vec<(String, String, f64)>,
    bybit: Vec<(String, String, f64)>,
}

struct ArbitrageThresholds {
    // exchange -> (spot_symbol, futures_symbol) -> threshold
    thresholds: HashMap<Exchange, HashMap<(String, String), f64>>,
}

impl ArbitrageThresholds {
    fn load() -> Result<Self> {
        let config_path = "config/tracking_symbol.json";
        let content = fs::read_to_string(config_path)?;
        
        // 解析JSON到ArbitrageConfig结构
        let config: ArbitrageConfig = serde_json::from_str(&content)?;
        
        let mut thresholds = HashMap::new();
        
        // 处理币安配置 - 现货和期货符号相同
        let mut binance_map = HashMap::new();
        for (spot, futures, threshold) in config.binance {
            binance_map.insert((spot, futures), threshold);
        }
        thresholds.insert(Exchange::Binance, binance_map);
        
        // 处理OKEx配置
        let mut okex_map = HashMap::new();
        for (spot, futures, threshold) in config.okex {
            okex_map.insert((spot, futures), threshold);
        }
        thresholds.insert(Exchange::OkexSwap, okex_map);
        
        // 处理Bybit配置
        let mut bybit_map = HashMap::new();
        for (spot, futures, threshold) in config.bybit {
            bybit_map.insert((spot, futures), threshold);
        }
        thresholds.insert(Exchange::Bybit, bybit_map);
        
        Ok(Self { thresholds })
    }
    
    /// 获取指定交易所的所有阈值
    fn get_binance_thresholds(&self) -> HashMap<String, f64> {
        // 对于币安，现货和期货符号相同，只需要一个符号作为键
        let mut result = HashMap::new();
        if let Some(map) = self.thresholds.get(&Exchange::Binance) {
            for ((spot, _), threshold) in map {
                result.insert(spot.clone(), *threshold);
            }
        }
        result
    }
}

// 使用lazy_static进行全局初始化
lazy_static! {
    static ref ARBITRAGE_THRESHOLDS: ArbitrageThresholds = {
        ArbitrageThresholds::load()
            .expect("Failed to load arbitrage config from config/tracking_symbol.json")
    };
}

/// 交易事件结构体
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TradeEvent {
    /// 交易对符号
    symbol: String,
    /// 交易所
    exchange: String,
    /// 事件类型: "open_long", "open_short", "close_long", "close_short"
    event_type: String,
    /// 触发信号类型: "spread" 或 "funding"
    signal_type: String,
    /// 价差偏离值（仅spread信号有效）
    spread_deviation: f64,
    /// 价差阈值（仅spread信号有效）
    spread_threshold: f64,
    /// 当前资金费率
    funding_rate: f64,
    /// 预测资金费率
    predicted_rate: f64,
    /// 借贷利率
    loan_rate: f64,
    /// 买价
    bid_price: f64,
    /// 买量
    bid_amount: f64,
    /// 卖价
    ask_price: f64,
    /// 卖量
    ask_amount: f64,
    /// 时间戳（毫秒）
    timestamp: i64,
}

/// 资金费率信息
#[derive(Debug, Clone)]
struct FundingRateData {
    funding_rate: f64,        // 当前资金费率
    predicted_rate: f64,      // 预测资金费率
    loan_rate: f64,         // 借贷利率
    next_funding_time: i64,   // 下次结算时间
    timestamp: i64,           // 更新时间戳
    // Ready标记
    funding_rate_ready: bool,
    predicted_rate_ready: bool,
    loan_rate_ready: bool,
}

impl FundingRateData {
    fn new() -> Self {
        Self {
            funding_rate: 0.0,
            predicted_rate: 0.0,
            loan_rate: 0.0,
            next_funding_time: 0,
            timestamp: 0,
            funding_rate_ready: false,
            predicted_rate_ready: false,
            loan_rate_ready: false,
        }
    }
    
    /// 一次性更新所有资金费率相关字段
    fn update(&mut self, 
        funding_rate: f64, 
        predicted_rate: f64, 
        loan_rate: f64,
        next_funding_time: i64,
        timestamp: i64
    ) -> bool {
        // 检查时间戳是否小于下次结算时间
        if timestamp >= next_funding_time {
            // 时间戳已经超过下次结算时间，数据可能过期
            return false;
        }
        
        // 如果已有数据，检查是否是更新的数据
        if self.timestamp > 0 && timestamp <= self.timestamp {
            // 新数据的时间戳不比现有数据新，跳过更新
            return false;
        }
        
        self.funding_rate = funding_rate;
        self.predicted_rate = predicted_rate;
        self.loan_rate = loan_rate;
        self.next_funding_time = next_funding_time;
        self.timestamp = timestamp;
        
        // 检查每个值是否非零，设置ready标记
        self.funding_rate_ready = funding_rate != 0.0;
        self.predicted_rate_ready = predicted_rate != 0.0;
        self.loan_rate_ready = loan_rate != 0.0;
        
        true
    }
    
    fn is_ready(&self) -> bool {
        self.funding_rate_ready && self.predicted_rate_ready && self.loan_rate_ready
    }
}

#[derive(Debug, Clone)]
struct SymbolState {
    // 最优报价字段
    bid_price: f64,
    bid_amount: f64,
    ask_price: f64,
    ask_amount: f64,
    last_update_time: i64,
    // 阈值
    spread_deviation_threshold: f64,
    // 资金费率
    funding_state: FundingRateData,
    // 信号状态
    spread_signal: i32,     // 价差信号: 1=做多, -1=做空
    funding_signal: i32,    // 资金费率信号: 1=做多, -1=做空, 2=平空, -2=平多
}

/// 策略主结构
struct FundingRateStrategy {
    // 币安的symbol状态，包含最优报价、阈值、资金费率等
    binance_symbol_states: HashMap<String, SymbolState>,
    // IceOryx 发布器
    publisher: SignalPublisher,
    // 统计
    messages_processed: u64,
    spread_messages: u64,
    funding_messages: u64,
    spread_updates: u64,
    trade_events_sent: u64,
    last_stats_time: Instant,
    // 每个周期收到过的symbol（按类型分别收集，且区分是否在跟踪列表中）
    received_spread_symbols: HashSet<String>,
    received_funding_symbols: HashSet<String>,
    untracked_spread_symbols: HashSet<String>,
    untracked_funding_symbols: HashSet<String>,
}

impl FundingRateStrategy {
    fn new(publisher: SignalPublisher) -> Self {
        // 触发配置加载
        let _ = &*ARBITRAGE_THRESHOLDS;
        
        // 从配置中获取binance的所有symbol和阈值
        let mut binance_symbol_states = HashMap::new();
        
        let symbols_map = ARBITRAGE_THRESHOLDS.get_binance_thresholds();
        for (symbol, threshold) in symbols_map {
            // 创建SymbolState
            let state = SymbolState {
                // 最优报价字段
                bid_price: 0.0,
                bid_amount: 0.0,
                ask_price: 0.0,
                ask_amount: 0.0,
                last_update_time: 0,
                // 阈值
                spread_deviation_threshold: threshold,
                // 资金费率
                funding_state: FundingRateData::new(),
                // 信号状态
                spread_signal: 0,
                funding_signal: 0,
            };
            
            binance_symbol_states.insert(symbol, state);
        }
        
        info!("Loaded {} tracking symbols from config", binance_symbol_states.len());
        
        Self {
            binance_symbol_states,
            publisher,
            messages_processed: 0,
            spread_messages: 0,
            funding_messages: 0,
            spread_updates: 0,
            trade_events_sent: 0,
            last_stats_time: Instant::now(),
            received_spread_symbols: HashSet::new(),
            received_funding_symbols: HashSet::new(),
            untracked_spread_symbols: HashSet::new(),
            untracked_funding_symbols: HashSet::new(),
        }
    }
    
    /// 处理币安现货买卖价差消息
    fn process_spot_spread(&mut self, data: &[u8]) {
        self.spread_messages += 1;
        
        // 解析消息
        let symbol = AskBidSpreadMsg::get_symbol(data);
        // 记录收到的symbol（区分是否在跟踪列表中）
        if self.binance_symbol_states.contains_key(symbol) {
            self.received_spread_symbols.insert(symbol.to_string());
        } else {
            self.untracked_spread_symbols.insert(symbol.to_string());
        }
        
        // 获取symbol的状态
        let symbol_state = match self.binance_symbol_states.get_mut(symbol) {
            Some(state) => state,
            None => return, // 不在跟踪列表中，跳过
        };
        
        let timestamp = AskBidSpreadMsg::get_timestamp(data);
        let bid_price = AskBidSpreadMsg::get_bid_price(data);
        let bid_amount = AskBidSpreadMsg::get_bid_amount(data);
        let ask_price = AskBidSpreadMsg::get_ask_price(data);
        let ask_amount = AskBidSpreadMsg::get_ask_amount(data);
        
        // 检查时间戳是否增加
        if timestamp <= symbol_state.last_update_time && timestamp != 0 {
            // 时间戳没有增加，跳过
            return;
        }
        
        // 更新报价
        symbol_state.bid_price = bid_price;
        symbol_state.bid_amount = bid_amount;
        symbol_state.ask_price = ask_price;
        symbol_state.ask_amount = ask_amount;
        symbol_state.last_update_time = timestamp;
        self.spread_updates += 1;
        
        // 检查价差信号
        self.check_spread_signal(symbol, timestamp);
    }
    
    /// 处理资金费率消息
    fn process_funding_rate(&mut self, data: &[u8]) {
        self.funding_messages += 1;
        
        // 使用零拷贝方法解析symbol
        let symbol = FundingRateMsg::get_symbol(data);
        // 记录收到的symbol（区分是否在跟踪列表中）
        if self.binance_symbol_states.contains_key(symbol) {
            self.received_funding_symbols.insert(symbol.to_string());
        } else {
            self.untracked_funding_symbols.insert(symbol.to_string());
        }
        
        // 获取symbol的状态
        let symbol_state = match self.binance_symbol_states.get_mut(symbol) {
            Some(state) => state,
            None => return, // 不在跟踪列表中，跳过
        };
        
        // 使用零拷贝方法解析所有字段
        let funding_rate = FundingRateMsg::get_funding_rate(data);
        let next_funding_time = FundingRateMsg::get_next_funding_time(data);
        let timestamp = FundingRateMsg::get_timestamp(data);
        let predicted_rate = FundingRateMsg::get_predicted_funding_rate(data);
        let loan_rate = FundingRateMsg::get_loan_rate_8h(data);
        
        // 更新资金费率状态
        let updated = symbol_state.funding_state.update(
            funding_rate,
            predicted_rate,
            loan_rate,
            next_funding_time,
            timestamp
        );
        
        // 如果更新成功，检查资金费率信号
        if updated {
            self.check_funding_signal(symbol);
        }
    }
    
    /// 打印统计信息
    fn print_stats(&mut self) {
        let elapsed = self.last_stats_time.elapsed().as_secs();
        if elapsed > 0 {
            let msg_per_sec = self.messages_processed as f64 / elapsed as f64;
            info!(
                "Stats: msgs={} spread={}/{} funding={} events={} msg/s={:.1} symbols={}",
                self.messages_processed,
                self.spread_updates,  // 实际更新的
                self.spread_messages,  // 总接收的
                self.funding_messages,
                self.trade_events_sent,
                msg_per_sec,
                self.binance_symbol_states.len()
            );
            // 打印本周期收到过的symbol集合（分别打印 spread / funding，且仅展示在跟踪列表中的）
            self.print_symbol_set("spread(tracked)", &self.received_spread_symbols);
            self.print_symbol_set("funding(tracked)", &self.received_funding_symbols);
            // 可选：打印未跟踪但收到的符号，帮助排查订阅面过宽
            if !self.untracked_spread_symbols.is_empty() {
                let mut list: Vec<_> = self.untracked_spread_symbols.iter().take(10).cloned().collect();
                list.sort();
                info!("  untracked spread symbols (≤10): {}", list.join(", "));
                if self.untracked_spread_symbols.len() > 10 {
                    info!("  ... and {} more", self.untracked_spread_symbols.len() - 10);
                }
            }
            if !self.untracked_funding_symbols.is_empty() {
                let mut list: Vec<_> = self.untracked_funding_symbols.iter().take(10).cloned().collect();
                list.sort();
                info!("  untracked funding symbols (≤10): {}", list.join(", "));
                if self.untracked_funding_symbols.len() > 10 {
                    info!("  ... and {} more", self.untracked_funding_symbols.len() - 10);
                }
            }
            
            // 打印一些示例报价
            for (symbol, state) in self.binance_symbol_states.iter().take(3) {
                let spread_deviation = if state.bid_price > 0.0 {
                    (state.ask_price - state.bid_price) / state.bid_price
                } else {
                    0.0
                };
                info!("  {} bid={:.2} ask={:.2} spread_signal={} funding_signal={}",
                    symbol, state.bid_price, state.ask_price, 
                    state.spread_signal, state.funding_signal);
                
                // 如果资金费率准备好，打印详细信息
                if state.funding_state.is_ready() {
                    info!("    deviation={:.6} threshold={:.6} | funding={:.6} predicted={:.6} loan={:.6}",
                        spread_deviation,
                        state.spread_deviation_threshold,
                        state.funding_state.funding_rate,
                        state.funding_state.predicted_rate,
                        state.funding_state.loan_rate);
                }
            }
            // 清空已收集的symbol集，便于下个统计周期观察
            self.received_spread_symbols.clear();
            self.received_funding_symbols.clear();
            self.untracked_spread_symbols.clear();
            self.untracked_funding_symbols.clear();
        }
    }

    fn print_symbol_set(&self, label: &str, set: &HashSet<String>) {
        if set.is_empty() {
            info!("  {}: <none this interval>", label);
            return;
        }
        // 为避免日志过长，仅打印最多前20个
        let mut list: Vec<_> = set.iter().take(20).cloned().collect();
        list.sort();
        info!("  {} (≤20): {}", label, list.join(", "));
        if set.len() > 20 {
            info!("  ... and {} more in {}", set.len() - 20, label);
        }
    }
    
    /// 发送交易事件
    fn send_trade_event(&mut self, event: TradeEvent) {
        // 序列化为字节
        match bincode::serialize(&event) {
            Ok(bytes) => {
                // 发送到IceOryx
                if let Err(e) = self.publisher.publish(&bytes) {
                    log::error!("Failed to publish trade event: {}", e);
                } else {
                    self.trade_events_sent += 1;
                    info!("Trade Event: {} {} {} (spread_dev={:.6}, funding={:.6}, predicted={:.6})",
                        event.event_type, event.symbol, event.signal_type,
                        event.spread_deviation, event.funding_rate, event.predicted_rate
                    );
                }
            }
            Err(e) => {
                log::error!("Failed to serialize trade event: {}", e);
            }
        }
    }
    
    /// 检查价差信号（开仓信号）
    fn check_spread_signal(&mut self, symbol: &str, timestamp: i64) {
        // 为了避免可变借用冲突，先构建事件数据，再在作用域外发送
        let mut trade_event_opt: Option<TradeEvent> = None;

        {
            //确认是交易符号
            let state = match self.binance_symbol_states.get_mut(symbol) {
                Some(s) => s,
                None => return,
            };

            //特殊过滤
            if state.bid_price <= 0.0 {
                return;
            }

            let spread_deviation = (state.ask_price - state.bid_price) / state.bid_price;
            let new_signal = if spread_deviation > state.spread_deviation_threshold {
                1  // 价差偏离大于阈值，做多
            } else {
                -1 // 否则做空
            };

            // 信号变化时记录并发送交易事件
            if new_signal != state.spread_signal {
                info!(
                    "Spread signal changed for {}: {} -> {} (deviation={:.6})",
                    symbol, state.spread_signal, new_signal, spread_deviation
                );

                // 生成交易事件
                let event_type = if new_signal == 1 {
                    "open_long".to_string()
                } else {
                    "open_short".to_string()
                };

                let trade_event = TradeEvent {
                    symbol: symbol.to_string(),
                    exchange: "binance".to_string(),
                    event_type,
                    signal_type: "spread".to_string(),
                    spread_deviation,
                    spread_threshold: state.spread_deviation_threshold,
                    funding_rate: state.funding_state.funding_rate,
                    predicted_rate: state.funding_state.predicted_rate,
                    loan_rate: state.funding_state.loan_rate,
                    bid_price: state.bid_price,
                    bid_amount: state.bid_amount,
                    ask_price: state.ask_price,
                    ask_amount: state.ask_amount,
                    timestamp,
                };

                // 先更新内部状态，再在作用域外发送事件
                state.spread_signal = new_signal;
                trade_event_opt = Some(trade_event);
            }
        }

        if let Some(event) = trade_event_opt {
            self.send_trade_event(event);
        }
    }
    
    /// 检查资金费率信号（开仓和平仓信号）
    fn check_funding_signal(&mut self, symbol: &str) {
        let mut trade_event_opt: Option<TradeEvent> = None;

        {
            let state = match self.binance_symbol_states.get_mut(symbol) {
                Some(s) => s,
                None => return,
            };

            if !state.funding_state.is_ready() {
                return;
            }

            // 常量定义
            const OPEN_SHORT_THRESHOLD: f64 = 0.00008; // 开空仓阈值
            const OPEN_LONG_THRESHOLD: f64 = -0.00008; // 开多仓阈值
            const CLOSE_THRESHOLD: f64 = 0.0005; // 平仓阈值

            let predicted = state.funding_state.predicted_rate;
            let loan_rate = state.funding_state.loan_rate;
            let current = state.funding_state.funding_rate;

            // 先检查平仓信号（优先级更高）
            let new_signal = if current > CLOSE_THRESHOLD {
                -2 // 平多仓
            } else if current < -CLOSE_THRESHOLD {
                2 // 平空仓
            } else if predicted >= OPEN_SHORT_THRESHOLD {
                -1 // 开空仓
            } else if predicted + loan_rate <= OPEN_LONG_THRESHOLD {
                1 // 开多仓
            } else {
                state.funding_signal // 保持当前信号
            };

            // 信号变化时记录并发送交易事件
            if new_signal != state.funding_signal {
                info!(
                    "Funding signal changed for {}: {} -> {} (predicted={:.6}, current={:.6}, loan={:.6})",
                    symbol, state.funding_signal, new_signal, predicted, current, loan_rate
                );

                // 生成交易事件
                let event_type = match new_signal {
                    1 => "open_long".to_string(),
                    -1 => "open_short".to_string(),
                    2 => "close_short".to_string(),
                    -2 => "close_long".to_string(),
                    _ => return,
                };

                let spread_deviation = if state.bid_price > 0.0 {
                    (state.ask_price - state.bid_price) / state.bid_price
                } else {
                    0.0
                };

                let trade_event = TradeEvent {
                    symbol: symbol.to_string(),
                    exchange: "binance".to_string(),
                    event_type,
                    signal_type: "funding".to_string(),
                    spread_deviation,
                    spread_threshold: state.spread_deviation_threshold,
                    funding_rate: current,
                    predicted_rate: predicted,
                    loan_rate,
                    bid_price: state.bid_price,
                    bid_amount: state.bid_amount,
                    ask_price: state.ask_price,
                    ask_amount: state.ask_amount,
                    timestamp: state.funding_state.timestamp,
                };

                // 更新状态，再在作用域外发送
                state.funding_signal = new_signal;
                trade_event_opt = Some(trade_event);
            }
        }

        if let Some(event) = trade_event_opt {
            self.send_trade_event(event);
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // 初始化日志
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();
    
    info!("Starting funding rate strategy");
    
    // 创建 IceOryx 发布器 - MT套利频道
    let publisher = SignalPublisher::new("mt_arbitrage")?;
    info!("Created MT arbitrage publisher");
    
    // 创建策略实例
    let mut strategy = FundingRateStrategy::new(publisher);
    
    // 创建 IceOryx 订阅器
    let node_name = format!("funding_strategy_{}", std::process::id());
    let mut subscriber = MultiChannelSubscriber::new(&node_name)?;
    
    // 订阅频道
    let subscriptions = vec![
        // 币安现货买卖价差
        SubscribeParams {
            exchange: "binance".to_string(),
            channel: ChannelType::AskBidSpread,
        },
        // 币安期货衍生品数据（包含资金费率）
        SubscribeParams {
            exchange: "binance-futures".to_string(),
            channel: ChannelType::Derivatives,
        },
    ];
    
    subscriber.subscribe_channels(subscriptions)?;
    info!("Subscribed to all channels");
    
    // 统计定时器
    let mut stats_timer = interval(Duration::from_secs(5));
    
    // 主循环
    loop {
        // 优先处理控制信号与定时器（非阻塞）
        select! {
            _ = signal::ctrl_c() => {
                info!("Received shutdown signal");
                break;
            }
            _ = stats_timer.tick() => {
                strategy.print_stats();
            }
            else => {
                let messages = subscriber.poll_msgs(Some(16));
                info!("polling msgs...");
                for msg_bytes in messages {
                    let msg_type = mkt_msg::get_msg_type(&msg_bytes);
                    match msg_type {
                        MktMsgType::AskBidSpread => {
                            strategy.process_spot_spread(&msg_bytes);
                        }
                        MktMsgType::FundingRate => {
                            strategy.process_funding_rate(&msg_bytes);
                        }
                        _ => {}
                    }
                }
                break;
            }
        }
    }
    info!("Funding rate strategy shutdown");
    Ok(())
}
