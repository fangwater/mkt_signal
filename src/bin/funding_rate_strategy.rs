use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::Parser;
use iceoryx2::prelude::*;
use log::{debug, info};
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::select;
use tokio::signal;
use tokio::time::{interval, Instant};

/// 资费策略信号生成器
/// 订阅多个交易所的现货买卖价差和衍生品数据，生成套利信号
#[derive(Parser, Debug)]
#[command(name = "funding_rate_strategy")]
#[command(about = "Funding rate arbitrage strategy signal generator")]
struct Args {
    /// 是否只订阅测试符号
    #[arg(short, long, default_value_t = false)]
    test_mode: bool,
}

/// 交易所类型
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Exchange {
    Binance,
    BinanceFutures,
    Okex,
    OkexSwap,
    Bybit,
    BybitSpot,
}

impl Exchange {
    fn as_str(&self) -> &str {
        match self {
            Exchange::Binance => "binance",
            Exchange::BinanceFutures => "binance-futures",
            Exchange::Okex => "okex",
            Exchange::OkexSwap => "okex-swap",
            Exchange::Bybit => "bybit",
            Exchange::BybitSpot => "bybit-spot",
        }
    }
    
    fn is_futures(&self) -> bool {
        matches!(self, Exchange::BinanceFutures | Exchange::OkexSwap | Exchange::Bybit)
    }
}

/// 买卖价差数据
#[derive(Debug, Clone)]
struct SpreadData {
    symbol: String,
    exchange: Exchange,
    timestamp: i64,
    bid_price: f64,
    bid_amount: f64,
    ask_price: f64,
    ask_amount: f64,
    mid_price: f64,
    spread: f64,
}

/// 资金费率数据
#[derive(Debug, Clone)]
struct FundingRateData {
    symbol: String,
    exchange: Exchange,
    timestamp: i64,
    funding_rate: f64,
    next_funding_time: i64,
}

/// 标记价格数据
#[derive(Debug, Clone)]
struct MarkPriceData {
    symbol: String,
    exchange: Exchange,
    timestamp: i64,
    mark_price: f64,
}

/// 套利机会信号
#[derive(Debug, Clone, Serialize)]
struct ArbitrageSignal {
    timestamp: DateTime<Utc>,
    symbol: String,
    spot_exchange: String,
    futures_exchange: String,
    spot_bid: f64,
    spot_ask: f64,
    futures_bid: f64,
    futures_ask: f64,
    funding_rate: f64,
    next_funding_hours: f64,
    #[serde(rename = "type")]
    signal_type: SignalType,
    expected_profit_bps: f64,  // 预期收益（基点）
}

#[derive(Debug, Clone, Serialize)]
enum SignalType {
    #[serde(rename = "positive_funding")]
    PositiveFunding,  // 正资金费率套利（做空期货，做多现货）
    #[serde(rename = "negative_funding")]
    NegativeFunding,  // 负资金费率套利（做多期货，做空现货）
}

struct FundingRateStrategy {
    // 现货市场数据
    spot_spreads: HashMap<(Exchange, String), SpreadData>,
    
    // 期货市场数据
    futures_spreads: HashMap<(Exchange, String), SpreadData>,
    funding_rates: HashMap<(Exchange, String), FundingRateData>,
    mark_prices: HashMap<(Exchange, String), MarkPriceData>,
    
    // 统计
    stats: Statistics,
    
    // 配置
    min_funding_rate_threshold: f64,  // 最小资金费率阈值（绝对值）
    min_profit_threshold_bps: f64,    // 最小利润阈值（基点）
}

struct Statistics {
    messages_received: u64,
    spreads_received: u64,
    funding_rates_received: u64,
    signals_generated: u64,
    last_reset: Instant,
}

impl FundingRateStrategy {
    fn new() -> Self {
        Self {
            spot_spreads: HashMap::new(),
            futures_spreads: HashMap::new(),
            funding_rates: HashMap::new(),
            mark_prices: HashMap::new(),
            stats: Statistics {
                messages_received: 0,
                spreads_received: 0,
                funding_rates_received: 0,
                signals_generated: 0,
                last_reset: Instant::now(),
            },
            min_funding_rate_threshold: 0.0001,  // 0.01%
            min_profit_threshold_bps: 5.0,       // 5个基点
        }
    }
    
    fn process_spread(&mut self, data: SpreadData) {
        self.stats.spreads_received += 1;
        
        let symbol = data.symbol.clone();
        let key = (data.exchange.clone(), data.symbol.clone());
        
        if data.exchange.is_futures() {
            self.futures_spreads.insert(key, data);
        } else {
            self.spot_spreads.insert(key, data);
        }
        
        // 尝试生成套利信号
        self.check_arbitrage_opportunity(&symbol);
    }
    
    fn process_funding_rate(&mut self, data: FundingRateData) {
        self.stats.funding_rates_received += 1;
        
        let key = (data.exchange.clone(), data.symbol.clone());
        self.funding_rates.insert(key, data.clone());
        
        // 尝试生成套利信号
        self.check_arbitrage_opportunity(&data.symbol);
    }
    
    fn check_arbitrage_opportunity(&mut self, symbol: &str) {
        // 标准化符号（移除交易所特定的后缀）
        let base_symbol = symbol.replace("-SWAP", "").replace("PERP", "");
        
        // 检查所有可能的现货-期货组合
        for spot_exchange in [Exchange::Binance, Exchange::Okex, Exchange::BybitSpot] {
            for futures_exchange in [Exchange::BinanceFutures, Exchange::OkexSwap, Exchange::Bybit] {
                if let Some(signal) = self.calculate_arbitrage_signal(
                    &base_symbol,
                    &spot_exchange,
                    &futures_exchange,
                ) {
                    self.emit_signal(signal);
                }
            }
        }
    }
    
    fn calculate_arbitrage_signal(
        &self,
        symbol: &str,
        spot_exchange: &Exchange,
        futures_exchange: &Exchange,
    ) -> Option<ArbitrageSignal> {
        // 获取现货价差
        let spot_spread = self.spot_spreads.get(&(spot_exchange.clone(), symbol.to_string()))?;
        
        // 获取期货价差
        let futures_spread = self.futures_spreads.get(&(futures_exchange.clone(), symbol.to_string()))?;
        
        // 获取资金费率
        let funding_rate = self.funding_rates.get(&(futures_exchange.clone(), symbol.to_string()))?;
        
        // 计算到下次资金费用的小时数
        let now = Utc::now().timestamp_millis();
        let next_funding_hours = (funding_rate.next_funding_time - now) as f64 / (1000.0 * 3600.0);
        
        // 跳过太久之后的资金费率
        if next_funding_hours > 8.5 || next_funding_hours < 0.0 {
            return None;
        }
        
        // 检查资金费率是否满足阈值
        if funding_rate.funding_rate.abs() < self.min_funding_rate_threshold {
            return None;
        }
        
        // 计算预期收益
        let (signal_type, expected_profit_bps) = if funding_rate.funding_rate > 0.0 {
            // 正资金费率：做空期货（收取资金费），做多现货
            // 成本：现货买入价 - 期货卖出价 + 手续费
            // 收益：资金费率
            let cost_bps = ((spot_spread.ask_price - futures_spread.bid_price) / spot_spread.ask_price) * 10000.0;
            let profit_bps = funding_rate.funding_rate * 10000.0 - cost_bps - 10.0; // 减去约10个基点的手续费
            
            (SignalType::PositiveFunding, profit_bps)
        } else {
            // 负资金费率：做多期货（收取资金费），做空现货
            // 成本：期货买入价 - 现货卖出价 + 手续费
            // 收益：资金费率的绝对值
            let cost_bps = ((futures_spread.ask_price - spot_spread.bid_price) / futures_spread.ask_price) * 10000.0;
            let profit_bps = funding_rate.funding_rate.abs() * 10000.0 - cost_bps - 10.0;
            
            (SignalType::NegativeFunding, profit_bps)
        };
        
        // 检查预期收益是否满足阈值
        if expected_profit_bps < self.min_profit_threshold_bps {
            return None;
        }
        
        Some(ArbitrageSignal {
            timestamp: Utc::now(),
            symbol: symbol.to_string(),
            spot_exchange: spot_exchange.as_str().to_string(),
            futures_exchange: futures_exchange.as_str().to_string(),
            spot_bid: spot_spread.bid_price,
            spot_ask: spot_spread.ask_price,
            futures_bid: futures_spread.bid_price,
            futures_ask: futures_spread.ask_price,
            funding_rate: funding_rate.funding_rate,
            next_funding_hours,
            signal_type,
            expected_profit_bps,
        })
    }
    
    fn emit_signal(&mut self, signal: ArbitrageSignal) {
        self.stats.signals_generated += 1;
        
        // 输出JSON格式的信号
        if let Ok(json) = serde_json::to_string(&signal) {
            info!("SIGNAL: {}", json);
        }
        
        // TODO: 发送到下游系统（Redis、消息队列等）
    }
    
    fn print_stats(&mut self) {
        let elapsed = self.stats.last_reset.elapsed().as_secs();
        if elapsed > 0 {
            info!(
                "Stats [{}s]: messages: {}, spreads: {}, funding_rates: {}, signals: {}, msg/s: {:.1}",
                elapsed,
                self.stats.messages_received,
                self.stats.spreads_received,
                self.stats.funding_rates_received,
                self.stats.signals_generated,
                self.stats.messages_received as f64 / elapsed as f64
            );
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "DEBUG");
    env_logger::init();
    let args = Args::parse();
    
    info!("Starting funding rate arbitrage strategy signal generator");
    info!("Test mode: {}", args.test_mode);
    
    // 创建策略实例
    let mut strategy = FundingRateStrategy::new();
    
    // 创建IceOryx node
    let node_name = format!("funding_strategy_{}", std::process::id());
    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;
    
    // 订阅的交易所列表
    let exchanges = if args.test_mode {
        vec![Exchange::Binance, Exchange::BinanceFutures]
    } else {
        vec![
            Exchange::Binance,
            Exchange::BinanceFutures,
            Exchange::Okex,
            Exchange::OkexSwap,
            Exchange::Bybit,
            Exchange::BybitSpot,
        ]
    };
    
    // 创建订阅者
    let mut spread_subscribers = Vec::new();
    let mut derivatives_subscribers = Vec::new();
    
    for exchange in &exchanges {
        // 订阅买卖价差数据
        let spread_service = node
            .service_builder(&ServiceName::new(&format!("data_pubs/{}/ask_bid_spread", exchange.as_str()))?)
            .publish_subscribe::<[u8; 512]>()
            .open_or_create()?;
        
        let spread_subscriber = spread_service.subscriber_builder().create()?;
        spread_subscribers.push((exchange.clone(), spread_subscriber));
        
        // 订阅衍生品数据（只有期货交易所）
        if exchange.is_futures() {
            let derivatives_service = node
                .service_builder(&ServiceName::new(&format!("data_pubs/{}/derivatives", exchange.as_str()))?)
                .publish_subscribe::<[u8; 1024]>()
                .open_or_create()?;
            
            let derivatives_subscriber = derivatives_service.subscriber_builder().create()?;
            derivatives_subscribers.push((exchange.clone(), derivatives_subscriber));
        }
    }
    
    info!("Subscribed to {} exchanges", exchanges.len());
    
    // 统计定时器
    let mut stats_timer = interval(Duration::from_secs(10));
    
    // 主事件循环
    loop {
        select! {
            _ = signal::ctrl_c() => {
                info!("Received shutdown signal");
                break;
            }
            
            _ = stats_timer.tick() => {
                strategy.print_stats();
            }
            
            // 这里应该添加实际的消息处理逻辑
            // 由于IceOryx的接收需要轮询，我们使用非阻塞接收
            _ = tokio::time::sleep(Duration::from_millis(1)) => {
                // 处理买卖价差数据
                for (exchange, subscriber) in &spread_subscribers {
                    while let Some(sample) = subscriber.receive()? {
                        strategy.stats.messages_received += 1;
                        
                        // 解析消息并处理
                        // TODO: 实现实际的消息解析
                        debug!("Received spread data from {}", exchange.as_str());
                    }
                }
                
                // 处理衍生品数据
                for (exchange, subscriber) in &derivatives_subscribers {
                    while let Some(sample) = subscriber.receive()? {
                        strategy.stats.messages_received += 1;
                        
                        // 解析消息并处理
                        // TODO: 实现实际的消息解析
                        debug!("Received derivatives data from {}", exchange.as_str());
                    }
                }
            }
        }
    }
    
    info!("Funding rate strategy shutdown complete");
    Ok(())
}