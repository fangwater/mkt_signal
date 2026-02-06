use crate::common::bbo::{Bbo, DualBbo};
use crate::common::time_util::get_timestamp_us;
use crate::common::trade_error_code::describe_trade_error_code;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::{OrderExecutionStatus, OrderType, Side};
use crate::pre_trade::{PersistChannel, SignalChannel, TradeEngHub};
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::common::{
    align_price_floor, ExecutionType, OrderStatus, SignalBytes, TimeInForce, TradingVenue,
};
use crate::signal::hedge_signal::ArbHedgeCtx;
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::record::SignalRecordMessage;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::manager::{ForceCloseControl, Strategy};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::query_engine_response::QueryEngineResponse;
use crate::strategy::query_order_updates::{OrderQueryOrderUpdate, OrderQueryTradeUpdate};
use crate::strategy::trade_engine_response::{TradeEngineResponse, TradeRequestKind};
use crate::strategy::trade_update::TradeUpdate;
use crate::trade_engine::query_parsers::compact_order::{
    CompactOrderQueryResp, COMPACT_ORDER_QUERY_RESP_LEN,
};
use crate::trade_engine::query_request::{GenericQueryRequest, QueryRequestType};
use log::{debug, error, info, warn};
use std::any::Any;
use std::collections::{HashMap, HashSet};

// 下单后若迟迟收不到 account monitor 的推送（New/Filled 等），用一次 query 回补。
// 仅用于“发出请求但没收到回报”的兜底，不做持续轮询，避免 maker 长时间挂单时产生额外负担。
const ORDER_QUERY_WATCHDOG_DELAY_US: i64 = 300_000;
const CANCEL_QUERY_WATCHDOG_DELAY_US: i64 = 300_000;
const HEDGE_RESIDUAL_EPS: f64 = 1e-12;

pub struct HedgeArbStrategy {
    pub strategy_id: i32,                         //策略id
    pub open_symbol: String,                      //开仓侧symbol
    pub open_venue: TradingVenue,                 //开仓侧交易场所
    pub open_order_id: i64,                       //开仓单唯一，报多单对应多个Strategy
    pub hedge_order_ids: Vec<i64>,                //对冲单会产生一个or多个，因为部分成交
    pub open_expire_ts: Option<i64>,              //开仓单挂单截止时间（绝对时间戳）
    pub hedge_timeout_us: Option<i64>, //对冲单允许的存活时间（微秒），>0 表示 MM，None 表示 MT
    pub hedge_expire_ts: Option<i64>,  //当前对冲挂单的截止时间（绝对时间戳）
    pub order_seq: u32,                //订单号计数器
    pub cumulative_hedged_qty: f64,    //累计对冲数量
    pub cumulative_open_qty: f64,      //累计开仓数量
    pub open_filled_hedge_triggered: bool, //开仓成交已触发对冲（防止query重复触发）
    pub alive_flag: bool,              //策略是否存活
    pub hedge_symbol: String,          //对冲侧symbol
    pub hedge_venue: TradingVenue,     //对冲侧交易场所
    pub hedge_side: Side,              //对冲侧方向
    pub hedge_request_seq: u32,        //累计对冲请求次数
    pub open_signal_ts: i64,    //开仓信号时间戳（微秒）
    pub open_from_key: String,  //开仓来源标记
    pub hedge_from_key: String, //对冲来源标记
    pub open_bbo: DualBbo,  //开仓时双腿盘口
    pub hedge_bbo: DualBbo, //对冲时刻双腿盘口
    pub force_close_mode: bool,        //是否强平模式
    pub hedge_retry_after_ts: Option<i64>, //对冲报单失败后的冷却截止时间
    pub hedge_retry_reason: Option<&'static str>, //对冲报单失败的重试原因
    pending_order_queries: HashMap<i64, PendingOrderQueryReason>,
    order_query_watchdog: Option<QueryWatchdog>,
    cancel_query_watchdog: Option<QueryWatchdog>,
    processed_cancel_updates: HashSet<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Leg {
    Open,
    Hedge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingOrderQueryReason {
    OpenLegCancelFailed,
    HedgeLegCancelFailed,
    CancelRejected,
    OrderWatchdog,
    CancelWatchdog,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QueryWatchdog {
    client_order_id: i64,
    due_ts_us: i64,
    reason: PendingOrderQueryReason,
}

impl HedgeArbStrategy {
    pub fn new(id: i32, symbol: String) -> Self {
        let strategy = Self {
            strategy_id: id,
            open_symbol: symbol,
            open_venue: TradingVenue::BinanceMargin, // 默认值，将在开仓时更新
            open_order_id: 0,
            hedge_order_ids: Vec::new(),
            open_expire_ts: None,
            hedge_timeout_us: None,
            hedge_expire_ts: None,
            order_seq: 0,
            cumulative_hedged_qty: 0.0,
            cumulative_open_qty: 0.0,
            open_filled_hedge_triggered: false,
            alive_flag: true,
            hedge_symbol: String::new(),
            hedge_venue: TradingVenue::BinanceMargin, // 默认值，将在开仓时更新
            hedge_side: Side::Buy,                    // 默认值，将在开仓时更新
            hedge_request_seq: 0,
            open_signal_ts: 0,
            open_from_key: String::new(),
            hedge_from_key: String::new(),
            open_bbo: DualBbo::default(),
            hedge_bbo: DualBbo::default(),
            force_close_mode: false,
            hedge_retry_after_ts: None,
            hedge_retry_reason: None,
            pending_order_queries: HashMap::new(),
            order_query_watchdog: None,
            cancel_query_watchdog: None,
            processed_cancel_updates: HashSet::new(),
        };
        strategy
    }

    fn describe_venue(venue: u8) -> String {
        TradingVenue::from_u8(venue)
            .map(|v| format!("{:?}", v))
            .unwrap_or_else(|| format!("Unknown({})", venue))
    }

    fn hedge_invalid_param_reason(code: i32) -> Option<&'static str> {
        match code {
            4001 | -4001 => Some("PRICE_LESS_THAN_ZERO/价格小于0"),
            4002 | -4002 => Some("PRICE_GREATER_THAN_MAX_PRICE/价格超过最大值"),
            -4003 => Some("QTY_LESS_THAN_ZERO/数量小于0"),
            -4004 => Some("QTY_LESS_THAN_MIN_QTY/数量小于最小值"),
            -4005 => Some("QTY_GREATER_THAN_MAX_QTY/数量大于最大值"),
            -4006 => Some("STOP_PRICE_LESS_THAN_ZERO/触发价小于最小值"),
            -4007 => Some("STOP_PRICE_GREATER_THAN_MAX_PRICE/触发价大于最大值"),
            -4008 => Some("TICK_SIZE_LESS_THAN_ZERO/价格精度小于0"),
            -4009 => Some("MAX_PRICE_LESS_THAN_MIN_PRICE/最大价格小于最小价格"),
            _ => None,
        }
    }

    fn push_hedge_residual_with_print(
        &mut self,
        reason: &str,
        detail: &str,
        qty: f64,
        price: f64,
        side: Side,
    ) {
        let added = if qty > HEDGE_RESIDUAL_EPS {
            MonitorChannel::instance().inc_hedge_residual(
                self.hedge_symbol.clone(),
                self.hedge_venue,
                qty,
            );
            qty
        } else {
            0.0
        };

        println!(
            "HedgeArbStrategy: strategy_id={} 对冲拒绝({}) | open_symbol={} hedge_symbol={} hedge_venue={:?} side={:?} qty={:.8} price={:.8} residual_add={:.8} detail={}",
            self.strategy_id,
            reason,
            self.open_symbol,
            self.hedge_symbol,
            self.hedge_venue,
            side,
            qty,
            price,
            added,
            detail
        );
    }

    fn log_force_close_skip(&self, check_name: &str, ctx: &ArbOpenCtx) {
        let opening_symbol = ctx.get_opening_symbol();
        let hedging_symbol = ctx.get_hedging_symbol();
        let opening_venue = Self::describe_venue(ctx.opening_leg.venue);
        let hedging_venue = Self::describe_venue(ctx.hedging_leg.venue);
        let side = Side::from_u8(ctx.side).unwrap_or(Side::Buy);
        info!(
            "HedgeArbStrategy: strategy_id={} force_close_mode=true, skip {} 风控 | opening={} {} hedging={} {} side={:?} amount={:.4} price={:.6}",
            self.strategy_id,
            check_name,
            opening_symbol,
            opening_venue,
            hedging_symbol,
            hedging_venue,
            side,
            ctx.amount,
            ctx.price
        );
    }

    /// 组合订单ID：高32位为策略ID，低32位为序列号
    fn compose_order_id(strategy_id: i32, seq: u32) -> i64 {
        ((strategy_id as i64) << 32) | seq as i64
    }

    /// 从订单ID中提取策略ID
    fn extract_strategy_id(order_id: i64) -> i32 {
        (order_id >> 32) as i32
    }

    fn handle_arb_open_signal(&mut self, ctx: ArbOpenCtx) {
        // 开仓open leg，打开头寸，根据信号创建开仓leg, 进行风控判断，失败就直接把策略标记为不活跃，等待定时器清理

        let force_close = self.is_force_close_mode();
        if ctx.price <= 0.0 {
            let order_type = OrderType::from_u8(ctx.order_type);
            warn!(
                "HedgeArbStrategy: strategy_id={} open signal price invalid: symbol={} order_type={:?} price={:.8}",
                self.strategy_id,
                ctx.get_opening_symbol(),
                order_type,
                ctx.price
            );
            self.alive_flag = false;
            return;
        }
        if force_close {
            let opening_symbol = ctx.get_opening_symbol();
            let hedging_symbol = ctx.get_hedging_symbol();
            let opening_venue = Self::describe_venue(ctx.opening_leg.venue);
            let hedging_venue = Self::describe_venue(ctx.hedging_leg.venue);
            let side = Side::from_u8(ctx.side).unwrap_or(Side::Buy);
            info!(
                "HedgeArbStrategy: strategy_id={} force_close_mode=true, 将跳过所有风控 | opening={} {} hedging={} {} side={:?} amount={:.4} price={:.6}",
                self.strategy_id,
                opening_symbol,
                opening_venue,
                hedging_symbol,
                hedging_venue,
                side,
                ctx.amount,
                ctx.price
            );
        }

        // 2、检查symbol的敞口，失败打印error
        if force_close {
            self.log_force_close_skip("单品种敞口", &ctx);
        } else if let Err(e) = MonitorChannel::instance().check_symbol_exposure(&self.open_symbol) {
            error!("HedgeArbStrategy: strategy_id={} symbol={} 单品种敞口风控检查失败: {}，标记策略为不活跃", self.strategy_id, self.open_symbol, e);
            self.alive_flag = false;
            return;
        }

        // 3、检查总敞口，失败打印error
        if force_close {
            self.log_force_close_skip("总体敞口", &ctx);
        } else if let Err(e) = MonitorChannel::instance().check_total_exposure() {
            error!(
                "HedgeArbStrategy: strategy_id={} 总敞口风控检查失败: {}，标记策略为不活跃",
                self.strategy_id, e
            );
            self.alive_flag = false;
            return;
        }

        // 4、检查限价挂单数量限制（如果是限价单）
        let order_type = OrderType::from_u8(ctx.order_type);
        if order_type == Some(OrderType::Limit) {
            if let Err(e) = MonitorChannel::instance().check_pending_limit_order(&self.open_symbol)
            {
                error!("HedgeArbStrategy: strategy_id={} symbol={} 限价挂单数量风控检查失败: {}，标记策略为不活跃", self.strategy_id, self.open_symbol, e);
                self.alive_flag = false;
                return;
            }
        }

        // 5、通过全部风控检查，生成订单ID
        self.order_seq += 1;
        let order_id = Self::compose_order_id(self.strategy_id, self.order_seq);
        self.open_order_id = order_id;

        // 6、根据资产类型创建开仓订单，并保存对冲侧信息
        self.open_order_id = order_id;
        self.cumulative_open_qty = 0.0;
        self.alive_flag = true;
        let ts = get_timestamp_us();
        self.open_expire_ts = if ctx.exp_time > 0 {
            Some(ctx.exp_time)
        } else {
            None
        };
        // Hedge timeout 为 0 表示 MT 模式，>0 为 MM 模式，保存原始 TTL，实际挂单后再换算绝对时间
        self.hedge_timeout_us = if ctx.hedge_timeout_us > 0 {
            Some(ctx.hedge_timeout_us)
        } else {
            None
        };
        self.hedge_expire_ts = None;
        self.hedge_retry_after_ts = None;
        self.hedge_retry_reason = None;

        // 保存对冲侧信息
        self.hedge_symbol = ctx.get_hedging_symbol();
        self.hedge_venue = TradingVenue::from_u8(ctx.hedging_leg.venue)
            .ok_or_else(|| format!("无效的对冲交易场所: {}", ctx.hedging_leg.venue))
            .unwrap();
        // 对冲侧方向与开仓侧相反
        let open_side = Side::from_u8(ctx.side).unwrap();
        self.hedge_side = if open_side == Side::Buy {
            Side::Sell
        } else {
            Side::Buy
        };
        self.open_signal_ts = ctx.create_ts;
        self.open_from_key = String::from_utf8_lossy(&ctx.from_key).to_string();
        self.open_bbo.open =
            Bbo::new(ctx.opening_leg.bid0, ctx.opening_leg.ask0, ctx.opening_leg.ts);
        self.open_bbo.hedge =
            Bbo::new(ctx.hedging_leg.bid0, ctx.hedging_leg.ask0, ctx.hedging_leg.ts);
        self.hedge_bbo = self.open_bbo;

        // 7、根据交易标的物，修正量、价格
        let symbol = ctx.get_opening_symbol();
        let raw_qty = ctx.amount as f64;
        let raw_price = ctx.price;

        let venue = TradingVenue::from_u8(ctx.opening_leg.venue)
            .ok_or_else(|| format!("无效的交易场所: {}", ctx.opening_leg.venue))
            .unwrap();

        // 保存开仓侧交易场所
        self.open_venue = venue;

        // 目前只支持 Binance / OKX / Gate 的 margin + futures，其它 venue 直接 panic
        match venue {
            TradingVenue::BinanceFutures
            | TradingVenue::BinanceMargin
            | TradingVenue::OkexFutures
            | TradingVenue::OkexMargin
            | TradingVenue::GateFutures
            | TradingVenue::GateMargin => {
                // 允许的交易场所，继续执行
            }
            _ => {
                panic!(
                    "HedgeArbStrategy: strategy_id={} 不支持的交易场所 {:?}，仅支持 Binance/OKX/Gate 的 futures 或 margin",
                    self.strategy_id,
                    venue
                );
            }
        }

        let align_result =
            MonitorChannel::instance().align_order_by_venue(venue, &symbol, raw_qty, raw_price);
        let (aligned_qty, aligned_price) = match align_result {
            Ok((qty, price)) => (qty, price),
            Err(e) => {
                error!(
                    "HedgeArbStrategy: strategy_id={} 订单对齐失败: {}，原始量={:.8} 原始价格={:.8}，标记策略为不活跃",
                    self.strategy_id,
                    e,
                    raw_qty,
                    raw_price
                );
                self.alive_flag = false;
                return;
            }
        };

        let signed_qty = match open_side {
            Side::Buy => aligned_qty.abs(),
            Side::Sell => -aligned_qty.abs(),
        };

        // 8、检查杠杆：若绝对持仓不增加，则可跳过
        if force_close {
            self.log_force_close_skip("杠杆", &ctx);
        } else {
            let add_base_qty = MonitorChannel::instance().qty_to_base(venue, &symbol, signed_qty);
            let current_base_qty = MonitorChannel::instance().get_position_qty(&symbol, venue);
            let projected_base_qty = current_base_qty + add_base_qty;
            let reduce_eps = 1e-12_f64;

            if projected_base_qty.abs() > current_base_qty.abs() + reduce_eps {
                if let Err(e) = MonitorChannel::instance().check_leverage() {
                    error!(
                        "HedgeArbStrategy: strategy_id={} 杠杆风控检查失败: {}，标记策略为不活跃",
                        self.strategy_id, e
                    );
                    self.alive_flag = false;
                    return;
                }
            }
        }

        // 9、考虑修正量，判断下单后是否会大于max u
        if force_close {
            self.log_force_close_skip("持仓上限 (max_pos_u)", &ctx);
        } else if let Err(e) =
            MonitorChannel::instance().ensure_max_pos_u(&symbol, signed_qty, aligned_price)
        {
            error!(
                "HedgeArbStrategy: strategy_id={} 仓位限制检查失败: {}，标记策略为不活跃",
                self.strategy_id, e
            );
            self.alive_flag = false;
            return;
        }

        // 10、用修正量价，开仓订单记录到order manager
        let client_order_id = MonitorChannel::instance()
            .order_manager()
            .borrow_mut()
            .create_order(
                venue,
                order_id,
                OrderType::from_u8(ctx.order_type).unwrap(),
                symbol.clone(),
                Side::from_u8(ctx.side).unwrap(),
                aligned_qty,
                aligned_price,
                self.is_force_close_mode(),
                ts,
            );
        info!(
            "📤 开仓订单已创建: strategy_id={} order_id={} client_order_id={} symbol={} {:?} side={:?} qty={:.4} price={:.6}",
            self.strategy_id, order_id, client_order_id, symbol, venue,
            Side::from_u8(ctx.side).unwrap(), aligned_qty, aligned_price
        );

        // 9、推送开仓订单到交易引擎
        if let Err(e) = self.create_and_send_order(client_order_id, "开仓", &symbol) {
            error!(
                "❌ 开仓订单发送失败: strategy_id={} {}",
                self.strategy_id, e
            );
            return;
        }
        info!(
            "✅ 开仓订单已发送: strategy_id={} client_order_id={}",
            self.strategy_id, client_order_id
        );
    }

    fn align_taker_qty(
        &self,
        venue: TradingVenue,
        symbol: &str,
        raw_qty: f64,
    ) -> Result<f64, String> {
        if raw_qty <= 0.0 {
            return Err(format!(
                "symbol={} 原始下单量无效 raw_qty={}",
                symbol, raw_qty
            ));
        }

        let symbol_key = match venue {
            TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
                symbol.to_uppercase().replace("-SWAP", "").replace('-', "")
            }
            TradingVenue::GateMargin | TradingVenue::GateFutures => {
                symbol.to_uppercase().replace('_', "").replace('-', "")
            }
            _ => symbol.to_uppercase(),
        };

        let Some(table) = MonitorChannel::instance().venue_min_qty_table(venue) else {
            return Err(format!(
                "未初始化 {:?} 的最小下单量表，请检查启动参数",
                venue
            ));
        };

        let mut qty = raw_qty;
        if venue == TradingVenue::OkexFutures {
            let contract_size = table.contract_multiplier_opt(&symbol_key).ok_or_else(|| {
                format!(
                    "symbol={} 缺少 OKX 合约乘数(ctVal×ctMult)，无法将 base qty 转成 contracts",
                    symbol_key
                )
            })?;
            if contract_size <= 0.0 {
                return Err(format!(
                    "symbol={} OKX contract multiplier invalid: {}",
                    symbol_key, contract_size
                ));
            }
            qty = raw_qty / contract_size;
        }

        if let Some(step) = table.step_size(&symbol_key) {
            if step > 0.0 {
                qty = align_price_floor(qty, step);
            }
        }

        if let Some(min_qty) = table.min_qty(&symbol_key) {
            if min_qty > 0.0 && qty < min_qty {
                qty = min_qty;
            }
        }

        if qty <= 0.0 {
            return Err(format!("symbol={} 对齐后数量无效 qty={}", symbol_key, qty));
        }

        Ok(qty)
    }

    // 收到对冲信号，按照需求进行maker对冲，或者直接taker对冲
    fn handle_arb_hedge_signal(&mut self, ctx: ArbHedgeCtx) -> Result<(), String> {
        // 1. 确定对冲数量
        let target_qty = if ctx.hedge_qty > 0.0 {
            ctx.hedge_qty as f64
        } else {
            warn!(
                "HedgeArbStrategy: strategy_id={} 对冲信号的数量无效: {}",
                self.strategy_id, ctx.hedge_qty
            );
            return Err(format!("对冲数量无效: {}", ctx.hedge_qty));
        };
        self.hedge_from_key = String::from_utf8_lossy(&ctx.from_key).to_string();

        if self.has_pending_hedge_order() {
            info!(
                "HedgeArbStrategy: strategy_id={} 对冲信号忽略（已有活跃对冲单） qty={:.8}",
                self.strategy_id, target_qty
            );
            return Ok(());
        }

        // 2. 获取对冲交易的 symbol 和 venue
        let hedge_symbol = ctx.get_hedging_symbol();
        let hedge_venue = TradingVenue::from_u8(ctx.hedging_leg.venue)
            .ok_or_else(|| format!("无效的对冲交易场所: {}", ctx.hedging_leg.venue))?;
        self.hedge_bbo.hedge =
            Bbo::new(ctx.hedging_leg.bid0, ctx.hedging_leg.ask0, ctx.hedging_leg.ts);
        self.hedge_bbo.open = Bbo::new(ctx.opening_leg.bid0, ctx.opening_leg.ask0, ctx.opening_leg.ts);
        let hedge_side = ctx
            .get_side()
            .ok_or_else(|| format!("无效的对冲方向: {}", ctx.hedge_side))?;
        if ctx.is_taker() {
            info!(
                "HedgeArbStrategy: strategy_id={} taker hedge signal hedge_symbol={} venue={:?} side={:?} qty={:.8}",
                self.strategy_id,
                hedge_symbol,
                hedge_venue,
                hedge_side,
                target_qty
            );
        }

        if ctx.is_maker() && ctx.limit_price <= 0.0 {
            let detail = format!(
                "limit_price_invalid client_order_id={} market_ts={} price_offset={:.6} opening_venue={} hedging_venue={}",
                ctx.client_order_id,
                ctx.market_ts,
                ctx.price_offset,
                ctx.opening_leg.venue,
                ctx.hedging_leg.venue
            );
            self.push_hedge_residual_with_print(
                "PRICE_LESS_THAN_ZERO/价格小于0",
                &detail,
                target_qty,
                ctx.limit_price,
                hedge_side,
            );
            return Ok(());
        }

        // 3. 使用预交易环境对齐订单量和价格
        let (aligned_qty, aligned_price) = if ctx.is_taker() {
            let aligned_qty = self.align_taker_qty(hedge_venue, &hedge_symbol, target_qty)?;
            (aligned_qty, 0.0)
        } else {
            // 使用get_hedge_price 函数获取合适的对冲价格
            let hedge_price = ctx.get_hedge_price();
            if hedge_price <= 0.0 {
                let detail = format!(
                    "pre_trade_price_invalid mode={} client_order_id={} market_ts={} price_offset={:.6} opening_venue={} hedging_venue={}",
                    if ctx.is_maker() { "maker" } else { "taker" },
                    ctx.client_order_id,
                    ctx.market_ts,
                    ctx.price_offset,
                    ctx.opening_leg.venue,
                    ctx.hedging_leg.venue
                );
                if ctx.is_maker() {
                    self.push_hedge_residual_with_print(
                        "PRICE_LESS_THAN_ZERO/价格小于0",
                        &detail,
                        target_qty,
                        hedge_price,
                        hedge_side,
                    );
                    return Ok(());
                }
                return Err(format!("对冲价格无效: {:.8}", hedge_price));
            }
            let (aligned_qty, aligned_price) = MonitorChannel::instance().align_order_by_venue(
                hedge_venue,
                &hedge_symbol,
                target_qty,
                hedge_price,
            )?;
            if aligned_price <= 0.0 {
                let detail = format!(
                    "aligned_price_invalid mode={} client_order_id={} raw_price={:.8} aligned_price={:.8}",
                    if ctx.is_maker() { "maker" } else { "taker" },
                    ctx.client_order_id,
                    hedge_price,
                    aligned_price
                );
                if ctx.is_maker() {
                    self.push_hedge_residual_with_print(
                        "PRICE_LESS_THAN_ZERO/价格小于0",
                        &detail,
                        target_qty,
                        aligned_price,
                        hedge_side,
                    );
                    return Ok(());
                }
                return Err(format!("对冲价格无效: {:.8}", aligned_price));
            }
            (aligned_qty, aligned_price)
        };

        // 4. 检查最小交易要求
        if !ctx.is_taker() {
            if let Err(e) = MonitorChannel::instance().check_min_trading_requirements(
                hedge_venue,
                &hedge_symbol,
                aligned_qty,
                Some(aligned_price),
            ) {
                debug!(
                    "HedgeArbStrategy: strategy_id={} 对冲订单不满足最小要求: {}，等待更多成交",
                    self.strategy_id, e
                );
                return Ok(());
            }
        }

        // 5. 对冲无需风控逻辑，直接构造订单即可
        // 生成对冲订单ID
        self.order_seq += 1;
        let hedge_order_id = Self::compose_order_id(self.strategy_id, self.order_seq);

        // 获取当前时间戳
        let ts = get_timestamp_us();

        // 根据对冲信号确定订单类型，确定是maker对冲，还是taker对冲
        let order_type = if ctx.is_maker() {
            OrderType::Limit // Maker订单使用限价单
        } else {
            OrderType::Market // Taker订单使用市价单
        };

        // 6. 创建对冲订单并记录到order manager
        let hedge_client_order_id = MonitorChannel::instance()
            .order_manager()
            .borrow_mut()
            .create_order(
                hedge_venue,
                hedge_order_id,
                order_type,
                hedge_symbol.clone(),
                hedge_side,
                aligned_qty,
                aligned_price,
                self.is_force_close_mode(),
                ts,
            );

        log::info!(
            "HedgeArbStrategy: created hedge order with client_order_id: {} for strategy_id: {}",
            hedge_client_order_id,
            self.strategy_id
        );
        // 7. 将对冲订单ID添加到策略的对冲订单列表
        self.hedge_order_ids.push(hedge_order_id);
        // 对冲量只有实质成交才会更新，挂单不更新

        // 8. 推送对冲订单到交易引擎
        if let Err(e) = self.create_and_send_order(hedge_client_order_id, "对冲", &hedge_symbol) {
            return Err(e);
        }

        // 9. 设置对冲挂单的截止时间（仅 maker 单需要）
        if ctx.exp_time > 0 {
            // 设置对冲挂单截止时间，对冲单的截止时间直接在上下文中传入，时间具体来自signal生成时的计算
            // let mut ctx = ArbHedgeCtx::new_maker(
            //     query.strategy_id,
            //     query.client_order_id,
            //     qty,
            //     side.to_u8(),
            //     limit_price,
            //     price_tick,
            //     false,
            //     now + self.hedge_timeout_mm_us,
            // );
            self.hedge_expire_ts = Some(ctx.exp_time);
        } else {
            self.hedge_expire_ts = None;
        }

        debug!(
            "HedgeArbStrategy: strategy_id={} 对冲订单创建成功 order_id={} symbol={} side={:?} qty={:.8} price={:.8} type={:?}",
            self.strategy_id,
            hedge_order_id,
            hedge_symbol,
            hedge_side,
            aligned_qty,
            aligned_price,
            order_type
        );

        Ok(())
    }

    /// 创建订单并发送到交易引擎的公共函数
    ///
    /// # Arguments
    /// * `client_order_id` - 客户端订单ID
    /// * `order_type_str` - 订单类型描述（用于日志）
    /// * `symbol` - 交易对符号
    ///
    /// # Returns
    /// * `Ok(())` - 订单成功发送
    /// * `Err(String)` - 发送失败的错误信息
    fn create_and_send_order(
        &mut self,
        client_order_id: i64,
        order_type_str: &str,
        symbol: &str,
    ) -> Result<(), String> {
        let order = MonitorChannel::instance()
            .order_manager()
            .borrow_mut()
            .get(client_order_id);
        if let Some(order) = order.as_ref() {
            let exchange = order.venue.trade_engine_exchange();
            match order.get_order_request_bytes() {
                Ok(req_bin) => {
                    if let Err(e) = TradeEngHub::publish_order_request(exchange, &req_bin) {
                        error!(
                            "HedgeArbStrategy: strategy_id={} symbol={} exchange={} 推送{}订单失败: {}，标记策略为不活跃",
                            self.strategy_id,
                            symbol,
                            exchange,
                            order_type_str,
                            e
                        );
                        self.alive_flag = false;
                        return Err(format!("推送{}订单失败: {}", order_type_str, e));
                    }
                    self.schedule_order_query_watchdog(client_order_id);
                    Ok(())
                }
                Err(e) => {
                    error!(
                        "HedgeArbStrategy: strategy_id={} symbol={} 获取{}订单请求字节失败: {}，标记策略为不活跃",
                        self.strategy_id,
                        symbol,
                        order_type_str,
                        e
                    );
                    self.alive_flag = false;
                    Err(format!("获取{}订单请求字节失败: {}", order_type_str, e))
                }
            }
        } else {
            error!(
                "HedgeArbStrategy: strategy_id={} symbol={} 未找到创建的{}订单 client_order_id={}",
                self.strategy_id, symbol, order_type_str, client_order_id
            );
            self.alive_flag = false;
            Err(format!("未找到创建的{}订单", order_type_str))
        }
    }

    // cancel的本质就是构造取消，实际处理的是account monitor的撤销回报
    // 修复bug cancel撤销开仓订单，但此时开仓单已经完全成交，所以不能强行cancel，需要先判断开仓单的状态
    // 如果直接cancel，等于发送一个不存在client order id，然后报错
    // 此时trade engine返回挂单错误，导致强行关闭了strategy，等价于报单失败的路径处理，强制关闭了整个对冲配对 strategy
    // 但和报单失败不同，此时对冲单已经挂上。但整个策略已经完全关闭，后续定时器相当于无法监控到这个对冲单，导致没有继续重新挂单
    // 细改
    // cancel时，不能直接对开仓单cancel。要先区分开仓单当前是否处于terminal状态
    // 如果已经完全成交，则不需要cancel，跳过cancel流程即可。
    // 只有非中止状态，挂单中或者部分成交，才需要发送cancel。
    fn handle_arb_cancel_signal(&mut self, _ctx: ArbCancelCtx) -> Result<(), String> {
        // 从 order manager 获取开仓订单
        let order = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(self.open_order_id);
        if let Some(order) = order {
            // 先检查订单状态，如果已经是终结状态，则跳过cancel流程
            if order.status.is_terminal() {
                info!(
                    "HedgeArbStrategy: strategy_id={} 开仓订单已处于终结状态 {:?}，跳过cancel流程 order_id={}",
                    self.strategy_id, order.status, self.open_order_id
                );
                return Ok(());
            }

            // 只有非终结状态（挂单中、部分成交等），才发送cancel请求
            debug!(
                "HedgeArbStrategy: strategy_id={} 开仓订单状态 {:?}，准备发送cancel请求 order_id={}",
                self.strategy_id, order.status, self.open_order_id
            );

            // 使用 order 的 get_order_cancel_bytes 方法获取撤单请求
            match order.get_order_cancel_bytes() {
                Ok(cancel_bytes) => {
                    let exchange = order.venue.trade_engine_exchange();
                    if let Err(e) = TradeEngHub::publish_order_request(exchange, &cancel_bytes) {
                        error!(
                            "HedgeArbStrategy: strategy_id={} exchange={} 发送撤单请求失败: {}",
                            self.strategy_id, exchange, e
                        );
                        return Err(format!("发送撤单请求失败: {}", e));
                    }
                    self.schedule_cancel_query_watchdog(order.client_order_id);
                }
                Err(e) => {
                    error!(
                        "HedgeArbStrategy: strategy_id={} 获取撤单请求字节失败: {}",
                        self.strategy_id, e
                    );
                    return Err(format!("获取撤单请求字节失败: {}", e));
                }
            }
        } else {
            warn!(
                "HedgeArbStrategy: strategy_id={} 未找到要撤销的订单 order_id={}",
                self.strategy_id, self.open_order_id
            );
            return Err(format!("未找到要撤销的订单"));
        }

        Ok(())
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        match &signal.signal_type {
            SignalType::ArbOpen => {
                // 套利开仓信号
                match ArbOpenCtx::from_bytes(signal.context.clone()) {
                    Ok(ctx) => {
                        self.handle_arb_open_signal(ctx);
                    }
                    Err(err) => {
                        error!(
                            "HedgeArbStrategy: strategy_id={} 解析开仓信号上下文失败: {}",
                            self.strategy_id, err
                        );
                    }
                }
            }
            SignalType::ArbCancel => {
                // 套利撤单信号
                match ArbCancelCtx::from_bytes(signal.context.clone()) {
                    Ok(ctx) => {
                        if let Err(err) = self.handle_arb_cancel_signal(ctx) {
                            warn!(
                                "HedgeArbStrategy: strategy_id={} 撤单处理失败: {}",
                                self.strategy_id, err
                            );
                        }
                    }
                    Err(err) => {
                        error!(
                            "HedgeArbStrategy: strategy_id={} 解析撤单信号上下文失败: {}",
                            self.strategy_id, err
                        );
                    }
                }
            }
            SignalType::ArbHedge => {
                // 套利对冲信号
                match ArbHedgeCtx::from_bytes(signal.context.clone()) {
                    Ok(ctx) => {
                        if let Err(err) = self.handle_arb_hedge_signal(ctx) {
                            warn!(
                                "HedgeArbStrategy: strategy_id={} 对冲处理失败: {}",
                                self.strategy_id, err
                            );
                        }
                    }
                    Err(err) => {
                        error!(
                            "HedgeArbStrategy: strategy_id={} 解析对冲信号上下文失败: {}",
                            self.strategy_id, err
                        );
                    }
                }
            }
            _ => {
                debug!(
                    "HedgeArbStrategy: strategy_id={} 忽略信号类型 {:?}",
                    self.strategy_id, signal.signal_type
                );
            }
        }
    }

    // 处理开仓测，超过最长挂单时间
    fn handle_open_leg_timeout(&mut self) {
        // 检查是否设置了超时时间，并且已经超时
        if let Some(expire_ts) = self.open_expire_ts {
            let now = get_timestamp_us();
            if now >= expire_ts && self.alive_flag && self.open_order_id != 0 {
                info!(
                    "HedgeArbStrategy: strategy_id={} 开仓订单超时，直接撤单 order_id={}",
                    self.strategy_id, self.open_order_id
                );

                // 获取开仓订单并直接发送撤单请求
                let order = MonitorChannel::instance()
                    .order_manager()
                    .borrow()
                    .get(self.open_order_id);
                if let Some(order) = order {
                    match order.get_order_cancel_bytes() {
                        Ok(cancel_bytes) => {
                            let exchange = order.venue.trade_engine_exchange();
                            if let Err(e) =
                                TradeEngHub::publish_order_request(exchange, &cancel_bytes)
                            {
                                info!(
                                    "HedgeArbStrategy: strategy_id={} exchange={} 发送开仓撤单请求失败: {}",
                                    self.strategy_id, exchange, e
                                );
                            } else {
                                info!(
                                    "HedgeArbStrategy: strategy_id={} 已发送开仓撤单请求 order_id={}",
                                    self.strategy_id, self.open_order_id
                                );
                                self.schedule_cancel_query_watchdog(order.client_order_id);
                                // 清除超时时间，避免重复发送撤单
                                self.open_expire_ts = None;
                            }
                        }
                        Err(e) => {
                            error!(
                                "HedgeArbStrategy: strategy_id={} 获取开仓撤单请求字节失败: {}",
                                self.strategy_id, e
                            );
                        }
                    }
                }
            }
        }
    }

    fn handle_hedge_leg_timeout(&mut self) {
        // 检查是否设置了对冲超时时间
        // 因此没有处理hedge信号之前，不会检查，会跳过
        if let Some(expire_ts) = self.hedge_expire_ts {
            let now = get_timestamp_us();
            if now >= expire_ts {
                info!(
                    "HedgeArbStrategy: strategy_id={} 对冲订单超时，直接撤单 expire_ts={} now={} hedge_orders_len={}",
                    self.strategy_id,
                    expire_ts,
                    now,
                    self.hedge_order_ids.len()
                );

                // 只撤最后一个对冲订单（任意时刻只有一个有效对冲单）
                if let Some(&hedge_order_id) = self.hedge_order_ids.last() {
                    let order = MonitorChannel::instance()
                        .order_manager()
                        .borrow()
                        .get(hedge_order_id);
                    if let Some(order) = order {
                        info!(
                            "HedgeArbStrategy: strategy_id={} 对冲超时撤单目标 order_id={} symbol={} status={:?} exch_ord_id={:?}",
                            self.strategy_id,
                            hedge_order_id,
                            order.symbol,
                            order.status,
                            order.exchange_order_id
                        );
                        if order.status.is_terminal() {
                            info!(
                                "HedgeArbStrategy: strategy_id={} 对冲订单已终结，跳过撤单 order_id={}",
                                self.strategy_id, hedge_order_id
                            );
                        } else {
                            match order.get_order_cancel_bytes() {
                                Ok(cancel_bytes) => {
                                    let exchange = order.venue.trade_engine_exchange();
                                    if let Err(e) =
                                        TradeEngHub::publish_order_request(exchange, &cancel_bytes)
                                    {
                                        error!(
                                            "HedgeArbStrategy: strategy_id={} exchange={} 发送对冲撤单请求失败 order_id={}: {}",
                                            self.strategy_id, exchange, hedge_order_id, e
                                        );
                                    } else {
                                        info!(
                                            "HedgeArbStrategy: strategy_id={} 已发送对冲撤单请求 order_id={} exchange={} symbol={}",
                                            self.strategy_id, hedge_order_id, exchange, order.symbol
                                        );
                                        self.schedule_cancel_query_watchdog(order.client_order_id);
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "HedgeArbStrategy: strategy_id={} 获取对冲撤单请求字节失败 order_id={}: {}",
                                        self.strategy_id, hedge_order_id, e
                                    );
                                }
                            }
                        }
                    } else {
                        warn!(
                            "HedgeArbStrategy: strategy_id={} 对冲超时撤单但本地订单缺失 order_id={}",
                            self.strategy_id, hedge_order_id
                        );
                    }
                } else {
                    warn!(
                        "HedgeArbStrategy: strategy_id={} 对冲超时撤单但未找到对冲订单ID",
                        self.strategy_id
                    );
                }

                // 清除对冲超时时间，避免重复发送撤单
                self.hedge_expire_ts = None;
            }
        }
    }

    // 当前策略只允许一个活跃对冲单，检查最后一个对冲单是否未终结
    fn has_pending_hedge_order(&self) -> bool {
        if let Some(&last_hedge_id) = self.hedge_order_ids.last() {
            let order = MonitorChannel::instance()
                .order_manager()
                .borrow()
                .get(last_hedge_id);
            if let Some(order) = order {
                return !order.status.is_terminal();
            }
        }
        false
    }

    /// 以市价单对冲
    ///
    /// # Arguments
    /// * `venue` - 对冲交易场所
    /// * `side` - 对冲方向
    /// * `eff_qty` - 有效对冲量（累计成交量 - 累计对冲量）
    fn hedge_as_market_order(&mut self, side: Side, eff_qty: f64) -> Result<(), String> {
        if eff_qty <= 1e-8 {
            debug!(
                "HedgeArbStrategy: strategy_id={} 对冲量过小 eff_qty={:.8}，跳过",
                self.strategy_id, eff_qty
            );
            return Ok(());
        }

        // 1. 创建对冲上下文（市价单模式，exp_time = 0）
        let mut hedge_ctx =
            ArbHedgeCtx::new_taker(self.strategy_id, self.open_order_id, eff_qty, side.to_u8());

        // 2. 设置开仓侧信息（as market对冲，盘口写0）
        hedge_ctx.opening_leg = crate::signal::common::TradingLeg {
            venue: self.open_venue.to_u8(),
            bid0: 0.0,
            ask0: 0.0,
            ts: 0,
        };
        hedge_ctx.set_opening_symbol(&self.open_symbol);

        // 3. 设置对冲symbol
        hedge_ctx.set_hedging_symbol(&self.hedge_symbol);

        // 4. 设置对冲leg（市价对冲，盘口写0）
        hedge_ctx.hedging_leg = crate::signal::common::TradingLeg {
            venue: self.hedge_venue.to_u8(),
            bid0: 0.0,
            ask0: 0.0,
            ts: 0,
        };
        self.hedge_bbo.hedge = Bbo::default();
        self.hedge_bbo.open = Bbo::default();

        // 5. 设置市场数据时间戳
        hedge_ctx.market_ts = get_timestamp_us();

        // 6. MT 对冲是市价单，price_offset 设置为 0
        hedge_ctx.price_offset = 0.0;
        let from_key = if !self.hedge_from_key.is_empty() {
            self.hedge_from_key.clone()
        } else {
            self.open_from_key.clone()
        };
        hedge_ctx.set_from_key(from_key.into_bytes());

        // 7. 直接调用对冲处理逻辑（不再通过队列循环）
        debug!(
            "HedgeArbStrategy: strategy_id={} 直接处理对冲 hedge venue={:?} side={:?} qty={:.8}",
            self.strategy_id, self.hedge_venue, side, eff_qty
        );

        self.handle_arb_hedge_signal(hedge_ctx)?;

        Ok(())
    }

    fn schedule_order_query_watchdog(&mut self, client_order_id: i64) {
        let due = get_timestamp_us().saturating_add(ORDER_QUERY_WATCHDOG_DELAY_US);
        self.order_query_watchdog = Some(QueryWatchdog {
            client_order_id,
            due_ts_us: due,
            reason: PendingOrderQueryReason::OrderWatchdog,
        });
    }

    fn schedule_cancel_query_watchdog(&mut self, client_order_id: i64) {
        self.schedule_cancel_query_watchdog_with_reason(
            client_order_id,
            PendingOrderQueryReason::CancelWatchdog,
        );
    }

    fn schedule_cancel_query_watchdog_with_reason(
        &mut self,
        client_order_id: i64,
        reason: PendingOrderQueryReason,
    ) {
        let due = get_timestamp_us().saturating_add(CANCEL_QUERY_WATCHDOG_DELAY_US);
        self.cancel_query_watchdog = Some(QueryWatchdog {
            client_order_id,
            due_ts_us: due,
            reason,
        });
        if self
            .order_query_watchdog
            .is_some_and(|w| w.client_order_id == client_order_id)
        {
            self.order_query_watchdog = None;
        }
    }

    fn clear_query_watchdogs(&mut self, client_order_id: i64) {
        if self
            .order_query_watchdog
            .is_some_and(|w| w.client_order_id == client_order_id)
        {
            self.order_query_watchdog = None;
        }
        if self
            .cancel_query_watchdog
            .is_some_and(|w| w.client_order_id == client_order_id)
        {
            self.cancel_query_watchdog = None;
        }
    }

    fn send_order_query(&mut self, client_order_id: i64, reason: PendingOrderQueryReason) {
        if let Some(existing) = self.pending_order_queries.get_mut(&client_order_id) {
            // Upgrade in-flight query semantics: if we already learned cancel is non-actionable,
            // ensure the eventual query response won't trigger re-cancel logic.
            if reason == PendingOrderQueryReason::CancelRejected
                && *existing != PendingOrderQueryReason::CancelRejected
            {
                *existing = PendingOrderQueryReason::CancelRejected;
            }
            return;
        }

        let order_mgr = MonitorChannel::instance().order_manager();
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "HedgeArbStrategy: strategy_id={} send_order_query but local order missing: client_order_id={} reason={:?}",
                self.strategy_id, client_order_id, reason
            );
            return;
        };

        match self.build_order_query_request(&order, client_order_id) {
            Ok((exchange, req_bytes)) => {
                if let Err(err) = crate::pre_trade::QueryEngHub::publish_query_request(
                    exchange.as_str(),
                    &req_bytes,
                ) {
                    warn!(
                        "HedgeArbStrategy: strategy_id={} publish order query failed: exchange={} client_order_id={} reason={:?} err={:#}",
                        self.strategy_id, exchange, client_order_id, reason, err
                    );
                    return;
                }
                self.pending_order_queries.insert(client_order_id, reason);
                debug!(
                    "HedgeArbStrategy: strategy_id={} order query sent: exchange={} client_order_id={} reason={:?}",
                    self.strategy_id, exchange, client_order_id, reason
                );
            }
            Err(err) => {
                warn!(
                    "HedgeArbStrategy: strategy_id={} build order query failed: client_order_id={} reason={:?} err={}",
                    self.strategy_id, client_order_id, reason, err
                );
            }
        }
    }

    fn handle_query_watchdogs(&mut self) {
        let now = get_timestamp_us();

        if let Some(w) = self.cancel_query_watchdog {
            if now >= w.due_ts_us {
                self.cancel_query_watchdog = None;
                let order_mgr = MonitorChannel::instance().order_manager();
                let order_opt = order_mgr.borrow().get(w.client_order_id);
                if let Some(order) = order_opt.as_ref().filter(|o| !o.status.is_terminal()) {
                    let leg = self.classify_leg(w.client_order_id);
                    let scheduled_at = w.due_ts_us.saturating_sub(CANCEL_QUERY_WATCHDOG_DELAY_US);
                    let waited_ms = now.saturating_sub(scheduled_at).saturating_div(1_000);
                    let hint = if w.reason == PendingOrderQueryReason::CancelRejected {
                        "CancelRejectedWatchdog触发"
                    } else {
                        "CancelWatchdog触发"
                    };
                    info!(
                        "{}: strategy_id={} client_order_id={} leg={:?} symbol={} status={:?} exch_ord_id={:?} 等待{}ms仍未收到撤单/终态回报，发送order query回补 reason={:?}",
                        hint,
                        self.strategy_id,
                        w.client_order_id,
                        leg,
                        order.symbol,
                        order.status,
                        order.exchange_order_id,
                        waited_ms,
                        w.reason
                    );
                    self.send_order_query(w.client_order_id, w.reason);
                }
            }
        }

        if let Some(w) = self.order_query_watchdog {
            if now >= w.due_ts_us {
                self.order_query_watchdog = None;
                let order_mgr = MonitorChannel::instance().order_manager();
                let order_opt = order_mgr.borrow().get(w.client_order_id);
                if let Some(order) = order_opt.as_ref().filter(|o| !o.status.is_terminal()) {
                    let leg = self.classify_leg(w.client_order_id);
                    let scheduled_at = w.due_ts_us.saturating_sub(ORDER_QUERY_WATCHDOG_DELAY_US);
                    let waited_ms = now.saturating_sub(scheduled_at).saturating_div(1_000);
                    let since_submit_ms = now
                        .saturating_sub(order.timestamp.submit_t)
                        .saturating_div(1_000);
                    let hint = if order.status == OrderExecutionStatus::Commit {
                        "（下单后未收到New/成交推送）"
                    } else {
                        ""
                    };
                    info!(
                        "OrderWatchdog触发{}: strategy_id={} client_order_id={} leg={:?} symbol={} status={:?} exch_ord_id={:?} 等待{}ms仍未收到回报，发送order query回补 (since_submit={}ms)",
                        hint,
                        self.strategy_id,
                        w.client_order_id,
                        leg,
                        order.symbol,
                        order.status,
                        order.exchange_order_id,
                        waited_ms,
                        since_submit_ms
                    );
                    self.send_order_query(w.client_order_id, w.reason);
                }
            }
        }
    }

    fn handle_hedge_retry_cooldown(&mut self) {
        let Some(due_ts) = self.hedge_retry_after_ts else {
            return;
        };
        let now = get_timestamp_us();
        if now < due_ts {
            return;
        }

        self.hedge_retry_after_ts = None;
        let reason = self.hedge_retry_reason.take().unwrap_or("cooldown");
        let base_pending_qty = self.cumulative_open_qty - self.cumulative_hedged_qty;
        if base_pending_qty <= 1e-8 {
            debug!(
                "HedgeArbStrategy: strategy_id={} hedge retry skipped after cooldown (no pending qty) reason={}",
                self.strategy_id, reason
            );
            return;
        }

        let (sent, hedged_qty, _) = self.try_hedge_with_residual(base_pending_qty);
        if sent {
            info!(
                "HedgeArbStrategy: strategy_id={} hedge retry after cooldown sent qty={:.8} reason={}",
                self.strategy_id, hedged_qty, reason
            );
        } else {
            warn!(
                "HedgeArbStrategy: strategy_id={} hedge retry after cooldown skipped/failed pending_qty={:.8} reason={}",
                self.strategy_id, base_pending_qty, reason
            );
        }
    }

    /// 以限价单对冲（需要先向上游模型查询挂单价格）
    ///
    /// # Arguments
    /// * `venue` - 对冲交易场所
    /// * `side` - 对冲方向
    /// * `eff_qty` - 有效对冲量（累计成交量 - 累计对冲量）
    fn hedge_as_limit_order(&mut self, side: Side, eff_qty: f64) -> Result<(), String> {
        if eff_qty <= 1e-8 {
            debug!(
                "HedgeArbStrategy: strategy_id={} 限价单对冲量过小 eff_qty={:.8}，跳过",
                self.strategy_id, eff_qty
            );
            return Ok(());
        }

        // 1. 创建对冲查询消息（携带开仓盘口快照，便于上游风控/止损判断）
        info!(
            "HedgeArbStrategy: strategy_id={} 第{}次对冲 hedge_symbol={} hedge_venue={:?} side={:?} qty={:.8}",
            self.strategy_id,
            self.hedge_request_seq + 1,
            self.hedge_symbol,
            self.hedge_venue,
            side,
            eff_qty
        );
        let query_msg = crate::signal::hedge_signal::ArbHedgeSignalQueryMsg::new(
            self.strategy_id,
            self.open_order_id,
            get_timestamp_us(),
            eff_qty,
            side.to_u8(),
            self.open_venue.to_u8(),
            &self.open_symbol,
            self.hedge_venue.to_u8(),
            &self.hedge_symbol,
            self.hedge_request_seq,
            self.open_signal_ts,
            self.open_bbo.open.bid0,
            self.open_bbo.open.ask0,
            self.open_bbo.open.ts,
            self.open_bbo.hedge.bid0,
            self.open_bbo.hedge.ask0,
            self.open_bbo.hedge.ts,
        );

        self.hedge_request_seq = self.hedge_request_seq.wrapping_add(1);

        // 2. 通过 SignalChannel 直接发送到上游
        let send_result = SignalChannel::with(|ch| ch.publish_backward(&query_msg.to_bytes()));

        match send_result {
            Ok(true) => {
                debug!(
                    "HedgeArbStrategy: strategy_id={} 发送对冲查询成功 hedge_venue={:?} side={:?} qty={:.8}",
                    self.strategy_id, self.hedge_venue, side, eff_qty
                );
            }
            Ok(false) => {
                warn!(
                    "HedgeArbStrategy: strategy_id={} backward publisher 未配置，无法发送对冲查询",
                    self.strategy_id
                );
                return Err("backward publisher 未配置".to_string());
            }
            Err(e) => {
                error!(
                    "HedgeArbStrategy: strategy_id={} 发送对冲查询失败: {}",
                    self.strategy_id, e
                );
                return Err(format!("发送对冲查询失败: {}", e));
            }
        }

        Ok(())
    }

    /// 尝试对冲（含残值处理）
    ///
    /// # Arguments
    /// * `base_qty` - 基础待对冲量（不含残值）
    ///
    /// # Returns
    /// (是否成功发起对冲, 实际对冲量, 从残值表取出的数量)
    /// - 成功发起对冲时：(true, 对冲数量, 从残值表增加的数量)
    /// - 不满足要求时：(false, 0.0, 累加到残值表的数量)
    fn try_hedge_with_residual(&mut self, base_qty: f64) -> (bool, f64, f64) {
        // 1. 计算总待对冲量（基础量 + 残值表中的残余量）
        let mut total_pending_qty = base_qty;
        let residual_qty =
            MonitorChannel::instance().clear_hedge_residual(&self.hedge_symbol, self.hedge_venue);
        total_pending_qty += residual_qty;

        debug!(
            "HedgeArbStrategy: strategy_id={} 待对冲量: 基础={:.8} 残余={:.8} 总计={:.8}",
            self.strategy_id, base_qty, residual_qty, total_pending_qty
        );

        // 2. 检查是否满足最小交易要求
        let mut min_req_reason: Option<String> = None;
        let can_hedge = match MonitorChannel::instance().check_min_trading_requirements(
            self.hedge_venue,
            &self.hedge_symbol,
            total_pending_qty,
            None,
        ) {
            Ok(_) => true,
            Err(e) => {
                min_req_reason = Some(e);
                false
            }
        };

        if !can_hedge {
            // 不满足最小交易要求，累加到残值表
            if total_pending_qty > 1e-12 {
                MonitorChannel::instance().inc_hedge_residual(
                    self.hedge_symbol.clone(),
                    self.hedge_venue,
                    total_pending_qty,
                );
                let reason = min_req_reason.as_deref().unwrap_or("unknown");
                info!(
                    "HedgeArbStrategy: strategy_id={} 待对冲量={:.8} 不满足最小交易要求，累加到残余量表 reason={}",
                    self.strategy_id, total_pending_qty, reason
                );
                return (false, 0.0, total_pending_qty);
            }
            return (false, 0.0, 0.0);
        }

        // 3. 满足要求，发起对冲
        debug!(
            "HedgeArbStrategy: strategy_id={} 待对冲量={:.8} 满足最小交易要求，准备对冲",
            self.strategy_id, total_pending_qty
        );

        let hedge_result: Result<(), String> = if self.hedge_timeout_us.is_some() {
            // MM 模式：使用限价单对冲
            debug!(
                "HedgeArbStrategy: strategy_id={} MM模式，使用限价单对冲",
                self.strategy_id
            );
            self.hedge_as_limit_order(self.hedge_side, total_pending_qty)
        } else {
            // MT 模式：使用市价单对冲
            debug!(
                "HedgeArbStrategy: strategy_id={} MT模式，使用市价单对冲",
                self.strategy_id
            );
            self.hedge_as_market_order(self.hedge_side, total_pending_qty)
        };

        match hedge_result {
            Ok(_) => {
                // 对冲成功
                debug!(
                    "HedgeArbStrategy: strategy_id={} 对冲信号已发送，数量={:.8}",
                    self.strategy_id, total_pending_qty
                );
                (true, total_pending_qty, residual_qty)
            }
            Err(e) => {
                error!(
                    "HedgeArbStrategy: strategy_id={} 对冲失败: {}",
                    self.strategy_id, e
                );
                // 对冲失败，将数量累加回残值表
                if total_pending_qty > 1e-12 {
                    MonitorChannel::instance().inc_hedge_residual(
                        self.hedge_symbol.clone(),
                        self.hedge_venue,
                        total_pending_qty,
                    );
                    (false, 0.0, total_pending_qty)
                } else {
                    (false, 0.0, 0.0)
                }
            }
        }
    }

    fn process_open_leg_cancel(&mut self, cancel_update: &dyn OrderUpdate) {
        self.open_expire_ts = None;
        self.processed_cancel_updates
            .insert(cancel_update.client_order_id());
        // 1. 确认 open-order 的订单状态，修改为 cancel。更新交易所 end 时间和本地 end 时间等数据
        let order_id = cancel_update.client_order_id();
        let event_time = cancel_update.event_time();

        let order_mgr = MonitorChannel::instance().order_manager();
        let mut order_manager = order_mgr.borrow_mut();
        let updated = order_manager.update(order_id, |order| {
            order.status = OrderExecutionStatus::Cancelled;
            order.timestamp.end_t = event_time;
        });
        drop(order_manager);
        if !updated {
            warn!(
                "HedgeArbStrategy: strategy_id={} 未找到开仓订单 order_id={}",
                self.strategy_id, order_id
            );
            return;
        }
        // 2. 计算待对冲量 = 已经成交的量 - 已经对冲的量
        let base_pending_qty = self.cumulative_open_qty - self.cumulative_hedged_qty;
        // 3. 尝试对冲（含残值处理）
        let (can_hedge, hedged_qty, residual_qty) = self.try_hedge_with_residual(base_pending_qty);

        if can_hedge {
            info!(
                "HedgeArbStrategy: strategy_id={} 开仓撤单后对冲成功，对冲量={:.8}",
                self.strategy_id, hedged_qty
            );
        } else if residual_qty > 1e-12 {
            info!(
                "HedgeArbStrategy: strategy_id={} 开仓撤单后无法对冲，残值={:.8}",
                self.strategy_id, residual_qty
            );
        }
        // 4. 剩余量已经无法开单
        if !can_hedge {
            if self.hedge_timeout_us.is_some() {
                self.alive_flag = false;
            } else {
                let has_pending_hedge = self.has_pending_hedge_order();
                if !has_pending_hedge {
                    self.alive_flag = false;
                }
            }
        }
    }

    fn process_hedge_leg_cancel(&mut self, cancel_update: &dyn OrderUpdate) {
        self.hedge_expire_ts = None;
        self.processed_cancel_updates
            .insert(cancel_update.client_order_id());
        // 只有 MM 模式才会出现对冲侧的 cancel（因为 MT 模式是市价单，立即成交）
        if self.hedge_timeout_us.is_none() {
            error!(
                "HedgeArbStrategy: strategy_id={} MT模式不应该有对冲侧撤单，订单ID={}",
                self.strategy_id,
                cancel_update.client_order_id()
            );
            return;
        }
        // 1. 更新订单状态为 Cancelled
        let order_id = cancel_update.client_order_id();
        let event_time = cancel_update.event_time();

        let order_mgr = MonitorChannel::instance().order_manager();
        let mut order_manager = order_mgr.borrow_mut();
        let updated = order_manager.update(order_id, |order| {
            order.status = OrderExecutionStatus::Cancelled;
            order.set_end_time(event_time);
            order.cumulative_filled_quantity = cancel_update.cumulative_filled_quantity();
        });
        drop(order_manager);

        if !updated {
            warn!(
                "HedgeArbStrategy: strategy_id={} 未找到对冲订单 order_id={}",
                self.strategy_id, order_id
            );
            return;
        }

        // 2. 计算待对冲量 = 已经成交的量 - 已经对冲的量
        let base_pending_qty = self.cumulative_open_qty - self.cumulative_hedged_qty;

        // 3、 尝试对冲（含残值处理）
        let (can_hedge, hedged_qty, residual_qty) = self.try_hedge_with_residual(base_pending_qty);

        if can_hedge {
            info!(
                "HedgeArbStrategy: strategy_id={} 对冲撤单后重新对冲成功，对冲量={:.8}",
                self.strategy_id, hedged_qty
            );
        } else if residual_qty > 1e-12 {
            info!(
                "HedgeArbStrategy: strategy_id={} 对冲撤单后无法对冲，残值={:.8}",
                self.strategy_id, residual_qty
            );
        }

        // 4. MM模式下，如果无法对冲且没有待处理的对冲订单，关闭策略
        if !can_hedge && !self.has_pending_hedge_order() {
            self.alive_flag = false;
        }
    }

    fn process_open_leg_trade(&mut self, trade: &dyn TradeUpdate) {
        if trade.order_status() == Some(OrderStatus::Filled) {
            self.open_expire_ts = None;
        }
        // 开仓成交后，触发对冲逻辑
        // MM 模式：只有完全成交才对冲, 部分成交只扣除量
        if self.hedge_timeout_us.is_some() {
            match trade.order_status() {
                Some(OrderStatus::Filled) => {
                    if self.open_filled_hedge_triggered {
                        debug!(
                            "HedgeArbStrategy: strategy_id={} 开仓成交已触发对冲，忽略重复成交回报 order_id={}",
                            self.strategy_id,
                            trade.client_order_id()
                        );
                        return;
                    }
                    // 计算待对冲量 = 已经成交的量 - 已经对冲的量
                    let base_pending_qty: f64 =
                        self.cumulative_open_qty - self.cumulative_hedged_qty;
                    // 尝试对冲（含残值处理）
                    let (can_hedge, _hedged_qty, residual_qty) =
                        self.try_hedge_with_residual(base_pending_qty);
                    if can_hedge {
                        self.open_filled_hedge_triggered = true;
                        //开仓量修改
                        self.cumulative_open_qty += residual_qty;
                        //MM 模式，挂单不一定成交。所以等实际成交再更新对冲量
                    }
                    if !can_hedge && !self.has_pending_hedge_order() {
                        self.alive_flag = false;
                    }
                }
                Some(OrderStatus::PartiallyFilled) => {
                    // 开仓侧部分成交，只是更新cumulative_open_qty。这一部分在外部已经做过了
                }
                _ => {
                    error!(
                        "unexpected MM model trade event: {} {}",
                        trade.client_order_id(),
                        trade.trade_id()
                    );
                }
            }
        }
        // MT 模式：部分成交和完全成交都对冲
        if self.hedge_timeout_us == None {
            if trade.order_status() == Some(OrderStatus::Filled)
                || trade.order_status() == Some(OrderStatus::PartiallyFilled)
            {
                let base_pending_qty = self.cumulative_open_qty - self.cumulative_hedged_qty;
                // 尝试对冲（含残值处理）
                let (can_hedge, hedged_qty, residual_qty) =
                    self.try_hedge_with_residual(base_pending_qty);
                if can_hedge {
                    self.cumulative_open_qty += residual_qty;
                    self.cumulative_hedged_qty += hedged_qty;
                } else if residual_qty > 1e-12 {
                    info!(
                        "HedgeArbStrategy: strategy_id={} MT模式开仓成交但无法对冲，残值={:.8}",
                        self.strategy_id, residual_qty
                    );
                }
                // MT 模式下，如果无法对冲且没有待处理的对冲订单，关闭策略
                if !can_hedge && !self.has_pending_hedge_order() {
                    self.alive_flag = false;
                }
            }
        }
    }

    fn process_hedge_leg_trade(&mut self, trade: &dyn TradeUpdate) {
        // 对冲侧成交处理
        info!(
            "HedgeArbStrategy: strategy_id={} 对冲成交: 开仓量={:.8} 对冲量={:.8}",
            self.strategy_id, self.cumulative_open_qty, self.cumulative_hedged_qty
        );

        // MT 模式：检查是否还有未对冲的量，如果有且无法对冲则关闭策略
        if self.hedge_timeout_us.is_none() {
            // 计算剩余未对冲量
            let remaining_qty = self.cumulative_open_qty - self.cumulative_hedged_qty;

            // 获取残值
            let residual_qty =
                MonitorChannel::instance().get_hedge_residual(&self.hedge_symbol, self.hedge_venue);
            let total_remaining = remaining_qty + residual_qty;

            debug!(
                "HedgeArbStrategy: strategy_id={} MT模式对冲成交后检查: 剩余={:.8} 残值={:.8} 总计={:.8}",
                self.strategy_id, remaining_qty, residual_qty, total_remaining
            );

            // 检查剩余量是否满足最小交易要求
            if total_remaining > 1e-12 {
                let can_hedge = MonitorChannel::instance()
                    .check_min_trading_requirements(
                        self.hedge_venue,
                        &self.hedge_symbol,
                        total_remaining,
                        None,
                    )
                    .is_ok();

                if !can_hedge {
                    // 剩余量不足以对冲，且没有待处理的对冲订单，关闭策略
                    if !self.has_pending_hedge_order() {
                        info!(
                            "HedgeArbStrategy: strategy_id={} MT模式，剩余量={:.8} 不足以对冲，关闭策略",
                            self.strategy_id, total_remaining
                        );
                        self.alive_flag = false;
                    }
                }
            }
        } else {
            // MM 模式：对冲侧成交需要区分成交状态
            if trade.order_status() == Some(OrderStatus::Filled) {
                // 完全成交即表示本次对冲已完成，直接关闭策略
                info!(
                    "HedgeArbStrategy: strategy_id={} MM模式对冲已全部成交，结束策略",
                    self.strategy_id
                );
                self.hedge_expire_ts = None;
                self.alive_flag = false;
            } else {
                // 非完全成交的数量已在上游累计，这里保持策略存活，等待定时器重新挂单
                debug!(
                    "HedgeArbStrategy: strategy_id={} MM模式对冲部分成交，等待后续重报",
                    self.strategy_id
                );
            }
        }
    }

    // 处理交易更新
    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) {
        let client_order_id = trade.client_order_id();
        self.clear_query_watchdogs(client_order_id);

        let cumulative_qty = trade.cumulative_filled_quantity();
        let cumulative_base_qty = MonitorChannel::instance().qty_to_base(
            trade.trading_venue(),
            trade.symbol(),
            cumulative_qty,
        );
        let trade_time = trade.trade_time();
        let event_time = trade.event_time();
        if let Some(OrderStatus::Filled) = trade.order_status() {
            let order_mgr = MonitorChannel::instance().order_manager();
            let mut order_manager = order_mgr.borrow_mut();
            let updated = order_manager.update(client_order_id, |order| {
                order.cumulative_filled_quantity = cumulative_qty;
                order.set_filled_time(trade_time);
                order.status = OrderExecutionStatus::Filled;
                order.set_end_time(event_time);
            });
            if !updated {
                warn!(
                    "HedgeArbStrategy: strategy_id={} 未找到成交对应的订单 client_order_id={}",
                    self.strategy_id, client_order_id
                );
            }
        }

        //1 根据client order id，判断是开仓成交，还是对冲成交, 更新开仓量或对冲量
        if client_order_id == self.open_order_id {
            // 开仓成交，更新累计开仓量, 打印成交量
            self.cumulative_open_qty = cumulative_base_qty;
            info!(
                "💰 开仓成交: strategy_id={} order_id={} symbol={} price={:.6} cumulative={:.4} | 已对冲={:.4}",
                self.strategy_id, client_order_id, self.open_symbol,
                trade.price(),
                self.cumulative_open_qty,
                self.cumulative_hedged_qty
            );
            self.process_open_leg_trade(trade);
        } else if self.hedge_order_ids.contains(&client_order_id) {
            // 对冲侧成交，增加累计对冲量
            self.cumulative_hedged_qty = cumulative_base_qty;
            info!(
                "🛡️ 对冲成交: strategy_id={} order_id={} symbol={} price={:.6} | 开仓量={:.4} 已对冲={:.4}",
                self.strategy_id, client_order_id, self.hedge_symbol,
                trade.price(),
                self.cumulative_open_qty,
                self.cumulative_hedged_qty
            );
            self.process_hedge_leg_trade(trade);
        } else {
            // 非法成交，忽略
            warn!(
                "⚠️ 收到未知订单成交: strategy_id={} order_id={}",
                self.strategy_id, client_order_id
            );
        }
    }

    fn apply_order_update(&mut self, order_update: &dyn OrderUpdate) {
        //状态更新是通用部分，非成交只更新状态，不更新量
        let client_order_id = order_update.client_order_id();
        self.clear_query_watchdogs(client_order_id);

        let order_mgr = MonitorChannel::instance().order_manager();
        let mut order_manager = order_mgr.borrow_mut();
        let updated = order_manager.update(client_order_id, |order| match order_update.status() {
            OrderStatus::New => {
                order.status = OrderExecutionStatus::Create;
                order.set_exchange_order_id(order_update.order_id());
                order.set_create_time(order_update.event_time());
                info!(
                    "✅ 订单已挂单: strategy_id={} client_order_id={} exchange_order_id={} symbol={} side={:?} price={:.6} qty={:.4}",
                    self.strategy_id, client_order_id, order_update.order_id(),
                    order.symbol, order.side, order.price, order.quantity
                );
            }
            OrderStatus::Canceled => {
                order.status = OrderExecutionStatus::Cancelled;
                order.set_end_time(order_update.event_time());
                info!(
                    "🚫 订单已撤销: strategy_id={} client_order_id={} exchange_order_id={} symbol={} filled={:.4}/{:.4}",
                    self.strategy_id, client_order_id, order_update.order_id(),
                    order.symbol, order.cumulative_filled_quantity, order.quantity
                );
            }
            OrderStatus::Expired => {
                order.status = OrderExecutionStatus::Rejected;
                order.set_end_time(order_update.event_time());
                warn!(
                    "⏰ 订单已过期: strategy_id={} client_order_id={} exchange_order_id={} symbol={}",
                    self.strategy_id, client_order_id, order_update.order_id(), order.symbol
                );
            }
            OrderStatus::ExpiredInMatch => {
                order.status = OrderExecutionStatus::Rejected;
                order.set_end_time(order_update.event_time());
                warn!(
                    "⏰ 订单匹配中过期: strategy_id={} client_order_id={} exchange_order_id={} symbol={}",
                    self.strategy_id, client_order_id, order_update.order_id(), order.symbol
                );
            }
            _ => {
                panic!(
                    "unexpected order status received {} {} {:?}",
                    order_update.client_order_id(),
                    order_update.order_id(),
                    order_update.status()
                );
            }
        });
        drop(order_manager);

        if !updated {
            error!(
                "update failed {} {} {:?}",
                order_update.client_order_id(),
                order_update.order_id(),
                order_update.status()
            );
            return;
        }
        if order_update.status() == OrderStatus::Canceled {
            if self
                .processed_cancel_updates
                .contains(&order_update.client_order_id())
            {
                return;
            }
            if order_update.client_order_id() == self.open_order_id {
                self.process_open_leg_cancel(order_update);
            }
            if self.hedge_timeout_us.is_some()
                && self
                    .hedge_order_ids
                    .contains(&order_update.client_order_id())
            {
                self.process_hedge_leg_cancel(order_update);
            }
        }
    }

    fn cleanup_strategy_orders(&mut self) {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };
        let mut mgr = order_mgr.borrow_mut();

        // 检查并清理开仓订单
        if self.open_order_id != 0 {
            if let Some(order) = mgr.get(self.open_order_id) {
                if !order.status.is_terminal() {
                    mgr.log_order_details(&order, "开仓订单未达到终结状态被清理", self.strategy_id);
                }
            }
            let _ = mgr.remove(self.open_order_id);
        }

        // 检查并清理对冲订单
        for id in &self.hedge_order_ids {
            if let Some(order) = mgr.get(*id) {
                if !order.status.is_terminal() {
                    mgr.log_order_details(&order, "对冲订单未达到终结状态被清理", self.strategy_id);
                }
            }
            let _ = mgr.remove(*id);
        }
    }

    fn classify_leg(&self, client_order_id: i64) -> Option<Leg> {
        if client_order_id == self.open_order_id {
            Some(Leg::Open)
        } else if self.hedge_order_ids.contains(&client_order_id) {
            Some(Leg::Hedge)
        } else {
            None
        }
    }

    fn build_order_query_request(
        &self,
        order: &crate::pre_trade::order_manager::Order,
        client_query_id: i64,
    ) -> Result<(String, bytes::Bytes), String> {
        let exchange = order.venue.trade_engine_exchange().to_string();
        let exchange_order_id = order.exchange_order_id.filter(|&id| id > 0);

        let req_type = match order.venue {
            TradingVenue::BinanceMargin => QueryRequestType::BinanceMarginQuery,
            TradingVenue::BinanceFutures => {
                if MonitorChannel::instance()
                    .order_manager()
                    .borrow()
                    .binance_is_standard()
                {
                    QueryRequestType::BinanceWsUMQuery
                } else {
                    QueryRequestType::BinanceUMQuery
                }
            }
            TradingVenue::OkexMargin => QueryRequestType::OkexMarginQuery,
            TradingVenue::OkexFutures => QueryRequestType::OkexUMQuery,
            TradingVenue::GateMargin => QueryRequestType::GateUnifiedOrderQuery,
            TradingVenue::GateFutures => QueryRequestType::GateFuturesOrderQuery,
            _ => return Err(format!("unsupported venue for query: {:?}", order.venue)),
        };

        let params = match order.venue {
            TradingVenue::BinanceMargin | TradingVenue::BinanceFutures => {
                if let Some(order_id) = exchange_order_id {
                    bytes::Bytes::from(format!("symbol={}&orderId={}", order.symbol, order_id))
                } else {
                    bytes::Bytes::from(format!(
                        "symbol={}&origClientOrderId={}",
                        order.symbol, client_query_id
                    ))
                }
            }
            TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
                let inst_id = crate::pre_trade::order_manager::okex_inst_id_from_symbol(
                    &order.symbol,
                    order.venue,
                )?;
                if let Some(order_id) = exchange_order_id {
                    bytes::Bytes::from(format!("instId={}&ordId={}", inst_id, order_id))
                } else {
                    bytes::Bytes::from(format!("instId={}&clOrdId={}", inst_id, client_query_id))
                }
            }
            TradingVenue::GateMargin => {
                let currency_pair =
                    crate::pre_trade::order_manager::gate_currency_pair_from_symbol(&order.symbol);
                let Some(order_id) = exchange_order_id else {
                    return Err(format!(
                        "gate order query requires exchange_order_id: client_order_id={} venue={:?}",
                        client_query_id, order.venue
                    ));
                };
                let req_param = serde_json::json!({
                    "order_id": order_id.to_string(),
                    "currency_pair": currency_pair,
                    "account": "cross_margin",
                });
                bytes::Bytes::from(req_param.to_string())
            }
            TradingVenue::GateFutures => {
                let Some(order_id) = exchange_order_id else {
                    return Err(format!(
                        "gate order query requires exchange_order_id: client_order_id={} venue={:?}",
                        client_query_id, order.venue
                    ));
                };
                let req_param = serde_json::json!({
                    "order_id": order_id.to_string(),
                });
                bytes::Bytes::from(req_param.to_string())
            }
            _ => bytes::Bytes::new(),
        };

        let now = get_timestamp_us();
        let req = GenericQueryRequest::create(req_type, now, client_query_id, params);
        Ok((exchange, req.to_bytes()))
    }

    fn send_order_query_for_cancel_failed(
        &mut self,
        client_order_id: i64,
        reason: PendingOrderQueryReason,
    ) {
        if matches!(
            reason,
            PendingOrderQueryReason::OpenLegCancelFailed
                | PendingOrderQueryReason::HedgeLegCancelFailed
                | PendingOrderQueryReason::CancelRejected
        ) {
            warn!(
                "HedgeArbStrategy: strategy_id={} cancel_failed, will query order status: client_order_id={} reason={:?}",
                self.strategy_id, client_order_id, reason
            );
        }
        self.send_order_query(client_order_id, reason);
    }

    fn parse_compact_order_query_resp(body: &bytes::Bytes) -> Option<CompactOrderQueryResp> {
        if body.len() < COMPACT_ORDER_QUERY_RESP_LEN {
            return None;
        }
        let parsed = CompactOrderQueryResp::from_bytes_prefix(body.as_ref()).ok()?;
        if !parsed.executed_qty.is_finite() || parsed.executed_qty < 0.0 {
            return None;
        }
        if parsed.order_id <= 0 {
            return None;
        }
        if crate::pre_trade::order_manager::OrderExecutionStatus::from_u8(parsed.status_u8)
            .is_none()
        {
            return None;
        }
        if TimeInForce::from_u8(parsed.time_in_force_u8).is_none() {
            return None;
        }
        if parsed.trade_id < 0 {
            return None;
        }
        if parsed.update_time_ms < 0 {
            return None;
        }
        if parsed.update_time_ms != 0 {
            let now_ms = get_timestamp_us().saturating_div(1_000);
            if parsed.update_time_ms < 1_300_000_000_000 {
                return None;
            }
            if parsed.update_time_ms > now_ms.saturating_add(86_400_000) {
                return None;
            }
        }
        Some(parsed)
    }

    //通过 order query 的结果，补充traade/order update，来推进strategy逻辑
    fn apply_parsed_order_query_updates(
        &mut self,
        order: &crate::pre_trade::order_manager::Order,
        parsed: CompactOrderQueryResp,
        reason: PendingOrderQueryReason,
    ) {
        // query里包含update time，用作event time
        let event_time_us = parsed.update_time_ms.saturating_mul(1_000);
        let order_id = parsed.order_id;
        let tif = TimeInForce::from_u8(parsed.time_in_force_u8).unwrap_or(TimeInForce::GTC);

        // query结果里包含的executed qty，用作trade update
        // executed_qty 是累计成交量；只在发生增量时才补发，避免 watchdog 查询反复打印/重复推进。
        if parsed.executed_qty > order.cumulative_filled_quantity + 1e-12 {
            let status = if parsed.status_u8 == OrderExecutionStatus::Filled.to_u8() {
                Some(OrderStatus::Filled)
            } else {
                Some(OrderStatus::PartiallyFilled)
            };
            let trade_id = parsed.trade_id;
            let trade = OrderQueryTradeUpdate::new(
                order,
                order_id,
                trade_id,
                event_time_us,
                parsed.executed_qty,
                status,
                tif,
            );
            self.apply_trade_update_with_record(&trade);
        }

        let status_u8 = parsed.status_u8;
        if status_u8 == OrderExecutionStatus::Create.to_u8() {
            // 仅在本地还没进入 Create（或未记录 exchange_order_id）时补发 New，避免每次 watchdog 都重复“已挂单”日志。
            let already_live = order.status == OrderExecutionStatus::Create
                && order.exchange_order_id.is_some_and(|id| id == order_id);
            if !already_live {
                let upd = OrderQueryOrderUpdate::new(
                    order,
                    order_id,
                    event_time_us,
                    OrderStatus::New,
                    ExecutionType::New,
                    parsed.executed_qty,
                    tif,
                );
                self.apply_order_update_with_record(&upd);
            }
        } else if status_u8 == OrderExecutionStatus::Cancelled.to_u8() {
            let already_processed = self
                .processed_cancel_updates
                .contains(&order.client_order_id);
            if order.status != OrderExecutionStatus::Cancelled || !already_processed {
                let upd = OrderQueryOrderUpdate::new(
                    order,
                    order_id,
                    event_time_us,
                    OrderStatus::Canceled,
                    ExecutionType::Canceled,
                    parsed.executed_qty,
                    tif,
                );
                self.apply_order_update_with_record(&upd);
            }
        } else if status_u8 == OrderExecutionStatus::Rejected.to_u8() {
            error!(
                "HedgeArbStrategy: strategy_id={} query_resp rejected: client_order_id={} order_id={} exec_qty={:.8} reason={:?}",
                self.strategy_id,
                order.client_order_id,
                order_id,
                parsed.executed_qty,
                reason
            );
        }

        info!(
            "HedgeArbStrategy: strategy_id={} query回补{}: leg={:?} client_order_id={} order_id={} exec_qty={:.8} status_u8={} reason={:?}",
            self.strategy_id,
            match reason {
                PendingOrderQueryReason::OrderWatchdog => "（下单回报缺失触发watchdog）",
                PendingOrderQueryReason::CancelWatchdog => "（撤单回报缺失触发watchdog）",
                PendingOrderQueryReason::OpenLegCancelFailed => "（开仓撤单失败后回补）",
                PendingOrderQueryReason::HedgeLegCancelFailed => "（对冲撤单失败后回补）",
                PendingOrderQueryReason::CancelRejected => "（撤单被拒绝/不可撤销后回补）",
            },
            self.classify_leg(order.client_order_id),
            order.client_order_id,
            order_id,
            parsed.executed_qty,
            parsed.status_u8,
            reason
        );
    }

    fn handle_open_leg_query_result(
        &mut self,
        _response: &dyn QueryEngineResponse,
        client_order_id: i64,
        reason: PendingOrderQueryReason,
        parsed: Option<CompactOrderQueryResp>,
    ) {
        // 先拿到本地订单（用于构造撤单请求、以及补发 order/trade update）。
        let order_mgr = MonitorChannel::instance().order_manager();
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "HedgeArbStrategy: strategy_id={} open_leg query_resp but order missing: client_order_id={} reason={:?}",
                self.strategy_id, client_order_id, reason
            );
            //如果本地订单拿不到，说明也是开仓就失败了，不处理后续逻辑
            self.alive_flag = false;
            return;
        };

        let Some(parsed) = parsed else {
            // query 失败：继续由 watchdog 驱动重试
            match reason {
                PendingOrderQueryReason::OrderWatchdog => {
                    self.schedule_order_query_watchdog(client_order_id);
                }
                PendingOrderQueryReason::CancelWatchdog
                | PendingOrderQueryReason::OpenLegCancelFailed
                | PendingOrderQueryReason::HedgeLegCancelFailed => {
                    self.schedule_cancel_query_watchdog(client_order_id);
                }
                PendingOrderQueryReason::CancelRejected => {
                    self.schedule_cancel_query_watchdog_with_reason(
                        client_order_id,
                        PendingOrderQueryReason::CancelRejected,
                    );
                }
            }
            return;
        };

        // 2、查询订单有状态，有match匹配状态，分情况处理
        let Some(st) = OrderExecutionStatus::from_u8(parsed.status_u8) else {
            warn!(
                "HedgeArbStrategy: strategy_id={} open_leg query invalid status_u8={} (close): client_order_id={} reason={:?}",
                self.strategy_id, parsed.status_u8, client_order_id, reason
            );
            self.alive_flag = false;
            return;
        };

        match st {
            // (a)状态是filled，说明订单已经完全成交，补充成交信息，然后触发对冲。
            OrderExecutionStatus::Filled => {
                self.apply_parsed_order_query_updates(&order, parsed, reason);
            }
            // (b)状态是canceled，说明订单已经撤销成功，补充撤销信息，然后触发对冲。
            OrderExecutionStatus::Cancelled => {
                self.apply_parsed_order_query_updates(&order, parsed, reason);
            }
            // (c)状态是partially_filled等，可以撤销的状态，说明rest api出错，重新发起撤单请求。
            OrderExecutionStatus::Create => match reason {
                PendingOrderQueryReason::OrderWatchdog => {
                    self.apply_parsed_order_query_updates(&order, parsed, reason);
                }
                PendingOrderQueryReason::CancelRejected => {
                    self.apply_parsed_order_query_updates(&order, parsed, reason);
                    self.schedule_cancel_query_watchdog_with_reason(
                        client_order_id,
                        PendingOrderQueryReason::CancelRejected,
                    );
                }
                PendingOrderQueryReason::CancelWatchdog
                | PendingOrderQueryReason::OpenLegCancelFailed
                | PendingOrderQueryReason::HedgeLegCancelFailed => {
                    self.apply_parsed_order_query_updates(&order, parsed, reason);
                    let exchange = order.venue.trade_engine_exchange();
                    match order.get_order_cancel_bytes() {
                        Ok(cancel_bytes) => {
                            if let Err(e) =
                                TradeEngHub::publish_order_request(exchange, &cancel_bytes)
                            {
                                warn!(
                                        "HedgeArbStrategy: strategy_id={} open_leg re-cancel publish failed: exchange={} client_order_id={} err={}",
                                        self.strategy_id, exchange, client_order_id, e
                                    );
                            } else {
                                info!(
                                        "HedgeArbStrategy: strategy_id={} open_leg re-cancel sent: exchange={} client_order_id={} reason={:?}",
                                        self.strategy_id, exchange, client_order_id, reason
                                    );
                                self.schedule_cancel_query_watchdog(client_order_id);
                            }
                        }
                        Err(e) => {
                            warn!(
                                    "HedgeArbStrategy: strategy_id={} open_leg get cancel bytes failed: client_order_id={} err={}",
                                    self.strategy_id, client_order_id, e
                                );
                            self.schedule_cancel_query_watchdog(client_order_id);
                        }
                    }
                }
            },
            OrderExecutionStatus::Rejected => {
                // 如果 query 返回 rejected，说明订单本身已被拒绝（不是撤单缺回报），无法再推进后续流程，直接关闭策略。
                error!(
                    "HedgeArbStrategy: strategy_id={} open_leg query shows rejected (close): client_order_id={} reason={:?}",
                    self.strategy_id, client_order_id, reason
                );
                self.alive_flag = false;
            }
            OrderExecutionStatus::Commit => {
                // Commit 是本地状态，query 不应该返回该状态；视为异常。
                error!(
                    "HedgeArbStrategy: strategy_id={} open_leg query shows commit(unexpected, close): client_order_id={} reason={:?}",
                    self.strategy_id, client_order_id, reason
                );
                self.alive_flag = false;
            }
        }
    }

    //通过 order query 的结果，补充traade/order update，来推进strategy逻辑
    fn handle_hedge_leg_query_result(
        &mut self,
        _response: &dyn QueryEngineResponse,
        client_order_id: i64,
        reason: PendingOrderQueryReason,
        parsed: Option<CompactOrderQueryResp>,
    ) {
        let order_mgr = MonitorChannel::instance().order_manager();
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "HedgeArbStrategy: strategy_id={} hedge_leg query_resp but order missing: client_order_id={} reason={:?}",
                self.strategy_id, client_order_id, reason
            );
            self.alive_flag = false;
            return;
        };

        let Some(parsed) = parsed else {
            match reason {
                PendingOrderQueryReason::OrderWatchdog => {
                    self.schedule_order_query_watchdog(client_order_id);
                }
                PendingOrderQueryReason::CancelWatchdog
                | PendingOrderQueryReason::OpenLegCancelFailed
                | PendingOrderQueryReason::HedgeLegCancelFailed => {
                    self.schedule_cancel_query_watchdog(client_order_id);
                }
                PendingOrderQueryReason::CancelRejected => {
                    self.schedule_cancel_query_watchdog_with_reason(
                        client_order_id,
                        PendingOrderQueryReason::CancelRejected,
                    );
                }
            }
            return;
        };

        let Some(st) = OrderExecutionStatus::from_u8(parsed.status_u8) else {
            warn!(
                "HedgeArbStrategy: strategy_id={} hedge_leg query invalid status_u8={} (close): client_order_id={} reason={:?}",
                self.strategy_id, parsed.status_u8, client_order_id, reason
            );
            self.alive_flag = false;
            return;
        };

        match st {
            OrderExecutionStatus::Filled | OrderExecutionStatus::Cancelled => {
                self.apply_parsed_order_query_updates(&order, parsed, reason);
            }
            OrderExecutionStatus::Create => {
                match reason {
                    PendingOrderQueryReason::OrderWatchdog => {
                        self.apply_parsed_order_query_updates(&order, parsed, reason);
                    }
                    PendingOrderQueryReason::CancelRejected => {
                        self.apply_parsed_order_query_updates(&order, parsed, reason);
                        self.schedule_cancel_query_watchdog_with_reason(
                            client_order_id,
                            PendingOrderQueryReason::CancelRejected,
                        );
                    }
                    PendingOrderQueryReason::CancelWatchdog
                    | PendingOrderQueryReason::OpenLegCancelFailed
                    | PendingOrderQueryReason::HedgeLegCancelFailed => {
                        self.apply_parsed_order_query_updates(&order, parsed, reason);
                        // 订单仍处于可撤销状态：重发撤单请求
                        let exchange = order.venue.trade_engine_exchange();
                        match order.get_order_cancel_bytes() {
                            Ok(cancel_bytes) => {
                                if let Err(e) =
                                    TradeEngHub::publish_order_request(exchange, &cancel_bytes)
                                {
                                    warn!(
                                        "HedgeArbStrategy: strategy_id={} hedge_leg re-cancel publish failed: exchange={} client_order_id={} err={}",
                                        self.strategy_id, exchange, client_order_id, e
                                    );
                                } else {
                                    info!(
                                        "HedgeArbStrategy: strategy_id={} hedge_leg re-cancel sent: exchange={} client_order_id={} reason={:?}",
                                        self.strategy_id, exchange, client_order_id, reason
                                    );
                                    self.schedule_cancel_query_watchdog(client_order_id);
                                }
                            }
                            Err(e) => {
                                warn!(
                                    "HedgeArbStrategy: strategy_id={} hedge_leg get cancel bytes failed: client_order_id={} err={}",
                                    self.strategy_id, client_order_id, e
                                );
                                self.schedule_cancel_query_watchdog(client_order_id);
                            }
                        }
                    }
                }
            }
            OrderExecutionStatus::Rejected => {
                // 如果 query 返回 rejected，说明订单本身可能未挂上（等价对冲侧 open_failed），清理后重试对冲。
                warn!(
                    "HedgeArbStrategy: strategy_id={} hedge_leg query shows rejected (treat as open_failed): client_order_id={} reason={:?}",
                    self.strategy_id, client_order_id, reason
                );
                self.cleanup_failed_hedge_order(client_order_id);
                let base_pending_qty = self.cumulative_open_qty - self.cumulative_hedged_qty;
                self.try_resend_hedge_after_open_failed(base_pending_qty, "rejected");
            }
            OrderExecutionStatus::Commit => {
                warn!(
                    "HedgeArbStrategy: strategy_id={} hedge_leg query shows commit(unexpected, close): client_order_id={} reason={:?}",
                    self.strategy_id, client_order_id, reason
                );
                self.alive_flag = false;
            }
        }
    }

    fn handle_open_leg_open_failed(
        &mut self,
        response: &dyn TradeEngineResponse,
        code_desc: &str,
        client_order_id: i64,
    ) {
        warn!(
            "HedgeArbStrategy: strategy_id={} open_leg open_failed: req_type={} status={} code={}({}) client_order_id={}",
            self.strategy_id,
            response.req_type(),
            response.status(),
            response.error_code(),
            code_desc,
            client_order_id
        );
        // open leg失败，无论是什么原因，都选择直接关闭
        // 1、即便是因为穿价，也不需要再报一笔
        // 2、todo，借币失败可以设计一个cooldown，因为之后短期应该都无法借贷，因此整个订单应该都无法操作
        // 3、开仓失败也不需要更新订单状态，实际没有产生订单，无意义
        self.alive_flag = false;
    }

    fn handle_open_leg_cancel_failed(
        &mut self,
        response: &dyn TradeEngineResponse,
        code_desc: &str,
        client_order_id: i64,
    ) {
        warn!(
            "HedgeArbStrategy: strategy_id={} open_leg cancel_failed: req_type={} status={} code={}({}) client_order_id={}",
            self.strategy_id,
            response.req_type(),
            response.status(),
            response.error_code(),
            code_desc,
            client_order_id
        );
        if response.is_cancel_not_cancellable() {
            // Cancel 已被交易所拒绝/不可撤销：只做 query 回补，不再重发 cancel。
            self.clear_query_watchdogs(client_order_id);
            self.send_order_query_for_cancel_failed(
                client_order_id,
                PendingOrderQueryReason::CancelRejected,
            );
            self.schedule_cancel_query_watchdog_with_reason(
                client_order_id,
                PendingOrderQueryReason::CancelRejected,
            );
        } else {
            self.send_order_query_for_cancel_failed(
                client_order_id,
                PendingOrderQueryReason::OpenLegCancelFailed,
            );
        }
    }

    fn handle_open_leg_other_failed(
        &mut self,
        response: &dyn TradeEngineResponse,
        code_desc: &str,
        client_order_id: i64,
    ) {
        warn!(
            "HedgeArbStrategy: strategy_id={} open_leg other_failed(TODO): req_type={} status={} code={}({}) client_order_id={}",
            self.strategy_id,
            response.req_type(),
            response.status(),
            response.error_code(),
            code_desc,
            client_order_id
        );
    }

    fn cleanup_failed_hedge_order(&mut self, client_order_id: i64) {
        let order_mgr = MonitorChannel::instance().order_manager();
        let _ = order_mgr.borrow_mut().remove(client_order_id);
        self.hedge_order_ids.retain(|&id| id != client_order_id);
        self.hedge_expire_ts = None;
    }

    fn try_resend_hedge_after_open_failed(&mut self, base_pending_qty: f64, reason: &'static str) {
        if base_pending_qty <= 1e-8 {
            self.hedge_retry_after_ts = None;
            self.hedge_retry_reason = None;
            return;
        }

        if let Some(cooldown_us) = self.hedge_timeout_us {
            let now = get_timestamp_us();
            let due_ts = now.saturating_add(cooldown_us);
            let cooldown_ms = cooldown_us.saturating_div(1_000);
            self.hedge_retry_after_ts = Some(due_ts);
            self.hedge_retry_reason = Some(reason);
            info!(
                "HedgeArbStrategy: strategy_id={} hedge open_failed, cooldown={}ms retry_at={} reason={} pending_qty={:.8}",
                self.strategy_id, cooldown_ms, due_ts, reason, base_pending_qty
            );
            return;
        }

        let (sent, hedged_qty, _) = self.try_hedge_with_residual(base_pending_qty);
        if sent {
            debug!(
                "HedgeArbStrategy: strategy_id={} hedge re-send after {} open_failed, qty={:.8}",
                self.strategy_id, reason, hedged_qty
            );
        } else {
            warn!(
                "HedgeArbStrategy: strategy_id={} hedge re-send skipped/failed after {} open_failed, pending_qty={:.8}",
                self.strategy_id, reason, base_pending_qty
            );
        }
    }

    fn handle_hedge_leg_open_failed(
        &mut self,
        response: &dyn TradeEngineResponse,
        code_desc: &str,
        client_order_id: i64,
    ) {
        let error_code = response.error_code();
        if let Some(reason) = Self::hedge_invalid_param_reason(error_code) {
            let order = MonitorChannel::instance()
                .order_manager()
                .borrow()
                .get(client_order_id);
            let (order_qty, order_price, order_side, order_symbol) = match order.as_ref() {
                Some(o) => (o.quantity, o.price, o.side, o.symbol.clone()),
                None => (0.0, 0.0, self.hedge_side, self.hedge_symbol.clone()),
            };
            let pending_qty = self.cumulative_open_qty - self.cumulative_hedged_qty;
            let detail = format!(
                "req_type={} status={} code={}({}) client_order_id={} order_symbol={} order_qty={:.8} order_price={:.8} pending_qty={:.8}",
                response.req_type(),
                response.status(),
                error_code,
                code_desc,
                client_order_id,
                order_symbol,
                order_qty,
                order_price,
                pending_qty
            );
            self.push_hedge_residual_with_print(
                reason,
                &detail,
                pending_qty,
                order_price,
                order_side,
            );
            self.cleanup_failed_hedge_order(client_order_id);
            return;
        }

        //对重新报单失败的原因进行分类
        //1、post only rejected是正常的，因为盘口出现穿价是正常情况，再次报单即可
        let is_post_only_rejected = response.is_post_only_rejected();
        let is_price_limit_rejected = response.is_price_limit_rejected();
        let is_insufficient_margin = response.is_insufficient_margin();

        let reason = if is_post_only_rejected {
            "post_only"
        } else if is_price_limit_rejected {
            "price_limit"
        } else if is_insufficient_margin {
            "insufficient_margin"
        } else {
            "other"
        };

        // 对冲侧报单失败：无论原因，都要继续对冲（不关闭策略），并让上游 cooldown/限频机制接管。
        if reason == "other" {
            // 仅对非post only拒单的情况打印error日志，之后检索error日志，判断为什么会对冲失
            debug!(
                "HedgeArbStrategy: strategy_id={} hedge_leg open_failed(retry): req_type={} status={} code={}({}) client_order_id={}",
                self.strategy_id,
                response.req_type(),
                response.status(),
                response.error_code(),
                code_desc,
                client_order_id
            );
        } else {
            warn!(
                "HedgeArbStrategy: strategy_id={} hedge_leg open_failed({}): req_type={} status={} code={}({}) client_order_id={}",
                self.strategy_id,
                reason,
                response.req_type(),
                response.status(),
                response.error_code(),
                code_desc,
                client_order_id
            );
        }
        //既然要撤单再报单，旧的不要了，没有作用了
        self.cleanup_failed_hedge_order(client_order_id);
        let base_pending_qty = self.cumulative_open_qty - self.cumulative_hedged_qty;
        self.try_resend_hedge_after_open_failed(base_pending_qty, reason);
    }

    // 撤单失败的处理逻辑：通过rest api查询订单状态，补发order/trade update
    fn handle_hedge_leg_cancel_failed(
        &mut self,
        response: &dyn TradeEngineResponse,
        code_desc: &str,
        client_order_id: i64,
    ) {
        warn!(
            "HedgeArbStrategy: strategy_id={} hedge_leg cancel_failed: req_type={} status={} code={}({}) client_order_id={}",
            self.strategy_id,
            response.req_type(),
            response.status(),
            response.error_code(),
            code_desc,
            client_order_id
        );
        if response.is_cancel_not_cancellable() {
            // Cancel 已被交易所拒绝/不可撤销：只做 query 回补，不再重发 cancel。
            self.clear_query_watchdogs(client_order_id);
            self.send_order_query_for_cancel_failed(
                client_order_id,
                PendingOrderQueryReason::CancelRejected,
            );
            self.schedule_cancel_query_watchdog_with_reason(
                client_order_id,
                PendingOrderQueryReason::CancelRejected,
            );
        } else {
            self.send_order_query_for_cancel_failed(
                client_order_id,
                PendingOrderQueryReason::HedgeLegCancelFailed,
            );
        }
    }

    fn handle_hedge_leg_other_failed(
        &mut self,
        response: &dyn TradeEngineResponse,
        code_desc: &str,
        client_order_id: i64,
    ) {
        warn!(
            "HedgeArbStrategy: strategy_id={} hedge_leg other_failed(TODO): req_type={} status={} code={}({}) client_order_id={}",
            self.strategy_id,
            response.req_type(),
            response.status(),
            response.error_code(),
            code_desc,
            client_order_id
        );
    }

    fn handle_open_leg_failed(
        &mut self,
        response: &dyn TradeEngineResponse,
        code_desc: &str,
        client_order_id: i64,
    ) {
        match response.request_kind() {
            TradeRequestKind::Open => {
                self.handle_open_leg_open_failed(response, code_desc, client_order_id)
            }
            TradeRequestKind::Cancel => {
                self.handle_open_leg_cancel_failed(response, code_desc, client_order_id)
            }
            TradeRequestKind::Other => {
                self.handle_open_leg_other_failed(response, code_desc, client_order_id)
            }
        }
    }

    fn handle_hedge_leg_failed(
        &mut self,
        response: &dyn TradeEngineResponse,
        code_desc: &str,
        client_order_id: i64,
    ) {
        match response.request_kind() {
            TradeRequestKind::Open => {
                self.handle_hedge_leg_open_failed(response, code_desc, client_order_id)
            }
            TradeRequestKind::Cancel => {
                self.handle_hedge_leg_cancel_failed(response, code_desc, client_order_id)
            }
            TradeRequestKind::Other => {
                self.handle_hedge_leg_other_failed(response, code_desc, client_order_id)
            }
        }
    }
}

impl Drop for HedgeArbStrategy {
    fn drop(&mut self) {
        self.cleanup_strategy_orders();
    }
}

impl ForceCloseControl for HedgeArbStrategy {
    fn set_force_close_mode(&mut self, enabled: bool) {
        self.force_close_mode = enabled;
    }

    fn is_force_close_mode(&self) -> bool {
        self.force_close_mode
    }
}

impl Strategy for HedgeArbStrategy {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn get_id(&self) -> i32 {
        self.strategy_id
    }

    fn symbol(&self) -> Option<&str> {
        Some(&self.open_symbol)
    }

    fn is_strategy_order(&self, order_id: i64) -> bool {
        Self::extract_strategy_id(order_id) == self.strategy_id
    }

    fn handle_signal_with_record(&mut self, signal: &TradeSignal) {
        HedgeArbStrategy::handle_signal(self, signal);

        // 持久化信号记录
        let record = SignalRecordMessage::new(
            self.strategy_id,
            signal.signal_type.clone(),
            signal.context.clone().to_vec(),
            signal.generation_time,
        );
        PersistChannel::with(|ch| ch.publish_signal_record(&record));
    }

    fn apply_order_update_with_record(&mut self, update: &dyn OrderUpdate) {
        HedgeArbStrategy::apply_order_update(self, update);

        // 持久化订单更新记录
        PersistChannel::with(|ch| ch.publish_order_update(update));
    }

    fn apply_trade_update_with_record(&mut self, trade: &dyn TradeUpdate) {
        HedgeArbStrategy::apply_trade_update(self, trade);

        // 持久化成交记录
        PersistChannel::with(|ch| ch.publish_trade_update(trade));
    }

    fn apply_trade_engine_response(&mut self, response: &dyn TradeEngineResponse) {
        // 1、is_request_success表示http200 + error code为0，这种不处理
        // 2、无论是下单、撤单的成功，等待account monitor来推送结果，推送缺少的情况下，通过rest api请求回补
        // 3、因此只要成功，就不进行处理
        if response.is_request_success() {
            return;
        }

        let client_order_id = response.client_order_id();
        let leg = self.classify_leg(client_order_id);
        let exchange = response.exchange_enum();
        let code_desc = exchange
            .and_then(|ex| describe_trade_error_code(ex, response.error_code()))
            .unwrap_or("unknown");

        match leg {
            Some(Leg::Open) => self.handle_open_leg_failed(response, code_desc, client_order_id),
            Some(Leg::Hedge) => self.handle_hedge_leg_failed(response, code_desc, client_order_id),
            None => {}
        }
    }

    fn apply_query_engine_response(&mut self, response: &dyn QueryEngineResponse) {
        let client_order_id = response.client_query_id();
        let Some(reason) = self.pending_order_queries.remove(&client_order_id) else {
            return;
        };

        let Some(leg) = self.classify_leg(client_order_id) else {
            return;
        };

        let body = response.body_bytes().as_ref();
        let has_any_byte = body.iter().any(|&b| b != 0);
        if !has_any_byte {
            return;
        }

        let actual_len = body
            .iter()
            .rposition(|&b| b != 0)
            .map(|pos| pos + 1)
            .unwrap_or(0);
        if actual_len == 1 && body[0] == b'E' {
            match (leg, reason) {
                (Leg::Open, PendingOrderQueryReason::OrderWatchdog) => {
                    warn!(
                        "HedgeArbStrategy: strategy_id={} open_leg order query failed (E, close): client_order_id={}",
                        self.strategy_id, client_order_id
                    );
                    self.alive_flag = false;
                    return;
                }
                (Leg::Hedge, PendingOrderQueryReason::OrderWatchdog) => {
                    warn!(
                        "HedgeArbStrategy: strategy_id={} hedge_leg order query failed (E, treat as open_failed): client_order_id={}",
                        self.strategy_id, client_order_id
                    );
                    self.cleanup_failed_hedge_order(client_order_id);
                    let base_pending_qty = self.cumulative_open_qty - self.cumulative_hedged_qty;
                    self.try_resend_hedge_after_open_failed(base_pending_qty, "query_failed");
                    return;
                }
                (Leg::Open, PendingOrderQueryReason::CancelWatchdog)
                | (Leg::Open, PendingOrderQueryReason::OpenLegCancelFailed)
                | (Leg::Open, PendingOrderQueryReason::HedgeLegCancelFailed) => {
                    self.schedule_cancel_query_watchdog(client_order_id);
                    return;
                }
                (Leg::Hedge, PendingOrderQueryReason::CancelWatchdog)
                | (Leg::Hedge, PendingOrderQueryReason::OpenLegCancelFailed)
                | (Leg::Hedge, PendingOrderQueryReason::HedgeLegCancelFailed) => {
                    self.schedule_cancel_query_watchdog(client_order_id);
                    return;
                }
                (_, PendingOrderQueryReason::CancelRejected) => {
                    self.schedule_cancel_query_watchdog_with_reason(
                        client_order_id,
                        PendingOrderQueryReason::CancelRejected,
                    );
                    return;
                }
            }
        }

        let body_bytes = response.body_bytes();
        let parsed = Self::parse_compact_order_query_resp(body_bytes);
        if parsed.is_none() {
            let text = if actual_len > 0 {
                String::from_utf8_lossy(&body[..actual_len]).to_string()
            } else {
                String::new()
            };
            warn!(
                "HedgeArbStrategy: strategy_id={} query_resp decode failed: client_order_id={} req_type={} reason={:?} body='{}'",
                self.strategy_id,
                client_order_id,
                response.req_type(),
                reason,
                text
                );
        }

        match leg {
            Leg::Open => {
                self.handle_open_leg_query_result(response, client_order_id, reason, parsed)
            }
            Leg::Hedge => {
                self.handle_hedge_leg_query_result(response, client_order_id, reason, parsed)
            }
        }
    }

    fn handle_period_clock(&mut self, _current_tp: i64) {
        // 周期性检查开仓和对冲订单的超时情况
        if self.is_active() {
            self.handle_open_leg_timeout();
            self.handle_hedge_leg_timeout();
            self.handle_query_watchdogs();
            self.handle_hedge_retry_cooldown();
        }
    }

    fn is_active(&self) -> bool {
        self.alive_flag
    }
}
