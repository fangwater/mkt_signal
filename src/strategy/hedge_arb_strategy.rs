use crate::common::bbo::{Bbo, DualBbo};
use crate::common::tick_math::QuantizedValue;
use crate::common::time_util::get_timestamp_us;
use crate::common::trade_error_code::describe_trade_error_code;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus, OrderManager, OrderType, Side};
use crate::pre_trade::signal_throttle::register_signal_throttle;
use crate::pre_trade::{PersistChannel, SignalChannel, TradeEngHub};
use crate::signal::arb_signal::ArbBackwardQueryMsg;
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::common::{align_price_floor, OrderStatus, SignalBytes, TradingVenue};
use crate::signal::hedge_signal::ArbHedgeCtx;
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::arb_orphan_strategy::ArbOrphanLeg;
use crate::strategy::manager::{
    ArbOpenPriceMapEntry, ArbOrphanHandoff, ArbOrphanResidualHandoff, ArbOrphanUniformCtx,
    ForceCloseControl, Strategy,
};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::query_engine_response::QueryEngineResponse;
use crate::strategy::trade_engine_response::{TradeEngineResponse, TradeRequestKind};
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::uniform_arb_publish::{
    publish_arb_uniform_new_order, publish_arb_uniform_terminal_order,
    publish_arb_uniform_trade_order, ArbUniformPublishCtx,
};
use crate::strategy::ws_order_update::WsOrderUpdate;
use log::{debug, error, info, warn};
use std::any::Any;

fn qv_decimal_or_fallback(value: f64) -> String {
    QuantizedValue::from_decimal(value)
        .map(|qv| qv.decimal_string())
        .unwrap_or_else(|| format!("{value:.8}"))
}
use std::collections::HashSet;

// 下单后若迟迟收不到 account monitor 的推送（New/Filled 等），触发兜底。
// Open/hedge leg 进入不确定状态后都移交 arb orphan 统一 query/cancel 收敛。
const ORDER_QUERY_WATCHDOG_DELAY_US: i64 = 300_000;
const CANCEL_QUERY_WATCHDOG_DELAY_US: i64 = 300_000;
const HEDGE_RESIDUAL_EPS: f64 = 1e-12;

pub struct HedgeArbStrategy {
    pub strategy_id: i32,                  //策略id
    pub open_symbol: String,               //开仓侧symbol
    pub open_venue: TradingVenue,          //开仓侧交易场所
    pub open_order_id: i64,                //开仓单唯一，报多单对应多个Strategy
    pub hedge_order_ids: Vec<i64>,         //对冲单会产生一个or多个，因为部分成交
    pub open_expire_ts: Option<i64>,       //开仓单挂单截止时间（绝对时间戳）
    pub hedge_timeout_us: Option<i64>,     //对冲单允许的存活时间（微秒），>0 表示 MM，None 表示 MT
    pub hedge_expire_ts: Option<i64>,      //当前对冲挂单的截止时间（绝对时间戳）
    pub order_seq: u32,                    //订单号计数器
    pub cumulative_hedged_qty: f64,        //累计对冲数量
    pub cumulative_open_qty: f64,          //累计开仓数量
    pub open_qty_multiplier: f64,          //开仓侧数量乘数（venue qty -> base qty）
    pub hedge_qty_multiplier: f64,         //对冲侧数量乘数（venue qty -> base qty）
    pub open_filled_hedge_triggered: bool, //开仓成交已触发对冲（防止query重复触发）
    pub alive_flag: bool,                  //策略是否存活
    pub hedge_symbol: String,              //对冲侧symbol
    pub hedge_venue: TradingVenue,         //对冲侧交易场所
    pub hedge_side: Side,                  //对冲侧方向
    pub hedge_request_seq: u32,            //累计对冲请求次数
    pub open_signal_ts: i64,               //开仓信号时间戳（微秒）
    pub hedge_signal_ts: i64,              //对冲信号时间戳（微秒）
    pub open_price_qv: QuantizedValue,     //开仓信号 price_qv
    pub open_price_offset: f64,            //开仓信号 price_offset
    pub hedge_price_offset: f64,           //对冲信号 price_offset
    pub open_from_key: String,             //开仓来源标记
    pub hedge_from_key: String,            //对冲来源标记
    pub open_bbo: DualBbo,                 //开仓时双腿盘口
    pub force_close_mode: bool,            //是否强平模式
    pub last_cancel_trigger_ts: Option<i64>,
    pub last_open_cancel_reason: Option<&'static str>,
    pub last_inactive_reason: Option<String>,
    order_query_watchdog: Option<QueryWatchdog>,
    cancel_query_watchdog: Option<QueryWatchdog>,
    keep_local_on_drop_order_ids: HashSet<i64>,
    pending_arb_orphan_handoffs: Vec<ArbOrphanHandoff>,
    pending_arb_orphan_residuals: Vec<ArbOrphanResidualHandoff>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Leg {
    Open,
    Hedge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingOrderQueryReason {
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
    fn mark_inactive(&mut self, reason: impl Into<String>) {
        self.alive_flag = false;
        self.last_inactive_reason = Some(reason.into());
    }

    pub fn inactive_reason(&self) -> Option<&str> {
        self.last_inactive_reason.as_deref()
    }

    pub fn open_side(&self) -> Side {
        if self.hedge_side == Side::Buy {
            Side::Sell
        } else {
            Side::Buy
        }
    }

    fn extract_assets_from_symbol(symbol: &str) -> (String, String) {
        let symbol_upper = symbol.to_uppercase();
        const QUOTE_ASSETS: [&str; 7] = ["USDT", "USDC", "BUSD", "FDUSD", "BIDR", "TRY", "USD"];

        for quote in QUOTE_ASSETS {
            if symbol_upper.ends_with(quote) && symbol_upper.len() > quote.len() {
                let base = &symbol_upper[..symbol_upper.len() - quote.len()];
                return (base.to_string(), quote.to_string());
            }
        }
        (symbol_upper, "USDT".to_string())
    }

    fn queue_arb_orphan_handoff(
        &mut self,
        client_order_id: i64,
        leg: ArbOrphanLeg,
        cancel_intent: bool,
        max_query_attempts: Option<u8>,
        reason: impl Into<String>,
    ) {
        if client_order_id <= 0
            || self
                .pending_arb_orphan_handoffs
                .iter()
                .any(|handoff| handoff.client_order_id == client_order_id)
        {
            return;
        }
        let reason = reason.into();
        let uniform_ctx = match leg {
            ArbOrphanLeg::Open => Some(ArbOrphanUniformCtx {
                signal_ts: self.open_signal_ts,
                from_key: format!("open|{}", self.open_from_key).into_bytes(),
                price_offset: self.open_price_offset,
            }),
            ArbOrphanLeg::Hedge => None,
        };
        self.keep_local_on_drop_order_ids.insert(client_order_id);
        self.pending_arb_orphan_handoffs.push(ArbOrphanHandoff {
            client_order_id,
            source_strategy_id: self.strategy_id,
            leg,
            cancel_intent,
            max_query_attempts,
            uniform_ctx,
            reason: reason.clone(),
        });
        info!(
            "HedgeArbStrategy: strategy_id={} handoff order to arb orphan client_order_id={} leg={:?} cancel_intent={} max_query_attempts={:?} reason={}",
            self.strategy_id,
            client_order_id,
            leg,
            cancel_intent,
            max_query_attempts,
            reason
        );
    }

    fn signed_residual_qty_for_hedge_side(side: Side, base_qty: f64) -> f64 {
        match side {
            // Sell hedge 处理正 open exposure。
            Side::Sell => base_qty,
            // Buy hedge 处理负 open exposure。
            Side::Buy => -base_qty,
        }
    }

    fn queue_arb_orphan_residual(&mut self, side: Side, base_qty: f64, reason: impl Into<String>) {
        if base_qty <= HEDGE_RESIDUAL_EPS || self.hedge_symbol.is_empty() {
            return;
        }
        let reason = reason.into();
        let signed_base_qty = Self::signed_residual_qty_for_hedge_side(side, base_qty);
        self.pending_arb_orphan_residuals
            .push(ArbOrphanResidualHandoff {
                symbol: self.hedge_symbol.clone(),
                venue: self.hedge_venue,
                signed_base_qty,
                source_strategy_id: self.strategy_id,
                reason: reason.clone(),
            });
        info!(
            "HedgeArbStrategy: strategy_id={} queue arb orphan residual symbol={} venue={:?} side={:?} base_qty={:.8} signed_base_qty={:.8} reason={}",
            self.strategy_id,
            self.hedge_symbol,
            self.hedge_venue,
            side,
            base_qty,
            signed_base_qty,
            reason
        );
    }

    fn handoff_open_order_to_arb_orphan(
        &mut self,
        cancel_intent: bool,
        max_query_attempts: Option<u8>,
        reason: impl Into<String>,
    ) {
        self.queue_arb_orphan_handoff(
            self.open_order_id,
            ArbOrphanLeg::Open,
            cancel_intent,
            max_query_attempts,
            reason,
        );
    }

    fn handoff_hedge_order_to_arb_orphan(
        &mut self,
        client_order_id: i64,
        cancel_intent: bool,
        max_query_attempts: Option<u8>,
        reason: impl Into<String>,
    ) {
        let reason = reason.into();
        if self.open_order_id > 0 {
            self.queue_arb_orphan_handoff(
                self.open_order_id,
                ArbOrphanLeg::Open,
                false,
                None,
                format!("paired_hedge_orphan:{}", reason),
            );
        }
        self.queue_arb_orphan_handoff(
            client_order_id,
            ArbOrphanLeg::Hedge,
            cancel_intent,
            max_query_attempts,
            reason,
        );
        self.alive_flag = false;
    }

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
            open_qty_multiplier: 1.0,
            hedge_qty_multiplier: 1.0,
            open_filled_hedge_triggered: false,
            alive_flag: true,
            hedge_symbol: String::new(),
            hedge_venue: TradingVenue::BinanceMargin, // 默认值，将在开仓时更新
            hedge_side: Side::Buy,                    // 默认值，将在开仓时更新
            hedge_request_seq: 0,
            open_signal_ts: 0,
            hedge_signal_ts: 0,
            open_price_qv: QuantizedValue::zero(),
            open_price_offset: 0.0,
            hedge_price_offset: 0.0,
            open_from_key: String::new(),
            hedge_from_key: String::new(),
            open_bbo: DualBbo::default(),
            force_close_mode: false,
            last_cancel_trigger_ts: None,
            last_open_cancel_reason: None,
            last_inactive_reason: None,
            order_query_watchdog: None,
            cancel_query_watchdog: None,
            keep_local_on_drop_order_ids: HashSet::new(),
            pending_arb_orphan_handoffs: Vec::new(),
            pending_arb_orphan_residuals: Vec::new(),
        };
        strategy
    }

    fn describe_venue(venue: u8) -> String {
        TradingVenue::from_u8(venue)
            .map(|v| format!("{:?}", v))
            .unwrap_or_else(|| format!("Unknown({})", venue))
    }

    fn qty_symbol_key(venue: TradingVenue, symbol: &str) -> String {
        match venue {
            TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
                symbol.to_uppercase().replace("-SWAP", "").replace('-', "")
            }
            TradingVenue::GateMargin | TradingVenue::GateFutures => {
                symbol.to_uppercase().replace('_', "").replace('-', "")
            }
            _ => symbol.to_uppercase(),
        }
    }

    fn resolve_qty_multiplier(venue: TradingVenue, symbol: &str) -> Result<f64, String> {
        match venue {
            TradingVenue::BinanceFutures => Ok(1.0),
            TradingVenue::OkexFutures | TradingVenue::GateFutures => {
                let symbol_key = Self::qty_symbol_key(venue, symbol);
                let Some(table) = MonitorChannel::instance().venue_min_qty_table(venue) else {
                    return Err(format!(
                        "未初始化 {:?} 的最小下单量表，无法获取乘数 symbol={}",
                        venue, symbol_key
                    ));
                };
                let Some(multiplier) = table.contract_multiplier_opt(&symbol_key) else {
                    return Err(format!(
                        "symbol={} 缺少 {:?} 合约乘数，无法转换 qty 口径",
                        symbol_key, venue
                    ));
                };
                if multiplier <= 0.0 {
                    return Err(format!(
                        "symbol={} {:?} contract multiplier invalid: {}",
                        symbol_key, venue, multiplier
                    ));
                }
                Ok(multiplier)
            }
            _ => Ok(1.0),
        }
    }

    fn refresh_qty_multipliers(&mut self, open_symbol: &str) -> Result<(), String> {
        self.open_qty_multiplier = Self::resolve_qty_multiplier(self.open_venue, open_symbol)?;
        self.hedge_qty_multiplier =
            Self::resolve_qty_multiplier(self.hedge_venue, &self.hedge_symbol)?;
        Ok(())
    }

    fn base_to_venue_qty(base_qty: f64, multiplier: f64, leg_name: &str) -> Result<f64, String> {
        if multiplier <= 0.0 {
            return Err(format!("{} multiplier invalid: {}", leg_name, multiplier));
        }
        Ok(base_qty / multiplier)
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
            // Binance PAPI 资产抵押上限：短期重试通常无效，转入 residual 等后续流程处理。
            51169 => Some("PLEDGED_COLLATERAL_LIMIT_REACHED/抵押上限已达"),
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
            self.queue_arb_orphan_residual(side, qty, reason);
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
            ctx.amount_value(),
            ctx.price_value()
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
        self.last_inactive_reason = None;

        let force_close = self.is_force_close_mode();
        let aligned_price = ctx.price_value();
        let aligned_qty = ctx.amount_value();
        let side = Side::from_u8(ctx.side).unwrap_or(Side::Buy);

        if aligned_price <= 0.0 {
            let order_type = OrderType::from_u8(ctx.order_type);
            warn!(
                "HedgeArbStrategy: strategy_id={} open signal price invalid: symbol={} order_type={:?} price={:.8}",
                self.strategy_id,
                ctx.get_opening_symbol(),
                order_type,
                aligned_price
            );
            self.mark_inactive("invalid_price");
            return;
        }
        if force_close {
            let opening_symbol = ctx.get_opening_symbol();
            let hedging_symbol = ctx.get_hedging_symbol();
            let opening_venue = Self::describe_venue(ctx.opening_leg.venue);
            let hedging_venue = Self::describe_venue(ctx.hedging_leg.venue);
            info!(
                "HedgeArbStrategy: strategy_id={} force_close_mode=true, 将跳过所有风控 | opening={} {} hedging={} {} side={:?} amount={:.4} price={:.6}",
                self.strategy_id,
                opening_symbol,
                opening_venue,
                hedging_symbol,
                hedging_venue,
                side,
                aligned_qty,
                aligned_price
            );
        }

        // 2、检查symbol的敞口，失败打印error
        if force_close {
            self.log_force_close_skip("单品种敞口", &ctx);
        } else if let Err(e) = MonitorChannel::instance().check_symbol_exposure(&self.open_symbol) {
            error!("HedgeArbStrategy: strategy_id={} symbol={} 单品种敞口风控检查失败: {}，标记策略为不活跃", self.strategy_id, self.open_symbol, e);
            self.mark_inactive(format!("symbol_exposure:{e}"));
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
            self.mark_inactive(format!("total_exposure:{e}"));
            return;
        }

        // 4、检查限价挂单数量限制（如果是限价单）
        let order_type = OrderType::from_u8(ctx.order_type);
        if order_type == Some(OrderType::Limit) {
            if let Err(e) =
                MonitorChannel::instance().check_pending_limit_order(&self.open_symbol, side)
            {
                error!("HedgeArbStrategy: strategy_id={} symbol={} 限价挂单数量风控检查失败: {}，标记策略为不活跃", self.strategy_id, self.open_symbol, e);
                self.mark_inactive(format!("pending_limit:{e}"));
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
        self.open_price_qv = ctx.price_qv;
        self.open_price_offset = ctx.price_offset;
        self.open_from_key = String::from_utf8_lossy(&ctx.from_key).to_string();
        self.open_bbo.open = Bbo::new(
            ctx.opening_leg.bid0,
            ctx.opening_leg.ask0,
            ctx.opening_leg.ts,
        );
        self.open_bbo.hedge = Bbo::new(
            ctx.hedging_leg.bid0,
            ctx.hedging_leg.ask0,
            ctx.hedging_leg.ts,
        );

        // 7、使用信号层已对齐的量价（fr/xarb 侧完成对齐）
        let symbol = ctx.get_opening_symbol();

        if aligned_qty <= 0.0 || aligned_price <= 0.0 {
            error!(
                "HedgeArbStrategy: strategy_id={} 开仓信号量价无效 qty={:.8} price={:.8}，标记策略为不活跃",
                self.strategy_id, aligned_qty, aligned_price
            );
            self.mark_inactive("invalid_qty_or_price");
            return;
        }

        let venue = TradingVenue::from_u8(ctx.opening_leg.venue)
            .ok_or_else(|| format!("无效的交易场所: {}", ctx.opening_leg.venue))
            .unwrap();

        // 保存开仓侧交易场所
        self.open_venue = venue;

        if !venue.supports_pre_trade_stack() {
            panic!(
                "HedgeArbStrategy: strategy_id={} 不支持的交易场所 {:?}，仅支持 Binance/OKX/Bybit/Bitget/Gate 的 futures 或 margin",
                self.strategy_id,
                venue
            );
        }

        if let Err(err) = self.refresh_qty_multipliers(&symbol) {
            error!(
                "HedgeArbStrategy: strategy_id={} 初始化数量乘数失败: {}",
                self.strategy_id, err
            );
            self.mark_inactive(format!("qty_multiplier:{err}"));
            return;
        }

        let signed_qty = match open_side {
            Side::Buy => aligned_qty.abs(),
            Side::Sell => -aligned_qty.abs(),
        };

        if !force_close
            && venue == TradingVenue::BinanceMargin
            && MonitorChannel::instance()
                .order_manager()
                .borrow()
                .binance_is_standard()
        {
            let (base_asset, quote_asset) = Self::extract_assets_from_symbol(&symbol);
            let (check_asset, required_amount) = match open_side {
                Side::Buy => (quote_asset, aligned_qty * aligned_price),
                Side::Sell => (base_asset, aligned_qty),
            };
            let available_balance =
                MonitorChannel::instance().balance_position_for_venue(venue, &check_asset);
            if available_balance + 1e-12 < required_amount {
                if open_side != Side::Sell {
                    error!(
                        "HedgeArbStrategy: strategy_id={} BinanceMargin STANDARD 余额不足，拒绝开仓并标记策略不活跃 symbol={} side={:?} asset={} required={:.8} available={:.8}",
                        self.strategy_id,
                        symbol,
                        open_side,
                        check_asset,
                        required_amount,
                        available_balance
                    );
                }
                self.mark_inactive(format!(
                    "balance_insufficient:{} required={:.8} available={:.8}",
                    check_asset, required_amount, available_balance
                ));
                return;
            }
        }

        // 8、检查杠杆：若绝对持仓不增加，则可跳过
        if force_close {
            self.log_force_close_skip("杠杆", &ctx);
        } else {
            let add_base_qty = signed_qty * self.open_qty_multiplier;
            let current_base_qty = MonitorChannel::instance().get_position_qty(&symbol, venue);
            let projected_base_qty = current_base_qty + add_base_qty;
            let reduce_eps = 1e-12_f64;

            if projected_base_qty.abs() > current_base_qty.abs() + reduce_eps {
                if let Err(e) = MonitorChannel::instance().check_leverage() {
                    error!(
                        "HedgeArbStrategy: strategy_id={} 杠杆风控检查失败: {}，标记策略为不活跃",
                        self.strategy_id, e
                    );
                    self.mark_inactive(format!("leverage:{e}"));
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
            self.mark_inactive(format!("position_limit:{e}"));
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
                self.open_qty_multiplier,
                ts,
            );
        info!(
            "📤 开仓订单已创建: strategy_id={} order_id={} client_order_id={} symbol={} {:?} side={:?} qty={} price={}",
            self.strategy_id, order_id, client_order_id, symbol, venue,
            Side::from_u8(ctx.side).unwrap(),
            qv_decimal_or_fallback(aligned_qty),
            qv_decimal_or_fallback(aligned_price)
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
        if matches!(
            venue,
            TradingVenue::BinanceFutures | TradingVenue::OkexFutures | TradingVenue::GateFutures
        ) {
            if venue == TradingVenue::BinanceFutures {
                qty = raw_qty;
            } else {
                let contract_size =
                    table.contract_multiplier_opt(&symbol_key).ok_or_else(|| {
                        format!(
                            "symbol={} 缺少 {:?} 合约乘数，无法将 base qty 转成 contracts",
                            symbol_key, venue
                        )
                    })?;
                if contract_size <= 0.0 {
                    return Err(format!(
                        "symbol={} {:?} contract multiplier invalid: {}",
                        symbol_key, venue, contract_size
                    ));
                }
                qty = raw_qty / contract_size;
            }
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
        let target_qty = ctx.hedge_qty_value();
        if target_qty <= 0.0 {
            warn!(
                "HedgeArbStrategy: strategy_id={} 对冲信号的数量无效: {}",
                self.strategy_id, target_qty
            );
            return Err(format!("对冲数量无效: {}", target_qty));
        }

        let target_price = ctx.hedge_price_value();
        if ctx.is_maker() && target_price <= 0.0 {
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
                target_price,
                ctx.get_side().unwrap_or(self.hedge_side),
            );
            return Ok(());
        }

        if ctx.hedge_qty_count() <= 0 || (ctx.is_maker() && ctx.hedge_price_count() <= 0) {
            warn!(
                "HedgeArbStrategy: strategy_id={} 对冲信号 qv 计数无效 qty_count={} price_count={}",
                self.strategy_id,
                ctx.hedge_qty_count(),
                ctx.hedge_price_count()
            );
            return Err("对冲信号 qv 无效".to_string());
        }
        self.hedge_signal_ts = ctx.market_ts;
        self.hedge_price_offset = ctx.price_offset;
        self.hedge_from_key = String::from_utf8_lossy(&ctx.from_key).to_string();

        if self.has_pending_hedge_order() {
            debug!(
                "HedgeArbStrategy: strategy_id={} 对冲信号忽略（已有活跃对冲单） qty={:.8}",
                self.strategy_id, target_qty
            );
            return Ok(());
        }

        // 2. 获取对冲交易的 symbol 和 venue
        let hedge_symbol = ctx.get_hedging_symbol();
        let hedge_venue = TradingVenue::from_u8(ctx.hedging_leg.venue)
            .ok_or_else(|| format!("无效的对冲交易场所: {}", ctx.hedging_leg.venue))?;
        let hedge_side = ctx
            .get_side()
            .ok_or_else(|| format!("无效的对冲方向: {}", ctx.hedge_side))?;
        if ctx.is_taker() {
            debug!(
                "HedgeArbStrategy: strategy_id={} taker hedge signal hedge_symbol={} venue={:?} side={:?} qty={:.8}",
                self.strategy_id,
                hedge_symbol,
                hedge_venue,
                hedge_side,
                target_qty
            );
        }

        // 3. 使用信号层已对齐的量价
        let (aligned_qty, aligned_price) = if ctx.is_taker() {
            let aligned_qty = target_qty;
            (aligned_qty, 0.0)
        } else {
            let hedge_price = target_price;
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
            let (aligned_qty, aligned_price) = (target_qty, hedge_price);
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
                self.hedge_qty_multiplier,
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
            "HedgeArbStrategy: strategy_id={} 对冲订单创建成功 order_id={} symbol={} side={:?} qty={} price={} type={:?}",
            self.strategy_id,
            hedge_order_id,
            hedge_symbol,
            hedge_side,
            qv_decimal_or_fallback(aligned_qty),
            qv_decimal_or_fallback(aligned_price),
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
                        if order_type_str == "开仓" {
                            self.mark_inactive(format!("send_open_order:{e}"));
                        } else {
                            self.alive_flag = false;
                        }
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
                    if order_type_str == "开仓" {
                        self.mark_inactive(format!("open_order_bytes:{e}"));
                    } else {
                        self.alive_flag = false;
                    }
                    Err(format!("获取{}订单请求字节失败: {}", order_type_str, e))
                }
            }
        } else {
            error!(
                "HedgeArbStrategy: strategy_id={} symbol={} 未找到创建的{}订单 client_order_id={}",
                self.strategy_id, symbol, order_type_str, client_order_id
            );
            if order_type_str == "开仓" {
                self.mark_inactive(format!("missing_open_order:{client_order_id}"));
            } else {
                self.alive_flag = false;
            }
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
    fn handle_arb_cancel_signal(&mut self, ctx: ArbCancelCtx) -> Result<(), String> {
        self.last_open_cancel_reason = Some(ctx.get_reason().as_log_reason());
        if self.last_cancel_trigger_ts == Some(ctx.trigger_ts) {
            debug!(
                "HedgeArbStrategy: strategy_id={} 跳过重复 ArbCancel trigger_ts={} order_id={}",
                self.strategy_id, ctx.trigger_ts, self.open_order_id
            );
            return Ok(());
        }
        let order = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(self.open_order_id);
        if order
            .as_ref()
            .is_some_and(|order| order.status.is_terminal())
        {
            return Ok(());
        }
        if order.is_none() {
            warn!(
                "HedgeArbStrategy: strategy_id={} 未找到要撤销的订单 order_id={}",
                self.strategy_id, self.open_order_id
            );
            return Err(format!("未找到要撤销的订单"));
        }
        self.last_cancel_trigger_ts = Some(ctx.trigger_ts);
        self.handoff_open_order_to_arb_orphan(
            true,
            None,
            format!("arb_cancel:{}", ctx.get_reason().as_log_reason()),
        );
        self.alive_flag = false;
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
                    "HedgeArbStrategy: strategy_id={} 开仓订单超时，移交 arb orphan 撤单收敛 order_id={}",
                    self.strategy_id, self.open_order_id
                );
                self.last_open_cancel_reason = Some("timeout");
                self.open_expire_ts = None;
                self.handoff_open_order_to_arb_orphan(true, None, "open_timeout");
                self.alive_flag = false;
            }
        }
    }

    fn handle_hedge_leg_timeout(&mut self) {
        // 检查是否设置了对冲超时时间
        // 因此没有处理hedge信号之前，不会检查，会跳过
        if let Some(expire_ts) = self.hedge_expire_ts {
            let now = get_timestamp_us();
            if now >= expire_ts {
                debug!(
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
                        debug!(
                            "HedgeArbStrategy: strategy_id={} 对冲超时撤单目标 order_id={} symbol={} status={:?} exch_ord_id={:?}",
                            self.strategy_id,
                            hedge_order_id,
                            order.symbol,
                            order.status,
                            order.exchange_order_id
                        );
                        if order.status.is_terminal() {
                            debug!(
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
                                        debug!(
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
        let aligned_qty = self.align_taker_qty(self.hedge_venue, &self.hedge_symbol, eff_qty)?;
        let mut hedge_ctx = ArbHedgeCtx::new_taker(
            self.strategy_id,
            self.open_order_id,
            side.to_u8(),
            aligned_qty,
            0.0,
        );

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
        self.schedule_order_query_watchdog_with_delay(
            client_order_id,
            ORDER_QUERY_WATCHDOG_DELAY_US,
        );
    }

    fn schedule_order_query_watchdog_with_delay(&mut self, client_order_id: i64, delay_us: i64) {
        let due = get_timestamp_us().saturating_add(delay_us);
        self.order_query_watchdog = Some(QueryWatchdog {
            client_order_id,
            due_ts_us: due,
            reason: PendingOrderQueryReason::OrderWatchdog,
        });
    }

    fn schedule_cancel_query_watchdog(&mut self, client_order_id: i64) {
        let due = get_timestamp_us().saturating_add(CANCEL_QUERY_WATCHDOG_DELAY_US);
        self.cancel_query_watchdog = Some(QueryWatchdog {
            client_order_id,
            due_ts_us: due,
            reason: PendingOrderQueryReason::CancelWatchdog,
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
                    debug!(
                        "CancelWatchdog触发: strategy_id={} client_order_id={} leg={:?} symbol={} status={:?} exch_ord_id={:?} 等待{}ms仍未收到撤单/终态回报，移交 arb orphan reason={:?}",
                        self.strategy_id,
                        w.client_order_id,
                        leg,
                        order.symbol,
                        order.status,
                        order.exchange_order_id,
                        waited_ms,
                        w.reason
                    );
                    if leg == Some(Leg::Open) {
                        drop(order_opt);
                        self.handoff_open_order_to_arb_orphan(
                            true,
                            None,
                            format!("open_cancel_watchdog:{:?}", w.reason),
                        );
                        self.alive_flag = false;
                    } else {
                        drop(order_opt);
                        self.handoff_hedge_order_to_arb_orphan(
                            w.client_order_id,
                            true,
                            None,
                            format!("hedge_cancel_watchdog:{:?}", w.reason),
                        );
                    }
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
                    debug!(
                        "OrderWatchdog触发{}: strategy_id={} client_order_id={} leg={:?} symbol={} status={:?} exch_ord_id={:?} 等待{}ms仍未收到回报 (since_submit={}ms)",
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
                    if leg == Some(Leg::Open) {
                        drop(order_opt);
                        self.handoff_open_order_to_arb_orphan(
                            true,
                            Some(2),
                            "open_order_watchdog_no_response",
                        );
                        self.alive_flag = false;
                    } else {
                        drop(order_opt);
                        self.handoff_hedge_order_to_arb_orphan(
                            w.client_order_id,
                            true,
                            Some(2),
                            "hedge_order_watchdog_no_response",
                        );
                    }
                }
            }
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
        debug!(
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
        let payload = ArbBackwardQueryMsg::Hedge(query_msg).to_bytes();
        let send_result = SignalChannel::with(|ch| ch.publish_backward(&payload));

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

    /// 尝试对冲。
    ///
    /// # Arguments
    /// * `base_qty` - 当前策略待对冲量，base 口径
    ///
    /// # Returns
    /// (是否成功发起对冲, 实际对冲量, 转交给 arb orphan 的 residual 数量)
    fn try_hedge_with_residual(&mut self, base_qty: f64) -> (bool, f64, f64) {
        debug!(
            "HedgeArbStrategy: strategy_id={} 待对冲量: 基础={:.8}",
            self.strategy_id, base_qty
        );

        // 2. 检查是否满足最小交易要求
        let mut min_req_reason: Option<String> = None;
        let can_hedge = match Self::base_to_venue_qty(base_qty, self.hedge_qty_multiplier, "hedge")
            .and_then(|hedge_check_qty| {
                MonitorChannel::instance().check_min_trading_requirements(
                    self.hedge_venue,
                    &self.hedge_symbol,
                    hedge_check_qty,
                    None,
                )
            }) {
            Ok(_) => true,
            Err(e) => {
                min_req_reason = Some(e);
                false
            }
        };

        if !can_hedge {
            // 不满足最小交易要求，交给 arb orphan 的 QV residual 累积。
            if base_qty > 1e-12 {
                let reason = min_req_reason.as_deref().unwrap_or("unknown");
                self.queue_arb_orphan_residual(
                    self.hedge_side,
                    base_qty,
                    format!("hedge min requirement failed: {}", reason),
                );
                info!(
                    "HedgeArbStrategy: strategy_id={} 待对冲量={:.8} 不满足最小交易要求，转交 arb orphan residual reason={}",
                    self.strategy_id, base_qty, reason
                );
                return (false, 0.0, base_qty);
            }
            return (false, 0.0, 0.0);
        }

        // 3. 满足要求，发起对冲
        debug!(
            "HedgeArbStrategy: strategy_id={} 待对冲量={:.8} 满足最小交易要求，准备对冲",
            self.strategy_id, base_qty
        );

        let hedge_result: Result<(), String> = if self.hedge_timeout_us.is_some() {
            // MM 模式：使用限价单对冲
            debug!(
                "HedgeArbStrategy: strategy_id={} MM模式，使用限价单对冲",
                self.strategy_id
            );
            self.hedge_as_limit_order(self.hedge_side, base_qty)
        } else {
            // MT 模式：使用市价单对冲
            debug!(
                "HedgeArbStrategy: strategy_id={} MT模式，使用市价单对冲",
                self.strategy_id
            );
            self.hedge_as_market_order(self.hedge_side, base_qty)
        };

        match hedge_result {
            Ok(_) => {
                // 对冲成功
                debug!(
                    "HedgeArbStrategy: strategy_id={} 对冲信号已发送，数量={:.8}",
                    self.strategy_id, base_qty
                );
                (true, base_qty, 0.0)
            }
            Err(e) => {
                error!(
                    "HedgeArbStrategy: strategy_id={} 对冲失败: {}",
                    self.strategy_id, e
                );
                // 对冲失败，将数量交给 arb orphan residual。
                if base_qty > 1e-12 {
                    self.queue_arb_orphan_residual(
                        self.hedge_side,
                        base_qty,
                        "hedge submit failed",
                    );
                    (false, 0.0, base_qty)
                } else {
                    (false, 0.0, 0.0)
                }
            }
        }
    }

    fn process_open_leg_cancel(&mut self, cancel_update: &dyn OrderUpdate) {
        self.open_expire_ts = None;
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
            debug!(
                "HedgeArbStrategy: strategy_id={} 开仓撤单后对冲成功，对冲量={:.8}",
                self.strategy_id, hedged_qty
            );
        } else if residual_qty > 1e-12 {
            debug!(
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
            debug!(
                "HedgeArbStrategy: strategy_id={} 对冲撤单后重新对冲成功，对冲量={:.8}",
                self.strategy_id, hedged_qty
            );
        } else if residual_qty > 1e-12 {
            debug!(
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
                        trade.order_id()
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
                    debug!(
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
        debug!(
            "HedgeArbStrategy: strategy_id={} 对冲成交: 开仓量={:.8} 对冲量={:.8}",
            self.strategy_id, self.cumulative_open_qty, self.cumulative_hedged_qty
        );

        // MT 模式：检查是否还有未对冲的量，如果有且无法对冲则关闭策略
        if self.hedge_timeout_us.is_none() {
            // 计算剩余未对冲量
            let remaining_qty = self.cumulative_open_qty - self.cumulative_hedged_qty;

            debug!(
                "HedgeArbStrategy: strategy_id={} MT模式对冲成交后检查: 剩余={:.8}",
                self.strategy_id, remaining_qty
            );

            // 检查剩余量是否满足最小交易要求
            if remaining_qty > 1e-12 {
                let can_hedge =
                    Self::base_to_venue_qty(remaining_qty, self.hedge_qty_multiplier, "hedge")
                        .and_then(|hedge_check_qty| {
                            MonitorChannel::instance().check_min_trading_requirements(
                                self.hedge_venue,
                                &self.hedge_symbol,
                                hedge_check_qty,
                                None,
                            )
                        })
                        .is_ok();

                if !can_hedge {
                    // 剩余量不足以对冲，且没有待处理的对冲订单，关闭策略
                    if !self.has_pending_hedge_order() {
                        debug!(
                            "HedgeArbStrategy: strategy_id={} MT模式，剩余量={:.8} 不足以对冲，关闭策略",
                            self.strategy_id, remaining_qty
                        );
                        self.alive_flag = false;
                    }
                }
            }
        } else {
            // MM 模式：对冲侧成交需要区分成交状态
            if trade.order_status() == Some(OrderStatus::Filled) {
                // 完全成交即表示本次对冲已完成，直接关闭策略
                debug!(
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
    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) -> bool {
        let client_order_id = trade.client_order_id();
        self.clear_query_watchdogs(client_order_id);

        let cumulative_qty = trade.cumulative_filled_quantity();
        let cumulative_base_qty = {
            let order_mgr = MonitorChannel::instance().order_manager();
            let base_qty_opt = order_mgr
                .borrow()
                .venue_qty_to_base_by_order(client_order_id, cumulative_qty);
            base_qty_opt.unwrap_or_else(|| {
                warn!(
                    "HedgeArbStrategy: strategy_id={} 未找到订单数量乘数，按 1.0 处理 client_order_id={} cumulative_qty={:.8}",
                    self.strategy_id, client_order_id, cumulative_qty
                );
                cumulative_qty
            })
        };
        let trade_time = trade.trade_time();
        let event_time = trade.event_time();

        if let Some(status @ (OrderStatus::PartiallyFilled | OrderStatus::Filled)) =
            trade.order_status()
        {
            let order_mgr = MonitorChannel::instance().order_manager();
            let mut order_manager = order_mgr.borrow_mut();
            let Some(current_order) = order_manager.get(client_order_id) else {
                warn!(
                    "HedgeArbStrategy: strategy_id={} 未找到成交对应的订单 client_order_id={}",
                    self.strategy_id, client_order_id
                );
                return false;
            };
            let prev_cumulative_filled_qty = current_order.cumulative_filled_quantity;

            if OrderManager::should_skip_idempotent_trade_update(
                &current_order,
                status,
                trade.cumulative_filled_quantity(),
                trade.event_time(),
                "HedgeArbStrategy",
                self.strategy_id,
            )
            .is_some()
            {
                return false;
            }

            let updated = order_manager.update(client_order_id, |order| {
                order.cumulative_filled_quantity = cumulative_qty;
                order.set_filled_time(trade_time);
                order.set_exchange_order_id(trade.order_id());
                match status {
                    OrderStatus::Filled => {
                        order.status = OrderExecutionStatus::Filled;
                        order.set_end_time(event_time);
                    }
                    OrderStatus::PartiallyFilled => {
                        if !order.status.is_terminal() {
                            order.status = OrderExecutionStatus::Create;
                        }
                    }
                    _ => {}
                }
            });
            if !updated {
                warn!(
                    "HedgeArbStrategy: strategy_id={} 未找到成交对应的订单 client_order_id={}",
                    self.strategy_id, client_order_id
                );
                return false;
            }
            if let Some(order) = order_manager.get(client_order_id) {
                if let Some(ctx) = self.uniform_publish_ctx(&order) {
                    publish_arb_uniform_trade_order(
                        trade,
                        &order,
                        prev_cumulative_filled_qty,
                        status,
                        &ctx,
                        "HedgeArbStrategy",
                        self.strategy_id,
                    );
                }
            }
        }

        //1 根据client order id，判断是开仓成交，还是对冲成交, 更新开仓量或对冲量
        if client_order_id == self.open_order_id {
            // 开仓成交，更新累计开仓量, 打印成交量
            self.cumulative_open_qty = cumulative_base_qty;
            info!(
                "💰 开仓成交: strategy_id={} order_id={} symbol={} price={} cumulative={} | 已对冲={}",
                self.strategy_id, client_order_id, self.open_symbol,
                qv_decimal_or_fallback(trade.price()),
                qv_decimal_or_fallback(self.cumulative_open_qty),
                qv_decimal_or_fallback(self.cumulative_hedged_qty)
            );
            self.process_open_leg_trade(trade);
        } else if self.hedge_order_ids.contains(&client_order_id) {
            // 对冲侧成交，增加累计对冲量
            self.cumulative_hedged_qty = cumulative_base_qty;
            debug!(
                "🛡️ 对冲成交: strategy_id={} order_id={} symbol={} price={} | 开仓量={} 已对冲={}",
                self.strategy_id,
                client_order_id,
                self.hedge_symbol,
                qv_decimal_or_fallback(trade.price()),
                qv_decimal_or_fallback(self.cumulative_open_qty),
                qv_decimal_or_fallback(self.cumulative_hedged_qty)
            );
            self.process_hedge_leg_trade(trade);
        } else {
            // 非法成交，忽略
            warn!(
                "⚠️ 收到未知订单成交: strategy_id={} order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        }

        true
    }

    fn apply_order_update(&mut self, order_update: &dyn OrderUpdate) -> bool {
        //状态更新是通用部分，非成交只更新状态，不更新量
        let client_order_id = order_update.client_order_id();
        self.clear_query_watchdogs(client_order_id);
        let incoming_cum_qty = order_update.cumulative_filled_quantity();

        let order_mgr = MonitorChannel::instance().order_manager();
        let mut order_manager = order_mgr.borrow_mut();

        let Some(current_order) = order_manager.get(client_order_id) else {
            error!(
                "update failed {} {} {:?}",
                order_update.client_order_id(),
                order_update.order_id(),
                order_update.status()
            );
            return false;
        };
        let incoming_cum_base_qty = incoming_cum_qty * current_order.qty_multiplier;

        if OrderManager::should_skip_idempotent_order_update(
            &current_order,
            order_update.status(),
            order_update.order_id(),
            order_update.cumulative_filled_quantity(),
            "HedgeArbStrategy",
            self.strategy_id,
        )
        .is_some()
        {
            return false;
        }

        let prev_cumulative_filled_qty = current_order.cumulative_filled_quantity;
        let updated = order_manager.update(client_order_id, |order| {
            order.set_exchange_order_id(order_update.order_id());
            if incoming_cum_qty > order.cumulative_filled_quantity + 1e-12 {
                order.cumulative_filled_quantity = incoming_cum_qty;
                order.set_filled_time(order_update.event_time());
            }

            match order_update.status() {
                OrderStatus::New => {
                    if client_order_id == self.open_order_id && !self.alive_flag {
                        warn!(
                            "HedgeArbStrategy: strategy_id={} revive on delayed open NEW: client_order_id={} exchange_order_id={} symbol={}",
                            self.strategy_id,
                            client_order_id,
                            order_update.order_id(),
                            order.symbol
                        );
                        self.alive_flag = true;
                    }
                    order.status = OrderExecutionStatus::Create;
                    order.set_create_time(order_update.event_time());
                    info!(
                        "✅ 订单已挂单: strategy_id={} client_order_id={} exchange_order_id={} symbol={} side={:?} price={} qty={}",
                        self.strategy_id, client_order_id, order_update.order_id(),
                        order.symbol,
                        order.side,
                        qv_decimal_or_fallback(order.price),
                        qv_decimal_or_fallback(order.quantity)
                    );
                }
                OrderStatus::Canceled => {
                    order.status = OrderExecutionStatus::Cancelled;
                    order.set_end_time(order_update.event_time());
                    let cancel_reason = self.last_open_cancel_reason.unwrap_or("unknown");
                    info!(
                        "🚫 订单已撤销: strategy_id={} client_order_id={} symbol={} reason={} filled={}/{}",
                        self.strategy_id, client_order_id,
                        order.symbol,
                        cancel_reason,
                        qv_decimal_or_fallback(order.cumulative_filled_quantity),
                        qv_decimal_or_fallback(order.quantity)
                    );
                    self.last_open_cancel_reason = None;
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
                OrderStatus::Filled => {
                    order.status = OrderExecutionStatus::Filled;
                    order.set_filled_time(order_update.event_time());
                    order.set_end_time(order_update.event_time());
                    info!(
                        "✅ 订单已完全成交: strategy_id={} client_order_id={} exchange_order_id={} symbol={}",
                        self.strategy_id, client_order_id, order_update.order_id(), order.symbol
                    );
                }
                OrderStatus::PartiallyFilled => {
                    if !order.status.is_terminal() {
                        order.status = OrderExecutionStatus::Create;
                    }
                    debug!(
                        "订单部分成交: strategy_id={} client_order_id={} exchange_order_id={} symbol={} cumulative={:.8}",
                        self.strategy_id,
                        client_order_id,
                        order_update.order_id(),
                        order.symbol,
                        order.cumulative_filled_quantity
                    );
                }
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
            return false;
        }

        // 同步推进策略累计成交量（base 口径）：
        // 某些交易所/链路可能先到 order update，后到 trade update；
        // 这里先做一次上限推进，避免策略累计量滞后。
        if client_order_id == self.open_order_id {
            if incoming_cum_base_qty > self.cumulative_open_qty + 1e-12 {
                self.cumulative_open_qty = incoming_cum_base_qty;
            }
        } else if self.hedge_order_ids.contains(&client_order_id)
            && incoming_cum_base_qty > self.cumulative_hedged_qty + 1e-12
        {
            self.cumulative_hedged_qty = incoming_cum_base_qty;
        }

        if order_update.status() == OrderStatus::New {
            if let Some(order) = MonitorChannel::instance()
                .order_manager()
                .borrow()
                .get(client_order_id)
            {
                if let Some(ctx) = self.uniform_publish_ctx(&order) {
                    publish_arb_uniform_new_order(
                        order_update,
                        &order,
                        prev_cumulative_filled_qty,
                        &ctx,
                        "HedgeArbStrategy",
                        self.strategy_id,
                    );
                }
            }
        }

        if matches!(
            order_update.status(),
            OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::ExpiredInMatch
        ) {
            if let Some(order) = MonitorChannel::instance()
                .order_manager()
                .borrow()
                .get(client_order_id)
            {
                if let Some(ctx) = self.uniform_publish_ctx(&order) {
                    publish_arb_uniform_terminal_order(
                        order_update,
                        &order,
                        prev_cumulative_filled_qty,
                        &ctx,
                        "HedgeArbStrategy",
                        self.strategy_id,
                    );
                }
            }
        }

        if order_update.status() == OrderStatus::Canceled {
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

        true
    }

    fn uniform_publish_ctx(&self, order: &Order) -> Option<ArbUniformPublishCtx> {
        let leg = self.classify_leg(order.client_order_id)?;
        match leg {
            Leg::Open => Some(ArbUniformPublishCtx {
                signal_ts: self.open_signal_ts,
                from_key: format!("open|{}", self.open_from_key).into_bytes(),
                price_offset: self.open_price_offset,
            }),
            Leg::Hedge => Some(ArbUniformPublishCtx {
                signal_ts: self.hedge_signal_ts,
                from_key: format!("hedge|{}", self.hedge_from_key).into_bytes(),
                price_offset: self.hedge_price_offset,
            }),
        }
    }

    fn cleanup_strategy_orders(&mut self) {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };
        let mut mgr = order_mgr.borrow_mut();

        // 检查并清理开仓订单
        if self.open_order_id != 0 {
            if self
                .keep_local_on_drop_order_ids
                .contains(&self.open_order_id)
            {
                debug!(
                    "HedgeArbStrategy: strategy_id={} keep open order local after arb orphan handoff client_order_id={}",
                    self.strategy_id, self.open_order_id
                );
            } else if let Some(order) = mgr.get(self.open_order_id) {
                if !order.status.is_terminal() {
                    mgr.log_order_details(&order, "开仓订单未达到终结状态被清理", self.strategy_id);
                }
                let _ = mgr.remove(self.open_order_id);
            }
        }

        // 检查并清理对冲订单
        for id in &self.hedge_order_ids {
            if self.keep_local_on_drop_order_ids.contains(id) {
                debug!(
                    "HedgeArbStrategy: strategy_id={} keep hedge order local after arb orphan handoff client_order_id={}",
                    self.strategy_id, id
                );
                continue;
            }
            if let Some(order) = mgr.get(*id) {
                if !order.status.is_terminal() {
                    mgr.log_order_details(&order, "对冲订单未达到终结状态被清理", self.strategy_id);
                }
            }
            let _ = mgr.remove(*id);
        }
    }

    fn terminalize_open_order_before_cleanup(&mut self, client_order_id: i64) {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };
        let event_time = get_timestamp_us();
        let _ = order_mgr.borrow_mut().update(client_order_id, |order| {
            if order.status.is_terminal() {
                return;
            }
            order.status = OrderExecutionStatus::Rejected;
            order.set_end_time(event_time);
        });
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

    fn try_apply_ws_order_update(&mut self, response: &dyn TradeEngineResponse) -> bool {
        if !WsOrderUpdate::supports_trade_response_req_type(response.req_type()) {
            return false;
        }

        let client_order_id = response.client_order_id();
        if self.classify_leg(client_order_id).is_none() {
            return false;
        }

        let order_mgr = MonitorChannel::instance().order_manager();
        let Some(order_snapshot) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "HedgeArbStrategy: strategy_id={} ws order update missing local order: client_order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        };
        let order_snapshot = order_snapshot.clone();

        let Some(update) = WsOrderUpdate::from_trade_response(response, &order_snapshot) else {
            return false;
        };

        if matches!(
            order_snapshot.venue,
            TradingVenue::BinanceMargin | TradingVenue::BinanceFutures
        ) {
            if matches!(update.status(), OrderStatus::New | OrderStatus::Canceled) {
                <Self as Strategy>::apply_order_update(self, &update);
            } else {
                debug!(
                    "HedgeArbStrategy: strategy_id={} skip non-NEW/CANCELED binance ws response: venue={:?} client_order_id={} status={:?}",
                    self.strategy_id,
                    order_snapshot.venue,
                    client_order_id,
                    update.status()
                );
            }
            return true;
        }

        false
    }

    fn handle_open_leg_open_failed(
        &mut self,
        response: &dyn TradeEngineResponse,
        code_desc: &str,
        client_order_id: i64,
    ) {
        let open_side = if self.hedge_side == Side::Buy {
            Side::Sell
        } else {
            Side::Buy
        };
        let _ = register_signal_throttle(
            &self.open_symbol,
            open_side,
            response.exchange_enum(),
            response.error_code(),
        );

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
        // 3、开仓失败本地标记为拒绝，避免 cleanup 将预提交的本地订单误报为未终结
        self.clear_query_watchdogs(client_order_id);
        self.terminalize_open_order_before_cleanup(client_order_id);
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
        self.clear_query_watchdogs(client_order_id);
        self.queue_arb_orphan_handoff(
            client_order_id,
            ArbOrphanLeg::Open,
            true,
            None,
            format!(
                "open_cancel_failed req_type={} status={} code={}({}) not_cancellable={}",
                response.req_type(),
                response.status(),
                response.error_code(),
                code_desc,
                response.is_cancel_not_cancellable()
            ),
        );
        self.alive_flag = false;
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
        //1、post only rejected是正常的，因为盘口出现穿价是正常情况，立即再次报单即可
        let is_post_only_rejected = response.is_post_only_rejected();
        let is_price_limit_rejected = response.is_price_limit_rejected();
        let is_order_placement_failed = response.is_order_placement_failed();
        let is_insufficient_margin = response.is_insufficient_margin();

        let reason = if is_post_only_rejected {
            "post_only"
        } else if is_price_limit_rejected {
            "price_limit"
        } else if is_order_placement_failed {
            "order_placement_failed"
        } else if is_insufficient_margin {
            "insufficient_margin"
        } else {
            "other"
        };

        if reason == "other" {
            debug!(
                "HedgeArbStrategy: strategy_id={} hedge_leg open_failed(other): req_type={} status={} code={}({}) client_order_id={}",
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

        self.cleanup_failed_hedge_order(client_order_id);
        let base_pending_qty = self.cumulative_open_qty - self.cumulative_hedged_qty;
        if is_post_only_rejected {
            let (sent, hedged_qty, _) = self.try_hedge_with_residual(base_pending_qty);
            if sent {
                debug!(
                    "HedgeArbStrategy: strategy_id={} hedge immediate re-send after post_only open_failed, qty={:.8}",
                    self.strategy_id, hedged_qty
                );
            } else {
                warn!(
                    "HedgeArbStrategy: strategy_id={} hedge immediate re-send skipped/failed after post_only open_failed, pending_qty={:.8}",
                    self.strategy_id, base_pending_qty
                );
            }
            return;
        }

        self.push_hedge_residual_with_print(
            reason,
            &format!(
                "req_type={} status={} code={}({}) client_order_id={} pending_qty={:.8}",
                response.req_type(),
                response.status(),
                response.error_code(),
                code_desc,
                client_order_id,
                base_pending_qty
            ),
            base_pending_qty,
            0.0,
            self.hedge_side,
        );
    }

    // 撤单失败后将对冲单移交 arb orphan，由 orphan 统一 query/cancel 收敛。
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
        self.clear_query_watchdogs(client_order_id);
        self.handoff_hedge_order_to_arb_orphan(
            client_order_id,
            true,
            None,
            format!(
                "hedge_cancel_failed req_type={} status={} code={}({}) not_cancellable={}",
                response.req_type(),
                response.status(),
                response.error_code(),
                code_desc,
                response.is_cancel_not_cancellable()
            ),
        );
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

    fn arb_open_price_map_entry(&self) -> Option<ArbOpenPriceMapEntry> {
        if self.open_symbol.is_empty() || self.open_order_id == 0 {
            return None;
        }
        let open_active = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(self.open_order_id)
            .is_some_and(|order| !order.status.is_terminal());
        if !open_active {
            return None;
        }
        Some(ArbOpenPriceMapEntry {
            symbol: self.open_symbol.clone(),
            client_order_id: self.open_order_id,
            price_qv: self.open_price_qv.into(),
        })
    }

    fn is_strategy_order(&self, order_id: i64) -> bool {
        Self::extract_strategy_id(order_id) == self.strategy_id
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        HedgeArbStrategy::handle_signal(self, signal);
    }

    fn apply_order_update(&mut self, update: &dyn OrderUpdate) {
        let should_persist = HedgeArbStrategy::apply_order_update(self, update);

        // 持久化订单更新记录
        if should_persist {
            PersistChannel::with(|ch| ch.publish_order_update(update));
        }
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) {
        let should_persist = HedgeArbStrategy::apply_trade_update(self, trade);

        // 持久化成交记录
        if should_persist {
            PersistChannel::with(|ch| ch.publish_trade_update(trade));
        }
    }

    fn apply_trade_engine_response(&mut self, response: &dyn TradeEngineResponse) {
        if self.try_apply_ws_order_update(response) {
            return;
        }

        // 1、is_request_success表示http200 + error code为0，这种不处理
        // 2、成功回包默认等待 account monitor 推送；推送缺失时由 query 回补
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

    fn apply_query_engine_response(&mut self, _response: &dyn QueryEngineResponse) {}

    fn drain_pending_arb_orphan_handoffs(&mut self) -> Vec<ArbOrphanHandoff> {
        std::mem::take(&mut self.pending_arb_orphan_handoffs)
    }

    fn drain_pending_arb_orphan_residuals(&mut self) -> Vec<ArbOrphanResidualHandoff> {
        std::mem::take(&mut self.pending_arb_orphan_residuals)
    }

    fn handle_period_clock(&mut self, _current_tp: i64) {
        // 周期性检查开仓和对冲订单的超时情况
        if self.is_active() {
            self.handle_open_leg_timeout();
            self.handle_hedge_leg_timeout();
            self.handle_query_watchdogs();
        }
    }

    fn is_active(&self) -> bool {
        self.alive_flag
    }
}
