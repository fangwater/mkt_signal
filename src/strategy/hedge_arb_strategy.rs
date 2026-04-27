use crate::common::bbo::{Bbo, DualBbo};
use crate::common::tick_math::QuantizedValue;
use crate::common::time_util::get_timestamp_us;
use crate::common::trade_error_code::{
    describe_non_retryable_order_error, describe_trade_error_code,
};
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus, OrderManager, OrderType, Side};
use crate::pre_trade::signal_throttle::register_signal_throttle;
use crate::pre_trade::{PersistChannel, SignalChannel, TradeEngHub};
use crate::signal::arb_signal::ArbBackwardQueryMsg;
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::common::{OrderStatus, SignalBytes, TradingVenue};
use crate::signal::hedge_signal::ArbHedgeCtx;
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::arb_check::OpenCheckContext;
use crate::strategy::arb_helper::{
    align_taker_qty, arb_cancel_log_reason, base_to_venue_qty, create_and_send_order,
    panic_invalid_maker_hedge_price, parse_arb_open_signal_params,
};
use crate::strategy::arb_orphan_strategy::ArbOrphanLeg;
use crate::strategy::manager::{
    ArbOrphanHandoff, ArbOrphanResidualHandoff, ForceCloseControl, OpenPriceMapEntry, Strategy,
};
use crate::strategy::order_reconcile::{qv_decimal_or_fallback, ORDER_QUERY_WATCHDOG_DELAY_US};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_engine_response::{TradeEngineResponse, TradeRequestKind};
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::uniform_order_helper::{
    publish_uniform_new_order, publish_uniform_terminal_order, publish_uniform_trade_order,
    UniformPublishCtx,
};
use crate::strategy::ws_order_update::try_apply_ws_order_update_for_strategy;
use log::{debug, error, info, warn};
use std::any::Any;

// 下单后若迟迟收不到 account monitor 的推送（New/Filled 等），触发兜底。
// Open/hedge leg 进入不确定状态后都移交 arb orphan 统一 query/cancel 收敛。
const CANCEL_QUERY_WATCHDOG_DELAY_US: i64 = 300_000;
const HEDGE_RESIDUAL_EPS: f64 = 1e-12;

pub struct HedgeArbStrategy {
    pub strategy_id: i32,                  //策略id
    pub open_symbol: String,               //开仓侧symbol
    pub open_venue: TradingVenue,          //开仓侧交易场所
    pub open_order_id: i64,                //开仓单唯一，报多单对应多个Strategy
    pub hedge_order_id: Option<i64>,       //当前对冲单；策略任意时刻只跟踪一个对冲单
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
    pub last_open_cancel_reason: Option<&'static str>,
    order_query_watchdog: Option<QueryWatchdog>,
    cancel_query_watchdog: Option<QueryWatchdog>,
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
    // 根据 leg 获取 uniform publish context，提供给 arb orphan
    // 保证后续可以补完from key
    fn uniform_publish_ctx_for_leg(&self, leg: Leg) -> UniformPublishCtx {
        match leg {
            Leg::Open => UniformPublishCtx {
                signal_ts: self.open_signal_ts,
                from_key: self.open_from_key.clone().into_bytes(),
                price_offset: self.open_price_offset,
            },
            Leg::Hedge => UniformPublishCtx {
                signal_ts: self.hedge_signal_ts,
                from_key: self.hedge_from_key.clone().into_bytes(),
                price_offset: self.hedge_price_offset,
            },
        }
    }

    // 将订单移交给 arb orphan 策略，后续 cancel/query 收敛完全由 orphan 自行决定。
    fn handoff_order_to_arb_orphan(&mut self, client_order_id: i64, leg: ArbOrphanLeg) -> bool {
        // 订单ID无效时不进行移交
        if client_order_id <= 0 {
            return false;
        }
        // 构造移交信息，尝试移交给 orphan manager
        let uniform_ctx = Some(match leg {
            ArbOrphanLeg::Open => self.uniform_publish_ctx_for_leg(Leg::Open),
            ArbOrphanLeg::Hedge => self.uniform_publish_ctx_for_leg(Leg::Hedge),
        });
        // 这里的 source_strategy_id 是为了让 orphan manager 能够记录来源，便于后续查询和监控
        let handoff = ArbOrphanHandoff {
            client_order_id,
            source_strategy_id: self.strategy_id,
            leg,
            uniform_ctx,
        };
        // 尝试移交给 orphan manager，移交成功后根据 leg 清理对应的订单信息
        let Some(orphan_mgr) = MonitorChannel::try_orphan_strategy_mgr() else {
            warn!(
                "HedgeArbStrategy: strategy_id={} arb orphan manager unavailable client_order_id={} leg={:?}",
                self.strategy_id, client_order_id, leg
            );
            return false;
        };
        let adopted = orphan_mgr.borrow_mut().adopt_arb_orphan_order_id(&handoff);
        if !adopted {
            warn!(
                "HedgeArbStrategy: strategy_id={} arb orphan handoff rejected client_order_id={} leg={:?}",
                self.strategy_id, client_order_id, leg
            );
            return false;
        }
        match leg {
            // 移交成功后清理订单信息，避免重复移交
            ArbOrphanLeg::Open if self.open_order_id == client_order_id => {
                self.clear_query_watchdogs(client_order_id);
                self.open_order_id = 0;
            }
            ArbOrphanLeg::Hedge if self.hedge_order_id == Some(client_order_id) => {
                self.clear_query_watchdogs(client_order_id);
                self.hedge_order_id = None;
            }
            _ => {}
        }
        info!(
            "HedgeArbStrategy: strategy_id={} handoff order to arb orphan adopted client_order_id={} leg={:?}",
            self.strategy_id,
            client_order_id,
            leg
        );
        true
    }

    // 将对冲残值移交给 arb orphan 策略，尝试让其找到后续的对冲机会进行消化，避免残值长期暴露在市场上
    // hedge 侧失败/不足时才会执行，相当于当前策略放弃对冲，转而让 orphan 策略接手残值的对冲责任
    fn handoff_arb_orphan_residual(&mut self, side: Side, base_qty: f64) -> bool {
        if base_qty <= HEDGE_RESIDUAL_EPS || self.hedge_symbol.is_empty() {
            return false;
        }
        let signed_base_qty = match side {
            // 卖出对冲说明开仓侧是正 base exposure，残值按正数累计。
            Side::Sell => base_qty,
            // 买入对冲说明开仓侧是负 base exposure，残值按负数累计。
            Side::Buy => -base_qty,
        };
        let residual = ArbOrphanResidualHandoff {
            symbol: self.hedge_symbol.clone(),
            venue: self.hedge_venue,
            signed_base_qty,
            source_strategy_id: self.strategy_id,
        };
        let Some(orphan_mgr) = MonitorChannel::try_orphan_strategy_mgr() else {
            warn!(
                "HedgeArbStrategy: strategy_id={} arb orphan manager unavailable for residual symbol={}",
                self.strategy_id, self.hedge_symbol
            );
            return false;
        };
        let adopted = orphan_mgr.borrow_mut().adopt_arb_orphan_residual(&residual);
        if !adopted {
            warn!(
                "HedgeArbStrategy: strategy_id={} arb orphan residual rejected symbol={}",
                self.strategy_id, self.hedge_symbol
            );
            return false;
        }
        info!(
            "HedgeArbStrategy: strategy_id={} handoff arb orphan residual symbol={} venue={:?} side={:?} base_qty={:.8} signed_base_qty={:.8}",
            self.strategy_id,
            self.hedge_symbol,
            self.hedge_venue,
            side,
            base_qty,
            signed_base_qty
        );
        true
    }

    pub fn new(id: i32, symbol: String) -> Self {
        let strategy = Self {
            strategy_id: id,
            open_symbol: symbol,
            open_venue: TradingVenue::BinanceMargin, // 默认值，将在开仓时更新
            open_order_id: 0,
            hedge_order_id: None,
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
            last_open_cancel_reason: None,
            order_query_watchdog: None,
            cancel_query_watchdog: None,
        };
        strategy
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
        // 解析开仓信号参数；参数无效时直接标记策略不活跃。
        let Some(params) = parse_arb_open_signal_params(self.strategy_id, &ctx) else {
            self.alive_flag = false;
            return;
        };
        // 统一执行开仓前检查；检查链会顺带产出下单和状态保存需要的派生量。
        let Some(open_check) = OpenCheckContext::from_open_params(
            self.strategy_id,
            &self.open_symbol,
            force_close,
            &ctx,
            &params,
        )
        .run_all() else {
            self.alive_flag = false;
            return;
        };

        // 通过全部开仓前检查后，生成订单ID并保存策略状态。
        self.order_seq += 1;
        let order_id = Self::compose_order_id(self.strategy_id, self.order_seq);
        self.open_order_id = order_id;
        self.cumulative_open_qty = 0.0;
        self.alive_flag = true;
        self.open_expire_ts = (ctx.exp_time > 0).then_some(ctx.exp_time);
        // Hedge timeout 为 0 表示 MT 模式，>0 为 MM 模式，保存原始 TTL，实际挂单后再换算绝对时间
        self.hedge_timeout_us = (ctx.hedge_timeout_us > 0).then_some(ctx.hedge_timeout_us);
        self.hedge_expire_ts = None;
        self.open_venue = params.venue;
        self.hedge_symbol = params.hedge_symbol.clone();
        self.hedge_venue = params.hedge_venue;
        self.hedge_side = open_check.hedge_side;
        self.open_qty_multiplier = open_check.open_qty_multiplier;
        self.hedge_qty_multiplier = open_check.hedge_qty_multiplier;
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

        // 用修正后的量价在 order manager 中创建开仓订单。
        let ts = get_timestamp_us();
        let client_order_id = MonitorChannel::instance()
            .order_manager()
            .borrow_mut()
            .create_order(
                params.venue,
                order_id,
                params.order_type,
                params.symbol.clone(),
                params.open_side,
                params.aligned_qty,
                params.aligned_price,
                force_close,
                open_check.open_qty_multiplier,
                ts,
            );
        info!(
            "📤 开仓订单已创建: strategy_id={} order_id={} client_order_id={} symbol={} {:?} side={:?} qty={} price={}",
            self.strategy_id, order_id, client_order_id, params.symbol, params.venue, params.open_side,
            qv_decimal_or_fallback(params.aligned_qty),
            qv_decimal_or_fallback(params.aligned_price)
        );

        // 推送开仓订单到交易引擎。
        // 注意：HedgeArb open leg 不记录 OrderRateBucket::ArbOpen。ArbOpen bucket
        // 只用于单腿 ArbOpenStrategy；这里是双腿 HedgeArb 生命周期的一部分，保持独立语义。
        if let Err(e) =
            create_and_send_order(self.strategy_id, client_order_id, "开仓", &params.symbol)
        {
            self.alive_flag = false;
            error!(
                "❌ 开仓订单发送失败: strategy_id={} {}",
                self.strategy_id, e
            );
            return;
        }
        self.schedule_order_query_watchdog(client_order_id);
        info!(
            "✅ 开仓订单已发送: strategy_id={} client_order_id={}",
            self.strategy_id, client_order_id
        );
    }

    // 收到对冲信号，按照需求进行maker对冲，或者直接taker对冲
    fn handle_arb_hedge_signal(&mut self, ctx: ArbHedgeCtx) -> Result<(), String> {
        // 1. 确定对冲数量
        let is_maker = ctx.is_maker();
        let is_taker = ctx.is_taker();
        let target_qty = ctx.hedge_qty_value();
        let target_price = ctx.hedge_price_value();

        if target_qty <= 0.0 {
            warn!(
                "HedgeArbStrategy: strategy_id={} 对冲信号的数量无效: {}",
                self.strategy_id, target_qty
            );
            return Err(format!("对冲数量无效: {}", target_qty));
        }

        if is_maker && target_price <= 0.0 {
            panic_invalid_maker_hedge_price(
                self.strategy_id,
                &self.hedge_symbol,
                self.hedge_side,
                &ctx,
                "limit_price_invalid",
                target_qty,
                target_price,
            );
        }

        if ctx.hedge_qty_count() <= 0 || (is_maker && ctx.hedge_price_count() <= 0) {
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
        if is_taker {
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
        let aligned_qty = target_qty;
        let aligned_price = if is_taker {
            0.0
        } else {
            if target_price <= 0.0 {
                return Err(format!("对冲价格无效: {:.8}", target_price));
            }
            target_price
        };

        // 4. 检查最小交易要求
        if !is_taker {
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
        let order_type = if is_maker {
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
        // 7. 记录当前对冲订单ID
        self.hedge_order_id = Some(hedge_order_id);
        // 对冲量只有实质成交才会更新，挂单不更新

        // 8. 推送对冲订单到交易引擎
        if let Err(e) = create_and_send_order(
            self.strategy_id,
            hedge_client_order_id,
            "对冲",
            &hedge_symbol,
        ) {
            self.alive_flag = false;
            return Err(e);
        }
        self.schedule_order_query_watchdog(hedge_client_order_id);

        // 9. 设置对冲挂单的截止时间（仅 maker 单需要）
        self.hedge_expire_ts = is_maker.then_some(ctx.exp_time);

        info!(
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

    fn handle_arb_cancel_signal(&mut self, ctx: ArbCancelCtx) -> Result<(), String> {
        let cancel_reason = arb_cancel_log_reason(&ctx);
        self.cancel_open_order(cancel_reason)
    }

    fn cancel_open_order(&mut self, cancel_reason: &'static str) -> Result<(), String> {
        let order = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(self.open_order_id);

        // 开仓单已终态时不再发送撤单，避免 trade engine 把无效 client order id
        // 当作报单失败处理，从而错误关闭已经进入对冲阶段的策略。
        if order
            .as_ref()
            .is_some_and(|order| order.status.is_terminal())
        {
            debug!(
                "HedgeArbStrategy: strategy_id={} ArbCancel ignored because open order is terminal order_id={} reason={}",
                self.strategy_id, self.open_order_id, cancel_reason
            );
            return Ok(());
        }

        let Some(order) = order else {
            warn!(
                "HedgeArbStrategy: strategy_id={} 未找到要撤销的订单 order_id={}",
                self.strategy_id, self.open_order_id
            );
            return Err("未找到要撤销的订单".to_string());
        };

        // 只有挂单中或部分成交的非终态开仓单才需要构造撤单请求；
        // 实际撤销结果仍以 account monitor 后续回报为准。
        let client_order_id = order.client_order_id;
        let exchange = order.venue.trade_engine_exchange();
        let cancel_bytes = order.get_order_cancel_bytes().map_err(|err| {
            format!(
                "获取开仓撤单请求字节失败 order_id={} err={}",
                self.open_order_id, err
            )
        })?;

        TradeEngHub::publish_order_request(exchange, &cancel_bytes)
            .map_err(|err| format!("发送开仓撤单请求失败: {err}"))?;
        self.schedule_cancel_query_watchdog(client_order_id);
        self.last_open_cancel_reason = Some(cancel_reason);
        info!(
            "HedgeArbStrategy: strategy_id={} sent open cancel order_id={} exchange={} cancel_reason={}",
            self.strategy_id, self.open_order_id, exchange, cancel_reason
        );
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

    // 处理开仓侧超过最长挂单时间。timeout 是正常撤单场景，不直接移交 orphan；
    // 若撤单后迟迟没有终态回报，再由 cancel watchdog 进入 orphan 收敛。
    fn handle_open_leg_timeout(&mut self) {
        // 检查是否设置了超时时间，并且已经超时
        if let Some(expire_ts) = self.open_expire_ts {
            let now = get_timestamp_us();
            if now >= expire_ts && self.alive_flag && self.open_order_id != 0 {
                info!(
                    "HedgeArbStrategy: strategy_id={} 开仓订单超时，发送正常撤单 order_id={}",
                    self.strategy_id, self.open_order_id
                );
                self.open_expire_ts = None;
                if let Err(err) = self.cancel_open_order("timeout") {
                    warn!(
                        "HedgeArbStrategy: strategy_id={} 开仓订单超时撤单失败 order_id={} err={}",
                        self.strategy_id, self.open_order_id, err
                    );
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
                debug!(
                    "HedgeArbStrategy: strategy_id={} 对冲订单超时，直接撤单 expire_ts={} now={} hedge_order_id={:?}",
                    self.strategy_id,
                    expire_ts,
                    now,
                    self.hedge_order_id
                );

                if let Some(hedge_order_id) = self.hedge_order_id {
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

    // 当前策略只允许一个活跃对冲单。
    fn has_pending_hedge_order(&self) -> bool {
        if let Some(hedge_order_id) = self.hedge_order_id {
            let order = MonitorChannel::instance()
                .order_manager()
                .borrow()
                .get(hedge_order_id);
            if let Some(order) = order {
                return !order.status.is_terminal();
            }
        }
        false
    }

    fn is_hedge_order(&self, client_order_id: i64) -> bool {
        self.hedge_order_id == Some(client_order_id)
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
        let aligned_qty = align_taker_qty(self.hedge_venue, &self.hedge_symbol, eff_qty)?;
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
                        info!(
                            "HedgeArbStrategy: strategy_id={} handoff open order to arb orphan after cancel watchdog order_id={} watchdog_reason={:?}",
                            self.strategy_id,
                            w.client_order_id,
                            w.reason
                        );
                        if self.handoff_order_to_arb_orphan(w.client_order_id, ArbOrphanLeg::Open) {
                            self.alive_flag = false;
                        }
                    } else {
                        drop(order_opt);
                        info!(
                            "HedgeArbStrategy: strategy_id={} handoff hedge order to arb orphan after cancel watchdog order_id={} watchdog_reason={:?}",
                            self.strategy_id,
                            w.client_order_id,
                            w.reason
                        );
                        if self.handoff_order_to_arb_orphan(w.client_order_id, ArbOrphanLeg::Hedge)
                        {
                            self.alive_flag = false;
                        }
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
                        info!(
                            "HedgeArbStrategy: strategy_id={} handoff open order to arb orphan after order watchdog no response order_id={}",
                            self.strategy_id,
                            w.client_order_id
                        );
                        if self.handoff_order_to_arb_orphan(w.client_order_id, ArbOrphanLeg::Open) {
                            self.alive_flag = false;
                        }
                    } else {
                        drop(order_opt);
                        info!(
                            "HedgeArbStrategy: strategy_id={} handoff hedge order to arb orphan after order watchdog no response order_id={}",
                            self.strategy_id,
                            w.client_order_id
                        );
                        if self.handoff_order_to_arb_orphan(w.client_order_id, ArbOrphanLeg::Hedge)
                        {
                            self.alive_flag = false;
                        }
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
        let can_hedge = match base_to_venue_qty(base_qty, self.hedge_qty_multiplier, "hedge")
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
                info!(
                    "HedgeArbStrategy: strategy_id={} handoff arb orphan residual because hedge min requirement failed hedge_symbol={} qty={:.8} reason={}",
                    self.strategy_id,
                    self.hedge_symbol,
                    base_qty,
                    reason
                );
                let _ = self.handoff_arb_orphan_residual(self.hedge_side, base_qty);
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
                    warn!(
                        "HedgeArbStrategy: strategy_id={} handoff arb orphan residual because hedge submit failed hedge_symbol={} qty={:.8}",
                        self.strategy_id,
                        self.hedge_symbol,
                        base_qty
                    );
                    let _ = self.handoff_arb_orphan_residual(self.hedge_side, base_qty);
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
                    base_to_venue_qty(remaining_qty, self.hedge_qty_multiplier, "hedge")
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
                    publish_uniform_trade_order(
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
        } else if self.is_hedge_order(client_order_id) {
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
        } else if self.is_hedge_order(client_order_id)
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
                    publish_uniform_new_order(
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
                    publish_uniform_terminal_order(
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
                && self.is_hedge_order(order_update.client_order_id())
            {
                self.process_hedge_leg_cancel(order_update);
            }
        }

        true
    }

    fn uniform_publish_ctx(&self, order: &Order) -> Option<UniformPublishCtx> {
        let leg = self.classify_leg(order.client_order_id)?;
        Some(self.uniform_publish_ctx_for_leg(leg))
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
                let _ = mgr.remove(self.open_order_id);
            }
        }

        // 检查并清理当前对冲订单
        if let Some(id) = self.hedge_order_id {
            if let Some(order) = mgr.get(id) {
                if !order.status.is_terminal() {
                    mgr.log_order_details(&order, "对冲订单未达到终结状态被清理", self.strategy_id);
                }
            }
            let _ = mgr.remove(id);
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
        } else if self.is_hedge_order(client_order_id) {
            Some(Leg::Hedge)
        } else {
            None
        }
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
        self.clear_query_watchdogs(client_order_id);
        warn!(
            "HedgeArbStrategy: strategy_id={} open_leg cancel_failed client_order_id={} req_type={} status={} code={}({}) not_cancellable={}; keep strategy local",
            self.strategy_id,
            client_order_id,
            response.req_type(),
            response.status(),
            response.error_code(),
            code_desc,
            response.is_cancel_not_cancellable()
        );
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
        if self.hedge_order_id == Some(client_order_id) {
            self.hedge_order_id = None;
        }
        self.hedge_expire_ts = None;
    }

    fn handle_hedge_leg_open_failed(
        &mut self,
        response: &dyn TradeEngineResponse,
        code_desc: &str,
        client_order_id: i64,
    ) {
        let error_code = response.error_code();
        if let Some(reason) = response
            .exchange_enum()
            .and_then(|exchange| describe_non_retryable_order_error(exchange, error_code))
        {
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
            let residual_add = if pending_qty > HEDGE_RESIDUAL_EPS {
                info!(
                    "HedgeArbStrategy: strategy_id={} handoff arb orphan residual due to hedge reject reason={} hedge_symbol={} side={:?} qty={:.8} price={:.8}",
                    self.strategy_id,
                    reason,
                    self.hedge_symbol,
                    order_side,
                    pending_qty,
                    order_price
                );
                let _ = self.handoff_arb_orphan_residual(order_side, pending_qty);
                pending_qty
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
                order_side,
                pending_qty,
                order_price,
                residual_add,
                detail
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

        let residual_add = if base_pending_qty > HEDGE_RESIDUAL_EPS {
            info!(
                "HedgeArbStrategy: strategy_id={} handoff arb orphan residual due to hedge reject reason={} hedge_symbol={} side={:?} qty={:.8} price={:.8}",
                self.strategy_id,
                reason,
                self.hedge_symbol,
                self.hedge_side,
                base_pending_qty,
                0.0
            );
            let _ = self.handoff_arb_orphan_residual(self.hedge_side, base_pending_qty);
            base_pending_qty
        } else {
            0.0
        };
        let detail = format!(
            "req_type={} status={} code={}({}) client_order_id={} pending_qty={:.8}",
            response.req_type(),
            response.status(),
            response.error_code(),
            code_desc,
            client_order_id,
            base_pending_qty
        );
        println!(
            "HedgeArbStrategy: strategy_id={} 对冲拒绝({}) | open_symbol={} hedge_symbol={} hedge_venue={:?} side={:?} qty={:.8} price={:.8} residual_add={:.8} detail={}",
            self.strategy_id,
            reason,
            self.open_symbol,
            self.hedge_symbol,
            self.hedge_venue,
            self.hedge_side,
            base_pending_qty,
            0.0,
            residual_add,
            detail
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
        warn!(
            "HedgeArbStrategy: strategy_id={} handoff hedge order to arb orphan after cancel failed client_order_id={} req_type={} status={} code={}({}) not_cancellable={}",
            self.strategy_id,
            client_order_id,
            response.req_type(),
            response.status(),
            response.error_code(),
            code_desc,
            response.is_cancel_not_cancellable()
        );
        if self.handoff_order_to_arb_orphan(client_order_id, ArbOrphanLeg::Hedge) {
            self.alive_flag = false;
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

    fn arb_open_price_map_entry(&self) -> Option<OpenPriceMapEntry> {
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
        let open_side = if self.hedge_side == Side::Buy {
            Side::Sell
        } else {
            Side::Buy
        };
        Some(OpenPriceMapEntry {
            symbol: self.open_symbol.clone(),
            side: open_side,
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
        if try_apply_ws_order_update_for_strategy(self, response) {
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

        match (leg, response.request_kind()) {
            (Some(Leg::Open), TradeRequestKind::Open) => {
                self.handle_open_leg_open_failed(response, code_desc, client_order_id)
            }
            (Some(Leg::Open), TradeRequestKind::Cancel) => {
                self.handle_open_leg_cancel_failed(response, code_desc, client_order_id)
            }
            (Some(Leg::Open), TradeRequestKind::Other) => {
                self.handle_open_leg_other_failed(response, code_desc, client_order_id)
            }
            (Some(Leg::Hedge), TradeRequestKind::Open) => {
                self.handle_hedge_leg_open_failed(response, code_desc, client_order_id)
            }
            (Some(Leg::Hedge), TradeRequestKind::Cancel) => {
                self.handle_hedge_leg_cancel_failed(response, code_desc, client_order_id)
            }
            (Some(Leg::Hedge), TradeRequestKind::Other) => {
                self.handle_hedge_leg_other_failed(response, code_desc, client_order_id)
            }
            (None, _) => {}
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "maker hedge limit price invalid")]
    fn maker_hedge_signal_with_non_positive_limit_price_panics() {
        let mut strategy = HedgeArbStrategy::new(42, "BTCUSDT".to_string());
        strategy.hedge_symbol = "BTCUSDT".to_string();

        let mut ctx =
            ArbHedgeCtx::new_maker(42, 7, Side::Buy.to_u8(), 1.0, 0.001, 0.0, 0.1, true, 1);
        ctx.market_ts = 123;
        ctx.opening_leg.venue = TradingVenue::BinanceMargin.to_u8();
        ctx.hedging_leg.venue = TradingVenue::BinanceFutures.to_u8();

        let _ = strategy.handle_arb_hedge_signal(ctx);
    }
}
