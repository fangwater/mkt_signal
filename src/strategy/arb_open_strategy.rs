use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::open_order_rate_limiter::OrderRateBucket;
use crate::pre_trade::PersistChannel;
use crate::strategy::manager::{OpenPriceMapEntry, OrphanStrategyRole, Strategy};
use crate::strategy::open_strategy_common::{
    OpenCancelInput, OpenSignalInput, OpenStrategyCommon, OpenStrategyState,
};
use crate::strategy::uniform_order_helper::{signal_bbo_from_legs, UniformPublishCtx};
use log::{debug, warn};
use order_common::OrderUpdate;
use order_common::TradeEngineResponse;
use order_common::TradeUpdate;
use order_common::TradeUpdateLite;
use order_common::TradingVenue;
use runtime_common::time_util::get_timestamp_us;
use signal_common::cancel_signal::ArbCancelCtx;
use signal_common::open_signal::ArbOpenCtxView;
use signal_common::trade_signal::{SignalType, TradeSignal};
use std::any::Any;
use std::borrow::Cow;
use std::sync::OnceLock;

/// 全局开关：`ARB_OPEN_PARTIAL_HEDGE=1` 时，ArbOpen 在部分成交时就推进对冲 watermark，
/// 不再等整单 Filled/Canceled。默认关闭，保持「整单终态才对冲」。
static ARB_OPEN_PARTIAL_HEDGE: OnceLock<bool> = OnceLock::new();

fn env_flag_value_enabled(value: &str) -> bool {
    matches!(value, "1" | "true" | "TRUE" | "True" | "on" | "ON")
}

fn env_flag_enabled(names: &[&str]) -> bool {
    names.iter().any(|name| {
        std::env::var(name)
            .ok()
            .map(|value| env_flag_value_enabled(value.as_str()))
            .unwrap_or(false)
    })
}

pub(crate) fn arb_open_partial_hedge_enabled() -> bool {
    *ARB_OPEN_PARTIAL_HEDGE.get_or_init(|| {
        let enabled = env_flag_enabled(&["ARB_OPEN_PARTIAL_HEDGE"]);
        if enabled {
            warn!(
                "ARB_OPEN_PARTIAL_HEDGE=on: ArbOpen will hedge each fill increment instead of waiting for order terminal"
            );
        }
        enabled
    })
}

/// 单腿套利开仓策略：只负责 open leg 生命周期，不保存 hedge leg 或双腿盘口。
pub struct ArbOpenStrategy {
    open_state: OpenStrategyState,
    pub close_ts: Option<i64>, //仓位希望持有的时间
    // TODO: apply_trade_update_lite 会使用这些 open leg 观测字段。
    pub cumulative_open_qty: f64, //累计开仓数量
    pub open_qty_multiplier: f64, //开仓侧数量乘数（venue qty -> base qty）
}

impl ArbOpenStrategy {
    pub fn new(strategy_id: i32) -> Self {
        Self {
            open_state: OpenStrategyState::new(strategy_id),
            close_ts: None,
            cumulative_open_qty: 0.0,
            open_qty_multiplier: 1.0,
        }
    }

    pub fn handle_arb_open_view_with_symbol(
        &mut self,
        ctx: ArbOpenCtxView<'_>,
        symbol: Cow<'_, str>,
        pending_limit_prechecked: bool,
        bitget_spot_order: bool,
        pre_trade_recv_ts: i64,
        pre_trade_handle_ts: i64,
    ) {
        let close_ts = if ctx.hedge_timeout_us > 0 {
            let base_ts = if ctx.create_ts > 0 {
                ctx.create_ts
            } else {
                get_timestamp_us()
            };
            base_ts.saturating_add(ctx.hedge_timeout_us)
        } else {
            0
        };

        if let Some(venue) = TradingVenue::from_u8(ctx.opening_leg.venue) {
            MonitorChannel::instance().seed_close_inventory_if_absent(venue, &symbol);
        }

        let mkt_ts = ctx.opening_leg.ts.max(ctx.hedging_leg.ts);
        let Some(init) = self.handle_open_signal_common(OpenSignalInput {
            signal_kind: "ArbOpen",
            order_log_name: "ArbOpen",
            order_rate_bucket: OrderRateBucket::ArbOpen,
            opening_symbol: symbol,
            opening_symbol_normalized: true,
            venue_u8: ctx.opening_leg.venue,
            side_u8: ctx.side,
            order_type_u8: ctx.order_type,
            qty: ctx.amount_value(),
            price: ctx.price_value(),
            price_count: ctx.price_count(),
            amount_count: ctx.amount_count(),
            exp_time: ctx.exp_time,
            create_ts: ctx.create_ts,
            from_key_len: ctx.from_key_len,
            from_key: Cow::Borrowed(ctx.from_key),
            signal_bbo: signal_bbo_from_legs(Some(&ctx.opening_leg), Some(&ctx.hedging_leg)),
            price_qv: ctx.price_qv,
            order_qty_qv: Some(ctx.amount_qv),
            order_price_qv: Some(ctx.price_qv),
            price_offset: ctx.price_offset,
            reduce_only: false,
            bitget_spot_order,
            client_order_id: None,
            pending_limit_prechecked,
            close_ts,
            mkt_ts,
            signal_type_u8: SignalType::ArbOpen as u8,
            pre_trade_recv_ts,
            pre_trade_handle_ts,
        }) else {
            return;
        };
        self.close_ts = if init.close_ts > 0 {
            Some(init.close_ts)
        } else {
            None
        };
        self.cumulative_open_qty = 0.0;
        self.open_qty_multiplier = init.qty_multiplier;
    }

    pub fn handle_arb_cancel_ctx(&mut self, ctx: &ArbCancelCtx) {
        let mkt_ts = ctx.opening_leg.ts.max(ctx.hedging_leg.ts);
        self.handle_open_cancel_signal_common(OpenCancelInput {
            signal_name: "ArbCancel",
            target_strategy_id: ctx.strategy_id,
            target_client_order_id: 0,
            cancel_side: ctx.get_side(),
            cancel_reason: ctx.get_reason().as_log_reason(),
            trigger_ts: ctx.trigger_ts,
            from_key: ctx.from_key.clone(),
            mkt_ts,
        })
    }

    fn apply_order_update(&mut self, order_update: &dyn OrderUpdate) -> bool {
        self.apply_order_update_common(order_update)
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) -> bool {
        self.apply_trade_update_common(trade)
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        match &signal.signal_type {
            SignalType::ArbOpen => warn!(
                "ArbOpenStrategy: strategy_id={} unexpected ArbOpen in strategy signal handler; open signals must be constructed by pre_trade fast path",
                self.open_state.strategy_id
            ),
            SignalType::ArbCancel => match ArbCancelCtx::from_slice(signal.context.as_ref()) {
                Ok(ctx) => self.handle_arb_cancel_ctx(&ctx),
                Err(err) => warn!(
                    "ArbOpenStrategy: strategy_id={} decode ArbCancel failed: {}",
                    self.open_state.strategy_id, err
                ),
            },
            _ => {
                debug!(
                    "ArbOpenStrategy: strategy_id={} ignore signal {:?}",
                    self.open_state.strategy_id, signal.signal_type
                );
            }
        }
    }
}

impl OpenStrategyCommon for ArbOpenStrategy {
    fn strategy_name(&self) -> &'static str {
        "ArbOpenStrategy"
    }

    fn cancel_signal_type_u8(&self) -> u8 {
        SignalType::ArbCancel as u8
    }

    fn open_state(&self) -> &OpenStrategyState {
        &self.open_state
    }

    fn open_state_mut(&mut self) -> &mut OpenStrategyState {
        &mut self.open_state
    }

    fn open_order_non_terminal_cleanup_reason(&self) -> &'static str {
        "ArbOpen开仓订单未达到终结状态被清理"
    }

    fn orphan_strategy_role(&self) -> OrphanStrategyRole {
        OrphanStrategyRole::Arb
    }

    fn open_order_rate_bucket(&self) -> OrderRateBucket {
        OrderRateBucket::ArbOpen
    }

    fn open_order_action_log_name(&self) -> &'static str {
        "arb open"
    }

    fn open_terminal_close_ts(&self) -> i64 {
        self.close_ts.unwrap_or(0)
    }

    fn hedge_on_incremental_open_fill(&self) -> bool {
        arb_open_partial_hedge_enabled()
    }

    fn log_open_deleveraging_risk_rejects(&self) -> bool {
        true
    }

    fn update_close_inventory_for_open_fill(
        &self,
        venue: TradingVenue,
        symbol: &str,
        side: order_common::Side,
        filled_base_delta: f64,
    ) {
        MonitorChannel::instance().apply_open_inventory_fill_delta(
            venue,
            symbol,
            side,
            filled_base_delta,
        );
    }

    fn resolve_open_qty_multiplier(
        &self,
        venue: TradingVenue,
        symbol: &str,
    ) -> Result<f64, String> {
        MonitorChannel::instance().qty_multiplier_for_venue(venue, symbol)
    }

    fn handoff_open_order_after_query_failure(
        &mut self,
        client_order_id: i64,
        marker: &'static str,
    ) {
        warn!(
            "ArbOpenStrategy: strategy_id={} order query {} failed, handoff to hedge orphan: client_order_id={}",
            self.open_state.strategy_id, marker, client_order_id
        );
        self.handoff_open_order_to_orphan(client_order_id, marker);
    }

    /// arb open 的 uniform from_key = "{open client_order_id}|{开仓信号 rich from_key}"：
    /// 前缀 id 与配对 hedge 单一致 —— 下游按第一个 '|' 切分取 id 即可 JOIN open/hedge；
    /// 后缀保留决策参数；信号时刻盘口由独立的 signal_bbo 字段承载，
    /// 避免在 from_key 中重复编码结构化行情。
    fn uniform_open_publish_ctx(&self) -> UniformPublishCtx {
        let open_state = self.open_state();
        let order_id = open_state.order.open_order_id.to_string();
        let mut from_key = Vec::with_capacity(order_id.len() + 1 + open_state.from_key.len());
        from_key.extend_from_slice(order_id.as_bytes());
        from_key.push(b'|');
        from_key.extend_from_slice(&open_state.from_key);
        UniformPublishCtx {
            signal_ts: open_state.signal_ts,
            from_key,
            signal_bbo: open_state.signal_bbo,
            price_offset: open_state.price_offset,
        }
    }
}

impl Strategy for ArbOpenStrategy {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn get_id(&self) -> i32 {
        self.strategy_id()
    }

    fn symbol(&self) -> Option<&str> {
        self.open_strategy_symbol()
    }

    fn arb_open_price_map_entry(&self) -> Option<OpenPriceMapEntry> {
        self.open_price_map_entry()
    }

    fn is_strategy_order(&self, order_id: i64) -> bool {
        Self::extract_strategy_id(order_id) == self.open_state.strategy_id
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        ArbOpenStrategy::handle_signal(self, signal);
    }

    fn apply_order_update(&mut self, update: &dyn OrderUpdate) {
        let should_persist = ArbOpenStrategy::apply_order_update(self, update);
        if should_persist {
            PersistChannel::with(|ch| ch.publish_order_update(update));
        }
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) {
        let should_persist = ArbOpenStrategy::apply_trade_update(self, trade);
        if should_persist {
            PersistChannel::with(|ch| ch.publish_trade_update(trade));
        }
    }

    fn apply_trade_update_lite(&mut self, trade: &dyn TradeUpdateLite) {
        let _ = self.apply_trade_update_lite_common(trade);
    }

    fn apply_trade_engine_response(&mut self, response: &dyn TradeEngineResponse) {
        self.apply_trade_engine_response_common(response);
    }

    fn handle_period_clock(&mut self, _current_tp: i64) {
        self.handle_open_leg_timeout_common();
        self.handle_query_watchdogs();
    }

    fn is_active(&self) -> bool {
        self.open_strategy_is_active()
    }
}

impl Drop for ArbOpenStrategy {
    fn drop(&mut self) {
        self.cleanup_strategy_orders();
    }
}

#[cfg(test)]
mod tests {
    use super::ArbOpenStrategy;
    use crate::strategy::open_strategy_common::{
        OpenStrategyCommon, PendingOrderQueryReason, QueryWatchdog,
    };
    use crate::strategy::order_reconcile::monotonic_cumulative_fill;

    #[test]
    fn handle_open_failed_cleanup_clears_local_strategy_state() {
        let mut strategy = ArbOpenStrategy::new(7);
        strategy.open_state.order.open_order_id = 7_i64 << 32 | 1;
        strategy.open_state.order.pending_order_query =
            Some(PendingOrderQueryReason::OrderWatchdog);
        strategy.open_state.order.order_query_watchdog = Some(QueryWatchdog {
            client_order_id: strategy.open_state.order.open_order_id,
            due_ts_us: 10,
            reason: PendingOrderQueryReason::OrderWatchdog,
        });
        strategy.open_state.order.cancel_query_watchdog = Some(QueryWatchdog {
            client_order_id: strategy.open_state.order.open_order_id,
            due_ts_us: 20,
            reason: PendingOrderQueryReason::CancelRejected,
        });

        strategy.handle_open_failed_cleanup(strategy.open_state.order.open_order_id);

        assert!(!strategy.open_state.alive);
        assert!(strategy.open_state.order.pending_order_query.is_none());
        assert!(strategy.open_state.order.order_query_watchdog.is_none());
        assert!(strategy.open_state.order.cancel_query_watchdog.is_none());
    }

    #[test]
    fn env_flag_value_enabled_parses_common_truthy_values() {
        assert!(super::env_flag_value_enabled("1"));
        assert!(super::env_flag_value_enabled("on"));
        assert!(super::env_flag_value_enabled("true"));
        assert!(super::env_flag_value_enabled("TRUE"));
        assert!(!super::env_flag_value_enabled("0"));
        assert!(!super::env_flag_value_enabled("off"));
        assert!(!super::env_flag_value_enabled(""));
    }

    #[test]
    fn incremental_open_fill_hedge_follows_env_flag() {
        let strategy = ArbOpenStrategy::new(1);
        assert_eq!(
            strategy.hedge_on_incremental_open_fill(),
            super::arb_open_partial_hedge_enabled()
        );
    }

    #[test]
    fn monotonic_cumulative_fill_keeps_local_value_on_rollback() {
        assert!((monotonic_cumulative_fill(4.2, 0.0) - 4.2).abs() < 1e-12);
    }
}
