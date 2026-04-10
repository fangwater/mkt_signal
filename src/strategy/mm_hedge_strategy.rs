use crate::common::time_util::get_timestamp_us;
use crate::funding_rate::mm_decision::from_key::append_mm_hedge_tlen_to_from_key;
use crate::market_maker::hedge_split::{
    split_hedge_orders_round_robin, HedgeLevel, HedgeSplitOrder,
};
use crate::market_maker::order_align::{contract_qty_multiplier, min_qty_symbol_key};
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::open_order_rate_limiter::{OrderRateBucket, OrderRateLimiter};
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus, OrderManager, OrderType, Side};
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::pre_trade::signal_channel::SignalChannel;
use crate::pre_trade::{PersistChannel, QueryEngHub, TradeEngHub};
use crate::signal::common::{ExecutionType, OrderStatus, SignalBytes, TimeInForce, TradingVenue};
use crate::signal::hedge_signal::{MmHedgeCtx, MmHedgeSignalQueryMsg};
use crate::signal::mm_signal::MmBackwardQueryMsg;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::cancel_reconcile_backoff::{
    cancel_reconcile_attempts_exhausted, cancel_reconcile_query_delay_us,
};
use crate::strategy::manager::{ForceCloseControl, Strategy};
use crate::strategy::net_qty_queue::NetQtyQueue;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::query_engine_response::QueryEngineResponse;
use crate::strategy::query_order_updates::{OrderQueryOrderUpdate, OrderQueryTradeUpdate};
use crate::strategy::trade_engine_response::{TradeEngineResponse, TradeRequestKind};
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::uniform_order_helper::{publish_uniform_order_event, UniformOrderEventKind};
use crate::strategy::ws_order_update::WsOrderUpdate;
use crate::trade_engine::query_parsers::compact_order::{
    is_order_query_not_found_marker, CompactOrderQueryResp, COMPACT_ORDER_QUERY_RESP_LEN,
};
use crate::trade_engine::query_request::{GenericQueryRequest, QueryRequestType};
use crate::trade_engine::trade_request::TradeRequestType;
use log::{debug, info, warn};
use std::any::Any;
use std::collections::{hash_map::Entry, HashMap, HashSet};

const HEDGE_QUERY_INTERVAL_US: i64 = 30_000_000;
const HEDGE_QUERY_WATCHDOG_US: i64 = 30_000;
const ORDER_QUERY_WATCHDOG_DELAY_US: i64 = 300_000;
const CANCEL_RESEND_THROTTLE_US: i64 = 500_000;
const NET_EXPOSURE_EPS_USDT: f64 = 5.0;

#[derive(Debug, Clone)]
pub struct MmHedgeSnapshot {
    pub symbol: String,
    pub net_qty: f64,
    pub buy_qty: f64,
    pub sell_qty: f64,
    pub hedge_ts_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct HedgePlanOrder {
    pub client_order_id: i64,
    pub level_index: usize,
    pub side: Side,
    pub order_type: OrderType,
    pub price: f64,
    pub qty: f64,
    pub from_key: Vec<u8>,
}

#[derive(Debug, Clone)]
struct HedgeOrderMeta {
    from_key: Vec<u8>,
    price_offset: f64,
}

/// 做市对冲策略（每个 symbol 仅一个实例）
pub struct MarketMakerHedgeStrategy {
    strategy_id: i32,
    symbol: String,
    net_qty: f64,
    net_qty_queue: NetQtyQueue,
    period_buy_qty: f64,
    period_sell_qty: f64,
    signal_ts: i64,
    last_hedge_ts_ms: Option<i64>,
    hedge_from_key: Vec<u8>,
    order_seq: u32,
    hedge_plan: Vec<HedgePlanOrder>,
    hedge_order_meta: HashMap<i64, HedgeOrderMeta>,
    hedge_order_ids: HashSet<i64>,
    hedge_request_seq: u64,
    pending_hedge_request_seq: Option<u64>,
    alive_flag: bool,
    next_query_ts_us: i64,
    pending_query: bool,
    query_watchdog_due_ts: i64,
    pending_order_queries: HashMap<i64, PendingOrderQueryReason>,
    order_query_watchdogs: HashMap<i64, (i64, PendingOrderQueryReason)>,
    cancel_reconcile_query_attempts: HashMap<i64, u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingOrderQueryReason {
    OrderWatchdog,
    CancelFailed,
    CancelRejected,
}

impl MarketMakerHedgeStrategy {
    pub fn new(strategy_id: i32, symbol: String) -> Self {
        let now = get_timestamp_us();
        Self {
            strategy_id,
            symbol,
            net_qty: 0.0,
            net_qty_queue: NetQtyQueue::new(),
            period_buy_qty: 0.0,
            period_sell_qty: 0.0,
            signal_ts: 0,
            last_hedge_ts_ms: None,
            hedge_from_key: Vec::new(),
            order_seq: 0,
            hedge_plan: Vec::new(),
            hedge_order_meta: HashMap::new(),
            hedge_order_ids: HashSet::new(),
            hedge_request_seq: 0,
            pending_hedge_request_seq: None,
            alive_flag: true,
            next_query_ts_us: now.saturating_add(HEDGE_QUERY_INTERVAL_US),
            pending_query: false,
            query_watchdog_due_ts: 0,
            pending_order_queries: HashMap::new(),
            order_query_watchdogs: HashMap::new(),
            cancel_reconcile_query_attempts: HashMap::new(),
        }
    }

    /// 组合订单ID：高32位为策略ID，低32位为序列号
    fn compose_order_id(strategy_id: i32, seq: u32) -> i64 {
        ((strategy_id as i64) << 32) | seq as i64
    }

    fn extract_strategy_id(order_id: i64) -> i32 {
        (order_id >> 32) as i32
    }

    fn is_cancel_reconcile_reason(reason: PendingOrderQueryReason) -> bool {
        matches!(
            reason,
            PendingOrderQueryReason::CancelFailed | PendingOrderQueryReason::CancelRejected
        )
    }

    fn pending_query_reason_priority(reason: PendingOrderQueryReason) -> u8 {
        match reason {
            PendingOrderQueryReason::OrderWatchdog => 0,
            PendingOrderQueryReason::CancelFailed => 1,
            PendingOrderQueryReason::CancelRejected => 2,
        }
    }

    fn stronger_pending_query_reason(
        existing: PendingOrderQueryReason,
        incoming: PendingOrderQueryReason,
    ) -> PendingOrderQueryReason {
        if Self::pending_query_reason_priority(incoming)
            > Self::pending_query_reason_priority(existing)
        {
            incoming
        } else {
            existing
        }
    }

    fn upgrade_existing_order_query_reason(
        &mut self,
        client_order_id: i64,
        reason: PendingOrderQueryReason,
    ) -> bool {
        let mut upgraded = false;

        if let Some(existing) = self.pending_order_queries.get_mut(&client_order_id) {
            let merged = Self::stronger_pending_query_reason(*existing, reason);
            if merged != *existing {
                *existing = merged;
                upgraded = true;
            }
        }

        if let Some((_, watchdog_reason)) = self.order_query_watchdogs.get_mut(&client_order_id) {
            let merged = Self::stronger_pending_query_reason(*watchdog_reason, reason);
            if merged != *watchdog_reason {
                *watchdog_reason = merged;
                upgraded = true;
            }
        }

        upgraded
    }

    fn is_cancel_reconciling(&self, client_order_id: i64) -> bool {
        self.pending_order_queries
            .get(&client_order_id)
            .copied()
            .is_some_and(Self::is_cancel_reconcile_reason)
            || self
                .order_query_watchdogs
                .get(&client_order_id)
                .map(|(_, reason)| *reason)
                .is_some_and(Self::is_cancel_reconcile_reason)
    }

    fn cancel_reconcile_query_attempts(&self, client_order_id: i64) -> u8 {
        self.cancel_reconcile_query_attempts
            .get(&client_order_id)
            .copied()
            .unwrap_or(0)
    }

    fn increment_cancel_reconcile_query_attempts(&mut self, client_order_id: i64) -> u8 {
        let next = self
            .cancel_reconcile_query_attempts(client_order_id)
            .saturating_add(1);
        self.cancel_reconcile_query_attempts
            .insert(client_order_id, next);
        next
    }

    fn schedule_order_query_watchdog_with_delay(
        &mut self,
        client_order_id: i64,
        reason: PendingOrderQueryReason,
        delay_us: i64,
    ) {
        let due = get_timestamp_us().saturating_add(delay_us);
        match self.order_query_watchdogs.entry(client_order_id) {
            Entry::Vacant(entry) => {
                entry.insert((due, reason));
            }
            Entry::Occupied(mut entry) => {
                let (existing_due, existing_reason) = entry.get_mut();
                *existing_due = due;
                *existing_reason = Self::stronger_pending_query_reason(*existing_reason, reason);
            }
        }
        debug!(
            "MMHedgeReconcile: strategy_id={} schedule_watchdog due_ts={} reason={:?} {}",
            self.strategy_id,
            due,
            reason,
            self.hedge_order_trace_snapshot(client_order_id)
        );
    }

    fn schedule_next_cancel_reconcile_query(
        &mut self,
        client_order_id: i64,
        reason: PendingOrderQueryReason,
    ) {
        let delay =
            cancel_reconcile_query_delay_us(self.cancel_reconcile_query_attempts(client_order_id));
        self.schedule_order_query_watchdog_with_delay(client_order_id, reason, delay);
    }

    fn hedge_order_trace_snapshot(&self, client_order_id: i64) -> String {
        let pending_reason = self.pending_order_queries.get(&client_order_id).copied();
        let watchdog = self
            .order_query_watchdogs
            .get(&client_order_id)
            .map(|(due_ts, reason)| (*due_ts, *reason));
        let query_attempt = self.cancel_reconcile_query_attempts(client_order_id);
        let tracked = self.hedge_order_ids.contains(&client_order_id);

        let order_desc = if let Some(order_mgr) = MonitorChannel::try_order_manager() {
            let mgr = order_mgr.borrow();
            if let Some(order) = mgr.get(client_order_id) {
                format!(
                    "local=present venue={:?} symbol={} side={:?} status={:?} exch_ord_id={:?} cum_fill={:.8} qty={:.8} px={:.8}",
                    order.venue,
                    order.symbol,
                    order.side,
                    order.status,
                    order.exchange_order_id,
                    order.cumulative_filled_quantity,
                    order.quantity,
                    order.price
                )
            } else {
                "local=missing".to_string()
            }
        } else {
            "local=order_manager_unavailable".to_string()
        };

        format!(
            "client_order_id={} tracked={} pending_query={:?} watchdog={:?} query_attempt={} {}",
            client_order_id, tracked, pending_reason, watchdog, query_attempt, order_desc
        )
    }

    fn hedge_order_set_snapshot(&self) -> String {
        let mut ids: Vec<i64> = self.hedge_order_ids.iter().copied().collect();
        ids.sort_unstable();
        if ids.is_empty() {
            return "[]".to_string();
        }
        let parts: Vec<String> = ids
            .into_iter()
            .map(|order_id| self.hedge_order_trace_snapshot(order_id))
            .collect();
        format!("[{}]", parts.join("; "))
    }

    fn log_hedge_order_release(&self, cause: &str, client_order_id: i64) {
        debug!(
            "MMHedgeTrace: strategy_id={} order_release cause={} {}",
            self.strategy_id,
            cause,
            self.hedge_order_trace_snapshot(client_order_id)
        );
    }

    fn log_hedge_order_state(&self, stage: &str, client_order_id: i64) {
        debug!(
            "MMHedgeTrace: strategy_id={} stage={} {}",
            self.strategy_id,
            stage,
            self.hedge_order_trace_snapshot(client_order_id)
        );
    }

    fn retire_hedge_order(&mut self, client_order_id: i64, cause: &str) {
        self.log_hedge_order_release(cause, client_order_id);
        self.clear_order_query_state(client_order_id);
        self.hedge_order_ids.remove(&client_order_id);
        self.hedge_order_meta.remove(&client_order_id);
        self.cancel_reconcile_query_attempts
            .remove(&client_order_id);
        if let Some(order_mgr) = MonitorChannel::try_order_manager() {
            let _ = order_mgr.borrow_mut().remove(client_order_id);
        }
    }

    fn cleanup_strategy_orders(&mut self) {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };
        let mut mgr = order_mgr.borrow_mut();
        let order_ids = mgr.get_all_ids();
        for order_id in order_ids {
            if Self::extract_strategy_id(order_id) != self.strategy_id {
                continue;
            }
            if let Some(order) = mgr.get(order_id) {
                if !order.status.is_terminal() {
                    mgr.log_order_details(&order, "对冲订单未达到终结状态被清理", self.strategy_id);
                }
            }
            let _ = mgr.remove(order_id);
        }
        self.hedge_order_ids.clear();
        self.hedge_order_meta.clear();
    }

    fn build_order_from_key(&self, batch_from_key: &[u8], tlen: f64) -> Vec<u8> {
        let batch = String::from_utf8_lossy(batch_from_key);
        append_mm_hedge_tlen_to_from_key(&batch, tlen).into_bytes()
    }

    fn order_from_key_bytes(&self, client_order_id: i64) -> Vec<u8> {
        self.hedge_order_meta
            .get(&client_order_id)
            .map(|meta| meta.from_key.clone())
            .unwrap_or_else(|| self.hedge_from_key.clone())
    }

    fn order_price_offset(&self, client_order_id: i64) -> f64 {
        self.hedge_order_meta
            .get(&client_order_id)
            .map(|meta| meta.price_offset)
            .unwrap_or(0.0)
    }

    fn try_apply_ws_order_update(&mut self, response: &dyn TradeEngineResponse) -> bool {
        if !WsOrderUpdate::supports_trade_response_req_type(response.req_type()) {
            return false;
        }

        let client_order_id = response.client_order_id();
        let order_mgr = MonitorChannel::instance().order_manager();
        let Some(order_snapshot) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "MarketMakerHedgeStrategy: strategy_id={} ws order update missing local order: client_order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        };
        let order_snapshot = order_snapshot.clone();

        let Some(update) = WsOrderUpdate::from_trade_response(response, &order_snapshot) else {
            return false;
        };

        // Binance WS 下单响应在 FULL/RESULT 模式下可能直接返回 FILLED/PartiallyFilled
        // （并携带 fills），不再稳定经过 NEW 阶段。
        // 这里统一只接收 NEW/CANCELED，其他状态等待 account ws 的正常推送处理。
        if matches!(
            order_snapshot.venue,
            TradingVenue::BinanceMargin | TradingVenue::BinanceFutures
        ) {
            if matches!(update.status(), OrderStatus::New | OrderStatus::Canceled) {
                <Self as Strategy>::apply_order_update(self, &update);
            } else {
                debug!(
                    "MarketMakerHedgeStrategy: strategy_id={} skip non-NEW/CANCELED binance ws response: venue={:?} client_order_id={} status={:?}",
                    self.strategy_id,
                    order_snapshot.venue,
                    client_order_id,
                    update.status()
                );
            }
            return true;
        }

        if matches!(
            update.status(),
            OrderStatus::PartiallyFilled | OrderStatus::Filled
        ) {
            let trade = OrderQueryTradeUpdate::new(
                &order_snapshot,
                update.order_id(),
                update.event_time(),
                update.cumulative_filled_quantity(),
                response.response_price(),
                Some(update.status()),
                update.time_in_force(),
            );
            <Self as Strategy>::apply_trade_update(self, &trade);
        } else {
            <Self as Strategy>::apply_order_update(self, &update);
        }
        true
    }

    fn apply_net_qty_fill(&mut self, fill_ts: i64, signed_qty: f64, price: f64, source: &str) {
        if signed_qty.abs() <= 1e-12 {
            return;
        }
        let before = self.net_qty;
        let result = self.net_qty_queue.apply_fill(fill_ts, signed_qty, price);
        self.net_qty = result.net_qty;
        debug!(
            "MMHedgeNetQueue: strategy_id={} symbol={} source={} fill_ts={} signed_qty={:.8} price={:.8} matched_qty={:.8} appended_qty={:.8} net_before={:.8} net_after={:.8} lots={}",
            self.strategy_id,
            self.symbol,
            source,
            fill_ts,
            signed_qty,
            price,
            result.matched_qty,
            result.appended_qty,
            before,
            self.net_qty,
            self.net_qty_queue.len()
        );
    }

    /// 累加成交（使用 base qty 口径）
    pub fn record_fill(
        &mut self,
        fill_ts: i64,
        signed_qty: f64,
        buy_qty: f64,
        sell_qty: f64,
        price: f64,
    ) {
        self.apply_net_qty_fill(fill_ts, signed_qty, price, "external_fill");
        self.period_buy_qty += buy_qty;
        self.period_sell_qty += sell_qty;
    }

    pub fn snapshot(&self) -> MmHedgeSnapshot {
        MmHedgeSnapshot {
            symbol: self.symbol.clone(),
            net_qty: self.net_qty,
            buy_qty: self.period_buy_qty,
            sell_qty: self.period_sell_qty,
            hedge_ts_ms: self.last_hedge_ts_ms,
        }
    }

    fn weighted_inventory_price(&self) -> f64 {
        self.net_qty_queue.weighted_avg_price().unwrap_or(0.0)
    }

    fn resolve_fill_price_from_order_update(
        &self,
        client_order_id: i64,
        order_update: &dyn OrderUpdate,
    ) -> f64 {
        let order_price = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(client_order_id)
            .map(|order| order.price)
            .unwrap_or(0.0);
        if order_update.price() > 0.0 {
            order_update.price()
        } else {
            order_price
        }
    }

    fn resolve_fill_price_from_trade_update(
        &self,
        order_snapshot_price: Option<f64>,
        trade: &dyn TradeUpdate,
    ) -> f64 {
        if trade.price() > 0.0 {
            trade.price()
        } else {
            order_snapshot_price.unwrap_or(0.0)
        }
    }

    fn mark_price(&self) -> Option<f64> {
        MonitorChannel::instance()
            .price_table()
            .borrow()
            .mark_price(&self.symbol)
    }

    fn net_exposure_usdt_with_mark_price(net_qty: f64, mark_price: f64) -> f64 {
        net_qty.abs() * mark_price.abs()
    }

    fn net_exposure_usdt(&self) -> Option<f64> {
        self.mark_price()
            .map(|mark_price| Self::net_exposure_usdt_with_mark_price(self.net_qty, mark_price))
    }

    fn should_send_hedge_query(&self) -> bool {
        self.mark_price()
            .map(|mark_price| {
                Self::net_exposure_usdt_with_mark_price(self.net_qty, mark_price)
                    > NET_EXPOSURE_EPS_USDT
            })
            .unwrap_or(false)
    }

    fn handle_flat_inventory_no_query(&mut self, now: i64) {
        self.pending_query = false;
        self.query_watchdog_due_ts = 0;
        self.pending_hedge_request_seq = None;
        self.period_buy_qty = 0.0;
        self.period_sell_qty = 0.0;
        self.next_query_ts_us = now.saturating_add(HEDGE_QUERY_INTERVAL_US);
    }

    fn next_hedge_request_seq(&mut self) -> u64 {
        self.hedge_request_seq = self.hedge_request_seq.wrapping_add(1);
        if self.hedge_request_seq == 0 {
            self.hedge_request_seq = 1;
        }
        self.hedge_request_seq
    }

    fn handle_mm_hedge_signal(&mut self, ctx: MmHedgeCtx) {
        let Some(expected_request_seq) = self.pending_hedge_request_seq else {
            warn!(
                "MarketMakerHedgeStrategy: strategy_id={} drop unexpected MMHedge reply without pending query: symbol={} request_seq={}",
                self.strategy_id,
                ctx.get_opening_symbol(),
                ctx.request_seq
            );
            return;
        };
        if ctx.request_seq != expected_request_seq {
            warn!(
                "MarketMakerHedgeStrategy: strategy_id={} drop stale/duplicate MMHedge reply: symbol={} request_seq={} expected_request_seq={}",
                self.strategy_id,
                ctx.get_opening_symbol(),
                ctx.request_seq,
                expected_request_seq
            );
            return;
        }
        self.pending_hedge_request_seq = None;
        self.signal_ts = ctx.signal_ts;
        self.hedge_from_key = ctx.from_key.clone();
        self.next_query_ts_us = ctx.next_query_ts;
        self.pending_query = false;
        self.query_watchdog_due_ts = 0;

        // 打印所有 MM 对冲策略的累积头寸（净头寸 + 买/卖累计）
        let strategy_mgr = MonitorChannel::instance().strategy_mgr();
        let snapshots = strategy_mgr.borrow().mm_hedge_snapshots();
        for snap in snapshots {
            debug!(
                "MMHedge snapshot: symbol={} net={:.8} buy={:.8} sell={:.8}",
                snap.symbol, snap.net_qty, snap.buy_qty, snap.sell_qty
            );
        }
        let from_key = String::from_utf8_lossy(&self.hedge_from_key);
        debug!(
            "MMHedge ctx: symbol={} price_levels={} amount_levels={} offset_levels={} signal_ts={} next_query_ts={} request_seq={} from_key='{}'",
            ctx.get_opening_symbol(),
            ctx.price_qv_list.len(),
            ctx.amount_qv_list.len(),
            ctx.price_offsets.len(),
            ctx.signal_ts,
            ctx.next_query_ts,
            ctx.request_seq,
            from_key
        );

        let net_qty = self.net_qty;
        let hedge_side = if net_qty > 0.0 {
            Some(Side::Sell)
        } else if net_qty < 0.0 {
            Some(Side::Buy)
        } else {
            None
        };

        let symbol = ctx.get_opening_symbol().to_uppercase();
        let venue = MonitorChannel::instance().open_venue();
        let symbol_key = min_qty_symbol_key(venue, &symbol);
        let qty_multiplier = MonitorChannel::instance()
            .venue_min_qty_table(venue)
            .and_then(|table| contract_qty_multiplier(&table, venue, &symbol_key))
            .unwrap_or(1.0);

        let levels: Vec<HedgeLevel> = ctx
            .price_qv_list
            .iter()
            .zip(ctx.amount_qv_list.iter())
            .filter_map(|(price_qv, amount_qv)| {
                let price = price_qv.get_val();
                let qty_venue = amount_qv.get_val();
                if price <= 0.0 || qty_venue <= 0.0 {
                    return None;
                }
                if qty_venue <= 0.0 {
                    return None;
                }
                Some(HedgeLevel {
                    price,
                    qty_venue_one_hand: qty_venue,
                })
            })
            .collect();

        let split = split_hedge_orders_round_robin(hedge_side, net_qty, &levels, qty_multiplier);
        let remaining_qty = split.remaining_qty_base;
        let total_qty = split.total_qty_base;
        let total_usdt = split.total_usdt;

        let level_stats_text = split
            .level_stats
            .iter()
            .map(|s| {
                format!(
                    "#{} p={:.8} hand_venue={:.8} hand_base={:.8} n={} out_venue={:.8} out_base={:.8}",
                    s.level_index,
                    s.price,
                    s.qty_venue_one_hand,
                    s.qty_base_one_hand,
                    s.hand_count,
                    s.order_qty_venue,
                    s.order_qty_base
                )
            })
            .collect::<Vec<_>>()
            .join(" | ");
        debug!(
            "MMHedge split detail: symbol={} venue={:?} side={:?} net_qty_base={:.8} qty_multiplier={:.8} levels={} remaining_base={:.8} detail={}",
            symbol,
            venue,
            hedge_side,
            net_qty,
            qty_multiplier,
            levels.len(),
            remaining_qty,
            level_stats_text,
        );
        let level_stats_json = split
            .level_stats
            .iter()
            .map(|s| {
                format!(
                    "{{\"idx\":{},\"price\":{:.8},\"hand_venue\":{:.8},\"hand_base\":{:.8},\"n\":{},\"out_venue\":{:.8},\"out_base\":{:.8}}}",
                    s.level_index,
                    s.price,
                    s.qty_venue_one_hand,
                    s.qty_base_one_hand,
                    s.hand_count,
                    s.order_qty_venue,
                    s.order_qty_base
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        debug!(
            "MMHedgeSplitSummary {{\"strategy_id\":{},\"symbol\":\"{}\",\"venue\":\"{:?}\",\"side\":\"{}\",\"net_qty_base\":{:.8},\"qty_multiplier\":{:.8},\"total_qty_base\":{:.8},\"total_usdt\":{:.8},\"remaining_base\":{:.8},\"levels\":[{}]}}",
            self.strategy_id,
            symbol,
            venue,
            hedge_side.map(|s| s.as_str()).unwrap_or("FLAT"),
            net_qty,
            qty_multiplier,
            total_qty,
            total_usdt,
            remaining_qty,
            level_stats_json,
        );

        self.hedge_plan.clear();
        self.hedge_order_meta.clear();
        let reply_is_taker = ctx.use_taker;
        if reply_is_taker {
            if self.order_seq >= u32::MAX {
                self.order_seq = 1;
            } else {
                self.order_seq += 1;
                if self.order_seq == 0 {
                    self.order_seq = 1;
                }
            }
            let client_order_id = Self::compose_order_id(self.strategy_id, self.order_seq);
            let total_qty = split.orders.iter().map(|order| order.qty).sum::<f64>();
            let side = split.orders.first().map(|order| order.side).unwrap_or_else(|| {
                if net_qty >= 0.0 {
                    Side::Sell
                } else {
                    Side::Buy
                }
            });
            self.hedge_order_meta.insert(
                client_order_id,
                HedgeOrderMeta {
                    from_key: ctx.from_key.clone(),
                    price_offset: 0.0,
                },
            );
            self.hedge_plan.push(HedgePlanOrder {
                client_order_id,
                level_index: 0,
                side,
                order_type: OrderType::Market,
                price: self.weighted_inventory_price(),
                qty: total_qty,
                from_key: ctx.from_key.clone(),
            });
        } else {
            for HedgeSplitOrder {
                level_index,
                side,
                price,
                qty,
            } in split.orders
            {
                if self.order_seq >= u32::MAX {
                    self.order_seq = 1;
                } else {
                    self.order_seq += 1;
                    if self.order_seq == 0 {
                        self.order_seq = 1;
                    }
                }
                let client_order_id = Self::compose_order_id(self.strategy_id, self.order_seq);
                let tlen = ctx.tlen_values.get(level_index).copied().unwrap_or(0.0);
                let order_from_key = self.build_order_from_key(&ctx.from_key, tlen);
                let price_offset = ctx.price_offsets.get(level_index).copied().unwrap_or(0.0);
                self.hedge_order_meta.insert(
                    client_order_id,
                    HedgeOrderMeta {
                        from_key: order_from_key.clone(),
                        price_offset,
                    },
                );
                self.hedge_plan.push(HedgePlanOrder {
                    client_order_id,
                    level_index,
                    side,
                    order_type: OrderType::Limit,
                    price,
                    qty,
                    from_key: order_from_key,
                });
            }
        }

        let mut table = String::new();
        table.push_str("+----------------------+----------+--------------+--------------+--------------+\n");
        table.push_str("| client_order_id      | type     | price        | qty          | usdt         |\n");
        table.push_str("+----------------------+----------+--------------+--------------+--------------+\n");
        for row in &self.hedge_plan {
            let usdt = row.price * row.qty;
            table.push_str(&format!(
                "| {:>20} | {:>8} | {:>12.6} | {:>12.6} | {:>12.6} |\n",
                row.client_order_id,
                if row.order_type == OrderType::Market {
                    "MARKET"
                } else {
                    "LIMIT"
                },
                row.price,
                row.qty,
                usdt
            ));
        }
        table.push_str("+----------------------+----------+--------------+--------------+--------------+");

        let hedge_side_str = match hedge_side {
            Some(Side::Buy) => "BUY",
            Some(Side::Sell) => "SELL",
            None => "FLAT",
        };
        debug!(
            "MMHedge split: symbol={} side={} mode={} net_qty={:.8} total_qty={:.8} total_usdt={:.8} remain_qty={:.8}\n{}",
            ctx.get_opening_symbol(),
            hedge_side_str,
            if ctx.use_taker { "taker" } else { "maker" },
            net_qty,
            total_qty,
            total_usdt,
            remaining_qty,
            table
        );

        if !self.hedge_plan.is_empty() {
            self.send_hedge_orders(venue, &symbol);
        }

        // 清空期间累计多空成交
        self.period_buy_qty = 0.0;
        self.period_sell_qty = 0.0;
    }

    fn handle_query_timer(&mut self) {
        if self.next_query_ts_us <= 0 {
            return;
        }

        let now = get_timestamp_us();
        if now < self.next_query_ts_us {
            return;
        }

        let mark_price = self.mark_price();
        let net_exposure_usdt = mark_price
            .map(|price| Self::net_exposure_usdt_with_mark_price(self.net_qty, price))
            .unwrap_or(0.0);
        debug!(
            "MMHedgeTrace: strategy_id={} query_timer fired symbol={} now={} next_query_ts_us={} net_qty_base={:.8} mark_price={:?} net_exposure_usdt={:.8} pending_query={} tracked_orders={}",
            self.strategy_id,
            self.symbol,
            now,
            self.next_query_ts_us,
            self.net_qty,
            mark_price,
            net_exposure_usdt,
            self.pending_query,
            self.hedge_order_set_snapshot()
        );

        if self.request_cancel_for_hedge_orders() {
            return;
        }

        if !self.should_send_hedge_query() {
            debug!(
                "MMHedgeTrace: strategy_id={} query_timer skip_hedge_query threshold_usdt={} mark_price={:?} net_qty_base={:.8} net_exposure_usdt={:?}",
                self.strategy_id,
                NET_EXPOSURE_EPS_USDT,
                mark_price,
                self.net_qty,
                self.net_exposure_usdt()
            );
            self.handle_flat_inventory_no_query(now);
            return;
        }

        self.send_hedge_query();
        self.next_query_ts_us = 0;
    }

    fn handle_query_watchdog(&mut self) {
        if !self.pending_query {
            return;
        }
        let now = get_timestamp_us();
        if now < self.query_watchdog_due_ts {
            return;
        }
        if !self.should_send_hedge_query() {
            self.handle_flat_inventory_no_query(now);
            return;
        }
        warn!(
            "MarketMakerHedgeStrategy: strategy_id={} symbol={} query watchdog timeout, retry send",
            self.strategy_id, self.symbol
        );
        self.send_hedge_query();
    }

    fn request_cancel_for_hedge_orders(&mut self) -> bool {
        if self.hedge_order_ids.is_empty() {
            return false;
        }

        debug!(
            "MMHedgeTrace: strategy_id={} cancel_scan_start tracked_orders={}",
            self.strategy_id,
            self.hedge_order_set_snapshot()
        );

        let order_mgr = MonitorChannel::instance().order_manager();
        let mgr = order_mgr.borrow();
        let mut to_remove: Vec<i64> = Vec::new();
        let mut to_cancel: Vec<i64> = Vec::new();
        for order_id in &self.hedge_order_ids {
            match mgr.get(*order_id) {
                Some(order) => {
                    if order.status.is_terminal() {
                        debug!(
                            "MMHedgeTrace: strategy_id={} cancel_scan terminal order will be retired: {}",
                            self.strategy_id,
                            self.hedge_order_trace_snapshot(*order_id)
                        );
                        to_remove.push(*order_id);
                    } else if self.is_cancel_reconciling(*order_id) {
                        // Cancel already failed and we are reconciling via order query; do not
                        // spam repeated cancel requests every tick.
                        debug!(
                            "MMHedgeTrace: strategy_id={} cancel_scan skip due to reconcile: {}",
                            self.strategy_id,
                            self.hedge_order_trace_snapshot(*order_id)
                        );
                    } else {
                        debug!(
                            "MMHedgeTrace: strategy_id={} cancel_scan enqueue cancel: {}",
                            self.strategy_id,
                            self.hedge_order_trace_snapshot(*order_id)
                        );
                        to_cancel.push(*order_id);
                    }
                }
                None => {
                    warn!(
                        "MMHedgeTrace: strategy_id={} cancel_scan local order missing, will retire: {}",
                        self.strategy_id,
                        self.hedge_order_trace_snapshot(*order_id)
                    );
                    to_remove.push(*order_id);
                }
            }
        }
        drop(mgr);

        for order_id in to_remove {
            self.retire_hedge_order(order_id, "cancel_scan_remove");
        }

        if self.hedge_order_ids.is_empty() {
            debug!(
                "MMHedgeTrace: strategy_id={} cancel_scan finished with no tracked orders remaining",
                self.strategy_id
            );
            return false;
        }

        if to_cancel.is_empty() {
            debug!(
                "MMHedgeTrace: strategy_id={} cancel_scan no cancel sent because all tracked orders are reconciling: tracked_orders={}",
                self.strategy_id,
                self.hedge_order_set_snapshot()
            );
        }

        for order_id in &to_cancel {
            let order_mgr = MonitorChannel::instance().order_manager();
            let mgr = order_mgr.borrow();
            let Some(order) = mgr.get(*order_id) else {
                warn!(
                    "MMHedgeTrace: strategy_id={} cancel_send skipped because local order disappeared: {}",
                    self.strategy_id,
                    self.hedge_order_trace_snapshot(*order_id)
                );
                continue;
            };
            let exchange = order.venue.trade_engine_exchange();
            debug!(
                "MMHedgeTrace: strategy_id={} cancel_send preparing exchange={} {}",
                self.strategy_id,
                exchange,
                self.hedge_order_trace_snapshot(*order_id)
            );
            match order.get_order_cancel_bytes() {
                Ok(req_bin) => {
                    if let Err(e) = TradeEngHub::publish_order_request(exchange, &req_bin) {
                        warn!(
                            "MarketMakerHedgeStrategy: strategy_id={} cancel hedge order failed: exchange={} client_order_id={} err={}",
                            self.strategy_id, exchange, order_id, e
                        );
                    } else {
                        debug!(
                            "MarketMakerHedgeStrategy: strategy_id={} cancel hedge order sent: exchange={} client_order_id={}",
                            self.strategy_id, exchange, order_id
                        );
                        self.log_hedge_order_state("cancel_sent", *order_id);
                    }
                }
                Err(e) => {
                    warn!(
                        "MarketMakerHedgeStrategy: strategy_id={} build cancel bytes failed: client_order_id={} err={}",
                        self.strategy_id, order_id, e
                    );
                }
            }
        }

        // Avoid spamming cancel requests within the same second if the period clock ticks fast.
        // We'll retry later if orders are still live.
        if !to_cancel.is_empty() {
            self.next_query_ts_us = get_timestamp_us().saturating_add(CANCEL_RESEND_THROTTLE_US);
            debug!(
                "MMHedgeTrace: strategy_id={} cancel_scan rescheduled next_query_ts_us={} tracked_orders={}",
                self.strategy_id,
                self.next_query_ts_us,
                self.hedge_order_set_snapshot()
            );
        }

        true
    }

    fn send_hedge_query(&mut self) {
        // 定时发送对冲查询（携带 symbol + 当期买/卖成交 + 累计净头寸）
        let hedge_ts_ms = (get_timestamp_us() / 1000) as i64;
        self.last_hedge_ts_ms = Some(hedge_ts_ms);
        let risk_loader = PreTradeParamsLoader::instance();
        let symbol_exposure_u =
            risk_loader.max_pos_u().max(0.0) * risk_loader.max_symbol_exposure_ratio().max(0.0);
        let request_seq = self
            .pending_hedge_request_seq
            .unwrap_or_else(|| self.next_hedge_request_seq());
        self.pending_hedge_request_seq = Some(request_seq);
        let query_msg = MmHedgeSignalQueryMsg::new(
            &self.symbol,
            self.period_buy_qty,
            self.period_sell_qty,
            self.net_qty,
            symbol_exposure_u,
            self.weighted_inventory_price(),
            request_seq,
        );
        let monitor = MonitorChannel::instance();
        let hedge_venue = monitor.hedge_venue();
        let risk_position_qty = monitor.get_position_qty(&self.symbol, hedge_venue);
        let risk_position_diff = self.net_qty - risk_position_qty;
        info!(
            "MarketMakerHedgeStrategy: strategy_id={} hedge net-vs-monitor symbol={} request_seq={} net_qty={:.8} weighted_inventory_price={:.8} monitor_qty={:.8} diff_qty={:.8} hedge_venue={:?} period_buy_qty={:.8} period_sell_qty={:.8}",
            self.strategy_id,
            self.symbol,
            request_seq,
            self.net_qty,
            self.weighted_inventory_price(),
            risk_position_qty,
            risk_position_diff,
            hedge_venue,
            self.period_buy_qty,
            self.period_sell_qty
        );
        let payload = MmBackwardQueryMsg::Hedge(query_msg).to_bytes();
        let send_result = SignalChannel::with(|ch| ch.publish_backward(&payload));
        match send_result {
            Ok(true) => {
                debug!(
                    "MarketMakerHedgeStrategy: strategy_id={} send hedge query ok symbol={} request_seq={}",
                    self.strategy_id, self.symbol, request_seq
                );
                let now = get_timestamp_us();
                self.pending_query = true;
                self.query_watchdog_due_ts = now.saturating_add(HEDGE_QUERY_WATCHDOG_US);
            }
            Ok(false) => {
                warn!(
                    "MarketMakerHedgeStrategy: backward publisher 未配置，无法发送对冲查询 symbol={} request_seq={}",
                    self.symbol, request_seq
                );
                self.pending_query = false;
                self.query_watchdog_due_ts = 0;
            }
            Err(err) => {
                warn!(
                    "MarketMakerHedgeStrategy: 发送对冲查询失败 symbol={} request_seq={} err={:#}",
                    self.symbol, request_seq, err
                );
                self.pending_query = false;
                self.query_watchdog_due_ts = 0;
            }
        }
    }

    fn schedule_order_query_watchdog(
        &mut self,
        client_order_id: i64,
        reason: PendingOrderQueryReason,
    ) {
        self.schedule_order_query_watchdog_with_delay(
            client_order_id,
            reason,
            ORDER_QUERY_WATCHDOG_DELAY_US,
        );
    }

    fn clear_order_query_state(&mut self, client_order_id: i64) {
        let removed_watchdog = self.order_query_watchdogs.remove(&client_order_id);
        let removed_pending = self.pending_order_queries.remove(&client_order_id);
        if removed_watchdog.is_some() || removed_pending.is_some() {
            debug!(
                "MMHedgeReconcile: strategy_id={} clear_query_state removed_pending={:?} removed_watchdog={:?} {}",
                self.strategy_id,
                removed_pending,
                removed_watchdog,
                self.hedge_order_trace_snapshot(client_order_id)
            );
        }
    }

    fn mark_hedge_order_terminal_on_query_failure(&mut self, client_order_id: i64, reason: &str) {
        warn!(
            "MMHedgeReconcile: strategy_id={} force_terminal_start reason={} {}",
            self.strategy_id,
            reason,
            self.hedge_order_trace_snapshot(client_order_id)
        );
        self.retire_hedge_order(client_order_id, "query_failure_force_terminal");

        warn!(
            "MarketMakerHedgeStrategy: strategy_id={} mark hedge order terminal: client_order_id={} reason={}",
            self.strategy_id, client_order_id, reason
        );
    }

    fn handle_order_query_watchdogs(&mut self) {
        if self.order_query_watchdogs.is_empty() {
            return;
        }
        let now = get_timestamp_us();
        let mut due_entries: Vec<(i64, PendingOrderQueryReason)> = Vec::new();
        for (client_order_id, (due_ts, reason)) in &self.order_query_watchdogs {
            if now >= *due_ts {
                due_entries.push((*client_order_id, *reason));
            }
        }
        if due_entries.is_empty() {
            return;
        }

        let order_mgr = MonitorChannel::instance().order_manager();
        for (client_order_id, reason) in due_entries {
            self.order_query_watchdogs.remove(&client_order_id);
            let order_opt = order_mgr.borrow().get(client_order_id);
            if let Some(order) = order_opt.as_ref().filter(|o| !o.status.is_terminal()) {
                // If a query was inflight but we didn't receive a response, allow watchdog to retry.
                self.pending_order_queries.remove(&client_order_id);
                let scheduled_at = now.saturating_sub(ORDER_QUERY_WATCHDOG_DELAY_US);
                let waited_ms = now.saturating_sub(scheduled_at).saturating_div(1_000);
                debug!(
                    "MMHedge OrderWatchdog触发: strategy_id={} client_order_id={} symbol={} status={:?} exch_ord_id={:?} 等待{}ms仍未收到回报，发送order query回补",
                    self.strategy_id,
                    client_order_id,
                    order.symbol,
                    order.status,
                    order.exchange_order_id,
                    waited_ms
                );
                if Self::is_cancel_reconcile_reason(reason) {
                    if cancel_reconcile_attempts_exhausted(
                        self.cancel_reconcile_query_attempts(client_order_id),
                    ) {
                        self.mark_hedge_order_terminal_on_query_failure(
                            client_order_id,
                            "cancel_reconcile query max attempts exhausted",
                        );
                        continue;
                    }
                    self.increment_cancel_reconcile_query_attempts(client_order_id);
                    let sent = self.send_order_query(client_order_id, reason);
                    if sent || self.hedge_order_ids.contains(&client_order_id) {
                        self.schedule_next_cancel_reconcile_query(client_order_id, reason);
                    }
                } else {
                    let _ = self.send_order_query(client_order_id, reason);
                }
            }
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
            TradingVenue::BinanceMargin => {
                if MonitorChannel::instance()
                    .order_manager()
                    .borrow()
                    .binance_is_standard()
                {
                    QueryRequestType::BinanceWsMarginQuery
                } else {
                    QueryRequestType::BinanceMarginQuery
                }
            }
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

    fn send_order_query(&mut self, client_order_id: i64, reason: PendingOrderQueryReason) -> bool {
        if self.pending_order_queries.contains_key(&client_order_id) {
            let upgraded = self.upgrade_existing_order_query_reason(client_order_id, reason);
            debug!(
                "MMHedgeReconcile: strategy_id={} skip_send_order_query because pending already exists: reason={:?} upgraded={} {}",
                self.strategy_id,
                reason,
                upgraded,
                self.hedge_order_trace_snapshot(client_order_id)
            );
            return true;
        }

        let order_mgr = MonitorChannel::instance().order_manager();
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "MarketMakerHedgeStrategy: strategy_id={} send_order_query but local order missing: client_order_id={} reason={:?}",
                self.strategy_id, client_order_id, reason
            );
            return false;
        };

        let query_by_exchange_order_id = order.exchange_order_id.is_some_and(|id| id > 0);

        match self.build_order_query_request(&order, client_order_id) {
            Ok((exchange, req_bytes)) => {
                if let Err(err) = QueryEngHub::publish_query_request(exchange.as_str(), &req_bytes)
                {
                    warn!(
                        "MarketMakerHedgeStrategy: strategy_id={} publish order query failed: exchange={} client_order_id={} reason={:?} err={:#}",
                        self.strategy_id, exchange, client_order_id, reason, err
                    );
                    return false;
                }
                self.pending_order_queries.insert(client_order_id, reason);
                // Ensure we eventually retry if query response is lost/empty.
                if !Self::is_cancel_reconcile_reason(reason) {
                    self.schedule_order_query_watchdog(client_order_id, reason);
                }
                debug!(
                    "MMHedgeReconcile: strategy_id={} order_query_sent exchange={} reason={:?} by_exchange_order_id={} payload_len={} {}",
                    self.strategy_id,
                    exchange,
                    reason,
                    query_by_exchange_order_id,
                    req_bytes.len(),
                    self.hedge_order_trace_snapshot(client_order_id)
                );
                true
            }
            Err(err) => {
                warn!(
                    "MarketMakerHedgeStrategy: strategy_id={} build order query failed: client_order_id={} reason={:?} err={}",
                    self.strategy_id, client_order_id, reason, err
                );
                false
            }
        }
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
        if OrderExecutionStatus::from_u8(parsed.status_u8).is_none() {
            return None;
        }
        if TimeInForce::from_u8(parsed.time_in_force_u8).is_none() {
            return None;
        }
        if !parsed.response_price.is_finite() || parsed.response_price < 0.0 {
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

    fn apply_parsed_order_query_updates(
        &mut self,
        order: &crate::pre_trade::order_manager::Order,
        parsed: CompactOrderQueryResp,
        reason: PendingOrderQueryReason,
    ) {
        debug!(
            "MMHedgeReconcile: strategy_id={} apply_query_result reason={:?} parsed_status_u8={} parsed_exec_qty={:.8} {}",
            self.strategy_id,
            reason,
            parsed.status_u8,
            parsed.executed_qty,
            self.hedge_order_trace_snapshot(order.client_order_id)
        );
        let event_time_us = parsed.update_time_ms.saturating_mul(1_000);
        let order_id = parsed.order_id;
        let tif = TimeInForce::from_u8(parsed.time_in_force_u8).unwrap_or(TimeInForce::GTC);

        if parsed.executed_qty > order.cumulative_filled_quantity + 1e-12 {
            let status = if parsed.status_u8 == OrderExecutionStatus::Filled.to_u8() {
                Some(OrderStatus::Filled)
            } else {
                Some(OrderStatus::PartiallyFilled)
            };
            let trade = OrderQueryTradeUpdate::new(
                order,
                order_id,
                event_time_us,
                parsed.executed_qty,
                Some(parsed.response_price),
                status,
                tif,
            );
            <Self as Strategy>::apply_trade_update(self, &trade);
        }

        let status_u8 = parsed.status_u8;
        if status_u8 == OrderExecutionStatus::Create.to_u8() {
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
                <Self as Strategy>::apply_order_update(self, &upd);
            }
        } else if status_u8 == OrderExecutionStatus::Cancelled.to_u8() {
            let upd = OrderQueryOrderUpdate::new(
                order,
                order_id,
                event_time_us,
                OrderStatus::Canceled,
                ExecutionType::Canceled,
                parsed.executed_qty,
                tif,
            );
            <Self as Strategy>::apply_order_update(self, &upd);
        } else if status_u8 == OrderExecutionStatus::Rejected.to_u8() {
            warn!(
                "MarketMakerHedgeStrategy: strategy_id={} query_resp rejected: client_order_id={} order_id={} exec_qty={:.8} reason={:?}",
                self.strategy_id,
                order.client_order_id,
                order_id,
                parsed.executed_qty,
                reason
            );
        }

        debug!(
            "MarketMakerHedgeStrategy: strategy_id={} query回补: client_order_id={} order_id={} exec_qty={:.8} status_u8={} reason={:?}",
            self.strategy_id,
            order.client_order_id,
            order_id,
            parsed.executed_qty,
            parsed.status_u8,
            reason
        );
    }

    fn handle_query_result(
        &mut self,
        client_order_id: i64,
        reason: PendingOrderQueryReason,
        parsed: Option<CompactOrderQueryResp>,
    ) {
        let order_mgr = MonitorChannel::instance().order_manager();
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "MarketMakerHedgeStrategy: strategy_id={} query_resp but order missing: client_order_id={} reason={:?}",
                self.strategy_id, client_order_id, reason
            );
            return;
        };

        let Some(parsed) = parsed else {
            debug!(
                "MMHedgeReconcile: strategy_id={} query_result parsed=None, retry by watchdog: reason={:?} {}",
                self.strategy_id,
                reason,
                self.hedge_order_trace_snapshot(client_order_id)
            );
            if Self::is_cancel_reconcile_reason(reason)
                && cancel_reconcile_attempts_exhausted(
                    self.cancel_reconcile_query_attempts(client_order_id),
                )
            {
                self.mark_hedge_order_terminal_on_query_failure(
                    client_order_id,
                    "cancel_reconcile parsed none (max attempts)",
                );
            } else {
                self.schedule_next_cancel_reconcile_query(client_order_id, reason);
            }
            return;
        };

        let Some(st) = OrderExecutionStatus::from_u8(parsed.status_u8) else {
            warn!(
                "MarketMakerHedgeStrategy: strategy_id={} query invalid status_u8={} client_order_id={} reason={:?}",
                self.strategy_id, parsed.status_u8, client_order_id, reason
            );
            return;
        };

        match st {
            OrderExecutionStatus::Filled | OrderExecutionStatus::Cancelled => {
                self.apply_parsed_order_query_updates(&order, parsed, reason);
            }
            OrderExecutionStatus::Create => {
                self.apply_parsed_order_query_updates(&order, parsed, reason);
                if Self::is_cancel_reconcile_reason(reason) {
                    if cancel_reconcile_attempts_exhausted(
                        self.cancel_reconcile_query_attempts(client_order_id),
                    ) {
                        self.mark_hedge_order_terminal_on_query_failure(
                            client_order_id,
                            "cancel_reconcile query still create (max attempts)",
                        );
                    } else {
                        self.schedule_next_cancel_reconcile_query(client_order_id, reason);
                    }
                }
            }
            OrderExecutionStatus::Rejected => {
                warn!(
                    "MarketMakerHedgeStrategy: strategy_id={} query shows {:?} client_order_id={} reason={:?}",
                    self.strategy_id, st, client_order_id, reason
                );
                self.retire_hedge_order(client_order_id, "query_result_rejected");
            }
            OrderExecutionStatus::Commit => {
                warn!(
                    "MarketMakerHedgeStrategy: strategy_id={} query shows {:?} client_order_id={} reason={:?}",
                    self.strategy_id, st, client_order_id, reason
                );
                if Self::is_cancel_reconcile_reason(reason) {
                    if cancel_reconcile_attempts_exhausted(
                        self.cancel_reconcile_query_attempts(client_order_id),
                    ) {
                        self.mark_hedge_order_terminal_on_query_failure(
                            client_order_id,
                            "cancel_reconcile query still commit (max attempts)",
                        );
                    } else {
                        self.schedule_next_cancel_reconcile_query(client_order_id, reason);
                    }
                }
            }
        }
    }

    fn send_hedge_orders(&mut self, venue: TradingVenue, symbol: &str) {
        let plans = self.hedge_plan.clone();
        let rate_params = PreTradeParamsLoader::instance();
        let mut borrowed_open_count_10s = 0usize;
        let mut borrowed_open_count_1m = 0usize;
        for plan in plans {
            let now_us = get_timestamp_us();
            let hedge_stats = OrderRateLimiter::stats(OrderRateBucket::MmHedge, now_us);
            let open_stats = OrderRateLimiter::stats(OrderRateBucket::MmOpen, now_us);
            let hedge_limit_per_min = rate_params.hedge_order_rate_limit_per_min();
            let hedge_limit_10s = rate_params.hedge_order_rate_limit_10s();
            let open_limit_per_min = rate_params.open_order_rate_limit_per_min();
            let open_limit_10s = rate_params.open_order_rate_limit_10s();

            let hedge_hit_10s =
                hedge_limit_10s > 0 && hedge_stats.count_10s >= hedge_limit_10s as usize;
            let hedge_hit_1m =
                hedge_limit_per_min > 0 && hedge_stats.count_1m >= hedge_limit_per_min as usize;

            if hedge_hit_10s || hedge_hit_1m {
                let open_can_cover_10s = !hedge_hit_10s
                    || (open_limit_10s > 0
                        && open_stats.count_10s + borrowed_open_count_10s
                            < open_limit_10s as usize);
                let open_can_cover_1m = !hedge_hit_1m
                    || (open_limit_per_min > 0
                        && open_stats.count_1m + borrowed_open_count_1m
                            < open_limit_per_min as usize);

                if open_can_cover_10s && open_can_cover_1m {
                    borrowed_open_count_10s += 1;
                    borrowed_open_count_1m += 1;
                    info!(
                        "MarketMakerHedgeStrategy: strategy_id={} symbol={} hedge rate limit hit, temporarily borrow open quota in current batch: hedge_count_10s={} hedge_limit_10s={} hedge_count_1m={} hedge_limit_1m={} open_count_10s={} open_limit_10s={} open_count_1m={} open_limit_1m={} borrowed_open_10s={} borrowed_open_1m={}",
                        self.strategy_id,
                        symbol,
                        hedge_stats.count_10s,
                        hedge_limit_10s,
                        hedge_stats.count_1m,
                        hedge_limit_per_min,
                        open_stats.count_10s,
                        open_limit_10s,
                        open_stats.count_1m,
                        open_limit_per_min,
                        borrowed_open_count_10s,
                        borrowed_open_count_1m
                    );
                } else {
                    info!(
                        "MarketMakerHedgeStrategy: strategy_id={} symbol={} hedge 下单频率风控触发，且 open 剩余额度不足: hedge_count_10s={} hedge_limit_10s={} hedge_count_1m={} hedge_limit_1m={} open_count_10s={} open_limit_10s={} open_count_1m={} open_limit_1m={} borrowed_open_10s={} borrowed_open_1m={}",
                        self.strategy_id,
                        symbol,
                        hedge_stats.count_10s,
                        hedge_limit_10s,
                        hedge_stats.count_1m,
                        hedge_limit_per_min,
                        open_stats.count_10s,
                        open_limit_10s,
                        open_stats.count_1m,
                        open_limit_per_min,
                        borrowed_open_count_10s,
                        borrowed_open_count_1m
                    );
                    break;
                }
            }

            let submit_ts = now_us;
            MonitorChannel::instance()
                .order_manager()
                .borrow_mut()
                .create_order(
                    venue,
                    plan.client_order_id,
                    plan.order_type,
                    symbol.to_string(),
                    plan.side,
                    plan.qty,
                    plan.price,
                    false,
                    1.0,
                    submit_ts,
                );
            self.hedge_order_ids.insert(plan.client_order_id);
            self.log_hedge_order_state("order_created_local", plan.client_order_id);

            let order = MonitorChannel::instance()
                .order_manager()
                .borrow()
                .get(plan.client_order_id);
            let Some(order) = order else {
                warn!(
                    "MarketMakerHedgeStrategy: strategy_id={} hedge order missing after create: client_order_id={}",
                    self.strategy_id, plan.client_order_id
                );
                continue;
            };
            let exchange = order.venue.trade_engine_exchange();
            match order.get_order_request_bytes() {
                Ok(req_bin) => {
                    let stats = OrderRateLimiter::record(
                        OrderRateBucket::MmHedge,
                        plan.client_order_id,
                        get_timestamp_us(),
                    );
                    info!(
                        "MarketMakerHedgeStrategy: strategy_id={} MM hedge order action recorded client_order_id={} count_10s={} count_1m={}",
                        self.strategy_id, plan.client_order_id, stats.count_10s, stats.count_1m
                    );
                    if let Err(e) = TradeEngHub::publish_order_request(exchange, &req_bin) {
                        warn!(
                            "MarketMakerHedgeStrategy: strategy_id={} send hedge order failed: exchange={} client_order_id={} err={}",
                            self.strategy_id, exchange, plan.client_order_id, e
                        );
                    } else {
                        debug!(
                            "MarketMakerHedgeStrategy: strategy_id={} hedge order sent: exchange={} client_order_id={} level_index={} side={:?} price={:.8} qty={:.8} from_key='{}'",
                            self.strategy_id,
                            exchange,
                            plan.client_order_id,
                            plan.level_index,
                            plan.side,
                            plan.price,
                            plan.qty,
                            String::from_utf8_lossy(&plan.from_key)
                        );
                        self.log_hedge_order_state("open_sent", plan.client_order_id);
                        self.schedule_order_query_watchdog(
                            plan.client_order_id,
                            PendingOrderQueryReason::OrderWatchdog,
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        "MarketMakerHedgeStrategy: strategy_id={} get hedge order bytes failed: client_order_id={} err={}",
                        self.strategy_id, plan.client_order_id, e
                    );
                }
            }
        }
    }

    fn uniform_hedge_fields(&self, client_order_id: i64) -> (i64, Vec<u8>, f64) {
        (
            self.signal_ts,
            format!(
                "hedge|{}",
                String::from_utf8_lossy(&self.order_from_key_bytes(client_order_id))
            )
            .into_bytes(),
            self.order_price_offset(client_order_id),
        )
    }

    fn compute_uniform_amount_update(
        &self,
        order: &Order,
        incoming_cum: f64,
        prev_cumulative_filled_qty: f64,
        status: OrderStatus,
    ) -> f64 {
        match OrderManager::compute_uniform_amount_update_from_cumulative(
            prev_cumulative_filled_qty,
            incoming_cum,
        ) {
            Some(delta) => delta,
            None => {
                warn!(
                    "MarketMakerHedgeStrategy: strategy_id={} uniform {:?} amount_update rollback detected: client_order_id={} prev={:.8} incoming={:.8}",
                    self.strategy_id,
                    status,
                    order.client_order_id,
                    prev_cumulative_filled_qty,
                    incoming_cum
                );
                0.0
            }
        }
    }

    fn publish_uniform_new_order(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
    ) {
        let (signal_ts, from_key, price_offset) = self.uniform_hedge_fields(order.client_order_id);
        let incoming_cum = order_update.cumulative_filled_quantity();
        let amount_update = self.compute_uniform_amount_update(
            order,
            incoming_cum,
            prev_cumulative_filled_qty,
            order_update.status(),
        );

        publish_uniform_order_event(
            order,
            UniformOrderEventKind::New,
            order_update.event_time(),
            order_update.status(),
            signal_ts,
            from_key,
            None,
            price_offset,
            amount_update,
        );
    }

    fn publish_uniform_terminal_order(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
    ) {
        let (signal_ts, from_key, price_offset) = self.uniform_hedge_fields(order.client_order_id);
        let incoming_cum = order_update.cumulative_filled_quantity();
        let amount_update = self.compute_uniform_amount_update(
            order,
            incoming_cum,
            prev_cumulative_filled_qty,
            order_update.status(),
        );
        publish_uniform_order_event(
            order,
            UniformOrderEventKind::Terminal,
            order_update.event_time(),
            order_update.status(),
            signal_ts,
            from_key,
            None,
            price_offset,
            amount_update,
        );
    }

    fn publish_uniform_trade_order(
        &self,
        trade: &dyn TradeUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
        status: OrderStatus,
    ) {
        if !matches!(status, OrderStatus::PartiallyFilled | OrderStatus::Filled) {
            return;
        }
        let (signal_ts, from_key, price_offset) = self.uniform_hedge_fields(order.client_order_id);
        let incoming_cum = trade.cumulative_filled_quantity();
        let amount_update = self.compute_uniform_amount_update(
            order,
            incoming_cum,
            prev_cumulative_filled_qty,
            status,
        );

        publish_uniform_order_event(
            order,
            UniformOrderEventKind::Trade,
            trade.event_time(),
            status,
            signal_ts,
            from_key,
            Some(trade.price()),
            price_offset,
            amount_update,
        );
    }

    fn publish_uniform_trade_order_from_order_update(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
    ) {
        let status = order_update.status();
        if !matches!(status, OrderStatus::PartiallyFilled | OrderStatus::Filled) {
            return;
        }
        let (signal_ts, from_key, price_offset) = self.uniform_hedge_fields(order.client_order_id);
        let incoming_cum = order_update.cumulative_filled_quantity();
        let amount_update = self.compute_uniform_amount_update(
            order,
            incoming_cum,
            prev_cumulative_filled_qty,
            status,
        );

        publish_uniform_order_event(
            order,
            UniformOrderEventKind::Trade,
            order_update.event_time(),
            status,
            signal_ts,
            from_key,
            None,
            price_offset,
            amount_update,
        );
    }

    fn apply_order_update(&mut self, order_update: &dyn OrderUpdate) -> bool {
        let client_order_id = order_update.client_order_id();
        self.clear_order_query_state(client_order_id);
        debug!(
            "MMHedgeTrace: strategy_id={} order_update_in execution_type={:?} status={:?} order_id={} cum_fill={:.8} {}",
            self.strategy_id,
            order_update.execution_type(),
            order_update.status(),
            order_update.order_id(),
            order_update.cumulative_filled_quantity(),
            self.hedge_order_trace_snapshot(client_order_id)
        );

        let order_mgr = MonitorChannel::instance().order_manager();
        let mut order_manager = order_mgr.borrow_mut();
        let status = order_update.status();
        let Some(current_order) = order_manager.get(client_order_id) else {
            warn!(
                "MarketMakerHedgeStrategy: strategy_id={} order update not found client_order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        };

        if OrderManager::should_skip_idempotent_order_update(
            &current_order,
            order_update.status(),
            order_update.order_id(),
            order_update.cumulative_filled_quantity(),
            "MarketMakerHedgeStrategy",
            self.strategy_id,
        )
        .is_some()
        {
            return false;
        }

        let prev_cumulative_filled_qty = current_order.cumulative_filled_quantity;

        let updated = order_manager.update(client_order_id, |order| match status {
            OrderStatus::New => {
                order.status = OrderExecutionStatus::Create;
                order.set_exchange_order_id(order_update.order_id());
                order.set_create_time(order_update.event_time());
                debug!(
                    "✅ MMHedge订单已挂单: strategy_id={} client_order_id={} exchange_order_id={} symbol={} side={:?} price={:.6} qty={:.4}",
                    self.strategy_id,
                    client_order_id,
                    order_update.order_id(),
                    order.symbol,
                    order.side,
                    order.price,
                    order.quantity
                );
            }
            OrderStatus::Canceled => {
                order.status = OrderExecutionStatus::Cancelled;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = order_update.cumulative_filled_quantity();
                order.set_end_time(order_update.event_time());
            }
            OrderStatus::Filled => {
                order.status = OrderExecutionStatus::Filled;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = order_update.cumulative_filled_quantity();
                order.set_filled_time(order_update.event_time());
                order.set_end_time(order_update.event_time());
            }
            OrderStatus::Expired | OrderStatus::ExpiredInMatch => {
                order.status = OrderExecutionStatus::Rejected;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = order_update.cumulative_filled_quantity();
                order.set_end_time(order_update.event_time());
            }
            OrderStatus::PartiallyFilled => {
                order.status = OrderExecutionStatus::Create;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = order_update.cumulative_filled_quantity();
                order.set_filled_time(order_update.event_time());
            }
        });
        drop(order_manager);

        if !updated {
            warn!(
                "MarketMakerHedgeStrategy: strategy_id={} order update not found client_order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        }

        self.log_hedge_order_state("order_update_applied", client_order_id);

        if status == OrderStatus::New {
            if let Some(order) = MonitorChannel::instance()
                .order_manager()
                .borrow()
                .get(client_order_id)
            {
                self.publish_uniform_new_order(order_update, &order, prev_cumulative_filled_qty);
            }
        }

        if matches!(
            status,
            OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::ExpiredInMatch
        ) {
            if let Some(order) = MonitorChannel::instance()
                .order_manager()
                .borrow()
                .get(client_order_id)
            {
                self.publish_uniform_terminal_order(
                    order_update,
                    &order,
                    prev_cumulative_filled_qty,
                );
            }
        }

        if matches!(status, OrderStatus::PartiallyFilled | OrderStatus::Filled) {
            if let Some(order) = MonitorChannel::instance()
                .order_manager()
                .borrow()
                .get(client_order_id)
            {
                self.publish_uniform_trade_order_from_order_update(
                    order_update,
                    &order,
                    prev_cumulative_filled_qty,
                );
            }
        }

        if status.is_finished() {
            let was_tracked = self.hedge_order_ids.contains(&client_order_id);
            let filled_qty = order_update.cumulative_filled_quantity();
            let fill_price = self.resolve_fill_price_from_order_update(client_order_id, order_update);
            if was_tracked && filled_qty > 0.0 {
                let base_qty = MonitorChannel::instance().qty_to_base(
                    order_update.trading_venue(),
                    order_update.symbol(),
                    filled_qty,
                );
                if base_qty > 0.0 {
                    let signed_qty = match order_update.side() {
                        Side::Buy => base_qty,
                        Side::Sell => -base_qty,
                    };
                    self.apply_net_qty_fill(
                        order_update.event_time(),
                        signed_qty,
                        fill_price,
                        "hedge_fill",
                    );
                }
            }
            self.retire_hedge_order(client_order_id, "order_update_finished");
            if was_tracked && filled_qty <= 0.0 && status == OrderStatus::Canceled {
                debug!(
                    "🚫 MMHedge订单撤销: strategy_id={} client_order_id={} exchange_order_id={} filled=0",
                    self.strategy_id,
                    client_order_id,
                    order_update.order_id()
                );
            }
        }

        true
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) -> bool {
        let client_order_id = trade.client_order_id();
        self.clear_order_query_state(client_order_id);
        debug!(
            "MMHedgeTrace: strategy_id={} trade_update_in order_status={:?} order_id={} cum_fill={:.8} price={:.8} {}",
            self.strategy_id,
            trade.order_status(),
            trade.order_id(),
            trade.cumulative_filled_quantity(),
            trade.price(),
            self.hedge_order_trace_snapshot(client_order_id)
        );

        let status = trade.order_status();
        let Some(status) = status else {
            return false;
        };
        if status != OrderStatus::Filled && status != OrderStatus::PartiallyFilled {
            return false;
        }

        let order_mgr = MonitorChannel::instance().order_manager();
        let mut order_manager = order_mgr.borrow_mut();
        let Some(current_order) = order_manager.get(client_order_id) else {
            warn!(
                "MarketMakerHedgeStrategy: strategy_id={} trade update order missing client_order_id={}",
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
            "MarketMakerHedgeStrategy",
            self.strategy_id,
        )
        .is_some()
        {
            return false;
        }

        let cumulative_qty = trade.cumulative_filled_quantity();
        let trade_time = trade.trade_time();
        let event_time = trade.event_time();
        let reported_trade_price = trade.price();

        let updated = order_manager.update(client_order_id, |order| {
            order.cumulative_filled_quantity = cumulative_qty;
            order.set_filled_time(trade_time);
            order.set_exchange_order_id(trade.order_id());
            if reported_trade_price > 0.0 {
                order.price = reported_trade_price;
            }
            order.status = if status == OrderStatus::Filled {
                OrderExecutionStatus::Filled
            } else {
                OrderExecutionStatus::Create
            };
            if status == OrderStatus::Filled {
                order.set_end_time(event_time);
            }
        });

        if !updated {
            warn!(
                "MarketMakerHedgeStrategy: strategy_id={} trade update order missing client_order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        }

        let order_snapshot = order_manager
            .get(client_order_id)
            .map(|order| (order.venue, order.symbol.clone(), order.side, order.price));
        if let Some(order) = order_manager.get(client_order_id) {
            self.publish_uniform_trade_order(trade, &order, prev_cumulative_filled_qty, status);
        }

        drop(order_manager);
        self.log_hedge_order_state("trade_update_applied", client_order_id);

        if status != OrderStatus::Filled {
            return true;
        }
        let was_tracked = self.hedge_order_ids.contains(&client_order_id);
        let fill_price = self.resolve_fill_price_from_trade_update(
            order_snapshot.as_ref().map(|(_, _, _, price)| *price),
            trade,
        );
        if was_tracked && cumulative_qty > 0.0 {
            let base_qty = MonitorChannel::instance().qty_to_base(
                trade.trading_venue(),
                trade.symbol(),
                cumulative_qty,
            );
            if base_qty > 0.0 {
                let signed_qty = match trade.side() {
                    Side::Buy => base_qty,
                    Side::Sell => -base_qty,
                };
                self.apply_net_qty_fill(event_time, signed_qty, fill_price, "hedge_fill");
            }
        }
        self.retire_hedge_order(client_order_id, "trade_update_filled");

        true
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        match &signal.signal_type {
            SignalType::MMHedge => match MmHedgeCtx::from_bytes(signal.context.clone()) {
                Ok(ctx) => self.handle_mm_hedge_signal(ctx),
                Err(err) => {
                    warn!(
                        "MarketMakerHedgeStrategy: strategy_id={} decode MMHedge failed: {}",
                        self.strategy_id, err
                    );
                }
            },
            _ => {
                debug!(
                    "MarketMakerHedgeStrategy: strategy_id={} ignore signal {:?}",
                    self.strategy_id, signal.signal_type
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        HedgeOrderMeta, MarketMakerHedgeStrategy, PendingOrderQueryReason, NET_EXPOSURE_EPS_USDT,
    };

    #[test]
    fn zero_net_exposure_does_not_send_hedge_query() {
        assert_eq!(
            MarketMakerHedgeStrategy::net_exposure_usdt_with_mark_price(0.0, 100.0),
            0.0
        );
    }

    #[test]
    fn net_exposure_threshold_uses_usdt_value() {
        let below = MarketMakerHedgeStrategy::net_exposure_usdt_with_mark_price(0.01, 100.0);
        let above = MarketMakerHedgeStrategy::net_exposure_usdt_with_mark_price(0.2, 100.0);
        assert!(below < NET_EXPOSURE_EPS_USDT);
        assert!(above > NET_EXPOSURE_EPS_USDT);
    }

    #[test]
    fn cancel_reconcile_reasons_are_classified() {
        assert!(MarketMakerHedgeStrategy::is_cancel_reconcile_reason(
            PendingOrderQueryReason::CancelFailed
        ));
        assert!(MarketMakerHedgeStrategy::is_cancel_reconcile_reason(
            PendingOrderQueryReason::CancelRejected
        ));
        assert!(!MarketMakerHedgeStrategy::is_cancel_reconcile_reason(
            PendingOrderQueryReason::OrderWatchdog
        ));
    }

    #[test]
    fn stronger_pending_query_reason_prefers_cancel_reconcile() {
        assert_eq!(
            MarketMakerHedgeStrategy::stronger_pending_query_reason(
                PendingOrderQueryReason::OrderWatchdog,
                PendingOrderQueryReason::CancelFailed,
            ),
            PendingOrderQueryReason::CancelFailed
        );
        assert_eq!(
            MarketMakerHedgeStrategy::stronger_pending_query_reason(
                PendingOrderQueryReason::CancelFailed,
                PendingOrderQueryReason::CancelRejected,
            ),
            PendingOrderQueryReason::CancelRejected
        );
        assert_eq!(
            MarketMakerHedgeStrategy::stronger_pending_query_reason(
                PendingOrderQueryReason::CancelRejected,
                PendingOrderQueryReason::OrderWatchdog,
            ),
            PendingOrderQueryReason::CancelRejected
        );
    }

    #[test]
    fn upgrade_existing_order_query_reason_updates_pending_and_watchdog() {
        let mut strategy = MarketMakerHedgeStrategy::new(1, "ETHUSDT".to_string());
        let client_order_id = 42_i64;
        strategy
            .pending_order_queries
            .insert(client_order_id, PendingOrderQueryReason::OrderWatchdog);
        strategy
            .order_query_watchdogs
            .insert(client_order_id, (1, PendingOrderQueryReason::OrderWatchdog));

        assert!(strategy.upgrade_existing_order_query_reason(
            client_order_id,
            PendingOrderQueryReason::CancelRejected,
        ));
        assert_eq!(
            strategy
                .pending_order_queries
                .get(&client_order_id)
                .copied(),
            Some(PendingOrderQueryReason::CancelRejected)
        );
        assert_eq!(
            strategy
                .order_query_watchdogs
                .get(&client_order_id)
                .map(|(_, reason)| *reason),
            Some(PendingOrderQueryReason::CancelRejected)
        );

        std::mem::forget(strategy);
    }

    #[test]
    fn retire_hedge_order_clears_open_failed_tracking_state() {
        let mut strategy = MarketMakerHedgeStrategy::new(1, "ETHUSDT".to_string());
        let client_order_id = 42_i64;
        strategy.hedge_order_ids.insert(client_order_id);
        strategy.hedge_order_meta.insert(
            client_order_id,
            HedgeOrderMeta {
                from_key: b"hedge".to_vec(),
                price_offset: 1.5,
            },
        );
        strategy
            .pending_order_queries
            .insert(client_order_id, PendingOrderQueryReason::CancelRejected);
        strategy.order_query_watchdogs.insert(
            client_order_id,
            (123, PendingOrderQueryReason::CancelRejected),
        );
        strategy
            .cancel_reconcile_query_attempts
            .insert(client_order_id, 2);

        strategy.retire_hedge_order(client_order_id, "open_failed");

        assert!(!strategy.hedge_order_ids.contains(&client_order_id));
        assert!(!strategy.hedge_order_meta.contains_key(&client_order_id));
        assert!(!strategy
            .pending_order_queries
            .contains_key(&client_order_id));
        assert!(!strategy
            .order_query_watchdogs
            .contains_key(&client_order_id));
        assert!(!strategy
            .cancel_reconcile_query_attempts
            .contains_key(&client_order_id));

        std::mem::forget(strategy);
    }
}

impl ForceCloseControl for MarketMakerHedgeStrategy {
    fn set_force_close_mode(&mut self, _enabled: bool) {}

    fn is_force_close_mode(&self) -> bool {
        false
    }
}

impl Strategy for MarketMakerHedgeStrategy {
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
        Some(&self.symbol)
    }

    fn is_strategy_order(&self, order_id: i64) -> bool {
        Self::extract_strategy_id(order_id) == self.strategy_id
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        MarketMakerHedgeStrategy::handle_signal(self, signal);
    }

    fn apply_order_update(&mut self, update: &dyn OrderUpdate) {
        let should_persist = MarketMakerHedgeStrategy::apply_order_update(self, update);
        if should_persist {
            PersistChannel::with(|ch| ch.publish_order_update(update));
        }
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) {
        let should_persist = MarketMakerHedgeStrategy::apply_trade_update(self, trade);
        if should_persist {
            PersistChannel::with(|ch| ch.publish_trade_update(trade));
        }
    }

    fn apply_trade_engine_response(&mut self, response: &dyn TradeEngineResponse) {
        if self.try_apply_ws_order_update(response) {
            return;
        }
        if response.is_request_success() {
            return;
        }

        let req_type_enum = match TradeRequestType::try_from(response.req_type()) {
            Ok(t) => t,
            Err(_) => return,
        };
        if !matches!(
            req_type_enum,
            TradeRequestType::BinanceWsNewUMOrder
                | TradeRequestType::BinanceWsCancelUMOrder
                | TradeRequestType::BinanceWsNewMarginOrder
                | TradeRequestType::BinanceWsCancelMarginOrder
        ) {
            return;
        }

        let req_type = response.req_type();
        match response.request_kind() {
            TradeRequestKind::Open => {
                let client_order_id = response.client_order_id();
                warn!(
                    "MarketMakerHedgeStrategy: strategy_id={} ws open failed: req_type={} status={} code={} client_order_id={}",
                    self.strategy_id,
                    req_type,
                    response.status(),
                    response.error_code(),
                    client_order_id
                );
                self.retire_hedge_order(client_order_id, "open_failed");
            }
            TradeRequestKind::Cancel => {
                let client_order_id = response.client_order_id();
                let cancel_not_cancellable = response.is_cancel_not_cancellable();
                warn!(
                    "MarketMakerHedgeStrategy: strategy_id={} ws cancel failed: req_type={} status={} code={} client_order_id={} is_cancel_not_cancellable={} {}",
                    self.strategy_id,
                    req_type,
                    response.status(),
                    response.error_code(),
                    client_order_id,
                    cancel_not_cancellable,
                    self.hedge_order_trace_snapshot(client_order_id)
                );
                let reason = if cancel_not_cancellable {
                    PendingOrderQueryReason::CancelRejected
                } else {
                    PendingOrderQueryReason::CancelFailed
                };
                self.schedule_next_cancel_reconcile_query(client_order_id, reason);
                debug!(
                    "MMHedgeReconcile: strategy_id={} ws_cancel_failed_followup req_type={} reason={:?} sent_query={} pending_now={} watchdog_now={} {}",
                    self.strategy_id,
                    req_type,
                    reason,
                    false,
                    self.pending_order_queries.contains_key(&client_order_id),
                    self.order_query_watchdogs.contains_key(&client_order_id),
                    self.hedge_order_trace_snapshot(client_order_id)
                );
            }
            TradeRequestKind::Other => {}
        }
    }

    fn apply_query_engine_response(&mut self, response: &dyn QueryEngineResponse) {
        let client_order_id = response.client_query_id();
        let Some(reason) = self.pending_order_queries.remove(&client_order_id) else {
            debug!(
                "MMHedgeReconcile: strategy_id={} query_response_without_pending req_type={} client_order_id={} tracked={} body_len={}",
                self.strategy_id,
                response.req_type(),
                client_order_id,
                self.hedge_order_ids.contains(&client_order_id),
                response.body_bytes().len()
            );
            return;
        };
        self.order_query_watchdogs.remove(&client_order_id);
        let is_cancel_reconcile = Self::is_cancel_reconcile_reason(reason);

        let body = response.body_bytes().as_ref();
        let has_any_byte = body.iter().any(|&b| b != 0);
        let actual_len = body
            .iter()
            .rposition(|&b| b != 0)
            .map(|pos| pos + 1)
            .unwrap_or(0);
        debug!(
            "MMHedgeReconcile: strategy_id={} query_response req_type={} reason={:?} cancel_reconcile={} body_has_nonzero={} actual_len={} {}",
            self.strategy_id,
            response.req_type(),
            reason,
            is_cancel_reconcile,
            has_any_byte,
            actual_len,
            self.hedge_order_trace_snapshot(client_order_id)
        );
        if !has_any_byte {
            if is_cancel_reconcile {
                if cancel_reconcile_attempts_exhausted(
                    self.cancel_reconcile_query_attempts(client_order_id),
                ) {
                    self.mark_hedge_order_terminal_on_query_failure(
                        client_order_id,
                        "cancel_reconcile query empty (max attempts)",
                    );
                    return;
                }
                self.schedule_next_cancel_reconcile_query(client_order_id, reason);
                return;
            }
            return;
        }

        if is_order_query_not_found_marker(&body[..actual_len]) {
            warn!(
                "MMHedgeReconcile: strategy_id={} query_response order_not_found (-2013) reason={:?} {}",
                self.strategy_id,
                reason,
                self.hedge_order_trace_snapshot(client_order_id)
            );
            self.mark_hedge_order_terminal_on_query_failure(
                client_order_id,
                "query order not found (-2013)",
            );
            return;
        }

        if actual_len == 1 && body[0] == b'E' {
            warn!(
                "MMHedgeReconcile: strategy_id={} query_response exchange_not_found reason={:?} {}",
                self.strategy_id,
                reason,
                self.hedge_order_trace_snapshot(client_order_id)
            );
            if is_cancel_reconcile {
                self.mark_hedge_order_terminal_on_query_failure(
                    client_order_id,
                    "cancel_reconcile query not-found",
                );
                return;
            }
            self.schedule_order_query_watchdog(client_order_id, reason);
            return;
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
                "MarketMakerHedgeStrategy: strategy_id={} query_resp decode failed: client_order_id={} req_type={} reason={:?} body='{}'",
                self.strategy_id,
                client_order_id,
                response.req_type(),
                reason,
                text
            );
            if is_cancel_reconcile {
                if cancel_reconcile_attempts_exhausted(
                    self.cancel_reconcile_query_attempts(client_order_id),
                ) {
                    self.mark_hedge_order_terminal_on_query_failure(
                        client_order_id,
                        "cancel_reconcile query decode_failed (max attempts)",
                    );
                    return;
                }
                self.schedule_next_cancel_reconcile_query(client_order_id, reason);
                return;
            }
        }

        self.handle_query_result(client_order_id, reason, parsed);
    }

    fn handle_period_clock(&mut self, _current_tp: i64) {
        if self.is_active() {
            self.handle_query_timer();
            self.handle_query_watchdog();
            self.handle_order_query_watchdogs();
        }
    }

    fn is_active(&self) -> bool {
        self.alive_flag
    }
}

impl Drop for MarketMakerHedgeStrategy {
    fn drop(&mut self) {
        self.alive_flag = false;
        self.cleanup_strategy_orders();
    }
}
