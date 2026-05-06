use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::time_util::get_timestamp_us;
use crate::funding_rate::mm_decision::from_key::append_mm_hedge_tlen_to_from_key;
use crate::market_maker::hedge_split::{
    split_hedge_orders_round_robin, HedgeLevel, HedgeSplitOrder,
};
use crate::market_maker::order_align::{
    align_final_order_qty, contract_qty_multiplier, min_qty_symbol_key,
};
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::open_order_rate_limiter::{OrderRateBucket, OrderRateLimiter};
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus, OrderManager, OrderType, Side};
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::pre_trade::signal_channel::SignalChannel;
use crate::pre_trade::{PersistChannel, TradeEngHub};
use crate::signal::common::{OrderStatus, SignalBytes, TradingVenue};
use crate::signal::hedge_signal::{MmHedgeCtx, MmHedgeSignalQueryMsg};
use crate::signal::mm_signal::MmBackwardQueryMsg;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::hedge_order_reconcile::{HedgeOrderReconcileCommon, HedgeOrderReconcileState};
use crate::strategy::hedge_strategy_common::{
    mark_price_lookup_symbol, parse_return_qtl_from_from_key, signed_qty_from_side,
    CANCEL_RESEND_THROTTLE_US, HEDGE_QUERY_INTERVAL_US, HEDGE_QUERY_WATCHDOG_US,
    NET_EXPOSURE_EPS_USDT, TERMINAL_QTY_EPS,
};
use crate::strategy::manager::{
    OrderTerminalRecorder, OrphanHandoff, OrphanStrategyRole, Strategy,
};
use crate::strategy::net_qty_queue::NetQtyQueue;
use crate::strategy::order_reconcile::{qv_decimal_or_fallback, PendingOrderQueryReason};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_engine_response::TradeEngineResponse;
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::uniform_order_helper::{
    publish_uniform_new_order, publish_uniform_terminal_order, publish_uniform_trade_order,
    publish_uniform_trade_order_from_order_update, UniformPublishCtx,
};
use log::{debug, info, warn};
use std::any::Any;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct MmHedgeSnapshot {
    pub symbol: String,
    pub net_qty: f64,
    pub buy_qty: f64,
    pub sell_qty: f64,
    pub hedge_ts_ms: Option<i64>,
    pub hedge_is_taker: Option<bool>,
    pub ret_qtl: Option<f64>,
    pub offset_low: Option<f64>,
    pub offset_high: Option<f64>,
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
    pub(super) period_buy_qty: f64,
    pub(super) period_sell_qty: f64,
    signal_ts: i64,
    last_hedge_ts_ms: Option<i64>,
    last_hedge_is_taker: Option<bool>,
    last_ret_qtl: Option<f64>,
    last_offset_low: Option<f64>,
    last_offset_high: Option<f64>,
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
    order_reconcile_state: HedgeOrderReconcileState,
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
            last_hedge_is_taker: None,
            last_ret_qtl: None,
            last_offset_low: None,
            last_offset_high: None,
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
            order_reconcile_state: HedgeOrderReconcileState::default(),
        }
    }

    /// 组合订单ID：高32位为策略ID，低32位为序列号
    fn compose_order_id(strategy_id: i32, seq: u32) -> i64 {
        ((strategy_id as i64) << 32) | seq as i64
    }

    fn extract_strategy_id(order_id: i64) -> i32 {
        (order_id >> 32) as i32
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

    fn release_hedge_tracking(&mut self, client_order_id: i64, cause: &str, remove_local: bool) {
        // MM hedge is a long-lived per-symbol state strategy. Releasing a hedge order only drops
        // this child order's tracking/query metadata; the strategy itself stays alive to maintain
        // inventory state and process future hedge signals.
        self.log_hedge_order_release(cause, client_order_id);
        self.clear_order_query_state(client_order_id);
        self.hedge_order_ids.remove(&client_order_id);
        self.hedge_order_meta.remove(&client_order_id);
        if remove_local {
            if let Some(order_mgr) = MonitorChannel::try_order_manager() {
                let _ = order_mgr.borrow_mut().remove(client_order_id);
            }
        }
    }

    fn release_hedge_order(&mut self, client_order_id: i64, cause: &str) {
        self.release_hedge_tracking(client_order_id, cause, true);
    }

    fn release_hedge_order_keep_local(&mut self, client_order_id: i64, cause: &str) {
        self.release_hedge_tracking(client_order_id, cause, false);
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

    pub(super) fn apply_net_qty_fill(
        &mut self,
        fill_ts: i64,
        signed_qty: f64,
        price: f64,
        source: &str,
    ) {
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

    fn apply_tracked_hedge_fill_cumulative(
        &mut self,
        client_order_id: i64,
        fill_ts: i64,
        side: Side,
        cumulative_base_qty: f64,
        price: f64,
        source: &str,
    ) -> bool {
        if !self.hedge_order_ids.contains(&client_order_id) || cumulative_base_qty <= 0.0 {
            return false;
        }

        // MM hedge keeps partial fills out of net_qty. Once the order is terminal, book the
        // full cumulative fill; duplicate/stale trade updates were already filtered upstream.
        let signed_qty = match side {
            Side::Buy => cumulative_base_qty,
            Side::Sell => -cumulative_base_qty,
        };
        self.apply_net_qty_fill(fill_ts, signed_qty, price, source);
        true
    }

    /// 累加外部成交/初始化净仓（使用 base qty 口径）
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

    pub fn record_net_fill_only(&mut self, fill_ts: i64, signed_qty: f64, price: f64) {
        self.apply_net_qty_fill(fill_ts, signed_qty, price, "orphan_terminal_fill");
    }

    pub fn snapshot(&self) -> MmHedgeSnapshot {
        MmHedgeSnapshot {
            symbol: self.symbol.clone(),
            net_qty: self.net_qty,
            buy_qty: self.period_buy_qty,
            sell_qty: self.period_sell_qty,
            hedge_ts_ms: self.last_hedge_ts_ms,
            hedge_is_taker: self.last_hedge_is_taker,
            ret_qtl: self.last_ret_qtl,
            offset_low: self.last_offset_low,
            offset_high: self.last_offset_high,
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
        let monitor = MonitorChannel::instance();
        let price_symbol = mark_price_lookup_symbol(&self.symbol, monitor.mark_price_exchange());
        monitor.price_table().borrow().mark_price(&price_symbol)
    }

    fn net_exposure_usdt_with_mark_price(net_qty: f64, mark_price: f64) -> f64 {
        net_qty.abs() * mark_price.abs()
    }

    fn net_exposure_usdt(&self) -> Option<f64> {
        self.mark_price()
            .map(|mark_price| Self::net_exposure_usdt_with_mark_price(self.net_qty, mark_price))
    }

    fn should_send_hedge_query(&self) -> bool {
        // 只有净敞口换算成 USDT 后超过阈值才发 hedge query；没有 mark price 时保守跳过。
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
        self.last_ret_qtl = parse_return_qtl_from_from_key(&ctx.from_key);
        self.last_offset_low = ctx
            .price_offsets
            .iter()
            .copied()
            .filter(|v| v.is_finite())
            .reduce(f64::min);
        self.last_offset_high = ctx
            .price_offsets
            .iter()
            .copied()
            .filter(|v| v.is_finite())
            .reduce(f64::max);
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

        let symbol = normalize_symbol_for_internal(&ctx.get_opening_symbol());
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
                let (qty_tick_i64, qty_tick_exp) = amount_qv.get_tick_parts();
                let qty_venue_tick = (qty_tick_i64 as f64) * 10f64.powi(qty_tick_exp);
                if price <= 0.0 || qty_venue <= 0.0 {
                    return None;
                }
                if qty_venue <= 0.0 {
                    return None;
                }
                Some(HedgeLevel {
                    price,
                    qty_venue_one_hand: qty_venue,
                    qty_venue_tick,
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
        self.last_hedge_is_taker = Some(reply_is_taker);
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
            let side = split
                .orders
                .first()
                .map(|order| order.side)
                .unwrap_or_else(|| {
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
        table.push_str(
            "+----------------------+----------+--------------+--------------+--------------+\n",
        );
        table.push_str(
            "| client_order_id      | type     | price        | qty          | usdt         |\n",
        );
        table.push_str(
            "+----------------------+----------+--------------+--------------+--------------+\n",
        );
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
        table.push_str(
            "+----------------------+----------+--------------+--------------+--------------+",
        );

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
        // 周期触发入口：由 handle_period_clock 驱动，到达 next_query_ts_us 后才考虑发 query。
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
            "MMHedgeTrace: strategy_id={} query_timer fired symbol={} now={} next_query_ts_us={} net_qty_base={:.8} mark_price={:?} net_exposure_usdt={:.8} pending_query={} tracked_order_count={}",
            self.strategy_id,
            self.symbol,
            now,
            self.next_query_ts_us,
            self.net_qty,
            mark_price,
            net_exposure_usdt,
            self.pending_query,
            self.hedge_order_ids.len()
        );

        // 如果上一轮 hedge 订单还在场内，先撤旧单；撤单完成/回补后再进入下一轮 query。
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
        // 已发 query 但短时间没有收到 MMHedge 回包时，使用同一个 request_seq 重发。
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
            "MarketMakerHedgeStrategy: strategy_id={} symbol={} query watchdog timeout, resend hedge query",
            self.strategy_id, self.symbol
        );
        self.send_hedge_query();
    }

    fn request_cancel_for_hedge_orders(&mut self) -> bool {
        if self.hedge_order_ids.is_empty() {
            return false;
        }

        debug!(
            "MMHedgeTrace: strategy_id={} cancel_scan_start tracked_order_count={}",
            self.strategy_id,
            self.hedge_order_ids.len()
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
                            "MMHedgeTrace: strategy_id={} cancel_scan terminal order will be released: {}",
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
                        "MMHedgeTrace: strategy_id={} cancel_scan local order missing, will release: {}",
                        self.strategy_id,
                        self.hedge_order_trace_snapshot(*order_id)
                    );
                    to_remove.push(*order_id);
                }
            }
        }
        drop(mgr);

        for order_id in to_remove {
            self.release_hedge_order(order_id, "cancel_scan_remove");
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
                "MMHedgeTrace: strategy_id={} cancel_scan no cancel sent because all tracked orders are reconciling",
                self.strategy_id
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
        if !to_cancel.is_empty() {
            self.next_query_ts_us = get_timestamp_us().saturating_add(CANCEL_RESEND_THROTTLE_US);
            debug!(
                "MMHedgeTrace: strategy_id={} cancel_scan rescheduled next_query_ts_us={} tracked_order_count={}",
                self.strategy_id,
                self.next_query_ts_us,
                self.hedge_order_ids.len()
            );
        }

        true
    }

    fn send_hedge_query(&mut self) {
        // 发送对冲查询：携带 symbol、当期买/卖成交、累计净头寸和风控敞口上限。
        // 调用方已确认净敞口超过阈值；这里负责组包、记录 pending 状态并启动 watchdog。
        let hedge_ts_ms = get_timestamp_us() / 1000;
        self.last_hedge_ts_ms = Some(hedge_ts_ms);
        let risk_loader = PreTradeParamsLoader::instance();
        let open_venue = MonitorChannel::instance().open_venue();
        let symbol_exposure_u = risk_loader
            .max_pos_u_for_symbol(open_venue, &self.symbol)
            .max(0.0)
            * risk_loader.max_symbol_exposure_ratio().max(0.0);
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
        debug!(
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

    fn send_hedge_orders(&mut self, venue: TradingVenue, symbol: &str) {
        let plans = self.hedge_plan.clone();
        let rate_params = PreTradeParamsLoader::instance();
        let symbol_key = min_qty_symbol_key(venue, symbol);
        let (qty_step, min_qty) = MonitorChannel::instance()
            .try_venue_min_qty_table(venue)
            .map(|table| {
                (
                    table.step_size(&symbol_key).unwrap_or(0.0),
                    table.min_qty(&symbol_key).unwrap_or(0.0),
                )
            })
            .unwrap_or((0.0, 0.0));
        let mut borrowed_open_count_10s = 0usize;
        let mut borrowed_open_count_1m = 0usize;
        for plan in plans {
            let (aligned_qty, dropped_qty) = align_final_order_qty(plan.qty, qty_step, min_qty);
            if aligned_qty <= 0.0 {
                warn!(
                    "MarketMakerHedgeStrategy: strategy_id={} skip hedge order after final qty align client_order_id={} venue={:?} symbol={} raw_qty={:.8} qty_step={:.8} min_qty={:.8} dropped_qty={:.8}",
                    self.strategy_id,
                    plan.client_order_id,
                    venue,
                    symbol,
                    plan.qty,
                    qty_step,
                    min_qty,
                    dropped_qty
                );
                continue;
            }
            if dropped_qty > 1e-12 {
                debug!(
                    "MarketMakerHedgeStrategy: strategy_id={} final hedge qty aligned client_order_id={} venue={:?} symbol={} raw_qty={:.8} aligned_qty={:.8} qty_step={:.8} dropped_qty={:.8}",
                    self.strategy_id,
                    plan.client_order_id,
                    venue,
                    symbol,
                    plan.qty,
                    aligned_qty,
                    qty_step,
                    dropped_qty
                );
            }

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
                    debug!(
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
                    warn!(
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
                .create_order_with_pending_limit_flag(
                    venue,
                    plan.client_order_id,
                    plan.order_type,
                    symbol.to_string(),
                    plan.side,
                    aligned_qty,
                    plan.price,
                    false,
                    1.0,
                    submit_ts,
                    false,
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
                    if let Err(e) = TradeEngHub::publish_order_request(exchange, &req_bin) {
                        warn!(
                            "MarketMakerHedgeStrategy: strategy_id={} send hedge order failed: exchange={} client_order_id={} err={}",
                            self.strategy_id, exchange, plan.client_order_id, e
                        );
                        self.release_hedge_order(plan.client_order_id, "send_failed");
                    } else {
                        let stats = OrderRateLimiter::record(
                            OrderRateBucket::MmHedge,
                            plan.client_order_id,
                            get_timestamp_us(),
                        );
                        debug!(
                            "MarketMakerHedgeStrategy: strategy_id={} MM hedge order action recorded client_order_id={} count_10s={} count_1m={}",
                            self.strategy_id, plan.client_order_id, stats.count_10s, stats.count_1m
                        );
                        debug!(
                            "MarketMakerHedgeStrategy: strategy_id={} hedge order sent: exchange={} client_order_id={} level_index={} side={:?} price={} qty={} from_key='{}'",
                            self.strategy_id,
                            exchange,
                            plan.client_order_id,
                            plan.level_index,
                            plan.side,
                            qv_decimal_or_fallback(plan.price),
                            qv_decimal_or_fallback(aligned_qty),
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
                    self.release_hedge_order(plan.client_order_id, "send_build_failed");
                }
            }
        }
    }

    fn uniform_hedge_publish_ctx(&self, client_order_id: i64) -> UniformPublishCtx {
        UniformPublishCtx {
            signal_ts: self.signal_ts,
            from_key: format!(
                "hedge|{}",
                String::from_utf8_lossy(&self.order_from_key_bytes(client_order_id))
            )
            .into_bytes(),
            price_offset: self.order_price_offset(client_order_id),
        }
    }

    fn publish_uniform_new_order(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
    ) {
        let ctx = self.uniform_hedge_publish_ctx(order.client_order_id);
        publish_uniform_new_order(
            order_update,
            order,
            prev_cumulative_filled_qty,
            &ctx,
            "MarketMakerHedgeStrategy",
            self.strategy_id,
        );
    }

    fn publish_uniform_terminal_order(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
    ) {
        let ctx = self.uniform_hedge_publish_ctx(order.client_order_id);
        publish_uniform_terminal_order(
            order_update,
            order,
            prev_cumulative_filled_qty,
            &ctx,
            "MarketMakerHedgeStrategy",
            self.strategy_id,
        );
    }

    fn publish_uniform_trade_order(
        &self,
        trade: &dyn TradeUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
        status: OrderStatus,
    ) {
        let ctx = self.uniform_hedge_publish_ctx(order.client_order_id);
        publish_uniform_trade_order(
            trade,
            order,
            prev_cumulative_filled_qty,
            status,
            &ctx,
            "MarketMakerHedgeStrategy",
            self.strategy_id,
        );
    }

    fn publish_uniform_trade_order_from_order_update(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
    ) {
        let ctx = self.uniform_hedge_publish_ctx(order.client_order_id);
        publish_uniform_trade_order_from_order_update(
            order_update,
            order,
            prev_cumulative_filled_qty,
            &ctx,
            "MarketMakerHedgeStrategy",
            self.strategy_id,
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
            drop(order_manager);
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
        let incoming_cumulative_filled_qty = order_update.cumulative_filled_quantity();
        let protected_cumulative_fill =
            current_order.protected_cumulative_fill(incoming_cumulative_filled_qty);
        if protected_cumulative_fill.rollback_detected {
            warn!(
                "MarketMakerHedgeStrategy: strategy_id={} protected cumulative fill rollback: client_order_id={} symbol={} status={:?} prev={:.8} incoming={:.8} effective={:.8}",
                self.strategy_id,
                current_order.client_order_id,
                current_order.symbol,
                status,
                current_order.cumulative_filled_quantity,
                incoming_cumulative_filled_qty,
                protected_cumulative_fill.effective_cum
            );
        }
        let effective_cumulative_filled_qty = protected_cumulative_fill.effective_cum;

        let updated = order_manager.update(client_order_id, |order| match status {
            OrderStatus::New => {
                order.status = OrderExecutionStatus::Create;
                order.set_exchange_order_id(order_update.order_id());
                order.set_create_time(order_update.event_time());
                debug!(
                    "✅ MMHedge订单已挂单: strategy_id={} client_order_id={} exchange_order_id={} symbol={} side={:?} price={} qty={}",
                    self.strategy_id,
                    client_order_id,
                    order_update.order_id(),
                    order.symbol,
                    order.side,
                    qv_decimal_or_fallback(order.price),
                    qv_decimal_or_fallback(order.quantity)
                );
            }
            OrderStatus::Canceled => {
                order.status = OrderExecutionStatus::Cancelled;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = effective_cumulative_filled_qty;
                order.set_end_time(order_update.event_time());
            }
            OrderStatus::Filled => {
                order.status = OrderExecutionStatus::Filled;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = effective_cumulative_filled_qty;
                order.set_filled_time(order_update.event_time());
                order.set_end_time(order_update.event_time());
            }
            OrderStatus::Expired | OrderStatus::ExpiredInMatch => {
                order.status = OrderExecutionStatus::Rejected;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = effective_cumulative_filled_qty;
                order.set_end_time(order_update.event_time());
            }
            OrderStatus::PartiallyFilled => {
                order.status = OrderExecutionStatus::Create;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = effective_cumulative_filled_qty;
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
            let filled_qty = MonitorChannel::instance()
                .order_manager()
                .borrow()
                .get(client_order_id)
                .map(|order| order.cumulative_filled_quantity)
                .unwrap_or(effective_cumulative_filled_qty);
            let fill_price =
                self.resolve_fill_price_from_order_update(client_order_id, order_update);
            let filled_base_qty = MonitorChannel::instance().qty_to_base(
                order_update.trading_venue(),
                order_update.symbol(),
                filled_qty,
            );
            let _applied = self.apply_tracked_hedge_fill_cumulative(
                client_order_id,
                order_update.event_time(),
                order_update.side(),
                filled_base_qty,
                fill_price,
                "hedge_fill",
            );
            self.release_hedge_order(client_order_id, "order_update_finished");
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
            drop(order_manager);
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
        let fill_price = self.resolve_fill_price_from_trade_update(
            order_snapshot.as_ref().map(|(_, _, _, price)| *price),
            trade,
        );
        let cumulative_base_qty = MonitorChannel::instance().qty_to_base(
            trade.trading_venue(),
            trade.symbol(),
            cumulative_qty,
        );
        let _applied = self.apply_tracked_hedge_fill_cumulative(
            client_order_id,
            event_time,
            trade.side(),
            cumulative_base_qty,
            fill_price,
            "hedge_fill",
        );
        self.release_hedge_order(client_order_id, "trade_update_filled");

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
    use super::{HedgeOrderMeta, MarketMakerHedgeStrategy};
    use crate::common::exchange::Exchange;
    use crate::pre_trade::order_manager::Side;
    use crate::strategy::hedge_order_reconcile::{
        HedgeOrderReconcileCommon, HedgeOrderReconcileState,
    };
    use crate::strategy::hedge_strategy_common::{mark_price_lookup_symbol, NET_EXPOSURE_EPS_USDT};
    use crate::strategy::order_reconcile::{monotonic_cumulative_fill, PendingOrderQueryReason};

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
    fn mark_price_lookup_symbol_uses_gate_contract_style() {
        assert_eq!(
            mark_price_lookup_symbol("SOLUSDT", Exchange::Gate),
            "SOL_USDT"
        );
        assert_eq!(
            mark_price_lookup_symbol("SOL_USDT", Exchange::Gate),
            "SOL_USDT"
        );
    }

    #[test]
    fn mark_price_lookup_symbol_keeps_internal_style_for_binance_like_exchanges() {
        assert_eq!(
            mark_price_lookup_symbol("SOLUSDT", Exchange::Binance),
            "SOLUSDT"
        );
        assert_eq!(
            mark_price_lookup_symbol("SOL_USDT", Exchange::Binance),
            "SOLUSDT"
        );
    }

    #[test]
    fn cancel_reconcile_reasons_are_classified() {
        assert!(HedgeOrderReconcileState::is_cancel_reconcile_reason(
            PendingOrderQueryReason::CancelFailed
        ));
        assert!(HedgeOrderReconcileState::is_cancel_reconcile_reason(
            PendingOrderQueryReason::CancelRejected
        ));
        assert!(!HedgeOrderReconcileState::is_cancel_reconcile_reason(
            PendingOrderQueryReason::OrderWatchdog
        ));
    }

    #[test]
    fn monotonic_cumulative_fill_keeps_local_value_on_rollback() {
        assert!((monotonic_cumulative_fill(4.2, 0.0) - 4.2).abs() < 1e-12);
    }

    #[test]
    fn monotonic_cumulative_fill_accepts_forward_progress() {
        assert!((monotonic_cumulative_fill(4.2, 5.6) - 5.6).abs() < 1e-12);
    }

    #[test]
    fn pending_order_query_reason_can_be_promoted_to_cancel_reconcile() {
        let mut strategy = MarketMakerHedgeStrategy::new(1, "ETHUSDT".to_string());
        let client_order_id = 42_i64;
        strategy
            .order_reconcile_state
            .pending_order_queries
            .insert(client_order_id, PendingOrderQueryReason::OrderWatchdog);
        strategy
            .order_reconcile_state
            .order_query_watchdogs
            .insert(client_order_id, (1, PendingOrderQueryReason::OrderWatchdog));

        assert!(strategy.send_order_query(client_order_id, PendingOrderQueryReason::CancelRejected));
        assert_eq!(
            strategy
                .order_reconcile_state
                .pending_order_queries
                .get(&client_order_id)
                .copied(),
            Some(PendingOrderQueryReason::CancelRejected)
        );
        assert_eq!(
            strategy
                .order_reconcile_state
                .order_query_watchdogs
                .get(&client_order_id)
                .map(|(_, reason)| *reason),
            Some(PendingOrderQueryReason::CancelRejected)
        );

        std::mem::forget(strategy);
    }

    #[test]
    fn release_hedge_order_clears_open_failed_tracking_state() {
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
            .order_reconcile_state
            .pending_order_queries
            .insert(client_order_id, PendingOrderQueryReason::CancelRejected);
        strategy.order_reconcile_state.order_query_watchdogs.insert(
            client_order_id,
            (123, PendingOrderQueryReason::CancelRejected),
        );
        strategy.release_hedge_order(client_order_id, "open_failed");

        assert!(!strategy.hedge_order_ids.contains(&client_order_id));
        assert!(!strategy.hedge_order_meta.contains_key(&client_order_id));
        assert!(!strategy
            .order_reconcile_state
            .pending_order_queries
            .contains_key(&client_order_id));
        assert!(!strategy
            .order_reconcile_state
            .order_query_watchdogs
            .contains_key(&client_order_id));

        std::mem::forget(strategy);
    }

    #[test]
    fn tracked_hedge_fill_books_full_cumulative_qty() {
        let mut strategy = MarketMakerHedgeStrategy::new(1, "ETHUSDT".to_string());
        let client_order_id = 42_i64;
        strategy.hedge_order_ids.insert(client_order_id);

        let applied = strategy.apply_tracked_hedge_fill_cumulative(
            client_order_id,
            123,
            Side::Sell,
            5.0,
            2500.0,
            "hedge_fill",
        );

        assert!(applied);
        assert_eq!(strategy.net_qty, -5.0);
        assert_eq!(strategy.net_qty_queue.net_qty(), -5.0);
        assert_eq!(strategy.net_qty_queue.len(), 1);
        assert_eq!(strategy.weighted_inventory_price(), 2500.0);

        std::mem::forget(strategy);
    }
}

impl HedgeOrderReconcileCommon for MarketMakerHedgeStrategy {
    fn hedge_reconcile_strategy_name(&self) -> &'static str {
        "MMHedge"
    }

    fn hedge_reconcile_strategy_id(&self) -> i32 {
        self.strategy_id
    }

    fn hedge_reconcile_state(&self) -> &HedgeOrderReconcileState {
        &self.order_reconcile_state
    }

    fn hedge_reconcile_state_mut(&mut self) -> &mut HedgeOrderReconcileState {
        &mut self.order_reconcile_state
    }

    fn is_hedge_order_tracked(&self, client_order_id: i64) -> bool {
        self.hedge_order_ids.contains(&client_order_id)
    }

    fn handoff_hedge_order_after_query_failure(
        &mut self,
        client_order_id: i64,
        reason: &str,
    ) -> bool {
        warn!(
            "MMHedgeReconcile: strategy_id={} orphan_handoff_start reason={} {}",
            self.strategy_id,
            reason,
            self.hedge_order_trace_snapshot(client_order_id)
        );

        let handoff = OrphanHandoff::from_hedge(
            client_order_id,
            self.strategy_id,
            self.uniform_hedge_publish_ctx(client_order_id),
            reason,
        );
        let Some(orphan_mgr) = MonitorChannel::try_orphan_strategy_mgr() else {
            warn!(
                "MarketMakerHedgeStrategy: strategy_id={} orphan manager unavailable client_order_id={} reason={}",
                self.strategy_id, client_order_id, reason
            );
            return false;
        };
        let adopted = orphan_mgr
            .borrow_mut()
            .adopt_orphan_order_id(OrphanStrategyRole::Mm, &handoff);
        if !adopted {
            warn!(
                "MarketMakerHedgeStrategy: strategy_id={} mm orphan handoff rejected client_order_id={} reason={}",
                self.strategy_id, client_order_id, reason
            );
            return false;
        }
        self.release_hedge_order_keep_local(client_order_id, "mm_orphan_handoff");

        warn!(
            "MarketMakerHedgeStrategy: strategy_id={} handoff hedge order to mm orphan adopted: client_order_id={} reason={}",
            self.strategy_id, client_order_id, reason
        );
        true
    }

    fn handle_hedge_open_failed(
        &mut self,
        response: &dyn TradeEngineResponse,
        _code_desc: &str,
        client_order_id: i64,
    ) {
        warn!(
            "MarketMakerHedgeStrategy: strategy_id={} trade open failed: req_type={} status={} code={} client_order_id={}",
            self.strategy_id,
            response.req_type(),
            response.status(),
            response.error_code(),
            client_order_id
        );
        self.release_hedge_order(client_order_id, "open_failed");
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

    fn has_order_terminal_recorder(&self) -> bool {
        true
    }

    fn order_terminal_recorder_mut(&mut self) -> Option<&mut dyn OrderTerminalRecorder> {
        Some(self)
    }

    fn is_strategy_order(&self, order_id: i64) -> bool {
        self.hedge_order_ids.contains(&order_id)
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
        self.apply_hedge_trade_engine_response_common(response);
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

impl OrderTerminalRecorder for MarketMakerHedgeStrategy {
    fn record_open_order_terminal(
        &mut self,
        terminal_ts: i64,
        side: Side,
        order_base_qty: f64,
        filled_base_qty: f64,
        price: f64,
        _close_ts: i64,
    ) -> bool {
        let order_base_qty = order_base_qty.abs();
        let filled_base_qty = filled_base_qty.abs();
        if filled_base_qty <= TERMINAL_QTY_EPS {
            return false;
        }
        let signed_base_qty = signed_qty_from_side(side, filled_base_qty);
        let buy_qty = if signed_base_qty > 0.0 {
            signed_base_qty
        } else {
            0.0
        };
        let sell_qty = if signed_base_qty < 0.0 {
            -signed_base_qty
        } else {
            0.0
        };
        info!(
            "MMHedgeRecord: strategy_id={} symbol={} leg=open side={:?} order_base_qty={:.8} filled_base_qty={:.8} qv={:.8} price={:.8} terminal_ts={}",
            self.get_id(),
            self.symbol().unwrap_or(""),
            side,
            order_base_qty,
            filled_base_qty,
            signed_base_qty,
            price,
            terminal_ts
        );
        self.apply_net_qty_fill(terminal_ts, signed_base_qty, price, "open_order_terminal");
        self.period_buy_qty += buy_qty;
        self.period_sell_qty += sell_qty;
        // MM open terminal 计入当前 period 的买/卖成交量，后续 hedge query 会带上这段增量。
        true
    }

    fn record_hedge_order_terminal(
        &mut self,
        terminal_ts: i64,
        side: Side,
        order_base_qty: f64,
        filled_base_qty: f64,
        price: f64,
    ) -> bool {
        let order_base_qty = order_base_qty.abs();
        let filled_base_qty = filled_base_qty.abs();
        if order_base_qty <= TERMINAL_QTY_EPS && filled_base_qty <= TERMINAL_QTY_EPS {
            return false;
        }
        let signed_base_qty = signed_qty_from_side(side, filled_base_qty);
        info!(
            "MMHedgeRecord: strategy_id={} symbol={} leg=hedge side={:?} order_base_qty={:.8} filled_base_qty={:.8} qv={:.8} price={:.8} terminal_ts={}",
            self.get_id(),
            self.symbol().unwrap_or(""),
            side,
            order_base_qty,
            filled_base_qty,
            signed_base_qty,
            price,
            terminal_ts
        );
        // MM hedge terminal 只更新净库存；period 买/卖量只由 open/external fill 累加。
        if filled_base_qty > TERMINAL_QTY_EPS {
            self.apply_net_qty_fill(terminal_ts, signed_base_qty, price, "hedge_order_terminal");
        }
        true
    }
}

impl Drop for MarketMakerHedgeStrategy {
    fn drop(&mut self) {
        self.alive_flag = false;
        self.cleanup_strategy_orders();
    }
}
