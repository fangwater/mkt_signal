use crate::common::time_util::get_timestamp_us;
use crate::market_maker::hedge_split::{
    split_hedge_orders_round_robin, HedgeLevel, HedgeSplitOrder,
};
use crate::market_maker::order_align::{contract_qty_multiplier, min_qty_symbol_key};
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus, OrderManager, OrderType, Side};
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::pre_trade::signal_channel::SignalChannel;
use crate::pre_trade::{PersistChannel, QueryEngHub, TradeEngHub};
use crate::signal::common::{ExecutionType, OrderStatus, SignalBytes, TimeInForce, TradingVenue};
use crate::signal::hedge_signal::{MmHedgeCtx, MmHedgeSignalQueryMsg};
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::manager::{ForceCloseControl, Strategy};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::query_engine_response::QueryEngineResponse;
use crate::strategy::query_order_updates::{OrderQueryOrderUpdate, OrderQueryTradeUpdate};
use crate::strategy::trade_engine_response::{TradeEngineResponse, TradeRequestKind};
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::uniform_order_helper::{publish_uniform_order_event, UniformOrderEventKind};
use crate::strategy::ws_order_update::WsOrderUpdate;
use crate::trade_engine::query_parsers::compact_order::{
    CompactOrderQueryResp, COMPACT_ORDER_QUERY_RESP_LEN,
};
use crate::trade_engine::query_request::{GenericQueryRequest, QueryRequestType};
use crate::trade_engine::trade_request::TradeRequestType;
use log::{debug, info, warn};
use std::any::Any;
use std::collections::{HashMap, HashSet};

const HEDGE_QUERY_INTERVAL_US: i64 = 30_000_000;
const HEDGE_QUERY_WATCHDOG_US: i64 = 30_000;
const ORDER_QUERY_WATCHDOG_DELAY_US: i64 = 300_000;

#[derive(Debug, Clone)]
pub struct MmHedgeSnapshot {
    pub symbol: String,
    pub net_qty: f64,
    pub buy_qty: f64,
    pub sell_qty: f64,
}

#[derive(Debug, Clone)]
pub struct HedgePlanOrder {
    pub client_order_id: i64,
    pub side: Side,
    pub price: f64,
    pub qty: f64,
}

/// 做市对冲策略（每个 symbol 仅一个实例）
pub struct MarketMakerHedgeStrategy {
    strategy_id: i32,
    symbol: String,
    net_qty: f64,
    period_buy_qty: f64,
    period_sell_qty: f64,
    signal_ts: i64,
    hedge_from_key: Vec<u8>,
    order_seq: u32,
    hedge_plan: Vec<HedgePlanOrder>,
    hedge_order_ids: HashSet<i64>,
    alive_flag: bool,
    next_query_ts_us: i64,
    pending_query: bool,
    query_watchdog_due_ts: i64,
    pending_order_queries: HashMap<i64, PendingOrderQueryReason>,
    order_query_watchdogs: HashMap<i64, i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingOrderQueryReason {
    OrderWatchdog,
    CancelFailed,
}

impl MarketMakerHedgeStrategy {
    pub fn new(strategy_id: i32, symbol: String) -> Self {
        let now = get_timestamp_us();
        Self {
            strategy_id,
            symbol,
            net_qty: 0.0,
            period_buy_qty: 0.0,
            period_sell_qty: 0.0,
            signal_ts: 0,
            hedge_from_key: Vec::new(),
            order_seq: 0,
            hedge_plan: Vec::new(),
            hedge_order_ids: HashSet::new(),
            alive_flag: true,
            next_query_ts_us: now.saturating_add(HEDGE_QUERY_INTERVAL_US),
            pending_query: false,
            query_watchdog_due_ts: 0,
            pending_order_queries: HashMap::new(),
            order_query_watchdogs: HashMap::new(),
        }
    }

    /// 组合订单ID：高32位为策略ID，低32位为序列号
    fn compose_order_id(strategy_id: i32, seq: u32) -> i64 {
        ((strategy_id as i64) << 32) | seq as i64
    }

    fn extract_strategy_id(order_id: i64) -> i32 {
        (order_id >> 32) as i32
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

    /// 累加成交（使用 base qty 口径）
    pub fn record_fill(&mut self, signed_qty: f64, buy_qty: f64, sell_qty: f64) {
        self.net_qty += signed_qty;
        self.period_buy_qty += buy_qty;
        self.period_sell_qty += sell_qty;
    }

    pub fn snapshot(&self) -> MmHedgeSnapshot {
        MmHedgeSnapshot {
            symbol: self.symbol.clone(),
            net_qty: self.net_qty,
            buy_qty: self.period_buy_qty,
            sell_qty: self.period_sell_qty,
        }
    }

    fn handle_mm_hedge_signal(&mut self, ctx: MmHedgeCtx) {
        self.signal_ts = ctx.signal_ts;
        self.hedge_from_key = ctx.from_key.clone();
        self.next_query_ts_us = ctx.next_query_ts;
        self.pending_query = false;
        self.query_watchdog_due_ts = 0;

        // 打印所有 MM 对冲策略的累积头寸（净头寸 + 买/卖累计）
        let strategy_mgr = MonitorChannel::instance().strategy_mgr();
        let snapshots = strategy_mgr.borrow().mm_hedge_snapshots();
        for snap in snapshots {
            info!(
                "MMHedge snapshot: symbol={} net={:.8} buy={:.8} sell={:.8}",
                snap.symbol, snap.net_qty, snap.buy_qty, snap.sell_qty
            );
        }
        let from_key = String::from_utf8_lossy(&self.hedge_from_key);
        info!(
            "MMHedge ctx: symbol={} price_tick_exp={} qty_tick_exp={} price_levels={} amount_levels={} signal_ts={} next_query_ts={} from_key='{}'",
            ctx.get_opening_symbol(),
            ctx.price_tick_exp,
            ctx.qty_tick_exp,
            ctx.price_list.len(),
            ctx.amount_list.len(),
            ctx.signal_ts,
            ctx.next_query_ts,
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

        let price_tick = 10f64.powi(ctx.price_tick_exp);
        let qty_tick = 10f64.powi(ctx.qty_tick_exp);
        let levels: Vec<HedgeLevel> = ctx
            .price_list
            .iter()
            .zip(ctx.amount_list.iter())
            .filter_map(|(price_tick_level, amount_tick)| {
                let qty_venue = (*amount_tick as f64) * qty_tick;
                if qty_venue <= 0.0 {
                    return None;
                }
                Some(HedgeLevel {
                    price: (*price_tick_level as f64) * price_tick,
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
        info!(
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

        self.hedge_plan.clear();
        for HedgeSplitOrder { side, price, qty } in split.orders {
            if self.order_seq >= u32::MAX {
                self.order_seq = 1;
            } else {
                self.order_seq += 1;
                if self.order_seq == 0 {
                    self.order_seq = 1;
                }
            }
            let client_order_id = Self::compose_order_id(self.strategy_id, self.order_seq);
            self.hedge_plan.push(HedgePlanOrder {
                client_order_id,
                side,
                price,
                qty,
            });
        }

        let mut table = String::new();
        table.push_str("+----------------------+--------------+--------------+--------------+\n");
        table.push_str("| client_order_id      | price        | qty          | usdt         |\n");
        table.push_str("+----------------------+--------------+--------------+--------------+\n");
        for row in &self.hedge_plan {
            let usdt = row.price * row.qty;
            table.push_str(&format!(
                "| {:>20} | {:>12.6} | {:>12.6} | {:>12.6} |\n",
                row.client_order_id, row.price, row.qty, usdt
            ));
        }
        table.push_str("+----------------------+--------------+--------------+--------------+");

        let hedge_side_str = match hedge_side {
            Some(Side::Buy) => "BUY",
            Some(Side::Sell) => "SELL",
            None => "FLAT",
        };
        info!(
            "MMHedge split: symbol={} side={} net_qty={:.8} total_qty={:.8} total_usdt={:.8} remain_qty={:.8}\n{}",
            ctx.get_opening_symbol(),
            hedge_side_str,
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

        if self.request_cancel_for_hedge_orders() {
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

        let order_mgr = MonitorChannel::instance().order_manager();
        let mgr = order_mgr.borrow();
        let mut to_remove: Vec<i64> = Vec::new();
        let mut to_cancel: Vec<i64> = Vec::new();
        for order_id in &self.hedge_order_ids {
            match mgr.get(*order_id) {
                Some(order) => {
                    if order.status.is_terminal() {
                        to_remove.push(*order_id);
                    } else {
                        to_cancel.push(*order_id);
                    }
                }
                None => {
                    to_remove.push(*order_id);
                }
            }
        }
        drop(mgr);

        for order_id in to_remove {
            self.hedge_order_ids.remove(&order_id);
        }

        if self.hedge_order_ids.is_empty() {
            return false;
        }

        for order_id in to_cancel {
            let order_mgr = MonitorChannel::instance().order_manager();
            let mgr = order_mgr.borrow();
            let Some(order) = mgr.get(order_id) else {
                continue;
            };
            let exchange = order.venue.trade_engine_exchange();
            match order.get_order_cancel_bytes() {
                Ok(req_bin) => {
                    if let Err(e) = TradeEngHub::publish_order_request(exchange, &req_bin) {
                        warn!(
                            "MarketMakerHedgeStrategy: strategy_id={} cancel hedge order failed: exchange={} client_order_id={} err={}",
                            self.strategy_id, exchange, order_id, e
                        );
                    } else {
                        info!(
                            "MarketMakerHedgeStrategy: strategy_id={} cancel hedge order sent: exchange={} client_order_id={}",
                            self.strategy_id, exchange, order_id
                        );
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

        true
    }

    fn send_hedge_query(&mut self) {
        // 定时发送对冲查询（携带 symbol + 当期买/卖成交 + 累计净头寸）
        let risk_loader = PreTradeParamsLoader::instance();
        let symbol_exposure_u =
            risk_loader.max_pos_u().max(0.0) * risk_loader.max_symbol_exposure_ratio().max(0.0);
        let query_msg = MmHedgeSignalQueryMsg::new(
            &self.symbol,
            self.period_buy_qty,
            self.period_sell_qty,
            self.net_qty,
            symbol_exposure_u,
        );
        let send_result = SignalChannel::with(|ch| ch.publish_backward(&query_msg.to_bytes()));
        match send_result {
            Ok(true) => {
                debug!(
                    "MarketMakerHedgeStrategy: strategy_id={} send hedge query ok symbol={}",
                    self.strategy_id, self.symbol
                );
                let now = get_timestamp_us();
                self.pending_query = true;
                self.query_watchdog_due_ts = now.saturating_add(HEDGE_QUERY_WATCHDOG_US);
            }
            Ok(false) => {
                warn!(
                    "MarketMakerHedgeStrategy: backward publisher 未配置，无法发送对冲查询 symbol={}",
                    self.symbol
                );
                self.pending_query = false;
                self.query_watchdog_due_ts = 0;
            }
            Err(err) => {
                warn!(
                    "MarketMakerHedgeStrategy: 发送对冲查询失败 symbol={} err={:#}",
                    self.symbol, err
                );
                self.pending_query = false;
                self.query_watchdog_due_ts = 0;
            }
        }
    }

    fn schedule_order_query_watchdog(&mut self, client_order_id: i64) {
        let due = get_timestamp_us().saturating_add(ORDER_QUERY_WATCHDOG_DELAY_US);
        self.order_query_watchdogs.insert(client_order_id, due);
    }

    fn clear_order_query_state(&mut self, client_order_id: i64) {
        self.order_query_watchdogs.remove(&client_order_id);
        self.pending_order_queries.remove(&client_order_id);
    }

    fn handle_order_query_watchdogs(&mut self) {
        if self.order_query_watchdogs.is_empty() {
            return;
        }
        let now = get_timestamp_us();
        let mut due_ids: Vec<i64> = Vec::new();
        for (client_order_id, due_ts) in &self.order_query_watchdogs {
            if now >= *due_ts {
                due_ids.push(*client_order_id);
            }
        }
        if due_ids.is_empty() {
            return;
        }

        let order_mgr = MonitorChannel::instance().order_manager();
        for client_order_id in due_ids {
            self.order_query_watchdogs.remove(&client_order_id);
            let order_opt = order_mgr.borrow().get(client_order_id);
            if let Some(order) = order_opt.as_ref().filter(|o| !o.status.is_terminal()) {
                let scheduled_at = now.saturating_sub(ORDER_QUERY_WATCHDOG_DELAY_US);
                let waited_ms = now.saturating_sub(scheduled_at).saturating_div(1_000);
                info!(
                    "MMHedge OrderWatchdog触发: strategy_id={} client_order_id={} symbol={} status={:?} exch_ord_id={:?} 等待{}ms仍未收到回报，发送order query回补",
                    self.strategy_id,
                    client_order_id,
                    order.symbol,
                    order.status,
                    order.exchange_order_id,
                    waited_ms
                );
                let _ =
                    self.send_order_query(client_order_id, PendingOrderQueryReason::OrderWatchdog);
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
                debug!(
                    "MarketMakerHedgeStrategy: strategy_id={} order query sent: exchange={} client_order_id={} reason={:?}",
                    self.strategy_id, exchange, client_order_id, reason
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

        info!(
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
            self.schedule_order_query_watchdog(client_order_id);
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
            }
            OrderExecutionStatus::Rejected | OrderExecutionStatus::Commit => {
                warn!(
                    "MarketMakerHedgeStrategy: strategy_id={} query shows {:?} client_order_id={} reason={:?}",
                    self.strategy_id, st, client_order_id, reason
                );
            }
        }
    }

    fn send_hedge_orders(&mut self, venue: TradingVenue, symbol: &str) {
        let plans = self.hedge_plan.clone();
        for plan in plans {
            let submit_ts = get_timestamp_us();
            MonitorChannel::instance()
                .order_manager()
                .borrow_mut()
                .create_order(
                    venue,
                    plan.client_order_id,
                    OrderType::Limit,
                    symbol.to_string(),
                    plan.side,
                    plan.qty,
                    plan.price,
                    false,
                    1.0,
                    submit_ts,
                );
            self.hedge_order_ids.insert(plan.client_order_id);

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
                    } else {
                        info!(
                            "MarketMakerHedgeStrategy: strategy_id={} hedge order sent: exchange={} client_order_id={} side={:?} price={:.8} qty={:.8}",
                            self.strategy_id,
                            exchange,
                            plan.client_order_id,
                            plan.side,
                            plan.price,
                            plan.qty
                        );
                        self.schedule_order_query_watchdog(plan.client_order_id);
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

    fn uniform_hedge_fields(&self) -> (i64, Vec<u8>, f64) {
        (
            self.signal_ts,
            format!("hedge|{}", String::from_utf8_lossy(&self.hedge_from_key)).into_bytes(),
            0.0,
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
        let (signal_ts, from_key, price_offset) = self.uniform_hedge_fields();
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
        let (signal_ts, from_key, price_offset) = self.uniform_hedge_fields();
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
        let (signal_ts, from_key, price_offset) = self.uniform_hedge_fields();
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
        let (signal_ts, from_key, price_offset) = self.uniform_hedge_fields();
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
                info!(
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

        if status.is_finished() && self.hedge_order_ids.remove(&client_order_id) {
            let filled_qty = order_update.cumulative_filled_quantity();
            if filled_qty <= 0.0 {
                if status == OrderStatus::Canceled {
                    info!(
                        "🚫 MMHedge订单撤销: strategy_id={} client_order_id={} exchange_order_id={} filled=0",
                        self.strategy_id,
                        client_order_id,
                        order_update.order_id()
                    );
                }
                return true;
            }
            let base_qty = MonitorChannel::instance().qty_to_base(
                order_update.trading_venue(),
                order_update.symbol(),
                filled_qty,
            );
            if base_qty <= 0.0 {
                return true;
            }
            let signed_qty = match order_update.side() {
                Side::Buy => base_qty,
                Side::Sell => -base_qty,
            };
            let before = self.net_qty;
            self.net_qty += signed_qty;
            let action = match status {
                OrderStatus::Canceled => "撤销",
                OrderStatus::Filled => "完全成交",
                _ => "更新",
            };
            info!(
                "✅ MMHedge订单{}: strategy_id={} client_order_id={} symbol={} side={:?} filled={:.8} base_filled={:.8} net_before={:.8} net_after={:.8}",
                action,
                self.strategy_id,
                client_order_id,
                order_update.symbol(),
                order_update.side(),
                filled_qty,
                base_qty,
                before,
                self.net_qty
            );
        }

        true
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) -> bool {
        let client_order_id = trade.client_order_id();
        self.clear_order_query_state(client_order_id);

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

        let updated = order_manager.update(client_order_id, |order| {
            order.cumulative_filled_quantity = cumulative_qty;
            order.set_filled_time(trade_time);
            order.set_exchange_order_id(trade.order_id());
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

        if let Some(order) = order_manager.get(client_order_id) {
            self.publish_uniform_trade_order(trade, &order, prev_cumulative_filled_qty, status);
        }

        drop(order_manager);

        if status != OrderStatus::Filled {
            return true;
        }

        if !self.hedge_order_ids.remove(&client_order_id) {
            return true;
        }
        if cumulative_qty <= 0.0 {
            return true;
        }
        let base_qty = MonitorChannel::instance().qty_to_base(
            trade.trading_venue(),
            trade.symbol(),
            cumulative_qty,
        );
        if base_qty <= 0.0 {
            return true;
        }
        let signed_qty = match trade.side() {
            Side::Buy => base_qty,
            Side::Sell => -base_qty,
        };
        let before = self.net_qty;
        self.net_qty += signed_qty;
        info!(
            "✅ MMHedge订单完全成交: strategy_id={} client_order_id={} symbol={} side={:?} filled={:.8} base_filled={:.8} net_before={:.8} net_after={:.8}",
            self.strategy_id,
            client_order_id,
            trade.symbol(),
            trade.side(),
            cumulative_qty,
            base_qty,
            before,
            self.net_qty
        );

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
                warn!(
                    "MarketMakerHedgeStrategy: strategy_id={} ws open failed: req_type={} status={} code={} client_order_id={}",
                    self.strategy_id,
                    req_type,
                    response.status(),
                    response.error_code(),
                    response.client_order_id()
                );
            }
            TradeRequestKind::Cancel => {
                warn!(
                    "MarketMakerHedgeStrategy: strategy_id={} ws cancel failed: req_type={} status={} code={} client_order_id={}",
                    self.strategy_id,
                    req_type,
                    response.status(),
                    response.error_code(),
                    response.client_order_id()
                );
                let client_order_id = response.client_order_id();
                let sent =
                    self.send_order_query(client_order_id, PendingOrderQueryReason::CancelFailed);
                if !sent {
                    self.hedge_order_ids.remove(&client_order_id);
                    if let Some(order_mgr) = MonitorChannel::try_order_manager() {
                        let _ = order_mgr.borrow_mut().remove(client_order_id);
                    }
                }
            }
            TradeRequestKind::Other => {}
        }
    }

    fn apply_query_engine_response(&mut self, response: &dyn QueryEngineResponse) {
        let client_order_id = response.client_query_id();
        let Some(reason) = self.pending_order_queries.remove(&client_order_id) else {
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
            if reason == PendingOrderQueryReason::CancelFailed {
                self.hedge_order_ids.remove(&client_order_id);
                if let Some(order_mgr) = MonitorChannel::try_order_manager() {
                    let _ = order_mgr.borrow_mut().remove(client_order_id);
                }
                return;
            }
            self.schedule_order_query_watchdog(client_order_id);
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
            if reason == PendingOrderQueryReason::CancelFailed {
                self.hedge_order_ids.remove(&client_order_id);
                if let Some(order_mgr) = MonitorChannel::try_order_manager() {
                    let _ = order_mgr.borrow_mut().remove(client_order_id);
                }
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
