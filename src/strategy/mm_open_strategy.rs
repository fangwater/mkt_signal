use crate::common::tick_math::QuantizedValue;
use crate::common::time_util::get_timestamp_us;
use crate::common::trade_error_code::describe_trade_error_code;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus, OrderManager, OrderType, Side};
use crate::pre_trade::{PersistChannel, QueryEngHub, TradeEngHub};
use crate::signal::cancel_signal::MmCancelCtx;
use crate::signal::common::{ExecutionType, OrderStatus, SignalBytes, TimeInForce, TradingVenue};
use crate::signal::open_signal::MmOpenCtx;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::manager::MmOpenPriceMapEntry;
use crate::strategy::manager::{ForceCloseControl, Strategy};
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
use log::{debug, error, info, warn};
use std::any::Any;

const ORDER_QUERY_WATCHDOG_DELAY_US: i64 = 300_000;
const ORDER_QUERY_RETRY_DELAY_US: i64 = 900_000;
const CANCEL_QUERY_WATCHDOG_DELAY_US: i64 = 300_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingOrderQueryReason {
    OrderWatchdog,
    CancelWatchdog,
    CancelRejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QueryWatchdog {
    client_order_id: i64,
    due_ts_us: i64,
    reason: PendingOrderQueryReason,
}

/// 做市开仓策略：仅处理开仓，不涉及对冲/强平/模式切换
pub struct MarketMakerOpenStrategy {
    strategy_id: i32,
    open_symbol: String,
    open_order_id: i64,
    open_expire_ts: Option<i64>,
    open_side: Option<Side>,
    signal_ts: i64,
    open_from_key: String,
    open_price_qv: QuantizedValue,
    open_price_offset: f64,
    alive_flag: bool,
    recorded_to_hedge: bool,
    open_order_query_retried: bool,
    pending_order_query: Option<PendingOrderQueryReason>,
    order_query_watchdog: Option<QueryWatchdog>,
    cancel_query_watchdog: Option<QueryWatchdog>,
}

impl MarketMakerOpenStrategy {
    pub fn new(strategy_id: i32) -> Self {
        Self {
            strategy_id,
            open_symbol: String::new(),
            open_order_id: 0,
            open_expire_ts: None,
            open_side: None,
            signal_ts: 0,
            open_from_key: String::new(),
            open_price_qv: QuantizedValue::zero(),
            open_price_offset: 0.0,
            alive_flag: true,
            recorded_to_hedge: false,
            open_order_query_retried: false,
            pending_order_query: None,
            order_query_watchdog: None,
            cancel_query_watchdog: None,
        }
    }

    /// 组合订单ID：高32位为策略ID，低32位为序列号
    fn compose_order_id(strategy_id: i32) -> i64 {
        ((strategy_id as i64) << 32) | 1
    }

    /// 从订单ID中提取策略ID
    fn extract_strategy_id(order_id: i64) -> i32 {
        (order_id >> 32) as i32
    }

    fn preview_text(raw: &str, max_chars: usize) -> String {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return "-".to_string();
        }
        let mut out = String::new();
        for (idx, ch) in trimmed.chars().enumerate() {
            if idx >= max_chars {
                out.push_str("...");
                break;
            }
            out.push(ch);
        }
        out
    }

    fn cleanup_strategy_orders(&mut self) {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };
        let mut mgr = order_mgr.borrow_mut();

        if self.open_order_id != 0 {
            if let Some(order) = mgr.get(self.open_order_id) {
                if !order.status.is_terminal() {
                    mgr.log_order_details(&order, "开仓订单未达到终结状态被清理", self.strategy_id);
                }
            }
            let _ = mgr.remove(self.open_order_id);
        }
    }

    fn try_apply_ws_order_update(&mut self, response: &dyn TradeEngineResponse) -> bool {
        if !WsOrderUpdate::supports_trade_response_req_type(response.req_type()) {
            return false;
        }

        let client_order_id = response.client_order_id();
        if client_order_id != self.open_order_id {
            return false;
        }
        let order_mgr = MonitorChannel::instance().order_manager();
        let Some(order_snapshot) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} ws order update missing local order: client_order_id={}",
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
                    "MarketMakerOpenStrategy: strategy_id={} skip non-NEW/CANCELED binance ws response: venue={:?} client_order_id={} status={:?}",
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

    fn handle_mm_open_signal(&mut self, ctx: MmOpenCtx) {
        let symbol = ctx.get_opening_symbol().to_uppercase();
        if symbol.is_empty() {
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} empty symbol",
                self.strategy_id
            );
            self.alive_flag = false;
            return;
        }

        let Some(venue) = TradingVenue::from_u8(ctx.opening_leg.venue) else {
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} invalid venue={}",
                self.strategy_id, ctx.opening_leg.venue
            );
            self.alive_flag = false;
            return;
        };

        let Some(side) = Side::from_u8(ctx.side) else {
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} invalid side={}",
                self.strategy_id, ctx.side
            );
            self.alive_flag = false;
            return;
        };

        let Some(order_type) = OrderType::from_u8(ctx.order_type) else {
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} invalid order_type={}",
                self.strategy_id, ctx.order_type
            );
            self.alive_flag = false;
            return;
        };

        let qty = ctx.amount_value();
        if qty <= 0.0 {
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} invalid qty={}",
                self.strategy_id, qty
            );
            self.alive_flag = false;
            return;
        }

        let signal_price = ctx.price_value();
        if signal_price <= 0.0 {
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} invalid price={} order_type={:?}",
                self.strategy_id, signal_price, order_type
            );
            self.alive_flag = false;
            return;
        }

        if ctx.price_count() <= 0 || ctx.amount_count() <= 0 {
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} invalid MMOpen qv count price_count={} amount_count={}",
                self.strategy_id,
                ctx.price_count(),
                ctx.amount_count()
            );
            self.alive_flag = false;
            return;
        }

        // 目前只支持 Binance / OKX / Gate 的 margin + futures，其它 venue 直接 panic
        match venue {
            TradingVenue::BinanceFutures
            | TradingVenue::BinanceMargin
            | TradingVenue::OkexFutures
            | TradingVenue::OkexMargin
            | TradingVenue::GateFutures
            | TradingVenue::GateMargin => {}
            _ => {
                panic!(
                    "MarketMakerOpenStrategy: strategy_id={} 不支持的交易场所 {:?}，仅支持 Binance/OKX/Gate 的 futures 或 margin",
                    self.strategy_id, venue
                );
            }
        }

        // 1、检查symbol敞口
        if let Err(e) = MonitorChannel::instance().check_symbol_exposure(&symbol) {
            error!(
                "MarketMakerOpenStrategy: strategy_id={} symbol={} 单品种敞口风控检查失败: {}，标记策略为不活跃",
                self.strategy_id, symbol, e
            );
            self.alive_flag = false;
            return;
        }

        // 2、检查总敞口
        if let Err(e) = MonitorChannel::instance().check_total_exposure() {
            error!(
                "MarketMakerOpenStrategy: strategy_id={} 总敞口风控检查失败: {}，标记策略为不活跃",
                self.strategy_id, e
            );
            self.alive_flag = false;
            return;
        }

        // 3、检查限价挂单数量限制（如果是限价单）
        if order_type == OrderType::Limit {
            if let Err(e) = MonitorChannel::instance().check_pending_limit_order(&symbol) {
                error!(
                    "MarketMakerOpenStrategy: strategy_id={} symbol={} 限价挂单数量风控检查失败: {}，标记策略为不活跃",
                    self.strategy_id, symbol, e
                );
                self.alive_flag = false;
                return;
            }
        }

        // 4、信号已完成量价对齐，直接使用
        let order_qty = qty;
        let order_price = signal_price;
        let signed_qty = match side {
            Side::Buy => order_qty.abs(),
            Side::Sell => -order_qty.abs(),
        };

        // 5、检查杠杆：若绝对持仓不增加，则可跳过
        let add_base_qty = MonitorChannel::instance().qty_to_base(venue, &symbol, signed_qty);
        let current_base_qty = MonitorChannel::instance().get_position_qty(&symbol, venue);
        let projected_base_qty = current_base_qty + add_base_qty;
        let reduce_eps = 1e-12_f64;

        if projected_base_qty.abs() > current_base_qty.abs() + reduce_eps {
            if let Err(e) = MonitorChannel::instance().check_leverage() {
                error!(
                    "MarketMakerOpenStrategy: strategy_id={} 杠杆风控检查失败: {}，标记策略为不活跃",
                    self.strategy_id, e
                );
                self.alive_flag = false;
                return;
            }
        }

        // 6、检查 max_pos_u
        if let Err(e) =
            MonitorChannel::instance().ensure_max_pos_u(&symbol, signed_qty, order_price)
        {
            error!(
                "MarketMakerOpenStrategy: strategy_id={} 仓位限制检查失败: {}，标记策略为不活跃",
                self.strategy_id, e
            );
            self.alive_flag = false;
            return;
        }

        self.open_symbol = symbol.clone();
        self.open_expire_ts = if ctx.exp_time > 0 {
            Some(ctx.exp_time)
        } else {
            None
        };
        self.open_side = Some(side);
        self.signal_ts = ctx.create_ts;
        self.open_from_key = String::from_utf8_lossy(&ctx.from_key).to_string();
        self.open_price_qv = ctx.price_qv;
        self.open_price_offset = ctx.price_offset;

        let client_order_id = Self::compose_order_id(self.strategy_id);
        self.open_order_id = client_order_id;
        self.open_order_query_retried = false;

        let submit_ts = get_timestamp_us();
        MonitorChannel::instance()
            .order_manager()
            .borrow_mut()
            .create_order(
                venue,
                client_order_id,
                order_type,
                symbol.clone(),
                side,
                order_qty,
                order_price,
                false,
                1.0,
                submit_ts,
            );

        info!(
            "📤 MM开仓订单已创建: strategy_id={} client_order_id={} symbol={} {:?} side={:?} qty={:.4} price={:.6} from_key_len={}",
            self.strategy_id,
            client_order_id,
            symbol,
            venue,
            side,
            order_qty,
            order_price,
            ctx.from_key_len
        );

        if let Err(err) = self.create_and_send_order(client_order_id, &symbol) {
            error!(
                "MarketMakerOpenStrategy: strategy_id={} open order send failed: {}",
                self.strategy_id, err
            );
        } else {
            info!(
                "✅ MM开仓订单已发送: strategy_id={} client_order_id={}",
                self.strategy_id, client_order_id
            );
        }
    }

    fn handle_open_leg_timeout(&mut self) {
        let Some(expire_ts) = self.open_expire_ts else {
            return;
        };
        let now = get_timestamp_us();
        if now < expire_ts || !self.alive_flag || self.open_order_id == 0 {
            return;
        }

        info!(
            "MarketMakerOpenStrategy: strategy_id={} 开仓订单超时，直接撤单 order_id={}",
            self.strategy_id, self.open_order_id
        );

        let order = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(self.open_order_id);
        if let Some(order) = order {
            match order.get_order_cancel_bytes() {
                Ok(cancel_bytes) => {
                    let exchange = order.venue.trade_engine_exchange();
                    if let Err(e) = TradeEngHub::publish_order_request(exchange, &cancel_bytes) {
                        info!(
                            "MarketMakerOpenStrategy: strategy_id={} exchange={} 发送开仓撤单请求失败: {}",
                            self.strategy_id, exchange, e
                        );
                    } else {
                        info!(
                            "MarketMakerOpenStrategy: strategy_id={} 已发送开仓撤单请求 order_id={}",
                            self.strategy_id, self.open_order_id
                        );
                        self.open_expire_ts = None;
                        self.schedule_cancel_query_watchdog(self.open_order_id);
                    }
                }
                Err(e) => {
                    error!(
                        "MarketMakerOpenStrategy: strategy_id={} 获取开仓撤单请求字节失败: {}",
                        self.strategy_id, e
                    );
                }
            }
        }
    }

    fn handle_mm_cancel_signal(&mut self, ctx: MmCancelCtx) {
        let precise_target = ctx.strategy_id > 0;
        let from_key_preview =
            Self::preview_text(&String::from_utf8_lossy(&ctx.from_key), 160);
        if precise_target {
            if ctx.strategy_id != self.strategy_id {
                info!(
                    "MarketMakerOpenStrategy: strategy_id={} ignore targeted MMCancel target_strategy_id={} trigger_ts={} from_key='{}'",
                    self.strategy_id,
                    ctx.strategy_id,
                    ctx.trigger_ts,
                    from_key_preview
                );
                return;
            }
        } else {
            if ctx.client_order_id > 0 && ctx.client_order_id != self.open_order_id {
                info!(
                    "MarketMakerOpenStrategy: strategy_id={} ignore MMCancel due to client_order_id mismatch signal_client_order_id={} open_order_id={} trigger_ts={} from_key='{}'",
                    self.strategy_id,
                    ctx.client_order_id,
                    self.open_order_id,
                    ctx.trigger_ts,
                    from_key_preview
                );
                return;
            }

            let cancel_side = ctx.get_side();
            if let Some(open_side) = self.open_side {
                if open_side != cancel_side {
                    info!(
                        "MarketMakerOpenStrategy: strategy_id={} skip MMCancel due to side mismatch open_side={:?} cancel_side={:?} open_order_id={} trigger_ts={} from_key='{}'",
                        self.strategy_id,
                        open_side,
                        cancel_side,
                        self.open_order_id,
                        ctx.trigger_ts,
                        from_key_preview
                    );
                    return;
                }
            }
        }

        info!(
            "MarketMakerOpenStrategy: strategy_id={} processing MMCancel precise={} open_order_id={} trigger_ts={} from_key='{}'",
            self.strategy_id,
            precise_target,
            self.open_order_id,
            ctx.trigger_ts,
            from_key_preview
        );
        if self.open_order_id == 0 {
            info!(
                "MarketMakerOpenStrategy: strategy_id={} skip MMCancel because open_order_id=0 trigger_ts={} from_key='{}'",
                self.strategy_id,
                ctx.trigger_ts,
                from_key_preview
            );
            return;
        }

        let order = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(self.open_order_id);
        if let Some(order) = order {
            if order.status.is_terminal() {
                info!(
                    "MarketMakerOpenStrategy: strategy_id={} open order already terminal {:?}, skip cancel order_id={} trigger_ts={} from_key='{}'",
                    self.strategy_id, order.status, self.open_order_id, ctx.trigger_ts, from_key_preview
                );
                return;
            }

            match order.get_order_cancel_bytes() {
                Ok(cancel_bytes) => {
                    let exchange = order.venue.trade_engine_exchange();
                    if let Err(e) = TradeEngHub::publish_order_request(exchange, &cancel_bytes) {
                        error!(
                            "MarketMakerOpenStrategy: strategy_id={} exchange={} 发送撤单请求失败 order_id={} trigger_ts={} from_key='{}' err={}",
                            self.strategy_id, exchange, self.open_order_id, ctx.trigger_ts, from_key_preview, e
                        );
                    } else {
                        info!(
                            "MarketMakerOpenStrategy: strategy_id={} 已发送撤单请求 order_id={} exchange={} trigger_ts={} from_key='{}'",
                            self.strategy_id, self.open_order_id, exchange, ctx.trigger_ts, from_key_preview
                        );
                        self.schedule_cancel_query_watchdog(order.client_order_id);
                    }
                }
                Err(e) => {
                    error!(
                        "MarketMakerOpenStrategy: strategy_id={} 获取撤单请求字节失败 order_id={} trigger_ts={} from_key='{}' err={}",
                        self.strategy_id, self.open_order_id, ctx.trigger_ts, from_key_preview, e
                    );
                }
            }
        } else {
            info!(
                "MarketMakerOpenStrategy: strategy_id={} 未找到要撤销的订单 order_id={} trigger_ts={} from_key='{}'",
                self.strategy_id, self.open_order_id, ctx.trigger_ts, from_key_preview
            );
        }
    }

    fn record_mm_hedge_qty(
        &mut self,
        venue: TradingVenue,
        symbol: &str,
        side: Side,
        cumulative_qty: f64,
    ) {
        if self.recorded_to_hedge {
            return;
        }
        self.recorded_to_hedge = true;
        if cumulative_qty <= 0.0 {
            return;
        }
        let base_qty = MonitorChannel::instance().qty_to_base(venue, symbol, cumulative_qty);
        if base_qty <= 0.0 {
            return;
        }
        let (signed_qty, buy_qty, sell_qty) = match side {
            Side::Buy => (base_qty, base_qty, 0.0),
            Side::Sell => (-base_qty, 0.0, base_qty),
        };

        let strategy_mgr = MonitorChannel::instance().strategy_mgr();
        let updated = strategy_mgr.borrow_mut().record_mm_hedge_fill(
            &symbol.to_ascii_uppercase(),
            signed_qty,
            buy_qty,
            sell_qty,
        );
        if !updated {
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} record mm hedge failed symbol={} qty={:.8}",
                self.strategy_id, symbol, base_qty
            );
        }
    }

    fn create_and_send_order(&mut self, client_order_id: i64, symbol: &str) -> Result<(), String> {
        let order = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(client_order_id);
        let Some(order) = order else {
            self.alive_flag = false;
            return Err(format!(
                "order not found: client_order_id={}",
                client_order_id
            ));
        };

        let exchange = order.venue.trade_engine_exchange();
        match order.get_order_request_bytes() {
            Ok(req_bin) => {
                if let Err(e) = TradeEngHub::publish_order_request(exchange, &req_bin) {
                    self.alive_flag = false;
                    return Err(format!(
                        "publish order request failed: symbol={} exchange={} err={}",
                        symbol, exchange, e
                    ));
                }
                self.schedule_order_query_watchdog(client_order_id);
                Ok(())
            }
            Err(e) => {
                self.alive_flag = false;
                Err(format!("get order request bytes failed: {}", e))
            }
        }
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

    fn retry_open_order_query_after_cooldown(
        &mut self,
        client_order_id: i64,
        marker: &'static str,
    ) {
        if self.open_order_query_retried {
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} order query {} after retry, close: client_order_id={}",
                self.strategy_id, marker, client_order_id
            );
            self.alive_flag = false;
            return;
        }

        self.open_order_query_retried = true;
        warn!(
            "MarketMakerOpenStrategy: strategy_id={} order query {} on first attempt, retry after {}ms: client_order_id={}",
            self.strategy_id,
            marker,
            ORDER_QUERY_RETRY_DELAY_US / 1_000,
            client_order_id
        );
        self.schedule_order_query_watchdog_with_delay(client_order_id, ORDER_QUERY_RETRY_DELAY_US);
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

    fn send_order_query(&mut self, client_order_id: i64, reason: PendingOrderQueryReason) {
        if let Some(existing) = self.pending_order_query {
            if reason == PendingOrderQueryReason::CancelRejected
                && existing != PendingOrderQueryReason::CancelRejected
            {
                self.pending_order_query = Some(PendingOrderQueryReason::CancelRejected);
            }
            return;
        }

        let order_mgr = MonitorChannel::instance().order_manager();
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} send_order_query but local order missing: client_order_id={} reason={:?}",
                self.strategy_id, client_order_id, reason
            );
            return;
        };

        match self.build_order_query_request(&order, client_order_id) {
            Ok((exchange, req_bytes)) => {
                if let Err(err) = QueryEngHub::publish_query_request(exchange.as_str(), &req_bytes)
                {
                    warn!(
                        "MarketMakerOpenStrategy: strategy_id={} publish order query failed: exchange={} client_order_id={} reason={:?} err={:#}",
                        self.strategy_id, exchange, client_order_id, reason, err
                    );
                    return;
                }
                self.pending_order_query = Some(reason);
                debug!(
                    "MarketMakerOpenStrategy: strategy_id={} order query sent: exchange={} client_order_id={} reason={:?}",
                    self.strategy_id, exchange, client_order_id, reason
                );
            }
            Err(err) => {
                warn!(
                    "MarketMakerOpenStrategy: strategy_id={} build order query failed: client_order_id={} reason={:?} err={}",
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
                    let scheduled_at = w.due_ts_us.saturating_sub(CANCEL_QUERY_WATCHDOG_DELAY_US);
                    let waited_ms = now.saturating_sub(scheduled_at).saturating_div(1_000);
                    let hint = if w.reason == PendingOrderQueryReason::CancelRejected {
                        "CancelRejectedWatchdog触发"
                    } else {
                        "CancelWatchdog触发"
                    };
                    info!(
                        "{}: strategy_id={} client_order_id={} symbol={} status={:?} exch_ord_id={:?} 等待{}ms仍未收到撤单/终态回报，发送order query回补 reason={:?}",
                        hint,
                        self.strategy_id,
                        w.client_order_id,
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
                        "OrderWatchdog触发{}: strategy_id={} client_order_id={} symbol={} status={:?} exch_ord_id={:?} 等待{}ms仍未收到回报，发送order query回补 (since_submit={}ms)",
                        hint,
                        self.strategy_id,
                        w.client_order_id,
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
            error!(
                "MarketMakerOpenStrategy: strategy_id={} query_resp rejected: client_order_id={} order_id={} exec_qty={:.8} reason={:?}",
                self.strategy_id,
                order.client_order_id,
                order_id,
                parsed.executed_qty,
                reason
            );
        }

        info!(
            "MarketMakerOpenStrategy: strategy_id={} query回补: client_order_id={} order_id={} exec_qty={:.8} status_u8={} reason={:?}",
            self.strategy_id,
            order.client_order_id,
            order_id,
            parsed.executed_qty,
            parsed.status_u8,
            reason
        );
    }

    fn handle_open_query_result(
        &mut self,
        client_order_id: i64,
        reason: PendingOrderQueryReason,
        parsed: Option<CompactOrderQueryResp>,
    ) {
        let order_mgr = MonitorChannel::instance().order_manager();
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} query_resp but order missing: client_order_id={} reason={:?}",
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
                PendingOrderQueryReason::CancelWatchdog => {
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
                "MarketMakerOpenStrategy: strategy_id={} query invalid status_u8={} (close): client_order_id={} reason={:?}",
                self.strategy_id, parsed.status_u8, client_order_id, reason
            );
            self.alive_flag = false;
            return;
        };

        match st {
            OrderExecutionStatus::Filled | OrderExecutionStatus::Cancelled => {
                self.apply_parsed_order_query_updates(&order, parsed, reason);
            }
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
                PendingOrderQueryReason::CancelWatchdog => {
                    self.apply_parsed_order_query_updates(&order, parsed, reason);
                    let exchange = order.venue.trade_engine_exchange();
                    match order.get_order_cancel_bytes() {
                        Ok(cancel_bytes) => {
                            if let Err(e) =
                                TradeEngHub::publish_order_request(exchange, &cancel_bytes)
                            {
                                warn!(
                                    "MarketMakerOpenStrategy: strategy_id={} re-cancel publish failed: exchange={} client_order_id={} err={}",
                                    self.strategy_id, exchange, client_order_id, e
                                );
                            } else {
                                info!(
                                    "MarketMakerOpenStrategy: strategy_id={} re-cancel sent: exchange={} client_order_id={} reason={:?}",
                                    self.strategy_id, exchange, client_order_id, reason
                                );
                                self.schedule_cancel_query_watchdog(client_order_id);
                            }
                        }
                        Err(e) => {
                            warn!(
                                "MarketMakerOpenStrategy: strategy_id={} get cancel bytes failed: client_order_id={} err={}",
                                self.strategy_id, client_order_id, e
                            );
                            self.schedule_cancel_query_watchdog(client_order_id);
                        }
                    }
                }
            },
            OrderExecutionStatus::Rejected => {
                error!(
                    "MarketMakerOpenStrategy: strategy_id={} query shows rejected (close): client_order_id={} reason={:?}",
                    self.strategy_id, client_order_id, reason
                );
                self.alive_flag = false;
            }
            OrderExecutionStatus::Commit => {
                error!(
                    "MarketMakerOpenStrategy: strategy_id={} query shows commit(unexpected, close): client_order_id={} reason={:?}",
                    self.strategy_id, client_order_id, reason
                );
                self.alive_flag = false;
            }
        }
    }

    fn apply_order_update(&mut self, order_update: &dyn OrderUpdate) -> bool {
        let client_order_id = order_update.client_order_id();
        self.clear_query_watchdogs(client_order_id);
        if client_order_id != self.open_order_id {
            debug!(
                "MarketMakerOpenStrategy: strategy_id={} ignore order_update client_order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        }

        let order_mgr = MonitorChannel::instance().order_manager();
        let mut order_manager = order_mgr.borrow_mut();
        let Some(current_order) = order_manager.get(client_order_id) else {
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} order update not found client_order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        };

        if OrderManager::should_skip_idempotent_order_update(
            &current_order,
            order_update.status(),
            order_update.order_id(),
            order_update.cumulative_filled_quantity(),
            "MarketMakerOpenStrategy",
            self.strategy_id,
        )
        .is_some()
        {
            return false;
        }

        let prev_cumulative_filled_qty = current_order.cumulative_filled_quantity;

        let mut record_fill: Option<(TradingVenue, String, Side, f64)> = None;
        let updated = order_manager.update(client_order_id, |order| match order_update.status() {
            OrderStatus::New => {
                if !self.alive_flag {
                    warn!(
                        "MarketMakerOpenStrategy: strategy_id={} revive on delayed open NEW: client_order_id={} exchange_order_id={} symbol={}",
                        self.strategy_id,
                        client_order_id,
                        order_update.order_id(),
                        order.symbol
                    );
                    self.alive_flag = true;
                }
                order.status = OrderExecutionStatus::Create;
                order.set_exchange_order_id(order_update.order_id());
                order.set_create_time(order_update.event_time());
                info!(
                    "✅ MM订单已挂单: strategy_id={} client_order_id={} exchange_order_id={} symbol={} side={:?} price={:.6} qty={:.4}",
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
                record_fill = Some((
                    order.venue,
                    order.symbol.clone(),
                    order.side,
                    order.cumulative_filled_quantity,
                ));
                info!(
                    "🚫 MM订单已撤销: strategy_id={} client_order_id={} exchange_order_id={} symbol={} filled={:.4}/{:.4}",
                    self.strategy_id,
                    client_order_id,
                    order_update.order_id(),
                    order.symbol,
                    order.cumulative_filled_quantity,
                    order.quantity
                );
                self.alive_flag = false;
            }
            OrderStatus::Filled => {
                order.status = OrderExecutionStatus::Filled;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = order_update.cumulative_filled_quantity();
                order.set_filled_time(order_update.event_time());
                order.set_end_time(order_update.event_time());
                record_fill = Some((
                    order.venue,
                    order.symbol.clone(),
                    order.side,
                    order.cumulative_filled_quantity,
                ));
                info!(
                    "✅ MM订单已完全成交: strategy_id={} client_order_id={} exchange_order_id={} symbol={}",
                    self.strategy_id,
                    client_order_id,
                    order_update.order_id(),
                    order.symbol
                );
                self.alive_flag = false;
            }
            OrderStatus::Expired | OrderStatus::ExpiredInMatch => {
                order.status = OrderExecutionStatus::Rejected;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = order_update.cumulative_filled_quantity();
                order.set_end_time(order_update.event_time());
                warn!(
                    "⏰ MM订单已过期: strategy_id={} client_order_id={} exchange_order_id={} symbol={}",
                    self.strategy_id,
                    client_order_id,
                    order_update.order_id(),
                    order.symbol
                );
                self.alive_flag = false;
            }
            OrderStatus::PartiallyFilled => {
                order.status = OrderExecutionStatus::Create;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = order_update.cumulative_filled_quantity();
                order.set_filled_time(order_update.event_time());
                debug!(
                    "MM订单部分成交: strategy_id={} client_order_id={} exchange_order_id={} symbol={}",
                    self.strategy_id,
                    client_order_id,
                    order_update.order_id(),
                    order.symbol
                );
            }
        });
        drop(order_manager);

        if !updated {
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} order update not found client_order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        }

        if let Some((venue, symbol, side, qty)) = record_fill {
            self.record_mm_hedge_qty(venue, &symbol, side, qty);
        }

        if order_update.status() == OrderStatus::New {
            if let Some(order) = MonitorChannel::instance()
                .order_manager()
                .borrow()
                .get(client_order_id)
            {
                self.publish_uniform_new_order(order_update, &order, prev_cumulative_filled_qty);
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
                self.publish_uniform_terminal_order(
                    order_update,
                    &order,
                    prev_cumulative_filled_qty,
                );
            }
        }

        if matches!(
            order_update.status(),
            OrderStatus::PartiallyFilled | OrderStatus::Filled
        ) {
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

        true
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
                    "MarketMakerOpenStrategy: strategy_id={} uniform {:?} amount_update rollback detected: client_order_id={} prev={:.8} incoming={:.8}",
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

    fn uniform_open_fields(&self) -> (i64, Vec<u8>, f64) {
        (
            self.signal_ts,
            format!("open|{}", self.open_from_key).into_bytes(),
            self.open_price_offset,
        )
    }

    fn publish_uniform_new_order(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
    ) {
        let (signal_ts, from_key, price_offset) = self.uniform_open_fields();

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
        let (signal_ts, from_key, price_offset) = self.uniform_open_fields();
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
        let (signal_ts, from_key, price_offset) = self.uniform_open_fields();
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
        let (signal_ts, from_key, price_offset) = self.uniform_open_fields();
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

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) -> bool {
        let client_order_id = trade.client_order_id();
        self.clear_query_watchdogs(client_order_id);
        if client_order_id != self.open_order_id {
            debug!(
                "MarketMakerOpenStrategy: strategy_id={} ignore trade_update client_order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        }

        let Some(status @ (OrderStatus::PartiallyFilled | OrderStatus::Filled)) =
            trade.order_status()
        else {
            return false;
        };

        let order_mgr = MonitorChannel::instance().order_manager();
        let mut order_manager = order_mgr.borrow_mut();
        let Some(current_order) = order_manager.get(client_order_id) else {
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} trade update order missing client_order_id={}",
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
            "MarketMakerOpenStrategy",
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
            if status == OrderStatus::Filled {
                order.status = OrderExecutionStatus::Filled;
                order.set_end_time(event_time);
            } else if !order.status.is_terminal() {
                order.status = OrderExecutionStatus::Create;
            }
        });
        if !updated {
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} trade update order missing client_order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        }

        if let Some(order) = order_manager.get(client_order_id) {
            self.publish_uniform_trade_order(trade, &order, prev_cumulative_filled_qty, status);
        }
        drop(order_manager);

        if status == OrderStatus::Filled {
            info!(
                "✅ MM订单成交完成: strategy_id={} client_order_id={} symbol={} price={:.6} cumulative={:.4}",
                self.strategy_id,
                client_order_id,
                trade.symbol(),
                trade.price(),
                cumulative_qty
            );
            self.record_mm_hedge_qty(
                trade.trading_venue(),
                trade.symbol(),
                trade.side(),
                cumulative_qty,
            );
            self.alive_flag = false;
        }

        true
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        match &signal.signal_type {
            SignalType::MMOpen => match MmOpenCtx::from_bytes(signal.context.clone()) {
                Ok(ctx) => self.handle_mm_open_signal(ctx),
                Err(err) => {
                    warn!(
                        "MarketMakerOpenStrategy: strategy_id={} decode MMOpen failed: {}",
                        self.strategy_id, err
                    );
                    self.alive_flag = false;
                }
            },
            SignalType::MMCancel => match MmCancelCtx::from_bytes(signal.context.clone()) {
                Ok(ctx) => self.handle_mm_cancel_signal(ctx),
                Err(err) => {
                    warn!(
                        "MarketMakerOpenStrategy: strategy_id={} decode MMCancel failed: {}",
                        self.strategy_id, err
                    );
                }
            },
            _ => {
                debug!(
                    "MarketMakerOpenStrategy: strategy_id={} ignore signal {:?}",
                    self.strategy_id, signal.signal_type
                );
            }
        }
    }
}

impl ForceCloseControl for MarketMakerOpenStrategy {
    fn set_force_close_mode(&mut self, _enabled: bool) {}

    fn is_force_close_mode(&self) -> bool {
        false
    }
}

impl Strategy for MarketMakerOpenStrategy {
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
        if self.open_symbol.is_empty() {
            None
        } else {
            Some(&self.open_symbol)
        }
    }

    fn mm_open_price_map_entry(&self) -> Option<MmOpenPriceMapEntry> {
        Some(MmOpenPriceMapEntry {
            symbol: self.open_symbol.clone(),
            side: self.open_side?,
            client_order_id: self.open_order_id,
            price_qv: self.open_price_qv.into(),
        })
        .filter(|entry| !entry.symbol.is_empty() && entry.client_order_id != 0)
    }

    fn is_strategy_order(&self, order_id: i64) -> bool {
        Self::extract_strategy_id(order_id) == self.strategy_id
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        MarketMakerOpenStrategy::handle_signal(self, signal);
    }

    fn apply_order_update(&mut self, update: &dyn OrderUpdate) {
        let should_persist = MarketMakerOpenStrategy::apply_order_update(self, update);
        if should_persist {
            PersistChannel::with(|ch| ch.publish_order_update(update));
        }
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) {
        let should_persist = MarketMakerOpenStrategy::apply_trade_update(self, trade);
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

        let client_order_id = response.client_order_id();
        if client_order_id != self.open_order_id {
            return;
        }

        let exchange = response.exchange_enum();
        let code_desc = exchange
            .and_then(|ex| describe_trade_error_code(ex, response.error_code()))
            .unwrap_or("unknown");

        match response.request_kind() {
            TradeRequestKind::Open => {
                warn!(
                    "MarketMakerOpenStrategy: strategy_id={} open_failed: req_type={} status={} code={}({}) client_order_id={}",
                    self.strategy_id,
                    response.req_type(),
                    response.status(),
                    response.error_code(),
                    code_desc,
                    client_order_id
                );
                self.alive_flag = false;
            }
            TradeRequestKind::Cancel => {
                warn!(
                    "MarketMakerOpenStrategy: strategy_id={} cancel_failed: req_type={} status={} code={}({}) client_order_id={}",
                    self.strategy_id,
                    response.req_type(),
                    response.status(),
                    response.error_code(),
                    code_desc,
                    client_order_id
                );
                if response.is_cancel_not_cancellable() {
                    self.clear_query_watchdogs(client_order_id);
                    self.send_order_query(client_order_id, PendingOrderQueryReason::CancelRejected);
                    self.schedule_cancel_query_watchdog_with_reason(
                        client_order_id,
                        PendingOrderQueryReason::CancelRejected,
                    );
                } else {
                    self.send_order_query(client_order_id, PendingOrderQueryReason::CancelWatchdog);
                }
            }
            TradeRequestKind::Other => {
                warn!(
                    "MarketMakerOpenStrategy: strategy_id={} other_failed(TODO): req_type={} status={} code={}({}) client_order_id={}",
                    self.strategy_id,
                    response.req_type(),
                    response.status(),
                    response.error_code(),
                    code_desc,
                    client_order_id
                );
            }
        }
    }

    fn apply_query_engine_response(&mut self, response: &dyn QueryEngineResponse) {
        let client_order_id = response.client_query_id();
        let Some(reason) = self.pending_order_query.take() else {
            return;
        };

        if client_order_id != self.open_order_id {
            return;
        }

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
        if is_order_query_not_found_marker(&body[..actual_len]) {
            match reason {
                PendingOrderQueryReason::OrderWatchdog => {
                    self.retry_open_order_query_after_cooldown(
                        client_order_id,
                        "not found (-2013)",
                    );
                }
                PendingOrderQueryReason::CancelWatchdog => {
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
        }

        if actual_len == 1 && body[0] == b'E' {
            match reason {
                PendingOrderQueryReason::OrderWatchdog => {
                    self.retry_open_order_query_after_cooldown(client_order_id, "failed (E)");
                }
                PendingOrderQueryReason::CancelWatchdog => {
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
                "MarketMakerOpenStrategy: strategy_id={} query_resp decode failed: client_order_id={} req_type={} reason={:?} body='{}'",
                self.strategy_id,
                client_order_id,
                response.req_type(),
                reason,
                text
            );
        }

        self.handle_open_query_result(client_order_id, reason, parsed);
    }

    fn handle_period_clock(&mut self, _current_tp: i64) {
        self.handle_open_leg_timeout();
        self.handle_query_watchdogs();
    }

    fn is_active(&self) -> bool {
        self.alive_flag
    }
}

impl Drop for MarketMakerOpenStrategy {
    fn drop(&mut self) {
        self.cleanup_strategy_orders();
    }
}
