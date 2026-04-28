use crate::common::symbol_util::{extract_assets_from_symbol, normalize_symbol_for_internal};
use crate::common::tick_math::QuantizedValue;
use crate::common::time_util::get_timestamp_us;
use crate::common::trade_error_code::describe_trade_error_code;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::open_order_rate_limiter::{OrderRateBucket, OrderRateLimiter};
use crate::pre_trade::order_manager::{OrderExecutionStatus, OrderManager, OrderType, Side};
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::pre_trade::{QueryEngHub, TradeEngHub};
use crate::signal::common::{OrderStatus, TradingVenue};
use crate::strategy::manager::{OpenPriceMapEntry, OrphanHandoff, OrphanStrategyRole, Strategy};
use crate::strategy::order_query_builder::build_order_query_request;
pub use crate::strategy::order_reconcile::PendingOrderQueryReason;
use crate::strategy::order_reconcile::{qv_decimal_or_fallback, ORDER_QUERY_WATCHDOG_DELAY_US};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_engine_response::{TradeEngineResponse, TradeRequestKind};
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::uniform_order_helper::{
    publish_uniform_new_order, publish_uniform_terminal_order, publish_uniform_trade_order,
    publish_uniform_trade_order_from_order_update, UniformPublishCtx,
};
use crate::strategy::ws_order_update::prepare_failed_trade_engine_response_for_strategy;
use log::{debug, error, info, warn};

const OPEN_BALANCE_EPS: f64 = 1e-12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryWatchdog {
    pub client_order_id: i64,
    pub due_ts_us: i64,
    pub reason: PendingOrderQueryReason,
}

#[derive(Debug, Clone, Default)]
pub struct OpenOrderState {
    pub open_order_id: i64,
    pub open_expire_ts: Option<i64>,
    pub open_side: Option<Side>,
    pub pending_order_query: Option<PendingOrderQueryReason>,
    pub order_query_watchdog: Option<QueryWatchdog>,
    pub cancel_query_watchdog: Option<QueryWatchdog>,
    pub last_cancel_trigger_ts: Option<i64>,
    pub last_open_cancel_reason: Option<&'static str>,
}

#[derive(Debug, Clone)]
pub struct OpenStrategyState {
    pub strategy_id: i32,
    pub open_symbol: String,
    pub open_venue: Option<TradingVenue>,
    pub order: OpenOrderState,
    pub signal_ts: i64,
    pub from_key: String,
    pub price_qv: QuantizedValue,
    pub price_offset: f64,
    pub alive: bool,
}

pub struct OpenSignalInput {
    pub signal_kind: &'static str,
    pub order_log_name: &'static str,
    pub order_rate_bucket: OrderRateBucket,
    pub opening_symbol: String,
    pub venue_u8: u8,
    pub side_u8: u8,
    pub order_type_u8: u8,
    pub qty: f64,
    pub price: f64,
    pub price_count: i64,
    pub amount_count: i64,
    pub exp_time: i64,
    pub create_ts: i64,
    pub from_key_len: u32,
    pub from_key: Vec<u8>,
    pub price_qv: QuantizedValue,
    pub price_offset: f64,
    // 绝对 close_ts。0 表示不设置藏仓窗口；>0 表示这笔 ArbOpen 不追求立刻对冲，
    // 而是先藏一段时间，到 close_ts 后才进入可对冲/可关闭的 due 数量。
    pub close_ts: i64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpenSignalInitResult {
    pub qty_multiplier: f64,
    pub close_ts: i64,
}

pub struct OpenCancelInput {
    pub signal_name: &'static str,
    pub target_strategy_id: i32,
    pub target_client_order_id: i64,
    pub cancel_side: Side,
    pub cancel_reason: &'static str,
    pub trigger_ts: i64,
    pub from_key: Vec<u8>,
}

impl OpenStrategyState {
    pub fn new(strategy_id: i32) -> Self {
        Self {
            strategy_id,
            open_symbol: String::new(),
            open_venue: None,
            order: OpenOrderState::default(),
            signal_ts: 0,
            from_key: String::new(),
            price_qv: QuantizedValue::zero(),
            price_offset: 0.0,
            alive: true,
        }
    }
}

pub trait OpenStrategyCommon {
    fn strategy_name(&self) -> &'static str;
    fn open_state(&self) -> &OpenStrategyState;
    fn open_state_mut(&mut self) -> &mut OpenStrategyState;

    fn handoff_open_order_after_query_failure(
        &mut self,
        client_order_id: i64,
        marker: &'static str,
    );

    fn orphan_strategy_role(&self) -> OrphanStrategyRole;

    fn open_order_rate_bucket(&self) -> OrderRateBucket;

    fn open_order_action_log_name(&self) -> &'static str;

    fn resolve_open_qty_multiplier(&self, venue: TradingVenue, symbol: &str)
        -> Result<f64, String>;

    fn open_order_qty_to_base(&self, signed_qty: f64, qty_multiplier: f64) -> Result<f64, String> {
        Ok(signed_qty * qty_multiplier)
    }

    fn open_terminal_close_ts(&self) -> i64 {
        0
    }

    fn record_open_order_terminal_base_qty(
        &mut self,
        symbol: &str,
        side: Side,
        base_qty: f64,
        fill_ts: i64,
        price: f64,
        update_detail: &str,
    ) -> bool {
        if base_qty <= 1e-12 {
            return false;
        }
        let signed_base_qty = match side {
            Side::Buy => base_qty.abs(),
            Side::Sell => -base_qty.abs(),
        };
        let close_ts = self.open_terminal_close_ts();
        let updated = MonitorChannel::instance()
            .strategy_mgr()
            .borrow_mut()
            .record_open_order_terminal(symbol, signed_base_qty, fill_ts, price, close_ts);
        if !updated {
            warn!(
                "{}: strategy_id={} record open order terminal failed symbol={} side={:?} base_qty={:.8} fill_ts={} price={:.8} close_ts={} detail={}",
                self.strategy_name(),
                self.strategy_id(),
                symbol,
                side,
                base_qty,
                fill_ts,
                price,
                close_ts,
                update_detail
            );
        }
        updated
    }

    fn send_open_order_common(&mut self, client_order_id: i64, symbol: &str) -> Result<(), String> {
        let order = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(client_order_id);
        let Some(order) = order else {
            self.open_state_mut().alive = false;
            return Err(format!(
                "order not found: client_order_id={}",
                client_order_id
            ));
        };

        let exchange = order.venue.trade_engine_exchange();
        match order.get_order_request_bytes() {
            Ok(req_bin) => {
                let stats = OrderRateLimiter::record(
                    self.open_order_rate_bucket(),
                    client_order_id,
                    get_timestamp_us(),
                );
                info!(
                    "{}: strategy_id={} {} order action recorded client_order_id={} count_10s={} count_1m={}",
                    self.strategy_name(),
                    self.strategy_id(),
                    self.open_order_action_log_name(),
                    client_order_id,
                    stats.count_10s,
                    stats.count_1m
                );
                if let Err(e) = TradeEngHub::publish_order_request(exchange, &req_bin) {
                    self.open_state_mut().alive = false;
                    return Err(format!(
                        "publish order request failed: symbol={} exchange={} err={}",
                        symbol, exchange, e
                    ));
                }
                self.schedule_order_query_watchdog(client_order_id);
                Ok(())
            }
            Err(e) => {
                self.open_state_mut().alive = false;
                Err(format!("get order request bytes failed: {}", e))
            }
        }
    }

    fn strategy_id(&self) -> i32 {
        self.open_state().strategy_id
    }

    fn open_strategy_symbol(&self) -> Option<&str> {
        let symbol = self.open_state().open_symbol.as_str();
        if symbol.is_empty() {
            None
        } else {
            Some(symbol)
        }
    }

    fn open_strategy_is_active(&self) -> bool {
        self.open_state().alive
    }

    fn open_price_map_entry(&self) -> Option<OpenPriceMapEntry> {
        let open_state = self.open_state();
        Some(OpenPriceMapEntry {
            symbol: open_state.open_symbol.clone(),
            side: open_state.order.open_side?,
            client_order_id: open_state.order.open_order_id,
            price_qv: open_state.price_qv.into(),
        })
        .filter(|entry| !entry.symbol.is_empty() && entry.client_order_id != 0)
    }

    fn open_order_non_terminal_cleanup_reason(&self) -> &'static str {
        "开仓订单未达到终结状态被清理"
    }

    fn uniform_open_publish_ctx(&self) -> UniformPublishCtx {
        let open_state = self.open_state();
        UniformPublishCtx {
            signal_ts: open_state.signal_ts,
            from_key: format!("open|{}", open_state.from_key).into_bytes(),
            price_offset: open_state.price_offset,
        }
    }

    fn handle_open_signal_common(
        &mut self,
        input: OpenSignalInput,
    ) -> Option<OpenSignalInitResult> {
        let symbol = normalize_symbol_for_internal(&input.opening_symbol);
        if symbol.is_empty() {
            warn!(
                "{}: strategy_id={} empty symbol",
                self.strategy_name(),
                self.strategy_id()
            );
            self.open_state_mut().alive = false;
            return None;
        }

        let Some(venue) = TradingVenue::from_u8(input.venue_u8) else {
            warn!(
                "{}: strategy_id={} invalid venue={}",
                self.strategy_name(),
                self.strategy_id(),
                input.venue_u8
            );
            self.open_state_mut().alive = false;
            return None;
        };
        let Some(side) = Side::from_u8(input.side_u8) else {
            warn!(
                "{}: strategy_id={} invalid side={}",
                self.strategy_name(),
                self.strategy_id(),
                input.side_u8
            );
            self.open_state_mut().alive = false;
            return None;
        };
        let Some(order_type) = OrderType::from_u8(input.order_type_u8) else {
            warn!(
                "{}: strategy_id={} invalid order_type={}",
                self.strategy_name(),
                self.strategy_id(),
                input.order_type_u8
            );
            self.open_state_mut().alive = false;
            return None;
        };

        if input.qty <= 0.0 {
            warn!(
                "{}: strategy_id={} invalid qty={}",
                self.strategy_name(),
                self.strategy_id(),
                input.qty
            );
            self.open_state_mut().alive = false;
            return None;
        }
        if input.price <= 0.0 {
            warn!(
                "{}: strategy_id={} invalid price={} order_type={:?}",
                self.strategy_name(),
                self.strategy_id(),
                input.price,
                order_type
            );
            self.open_state_mut().alive = false;
            return None;
        }
        if input.price_count <= 0 || input.amount_count <= 0 {
            warn!(
                "{}: strategy_id={} invalid {} qv count price_count={} amount_count={}",
                self.strategy_name(),
                self.strategy_id(),
                input.signal_kind,
                input.price_count,
                input.amount_count
            );
            self.open_state_mut().alive = false;
            return None;
        }
        if !venue.supports_pre_trade_stack() {
            panic!(
                "{}: strategy_id={} 不支持的交易场所 {:?}，仅支持 Binance/OKX/Bybit/Bitget/Gate 的 futures 或 margin",
                self.strategy_name(),
                self.strategy_id(),
                venue
            );
        }

        if let Err(e) = MonitorChannel::instance().check_symbol_exposure(&symbol) {
            error!(
                "{}: strategy_id={} symbol={} 单品种敞口风控检查失败: {}，标记策略为不活跃",
                self.strategy_name(),
                self.strategy_id(),
                symbol,
                e
            );
            self.open_state_mut().alive = false;
            return None;
        }
        if let Err(e) = MonitorChannel::instance().check_total_exposure() {
            error!(
                "{}: strategy_id={} 总敞口风控检查失败: {}，标记策略为不活跃",
                self.strategy_name(),
                self.strategy_id(),
                e
            );
            self.open_state_mut().alive = false;
            return None;
        }
        if order_type == OrderType::Limit {
            if let Err(e) = MonitorChannel::instance().check_pending_limit_order(&symbol, side) {
                error!(
                    "{}: strategy_id={} symbol={} 限价挂单数量风控检查失败: {}，标记策略为不活跃",
                    self.strategy_name(),
                    self.strategy_id(),
                    symbol,
                    e
                );
                self.open_state_mut().alive = false;
                return None;
            }
        }

        let rate_params = PreTradeParamsLoader::instance();
        if let Err(e) = OrderRateLimiter::check_limit(
            input.order_rate_bucket,
            rate_params.open_order_rate_limit_per_min(),
            rate_params.open_order_rate_limit_10s(),
            get_timestamp_us(),
        ) {
            info!(
                "{}: strategy_id={} symbol={} 开仓下单频率风控触发: {}，标记策略为不活跃",
                self.strategy_name(),
                self.strategy_id(),
                symbol,
                e
            );
            self.open_state_mut().alive = false;
            return None;
        }

        let qty_multiplier = match self.resolve_open_qty_multiplier(venue, &symbol) {
            Ok(multiplier) => multiplier,
            Err(err) => {
                error!(
                    "{}: strategy_id={} 初始化开仓数量乘数失败 symbol={} venue={:?}: {}",
                    self.strategy_name(),
                    self.strategy_id(),
                    symbol,
                    venue,
                    err
                );
                self.open_state_mut().alive = false;
                return None;
            }
        };

        let order_qty = input.qty;
        let order_price = input.price;
        let signed_qty = match side {
            Side::Buy => order_qty.abs(),
            Side::Sell => -order_qty.abs(),
        };

        let binance_standard = venue == TradingVenue::BinanceMargin
            && MonitorChannel::instance()
                .order_manager()
                .borrow()
                .binance_is_standard();
        if binance_standard {
            // Binance margin 非统一账户不能在下单时自动借币；资产或仓位不足时，
            // 反向开仓会被交易所拒绝，所以这里提前拦截。
            let (base_asset, quote_asset) = extract_assets_from_symbol(&symbol);
            let (check_asset, required_amount) = match side {
                Side::Buy => (quote_asset, order_qty * order_price),
                Side::Sell => (base_asset, order_qty),
            };
            let available_balance =
                MonitorChannel::instance().balance_position_for_venue(venue, &check_asset);
            if available_balance + OPEN_BALANCE_EPS < required_amount {
                error!(
                    "{}: strategy_id={} BinanceMargin STANDARD 余额不足，拒绝开仓并标记策略不活跃 symbol={} side={:?} asset={} required={:.8} available={:.8}",
                    self.strategy_name(),
                    self.strategy_id(),
                    symbol,
                    side,
                    check_asset,
                    required_amount,
                    available_balance
                );
                self.open_state_mut().alive = false;
                return None;
            }
        }

        let add_base_qty = match self.open_order_qty_to_base(signed_qty, qty_multiplier) {
            Ok(base_qty) => base_qty,
            Err(err) => {
                error!(
                    "{}: strategy_id={} 开仓数量转换 base qty 失败 symbol={} venue={:?}: {}",
                    self.strategy_name(),
                    self.strategy_id(),
                    symbol,
                    venue,
                    err
                );
                self.open_state_mut().alive = false;
                return None;
            }
        };
        let current_base_qty = MonitorChannel::instance().get_position_qty(&symbol, venue);
        let projected_base_qty = current_base_qty + add_base_qty;
        if projected_base_qty.abs() > current_base_qty.abs() + 1e-12_f64 {
            if let Err(e) = MonitorChannel::instance().check_leverage() {
                error!(
                    "{}: strategy_id={} 杠杆风控检查失败: {}，标记策略为不活跃",
                    self.strategy_name(),
                    self.strategy_id(),
                    e
                );
                self.open_state_mut().alive = false;
                return None;
            }
        }
        if let Err(e) =
            MonitorChannel::instance().ensure_max_pos_u(&symbol, signed_qty, order_price)
        {
            error!(
                "{}: strategy_id={} 仓位限制检查失败: {}，标记策略为不活跃",
                self.strategy_name(),
                self.strategy_id(),
                e
            );
            self.open_state_mut().alive = false;
            return None;
        }

        {
            let state = self.open_state_mut();
            state.open_symbol = symbol.clone();
            state.open_venue = Some(venue);
            state.order.open_expire_ts = (input.exp_time > 0).then_some(input.exp_time);
            state.order.open_side = Some(side);
            state.signal_ts = input.create_ts;
            state.from_key = String::from_utf8_lossy(&input.from_key).to_string();
            state.price_qv = input.price_qv;
            state.price_offset = input.price_offset;
        }
        let client_order_id = Self::compose_order_id(self.strategy_id());
        self.open_order_state_mut().open_order_id = client_order_id;

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
                qty_multiplier,
                submit_ts,
            );

        info!(
            "📤 {}订单已创建: strategy_id={} client_order_id={} symbol={} {:?} side={:?} qty={} price={} qty_multiplier={:.8} from_key_len={}",
            input.order_log_name,
            self.strategy_id(),
            client_order_id,
            symbol,
            venue,
            side,
            qv_decimal_or_fallback(order_qty),
            qv_decimal_or_fallback(order_price),
            qty_multiplier,
            input.from_key_len
        );

        if let Err(err) = self.send_open_order_common(client_order_id, &symbol) {
            error!(
                "{}: strategy_id={} open order send failed: {}",
                self.strategy_name(),
                self.strategy_id(),
                err
            );
        } else {
            info!(
                "✅ {}订单已发送: strategy_id={} client_order_id={}",
                input.order_log_name,
                self.strategy_id(),
                client_order_id
            );
        }
        Some(OpenSignalInitResult {
            qty_multiplier,
            close_ts: input.close_ts,
        })
    }

    fn handle_open_leg_timeout_common(&mut self) {
        let Some(expire_ts) = self.open_order_state().open_expire_ts else {
            return;
        };
        let client_order_id = self.open_order_id();
        let now = get_timestamp_us();
        if now < expire_ts || !self.open_strategy_is_active() || client_order_id == 0 {
            return;
        }

        info!(
            "{}: strategy_id={} 开仓订单超时，直接撤单 order_id={}",
            self.strategy_name(),
            self.strategy_id(),
            client_order_id
        );
        self.open_order_state_mut().last_open_cancel_reason = Some("timeout");

        let order = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(client_order_id);
        if let Some(order) = order {
            match order.get_order_cancel_bytes() {
                Ok(cancel_bytes) => {
                    let exchange = order.venue.trade_engine_exchange();
                    if let Err(e) = TradeEngHub::publish_order_request(exchange, &cancel_bytes) {
                        info!(
                            "{}: strategy_id={} exchange={} 发送开仓撤单请求失败: {}",
                            self.strategy_name(),
                            self.strategy_id(),
                            exchange,
                            e
                        );
                    } else {
                        info!(
                            "{}: strategy_id={} exchange={} reason=timeout 已发送开仓撤单请求 order_id={}",
                            self.strategy_name(),
                            self.strategy_id(),
                            exchange,
                            client_order_id
                        );
                        self.open_order_state_mut().open_expire_ts = None;
                        self.schedule_cancel_query_watchdog(client_order_id);
                    }
                }
                Err(e) => {
                    error!(
                        "{}: strategy_id={} 获取开仓撤单请求字节失败: {}",
                        self.strategy_name(),
                        self.strategy_id(),
                        e
                    );
                }
            }
        }
    }

    fn handle_open_cancel_signal_common(&mut self, input: OpenCancelInput) {
        let precise_target = input.target_strategy_id > 0;
        let from_key_preview = Self::preview_text(&String::from_utf8_lossy(&input.from_key), 160);
        let open_order_id = self.open_order_id();

        if self.open_order_state().last_cancel_trigger_ts == Some(input.trigger_ts) {
            debug!(
                "{}: strategy_id={} skip duplicate {} trigger_ts={} open_order_id={} from_key='{}'",
                self.strategy_name(),
                self.strategy_id(),
                input.signal_name,
                input.trigger_ts,
                open_order_id,
                from_key_preview
            );
            return;
        }

        if precise_target {
            if input.target_strategy_id != self.strategy_id() {
                info!(
                    "{}: strategy_id={} ignore targeted {} target_strategy_id={} trigger_ts={} from_key='{}'",
                    self.strategy_name(),
                    self.strategy_id(),
                    input.signal_name,
                    input.target_strategy_id,
                    input.trigger_ts,
                    from_key_preview
                );
                return;
            }
        } else {
            if input.target_client_order_id > 0 && input.target_client_order_id != open_order_id {
                info!(
                    "{}: strategy_id={} ignore {} due to client_order_id mismatch signal_client_order_id={} open_order_id={} trigger_ts={} from_key='{}'",
                    self.strategy_name(),
                    self.strategy_id(),
                    input.signal_name,
                    input.target_client_order_id,
                    open_order_id,
                    input.trigger_ts,
                    from_key_preview
                );
                return;
            }

            if let Some(open_side) = self.open_side() {
                if open_side != input.cancel_side {
                    info!(
                        "{}: strategy_id={} skip {} due to side mismatch open_side={:?} cancel_side={:?} open_order_id={} trigger_ts={} from_key='{}'",
                        self.strategy_name(),
                        self.strategy_id(),
                        input.signal_name,
                        open_side,
                        input.cancel_side,
                        open_order_id,
                        input.trigger_ts,
                        from_key_preview
                    );
                    return;
                }
            }
        }

        if self.pending_order_query().is_some()
            || self
                .cancel_query_watchdog()
                .is_some_and(|w| w.client_order_id == open_order_id)
        {
            debug!(
                "{}: strategy_id={} skip {} because cancel reconcile already in flight open_order_id={} trigger_ts={} from_key='{}'",
                self.strategy_name(),
                self.strategy_id(),
                input.signal_name,
                open_order_id,
                input.trigger_ts,
                from_key_preview
            );
            self.open_order_state_mut().last_cancel_trigger_ts = Some(input.trigger_ts);
            return;
        }

        if open_order_id == 0 {
            info!(
                "{}: strategy_id={} skip {} because open_order_id=0 trigger_ts={} from_key='{}'",
                self.strategy_name(),
                self.strategy_id(),
                input.signal_name,
                input.trigger_ts,
                from_key_preview
            );
            return;
        }

        let order = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(open_order_id);
        let Some(order) = order else {
            info!(
                "{}: strategy_id={} 未找到要撤销的订单 order_id={} trigger_ts={} from_key='{}'",
                self.strategy_name(),
                self.strategy_id(),
                open_order_id,
                input.trigger_ts,
                from_key_preview
            );
            return;
        };

        if order.status.is_terminal() {
            info!(
                "{}: strategy_id={} open order already terminal {:?}, skip cancel order_id={} trigger_ts={} from_key='{}'",
                self.strategy_name(),
                self.strategy_id(),
                order.status,
                open_order_id,
                input.trigger_ts,
                from_key_preview
            );
            return;
        }

        match order.get_order_cancel_bytes() {
            Ok(cancel_bytes) => {
                let exchange = order.venue.trade_engine_exchange();
                if let Err(e) = TradeEngHub::publish_order_request(exchange, &cancel_bytes) {
                    error!(
                        "{}: strategy_id={} exchange={} 发送撤单请求失败 order_id={} trigger_ts={} from_key='{}' err={}",
                        self.strategy_name(),
                        self.strategy_id(),
                        exchange,
                        open_order_id,
                        input.trigger_ts,
                        from_key_preview,
                        e
                    );
                } else {
                    self.open_order_state_mut().last_open_cancel_reason = Some(input.cancel_reason);
                    self.open_order_state_mut().last_cancel_trigger_ts = Some(input.trigger_ts);
                    self.schedule_cancel_query_watchdog(order.client_order_id);
                    info!(
                        "{}: strategy_id={} exchange={} reason={} 已发送开仓撤单请求 order_id={} trigger_ts={} from_key='{}'",
                        self.strategy_name(),
                        self.strategy_id(),
                        exchange,
                        input.cancel_reason,
                        open_order_id,
                        input.trigger_ts,
                        from_key_preview
                    );
                }
            }
            Err(e) => {
                error!(
                    "{}: strategy_id={} 获取撤单请求字节失败 order_id={} trigger_ts={} from_key='{}' err={}",
                    self.strategy_name(),
                    self.strategy_id(),
                    open_order_id,
                    input.trigger_ts,
                    from_key_preview,
                    e
                );
            }
        }
    }

    fn apply_trade_engine_response_common(&mut self, response: &dyn TradeEngineResponse)
    where
        Self: Strategy + Sized,
    {
        let Some(client_order_id) =
            prepare_failed_trade_engine_response_for_strategy(self, response)
        else {
            return;
        };

        let exchange = response.exchange_enum();
        let code_desc = exchange
            .and_then(|ex| describe_trade_error_code(ex, response.error_code()))
            .unwrap_or("unknown");

        match response.request_kind() {
            TradeRequestKind::Open => {
                warn!(
                    "{}: strategy_id={} open_failed: req_type={} status={} code={}({}) client_order_id={}",
                    self.strategy_name(),
                    self.strategy_id(),
                    response.req_type(),
                    response.status(),
                    response.error_code(),
                    code_desc,
                    client_order_id
                );
                self.handle_open_failed_cleanup(client_order_id);
            }
            TradeRequestKind::Cancel => {
                let reason = if response.is_cancel_not_cancellable() {
                    PendingOrderQueryReason::CancelRejected
                } else {
                    PendingOrderQueryReason::CancelWatchdog
                };
                warn!(
                    "{}: strategy_id={} cancel_failed: req_type={} status={} code={}({}) client_order_id={} query_reason={:?}",
                    self.strategy_name(),
                    self.strategy_id(),
                    response.req_type(),
                    response.status(),
                    response.error_code(),
                    code_desc,
                    client_order_id,
                    reason
                );
                self.clear_query_watchdogs(client_order_id);
                if !self.send_order_query(client_order_id, reason) {
                    let marker = if response.is_cancel_not_cancellable() {
                        "cancel rejected query send failed"
                    } else {
                        "cancel failed query send failed"
                    };
                    self.handoff_open_order_after_query_failure(client_order_id, marker);
                }
            }
            TradeRequestKind::Other => {
                warn!(
                    "{}: strategy_id={} other_failed(TODO): req_type={} status={} code={}({}) client_order_id={}",
                    self.strategy_name(),
                    self.strategy_id(),
                    response.req_type(),
                    response.status(),
                    response.error_code(),
                    code_desc,
                    client_order_id
                );
            }
        }
    }

    fn apply_order_update_common(&mut self, order_update: &dyn OrderUpdate) -> bool {
        let client_order_id = order_update.client_order_id();
        self.clear_query_watchdogs(client_order_id);
        if client_order_id != self.open_order_id() {
            debug!(
                "{}: strategy_id={} ignore order_update client_order_id={}",
                self.strategy_name(),
                self.strategy_id(),
                client_order_id
            );
            return false;
        }

        let order_mgr = MonitorChannel::instance().order_manager();
        let mut order_manager = order_mgr.borrow_mut();
        let Some(current_order) = order_manager.get(client_order_id) else {
            warn!(
                "{}: strategy_id={} order update not found client_order_id={}",
                self.strategy_name(),
                self.strategy_id(),
                client_order_id
            );
            return false;
        };

        if OrderManager::should_skip_idempotent_order_update(
            &current_order,
            order_update.status(),
            order_update.order_id(),
            order_update.cumulative_filled_quantity(),
            self.strategy_name(),
            self.strategy_id(),
        )
        .is_some()
        {
            return false;
        }

        let prev_cumulative_filled_qty = current_order.cumulative_filled_quantity;
        let prev_order_terminal = current_order.status.is_terminal();
        let incoming_cumulative_filled_qty = order_update.cumulative_filled_quantity();
        let protected_cumulative_fill =
            current_order.protected_cumulative_fill(incoming_cumulative_filled_qty);
        if protected_cumulative_fill.rollback_detected {
            warn!(
                "{}: strategy_id={} protected cumulative fill rollback: client_order_id={} symbol={} status={:?} prev={:.8} incoming={:.8} effective={:.8}",
                self.strategy_name(),
                self.strategy_id(),
                current_order.client_order_id,
                current_order.symbol,
                order_update.status(),
                current_order.cumulative_filled_quantity,
                incoming_cumulative_filled_qty,
                protected_cumulative_fill.effective_cum
            );
        }
        let effective_cumulative_filled_qty = protected_cumulative_fill.effective_cum;

        let updated = order_manager.update(client_order_id, |order| match order_update.status() {
            OrderStatus::New => {
                if !self.open_state().alive {
                    warn!(
                        "{}: strategy_id={} revive on delayed open NEW: client_order_id={} exchange_order_id={} symbol={}",
                        self.strategy_name(),
                        self.strategy_id(),
                        client_order_id,
                        order_update.order_id(),
                        order.symbol
                    );
                    self.open_state_mut().alive = true;
                }
                order.status = OrderExecutionStatus::Create;
                order.set_exchange_order_id(order_update.order_id());
                order.set_create_time(order_update.event_time());
                debug!(
                    "{}: strategy_id={} open order NEW client_order_id={} exchange_order_id={} symbol={} side={:?} price={} qty={}",
                    self.strategy_name(),
                    self.strategy_id(),
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
                let cancel_reason = self
                    .open_order_state()
                    .last_open_cancel_reason
                    .unwrap_or("unknown");
                info!(
                    "{}: strategy_id={} open order canceled client_order_id={} exchange_order_id={} exchange={} symbol={} reason={} side={:?} price={} filled={}/{}",
                    self.strategy_name(),
                    self.strategy_id(),
                    client_order_id,
                    order_update.order_id(),
                    order.venue.trade_engine_exchange(),
                    order.symbol,
                    cancel_reason,
                    order.side,
                    qv_decimal_or_fallback(order.price),
                    qv_decimal_or_fallback(order.cumulative_filled_quantity),
                    qv_decimal_or_fallback(order.quantity)
                );
                self.open_order_state_mut().last_open_cancel_reason = None;
                self.open_state_mut().alive = false;
            }
            OrderStatus::Filled => {
                order.status = OrderExecutionStatus::Filled;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = effective_cumulative_filled_qty;
                order.set_filled_time(order_update.event_time());
                order.set_end_time(order_update.event_time());
                debug!(
                    "{}: strategy_id={} open order filled client_order_id={} exchange_order_id={} symbol={}",
                    self.strategy_name(),
                    self.strategy_id(),
                    client_order_id,
                    order_update.order_id(),
                    order.symbol
                );
                self.open_state_mut().alive = false;
            }
            OrderStatus::Expired | OrderStatus::ExpiredInMatch => {
                order.status = OrderExecutionStatus::Rejected;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = effective_cumulative_filled_qty;
                order.set_end_time(order_update.event_time());
                warn!(
                    "{}: strategy_id={} open order expired client_order_id={} exchange_order_id={} symbol={} status={:?}",
                    self.strategy_name(),
                    self.strategy_id(),
                    client_order_id,
                    order_update.order_id(),
                    order.symbol,
                    order_update.status()
                );
                self.open_state_mut().alive = false;
            }
            OrderStatus::PartiallyFilled => {
                order.status = OrderExecutionStatus::Create;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = effective_cumulative_filled_qty;
                order.set_filled_time(order_update.event_time());
                debug!(
                    "{}: strategy_id={} open order partially filled client_order_id={} exchange_order_id={} symbol={}",
                    self.strategy_name(),
                    self.strategy_id(),
                    client_order_id,
                    order_update.order_id(),
                    order.symbol
                );
            }
        });

        if !updated {
            warn!(
                "{}: strategy_id={} order update not found client_order_id={}",
                self.strategy_name(),
                self.strategy_id(),
                client_order_id
            );
            return false;
        }

        let updated_order = order_manager.get(client_order_id);
        drop(order_manager);

        let Some(order) = updated_order else {
            warn!(
                "{}: strategy_id={} order update missing local order after update client_order_id={}",
                self.strategy_name(),
                self.strategy_id(),
                client_order_id
            );
            return false;
        };

        if matches!(
            order_update.status(),
            OrderStatus::Canceled | OrderStatus::Filled
        ) {
            let terminal_venue_qty = if prev_order_terminal {
                effective_cumulative_filled_qty - prev_cumulative_filled_qty
            } else {
                effective_cumulative_filled_qty
            };
            let terminal_base_qty = terminal_venue_qty * order.qty_multiplier;
            let update_detail = format!(
                "{} local_order_symbol={} local_order_qty={:.8} qty_multiplier={:.8} terminal_venue_qty={:.8} terminal_base_qty={:.8} local_order_status={:?}",
                order_update.debug_summary(),
                order.symbol,
                order.quantity,
                order.qty_multiplier,
                terminal_venue_qty,
                terminal_base_qty,
                order.status
            );
            self.record_open_order_terminal_base_qty(
                &order.symbol,
                order.side,
                terminal_base_qty,
                order_update.event_time(),
                order.price,
                &update_detail,
            );
        }

        let ctx = self.uniform_open_publish_ctx();
        if order_update.status() == OrderStatus::New {
            publish_uniform_new_order(
                order_update,
                &order,
                prev_cumulative_filled_qty,
                &ctx,
                self.strategy_name(),
                self.strategy_id(),
            );
        }
        if matches!(
            order_update.status(),
            OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::ExpiredInMatch
        ) {
            publish_uniform_terminal_order(
                order_update,
                &order,
                prev_cumulative_filled_qty,
                &ctx,
                self.strategy_name(),
                self.strategy_id(),
            );
        }
        if matches!(
            order_update.status(),
            OrderStatus::PartiallyFilled | OrderStatus::Filled
        ) {
            publish_uniform_trade_order_from_order_update(
                order_update,
                &order,
                prev_cumulative_filled_qty,
                &ctx,
                self.strategy_name(),
                self.strategy_id(),
            );
        }

        true
    }

    fn apply_trade_update_common(&mut self, trade: &dyn TradeUpdate) -> bool {
        let client_order_id = trade.client_order_id();
        self.clear_query_watchdogs(client_order_id);
        if client_order_id != self.open_order_id() {
            debug!(
                "{}: strategy_id={} ignore trade_update client_order_id={}",
                self.strategy_name(),
                self.strategy_id(),
                client_order_id
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
                "{}: strategy_id={} trade update order missing client_order_id={}",
                self.strategy_name(),
                self.strategy_id(),
                client_order_id
            );
            return false;
        };

        if OrderManager::should_skip_idempotent_trade_update(
            &current_order,
            status,
            trade.cumulative_filled_quantity(),
            trade.event_time(),
            self.strategy_name(),
            self.strategy_id(),
        )
        .is_some()
        {
            return false;
        }

        let prev_cumulative_filled_qty = current_order.cumulative_filled_quantity;
        let prev_order_terminal = current_order.status.is_terminal();
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
                "{}: strategy_id={} trade update order missing client_order_id={}",
                self.strategy_name(),
                self.strategy_id(),
                client_order_id
            );
            return false;
        }

        let updated_order = order_manager.get(client_order_id);
        drop(order_manager);

        let Some(order) = updated_order else {
            warn!(
                "{}: strategy_id={} trade update missing local order snapshot client_order_id={}",
                self.strategy_name(),
                self.strategy_id(),
                client_order_id
            );
            return false;
        };

        let ctx = self.uniform_open_publish_ctx();
        publish_uniform_trade_order(
            trade,
            &order,
            prev_cumulative_filled_qty,
            status,
            &ctx,
            self.strategy_name(),
            self.strategy_id(),
        );

        if status == OrderStatus::Filled {
            let terminal_venue_qty = if prev_order_terminal {
                cumulative_qty - prev_cumulative_filled_qty
            } else {
                cumulative_qty
            };
            let terminal_base_qty = terminal_venue_qty * order.qty_multiplier;
            let update_detail = format!(
                "{} local_order_symbol={} local_order_qty={:.8} qty_multiplier={:.8} terminal_venue_qty={:.8} terminal_base_qty={:.8} local_order_status={:?}",
                trade.debug_summary(),
                order.symbol,
                order.quantity,
                order.qty_multiplier,
                terminal_venue_qty,
                terminal_base_qty,
                order.status
            );
            self.record_open_order_terminal_base_qty(
                &order.symbol,
                order.side,
                terminal_base_qty,
                event_time,
                order.price,
                &update_detail,
            );
            debug!(
                "{}: strategy_id={} open trade filled client_order_id={} symbol={} cumulative={:.8} detail={}",
                self.strategy_name(),
                self.strategy_id(),
                client_order_id,
                order.symbol,
                cumulative_qty,
                trade.debug_summary()
            );
            self.open_state_mut().alive = false;
        }

        true
    }

    fn handoff_open_order_to_orphan(&mut self, client_order_id: i64, reason: &str) -> bool {
        if client_order_id <= 0 {
            self.open_state_mut().alive = false;
            return false;
        }
        let role = self.orphan_strategy_role();
        warn!(
            "{}: strategy_id={} orphan_handoff_start role={} client_order_id={} reason={}",
            self.strategy_name(),
            self.strategy_id(),
            role.as_str(),
            client_order_id,
            reason
        );
        let handoff = OrphanHandoff::from_open(
            client_order_id,
            self.strategy_id(),
            self.uniform_open_publish_ctx(),
            reason,
        );
        let Some(orphan_mgr) = MonitorChannel::try_orphan_strategy_mgr() else {
            warn!(
                "{}: strategy_id={} orphan manager unavailable role={} client_order_id={} reason={}",
                self.strategy_name(),
                self.strategy_id(),
                role.as_str(),
                client_order_id,
                reason
            );
            return false;
        };
        let adopted = orphan_mgr
            .borrow_mut()
            .adopt_orphan_order_id(role, &handoff);
        if !adopted {
            warn!(
                "{}: strategy_id={} orphan handoff rejected role={} client_order_id={} reason={}",
                self.strategy_name(),
                self.strategy_id(),
                role.as_str(),
                client_order_id,
                reason
            );
            return false;
        }
        self.release_open_order_keep_local(client_order_id, reason);
        self.open_state_mut().alive = false;
        true
    }

    fn open_order_state(&self) -> &OpenOrderState {
        &self.open_state().order
    }

    fn open_order_state_mut(&mut self) -> &mut OpenOrderState {
        &mut self.open_state_mut().order
    }

    fn open_order_id(&self) -> i64 {
        self.open_order_state().open_order_id
    }

    fn open_side(&self) -> Option<Side> {
        self.open_order_state().open_side
    }

    fn pending_order_query(&self) -> Option<PendingOrderQueryReason> {
        self.open_order_state().pending_order_query
    }

    fn set_pending_order_query(&mut self, reason: Option<PendingOrderQueryReason>) {
        self.open_order_state_mut().pending_order_query = reason;
    }

    fn order_query_watchdog(&self) -> Option<QueryWatchdog> {
        self.open_order_state().order_query_watchdog
    }

    fn set_order_query_watchdog(&mut self, watchdog: Option<QueryWatchdog>) {
        self.open_order_state_mut().order_query_watchdog = watchdog;
    }

    fn cancel_query_watchdog(&self) -> Option<QueryWatchdog> {
        self.open_order_state().cancel_query_watchdog
    }

    fn set_cancel_query_watchdog(&mut self, watchdog: Option<QueryWatchdog>) {
        self.open_order_state_mut().cancel_query_watchdog = watchdog;
    }

    /// 组合订单ID：高32位为策略ID，低32位为序列号。
    fn compose_order_id(strategy_id: i32) -> i64 {
        ((strategy_id as i64) << 32) | 1
    }

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

    fn schedule_order_query_watchdog(&mut self, client_order_id: i64) {
        self.set_order_query_watchdog(Some(QueryWatchdog {
            client_order_id,
            due_ts_us: get_timestamp_us().saturating_add(ORDER_QUERY_WATCHDOG_DELAY_US),
            reason: PendingOrderQueryReason::OrderWatchdog,
        }));
    }

    fn schedule_cancel_query_watchdog(&mut self, client_order_id: i64) {
        self.set_cancel_query_watchdog(Some(QueryWatchdog {
            client_order_id,
            due_ts_us: get_timestamp_us().saturating_add(ORDER_QUERY_WATCHDOG_DELAY_US),
            reason: PendingOrderQueryReason::CancelWatchdog,
        }));
        if self
            .order_query_watchdog()
            .is_some_and(|w| w.client_order_id == client_order_id)
        {
            self.set_order_query_watchdog(None);
        }
    }

    fn clear_query_watchdogs(&mut self, client_order_id: i64) {
        if client_order_id == self.open_order_id() {
            self.set_pending_order_query(None);
        }
        if self
            .order_query_watchdog()
            .is_some_and(|w| w.client_order_id == client_order_id)
        {
            self.set_order_query_watchdog(None);
        }
        if self
            .cancel_query_watchdog()
            .is_some_and(|w| w.client_order_id == client_order_id)
        {
            self.set_cancel_query_watchdog(None);
        }
    }

    fn release_open_order_keep_local(&mut self, client_order_id: i64, reason: &str) {
        self.set_pending_order_query(None);
        self.clear_query_watchdogs(client_order_id);
        self.open_order_state_mut().open_order_id = 0;
        warn!(
            "{}: strategy_id={} release open order keep local client_order_id={} reason={}",
            self.strategy_name(),
            self.strategy_id(),
            client_order_id,
            reason
        );
    }

    fn cleanup_strategy_orders(&mut self) {
        let open_order_id = self.open_order_id();
        if open_order_id == 0 {
            return;
        }

        let strategy_id = self.strategy_id();
        let non_terminal_reason = self.open_order_non_terminal_cleanup_reason();
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };
        let mut mgr = order_mgr.borrow_mut();
        if let Some(order) = mgr.get(open_order_id) {
            if !order.status.is_terminal() {
                mgr.log_order_details(&order, non_terminal_reason, strategy_id);
            }
        }
        let _ = mgr.remove(open_order_id);
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

    fn handle_open_failed_cleanup(&mut self, client_order_id: i64) {
        // Open strategies are one-shot owners of a single open order. If the exchange rejects
        // creation, there is no live exchange order to reconcile, so the strategy terminalizes the
        // local order, removes it, and exits instead of handing it to an orphan strategy.
        self.set_pending_order_query(None);
        self.clear_query_watchdogs(client_order_id);
        self.terminalize_open_order_before_cleanup(client_order_id);
        self.cleanup_strategy_orders();
        self.open_state_mut().alive = false;
    }

    fn send_order_query(&mut self, client_order_id: i64, reason: PendingOrderQueryReason) -> bool {
        if let Some(existing) = self.pending_order_query() {
            if reason.is_cancel_rejected() && !existing.is_cancel_rejected() {
                self.set_pending_order_query(Some(PendingOrderQueryReason::CancelRejected));
            }
            return true;
        }

        let order_mgr = MonitorChannel::instance().order_manager();
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "{}: strategy_id={} send_order_query but local order missing: client_order_id={} reason={:?}",
                self.strategy_name(),
                self.strategy_id(),
                client_order_id,
                reason
            );
            return false;
        };

        match build_order_query_request(&order, client_order_id, client_order_id) {
            Ok((exchange, req_bytes)) => {
                if let Err(err) = QueryEngHub::publish_query_request(exchange.as_str(), &req_bytes)
                {
                    warn!(
                        "{}: strategy_id={} publish order query failed: exchange={} client_order_id={} reason={:?} err={:#}",
                        self.strategy_name(),
                        self.strategy_id(),
                        exchange,
                        client_order_id,
                        reason,
                        err
                    );
                    return false;
                }
                self.set_pending_order_query(Some(reason));
                debug!(
                    "{}: strategy_id={} order query sent: exchange={} client_order_id={} reason={:?}",
                    self.strategy_name(),
                    self.strategy_id(),
                    exchange,
                    client_order_id,
                    reason
                );
                true
            }
            Err(err) => {
                warn!(
                    "{}: strategy_id={} build order query failed: client_order_id={} reason={:?} err={}",
                    self.strategy_name(),
                    self.strategy_id(),
                    client_order_id,
                    reason,
                    err
                );
                false
            }
        }
    }

    fn handle_query_watchdogs(&mut self) {
        let now = get_timestamp_us();

        if let Some(w) = self.cancel_query_watchdog() {
            if now >= w.due_ts_us {
                self.set_cancel_query_watchdog(None);
                let order_mgr = MonitorChannel::instance().order_manager();
                let order_opt = order_mgr.borrow().get(w.client_order_id);
                if let Some(order) = order_opt.as_ref().filter(|o| !o.status.is_terminal()) {
                    let scheduled_at = w.due_ts_us.saturating_sub(ORDER_QUERY_WATCHDOG_DELAY_US);
                    let waited_ms = now.saturating_sub(scheduled_at).saturating_div(1_000);
                    info!(
                        "{}: strategy_id={} client_order_id={} symbol={} status={:?} exch_ord_id={:?} 等待{}ms仍未收到撤单/终态回报，发送order query回补 reason={:?}",
                        w.reason.watchdog_hint(),
                        self.strategy_id(),
                        w.client_order_id,
                        order.symbol,
                        order.status,
                        order.exchange_order_id,
                        waited_ms,
                        w.reason
                    );
                    if !self.send_order_query(w.client_order_id, w.reason) {
                        self.handoff_open_order_after_query_failure(
                            w.client_order_id,
                            w.reason.query_send_failed_trigger(),
                        );
                    }
                }
            }
        }

        if let Some(w) = self.order_query_watchdog() {
            if now >= w.due_ts_us {
                self.set_order_query_watchdog(None);
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
                        self.strategy_id(),
                        w.client_order_id,
                        order.symbol,
                        order.status,
                        order.exchange_order_id,
                        waited_ms,
                        since_submit_ms
                    );
                    if !self.send_order_query(w.client_order_id, w.reason) {
                        self.handoff_open_order_after_query_failure(
                            w.client_order_id,
                            "order query send failed",
                        );
                    }
                }
            }
        }
    }
}
