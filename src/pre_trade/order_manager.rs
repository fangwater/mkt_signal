use crate::pre_trade::runtime_flags::suppress_pre_submit_hot_path_logs;
use bytes::Bytes;
use log::{info, warn};
pub use order_common::{
    gate_text_from_client_order_id, BinanceAccountMode, Order, OrderExecutionStatus, OrderManager,
    OrderQuantizedValue, OrderStatus, OrderType, OrderUpdateSkipReason, ProtectedCumulativeFill,
    Side, TradeUpdateSkipReason, TradingVenue, CUMULATIVE_FILL_ROLLBACK_EPS,
};
use runtime_common::time_util::get_timestamp_us;
use signal_common::tick_math::QuantizedValue;
pub use symbol_utils::symbol_util::gate_currency_pair_from_symbol;
use symbol_utils::symbol_util::{
    extract_assets_from_internal_symbol, normalize_symbol_for_internal, okex_inst_id_from_symbol,
};
use trade_engine::bybit::{
    BybitCancelOrderParams, BybitCancelOrderRequest, BybitNewOrderParams, BybitNewOrderRequest,
};
use trade_engine::okex::{
    OkexCancelOrderParams, OkexCancelOrderRequest, OkexNewOrderParams, OkexNewOrderRequest,
    OkexOrderType,
};
use trade_engine::trade_request::{
    BinanceCancelOrderParams, BinanceNewOrderParams, BitgetCancelOrderParams, BitgetNewOrderParams,
    GateCancelOrderParams, GateNewOrderParams, PreparedTradeRequest,
};
fn quantize_order_decimal(value: f64) -> Option<QuantizedValue> {
    if let Some(qv) = QuantizedValue::from_decimal(value) {
        return Some(qv);
    }
    if !value.is_finite() || value <= 0.0 {
        return None;
    }

    const SCALE_EXP: i32 = -12;
    const SCALE: f64 = 1_000_000_000_000.0;
    let scaled = (value * SCALE).round();
    if !scaled.is_finite() || scaled <= 0.0 || scaled > i64::MAX as f64 {
        return None;
    }

    let mut int_value = scaled as i64;
    let mut int_exp = SCALE_EXP;
    while int_exp < 0 && int_value % 10 == 0 {
        int_value /= 10;
        int_exp += 1;
    }
    (int_value > 0).then(|| QuantizedValue::from_parts(int_value, int_exp, 1))
}

fn qv_from_order_cache(qv: OrderQuantizedValue) -> QuantizedValue {
    QuantizedValue::from_parts(qv.tick_i64, qv.tick_exp, qv.count)
}

#[derive(Debug, Clone, Copy)]
struct ResolvedOrderQuantities {
    quantity_qv: Option<QuantizedValue>,
    price_qv: Option<QuantizedValue>,
}

impl ResolvedOrderQuantities {
    fn from_order(order: &Order) -> Self {
        Self {
            quantity_qv: order_quantity_qv(order),
            price_qv: order_price_qv(order),
        }
    }

    fn quantity_text(self) -> String {
        qv_text_or_zero(self.quantity_qv)
    }

    fn price_text(self) -> String {
        qv_text_or_zero(self.price_qv)
    }

    fn require_quantity_qv(
        self,
        order: &Order,
        venue_name: &str,
    ) -> Result<QuantizedValue, String> {
        self.quantity_qv.ok_or_else(|| {
            format!(
                "failed to quantize {venue_name} quantity: qty={:.12} symbol={} client_order_id={}",
                order.quantity, order.symbol, order.client_order_id
            )
        })
    }

    fn limit_price_qv_or_zero(
        self,
        order: &Order,
        venue_name: &str,
    ) -> Result<QuantizedValue, String> {
        if order.order_type.is_limit() {
            self.price_qv.ok_or_else(|| {
                format!(
                    "failed to quantize {venue_name} price: price={:.12} symbol={} client_order_id={}",
                    order.price, order.symbol, order.client_order_id
                )
            })
        } else {
            Ok(QuantizedValue::zero())
        }
    }
}

fn order_quantity_qv(order: &Order) -> Option<QuantizedValue> {
    order
        .quantity_qv
        .map(qv_from_order_cache)
        .or_else(|| quantize_order_decimal(order.quantity))
}

fn order_price_qv(order: &Order) -> Option<QuantizedValue> {
    order
        .price_qv
        .map(qv_from_order_cache)
        .or_else(|| quantize_order_decimal(order.price))
}

fn qv_text_or_zero(qv: Option<QuantizedValue>) -> String {
    qv.map(|qv| qv.decimal_string())
        .unwrap_or_else(|| "0".to_string())
}

fn binance_margin_should_use_margin_buy(use_binance_ws_margin: bool, reduce_only: bool) -> bool {
    !use_binance_ws_margin && !reduce_only
}

const BINANCE_STANDARD_BALANCE_EPS: f64 = 1e-12;

fn binance_standard_order_lock_for_asset(order: &Order, asset: &str) -> f64 {
    if order.venue != TradingVenue::BinanceMargin
        || order.status.is_terminal()
        || !order.order_type.is_limit()
    {
        return 0.0;
    }

    let remaining_qty = (order.quantity - order.cumulative_filled_quantity).max(0.0);
    if remaining_qty <= BINANCE_STANDARD_BALANCE_EPS {
        return 0.0;
    }

    let (base_asset, quote_asset) = extract_assets_from_internal_symbol(&order.symbol);
    if order.side == Side::Buy && quote_asset.eq_ignore_ascii_case(asset) {
        (remaining_qty * order.price).max(0.0)
    } else if order.side == Side::Sell && base_asset.eq_ignore_ascii_case(asset) {
        remaining_qty
    } else {
        0.0
    }
}

fn binance_standard_available_after_local_locks(
    order_manager: &OrderManager,
    asset: &str,
    balance_position: f64,
) -> (f64, f64) {
    let local_locked = order_manager
        .get_all_ids()
        .into_iter()
        .filter_map(|order_id| order_manager.get(order_id))
        .map(|order| binance_standard_order_lock_for_asset(&order, asset))
        .sum::<f64>();
    ((balance_position - local_locked).max(0.0), local_locked)
}

fn ensure_binance_standard_margin_open_balance(
    order_manager: &OrderManager,
    venue: TradingVenue,
    symbol: &str,
    side: Side,
    quantity: f64,
    price: f64,
    reduce_only: bool,
) -> Result<(), String> {
    if venue != TradingVenue::BinanceMargin || !order_manager.binance_is_standard() || reduce_only {
        return Ok(());
    }

    let (base_asset, quote_asset) = extract_assets_from_internal_symbol(symbol);
    let (check_asset, required_amount) = match side {
        Side::Buy => (quote_asset, quantity * price),
        Side::Sell => (base_asset, quantity),
    };

    use crate::pre_trade::monitor_channel::MonitorChannel;
    let balance_position =
        MonitorChannel::instance().balance_position_for_venue(venue, &check_asset);
    let (available_estimated, local_locked) =
        binance_standard_available_after_local_locks(order_manager, &check_asset, balance_position);

    if available_estimated + BINANCE_STANDARD_BALANCE_EPS < required_amount {
        return Err(format!(
            "BinanceMargin STANDARD estimated free balance insufficient: asset={} required={:.8} balance_position={:.8} local_locked={:.8} estimated_free={:.8} symbol={} side={:?} qty={:.8} price={:.8}",
            check_asset,
            required_amount,
            balance_position,
            local_locked,
            available_estimated,
            symbol,
            side,
            quantity,
            price
        ));
    }

    Ok(())
}

fn bybit_margin_should_use_leverage(reduce_only: bool) -> bool {
    !reduce_only
}

fn bybit_symbol_from_symbol(symbol: &str) -> String {
    normalize_symbol_for_internal(symbol)
}

fn okex_order_type_from_order_type(order_type: OrderType) -> Result<OkexOrderType, String> {
    match order_type {
        OrderType::Market => Ok(OkexOrderType::Market),
        OrderType::Limit => Ok(OkexOrderType::PostOnly),
        _ => Err(format!("unsupported okex order type: {:?}", order_type)),
    }
}

pub trait PreTradeOrderManagerRequestExt {
    fn build_unmatched_cancel_bytes(
        &self,
        venue: TradingVenue,
        symbol: &str,
        client_order_id: i64,
    ) -> Result<Bytes, String>;
    #[allow(clippy::too_many_arguments)]
    fn create_open_order_request_bytes(
        &mut self,
        venue: TradingVenue,
        client_order_id: i64,
        order_type: OrderType,
        symbol: String,
        side: Side,
        quantity: f64,
        price: f64,
        quantity_qv: Option<OrderQuantizedValue>,
        price_qv: Option<OrderQuantizedValue>,
        reduce_only: bool,
        qty_multiplier: f64,
        signal_t: i64,
        signal_kind: u8,
        mkt_t: i64,
        pre_trade_recv_t: i64,
        pre_trade_handle_t: i64,
    ) -> Result<(&'static str, Bytes), String>;
    #[allow(clippy::too_many_arguments)]
    fn create_open_order_request_bytes_normalized_symbol(
        &mut self,
        venue: TradingVenue,
        client_order_id: i64,
        order_type: OrderType,
        symbol: &str,
        side: Side,
        quantity: f64,
        price: f64,
        quantity_qv: Option<OrderQuantizedValue>,
        price_qv: Option<OrderQuantizedValue>,
        reduce_only: bool,
        qty_multiplier: f64,
        signal_t: i64,
        signal_kind: u8,
        mkt_t: i64,
        pre_trade_recv_t: i64,
        pre_trade_handle_t: i64,
    ) -> Result<(&'static str, Bytes), String>;
    #[allow(clippy::too_many_arguments)]
    fn create_open_order_request_prepared_normalized_symbol(
        &mut self,
        venue: TradingVenue,
        client_order_id: i64,
        order_type: OrderType,
        symbol: &str,
        side: Side,
        quantity: f64,
        price: f64,
        quantity_qv: Option<OrderQuantizedValue>,
        price_qv: Option<OrderQuantizedValue>,
        reduce_only: bool,
        qty_multiplier: f64,
        signal_t: i64,
        signal_kind: u8,
        mkt_t: i64,
        pre_trade_recv_t: i64,
        pre_trade_handle_t: i64,
    ) -> Result<(&'static str, PreparedTradeRequest), String>;
}

impl PreTradeOrderManagerRequestExt for OrderManager {
    fn build_unmatched_cancel_bytes(
        &self,
        venue: TradingVenue,
        symbol: &str,
        client_order_id: i64,
    ) -> Result<Bytes, String> {
        if client_order_id <= 0 {
            return Err(format!(
                "invalid unmatched client_order_id for cancel: {}",
                client_order_id
            ));
        }

        match venue {
            TradingVenue::BinanceMargin => {
                let req_type = if self.binance_is_standard() {
                    trade_engine::trade_request::TradeRequestType::BinanceWsCancelMarginOrder
                } else {
                    trade_engine::trade_request::TradeRequestType::BinanceCancelMarginOrder
                };
                BinanceCancelOrderParams::request_bytes_from_parts(
                    req_type,
                    get_timestamp_us(),
                    client_order_id,
                    symbol,
                    client_order_id,
                )
                .ok_or_else(|| "failed to build binance margin cancel params".to_string())
            }
            TradingVenue::BinanceFutures => {
                let req_type = if self.binance_is_standard() {
                    trade_engine::trade_request::TradeRequestType::BinanceWsCancelUMOrder
                } else {
                    trade_engine::trade_request::TradeRequestType::BinanceCancelUMOrder
                };
                BinanceCancelOrderParams::request_bytes_from_parts(
                    req_type,
                    get_timestamp_us(),
                    client_order_id,
                    symbol,
                    client_order_id,
                )
                .ok_or_else(|| "failed to build binance um cancel params".to_string())
            }
            _ => Err(format!(
                "unmatched cancel fallback not supported for venue {:?}",
                venue
            )),
        }
    }

    fn create_open_order_request_bytes(
        &mut self,
        venue: TradingVenue,
        client_order_id: i64,
        order_type: OrderType,
        symbol: String,
        side: Side,
        quantity: f64,
        price: f64,
        quantity_qv: Option<OrderQuantizedValue>,
        price_qv: Option<OrderQuantizedValue>,
        reduce_only: bool,
        qty_multiplier: f64,
        signal_t: i64,
        signal_kind: u8,
        mkt_t: i64,
        pre_trade_recv_t: i64,
        pre_trade_handle_t: i64,
    ) -> Result<(&'static str, Bytes), String> {
        let symbol = normalize_symbol_for_internal(&symbol);
        self.create_open_order_request_bytes_normalized_symbol(
            venue,
            client_order_id,
            order_type,
            &symbol,
            side,
            quantity,
            price,
            quantity_qv,
            price_qv,
            reduce_only,
            qty_multiplier,
            signal_t,
            signal_kind,
            mkt_t,
            pre_trade_recv_t,
            pre_trade_handle_t,
        )
    }

    fn create_open_order_request_bytes_normalized_symbol(
        &mut self,
        venue: TradingVenue,
        client_order_id: i64,
        order_type: OrderType,
        symbol: &str,
        side: Side,
        quantity: f64,
        price: f64,
        quantity_qv: Option<OrderQuantizedValue>,
        price_qv: Option<OrderQuantizedValue>,
        reduce_only: bool,
        qty_multiplier: f64,
        signal_t: i64,
        signal_kind: u8,
        mkt_t: i64,
        pre_trade_recv_t: i64,
        pre_trade_handle_t: i64,
    ) -> Result<(&'static str, Bytes), String> {
        ensure_binance_standard_margin_open_balance(
            self,
            venue,
            symbol,
            side,
            quantity,
            price,
            reduce_only,
        )?;
        let Some(result) = self.create_order_with_mut_normalized_symbol(
            venue,
            client_order_id,
            order_type,
            symbol,
            side,
            quantity,
            price,
            reduce_only,
            qty_multiplier,
            true,
            |order| {
                if let Some(quantity_qv) = quantity_qv {
                    order.set_quantity_qv(quantity_qv);
                }
                if let Some(price_qv) = price_qv {
                    order.set_price_qv(price_qv);
                }
                order.set_signal_meta(signal_t, signal_kind);
                if mkt_t > 0 {
                    order.set_mkt_time(mkt_t);
                }
                if pre_trade_recv_t > 0 || pre_trade_handle_t > 0 {
                    order.set_pre_trade_open_trace(pre_trade_recv_t, pre_trade_handle_t);
                }
                let exchange = order.venue.trade_engine_exchange();
                order
                    .get_order_request_bytes()
                    .map(|req_bin| (exchange, req_bin))
            },
        ) else {
            return Err(format!(
                "order not found after create: client_order_id={}",
                client_order_id
            ));
        };
        result
    }

    fn create_open_order_request_prepared_normalized_symbol(
        &mut self,
        venue: TradingVenue,
        client_order_id: i64,
        order_type: OrderType,
        symbol: &str,
        side: Side,
        quantity: f64,
        price: f64,
        quantity_qv: Option<OrderQuantizedValue>,
        price_qv: Option<OrderQuantizedValue>,
        reduce_only: bool,
        qty_multiplier: f64,
        signal_t: i64,
        signal_kind: u8,
        mkt_t: i64,
        pre_trade_recv_t: i64,
        pre_trade_handle_t: i64,
    ) -> Result<(&'static str, PreparedTradeRequest), String> {
        ensure_binance_standard_margin_open_balance(
            self,
            venue,
            symbol,
            side,
            quantity,
            price,
            reduce_only,
        )?;
        let Some(result) = self.create_order_with_mut_normalized_symbol(
            venue,
            client_order_id,
            order_type,
            symbol,
            side,
            quantity,
            price,
            reduce_only,
            qty_multiplier,
            true,
            |order| {
                if let Some(quantity_qv) = quantity_qv {
                    order.set_quantity_qv(quantity_qv);
                }
                if let Some(price_qv) = price_qv {
                    order.set_price_qv(price_qv);
                }
                order.set_signal_meta(signal_t, signal_kind);
                if mkt_t > 0 {
                    order.set_mkt_time(mkt_t);
                }
                if pre_trade_recv_t > 0 || pre_trade_handle_t > 0 {
                    order.set_pre_trade_open_trace(pre_trade_recv_t, pre_trade_handle_t);
                }
                let exchange = order.venue.trade_engine_exchange();
                order
                    .get_order_request_prepared()
                    .map(|request| (exchange, request))
            },
        ) else {
            return Err(format!(
                "order not found after create: client_order_id={}",
                client_order_id
            ));
        };
        result
    }
}

pub trait PreTradeOrderRequestExt {
    fn get_order_cancel_bytes(&self) -> Result<Bytes, String>;
    fn get_order_request_bytes(&self) -> Result<Bytes, String>;
    fn get_order_request_prepared(&self) -> Result<PreparedTradeRequest, String>;
}

impl PreTradeOrderRequestExt for Order {
    fn get_order_cancel_bytes(&self) -> Result<Bytes, String> {
        let now = get_timestamp_us();
        match self.venue {
            TradingVenue::BinanceMargin => {
                // 使用 origClientOrderId 以客户端订单ID撤单；当前未保存交易所 orderId
                let req_type =
                    if self.require_binance_account_mode() == BinanceAccountMode::Standard {
                        trade_engine::trade_request::TradeRequestType::BinanceWsCancelMarginOrder
                    } else {
                        trade_engine::trade_request::TradeRequestType::BinanceCancelMarginOrder
                    };
                BinanceCancelOrderParams::request_bytes_from_parts(
                    req_type,
                    now,
                    self.client_order_id,
                    &self.symbol,
                    self.client_order_id,
                )
                .ok_or_else(|| "failed to build binance margin cancel params".to_string())
            }
            TradingVenue::BinanceFutures => {
                let req_type =
                    if self.require_binance_account_mode() == BinanceAccountMode::Standard {
                        trade_engine::trade_request::TradeRequestType::BinanceWsCancelUMOrder
                    } else {
                        trade_engine::trade_request::TradeRequestType::BinanceCancelUMOrder
                    };
                BinanceCancelOrderParams::request_bytes_from_parts(
                    req_type,
                    now,
                    self.client_order_id,
                    &self.symbol,
                    self.client_order_id,
                )
                .ok_or_else(|| "failed to build binance um cancel params".to_string())
            }
            TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
                let inst_id = okex_inst_id_from_symbol(&self.symbol, self.venue)?;
                // Use a distinct request id to avoid overwriting inflight mapping for the original order.
                let mut cancel_req_id = now;
                if cancel_req_id == self.client_order_id {
                    cancel_req_id = cancel_req_id.saturating_add(1);
                }
                let params = OkexCancelOrderParams {
                    ord_id: self.exchange_order_id.unwrap_or(0),
                    cl_ord_id: self.client_order_id,
                    inst_id,
                };
                let request = match self.venue {
                    TradingVenue::OkexMargin => {
                        OkexCancelOrderRequest::create_margin(now, cancel_req_id, params)
                    }
                    TradingVenue::OkexFutures => {
                        OkexCancelOrderRequest::create_um(now, cancel_req_id, params)
                    }
                    _ => None,
                }
                .ok_or_else(|| "failed to build okex cancel request".to_string())?;
                Ok(request.to_bytes())
            }
            TradingVenue::GateMargin => {
                let currency_pair = gate_currency_pair_from_symbol(&self.symbol);
                let order_id = self
                    .exchange_order_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| gate_text_from_client_order_id(self.client_order_id));
                GateCancelOrderParams::request_bytes_from_parts(
                    trade_engine::trade_request::TradeRequestType::GateUnifiedCancelOrder,
                    now,
                    self.client_order_id,
                    &currency_pair,
                    &order_id,
                )
                .ok_or_else(|| "failed to build gate unified cancel params".to_string())
            }
            TradingVenue::GateFutures => {
                let contract = gate_currency_pair_from_symbol(&self.symbol);
                let order_id = self
                    .exchange_order_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| gate_text_from_client_order_id(self.client_order_id));
                GateCancelOrderParams::request_bytes_from_parts(
                    trade_engine::trade_request::TradeRequestType::GateFuturesCancelOrder,
                    now,
                    self.client_order_id,
                    &contract,
                    &order_id,
                )
                .ok_or_else(|| "failed to build gate futures cancel params".to_string())
            }
            TradingVenue::BybitMargin | TradingVenue::BybitFutures => {
                let symbol = bybit_symbol_from_symbol(&self.symbol);
                let params = BybitCancelOrderParams {
                    symbol,
                    order_link_id: self.client_order_id,
                };
                let request = match self.venue {
                    TradingVenue::BybitMargin => {
                        BybitCancelOrderRequest::create_margin(now, self.client_order_id, params)
                    }
                    TradingVenue::BybitFutures => {
                        BybitCancelOrderRequest::create_um(now, self.client_order_id, params)
                    }
                    _ => None,
                }
                .ok_or_else(|| "failed to build bybit cancel request".to_string())?;
                Ok(request.to_bytes())
            }
            TradingVenue::BitgetMargin | TradingVenue::BitgetFutures => {
                let order_id = self
                    .exchange_order_id
                    .filter(|&id| id > 0)
                    .map(|id| id.to_string());
                let client_order_id = self.client_order_id.to_string();
                let req_type = match self.venue {
                    TradingVenue::BitgetMargin => {
                        trade_engine::trade_request::TradeRequestType::BitgetCancelMarginOrder
                    }
                    TradingVenue::BitgetFutures => {
                        trade_engine::trade_request::TradeRequestType::BitgetCancelUMOrder
                    }
                    _ => unreachable!(),
                };
                BitgetCancelOrderParams::request_bytes_from_parts(
                    req_type,
                    now,
                    self.client_order_id,
                    order_id.as_deref(),
                    &client_order_id,
                )
                .ok_or_else(|| "failed to build bitget cancel params".to_string())
            }
            _ => Err(format!("Unsupported trading venue: {:?}", self.venue)),
        }
    }

    fn get_order_request_bytes(&self) -> Result<Bytes, String> {
        if self.order_type.is_limit() && self.price <= 0.0 {
            return Err(format!(
                "invalid limit price: price={:.8} order_type={:?} symbol={} client_order_id={}",
                self.price, self.order_type, self.symbol, self.client_order_id
            ));
        }
        let resolved = ResolvedOrderQuantities::from_order(self);

        match self.venue {
            //币安的杠杆账户下单
            TradingVenue::BinanceMargin => {
                let use_binance_ws_margin =
                    self.require_binance_account_mode() == BinanceAccountMode::Standard;
                let local_create_ts = get_timestamp_us();
                // ===== 余额检查和日志记录 =====
                // 提取 base asset 和 quote asset
                let (base_asset, quote_asset) = extract_assets_from_internal_symbol(&self.symbol);

                // 根据 side 确定需要检查的资产和所需金额
                let (check_asset, required_amount) = match self.side {
                    Side::Buy => {
                        // BUY: 需要 quote asset (USDT) 的余额
                        let required = self.quantity * self.price;
                        (quote_asset, required)
                    }
                    Side::Sell => {
                        // SELL: 需要 base asset 的余额
                        (base_asset, self.quantity)
                    }
                };

                // 从 MonitorChannel 获取 basic margin 余额（当前实现以净余额作为可用余额近似）
                use crate::pre_trade::monitor_channel::MonitorChannel;
                let available_balance =
                    MonitorChannel::instance().balance_position_for_venue(self.venue, &check_asset);

                // 余额判断：保留 reduce-only 防护和日志。
                // PM REST 的 crossMarginFree 无 websocket 推送，本地净钱包余额不能可靠判断是否需要借币；
                // 非 reduce-only PM 下单统一带 MARGIN_BUY，避免 free 被挂单占用时误走 NO_SIDE_EFFECT。
                if available_balance < required_amount {
                    let borrow_amount = required_amount - available_balance;
                    if use_binance_ws_margin || self.reduce_only {
                        return Err(format!(
                            "BinanceMargin order has insufficient balance: mode={} asset={} required={:.8} available={:.8} borrow={:.8} symbol={} side={:?} reduce_only={} qty={} price={}",
                            if use_binance_ws_margin {
                                "STANDARD"
                            } else {
                                "UNIFIED"
                            },
                            check_asset,
                            required_amount,
                            available_balance,
                            borrow_amount,
                            self.symbol,
                            self.side,
                            self.reduce_only,
                            resolved.quantity_text(),
                            resolved.price_text()
                        ));
                    }
                    if !suppress_pre_submit_hot_path_logs()
                        && !(use_binance_ws_margin && self.side == Side::Sell)
                    {
                        warn!(
                            "💰 余额不足将借币: 资产={} 需要={:.8} 可用={:.8} 需借={:.8} symbol={} side={:?} qty={} price={}",
                            check_asset, required_amount, available_balance, borrow_amount,
                            self.symbol,
                            self.side,
                            resolved.quantity_text(),
                            resolved.price_text()
                        );
                    }
                    if use_binance_ws_margin && !suppress_pre_submit_hot_path_logs() {
                        info!(
                            "BinanceMargin STANDARD mode: omit sideEffectType for symbol={} side={:?}",
                            self.symbol, self.side
                        );
                    }
                } else if !suppress_pre_submit_hot_path_logs() {
                    info!(
                        "✅ 余额充足: 资产={} 需要={:.8} 可用={:.8} symbol={} side={:?}",
                        check_asset, required_amount, available_balance, self.symbol, self.side
                    );
                    // 本地余额充足只代表净钱包口径充足；PM REST 仍可能需要自动借币。
                }
                let margin_buy =
                    binance_margin_should_use_margin_buy(use_binance_ws_margin, self.reduce_only);
                // ===== 余额检查结束 =====/

                let quantity_qv = resolved.require_quantity_qv(self, "binance")?;
                let price_qv = resolved.limit_price_qv_or_zero(self, "binance")?;
                if !suppress_pre_submit_hot_path_logs() {
                    info!(
                        "OrderManager: venue={:?} client_order_id={} symbol={} side={:?} type={:?} reduce_only={} typed_params=binance_new_order",
                        self.venue,
                        self.client_order_id,
                        self.symbol,
                        self.side,
                        self.order_type,
                        self.reduce_only
                    );
                }
                let req_type = if use_binance_ws_margin {
                    trade_engine::trade_request::TradeRequestType::BinanceWsNewMarginOrder
                } else {
                    trade_engine::trade_request::TradeRequestType::BinanceNewMarginOrder
                };
                BinanceNewOrderParams::request_bytes_from_parts(
                    req_type,
                    local_create_ts,
                    self.client_order_id,
                    &self.symbol,
                    self.side,
                    self.order_type,
                    quantity_qv,
                    price_qv,
                    self.reduce_only,
                    margin_buy,
                    use_binance_ws_margin,
                    false,
                    use_binance_ws_margin,
                )
                .ok_or_else(|| "failed to build binance margin order params".to_string())
            }
            TradingVenue::BinanceFutures => {
                let use_binance_ws_um =
                    self.require_binance_account_mode() == BinanceAccountMode::Standard;
                let local_create_ts = get_timestamp_us();
                let quantity_qv = resolved.require_quantity_qv(self, "binance")?;
                let price_qv = resolved.limit_price_qv_or_zero(self, "binance")?;
                if !suppress_pre_submit_hot_path_logs() {
                    info!(
                        "OrderManager: venue={:?} client_order_id={} symbol={} side={:?} type={:?} reduce_only={} typed_params=binance_new_order",
                        self.venue,
                        self.client_order_id,
                        self.symbol,
                        self.side,
                        self.order_type,
                        self.reduce_only
                    );
                }
                let req_type = if use_binance_ws_um {
                    trade_engine::trade_request::TradeRequestType::BinanceWsNewUMOrder
                } else {
                    trade_engine::trade_request::TradeRequestType::BinanceNewUMOrder
                };
                BinanceNewOrderParams::request_bytes_from_parts(
                    req_type,
                    local_create_ts,
                    self.client_order_id,
                    &self.symbol,
                    self.side,
                    self.order_type,
                    quantity_qv,
                    price_qv,
                    self.reduce_only,
                    false,
                    false,
                    use_binance_ws_um,
                    false,
                )
                .ok_or_else(|| "failed to build binance um order params".to_string())
            }
            TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
                let create_ts = get_timestamp_us();
                let inst_id = okex_inst_id_from_symbol(&self.symbol, self.venue)?;
                let okex_order_type = okex_order_type_from_order_type(self.order_type)?;
                let quantity_qv = resolved.require_quantity_qv(self, "okex")?;
                let price_qv = resolved.limit_price_qv_or_zero(self, "okex")?;

                let params = OkexNewOrderParams {
                    side: self.side,
                    order_type: okex_order_type,
                    quantity_qv,
                    price_qv,
                    symbol: inst_id,
                    reduce_only: self.reduce_only,
                    client_order_id: self.client_order_id,
                };

                let request = match self.venue {
                    TradingVenue::OkexMargin => {
                        OkexNewOrderRequest::create_margin(create_ts, self.client_order_id, params)
                    }
                    TradingVenue::OkexFutures => {
                        OkexNewOrderRequest::create_um(create_ts, self.client_order_id, params)
                    }
                    _ => None,
                }
                .ok_or_else(|| "failed to build okex new order request".to_string())?;
                Ok(request.to_bytes())
            }
            TradingVenue::GateMargin => {
                let create_ts = get_timestamp_us();
                let currency_pair = gate_currency_pair_from_symbol(&self.symbol);
                if !matches!(self.order_type, OrderType::Limit | OrderType::Market) {
                    return Err(format!(
                        "unsupported gate order type: {:?}",
                        self.order_type
                    ));
                }
                let quantity_qv = resolved.require_quantity_qv(self, "gate")?;
                let price_qv = resolved.limit_price_qv_or_zero(self, "gate")?;
                GateNewOrderParams::request_bytes_from_parts(
                    trade_engine::trade_request::TradeRequestType::GateUnifiedNewOrder,
                    create_ts,
                    self.client_order_id,
                    &currency_pair,
                    self.side,
                    self.order_type,
                    quantity_qv,
                    price_qv,
                    self.reduce_only,
                    // 仅 unified/cross-margin 买入单可能触发本单借款；成交所得用于偿还本单借入。
                    self.side == Side::Buy,
                )
                .ok_or_else(|| "failed to build gate unified order params".to_string())
            }
            TradingVenue::GateFutures => {
                let create_ts = get_timestamp_us();
                let contract = gate_currency_pair_from_symbol(&self.symbol);
                if !matches!(self.order_type, OrderType::Limit | OrderType::Market) {
                    return Err(format!(
                        "unsupported gate order type: {:?}",
                        self.order_type
                    ));
                }
                let quantity_qv = resolved.require_quantity_qv(self, "gate")?;
                let price_qv = resolved.limit_price_qv_or_zero(self, "gate")?;
                GateNewOrderParams::request_bytes_from_parts(
                    trade_engine::trade_request::TradeRequestType::GateFuturesNewOrder,
                    create_ts,
                    self.client_order_id,
                    &contract,
                    self.side,
                    self.order_type,
                    quantity_qv,
                    price_qv,
                    self.reduce_only,
                    false,
                )
                .ok_or_else(|| "failed to build gate futures order params".to_string())
            }
            TradingVenue::BybitMargin | TradingVenue::BybitFutures => {
                let create_ts = get_timestamp_us();
                let symbol = bybit_symbol_from_symbol(&self.symbol);
                let quantity_qv = resolved.require_quantity_qv(self, "bybit")?;
                let price_qv = resolved.limit_price_qv_or_zero(self, "bybit")?;
                let params = BybitNewOrderParams {
                    side: self.side,
                    order_type: self.order_type,
                    reduce_only: self.reduce_only,
                    is_leverage: matches!(self.venue, TradingVenue::BybitMargin)
                        && bybit_margin_should_use_leverage(self.reduce_only),
                    quantity_qv,
                    price_qv,
                    symbol,
                };
                match self.venue {
                    TradingVenue::BybitMargin => BybitNewOrderRequest::margin_order_bytes(
                        create_ts,
                        self.client_order_id,
                        &params,
                    ),
                    TradingVenue::BybitFutures => BybitNewOrderRequest::um_order_bytes(
                        create_ts,
                        self.client_order_id,
                        &params,
                    ),
                    _ => None,
                }
                .ok_or_else(|| "failed to build bybit new order request".to_string())
            }
            TradingVenue::BitgetMargin => {
                let create_ts = get_timestamp_us();
                let quantity_qv = resolved.require_quantity_qv(self, "bitget")?;
                let price_qv = resolved.limit_price_qv_or_zero(self, "bitget")?;
                BitgetNewOrderParams::request_bytes_from_parts(
                    trade_engine::trade_request::TradeRequestType::BitgetNewMarginOrder,
                    create_ts,
                    self.client_order_id,
                    &self.symbol,
                    self.side,
                    self.order_type,
                    quantity_qv,
                    price_qv,
                    self.reduce_only,
                )
                .ok_or_else(|| "failed to build bitget margin order params".to_string())
            }
            TradingVenue::BitgetFutures => {
                let create_ts = get_timestamp_us();
                // trade_engine precheck 强制 Bitget UTA futures 为 one_way_mode。
                // one-way 模式下开/平仓由 side + reduceOnly 表达，不传 hedge-mode 的 posSide。
                let quantity_qv = resolved.require_quantity_qv(self, "bitget")?;
                let price_qv = resolved.limit_price_qv_or_zero(self, "bitget")?;
                BitgetNewOrderParams::request_bytes_from_parts(
                    trade_engine::trade_request::TradeRequestType::BitgetNewUMOrder,
                    create_ts,
                    self.client_order_id,
                    &self.symbol,
                    self.side,
                    self.order_type,
                    quantity_qv,
                    price_qv,
                    self.reduce_only,
                )
                .ok_or_else(|| "failed to build bitget um order params".to_string())
            }
            //之后在这支持别的类型下单，根据资产类型决定下单的request，统一序列化为bytes
            _ => Err(format!("Unsupported trading venue: {:?}", self.venue)),
        }
    }

    fn get_order_request_prepared(&self) -> Result<PreparedTradeRequest, String> {
        if self.order_type.is_limit() && self.price <= 0.0 {
            return Err(format!(
                "invalid limit price: price={:.8} order_type={:?} symbol={} client_order_id={}",
                self.price, self.order_type, self.symbol, self.client_order_id
            ));
        }
        let resolved = ResolvedOrderQuantities::from_order(self);

        match self.venue {
            TradingVenue::BinanceMargin => {
                let use_binance_ws_margin =
                    self.require_binance_account_mode() == BinanceAccountMode::Standard;
                let local_create_ts = get_timestamp_us();
                let (base_asset, quote_asset) = extract_assets_from_internal_symbol(&self.symbol);
                let (check_asset, required_amount) = match self.side {
                    Side::Buy => (quote_asset, self.quantity * self.price),
                    Side::Sell => (base_asset, self.quantity),
                };

                use crate::pre_trade::monitor_channel::MonitorChannel;
                let available_balance =
                    MonitorChannel::instance().balance_position_for_venue(self.venue, &check_asset);

                if available_balance < required_amount {
                    let borrow_amount = required_amount - available_balance;
                    if use_binance_ws_margin || self.reduce_only {
                        return Err(format!(
                            "BinanceMargin order has insufficient balance: mode={} asset={} required={:.8} available={:.8} borrow={:.8} symbol={} side={:?} reduce_only={} qty={} price={}",
                            if use_binance_ws_margin {
                                "STANDARD"
                            } else {
                                "UNIFIED"
                            },
                            check_asset,
                            required_amount,
                            available_balance,
                            borrow_amount,
                            self.symbol,
                            self.side,
                            self.reduce_only,
                            resolved.quantity_text(),
                            resolved.price_text()
                        ));
                    }
                    if !suppress_pre_submit_hot_path_logs()
                        && !(use_binance_ws_margin && self.side == Side::Sell)
                    {
                        warn!(
                            "💰 余额不足将借币: 资产={} 需要={:.8} 可用={:.8} 需借={:.8} symbol={} side={:?} qty={} price={}",
                            check_asset, required_amount, available_balance, borrow_amount,
                            self.symbol,
                            self.side,
                            resolved.quantity_text(),
                            resolved.price_text()
                        );
                    }
                    if use_binance_ws_margin && !suppress_pre_submit_hot_path_logs() {
                        info!(
                            "BinanceMargin STANDARD mode: omit sideEffectType for symbol={} side={:?}",
                            self.symbol, self.side
                        );
                    }
                } else if !suppress_pre_submit_hot_path_logs() {
                    info!(
                        "✅ 余额充足: 资产={} 需要={:.8} 可用={:.8} symbol={} side={:?}",
                        check_asset, required_amount, available_balance, self.symbol, self.side
                    );
                }
                let margin_buy =
                    binance_margin_should_use_margin_buy(use_binance_ws_margin, self.reduce_only);
                let quantity_qv = resolved.require_quantity_qv(self, "binance")?;
                let price_qv = resolved.limit_price_qv_or_zero(self, "binance")?;
                if !suppress_pre_submit_hot_path_logs() {
                    info!(
                        "OrderManager: venue={:?} client_order_id={} symbol={} side={:?} type={:?} reduce_only={} typed_params=binance_new_order",
                        self.venue,
                        self.client_order_id,
                        self.symbol,
                        self.side,
                        self.order_type,
                        self.reduce_only
                    );
                }
                let req_type = if use_binance_ws_margin {
                    trade_engine::trade_request::TradeRequestType::BinanceWsNewMarginOrder
                } else {
                    trade_engine::trade_request::TradeRequestType::BinanceNewMarginOrder
                };
                BinanceNewOrderParams::prepared_request_from_parts(
                    req_type,
                    local_create_ts,
                    self.client_order_id,
                    &self.symbol,
                    self.side,
                    self.order_type,
                    quantity_qv,
                    price_qv,
                    self.reduce_only,
                    margin_buy,
                    use_binance_ws_margin,
                    false,
                    use_binance_ws_margin,
                )
                .ok_or_else(|| "failed to build binance margin order params".to_string())
            }
            TradingVenue::BinanceFutures => {
                let use_binance_ws_um =
                    self.require_binance_account_mode() == BinanceAccountMode::Standard;
                let local_create_ts = get_timestamp_us();
                let quantity_qv = resolved.require_quantity_qv(self, "binance")?;
                let price_qv = resolved.limit_price_qv_or_zero(self, "binance")?;
                if !suppress_pre_submit_hot_path_logs() {
                    info!(
                        "OrderManager: venue={:?} client_order_id={} symbol={} side={:?} type={:?} reduce_only={} typed_params=binance_new_order",
                        self.venue,
                        self.client_order_id,
                        self.symbol,
                        self.side,
                        self.order_type,
                        self.reduce_only
                    );
                }
                let req_type = if use_binance_ws_um {
                    trade_engine::trade_request::TradeRequestType::BinanceWsNewUMOrder
                } else {
                    trade_engine::trade_request::TradeRequestType::BinanceNewUMOrder
                };
                BinanceNewOrderParams::prepared_request_from_parts(
                    req_type,
                    local_create_ts,
                    self.client_order_id,
                    &self.symbol,
                    self.side,
                    self.order_type,
                    quantity_qv,
                    price_qv,
                    self.reduce_only,
                    false,
                    false,
                    use_binance_ws_um,
                    false,
                )
                .ok_or_else(|| "failed to build binance um order params".to_string())
            }
            TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
                let create_ts = get_timestamp_us();
                let inst_id = okex_inst_id_from_symbol(&self.symbol, self.venue)?;
                let okex_order_type = okex_order_type_from_order_type(self.order_type)?;
                let quantity_qv = resolved.require_quantity_qv(self, "okex")?;
                let price_qv = resolved.limit_price_qv_or_zero(self, "okex")?;

                let params = OkexNewOrderParams {
                    side: self.side,
                    order_type: okex_order_type,
                    quantity_qv,
                    price_qv,
                    symbol: inst_id,
                    reduce_only: self.reduce_only,
                    client_order_id: self.client_order_id,
                };

                match self.venue {
                    TradingVenue::OkexMargin => OkexNewOrderRequest::prepared_margin(
                        create_ts,
                        self.client_order_id,
                        params,
                    ),
                    TradingVenue::OkexFutures => {
                        OkexNewOrderRequest::prepared_um(create_ts, self.client_order_id, params)
                    }
                    _ => None,
                }
                .ok_or_else(|| "failed to build okex new order request".to_string())
            }
            TradingVenue::GateMargin => {
                let create_ts = get_timestamp_us();
                let currency_pair = gate_currency_pair_from_symbol(&self.symbol);
                if !matches!(self.order_type, OrderType::Limit | OrderType::Market) {
                    return Err(format!(
                        "unsupported gate order type: {:?}",
                        self.order_type
                    ));
                }
                let quantity_qv = resolved.require_quantity_qv(self, "gate")?;
                let price_qv = resolved.limit_price_qv_or_zero(self, "gate")?;
                GateNewOrderParams::prepared_request_from_parts(
                    trade_engine::trade_request::TradeRequestType::GateUnifiedNewOrder,
                    create_ts,
                    self.client_order_id,
                    &currency_pair,
                    self.side,
                    self.order_type,
                    quantity_qv,
                    price_qv,
                    self.reduce_only,
                    self.side == Side::Buy,
                )
                .ok_or_else(|| "failed to build gate unified order params".to_string())
            }
            TradingVenue::GateFutures => {
                let create_ts = get_timestamp_us();
                let contract = gate_currency_pair_from_symbol(&self.symbol);
                if !matches!(self.order_type, OrderType::Limit | OrderType::Market) {
                    return Err(format!(
                        "unsupported gate order type: {:?}",
                        self.order_type
                    ));
                }
                let quantity_qv = resolved.require_quantity_qv(self, "gate")?;
                let price_qv = resolved.limit_price_qv_or_zero(self, "gate")?;
                GateNewOrderParams::prepared_request_from_parts(
                    trade_engine::trade_request::TradeRequestType::GateFuturesNewOrder,
                    create_ts,
                    self.client_order_id,
                    &contract,
                    self.side,
                    self.order_type,
                    quantity_qv,
                    price_qv,
                    self.reduce_only,
                    false,
                )
                .ok_or_else(|| "failed to build gate futures order params".to_string())
            }
            TradingVenue::BitgetMargin => {
                let create_ts = get_timestamp_us();
                let quantity_qv = resolved.require_quantity_qv(self, "bitget")?;
                let price_qv = resolved.limit_price_qv_or_zero(self, "bitget")?;
                BitgetNewOrderParams::prepared_request_from_parts(
                    trade_engine::trade_request::TradeRequestType::BitgetNewMarginOrder,
                    create_ts,
                    self.client_order_id,
                    &self.symbol,
                    self.side,
                    self.order_type,
                    quantity_qv,
                    price_qv,
                    self.reduce_only,
                )
                .ok_or_else(|| "failed to build bitget margin order params".to_string())
            }
            TradingVenue::BitgetFutures => {
                let create_ts = get_timestamp_us();
                let quantity_qv = resolved.require_quantity_qv(self, "bitget")?;
                let price_qv = resolved.limit_price_qv_or_zero(self, "bitget")?;
                BitgetNewOrderParams::prepared_request_from_parts(
                    trade_engine::trade_request::TradeRequestType::BitgetNewUMOrder,
                    create_ts,
                    self.client_order_id,
                    &self.symbol,
                    self.side,
                    self.order_type,
                    quantity_qv,
                    price_qv,
                    self.reduce_only,
                )
                .ok_or_else(|| "failed to build bitget um order params".to_string())
            }
            TradingVenue::BybitMargin | TradingVenue::BybitFutures => {
                let create_ts = get_timestamp_us();
                let quantity_qv = resolved.require_quantity_qv(self, "bybit")?;
                let price_qv = resolved.limit_price_qv_or_zero(self, "bybit")?;
                let symbol = bybit_symbol_from_symbol(&self.symbol);
                let params = BybitNewOrderParams {
                    side: self.side,
                    order_type: self.order_type,
                    reduce_only: self.reduce_only,
                    is_leverage: matches!(self.venue, TradingVenue::BybitMargin)
                        && bybit_margin_should_use_leverage(self.reduce_only),
                    quantity_qv,
                    price_qv,
                    symbol,
                };
                match self.venue {
                    TradingVenue::BybitMargin => BybitNewOrderRequest::prepared_margin_order(
                        create_ts,
                        self.client_order_id,
                        &params,
                    ),
                    TradingVenue::BybitFutures => BybitNewOrderRequest::prepared_um_order(
                        create_ts,
                        self.client_order_id,
                        &params,
                    ),
                    _ => None,
                }
                .ok_or_else(|| "failed to build bybit new order request".to_string())
            }
            _ => Err(format!(
                "prepared order request unsupported for venue {:?}",
                self.venue
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        binance_margin_should_use_margin_buy, binance_standard_available_after_local_locks,
        bybit_margin_should_use_leverage, BybitNewOrderRequest, Order, OrderExecutionStatus,
        OrderManager, OrderQuantizedValue, OrderStatus, OrderType, PreTradeOrderManagerRequestExt,
        PreTradeOrderRequestExt, Side, TradeUpdateSkipReason,
    };
    use order_common::{BinanceAccountMode, TradingVenue};
    use serde_json::Value;
    use symbol_utils::symbol_util::extract_assets_from_internal_symbol;
    use trade_engine::trade_request::{TradeRequestMsg, TradeRequestType};
    use trade_engine::{bitget_ws, gate_ws};

    fn extract_request_json(bytes: &[u8]) -> Value {
        let msg = TradeRequestMsg::parse(bytes).expect("trade request should parse");
        match msg.req_type {
            TradeRequestType::GateUnifiedNewOrder
            | TradeRequestType::GateFuturesNewOrder
            | TradeRequestType::GateUnifiedCancelOrder
            | TradeRequestType::GateFuturesCancelOrder => {
                let payload =
                    gate_ws::build_api_payload(&msg, 999).expect("gate ws payload should build");
                let payload: Value =
                    serde_json::from_str(&payload).expect("gate ws payload should be json");
                payload["payload"]["req_param"].clone()
            }
            TradeRequestType::BitgetNewMarginOrder
            | TradeRequestType::BitgetNewUMOrder
            | TradeRequestType::BitgetCancelMarginOrder
            | TradeRequestType::BitgetCancelUMOrder => {
                let payload = bitget_ws::build_order_payload(&msg, 999)
                    .expect("bitget ws payload should build");
                let payload: Value =
                    serde_json::from_str(&payload).expect("bitget ws payload should be json");
                payload["args"][0].clone()
            }
            _ => serde_json::from_slice(&msg.params).expect("request params should be valid json"),
        }
    }

    fn extract_bitget_ws_payload_json(bytes: &[u8]) -> Value {
        let msg = TradeRequestMsg::parse(bytes).expect("trade request should parse");
        let payload =
            bitget_ws::build_order_payload(&msg, 999).expect("bitget ws payload should build");
        serde_json::from_str(&payload).expect("bitget ws payload should be json")
    }

    #[test]
    fn binance_pm_margin_open_uses_margin_buy() {
        assert!(binance_margin_should_use_margin_buy(false, false));
    }

    #[test]
    fn binance_standard_or_reduce_only_margin_open_omits_margin_buy() {
        assert!(!binance_margin_should_use_margin_buy(true, false));
        assert!(!binance_margin_should_use_margin_buy(false, true));
    }

    #[test]
    fn binance_standard_available_subtracts_local_open_order_locks() {
        let mut manager = OrderManager::new(Some(BinanceAccountMode::Standard));
        manager.create_order(
            TradingVenue::BinanceMargin,
            1001,
            OrderType::Limit,
            "BTCUSDT".to_string(),
            Side::Buy,
            2.0,
            100.0,
            false,
            1.0,
        );
        manager.update(1001, |order| {
            order.status = OrderExecutionStatus::Create;
            order.cumulative_filled_quantity = 0.25;
        });

        let (available, locked) =
            binance_standard_available_after_local_locks(&manager, "USDT", 1_000.0);

        assert!((locked - 175.0).abs() < 1e-12);
        assert!((available - 825.0).abs() < 1e-12);

        manager.update(1001, |order| order.status = OrderExecutionStatus::Cancelled);
        let (available_after_cancel, locked_after_cancel) =
            binance_standard_available_after_local_locks(&manager, "USDT", 1_000.0);
        assert_eq!(locked_after_cancel, 0.0);
        assert_eq!(available_after_cancel, 1_000.0);
    }

    #[test]
    fn bybit_margin_reduce_only_omits_leverage() {
        assert!(bybit_margin_should_use_leverage(false));
        assert!(!bybit_margin_should_use_leverage(true));
    }

    #[test]
    fn bybit_margin_order_sets_leverage_only_for_non_reduce_only() {
        let open_order = Order::new(
            TradingVenue::BybitMargin,
            41,
            OrderType::Market,
            "CCUSDT".to_string(),
            Side::Buy,
            10.0,
            0.0,
            false,
            1.0,
            None,
            true,
        );
        let close_order = Order::new(
            TradingVenue::BybitMargin,
            42,
            OrderType::Market,
            "CCUSDT".to_string(),
            Side::Sell,
            10.0,
            0.0,
            true,
            1.0,
            None,
            true,
        );

        let open_request = BybitNewOrderRequest::from_bytes(
            open_order
                .get_order_request_bytes()
                .expect("bybit margin open request should build")
                .as_ref(),
        )
        .expect("bybit margin open request should parse");
        let close_request = BybitNewOrderRequest::from_bytes(
            close_order
                .get_order_request_bytes()
                .expect("bybit margin close request should build")
                .as_ref(),
        )
        .expect("bybit margin close request should parse");

        assert!(open_request.params_struct().unwrap().is_leverage);
        assert!(!close_request.params_struct().unwrap().is_leverage);
    }

    #[test]
    fn create_open_order_request_bytes_sets_meta_and_builds_request() {
        let mut mgr = OrderManager::new(None);
        let (exchange, req_bin) = mgr
            .create_open_order_request_bytes(
                TradingVenue::BybitMargin,
                43,
                OrderType::Market,
                "CCUSDT".to_string(),
                Side::Buy,
                10.0,
                0.0,
                Some(OrderQuantizedValue::new(1, 0, 10)),
                None,
                false,
                1.0,
                123456,
                7,
                654321,
                0,
                0,
            )
            .expect("bybit margin open request should build");

        assert_eq!(exchange, "bybit");
        let order = mgr.get(43).expect("order should be inserted");
        assert_eq!(order.timestamp.signal_t, 123456);
        assert_eq!(order.timestamp.signal_kind, 7);
        assert_eq!(order.timestamp.mkt_t, 654321);

        let request = BybitNewOrderRequest::from_bytes(req_bin.as_ref())
            .expect("bybit margin open request should parse");
        let params = request.params_struct().unwrap();
        assert_eq!(params.symbol, "CCUSDT");
        assert_eq!(params.side, Side::Buy);
        assert!(params.is_leverage);
    }

    #[test]
    fn binance_margin_open_uses_normalized_symbol_for_asset_split() {
        let mut mgr = OrderManager::new(Some(BinanceAccountMode::Unified));
        mgr.create_order(
            TradingVenue::BinanceMargin,
            45,
            OrderType::Limit,
            "BTC-USDT".to_string(),
            Side::Buy,
            0.01,
            50000.0,
            false,
            1.0,
        );

        let order = mgr.get(45).expect("order should be inserted");
        assert_eq!(order.symbol, "BTCUSDT");
        let (base_asset, quote_asset) = extract_assets_from_internal_symbol(&order.symbol);
        assert_eq!(base_asset, "BTC");
        assert_eq!(quote_asset, "USDT");
    }

    #[test]
    fn bybit_request_prefers_cached_quantized_values() {
        let mut order = Order::new(
            TradingVenue::BybitFutures,
            44,
            OrderType::Limit,
            "BTCUSDT".to_string(),
            Side::Sell,
            0.30000000000000004,
            123.45000000000002,
            false,
            1.0,
            None,
            false,
        );
        order.set_quantity_qv(OrderQuantizedValue::new(1, -3, 300));
        order.set_price_qv(OrderQuantizedValue::new(1, -2, 12345));

        let request = BybitNewOrderRequest::from_bytes(
            order
                .get_order_request_bytes()
                .expect("bybit futures request should build")
                .as_ref(),
        )
        .expect("bybit futures request should parse");
        let params = request.params_struct().unwrap();

        assert_eq!(params.quantity_qv.decimal_string(), "0.300");
        assert_eq!(params.price_qv.decimal_string(), "123.45");
    }

    #[test]
    fn pending_limit_counts_are_cached_by_symbol_and_side() {
        let mut mgr = OrderManager::new(None);
        mgr.create_order(
            TradingVenue::OkexMargin,
            1,
            OrderType::Limit,
            "FIL-USDT".to_string(),
            Side::Buy,
            1.0,
            100.0,
            false,
            1.0,
        );
        mgr.create_order(
            TradingVenue::OkexMargin,
            2,
            OrderType::Limit,
            "FILUSDT".to_string(),
            Side::Sell,
            1.0,
            101.0,
            false,
            1.0,
        );
        mgr.create_order(
            TradingVenue::OkexMargin,
            3,
            OrderType::Market,
            "FILUSDT".to_string(),
            Side::Buy,
            1.0,
            0.0,
            false,
            1.0,
        );
        mgr.create_order_with_pending_limit_flag(
            TradingVenue::OkexMargin,
            4,
            OrderType::Limit,
            "FILUSDT".to_string(),
            Side::Buy,
            1.0,
            99.0,
            false,
            1.0,
            false,
        );

        assert_eq!(mgr.get_symbol_pending_limit_order_count("FILUSDT"), 2);
        assert_eq!(
            mgr.get_symbol_pending_limit_order_count_by_side("FILUSDT", Side::Buy),
            1
        );
        assert_eq!(
            mgr.get_symbol_pending_limit_order_count_by_side("FILUSDT", Side::Sell),
            1
        );

        mgr.remove(1);
        assert_eq!(mgr.get_symbol_pending_limit_order_count("FILUSDT"), 1);
        assert_eq!(
            mgr.get_symbol_pending_limit_order_count_by_side("FILUSDT", Side::Buy),
            0
        );
        assert_eq!(
            mgr.get_symbol_pending_limit_order_count_by_side("FILUSDT", Side::Sell),
            1
        );

        let replacement = Order::new(
            TradingVenue::OkexMargin,
            2,
            OrderType::Limit,
            "FILUSDT".to_string(),
            Side::Buy,
            1.0,
            102.0,
            false,
            1.0,
            None,
            true,
        );
        mgr.insert(replacement);
        assert_eq!(mgr.get_symbol_pending_limit_order_count("FILUSDT"), 1);
        assert_eq!(
            mgr.get_symbol_pending_limit_order_count_by_side("FILUSDT", Side::Buy),
            1
        );
        assert_eq!(
            mgr.get_symbol_pending_limit_order_count_by_side("FILUSDT", Side::Sell),
            0
        );

        mgr.update(2, |order| order.status = OrderExecutionStatus::Cancelled);
        assert_eq!(mgr.get_symbol_pending_limit_order_count("FILUSDT"), 0);
        assert_eq!(
            mgr.get_symbol_pending_limit_order_count_by_side("FILUSDT", Side::Buy),
            0
        );

        mgr.clear();
        assert_eq!(mgr.get_symbol_pending_limit_order_count("FILUSDT"), 0);
        assert_eq!(
            mgr.get_symbol_pending_limit_order_count_by_side("FILUSDT", Side::Buy),
            0
        );
    }

    #[test]
    fn duplicate_partial_trade_is_skipped_by_cumulative_qty_even_with_newer_ts() {
        let mut order = Order::new(
            TradingVenue::BybitFutures,
            42,
            OrderType::Limit,
            "ETHUSDT".to_string(),
            Side::Buy,
            0.04,
            2300.0,
            false,
            1.0,
            None,
            true,
        );
        order.status = OrderExecutionStatus::Create;
        order.cumulative_filled_quantity = 0.04;

        let skip = OrderManager::should_skip_idempotent_trade_update(
            &order,
            OrderStatus::PartiallyFilled,
            0.04,
            9_999,
            "test",
            1,
        );

        assert_eq!(skip, Some(TradeUpdateSkipReason::StaleOrDuplicatePartial));
    }

    #[test]
    fn terminal_filled_with_same_cumulative_qty_is_not_skipped() {
        let mut order = Order::new(
            TradingVenue::GateFutures,
            42,
            OrderType::Limit,
            "SOLUSDT".to_string(),
            Side::Sell,
            1.0,
            86.05,
            false,
            1.0,
            None,
            true,
        );
        order.status = OrderExecutionStatus::Create;
        order.cumulative_filled_quantity = 1.0;

        let skip = OrderManager::should_skip_idempotent_trade_update(
            &order,
            OrderStatus::Filled,
            1.0,
            9_999,
            "test",
            1,
        );

        assert_eq!(skip, None);
    }

    #[test]
    fn order_protected_cumulative_fill_keeps_local_value_on_rollback() {
        let mut order = Order::new(
            TradingVenue::GateFutures,
            42,
            OrderType::Limit,
            "SOLUSDT".to_string(),
            Side::Sell,
            1.0,
            86.05,
            false,
            1.0,
            None,
            true,
        );
        order.cumulative_filled_quantity = 4.2;

        let protected = order.protected_cumulative_fill(0.0);

        assert!(protected.rollback_detected);
        assert!((protected.effective_cum - 4.2).abs() < 1e-12);
    }

    #[test]
    fn order_protected_cumulative_fill_accepts_forward_progress() {
        let mut order = Order::new(
            TradingVenue::GateFutures,
            42,
            OrderType::Limit,
            "SOLUSDT".to_string(),
            Side::Sell,
            1.0,
            86.05,
            false,
            1.0,
            None,
            true,
        );
        order.cumulative_filled_quantity = 4.2;

        let protected = order.protected_cumulative_fill(5.6);

        assert!(!protected.rollback_detected);
        assert!((protected.effective_cum - 5.6).abs() < 1e-12);
    }

    #[test]
    fn gate_futures_sell_order_serializes_negative_size() {
        let order = Order::new(
            TradingVenue::GateFutures,
            42,
            OrderType::Limit,
            "SOLUSDT".to_string(),
            Side::Sell,
            3.0,
            88.56,
            true,
            1.0,
            None,
            true,
        );

        let bytes = order
            .get_order_request_bytes()
            .expect("gate futures request should build");
        let payload = extract_request_json(bytes.as_ref());

        assert_eq!(
            payload.get("contract").and_then(Value::as_str),
            Some("SOL_USDT")
        );
        assert_eq!(payload.get("size").and_then(Value::as_str), Some("-3"));
        assert_eq!(payload.get("price").and_then(Value::as_str), Some("88.56"));
        assert_eq!(payload.get("tif").and_then(Value::as_str), Some("poc"));
        assert_eq!(
            payload.get("reduce_only").and_then(Value::as_bool),
            Some(true)
        );
    }

    #[test]
    fn gate_futures_request_prefers_cached_quantized_values() {
        let mut order = Order::new(
            TradingVenue::GateFutures,
            45,
            OrderType::Limit,
            "SOLUSDT".to_string(),
            Side::Sell,
            3.0000000000000004,
            88.56000000000002,
            false,
            1.0,
            None,
            true,
        );
        order.set_quantity_qv(OrderQuantizedValue::new(1, -2, 300));
        order.set_price_qv(OrderQuantizedValue::new(1, -3, 88560));

        let bytes = order
            .get_order_request_bytes()
            .expect("gate futures request should build");
        let payload = extract_request_json(bytes.as_ref());

        assert_eq!(payload.get("size").and_then(Value::as_str), Some("-3.00"));
        assert_eq!(payload.get("price").and_then(Value::as_str), Some("88.560"));
    }

    #[test]
    fn gate_margin_buy_order_serializes_auto_borrow_and_repay() {
        let order = Order::new(
            TradingVenue::GateMargin,
            43,
            OrderType::Limit,
            "CCUSDT".to_string(),
            Side::Buy,
            7.0,
            0.15309,
            false,
            1.0,
            None,
            true,
        );

        let bytes = order
            .get_order_request_bytes()
            .expect("gate margin buy request should build");
        let payload = extract_request_json(bytes.as_ref());

        assert_eq!(
            payload.get("currency_pair").and_then(Value::as_str),
            Some("CC_USDT")
        );
        assert_eq!(payload.get("side").and_then(Value::as_str), Some("buy"));
        assert_eq!(
            payload.get("auto_borrow").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            payload.get("auto_repay").and_then(Value::as_bool),
            Some(true)
        );
    }

    #[test]
    fn gate_margin_sell_order_omits_auto_borrow_and_repay() {
        let order = Order::new(
            TradingVenue::GateMargin,
            44,
            OrderType::Limit,
            "CCUSDT".to_string(),
            Side::Sell,
            7.0,
            0.15309,
            false,
            1.0,
            None,
            true,
        );

        let bytes = order
            .get_order_request_bytes()
            .expect("gate margin sell request should build");
        let payload = extract_request_json(bytes.as_ref());

        assert_eq!(
            payload.get("currency_pair").and_then(Value::as_str),
            Some("CC_USDT")
        );
        assert_eq!(payload.get("side").and_then(Value::as_str), Some("sell"));
        assert!(payload.get("auto_borrow").is_none());
        assert!(payload.get("auto_repay").is_none());
    }

    #[test]
    fn bitget_futures_sell_order_serializes_one_way_fields() {
        let order = Order::new(
            TradingVenue::BitgetFutures,
            77,
            OrderType::Limit,
            "ETHUSDT".to_string(),
            Side::Sell,
            0.21,
            2363.73,
            false,
            1.0,
            None,
            true,
        );

        let bytes = order
            .get_order_request_bytes()
            .expect("bitget futures request should build");
        let ws_payload = extract_bitget_ws_payload_json(bytes.as_ref());
        let payload = &ws_payload["args"][0];

        assert_eq!(
            ws_payload.get("category").and_then(Value::as_str),
            Some("usdt-futures")
        );
        assert_eq!(
            payload.get("symbol").and_then(Value::as_str),
            Some("ETHUSDT")
        );
        assert_eq!(payload.get("side").and_then(Value::as_str), Some("sell"));
        assert_eq!(
            payload.get("orderType").and_then(Value::as_str),
            Some("limit")
        );
        assert_eq!(
            payload.get("timeInForce").and_then(Value::as_str),
            Some("post_only")
        );
        assert_eq!(
            payload.get("price").and_then(Value::as_str),
            Some("2363.73")
        );
        assert_eq!(payload.get("qty").and_then(Value::as_str), Some("0.21"));
        assert_eq!(payload.get("clientOid").and_then(Value::as_str), Some("77"));
        assert!(payload.get("reduceOnly").is_none());
        assert!(payload.get("posSide").is_none());
    }

    #[test]
    fn bitget_futures_reduce_only_order_serializes_reduce_only_flag() {
        let order = Order::new(
            TradingVenue::BitgetFutures,
            78,
            OrderType::Market,
            "ETHUSDT".to_string(),
            Side::Buy,
            0.21,
            0.0,
            true,
            1.0,
            None,
            true,
        );

        let bytes = order
            .get_order_request_bytes()
            .expect("bitget futures request should build");
        let ws_payload = extract_bitget_ws_payload_json(bytes.as_ref());
        let payload = &ws_payload["args"][0];

        assert_eq!(
            ws_payload.get("category").and_then(Value::as_str),
            Some("usdt-futures")
        );
        assert_eq!(
            payload.get("reduceOnly").and_then(Value::as_str),
            Some("YES")
        );
        assert!(payload.get("posSide").is_none());
    }

    #[test]
    fn gate_futures_cancel_uses_typed_params_and_renders_contract() {
        let order = Order::new(
            TradingVenue::GateFutures,
            79,
            OrderType::Limit,
            "SOLUSDT".to_string(),
            Side::Sell,
            1.0,
            88.0,
            true,
            1.0,
            None,
            true,
        );

        let bytes = order
            .get_order_cancel_bytes()
            .expect("gate futures cancel should build");
        let payload = extract_request_json(bytes.as_ref());

        assert_eq!(
            payload.get("contract").and_then(Value::as_str),
            Some("SOL_USDT")
        );
        assert_eq!(
            payload.get("order_id").and_then(Value::as_str),
            Some("t-79")
        );
    }

    #[test]
    fn bitget_futures_cancel_uses_typed_params() {
        let order = Order::new(
            TradingVenue::BitgetFutures,
            80,
            OrderType::Limit,
            "ETHUSDT".to_string(),
            Side::Buy,
            1.0,
            100.0,
            true,
            1.0,
            None,
            true,
        );

        let bytes = order
            .get_order_cancel_bytes()
            .expect("bitget futures cancel should build");
        let ws_payload = extract_bitget_ws_payload_json(bytes.as_ref());
        let payload = &ws_payload["args"][0];

        assert_eq!(
            ws_payload.get("category").and_then(Value::as_str),
            Some("usdt-futures")
        );
        assert_eq!(payload.get("clientOid").and_then(Value::as_str), Some("80"));
        assert!(payload.get("orderId").is_none());
    }
}
