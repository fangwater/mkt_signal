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
    extract_assets_from_symbol, normalize_symbol_for_internal, okex_inst_id_from_symbol,
};
use trade_engine::bybit::{
    BybitCancelOrderParams, BybitCancelOrderRequest, BybitNewOrderParams, BybitNewOrderRequest,
};
use trade_engine::okex::{
    OkexCancelOrderParams, OkexCancelOrderRequest, OkexNewOrderParams, OkexNewOrderRequest,
    OkexOrderType,
};
use trade_engine::trade_request::BinanceNewMarginOrderRequest;
use trade_engine::trade_request::BinanceNewUMOrderRequest;
use trade_engine::trade_request::{
    BinanceCancelMarginOrderRequest, BinanceCancelOrderParams, BinanceCancelUMOrderRequest,
    BinanceNewOrderParams, BinanceWsCancelMarginOrderRequest, BinanceWsCancelUMOrderRequest,
    BinanceWsNewMarginOrderRequest, BinanceWsNewUMOrderRequest, BitgetCancelOrderParams,
    BitgetMarginCancelOrderRequest, BitgetMarginNewOrderRequest, BitgetNewOrderParams,
    BitgetUmCancelOrderRequest, BitgetUmNewOrderRequest, GateCancelOrderParams,
    GateFuturesCancelOrderRequest, GateFuturesNewOrderRequest, GateNewOrderParams,
    GateUnifiedCancelOrderRequest, GateUnifiedNewOrderRequest,
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
    ) -> Result<(&'static str, Bytes), String>;
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

        let params = BinanceCancelOrderParams {
            symbol: symbol.to_string(),
            orig_client_order_id: client_order_id,
        };
        match venue {
            TradingVenue::BinanceMargin => {
                if self.binance_is_standard() {
                    let request = BinanceWsCancelMarginOrderRequest::create_typed(
                        get_timestamp_us(),
                        client_order_id,
                        params,
                    )
                    .ok_or_else(|| "failed to build binance ws margin cancel params".to_string())?;
                    Ok(request.to_bytes())
                } else {
                    let request = BinanceCancelMarginOrderRequest::create_typed(
                        get_timestamp_us(),
                        client_order_id,
                        params,
                    )
                    .ok_or_else(|| "failed to build binance margin cancel params".to_string())?;
                    Ok(request.to_bytes())
                }
            }
            TradingVenue::BinanceFutures => {
                if self.binance_is_standard() {
                    let request = BinanceWsCancelUMOrderRequest::create_typed(
                        get_timestamp_us(),
                        client_order_id,
                        params,
                    )
                    .ok_or_else(|| "failed to build binance ws um cancel params".to_string())?;
                    Ok(request.to_bytes())
                } else {
                    let request = BinanceCancelUMOrderRequest::create_typed(
                        get_timestamp_us(),
                        client_order_id,
                        params,
                    )
                    .ok_or_else(|| "failed to build binance um cancel params".to_string())?;
                    Ok(request.to_bytes())
                }
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
    ) -> Result<(&'static str, Bytes), String> {
        let Some(result) = self.create_order_with_mut(
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
}

pub trait PreTradeOrderRequestExt {
    fn get_order_cancel_bytes(&self) -> Result<Bytes, String>;
    fn get_order_request_bytes(&self) -> Result<Bytes, String>;
}

impl PreTradeOrderRequestExt for Order {
    fn get_order_cancel_bytes(&self) -> Result<Bytes, String> {
        let now = get_timestamp_us();
        match self.venue {
            TradingVenue::BinanceMargin => {
                // 使用 origClientOrderId 以客户端订单ID撤单；当前未保存交易所 orderId
                let params = BinanceCancelOrderParams {
                    symbol: self.symbol.clone(),
                    orig_client_order_id: self.client_order_id,
                };
                if self.require_binance_account_mode() == BinanceAccountMode::Standard {
                    let request: BinanceWsCancelMarginOrderRequest =
                        BinanceWsCancelMarginOrderRequest::create_typed(
                            now,
                            self.client_order_id,
                            params,
                        )
                        .ok_or_else(|| {
                            "failed to build binance ws margin cancel params".to_string()
                        })?;
                    return Ok(request.to_bytes());
                }
                let request: BinanceCancelMarginOrderRequest =
                    BinanceCancelMarginOrderRequest::create_typed(
                        now,
                        self.client_order_id,
                        params,
                    )
                    .ok_or_else(|| "failed to build binance margin cancel params".to_string())?;
                Ok(request.to_bytes())
            }
            TradingVenue::BinanceFutures => {
                let params = BinanceCancelOrderParams {
                    symbol: self.symbol.clone(),
                    orig_client_order_id: self.client_order_id,
                };
                if self.require_binance_account_mode() == BinanceAccountMode::Standard {
                    let request: BinanceWsCancelUMOrderRequest =
                        BinanceWsCancelUMOrderRequest::create_typed(
                            now,
                            self.client_order_id,
                            params,
                        )
                        .ok_or_else(|| "failed to build binance ws um cancel params".to_string())?;
                    return Ok(request.to_bytes());
                }
                let request: BinanceCancelUMOrderRequest =
                    BinanceCancelUMOrderRequest::create_typed(now, self.client_order_id, params)
                        .ok_or_else(|| "failed to build binance um cancel params".to_string())?;
                Ok(request.to_bytes())
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
                let params = GateCancelOrderParams {
                    symbol: currency_pair,
                    order_id,
                };
                let request =
                    GateUnifiedCancelOrderRequest::create_typed(now, self.client_order_id, params)
                        .ok_or_else(|| "failed to build gate unified cancel params".to_string())?;
                Ok(request.to_bytes())
            }
            TradingVenue::GateFutures => {
                let contract = gate_currency_pair_from_symbol(&self.symbol);
                let order_id = self
                    .exchange_order_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| gate_text_from_client_order_id(self.client_order_id));
                let params = GateCancelOrderParams {
                    symbol: contract,
                    order_id,
                };
                let request =
                    GateFuturesCancelOrderRequest::create_typed(now, self.client_order_id, params)
                        .ok_or_else(|| "failed to build gate futures cancel params".to_string())?;
                Ok(request.to_bytes())
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
                let params = BitgetCancelOrderParams {
                    order_id: self
                        .exchange_order_id
                        .filter(|&id| id > 0)
                        .map(|id| id.to_string()),
                    client_order_id: self.client_order_id.to_string(),
                };
                match self.venue {
                    TradingVenue::BitgetMargin => {
                        let request = BitgetMarginCancelOrderRequest::create_typed(
                            now,
                            self.client_order_id,
                            params,
                        )
                        .ok_or_else(|| "failed to build bitget margin cancel params".to_string())?;
                        Ok(request.to_bytes())
                    }
                    TradingVenue::BitgetFutures => {
                        let request = BitgetUmCancelOrderRequest::create_typed(
                            now,
                            self.client_order_id,
                            params,
                        )
                        .ok_or_else(|| "failed to build bitget um cancel params".to_string())?;
                        Ok(request.to_bytes())
                    }
                    _ => unreachable!(),
                }
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
                let (base_asset, quote_asset) = extract_assets_from_symbol(&self.symbol);

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
                    if self.reduce_only {
                        return Err(format!(
                            "reduce-only BinanceMargin order has insufficient balance: asset={} required={:.8} available={:.8} borrow={:.8} symbol={} side={:?} qty={} price={}",
                            check_asset,
                            required_amount,
                            available_balance,
                            borrow_amount,
                            self.symbol,
                            self.side,
                            resolved.quantity_text(),
                            resolved.price_text()
                        ));
                    }
                    if !(use_binance_ws_margin && self.side == Side::Sell) {
                        warn!(
                            "💰 余额不足将借币: 资产={} 需要={:.8} 可用={:.8} 需借={:.8} symbol={} side={:?} qty={} price={}",
                            check_asset, required_amount, available_balance, borrow_amount,
                            self.symbol,
                            self.side,
                            resolved.quantity_text(),
                            resolved.price_text()
                        );
                    }
                    if use_binance_ws_margin {
                        info!(
                            "BinanceMargin STANDARD mode: omit sideEffectType for symbol={} side={:?}",
                            self.symbol, self.side
                        );
                    }
                } else {
                    info!(
                        "✅ 余额充足: 资产={} 需要={:.8} 可用={:.8} symbol={} side={:?}",
                        check_asset, required_amount, available_balance, self.symbol, self.side
                    );
                    // 本地余额充足只代表净钱包口径充足；PM REST 仍可能需要自动借币。
                }
                let margin_buy =
                    binance_margin_should_use_margin_buy(use_binance_ws_margin, self.reduce_only);
                // ===== 余额检查结束 =====/

                let params = BinanceNewOrderParams {
                    symbol: self.symbol.clone(),
                    side: self.side,
                    order_type: self.order_type,
                    quantity_qv: resolved.require_quantity_qv(self, "binance")?,
                    price_qv: resolved.limit_price_qv_or_zero(self, "binance")?,
                    reduce_only: self.reduce_only,
                    margin_buy,
                    ws_response_full: use_binance_ws_margin,
                    ws_um_response_result: false,
                    ws_margin_limit_maker: use_binance_ws_margin,
                };
                info!(
                    "OrderManager: venue={:?} client_order_id={} symbol={} side={:?} type={:?} reduce_only={} typed_params=binance_new_order",
                    self.venue,
                    self.client_order_id,
                    self.symbol,
                    self.side,
                    self.order_type,
                    self.reduce_only
                );
                if use_binance_ws_margin {
                    let request = BinanceWsNewMarginOrderRequest::create_typed(
                        local_create_ts,
                        self.client_order_id,
                        params,
                    )
                    .ok_or_else(|| "failed to build binance ws margin order params".to_string())?;
                    Ok(request.to_bytes())
                } else {
                    let request = BinanceNewMarginOrderRequest::create_typed(
                        local_create_ts,
                        self.client_order_id,
                        params,
                    )
                    .ok_or_else(|| "failed to build binance margin order params".to_string())?;
                    Ok(request.to_bytes())
                }
            }
            TradingVenue::BinanceFutures => {
                let use_binance_ws_um =
                    self.require_binance_account_mode() == BinanceAccountMode::Standard;
                let local_create_ts = get_timestamp_us();
                let params = BinanceNewOrderParams {
                    symbol: self.symbol.clone(),
                    side: self.side,
                    order_type: self.order_type,
                    quantity_qv: resolved.require_quantity_qv(self, "binance")?,
                    price_qv: resolved.limit_price_qv_or_zero(self, "binance")?,
                    reduce_only: self.reduce_only,
                    margin_buy: false,
                    ws_response_full: false,
                    ws_um_response_result: use_binance_ws_um,
                    ws_margin_limit_maker: false,
                };
                info!(
                    "OrderManager: venue={:?} client_order_id={} symbol={} side={:?} type={:?} reduce_only={} typed_params=binance_new_order",
                    self.venue,
                    self.client_order_id,
                    self.symbol,
                    self.side,
                    self.order_type,
                    self.reduce_only
                );
                if use_binance_ws_um {
                    let request = BinanceWsNewUMOrderRequest::create_typed(
                        local_create_ts,
                        self.client_order_id,
                        params,
                    )
                    .ok_or_else(|| "failed to build binance ws um order params".to_string())?;
                    Ok(request.to_bytes())
                } else {
                    let request = BinanceNewUMOrderRequest::create_typed(
                        local_create_ts,
                        self.client_order_id,
                        params,
                    )
                    .ok_or_else(|| "failed to build binance um order params".to_string())?;
                    Ok(request.to_bytes())
                }
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
                let params = GateNewOrderParams {
                    symbol: currency_pair,
                    side: self.side,
                    order_type: self.order_type,
                    quantity_qv: resolved.require_quantity_qv(self, "gate")?,
                    price_qv: resolved.limit_price_qv_or_zero(self, "gate")?,
                    reduce_only: self.reduce_only,
                    // 仅 unified/cross-margin 买入单可能触发本单借款；成交所得用于偿还本单借入。
                    auto_borrow_repay: self.side == Side::Buy,
                };
                let request = GateUnifiedNewOrderRequest::create_typed(
                    create_ts,
                    self.client_order_id,
                    params,
                )
                .ok_or_else(|| "failed to build gate unified order params".to_string())?;
                Ok(request.to_bytes())
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
                let params = GateNewOrderParams {
                    symbol: contract,
                    side: self.side,
                    order_type: self.order_type,
                    quantity_qv: resolved.require_quantity_qv(self, "gate")?,
                    price_qv: resolved.limit_price_qv_or_zero(self, "gate")?,
                    reduce_only: self.reduce_only,
                    auto_borrow_repay: false,
                };
                let request = GateFuturesNewOrderRequest::create_typed(
                    create_ts,
                    self.client_order_id,
                    params,
                )
                .ok_or_else(|| "failed to build gate futures order params".to_string())?;
                Ok(request.to_bytes())
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
                let params = BitgetNewOrderParams {
                    symbol: self.symbol.clone(),
                    side: self.side,
                    order_type: self.order_type,
                    quantity_qv: resolved.require_quantity_qv(self, "bitget")?,
                    price_qv: resolved.limit_price_qv_or_zero(self, "bitget")?,
                    reduce_only: self.reduce_only,
                };
                let request = BitgetMarginNewOrderRequest::create_typed(
                    create_ts,
                    self.client_order_id,
                    params,
                )
                .ok_or_else(|| "failed to build bitget margin order params".to_string())?;
                Ok(request.to_bytes())
            }
            TradingVenue::BitgetFutures => {
                let create_ts = get_timestamp_us();
                // trade_engine precheck 强制 Bitget UTA futures 为 one_way_mode。
                // one-way 模式下开/平仓由 side + reduceOnly 表达，不传 hedge-mode 的 posSide。
                let params = BitgetNewOrderParams {
                    symbol: self.symbol.clone(),
                    side: self.side,
                    order_type: self.order_type,
                    quantity_qv: resolved.require_quantity_qv(self, "bitget")?,
                    price_qv: resolved.limit_price_qv_or_zero(self, "bitget")?,
                    reduce_only: self.reduce_only,
                };
                let request =
                    BitgetUmNewOrderRequest::create_typed(create_ts, self.client_order_id, params)
                        .ok_or_else(|| "failed to build bitget um order params".to_string())?;
                Ok(request.to_bytes())
            }
            //之后在这支持别的类型下单，根据资产类型决定下单的request，统一序列化为bytes
            _ => Err(format!("Unsupported trading venue: {:?}", self.venue)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        binance_margin_should_use_margin_buy, bybit_margin_should_use_leverage,
        BybitNewOrderRequest, Order, OrderExecutionStatus, OrderManager, OrderQuantizedValue,
        OrderStatus, OrderType, PreTradeOrderManagerRequestExt, PreTradeOrderRequestExt, Side,
        TradeUpdateSkipReason,
    };
    use order_common::TradingVenue;
    use serde_json::Value;
    use trade_engine::trade_request::{
        BitgetCancelOrderParams, BitgetNewOrderParams, GateFuturesCancelOrderRequest,
        GateFuturesNewOrderRequest, GateUnifiedCancelOrderRequest, GateUnifiedNewOrderRequest,
        TradeRequestHeader, TradeRequestMsg, TradeRequestType,
    };

    fn extract_request_json(bytes: &[u8]) -> Value {
        let msg = TradeRequestMsg::parse(bytes).expect("trade request should parse");
        let header = TradeRequestHeader {
            msg_type: msg.req_type as u32,
            params_length: msg.params.len() as u32,
            create_time: msg.create_time,
            client_order_id: msg.client_order_id,
        };
        match msg.req_type {
            TradeRequestType::GateUnifiedNewOrder => GateUnifiedNewOrderRequest {
                header,
                params: msg.params,
            }
            .params_struct()
            .map(|params| params.to_gate_unified_json(msg.client_order_id))
            .expect("gate unified typed params should parse"),
            TradeRequestType::GateFuturesNewOrder => GateFuturesNewOrderRequest {
                header,
                params: msg.params,
            }
            .params_struct()
            .map(|params| params.to_gate_futures_json(msg.client_order_id))
            .expect("gate futures typed params should parse"),
            TradeRequestType::GateUnifiedCancelOrder => GateUnifiedCancelOrderRequest {
                header,
                params: msg.params,
            }
            .params_struct()
            .map(|params| params.to_gate_unified_json())
            .expect("gate unified cancel typed params should parse"),
            TradeRequestType::GateFuturesCancelOrder => GateFuturesCancelOrderRequest {
                header,
                params: msg.params,
            }
            .params_struct()
            .map(|params| params.to_gate_futures_json())
            .expect("gate futures cancel typed params should parse"),
            TradeRequestType::BitgetNewMarginOrder | TradeRequestType::BitgetNewUMOrder => {
                BitgetNewOrderParams::from_bytes(&msg.params)
                    .map(|params| params.to_bitget_ws_arg(msg.req_type, msg.client_order_id))
                    .expect("bitget typed params should parse")
            }
            TradeRequestType::BitgetCancelMarginOrder | TradeRequestType::BitgetCancelUMOrder => {
                BitgetCancelOrderParams::from_bytes(&msg.params)
                    .map(|params| params.to_bitget_ws_arg(msg.req_type))
                    .expect("bitget cancel typed params should parse")
            }
            _ => serde_json::from_slice(&msg.params).expect("request params should be valid json"),
        }
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
        let payload = extract_request_json(bytes.as_ref());

        assert_eq!(
            payload.get("category").and_then(Value::as_str),
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
        let payload = extract_request_json(bytes.as_ref());

        assert_eq!(
            payload.get("category").and_then(Value::as_str),
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
        let payload = extract_request_json(bytes.as_ref());

        assert_eq!(
            payload.get("category").and_then(Value::as_str),
            Some("usdt-futures")
        );
        assert_eq!(payload.get("clientOid").and_then(Value::as_str), Some("80"));
        assert!(payload.get("orderId").is_none());
    }
}
