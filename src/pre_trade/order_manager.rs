use bytes::Bytes;
use log::{info, warn};
pub use order_common::{
    gate_text_from_client_order_id, BinanceAccountMode, Order, OrderExecutionStatus, OrderManager,
    OrderStatus, OrderType, OrderUpdateSkipReason, ProtectedCumulativeFill, Side,
    TradeUpdateSkipReason, TradingVenue, CUMULATIVE_FILL_ROLLBACK_EPS,
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
    BinanceCancelMarginOrderRequest, BinanceCancelUMOrderRequest,
    BinanceWsCancelMarginOrderRequest, BinanceWsCancelUMOrderRequest,
    BinanceWsNewMarginOrderRequest, BinanceWsNewUMOrderRequest, BitgetMarginCancelOrderRequest,
    BitgetMarginNewOrderRequest, BitgetUmCancelOrderRequest, BitgetUmNewOrderRequest,
    GateFuturesCancelOrderRequest, GateFuturesNewOrderRequest, GateUnifiedCancelOrderRequest,
    GateUnifiedNewOrderRequest,
};
fn format_decimal(value: f64) -> String {
    QuantizedValue::from_decimal(value)
        .map(|qv| qv.decimal_string())
        .unwrap_or_else(|| "0".to_string())
}

fn format_quantity(quantity: f64) -> String {
    format_decimal(quantity)
}

fn format_signed_quantity(quantity: f64) -> String {
    if quantity < 0.0 {
        let abs = format_decimal(-quantity);
        if abs == "0" {
            abs
        } else {
            format!("-{abs}")
        }
    } else {
        format_decimal(quantity)
    }
}

fn format_price(price: f64) -> String {
    format_decimal(price)
}

fn format_order_quantity(quantity: f64) -> String {
    QuantizedValue::from_decimal(quantity)
        .map(|qv| qv.decimal_string())
        .unwrap_or_else(|| format_quantity(quantity))
}

fn format_order_price(price: f64) -> String {
    QuantizedValue::from_decimal(price)
        .map(|qv| qv.decimal_string())
        .unwrap_or_else(|| format_price(price))
}

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

fn binance_ws_um_new_order_resp_type() -> &'static str {
    "RESULT"
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

use serde_json::{json, Value};

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

        let params = Bytes::from(format!(
            "symbol={}&origClientOrderId={}",
            symbol, client_order_id
        ));
        match venue {
            TradingVenue::BinanceMargin => {
                if self.binance_is_standard() {
                    let request = BinanceWsCancelMarginOrderRequest::create(
                        get_timestamp_us(),
                        client_order_id,
                        params,
                    );
                    Ok(request.to_bytes())
                } else {
                    let request = BinanceCancelMarginOrderRequest::create(
                        get_timestamp_us(),
                        client_order_id,
                        params,
                    );
                    Ok(request.to_bytes())
                }
            }
            TradingVenue::BinanceFutures => {
                if self.binance_is_standard() {
                    let request = BinanceWsCancelUMOrderRequest::create(
                        get_timestamp_us(),
                        client_order_id,
                        params,
                    );
                    Ok(request.to_bytes())
                } else {
                    let request = BinanceCancelUMOrderRequest::create(
                        get_timestamp_us(),
                        client_order_id,
                        params,
                    );
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
                let params = Bytes::from(format!(
                    "symbol={}&origClientOrderId={}",
                    self.symbol, self.client_order_id
                ));
                if self.require_binance_account_mode() == BinanceAccountMode::Standard {
                    let request: BinanceWsCancelMarginOrderRequest =
                        BinanceWsCancelMarginOrderRequest::create(
                            now,
                            self.client_order_id,
                            params,
                        );
                    return Ok(request.to_bytes());
                }
                let request: BinanceCancelMarginOrderRequest =
                    BinanceCancelMarginOrderRequest::create(now, self.client_order_id, params);
                Ok(request.to_bytes())
            }
            TradingVenue::BinanceFutures => {
                let params = Bytes::from(format!(
                    "symbol={}&origClientOrderId={}",
                    self.symbol, self.client_order_id
                ));
                if self.require_binance_account_mode() == BinanceAccountMode::Standard {
                    let request: BinanceWsCancelUMOrderRequest =
                        BinanceWsCancelUMOrderRequest::create(now, self.client_order_id, params);
                    return Ok(request.to_bytes());
                }
                let request: BinanceCancelUMOrderRequest =
                    BinanceCancelUMOrderRequest::create(now, self.client_order_id, params);
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
                let req_param = json!({
                    "order_id": order_id,
                    "currency_pair": currency_pair,
                    "account": "unified",
                });
                let params = Bytes::from(req_param.to_string());
                let request =
                    GateUnifiedCancelOrderRequest::create(now, self.client_order_id, params);
                Ok(request.to_bytes())
            }
            TradingVenue::GateFutures => {
                let contract = gate_currency_pair_from_symbol(&self.symbol);
                let order_id = self
                    .exchange_order_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| gate_text_from_client_order_id(self.client_order_id));
                let req_param = json!({
                    "order_id": order_id,
                    "contract": contract,
                });
                let params = Bytes::from(req_param.to_string());
                let request =
                    GateFuturesCancelOrderRequest::create(now, self.client_order_id, params);
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
                let mut req_param = serde_json::Map::new();
                if let Some(order_id) = self.exchange_order_id.filter(|&id| id > 0) {
                    req_param.insert("orderId".to_string(), json!(order_id.to_string()));
                }
                req_param.insert(
                    "clientOid".to_string(),
                    json!(self.client_order_id.to_string()),
                );
                let params = Bytes::from(Value::Object(req_param).to_string());
                match self.venue {
                    TradingVenue::BitgetMargin => {
                        let request = BitgetMarginCancelOrderRequest::create(
                            now,
                            self.client_order_id,
                            params,
                        );
                        Ok(request.to_bytes())
                    }
                    TradingVenue::BitgetFutures => {
                        let request =
                            BitgetUmCancelOrderRequest::create(now, self.client_order_id, params);
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

        match self.venue {
            //币安的杠杆账户下单
            TradingVenue::BinanceMargin => {
                let use_binance_ws_margin =
                    self.require_binance_account_mode() == BinanceAccountMode::Standard;
                let binance_margin_type = if use_binance_ws_margin && self.order_type.is_limit() {
                    "LIMIT_MAKER"
                } else {
                    self.order_type.as_str()
                };

                let mut params_parts = vec![
                    format!("symbol={}", self.symbol),
                    format!("side={}", self.side.as_str()), //下单方向确定就可以
                    format!("type={}", binance_margin_type),
                    format!("quantity={}", format_quantity(self.quantity)),
                    format!("newClientOrderId={}", self.client_order_id),
                ];
                if use_binance_ws_margin {
                    params_parts.push("newOrderRespType=FULL".to_string());
                }
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
                            format_order_quantity(self.quantity),
                            format_order_price(self.price)
                        ));
                    }
                    if !(use_binance_ws_margin && self.side == Side::Sell) {
                        warn!(
                            "💰 余额不足将借币: 资产={} 需要={:.8} 可用={:.8} 需借={:.8} symbol={} side={:?} qty={} price={}",
                            check_asset, required_amount, available_balance, borrow_amount,
                            self.symbol,
                            self.side,
                            format_order_quantity(self.quantity),
                            format_order_price(self.price)
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
                if binance_margin_should_use_margin_buy(use_binance_ws_margin, self.reduce_only) {
                    params_parts.push("sideEffectType=MARGIN_BUY".to_string());
                }
                // ===== 余额检查结束 =====/

                // WS margin: LIMIT_MAKER 作为 post-only，不传 tif。
                // REST margin: 保持原逻辑（LIMIT + GTC）。
                if self.order_type.is_limit() {
                    if !use_binance_ws_margin {
                        params_parts.push("timeInForce=GTC".to_string());
                    }
                    params_parts.push(format!("price={}", format_price(self.price)));
                }
                //如果是市价单，不需要价格和tif参数
                let params_plain = params_parts.join("&");
                info!(
                    "OrderManager: venue={:?} client_order_id={} params={}",
                    self.venue, self.client_order_id, params_plain
                );
                let params = Bytes::from(params_plain);
                if use_binance_ws_margin {
                    let request = BinanceWsNewMarginOrderRequest::create(
                        local_create_ts,
                        self.client_order_id,
                        params,
                    );
                    Ok(request.to_bytes())
                } else {
                    let request = BinanceNewMarginOrderRequest::create(
                        local_create_ts,
                        self.client_order_id,
                        params,
                    );
                    Ok(request.to_bytes())
                }
            }
            TradingVenue::BinanceFutures => {
                let use_binance_ws_um =
                    self.require_binance_account_mode() == BinanceAccountMode::Standard;
                let mut params_parts = vec![
                    format!("symbol={}", self.symbol),
                    format!("side={}", self.side.as_str()), //下单方向确定就可以
                    format!("type={}", self.order_type.as_str()),
                    format!("quantity={}", format_quantity(self.quantity)),
                    format!("reduceOnly={}", self.reduce_only),
                    format!("newClientOrderId={}", self.client_order_id),
                ];
                if use_binance_ws_um {
                    params_parts.push(format!(
                        "newOrderRespType={}",
                        binance_ws_um_new_order_resp_type()
                    ));
                }
                let local_create_ts = get_timestamp_us();
                //UM合约下单
                if self.order_type.is_limit() {
                    params_parts.push("timeInForce=GTX".to_string());
                    params_parts.push(format!("price={}", format_price(self.price)));
                }
                let params_plain = params_parts.join("&");
                info!(
                    "OrderManager: venue={:?} client_order_id={} params={}",
                    self.venue, self.client_order_id, params_plain
                );
                let params = Bytes::from(params_plain);
                if use_binance_ws_um {
                    let request = BinanceWsNewUMOrderRequest::create(
                        local_create_ts,
                        self.client_order_id,
                        params,
                    );
                    Ok(request.to_bytes())
                } else {
                    let request = BinanceNewUMOrderRequest::create(
                        local_create_ts,
                        self.client_order_id,
                        params,
                    );
                    Ok(request.to_bytes())
                }
            }
            TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
                let create_ts = get_timestamp_us();
                let inst_id = okex_inst_id_from_symbol(&self.symbol, self.venue)?;
                let okex_order_type = okex_order_type_from_order_type(self.order_type)?;
                let quantity_qv = quantize_order_decimal(self.quantity).ok_or_else(|| {
                    format!(
                        "failed to quantize okex quantity: qty={:.12} symbol={} client_order_id={}",
                        self.quantity, self.symbol, self.client_order_id
                    )
                })?;
                let price_qv = if self.order_type.is_limit() {
                    quantize_order_decimal(self.price).ok_or_else(|| {
                        format!(
                            "failed to quantize okex price: price={:.12} symbol={} client_order_id={}",
                            self.price, self.symbol, self.client_order_id
                        )
                    })?
                } else {
                    QuantizedValue::zero()
                };

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
                let order_type = match self.order_type {
                    OrderType::Limit => "limit",
                    OrderType::Market => "market",
                    _ => {
                        return Err(format!(
                            "unsupported gate order type: {:?}",
                            self.order_type
                        ));
                    }
                };
                let time_in_force = match self.order_type {
                    OrderType::Limit => Some("poc"),
                    OrderType::Market => None,
                    _ => None,
                };

                let mut req_param = serde_json::Map::new();
                req_param.insert("text".to_string(), json!(self.client_order_id.to_string()));
                req_param.insert("currency_pair".to_string(), json!(currency_pair));
                req_param.insert("type".to_string(), json!(order_type));
                req_param.insert("account".to_string(), json!("unified"));
                req_param.insert("side".to_string(), json!(self.side.as_str_lower()));
                req_param.insert("amount".to_string(), json!(format_quantity(self.quantity)));
                if self.side == Side::Buy {
                    req_param.insert("auto_borrow".to_string(), json!(true));
                    // 仅 unified/cross-margin 买入单可能触发本单借款；成交所得用于偿还本单借入。
                    req_param.insert("auto_repay".to_string(), json!(true));
                }
                if self.order_type.is_limit() {
                    req_param.insert("price".to_string(), json!(format_price(self.price)));
                }
                if let Some(tif) = time_in_force {
                    req_param.insert("time_in_force".to_string(), json!(tif));
                }

                let params = Bytes::from(Value::Object(req_param).to_string());
                let request =
                    GateUnifiedNewOrderRequest::create(create_ts, self.client_order_id, params);
                Ok(request.to_bytes())
            }
            TradingVenue::GateFutures => {
                let create_ts = get_timestamp_us();
                let contract = gate_currency_pair_from_symbol(&self.symbol);
                let time_in_force = match self.order_type {
                    OrderType::Limit => Some("poc"),
                    OrderType::Market => Some("ioc"),
                    _ => None,
                };

                let signed_size = match self.side {
                    Side::Buy => self.quantity,
                    Side::Sell => -self.quantity,
                };

                let mut req_param = serde_json::Map::new();
                req_param.insert("text".to_string(), json!(self.client_order_id.to_string()));
                req_param.insert("contract".to_string(), json!(contract));
                req_param.insert("account".to_string(), json!("unified"));
                req_param.insert(
                    "size".to_string(),
                    json!(format_signed_quantity(signed_size)),
                );

                if self.order_type.is_limit() {
                    req_param.insert("price".to_string(), json!(format_price(self.price)));
                } else {
                    req_param.insert("price".to_string(), json!("0"));
                }
                if let Some(tif) = time_in_force {
                    req_param.insert("tif".to_string(), json!(tif));
                }
                if self.reduce_only {
                    req_param.insert("reduce_only".to_string(), json!(true));
                }

                let params = Bytes::from(Value::Object(req_param).to_string());
                let request =
                    GateFuturesNewOrderRequest::create(create_ts, self.client_order_id, params);
                Ok(request.to_bytes())
            }
            TradingVenue::BybitMargin | TradingVenue::BybitFutures => {
                let create_ts = get_timestamp_us();
                let symbol = bybit_symbol_from_symbol(&self.symbol);
                let quantity_qv = quantize_order_decimal(self.quantity).ok_or_else(|| {
                    format!(
                        "failed to quantize bybit quantity: qty={:.12} symbol={} client_order_id={}",
                        self.quantity, self.symbol, self.client_order_id
                    )
                })?;
                let price_qv = if self.order_type.is_limit() {
                    quantize_order_decimal(self.price).ok_or_else(|| {
                        format!(
                            "failed to quantize bybit price: price={:.12} symbol={} client_order_id={}",
                            self.price, self.symbol, self.client_order_id
                        )
                    })?
                } else {
                    QuantizedValue::zero()
                };
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
                let request = match self.venue {
                    TradingVenue::BybitMargin => {
                        BybitNewOrderRequest::create_margin(create_ts, self.client_order_id, params)
                    }
                    TradingVenue::BybitFutures => {
                        BybitNewOrderRequest::create_um(create_ts, self.client_order_id, params)
                    }
                    _ => None,
                }
                .ok_or_else(|| "failed to build bybit new order request".to_string())?;
                Ok(request.to_bytes())
            }
            TradingVenue::BitgetMargin => {
                let create_ts = get_timestamp_us();
                let mut req_param = serde_json::Map::new();
                // Bitget UTA v3：category=margin 走 cross-margin 现货并自动借币；category=spot 不会借币。
                req_param.insert("category".to_string(), json!("margin"));
                req_param.insert(
                    "symbol".to_string(),
                    json!(self.symbol.to_ascii_uppercase()),
                );
                req_param.insert("side".to_string(), json!(self.side.as_str_lower()));
                req_param.insert(
                    "orderType".to_string(),
                    json!(if self.order_type.is_limit() {
                        "limit"
                    } else {
                        "market"
                    }),
                );
                if self.order_type.is_limit() {
                    req_param.insert("force".to_string(), json!("post_only"));
                    req_param.insert("price".to_string(), json!(format_price(self.price)));
                }
                req_param.insert("size".to_string(), json!(format_quantity(self.quantity)));
                req_param.insert(
                    "clientOid".to_string(),
                    json!(self.client_order_id.to_string()),
                );
                let params = Bytes::from(Value::Object(req_param).to_string());
                let request =
                    BitgetMarginNewOrderRequest::create(create_ts, self.client_order_id, params);
                Ok(request.to_bytes())
            }
            TradingVenue::BitgetFutures => {
                let create_ts = get_timestamp_us();
                let mut req_param = serde_json::Map::new();
                // trade_engine precheck 强制 Bitget UTA futures 为 one_way_mode。
                // one-way 模式下开/平仓由 side + reduceOnly 表达，不传 hedge-mode 的 posSide。
                req_param.insert("category".to_string(), json!("usdt-futures"));
                req_param.insert(
                    "symbol".to_string(),
                    json!(self.symbol.to_ascii_uppercase()),
                );
                req_param.insert("side".to_string(), json!(self.side.as_str_lower()));
                req_param.insert(
                    "orderType".to_string(),
                    json!(if self.order_type.is_limit() {
                        "limit"
                    } else {
                        "market"
                    }),
                );
                if self.order_type.is_limit() {
                    req_param.insert("force".to_string(), json!("post_only"));
                    req_param.insert("price".to_string(), json!(format_price(self.price)));
                }
                req_param.insert("size".to_string(), json!(format_quantity(self.quantity)));
                req_param.insert(
                    "clientOid".to_string(),
                    json!(self.client_order_id.to_string()),
                );
                if self.reduce_only {
                    req_param.insert("reduceOnly".to_string(), json!("YES"));
                }
                let params = Bytes::from(Value::Object(req_param).to_string());
                let request =
                    BitgetUmNewOrderRequest::create(create_ts, self.client_order_id, params);
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
        BybitNewOrderRequest, Order, OrderExecutionStatus, OrderManager, OrderStatus, OrderType,
        PreTradeOrderManagerRequestExt, PreTradeOrderRequestExt, Side, TradeUpdateSkipReason,
    };
    use order_common::TradingVenue;
    use serde_json::Value;

    fn extract_request_json(bytes: &[u8]) -> Value {
        serde_json::from_slice(&bytes[24..]).expect("gate request params should be valid json")
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
            payload.get("force").and_then(Value::as_str),
            Some("post_only")
        );
        assert_eq!(
            payload.get("price").and_then(Value::as_str),
            Some("2363.73")
        );
        assert_eq!(payload.get("size").and_then(Value::as_str), Some("0.21"));
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
}
