use crate::pre_trade::hyperliquid_account_hash_from_env;
use crate::pre_trade::monitor_channel::MonitorChannel;
use order_common::{
    gate_text_from_client_order_id, hyperliquid_cloid_from_client_order_id, Order, TradingVenue,
};
use runtime_common::time_util::get_timestamp_us;
use std::fmt::Write as _;
use symbol_utils::symbol_util::{
    binance_coin_futures_symbol, bitget_coin_futures_symbol, gate_currency_pair_from_symbol,
    normalize_symbol_for_internal, okex_inst_id_from_symbol,
};
use trade_engine::query_request::{GenericQueryRequest, HyperliquidQueryParams, QueryRequestType};

pub fn build_order_query_request(
    order: &Order,
    request_query_id: i64,
    lookup_client_order_id: i64,
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
        TradingVenue::BinanceCoinFutures => {
            if MonitorChannel::instance()
                .order_manager()
                .borrow()
                .binance_is_standard()
            {
                QueryRequestType::BinanceCmQuery
            } else {
                QueryRequestType::BinancePmCmQuery
            }
        }
        TradingVenue::OkexMargin => QueryRequestType::OkexMarginQuery,
        TradingVenue::OkexFutures => QueryRequestType::OkexUMQuery,
        TradingVenue::BybitMargin => QueryRequestType::BybitMarginQuery,
        TradingVenue::BybitFutures => QueryRequestType::BybitUMQuery,
        TradingVenue::BitgetMargin => QueryRequestType::BitgetMarginQuery,
        TradingVenue::BitgetFutures => QueryRequestType::BitgetUMQuery,
        TradingVenue::BitgetCoinFutures => QueryRequestType::BitgetCoinFuturesQuery,
        TradingVenue::GateMargin => QueryRequestType::GateUnifiedOrderQuery,
        TradingVenue::GateFutures => QueryRequestType::GateFuturesOrderQuery,
        TradingVenue::HyperliquidMargin => QueryRequestType::HyperliquidMarginQuery,
        TradingVenue::HyperliquidFutures => QueryRequestType::HyperliquidUMQuery,
        _ => return Err(format!("unsupported venue for query: {:?}", order.venue)),
    };

    let params = match order.venue {
        TradingVenue::BinanceMargin | TradingVenue::BinanceFutures => {
            if let Some(order_id) = exchange_order_id {
                query_bytes_with_i64_pairs("symbol", &order.symbol, "orderId", order_id)
            } else {
                query_bytes_with_i64_pairs(
                    "symbol",
                    &order.symbol,
                    "origClientOrderId",
                    lookup_client_order_id,
                )
            }
        }
        TradingVenue::BinanceCoinFutures => {
            let symbol = binance_coin_futures_symbol(&order.symbol);
            if let Some(order_id) = exchange_order_id {
                query_bytes_with_i64_pairs("symbol", &symbol, "orderId", order_id)
            } else {
                query_bytes_with_i64_pairs(
                    "symbol",
                    &symbol,
                    "origClientOrderId",
                    lookup_client_order_id,
                )
            }
        }
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
            let inst_id = okex_inst_id_from_symbol(&order.symbol, order.venue)?;
            if let Some(order_id) = exchange_order_id {
                query_bytes_with_i64_pairs("instId", &inst_id, "ordId", order_id)
            } else {
                query_bytes_with_i64_pairs("instId", &inst_id, "clOrdId", lookup_client_order_id)
            }
        }
        TradingVenue::BybitMargin => bybit_query_bytes(
            "spot",
            &normalize_symbol_for_internal(&order.symbol),
            lookup_client_order_id,
        ),
        TradingVenue::BybitFutures => bybit_query_bytes(
            "linear",
            &normalize_symbol_for_internal(&order.symbol),
            lookup_client_order_id,
        ),
        TradingVenue::BitgetMargin | TradingVenue::BitgetFutures => {
            let category = if order.venue == TradingVenue::BitgetMargin {
                "MARGIN"
            } else {
                "USDT-FUTURES"
            };
            bitget_query_bytes(category, None, exchange_order_id, lookup_client_order_id)
        }
        TradingVenue::BitgetCoinFutures => {
            let symbol = bitget_coin_futures_symbol(&order.symbol);
            bitget_query_bytes(
                "COIN-FUTURES",
                Some(&symbol),
                exchange_order_id,
                lookup_client_order_id,
            )
        }
        TradingVenue::GateMargin => {
            let currency_pair = gate_currency_pair_from_symbol(&order.symbol);
            let order_id = exchange_order_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| gate_text_from_client_order_id(lookup_client_order_id));
            gate_margin_query_json_bytes(&order_id, &currency_pair)
        }
        TradingVenue::GateFutures => {
            let order_id = exchange_order_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| gate_text_from_client_order_id(lookup_client_order_id));
            gate_futures_query_json_bytes(&order_id)
        }
        TradingVenue::HyperliquidMargin | TradingVenue::HyperliquidFutures => {
            let body =
                hyperliquid_order_status_query_bytes(exchange_order_id, lookup_client_order_id)?;
            HyperliquidQueryParams::create(hyperliquid_account_hash_from_env()?, body).to_bytes()
        }
        _ => bytes::Bytes::new(),
    };

    let req = GenericQueryRequest::create(req_type, get_timestamp_us(), request_query_id, params);
    Ok((exchange, req.to_bytes()))
}

fn hyperliquid_order_status_query_bytes(
    exchange_order_id: Option<i64>,
    client_order_id: i64,
) -> Result<bytes::Bytes, String> {
    let value = if let Some(order_id) = exchange_order_id.filter(|order_id| *order_id > 0) {
        serde_json::json!({"oid": order_id})
    } else {
        let cloid = hyperliquid_cloid_from_client_order_id(client_order_id)
            .ok_or_else(|| format!("invalid Hyperliquid client order id {client_order_id}"))?;
        serde_json::json!({"oid": cloid})
    };
    serde_json::to_vec(&value)
        .map(bytes::Bytes::from)
        .map_err(|err| format!("encode Hyperliquid order query: {err}"))
}

fn bitget_query_bytes(
    category: &str,
    symbol: Option<&str>,
    exchange_order_id: Option<i64>,
    client_order_id: i64,
) -> bytes::Bytes {
    let mut out = String::with_capacity(category.len() + symbol.map(str::len).unwrap_or(0) + 64);
    out.push_str("category=");
    out.push_str(category);
    if let Some(symbol) = symbol {
        out.push_str("&symbol=");
        out.push_str(symbol);
    }
    if let Some(order_id) = exchange_order_id {
        out.push_str("&orderId=");
        write!(out, "{}", order_id).expect("write Bitget order id");
    } else {
        out.push_str("&clientOid=");
        write!(out, "{}", client_order_id).expect("write Bitget client order id");
    }
    bytes::Bytes::from(out)
}

fn query_bytes_with_i64_pairs(key1: &str, value1: &str, key2: &str, value2: i64) -> bytes::Bytes {
    let mut out = String::with_capacity(key1.len() + value1.len() + key2.len() + 28);
    out.push_str(key1);
    out.push('=');
    out.push_str(value1);
    out.push('&');
    out.push_str(key2);
    out.push('=');
    write!(out, "{}", value2).expect("write query i64 value");
    bytes::Bytes::from(out)
}

fn bybit_query_bytes(category: &str, symbol: &str, order_link_id: i64) -> bytes::Bytes {
    let mut out = String::with_capacity(category.len() + symbol.len() + 48);
    out.push_str("category=");
    out.push_str(category);
    out.push_str("&symbol=");
    out.push_str(symbol);
    out.push_str("&orderLinkId=");
    write!(out, "{}", order_link_id).expect("write bybit order link id");
    bytes::Bytes::from(out)
}

fn gate_margin_query_json_bytes(order_id: &str, currency_pair: &str) -> bytes::Bytes {
    let mut out = String::with_capacity(order_id.len() + currency_pair.len() + 72);
    out.push_str("{\"order_id\":");
    push_json_string(&mut out, order_id);
    out.push_str(",\"currency_pair\":");
    push_json_string(&mut out, currency_pair);
    out.push_str(",\"account\":\"unified\"}");
    bytes::Bytes::from(out)
}

fn gate_futures_query_json_bytes(order_id: &str) -> bytes::Bytes {
    let mut out = String::with_capacity(order_id.len() + 18);
    out.push_str("{\"order_id\":");
    push_json_string(&mut out, order_id);
    out.push('}');
    bytes::Bytes::from(out)
}

fn push_json_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if c <= '\u{1f}' => {
                write!(out, "\\u{:04x}", c as u32).expect("write json escape");
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::hyperliquid_account_msg::hyperliquid_account_identity_hash;
    use order_common::{OrderManager, OrderType, Side};
    use serde_json::Value;
    use trade_engine::query_request::{HyperliquidQueryParams, QueryRequestMsg, QueryRequestType};

    const HYPERLIQUID_TEST_ACCOUNT: &str = "0x1111111111111111111111111111111111111111";

    fn configure_hyperliquid_test_account() -> [u8; 32] {
        std::env::set_var("HYPERLIQUID_ACCOUNT_ADDRESS", HYPERLIQUID_TEST_ACCOUNT);
        std::env::set_var("HYPERLIQUID_TESTNET", "0");
        hyperliquid_account_identity_hash(HYPERLIQUID_TEST_ACCOUNT, false).unwrap()
    }

    fn order_from_manager(
        venue: TradingVenue,
        client_order_id: i64,
        symbol: &str,
        exchange_order_id: Option<i64>,
    ) -> Order {
        let mut order_manager = OrderManager::new(None);
        order_manager.create_order(
            venue,
            client_order_id,
            OrderType::Limit,
            symbol.to_string(),
            Side::Sell,
            7.0,
            0.14653,
            false,
            1.0,
        );
        let mut order = order_manager
            .get(client_order_id)
            .expect("test order should exist");
        order.exchange_order_id = exchange_order_id;
        order
    }

    fn query_params(bytes: bytes::Bytes) -> (QueryRequestMsg, String) {
        let msg = QueryRequestMsg::parse(bytes.as_ref()).expect("query request should parse");
        let params = std::str::from_utf8(msg.params.as_ref())
            .expect("params should be utf-8")
            .to_string();
        (msg, params)
    }

    #[test]
    fn binance_query_helpers_preserve_required_order() {
        assert_eq!(
            query_bytes_with_i64_pairs("symbol", "BTCUSDT", "orderId", 99),
            bytes::Bytes::from_static(b"symbol=BTCUSDT&orderId=99")
        );
        assert_eq!(
            query_bytes_with_i64_pairs("symbol", "BTCUSDT", "origClientOrderId", 42),
            bytes::Bytes::from_static(b"symbol=BTCUSDT&origClientOrderId=42")
        );
    }

    #[test]
    fn okex_order_query_uses_inst_id_and_exchange_order_id() {
        let client_order_id = 1133736985207242753;
        let order = order_from_manager(
            TradingVenue::OkexFutures,
            client_order_id,
            "BTCUSDT",
            Some(998877),
        );

        let (exchange, bytes) = build_order_query_request(&order, client_order_id, client_order_id)
            .expect("okex futures order query should build");
        let (msg, params) = query_params(bytes);

        assert_eq!(exchange, "okex");
        assert_eq!(msg.req_type, QueryRequestType::OkexUMQuery);
        assert_eq!(params, "instId=BTC-USDT-SWAP&ordId=998877");
    }

    #[test]
    fn bybit_margin_order_query_uses_spot_category() {
        let client_order_id = 1133736985207242753;
        let order =
            order_from_manager(TradingVenue::BybitMargin, client_order_id, "BTC_USDT", None);

        let (exchange, bytes) = build_order_query_request(&order, client_order_id, client_order_id)
            .expect("bybit margin order query should build");
        let (msg, params) = query_params(bytes);

        assert_eq!(exchange, "bybit");
        assert_eq!(msg.req_type, QueryRequestType::BybitMarginQuery);
        assert_eq!(
            params,
            "category=spot&symbol=BTCUSDT&orderLinkId=1133736985207242753"
        );
    }

    #[test]
    fn bitget_coin_futures_query_uses_category_and_exchange_symbol() {
        let client_order_id = 1133736985207242753;
        let order = order_from_manager(
            TradingVenue::BitgetCoinFutures,
            client_order_id,
            "BTCUSDCM",
            None,
        );

        let (exchange, bytes) = build_order_query_request(&order, 99, client_order_id)
            .expect("bitget coin futures query should build");
        let (msg, params) = query_params(bytes);

        assert_eq!(exchange, "bitget");
        assert_eq!(msg.req_type, QueryRequestType::BitgetCoinFuturesQuery);
        assert_eq!(
            params,
            "category=COIN-FUTURES&symbol=BTCUSD_CM&clientOid=1133736985207242753"
        );
    }

    #[test]
    fn gate_margin_order_query_uses_unified_account() {
        let client_order_id = 1133736985207242753;
        let order = order_from_manager(TradingVenue::GateMargin, client_order_id, "CCUSDT", None);

        let (exchange, bytes) = build_order_query_request(&order, client_order_id, client_order_id)
            .expect("gate margin order query should build");
        let msg = QueryRequestMsg::parse(bytes.as_ref()).expect("query request should parse");
        let params: Value =
            serde_json::from_slice(msg.params.as_ref()).expect("params should be json");

        assert_eq!(exchange, "gate");
        assert_eq!(msg.req_type, QueryRequestType::GateUnifiedOrderQuery);
        assert_eq!(
            params.get("currency_pair").and_then(Value::as_str),
            Some("CC_USDT")
        );
        assert_eq!(
            params.get("order_id").and_then(Value::as_str),
            Some("t-1133736985207242753")
        );
        assert_eq!(
            params.get("account").and_then(Value::as_str),
            Some("unified")
        );
    }

    #[test]
    fn gate_futures_order_query_uses_text_client_order_id() {
        let client_order_id = 1133736985207242753;
        let order =
            order_from_manager(TradingVenue::GateFutures, client_order_id, "BTC_USDT", None);

        let (exchange, bytes) = build_order_query_request(&order, client_order_id, client_order_id)
            .expect("gate futures order query should build");
        let msg = QueryRequestMsg::parse(bytes.as_ref()).expect("query request should parse");
        let params: Value =
            serde_json::from_slice(msg.params.as_ref()).expect("params should be json");

        assert_eq!(exchange, "gate");
        assert_eq!(msg.req_type, QueryRequestType::GateFuturesOrderQuery);
        assert_eq!(
            params.get("order_id").and_then(Value::as_str),
            Some("t-1133736985207242753")
        );
    }

    #[test]
    fn hyperliquid_order_query_prefers_exchange_oid() {
        let expected_account_hash = configure_hyperliquid_test_account();
        let client_order_id = 1133736985207242753;
        let order = order_from_manager(
            TradingVenue::HyperliquidFutures,
            client_order_id,
            "BTCUSDC",
            Some(998877),
        );

        let (exchange, bytes) = build_order_query_request(&order, 99, client_order_id).unwrap();
        let msg = QueryRequestMsg::parse(bytes.as_ref()).unwrap();
        let params = HyperliquidQueryParams::from_bytes(msg.params.as_ref()).unwrap();
        assert_eq!(params.account_hash, expected_account_hash);
        let body: Value = serde_json::from_slice(params.body.as_ref()).unwrap();

        assert_eq!(exchange, "hyperliquid");
        assert_eq!(msg.req_type, QueryRequestType::HyperliquidUMQuery);
        assert_eq!(body["oid"], serde_json::json!(998877));
    }

    #[test]
    fn hyperliquid_order_query_falls_back_to_internal_cloid() {
        let expected_account_hash = configure_hyperliquid_test_account();
        let client_order_id = 42;
        let order = order_from_manager(
            TradingVenue::HyperliquidMargin,
            client_order_id,
            "HYPEUSDC",
            None,
        );

        let (_, bytes) = build_order_query_request(&order, 99, client_order_id).unwrap();
        let msg = QueryRequestMsg::parse(bytes.as_ref()).unwrap();
        let params = HyperliquidQueryParams::from_bytes(msg.params.as_ref()).unwrap();
        assert_eq!(params.account_hash, expected_account_hash);
        let body: Value = serde_json::from_slice(params.body.as_ref()).unwrap();

        assert_eq!(msg.req_type, QueryRequestType::HyperliquidMarginQuery);
        assert_eq!(body["oid"], "0x6d6b745f73696731000000000000002a");
    }
}
