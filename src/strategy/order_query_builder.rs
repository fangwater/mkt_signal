use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::trade_engine::query_request::{GenericQueryRequest, QueryRequestType};
use order_common::{gate_text_from_client_order_id, Order, TradingVenue};
use runtime_common::time_util::get_timestamp_us;
use symbol_utils::symbol_util::{
    gate_currency_pair_from_symbol, normalize_symbol_for_internal, okex_inst_id_from_symbol,
};

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
        TradingVenue::OkexMargin => QueryRequestType::OkexMarginQuery,
        TradingVenue::OkexFutures => QueryRequestType::OkexUMQuery,
        TradingVenue::BybitMargin => QueryRequestType::BybitMarginQuery,
        TradingVenue::BybitFutures => QueryRequestType::BybitUMQuery,
        TradingVenue::BitgetMargin => QueryRequestType::BitgetMarginQuery,
        TradingVenue::BitgetFutures => QueryRequestType::BitgetUMQuery,
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
                    order.symbol, lookup_client_order_id
                ))
            }
        }
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
            let inst_id = okex_inst_id_from_symbol(&order.symbol, order.venue)?;
            if let Some(order_id) = exchange_order_id {
                bytes::Bytes::from(format!("instId={}&ordId={}", inst_id, order_id))
            } else {
                bytes::Bytes::from(format!(
                    "instId={}&clOrdId={}",
                    inst_id, lookup_client_order_id
                ))
            }
        }
        TradingVenue::BybitMargin => bytes::Bytes::from(format!(
            "category=spot&symbol={}&orderLinkId={}",
            normalize_symbol_for_internal(&order.symbol),
            lookup_client_order_id
        )),
        TradingVenue::BybitFutures => bytes::Bytes::from(format!(
            "category=linear&symbol={}&orderLinkId={}",
            normalize_symbol_for_internal(&order.symbol),
            lookup_client_order_id
        )),
        TradingVenue::BitgetMargin | TradingVenue::BitgetFutures => {
            if let Some(order_id) = exchange_order_id {
                bytes::Bytes::from(format!("orderId={}", order_id))
            } else {
                bytes::Bytes::from(format!("clientOid={}", lookup_client_order_id))
            }
        }
        TradingVenue::GateMargin => {
            let currency_pair = gate_currency_pair_from_symbol(&order.symbol);
            let order_id = exchange_order_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| gate_text_from_client_order_id(lookup_client_order_id));
            let req_param = serde_json::json!({
                "order_id": order_id,
                "currency_pair": currency_pair,
                "account": "unified",
            });
            bytes::Bytes::from(req_param.to_string())
        }
        TradingVenue::GateFutures => {
            let order_id = exchange_order_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| gate_text_from_client_order_id(lookup_client_order_id));
            let req_param = serde_json::json!({
                "order_id": order_id,
            });
            bytes::Bytes::from(req_param.to_string())
        }
        _ => bytes::Bytes::new(),
    };

    let req = GenericQueryRequest::create(req_type, get_timestamp_us(), request_query_id, params);
    Ok((exchange, req.to_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trade_engine::query_request::{QueryRequestMsg, QueryRequestType};
    use order_common::{OrderManager, OrderType, Side};
    use serde_json::Value;

    #[test]
    fn gate_margin_order_query_uses_unified_account() {
        let mut order_manager = OrderManager::new(None);
        let client_order_id = 1133736985207242753;
        order_manager.create_order(
            TradingVenue::GateMargin,
            client_order_id,
            OrderType::Limit,
            "CCUSDT".to_string(),
            Side::Sell,
            7.0,
            0.14653,
            false,
            1.0,
        );
        let order = order_manager
            .get(client_order_id)
            .expect("test order should exist");

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
}
