use crate::common::bybit_account_msg::BybitBasicOrderMsg;
use crate::pre_trade::order_manager::OrderExecutionStatus;
use crate::signal::common::TimeInForce;
use serde::Deserialize;

use super::binance_um_order::BinanceUmOrderQueryResp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BybitOrderQueryParseErrorKind {
    OrderNotFound,
    Other,
}

#[derive(Debug, Clone)]
pub enum BybitOrderQueryParseResult {
    Success(BinanceUmOrderQueryResp),
    Error {
        kind: BybitOrderQueryParseErrorKind,
        code: i32,
        msg: String,
    },
}

#[derive(Debug, Deserialize)]
struct BybitOrderQueryOuter {
    #[serde(default, rename = "retCode")]
    ret_code: i32,
    #[serde(default, rename = "retMsg")]
    ret_msg: String,
    #[serde(default)]
    result: Option<BybitOrderQueryResult>,
}

#[derive(Debug, Deserialize)]
struct BybitOrderQueryResult {
    #[serde(default)]
    list: Vec<BybitOrderQueryItem>,
}

#[derive(Debug, Deserialize)]
struct BybitOrderQueryItem {
    #[serde(default, rename = "cumExecQty")]
    cum_exec_qty: String,
    #[serde(default, rename = "avgPrice")]
    avg_price: String,
    #[serde(default)]
    price: String,
    #[serde(default, rename = "orderId")]
    order_id: String,
    #[serde(default, rename = "orderStatus")]
    order_status: String,
    #[serde(default, rename = "timeInForce")]
    time_in_force: String,
    #[serde(default, rename = "updatedTime")]
    updated_time: String,
}

fn parse_f64_str(v: &str) -> f64 {
    v.trim().parse::<f64>().unwrap_or(0.0)
}

fn parse_i64_str(v: &str) -> i64 {
    v.trim().parse::<i64>().unwrap_or(0)
}

fn status_to_u8(state: &str) -> u8 {
    match state.to_ascii_lowercase().as_str() {
        "new" | "created" | "active" | "partiallyfilled" | "partially_filled" => {
            OrderExecutionStatus::Create.to_u8()
        }
        "filled" => OrderExecutionStatus::Filled.to_u8(),
        "cancelled" | "canceled" | "deactivated" => OrderExecutionStatus::Cancelled.to_u8(),
        "rejected" | "expired" | "partiallyfilledcanceled" => OrderExecutionStatus::Rejected.to_u8(),
        _ => OrderExecutionStatus::Create.to_u8(),
    }
}

fn tif_to_u8(raw: &str) -> u8 {
    match raw.to_ascii_lowercase().as_str() {
        "fok" => TimeInForce::FOK.to_u8(),
        "ioc" => TimeInForce::IOC.to_u8(),
        "postonly" | "post_only" | "post-only" => TimeInForce::GTX.to_u8(),
        _ => TimeInForce::GTC.to_u8(),
    }
}

fn response_price(item: &BybitOrderQueryItem) -> f64 {
    let avg = parse_f64_str(&item.avg_price);
    if avg > 0.0 {
        return avg;
    }
    parse_f64_str(&item.price)
}

pub fn parse_bybit_order_query_json(json: &str) -> BybitOrderQueryParseResult {
    let outer: BybitOrderQueryOuter = match serde_json::from_str(json) {
        Ok(v) => v,
        Err(_) => {
            return BybitOrderQueryParseResult::Error {
                kind: BybitOrderQueryParseErrorKind::Other,
                code: 0,
                msg: String::new(),
            };
        }
    };

    if outer.ret_code != 0 {
        let kind = if matches!(outer.ret_code, 110001 | 170213) {
            BybitOrderQueryParseErrorKind::OrderNotFound
        } else {
            BybitOrderQueryParseErrorKind::Other
        };
        return BybitOrderQueryParseResult::Error {
            kind,
            code: outer.ret_code,
            msg: outer.ret_msg,
        };
    }

    let Some(first) = outer.result.and_then(|r| r.list.into_iter().next()) else {
        return BybitOrderQueryParseResult::Error {
            kind: BybitOrderQueryParseErrorKind::OrderNotFound,
            code: 0,
            msg: "empty result".to_string(),
        };
    };

    BybitOrderQueryParseResult::Success(BinanceUmOrderQueryResp {
        executed_qty: parse_f64_str(&first.cum_exec_qty),
        order_id: BybitBasicOrderMsg::stable_i64_from_str(&first.order_id),
        status_u8: status_to_u8(&first.order_status),
        update_time_ms: parse_i64_str(&first.updated_time),
        time_in_force_u8: tif_to_u8(&first.time_in_force),
        response_price: response_price(&first),
    })
}

#[cfg(test)]
mod tests {
    use super::{parse_bybit_order_query_json, BybitOrderQueryParseErrorKind, BybitOrderQueryParseResult};

    #[test]
    fn parse_bybit_order_query_success() {
        let json = r#"{"retCode":0,"retMsg":"OK","result":{"list":[{"cumExecQty":"12.44","avgPrice":"0","price":"0.09560074","orderId":"abcdef","timeInForce":"PostOnly","orderStatus":"New","updatedTime":"1776211388000"}]}}"#;
        let parsed = parse_bybit_order_query_json(json);
        match parsed {
            BybitOrderQueryParseResult::Success(resp) => {
                assert!(resp.order_id > 0);
                assert_eq!(resp.executed_qty, 12.44);
                assert_eq!(resp.response_price, 0.09560074);
            }
            other => panic!("unexpected parse result: {:?}", other),
        }
    }

    #[test]
    fn parse_bybit_order_query_not_found() {
        let json = r#"{"retCode":110001,"retMsg":"Order not exists","result":{"list":[]}}"#;
        let parsed = parse_bybit_order_query_json(json);
        match parsed {
            BybitOrderQueryParseResult::Error { kind, code, .. } => {
                assert_eq!(kind, BybitOrderQueryParseErrorKind::OrderNotFound);
                assert_eq!(code, 110001);
            }
            other => panic!("unexpected parse result: {:?}", other),
        }
    }
}
