use crate::pre_trade::order_manager::OrderExecutionStatus;
use crate::signal::common::TimeInForce;
use serde::Deserialize;

use super::binance_um_order::BinanceUmOrderQueryResp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OkexOrderQueryParseErrorKind {
    OrderNotFound,
    Other,
}

#[derive(Debug, Clone)]
pub enum OkexOrderQueryParseResult {
    Success(BinanceUmOrderQueryResp),
    Error {
        kind: OkexOrderQueryParseErrorKind,
        code: String,
        msg: String,
    },
}

#[derive(Debug, Deserialize)]
struct OkexOrderQueryOuter {
    #[serde(default)]
    code: String,
    #[serde(default)]
    msg: String,
    #[serde(default)]
    data: Vec<OkexOrderQueryItem>,
}

#[derive(Debug, Deserialize)]
struct OkexOrderQueryItem {
    #[serde(default, rename = "accFillSz")]
    acc_fill_sz: String,
    #[serde(default, rename = "fillPx")]
    fill_px: String,
    #[serde(default, rename = "px")]
    px: String,
    #[serde(default, rename = "ordId")]
    ord_id: String,
    #[serde(default, rename = "ordType")]
    ord_type: String,
    #[serde(default, rename = "state")]
    state: String,
    #[serde(default, rename = "uTime")]
    u_time: String,
}

fn parse_f64_str(v: &str) -> f64 {
    v.parse::<f64>().unwrap_or(0.0)
}

fn parse_i64_str(v: &str) -> i64 {
    v.parse::<i64>().unwrap_or(0)
}

fn status_to_u8(state: &str) -> u8 {
    match state {
        "live" | "partially_filled" => OrderExecutionStatus::Create.to_u8(),
        "filled" => OrderExecutionStatus::Filled.to_u8(),
        "canceled" | "cancelled" | "mmp_canceled" => OrderExecutionStatus::Cancelled.to_u8(),
        "rejected" => OrderExecutionStatus::Rejected.to_u8(),
        _ => OrderExecutionStatus::Create.to_u8(),
    }
}

fn tif_to_u8(ord_type: &str) -> u8 {
    match ord_type.to_ascii_lowercase().as_str() {
        "fok" | "op_fok" => TimeInForce::FOK.to_u8(),
        "ioc" | "optimal_limit_ioc" => TimeInForce::IOC.to_u8(),
        "post_only" | "mmp_and_post_only" => TimeInForce::GTX.to_u8(),
        _ => TimeInForce::GTC.to_u8(),
    }
}

fn response_price_for_state(state: &str, fill_px: f64, px: f64) -> f64 {
    match state.trim().to_ascii_lowercase().as_str() {
        "partially_filled" | "filled" => fill_px,
        _ => px,
    }
}

pub fn parse_okex_order_query_json(json: &str) -> OkexOrderQueryParseResult {
    let outer: OkexOrderQueryOuter = match serde_json::from_str(json) {
        Ok(v) => v,
        Err(_) => {
            return OkexOrderQueryParseResult::Error {
                kind: OkexOrderQueryParseErrorKind::Other,
                code: String::new(),
                msg: String::new(),
            };
        }
    };
    if outer.code != "0" {
        let kind =
            if outer.code == "51603" || outer.msg.eq_ignore_ascii_case("Order does not exist") {
                OkexOrderQueryParseErrorKind::OrderNotFound
            } else {
                OkexOrderQueryParseErrorKind::Other
            };
        return OkexOrderQueryParseResult::Error {
            kind,
            code: outer.code,
            msg: outer.msg,
        };
    }
    let Some(first) = outer.data.first() else {
        return OkexOrderQueryParseResult::Error {
            kind: OkexOrderQueryParseErrorKind::Other,
            code: outer.code,
            msg: outer.msg,
        };
    };
    let fill_px = parse_f64_str(first.fill_px.as_str());
    let px = parse_f64_str(first.px.as_str());
    let response_price = response_price_for_state(first.state.as_str(), fill_px, px);
    // OKX query:
    // - spot/margin: accFillSz 为 base qty
    // - futures(swap): accFillSz 为 contracts
    // 这里保持交易所原始口径；策略层再通过 qty_to_base(...) 统一转换为 base qty。
    OkexOrderQueryParseResult::Success(BinanceUmOrderQueryResp {
        executed_qty: parse_f64_str(first.acc_fill_sz.as_str()),
        order_id: parse_i64_str(first.ord_id.as_str()),
        status_u8: status_to_u8(first.state.as_str()),
        update_time_ms: parse_i64_str(first.u_time.as_str()),
        time_in_force_u8: tif_to_u8(first.ord_type.as_str()),
        response_price,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        parse_okex_order_query_json, OkexOrderQueryParseErrorKind, OkexOrderQueryParseResult,
    };

    #[test]
    fn parse_okex_order_query_success() {
        let json = r#"{"code":"0","msg":"","data":[{"accFillSz":"12.44","fillPx":"0","px":"0.09560074","ordId":"123456","ordType":"post_only","state":"live","uTime":"1776211388000"}]}"#;
        let parsed = parse_okex_order_query_json(json);
        match parsed {
            OkexOrderQueryParseResult::Success(resp) => {
                assert_eq!(resp.order_id, 123456);
                assert_eq!(resp.executed_qty, 12.44);
                assert_eq!(resp.response_price, 0.09560074);
            }
            other => panic!("unexpected parse result: {:?}", other),
        }
    }

    #[test]
    fn parse_okex_order_query_not_found() {
        let json = r#"{"code":"51603","msg":"Order does not exist","data":[]}"#;
        let parsed = parse_okex_order_query_json(json);
        match parsed {
            OkexOrderQueryParseResult::Error { kind, code, msg } => {
                assert_eq!(kind, OkexOrderQueryParseErrorKind::OrderNotFound);
                assert_eq!(code, "51603");
                assert_eq!(msg, "Order does not exist");
            }
            other => panic!("unexpected parse result: {:?}", other),
        }
    }
}
