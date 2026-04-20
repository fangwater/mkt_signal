use crate::pre_trade::order_manager::OrderExecutionStatus;
use crate::signal::common::TimeInForce;
use serde::Deserialize;

use super::binance_um_order::BinanceUmOrderQueryResp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BitgetOrderQueryParseErrorKind {
    OrderNotFound,
    Other,
}

#[derive(Debug, Clone)]
pub enum BitgetOrderQueryParseResult {
    Success(BinanceUmOrderQueryResp),
    Error {
        kind: BitgetOrderQueryParseErrorKind,
        code: i32,
        msg: String,
    },
}

#[derive(Debug, Deserialize)]
struct BitgetOrderQueryOuter {
    #[serde(default)]
    code: String,
    #[serde(default)]
    msg: String,
    #[serde(default)]
    data: Option<BitgetOrderQueryItem>,
}

#[derive(Debug, Deserialize)]
struct BitgetOrderQueryItem {
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

fn parse_i32_str(v: &str) -> i32 {
    v.trim().parse::<i32>().unwrap_or(0)
}

fn status_to_u8(state: &str) -> u8 {
    match state.to_ascii_lowercase().as_str() {
        "new" | "init" | "live" => OrderExecutionStatus::Create.to_u8(),
        "partially_filled" | "partially-filled" | "partial-fill" => {
            OrderExecutionStatus::Create.to_u8()
        }
        "filled" | "full-fill" => OrderExecutionStatus::Filled.to_u8(),
        "cancelled" | "canceled" => OrderExecutionStatus::Cancelled.to_u8(),
        "rejected" | "expired" => OrderExecutionStatus::Rejected.to_u8(),
        _ => OrderExecutionStatus::Create.to_u8(),
    }
}

fn tif_to_u8(raw: &str) -> u8 {
    match raw.to_ascii_lowercase().as_str() {
        "fok" => TimeInForce::FOK.to_u8(),
        "ioc" => TimeInForce::IOC.to_u8(),
        "post_only" | "post-only" => TimeInForce::GTX.to_u8(),
        _ => TimeInForce::GTC.to_u8(),
    }
}

fn response_price(item: &BitgetOrderQueryItem) -> f64 {
    let avg = parse_f64_str(&item.avg_price);
    if avg > 0.0 {
        return avg;
    }
    parse_f64_str(&item.price)
}

pub fn parse_bitget_order_query_json(json: &str) -> BitgetOrderQueryParseResult {
    let outer: BitgetOrderQueryOuter = match serde_json::from_str(json) {
        Ok(v) => v,
        Err(_) => {
            return BitgetOrderQueryParseResult::Error {
                kind: BitgetOrderQueryParseErrorKind::Other,
                code: 0,
                msg: String::new(),
            };
        }
    };

    let code = parse_i32_str(&outer.code);
    if code != 0 {
        let kind = match code {
            43001 | 45057 => BitgetOrderQueryParseErrorKind::OrderNotFound,
            _ => BitgetOrderQueryParseErrorKind::Other,
        };
        return BitgetOrderQueryParseResult::Error {
            kind,
            code,
            msg: outer.msg,
        };
    }

    let Some(item) = outer.data else {
        return BitgetOrderQueryParseResult::Error {
            kind: BitgetOrderQueryParseErrorKind::OrderNotFound,
            code: 0,
            msg: "empty result".to_string(),
        };
    };

    BitgetOrderQueryParseResult::Success(BinanceUmOrderQueryResp {
        executed_qty: parse_f64_str(&item.cum_exec_qty),
        order_id: parse_i64_str(&item.order_id),
        status_u8: status_to_u8(&item.order_status),
        update_time_ms: parse_i64_str(&item.updated_time),
        time_in_force_u8: tif_to_u8(&item.time_in_force),
        response_price: response_price(&item),
    })
}
