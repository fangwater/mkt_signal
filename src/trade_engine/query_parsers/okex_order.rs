use crate::pre_trade::order_manager::OrderExecutionStatus;
use serde::Deserialize;

use super::binance_um_order::BinanceUmOrderQueryResp;

#[derive(Debug, Deserialize)]
struct OkexOrderQueryOuter {
    #[serde(default)]
    code: String,
    #[serde(default)]
    data: Vec<OkexOrderQueryItem>,
}

#[derive(Debug, Deserialize)]
struct OkexOrderQueryItem {
    #[serde(default, rename = "accFillSz")]
    acc_fill_sz: String,
    #[serde(default, rename = "ordId")]
    ord_id: String,
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
        "canceled" | "cancelled" => OrderExecutionStatus::Cancelled.to_u8(),
        "rejected" => OrderExecutionStatus::Rejected.to_u8(),
        _ => OrderExecutionStatus::Create.to_u8(),
    }
}

pub fn parse_okex_order_query_json(json: &str) -> Option<BinanceUmOrderQueryResp> {
    let outer: OkexOrderQueryOuter = serde_json::from_str(json).ok()?;
    if outer.code != "0" {
        return None;
    }
    let first = outer.data.first()?;
    Some(BinanceUmOrderQueryResp {
        executed_qty: parse_f64_str(first.acc_fill_sz.as_str()),
        order_id: parse_i64_str(first.ord_id.as_str()),
        status_u8: status_to_u8(first.state.as_str()),
        update_time_ms: parse_i64_str(first.u_time.as_str()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_okex_order_query() {
        let json = r#"{
            "code": "0",
            "data": [{
                "accFillSz": "0.00192834",
                "ordId": "680800019749904384",
                "state": "filled",
                "uTime": "1708587373362"
            }],
            "msg": ""
        }"#;
        let parsed = parse_okex_order_query_json(json).expect("parse ok");
        assert!((parsed.executed_qty - 0.00192834).abs() < 1e-12);
        assert_eq!(parsed.order_id, 680800019749904384);
        assert_eq!(parsed.status_u8, OrderExecutionStatus::Filled.to_u8());
        assert_eq!(parsed.update_time_ms, 1708587373362);
    }
}
