use crate::pre_trade::order_manager::OrderExecutionStatus;
use serde::Deserialize;

use super::binance_um_order::BinanceUmOrderQueryResp;

#[derive(Debug, Deserialize)]
struct BinanceMarginOrderQueryJson {
    #[serde(default, rename = "executedQty")]
    executed_qty: String,
    #[serde(default, rename = "orderId")]
    order_id: i64,
    #[serde(default)]
    status: String,
    #[serde(default, rename = "updateTime")]
    update_time_ms: i64,
}

fn status_to_u8(status: &str) -> u8 {
    match status {
        "NEW" | "PARTIALLY_FILLED" | "PENDING_CANCEL" => OrderExecutionStatus::Create.to_u8(),
        "FILLED" => OrderExecutionStatus::Filled.to_u8(),
        "CANCELED" | "CANCELLED" | "EXPIRED" => OrderExecutionStatus::Cancelled.to_u8(),
        "REJECTED" => OrderExecutionStatus::Rejected.to_u8(),
        _ => OrderExecutionStatus::Create.to_u8(),
    }
}

pub fn parse_binance_margin_order_query_json(json: &str) -> Option<BinanceUmOrderQueryResp> {
    let parsed: BinanceMarginOrderQueryJson = serde_json::from_str(json).ok()?;
    let executed_qty = parsed.executed_qty.parse::<f64>().unwrap_or(0.0);
    Some(BinanceUmOrderQueryResp {
        executed_qty,
        order_id: parsed.order_id,
        status_u8: status_to_u8(parsed.status.as_str()),
        update_time_ms: parsed.update_time_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_binance_margin_order_query() {
        let json = r#"{
            "executedQty": "0.00000000",
            "orderId": 213205622,
            "status": "NEW",
            "updateTime": 1562133008725
        }"#;
        let parsed = parse_binance_margin_order_query_json(json).expect("parse ok");
        assert_eq!(parsed.order_id, 213205622);
        assert_eq!(parsed.update_time_ms, 1562133008725);
        assert_eq!(parsed.status_u8, OrderExecutionStatus::Create.to_u8());
        assert!((parsed.executed_qty - 0.0).abs() < 1e-12);
    }
}
