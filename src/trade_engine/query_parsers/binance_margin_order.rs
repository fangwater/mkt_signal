use crate::pre_trade::order_manager::OrderExecutionStatus;
use crate::signal::common::TimeInForce;
use serde::Deserialize;

use super::binance_um_order::BinanceUmOrderQueryResp;

#[derive(Debug, Deserialize)]
struct BinanceMarginOrderQueryJson {
    #[serde(default, rename = "executedQty")]
    executed_qty: String,
    #[serde(default, rename = "avgPrice")]
    avg_price: String,
    #[serde(default)]
    price: String,
    #[serde(default, rename = "orderId")]
    order_id: i64,
    #[serde(default)]
    status: String,
    #[serde(default, rename = "timeInForce")]
    time_in_force: String,
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
    let avg_price = parsed.avg_price.parse::<f64>().unwrap_or(0.0);
    let limit_price = parsed.price.parse::<f64>().unwrap_or(0.0);
    let response_price = if avg_price > 0.0 {
        avg_price
    } else if limit_price > 0.0 {
        limit_price
    } else {
        0.0
    };
    let tif_u8 = TimeInForce::from_str(parsed.time_in_force.as_str())
        .unwrap_or(TimeInForce::GTC)
        .to_u8();
    Some(BinanceUmOrderQueryResp {
        executed_qty,
        order_id: parsed.order_id,
        status_u8: status_to_u8(parsed.status.as_str()),
        update_time_ms: parsed.update_time_ms,
        time_in_force_u8: tif_u8,
        response_price,
    })
}
