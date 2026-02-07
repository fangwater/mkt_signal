use crate::common::basic_account_msg::GateBasicOrderMsg;
use crate::pre_trade::order_manager::OrderExecutionStatus;
use crate::trade_engine::query_parsers::compact_order::CompactOrderQueryResp;
use serde_json::Value;

fn parse_f64_value(v: Option<&Value>) -> Option<f64> {
    v.and_then(|val| {
        if let Some(n) = val.as_f64() {
            Some(n)
        } else if let Some(i) = val.as_i64() {
            Some(i as f64)
        } else if let Some(u) = val.as_u64() {
            Some(u as f64)
        } else if let Some(s) = val.as_str() {
            s.parse::<f64>().ok()
        } else {
            None
        }
    })
}

fn parse_i64_value(v: Option<&Value>) -> Option<i64> {
    v.and_then(|val| {
        if let Some(n) = val.as_i64() {
            Some(n)
        } else if let Some(u) = val.as_u64() {
            Some(u as i64)
        } else if let Some(s) = val.as_str() {
            s.parse::<i64>().ok()
        } else {
            None
        }
    })
}

fn parse_i64_ms(v: Option<&Value>) -> Option<i64> {
    let raw = parse_f64_value(v)?;
    if raw.abs() >= 1_000_000_000_000.0 {
        Some(raw as i64)
    } else {
        Some((raw * 1000.0).round() as i64)
    }
}

fn extract_result<'a>(root: &'a Value) -> Option<&'a Value> {
    if let Some(r) = root.get("data").and_then(|d| d.get("result")) {
        return Some(r);
    }
    root.get("result")
}

fn map_gate_status(
    status: &str,
    finish_as: &str,
    executed_qty: f64,
    left: Option<f64>,
    total: Option<f64>,
) -> u8 {
    let status = status.trim().to_ascii_lowercase();
    let finish = finish_as.trim().to_ascii_lowercase();

    if matches!(finish.as_str(), "filled" | "ioc" | "auto_deleveraging") {
        return OrderExecutionStatus::Filled.to_u8();
    }
    if matches!(
        finish.as_str(),
        "cancelled"
            | "canceled"
            | "liquidated"
            | "reduce_only"
            | "position_close"
            | "reduce_out"
            | "stp"
    ) {
        return OrderExecutionStatus::Cancelled.to_u8();
    }
    if matches!(status.as_str(), "rejected" | "failed") {
        return OrderExecutionStatus::Rejected.to_u8();
    }
    if matches!(status.as_str(), "open" | "new") {
        return OrderExecutionStatus::Create.to_u8();
    }
    if matches!(
        status.as_str(),
        "finished" | "closed" | "cancelled" | "canceled"
    ) {
        if let (Some(left), Some(total)) = (left, total) {
            if left <= 0.0 || (total > 0.0 && (executed_qty - total).abs() < 1e-9) {
                return OrderExecutionStatus::Filled.to_u8();
            }
        }
        if executed_qty > 0.0 {
            return OrderExecutionStatus::Filled.to_u8();
        }
        return OrderExecutionStatus::Cancelled.to_u8();
    }

    OrderExecutionStatus::Create.to_u8()
}

pub fn parse_gate_spot_order_status_json(json: &str) -> Option<CompactOrderQueryResp> {
    let root: Value = serde_json::from_str(json).ok()?;
    let result = extract_result(&root)?;

    let order_id =
        parse_i64_value(result.get("id")).or_else(|| parse_i64_value(result.get("order_id")))?;
    let filled_total = parse_f64_value(result.get("filled_total")).unwrap_or(0.0);
    let amount = parse_f64_value(result.get("amount")).unwrap_or(0.0);
    let left = parse_f64_value(result.get("left")).unwrap_or(0.0);
    let executed_qty = if filled_total > 0.0 {
        filled_total
    } else if amount > 0.0 {
        (amount - left).max(0.0)
    } else {
        0.0
    };

    let status = result.get("status").and_then(|v| v.as_str()).unwrap_or("");
    let finish_as = result
        .get("finish_as")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let status_u8 = map_gate_status(status, finish_as, executed_qty, Some(left), Some(amount));

    let update_time_ms = parse_i64_value(result.get("update_time_ms"))
        .or_else(|| parse_i64_ms(result.get("update_time")))
        .or_else(|| parse_i64_value(result.get("create_time_ms")))
        .or_else(|| parse_i64_ms(result.get("create_time")))
        .unwrap_or(0);

    let tif = result
        .get("time_in_force")
        .and_then(|v| v.as_str())
        .unwrap_or("gtc");
    let tif_u8 = GateBasicOrderMsg::time_in_force_to_u8(tif);

    Some(CompactOrderQueryResp {
        executed_qty,
        order_id,
        status_u8,
        update_time_ms,
        time_in_force_u8: tif_u8,
        response_price: 0.0,
    })
}

pub fn parse_gate_futures_order_status_json(json: &str) -> Option<CompactOrderQueryResp> {
    let root: Value = serde_json::from_str(json).ok()?;
    let result = extract_result(&root)?;

    let order_id =
        parse_i64_value(result.get("id")).or_else(|| parse_i64_value(result.get("order_id")))?;
    let size = parse_f64_value(result.get("size")).unwrap_or(0.0);
    let left = parse_f64_value(result.get("left")).unwrap_or(0.0);
    let executed_qty = (size.abs() - left.abs()).max(0.0);

    let status = result.get("status").and_then(|v| v.as_str()).unwrap_or("");
    let finish_as = result
        .get("finish_as")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let status_u8 = map_gate_status(
        status,
        finish_as,
        executed_qty,
        Some(left.abs()),
        Some(size.abs()),
    );

    let update_time_ms = parse_i64_ms(result.get("update_time"))
        .or_else(|| parse_i64_ms(result.get("finish_time")))
        .or_else(|| parse_i64_ms(result.get("create_time")))
        .unwrap_or(0);

    let tif = result.get("tif").and_then(|v| v.as_str()).unwrap_or("gtc");
    let tif_u8 = GateBasicOrderMsg::time_in_force_to_u8(tif);

    Some(CompactOrderQueryResp {
        executed_qty,
        order_id,
        status_u8,
        update_time_ms,
        time_in_force_u8: tif_u8,
        response_price: 0.0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::common::TimeInForce;

    #[test]
    fn parse_gate_spot_order_status() {
        let json = r#"{
          "request_id": "request-3",
          "header": {
            "response_time": "1681986205829",
            "status": "200",
            "channel": "spot.order_status",
            "event": "api"
          },
          "data": {
            "result": {
              "id": "1700664330",
              "text": "t-my-custom-id",
              "create_time": "1681986204",
              "update_time": "1681986204",
              "create_time_ms": 1681986204939,
              "update_time_ms": 1681986204939,
              "status": "open",
              "currency_pair": "GT_USDT",
              "type": "limit",
              "account": "spot",
              "side": "buy",
              "amount": "1",
              "price": "1",
              "time_in_force": "gtc",
              "left": "1",
              "fill_price": "0",
              "filled_total": "0",
              "finish_as": "open"
            }
          }
        }"#;

        let parsed = parse_gate_spot_order_status_json(json).expect("parse ok");
        assert_eq!(parsed.order_id, 1_700_664_330);
        assert_eq!(parsed.executed_qty, 0.0);
        assert_eq!(parsed.time_in_force_u8, TimeInForce::GTC.to_u8());
        assert_eq!(parsed.update_time_ms, 1681986204939);
        assert_eq!(parsed.status_u8, OrderExecutionStatus::Create.to_u8());
    }

    #[test]
    fn parse_gate_futures_order_status() {
        let json = r#"{
          "request_id": "request-id-2",
          "header": {
            "response_time": "1681196535985",
            "status": "200",
            "channel": "futures.order_status",
            "event": "api"
          },
          "data": {
            "result": {
              "id": 74046543,
              "create_time": 1681196535.01,
              "status": "open",
              "contract": "BTC_USDT",
              "size": "10",
              "price": "31403.2",
              "tif": "gtc",
              "left": "10",
              "fill_price": "0",
              "text": "t-my-custom-id"
            }
          }
        }"#;

        let parsed = parse_gate_futures_order_status_json(json).expect("parse ok");
        assert_eq!(parsed.order_id, 74_046_543);
        assert_eq!(parsed.executed_qty, 0.0);
        assert_eq!(parsed.time_in_force_u8, TimeInForce::GTC.to_u8());
        assert_eq!(parsed.status_u8, OrderExecutionStatus::Create.to_u8());
        assert!(parsed.update_time_ms > 0);
    }
}
