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

fn select_gate_response_price(
    status: &str,
    finish_as: &str,
    fill_price: f64,
    order_price: f64,
) -> f64 {
    let status = status.trim().to_ascii_lowercase();
    let finish = finish_as.trim().to_ascii_lowercase();

    // Gate 规则（按状态语义）：
    // - trade update: finish_as == "_update" 或 "filled" => 取 fill_price
    // - order update: 其余状态 => 取 order_price
    let use_fill_price =
        matches!(finish.as_str(), "_update" | "filled") || matches!(status.as_str(), "filled");

    let selected = if use_fill_price {
        fill_price
    } else {
        order_price
    };

    if selected.is_finite() && selected > 0.0 {
        selected
    } else {
        0.0
    }
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

    let order_price = parse_f64_value(result.get("price")).unwrap_or(0.0);
    let fill_price = parse_f64_value(result.get("fill_price"))
        .or_else(|| parse_f64_value(result.get("avg_deal_price")))
        .or_else(|| parse_f64_value(result.get("avg_price")))
        .unwrap_or(0.0);
    let response_price = select_gate_response_price(status, finish_as, fill_price, order_price);

    Some(CompactOrderQueryResp {
        executed_qty,
        order_id,
        status_u8,
        update_time_ms,
        time_in_force_u8: tif_u8,
        response_price,
    })
}

pub fn parse_gate_futures_order_status_json(json: &str) -> Option<CompactOrderQueryResp> {
    let root: Value = serde_json::from_str(json).ok()?;
    let result = extract_result(&root)?;

    let order_id =
        parse_i64_value(result.get("id")).or_else(|| parse_i64_value(result.get("order_id")))?;
    // Gate futures query: size/left 口径均为 contracts。
    // 这里保持 executed_qty 为 contracts；策略层再统一通过 qty_to_base 转换为 base qty。
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

    let order_price = parse_f64_value(result.get("price")).unwrap_or(0.0);
    let fill_price = parse_f64_value(result.get("fill_price"))
        .or_else(|| parse_f64_value(result.get("avg_price")))
        .unwrap_or(0.0);
    let response_price = select_gate_response_price(status, finish_as, fill_price, order_price);

    Some(CompactOrderQueryResp {
        executed_qty,
        order_id,
        status_u8,
        update_time_ms,
        time_in_force_u8: tif_u8,
        response_price,
    })
}
