use crate::common::basic_account_msg::{BasicPositionMsg, BasicUmUnrealizedMsg};
use bytes::Bytes;
use serde_json::Value;

fn parse_f32_value(v: &Value) -> Option<f32> {
    if let Some(n) = v.as_f64() {
        return Some(n as f32);
    }
    if let Some(s) = v.as_str() {
        if s.trim().is_empty() {
            return None;
        }
        return s.parse::<f32>().ok();
    }
    None
}

fn parse_i64_value(v: &Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    if let Some(s) = v.as_str() {
        if s.trim().is_empty() {
            return None;
        }
        return s.parse::<i64>().ok();
    }
    None
}

fn normalize_contract(raw: &str) -> String {
    let mut inst = raw.trim().to_ascii_uppercase();
    inst = inst.replace('_', "").replace('-', "");
    if let Some(stripped) = inst.strip_suffix("SWAP") {
        return stripped.to_string();
    }
    inst
}

fn side_from_str(raw: &str) -> Option<char> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "long" | "buy" => Some('L'),
        "short" | "sell" => Some('S'),
        "net" | "both" => Some('N'),
        _ => None,
    }
}

fn extract_rows(value: &Value) -> Option<&Vec<Value>> {
    if let Some(arr) = value.as_array() {
        return Some(arr);
    }
    if let Some(arr) = value.get("data").and_then(|v| v.as_array()) {
        return Some(arr);
    }
    if let Some(arr) = value.get("result").and_then(|v| v.as_array()) {
        return Some(arr);
    }
    if let Some(arr) = value.get("positions").and_then(|v| v.as_array()) {
        return Some(arr);
    }
    if let Some(arr) = value
        .get("data")
        .and_then(|v| v.get("positions"))
        .and_then(|v| v.as_array())
    {
        return Some(arr);
    }
    if let Some(arr) = value
        .get("result")
        .and_then(|v| v.get("positions"))
        .and_then(|v| v.as_array())
    {
        return Some(arr);
    }
    None
}

fn find_str<'a>(row: &'a Value, keys: &[&str]) -> Option<&'a str> {
    for key in keys {
        if let Some(s) = row.get(*key).and_then(|v| v.as_str()) {
            if !s.trim().is_empty() {
                return Some(s);
            }
        }
    }
    None
}

fn find_time_ms(row: &Value) -> Option<i64> {
    let keys = [
        "update_time_ms",
        "updateTimeMs",
        "update_time",
        "updateTime",
        "u_time",
        "uTime",
        "time_ms",
        "time",
    ];
    for key in keys {
        if let Some(v) = row.get(key).and_then(parse_i64_value) {
            if v <= 0 {
                continue;
            }
            if v < 1_000_000_000_000 {
                return Some(v * 1000);
            }
            return Some(v);
        }
    }
    None
}

fn find_size(row: &Value) -> Option<f32> {
    let keys = [
        "size",
        "position",
        "pos",
        "qty",
        "amount",
        "position_size",
        "positionSize",
    ];
    for key in keys {
        if let Some(v) = row.get(key).and_then(parse_f32_value) {
            return Some(v);
        }
    }
    None
}

fn find_side(row: &Value) -> Option<char> {
    let keys = ["side", "position_side", "pos_side", "direction", "posSide"];
    for key in keys {
        if let Some(s) = row.get(key).and_then(|v| v.as_str()) {
            if let Some(side) = side_from_str(s) {
                return Some(side);
            }
        }
    }
    None
}

fn find_unrealized_pnl(row: &Value) -> Option<f64> {
    let keys = ["unrealised_pnl", "unrealized_pnl", "upl", "unrealisedPnl", "unrealizedPnl"];
    for key in keys {
        if let Some(v) = row.get(key).and_then(parse_f32_value) {
            return Some(v as f64);
        }
    }
    None
}

pub struct GatePositionsSnapshotParse {
    pub msgs: Vec<Bytes>,
    pub rows_total: usize,
    pub rows_with_inst: usize,
    pub rows_with_nonzero_size: usize,
    pub rows_with_pnl: usize,
}

pub fn parse_gate_positions_snapshot_with_meta(
    json: &str,
) -> Option<GatePositionsSnapshotParse> {
    let value: Value = serde_json::from_str(json).ok()?;
    let rows = extract_rows(&value)?;
    let now_ts = chrono::Utc::now().timestamp_millis();
    let mut out = Vec::new();
    let mut rows_with_inst = 0;
    let mut rows_with_nonzero_size = 0;
    let mut rows_with_pnl = 0;

    for row in rows {
        let Some(inst_raw) =
            find_str(row, &["contract", "symbol", "inst_id", "instId", "currency_pair"])
        else {
            continue;
        };
        rows_with_inst += 1;
        let size = find_size(row).unwrap_or(0.0);
        let ts = find_time_ms(row).unwrap_or(now_ts);
        let inst_id = normalize_contract(inst_raw);
        if inst_id.is_empty() {
            continue;
        }

        let side = find_side(row);
        let (side_char, amount) = match side {
            Some('N') => ('N', size),
            Some('L') => ('L', size.abs()),
            Some('S') => ('S', size.abs()),
            _ => {
                if size > 0.0 {
                    ('L', size.abs())
                } else if size < 0.0 {
                    ('S', size.abs())
                } else {
                    ('N', 0.0)
                }
            }
        };

        if size != 0.0 {
            rows_with_nonzero_size += 1;
            out.push(BasicPositionMsg::create(ts, inst_id.clone(), side_char, amount).to_bytes());
        }

        if let Some(pnl) = find_unrealized_pnl(row) {
            rows_with_pnl += 1;
            out.push(BasicUmUnrealizedMsg::create(ts, inst_id, side_char, pnl).to_bytes());
        }
    }

    Some(GatePositionsSnapshotParse {
        msgs: out,
        rows_total: rows.len(),
        rows_with_inst,
        rows_with_nonzero_size,
        rows_with_pnl,
    })
}

pub fn parse_gate_positions_snapshot(json: &str) -> Option<Vec<Bytes>> {
    parse_gate_positions_snapshot_with_meta(json).map(|parsed| parsed.msgs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::basic_account_msg::{
        get_basic_event_type, BasicAccountEventType, BasicPositionMsg, BasicUmUnrealizedMsg,
    };

    #[test]
    fn parses_gate_positions_array() {
        let json = r#"[{
            "contract": "BTC_USDT",
            "size": "-2",
            "unrealised_pnl": "-0.0005",
            "update_time": 1716796364
        }]"#;
        let msgs = parse_gate_positions_snapshot(json).expect("parse ok");
        assert_eq!(msgs.len(), 2);
        let mut pos_msg = None;
        let mut pnl_msg = None;
        for msg in msgs {
            match get_basic_event_type(&msg) {
                BasicAccountEventType::PositionUpdate => {
                    pos_msg = Some(BasicPositionMsg::from_bytes(&msg).expect("pos ok"));
                }
                BasicAccountEventType::UnrealizedPnlUpdate => {
                    pnl_msg = Some(BasicUmUnrealizedMsg::from_bytes(&msg).expect("pnl ok"));
                }
                _ => {}
            }
        }
        let pos = pos_msg.expect("pos msg");
        assert_eq!(pos.inst_id, "BTCUSDT");
        assert_eq!(pos.position_side, 'S');
        assert!((pos.position_amount - 2.0).abs() < 1e-6);
        assert_eq!(pos.timestamp, 1716796364000);

        let pnl = pnl_msg.expect("pnl msg");
        assert_eq!(pnl.inst_id, "BTCUSDT");
        assert_eq!(pnl.position_side, 'S');
        assert!((pnl.unrealized_pnl + 0.0005).abs() < 1e-9);
    }

    #[test]
    fn parses_gate_positions_data_object() {
        let json = r#"{
            "data": [{
                "symbol": "ETH_USDT",
                "size": 1.5,
                "update_time_ms": 1716796364000,
                "side": "long"
            }]
        }"#;
        let msgs = parse_gate_positions_snapshot(json).expect("parse ok");
        assert_eq!(msgs.len(), 1);
        let pos = BasicPositionMsg::from_bytes(&msgs[0]).expect("pos ok");
        assert_eq!(pos.inst_id, "ETHUSDT");
        assert_eq!(pos.position_side, 'L');
        assert!((pos.position_amount - 1.5).abs() < 1e-6);
        assert_eq!(pos.timestamp, 1716796364000);
    }
}
