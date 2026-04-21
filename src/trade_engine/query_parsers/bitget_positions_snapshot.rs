use crate::common::basic_account_msg::{BasicPositionMsg, BasicUmUnrealizedMsg};
use bytes::Bytes;
use serde_json::Value;

fn parse_i64_value(v: Option<&Value>) -> Option<i64> {
    let v = v?;
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    if let Some(n) = v.as_u64() {
        return Some(n as i64);
    }
    v.as_str()?.trim().parse::<i64>().ok()
}

fn parse_f32_value(v: Option<&Value>) -> Option<f32> {
    let v = v?;
    if let Some(n) = v.as_f64() {
        return Some(n as f32);
    }
    v.as_str()?.trim().parse::<f32>().ok()
}

fn side_char(raw: &str) -> char {
    match raw.to_ascii_lowercase().as_str() {
        "long" | "buy" => 'L',
        "short" | "sell" => 'S',
        _ => 'N',
    }
}

pub fn parse_bitget_positions_snapshot(json: &str) -> Option<Vec<Bytes>> {
    let value: Value = serde_json::from_str(json).ok()?;
    let code = value.get("code").and_then(|v| v.as_str()).unwrap_or_default();
    if code != "00000" && code != "0" {
        return None;
    }

    let rows = value
        .get("data")
        .and_then(|v| v.get("list"))
        .and_then(|v| v.as_array())
        .or_else(|| value.get("data").and_then(|v| v.as_array()))?;

    let now_ts = chrono::Utc::now().timestamp_millis();
    let mut out = Vec::new();
    for row in rows {
        let inst_id = row
            .get("symbol")
            .or_else(|| row.get("instId"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if inst_id.is_empty() {
            continue;
        }

        let side = row
            .get("holdSide")
            .or_else(|| row.get("posSide"))
            .or_else(|| row.get("side"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let pos_side = side_char(side);
        let size = parse_f32_value(
            row.get("total")
                .or_else(|| row.get("size"))
                .or_else(|| row.get("pos")),
        )
        .unwrap_or(0.0);
        let ts = parse_i64_value(
            row.get("uTime")
                .or_else(|| row.get("updatedTime"))
                .or_else(|| row.get("cTime"))
                .or_else(|| row.get("ts")),
        )
        .unwrap_or(now_ts);

        out.push(BasicPositionMsg::create(ts, inst_id.clone(), pos_side, size).to_bytes());

        if let Some(pnl) = parse_f32_value(
            row.get("unrealizedPL")
                .or_else(|| row.get("unrealizedPnl"))
                .or_else(|| row.get("unrealisedPnl"))
                .or_else(|| row.get("upl")),
        ) {
            out.push(BasicUmUnrealizedMsg::create(ts, inst_id, pos_side, pnl as f64).to_bytes());
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::basic_account_msg::{
        BasicAccountEventType, BasicPositionMsg, BasicUmUnrealizedMsg,
    };

    #[test]
    fn parses_bitget_positions_snapshot() {
        let json = r#"{
            "code":"00000",
            "data":{
                "list":[
                    {
                        "symbol":"BTCUSDT",
                        "holdSide":"long",
                        "total":"2",
                        "uTime":"1724742632153",
                        "unrealizedPL":"-12.25"
                    }
                ]
            }
        }"#;
        let msgs = parse_bitget_positions_snapshot(json).expect("parse ok");
        assert_eq!(msgs.len(), 2);

        let pos = BasicPositionMsg::from_bytes(&msgs[0]).expect("pos");
        assert_eq!(
            pos.msg_type as u32,
            BasicAccountEventType::PositionUpdate as u32
        );
        assert_eq!(pos.inst_id, "BTCUSDT");
        assert_eq!(pos.position_side, 'L');

        let pnl = BasicUmUnrealizedMsg::from_bytes(&msgs[1]).expect("pnl");
        assert_eq!(
            pnl.msg_type as u32,
            BasicAccountEventType::UnrealizedPnlUpdate as u32
        );
        assert_eq!(pnl.inst_id, "BTCUSDT");
        assert_eq!(pnl.position_side, 'L');
        assert!((pnl.unrealized_pnl + 12.25).abs() < 1e-9);
    }

    #[test]
    fn parses_empty_bitget_positions_snapshot() {
        let json = r#"{"code":"00000","data":{"list":[]}}"#;
        let msgs = parse_bitget_positions_snapshot(json).expect("parse ok");
        assert!(msgs.is_empty());
    }
}
