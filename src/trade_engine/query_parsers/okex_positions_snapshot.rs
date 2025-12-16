use crate::common::basic_account_msg::BasicPositionMsg;
use bytes::Bytes;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct OkexPositionsResponse {
    #[serde(default)]
    data: Vec<OkexPositionRow>,
}

#[derive(Debug, Deserialize)]
struct OkexPositionRow {
    #[serde(default, rename = "instId")]
    inst_id: String,
    #[serde(default, rename = "pos")]
    pos: String,
    #[serde(default, rename = "posSide")]
    pos_side: String,
    #[serde(default, rename = "uTime")]
    u_time: String,
    #[serde(default, rename = "instType")]
    inst_type: String,
}

fn parse_i64(v: &str) -> Option<i64> {
    let s = v.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<i64>().ok()
}

fn parse_f32(v: &str) -> Option<f32> {
    let s = v.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<f32>().ok()
}

fn pos_side_char(s: &str) -> char {
    match s.to_ascii_lowercase().as_str() {
        "long" => 'L',
        "short" => 'S',
        "net" => 'N',
        _ => 'N',
    }
}

pub fn parse_okex_positions_snapshot(json: &str) -> Option<Vec<Bytes>> {
    let resp: OkexPositionsResponse = serde_json::from_str(json).ok()?;
    let mut out = Vec::new();
    for row in resp.data {
        if row.inst_id.is_empty() {
            continue;
        }
        // Pre-trade only needs futures/swap positions for leverage/exposure. Filter out obvious non-derivatives.
        if !row.inst_type.is_empty()
            && !row.inst_type.eq_ignore_ascii_case("SWAP")
            && !row.inst_type.eq_ignore_ascii_case("FUTURES")
        {
            continue;
        }
        let Some(pos) = parse_f32(&row.pos) else {
            continue;
        };
        let ts = parse_i64(&row.u_time).unwrap_or(chrono::Utc::now().timestamp_millis());
        out.push(
            BasicPositionMsg::create(ts, row.inst_id, pos_side_char(&row.pos_side), pos).to_bytes(),
        );
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::basic_account_msg::BasicAccountEventType;

    #[test]
    fn parses_okex_positions_snapshot_to_basic_position_msgs() {
        let json = r#"{
            "code": "0",
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "instType": "SWAP",
                "pos": "2",
                "posSide": "net",
                "uTime": "1724742632153"
            }],
            "msg": ""
        }"#;
        let msgs = parse_okex_positions_snapshot(json).expect("parse ok");
        assert_eq!(msgs.len(), 1);
        let pos = BasicPositionMsg::from_bytes(&msgs[0]).expect("pos ok");
        assert_eq!(
            pos.msg_type as u32,
            BasicAccountEventType::PositionUpdate as u32
        );
        assert_eq!(pos.inst_id, "BTC-USDT-SWAP");
        assert_eq!(pos.position_side, 'N');
        assert!((pos.position_amount - 2.0).abs() < 1e-6);
    }
}
