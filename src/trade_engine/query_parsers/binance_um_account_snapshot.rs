use crate::common::basic_account_msg::BasicPositionMsg;
use bytes::Bytes;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct RawUmAccountResponse {
    #[serde(default)]
    positions: Vec<RawUmPosition>,
}

#[derive(Debug, Deserialize)]
struct RawUmPosition {
    #[serde(default)]
    symbol: String,
    #[serde(default, rename = "positionSide")]
    position_side: String,
    #[serde(default, rename = "positionAmt")]
    position_amt: String,
    #[serde(default, rename = "updateTime")]
    update_time: i64,
}

fn parse_f32(v: &str) -> f32 {
    v.parse::<f32>().unwrap_or(0.0)
}

fn side_to_char(side: &str) -> char {
    match side {
        "LONG" => 'L',
        "SHORT" => 'S',
        _ => 'N',
    }
}

pub fn parse_binance_um_account_snapshot(json: &str) -> Option<Vec<Bytes>> {
    let raw: RawUmAccountResponse = serde_json::from_str(json).ok()?;
    let mut out = Vec::new();
    for pos in raw.positions {
        if pos.symbol.is_empty() {
            continue;
        }
        let amount = parse_f32(&pos.position_amt);
        if amount == 0.0 {
            continue;
        }
        out.push(
            BasicPositionMsg::create(
                pos.update_time,
                pos.symbol.to_ascii_uppercase(),
                side_to_char(&pos.position_side),
                amount,
            )
            .to_bytes(),
        );
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::basic_account_msg::BasicPositionMsg;

    #[test]
    fn parse_um_snapshot_positions() {
        let json = r#"{
            "positions": [{
                "symbol": "BTCUSDT",
                "positionSide": "LONG",
                "positionAmt": "0.5",
                "updateTime": 1700000000001
            }]
        }"#;
        let msgs = parse_binance_um_account_snapshot(json).expect("parse ok");
        assert_eq!(msgs.len(), 1);
        let p = BasicPositionMsg::from_bytes(&msgs[0]).expect("pos ok");
        assert_eq!(p.inst_id, "BTCUSDT");
        assert_eq!(p.position_side, 'L');
        assert!((p.position_amount - 0.5).abs() < 1e-6);
    }
}

