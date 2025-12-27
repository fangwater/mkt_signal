use crate::common::basic_account_msg::{BasicPositionMsg, BasicUmUnrealizedMsg};
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
    #[serde(default, rename = "unrealizedProfit", alias = "unRealizedProfit")]
    unrealized_profit: String,
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
        let inst_id = pos.symbol.to_ascii_uppercase();
        let side = side_to_char(&pos.position_side);
        if amount != 0.0 {
            out.push(
                BasicPositionMsg::create(pos.update_time, inst_id.clone(), side, amount)
                    .to_bytes(),
            );
        }
        if !pos.unrealized_profit.trim().is_empty() {
            if let Ok(pnl) = pos.unrealized_profit.parse::<f64>() {
                if pnl.abs() > 0.0 {
                    out.push(
                        BasicUmUnrealizedMsg::create(pos.update_time, inst_id, side, pnl)
                            .to_bytes(),
                    );
                }
            }
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::basic_account_msg::{BasicPositionMsg, BasicUmUnrealizedMsg};

    #[test]
    fn parse_um_snapshot_positions() {
        let json = r#"{
            "positions": [{
                "symbol": "BTCUSDT",
                "positionSide": "LONG",
                "positionAmt": "0.5",
                "unRealizedProfit": "12.5",
                "updateTime": 1700000000001
            }]
        }"#;
        let msgs = parse_binance_um_account_snapshot(json).expect("parse ok");
        assert_eq!(msgs.len(), 2);
        let p = BasicPositionMsg::from_bytes(&msgs[0]).expect("pos ok");
        assert_eq!(p.inst_id, "BTCUSDT");
        assert_eq!(p.position_side, 'L');
        assert!((p.position_amount - 0.5).abs() < 1e-6);
        let pnl = BasicUmUnrealizedMsg::from_bytes(&msgs[1]).expect("pnl ok");
        assert_eq!(pnl.inst_id, "BTCUSDT");
        assert_eq!(pnl.position_side, 'L');
        assert!((pnl.unrealized_pnl - 12.5).abs() < 1e-9);
    }
}
