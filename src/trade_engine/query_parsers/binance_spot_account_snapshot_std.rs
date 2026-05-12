use crate::common::basic_account_msg::{BasicBalanceMsg, BasicBorrowInterestMsg};
use bytes::Bytes;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct BinanceSpotAccountSnapshot {
    #[serde(default)]
    balances: Vec<BinanceSpotAssetRow>,
}

#[derive(Debug, Deserialize)]
struct BinanceSpotAssetRow {
    #[serde(default)]
    asset: String,
    #[serde(default)]
    free: String,
    #[serde(default)]
    locked: String,
}

fn parse_f64(v: &str) -> f64 {
    v.parse::<f64>().unwrap_or(0.0)
}

pub fn parse_binance_spot_account_snapshot_std(json: &str) -> Option<Vec<Bytes>> {
    let snapshot: BinanceSpotAccountSnapshot = serde_json::from_str(json).ok()?;
    let mut out = Vec::new();
    let now_ms = chrono::Utc::now().timestamp_millis();

    for row in snapshot.balances {
        if row.asset.is_empty() {
            continue;
        }

        let total_asset = parse_f64(&row.free) + parse_f64(&row.locked);
        let asset = row.asset.to_ascii_uppercase();
        out.push(BasicBalanceMsg::create(now_ms, asset.clone(), total_asset).to_bytes());
        out.push(BasicBorrowInterestMsg::create(now_ms, asset, 0.0, 0.0).to_bytes());
    }

    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::basic_account_msg::{BasicBalanceMsg, BasicBorrowInterestMsg};

    #[test]
    fn parse_spot_snapshot_to_basic_msgs() {
        let json = r#"{
            "balances": [{
                "asset": "USDT",
                "free": "100.5",
                "locked": "1.5"
            }]
        }"#;

        let msgs = parse_binance_spot_account_snapshot_std(json).expect("parse ok");
        assert_eq!(msgs.len(), 2);

        let bal = BasicBalanceMsg::from_bytes(&msgs[0]).expect("balance");
        assert_eq!(bal.symbol, "USDT");
        assert!((bal.wallet - 102.0).abs() < 1e-12);

        let borrow = BasicBorrowInterestMsg::from_bytes(&msgs[1]).expect("borrow");
        assert_eq!(borrow.symbol, "USDT");
        assert_eq!(borrow.borrowed, 0.0);
        assert_eq!(borrow.interest, 0.0);
    }
}
