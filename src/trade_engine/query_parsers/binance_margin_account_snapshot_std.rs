use crate::common::basic_account_msg::{BasicBalanceMsg, BasicBorrowInterestMsg};
use bytes::Bytes;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct BinanceMarginAccountSnapshot {
    #[serde(default, rename = "userAssets")]
    user_assets: Vec<BinanceMarginAssetRow>,
}

#[derive(Debug, Deserialize)]
struct BinanceMarginAssetRow {
    #[serde(default)]
    asset: String,
    #[serde(default)]
    free: String,
    #[serde(default)]
    locked: String,
    #[serde(default)]
    borrowed: String,
    #[serde(default)]
    interest: String,
    #[serde(default, rename = "netAsset")]
    net_asset: String,
}

fn parse_f64(v: &str) -> f64 {
    v.parse::<f64>().unwrap_or(0.0)
}

pub fn parse_binance_margin_account_snapshot_std(json: &str) -> Option<Vec<Bytes>> {
    let snapshot: BinanceMarginAccountSnapshot = serde_json::from_str(json).ok()?;
    let mut out = Vec::new();
    let now_ms = chrono::Utc::now().timestamp_millis();

    for row in snapshot.user_assets {
        if row.asset.is_empty() {
            continue;
        }

        let free = parse_f64(&row.free);
        let locked = parse_f64(&row.locked);
        let borrowed = parse_f64(&row.borrowed);
        let interest = parse_f64(&row.interest);
        let net_asset = parse_f64(&row.net_asset);

        let total_asset = if free != 0.0 || locked != 0.0 {
            free + locked
        } else {
            net_asset + borrowed + interest
        };

        let asset = row.asset.to_ascii_uppercase();
        out.push(BasicBalanceMsg::create(now_ms, asset.clone(), total_asset).to_bytes());
        out.push(BasicBorrowInterestMsg::create(now_ms, asset, borrowed, interest).to_bytes());
    }

    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::basic_account_msg::{BasicBalanceMsg, BasicBorrowInterestMsg};

    #[test]
    fn parse_margin_snapshot_to_basic_msgs() {
        let json = r#"{
            "userAssets": [{
                "asset": "USDT",
                "free": "100.5",
                "locked": "1.5",
                "borrowed": "10.0",
                "interest": "0.5",
                "netAsset": "91.5"
            }]
        }"#;

        let msgs = parse_binance_margin_account_snapshot_std(json).expect("parse ok");
        assert_eq!(msgs.len(), 2);

        let bal = BasicBalanceMsg::from_bytes(&msgs[0]).expect("balance");
        assert_eq!(bal.symbol, "USDT");
        assert!((bal.balance - 102.0).abs() < 1e-12);

        let borrow = BasicBorrowInterestMsg::from_bytes(&msgs[1]).expect("borrow");
        assert_eq!(borrow.symbol, "USDT");
        assert!((borrow.borrowed - 10.0).abs() < 1e-12);
        assert!((borrow.interest - 0.5).abs() < 1e-12);
    }
}
