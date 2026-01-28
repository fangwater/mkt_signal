use crate::common::basic_account_msg::BasicBalanceMsg;
use bytes::Bytes;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct BinanceUmBalanceRow {
    #[serde(default)]
    asset: String,
    #[serde(default, rename = "crossWalletBalance")]
    cross_wallet_balance: String,
    #[serde(default, rename = "balance")]
    balance: String,
    #[serde(default, rename = "updateTime")]
    update_time: i64,
}

fn parse_f64(v: &str) -> f64 {
    v.parse::<f64>().unwrap_or(0.0)
}

pub fn parse_binance_um_balance_snapshot_std(json: &str) -> Option<Vec<Bytes>> {
    let rows: Vec<BinanceUmBalanceRow> = serde_json::from_str(json).ok()?;
    let mut out = Vec::new();
    let now_ms = chrono::Utc::now().timestamp_millis();
    for row in rows {
        if row.asset.is_empty() {
            continue;
        }
        let ts = if row.update_time > 0 {
            row.update_time
        } else {
            now_ms
        };
        let asset = row.asset.to_ascii_uppercase();
        let balance = if !row.cross_wallet_balance.trim().is_empty() {
            parse_f64(&row.cross_wallet_balance)
        } else {
            parse_f64(&row.balance)
        };
        out.push(BasicBalanceMsg::create(ts, asset, balance).to_bytes());
    }
    Some(out)
}

