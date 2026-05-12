use crate::common::basic_account_msg::{BasicBalanceMsg, BasicBorrowInterestMsg};
use bytes::Bytes;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct BitgetAccountAssetsResponse {
    #[serde(default)]
    code: String,
    #[serde(default)]
    data: Option<BitgetAccountAssetsData>,
}

#[derive(Debug, Deserialize)]
struct BitgetAccountAssetsData {
    #[serde(default)]
    assets: Vec<BitgetAccountAssetRow>,
}

#[derive(Debug, Deserialize)]
struct BitgetAccountAssetRow {
    #[serde(default)]
    coin: String,
    #[serde(default)]
    equity: String,
    #[serde(default)]
    balance: String,
    #[serde(default)]
    debt: String,
    #[serde(default)]
    debts: String,
    #[serde(default)]
    borrow: String,
}

fn parse_f64(v: &str) -> Option<f64> {
    let s = v.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<f64>().ok()
}

pub fn parse_bitget_account_balance_snapshot(json: &str) -> Option<Vec<Bytes>> {
    let resp: BitgetAccountAssetsResponse = serde_json::from_str(json).ok()?;
    if resp.code != "00000" && resp.code != "0" {
        return None;
    }

    let ts = chrono::Utc::now().timestamp_millis();
    let mut out = Vec::new();
    for row in resp.data?.assets {
        let coin = row.coin.to_ascii_uppercase();
        if coin.is_empty() {
            continue;
        }

        let wallet = parse_f64(&row.balance)
            .or_else(|| parse_f64(&row.equity))
            .unwrap_or(0.0);
        out.push(BasicBalanceMsg::create(ts, coin.clone(), wallet).to_bytes());

        let has_liability_fields = !row.borrow.trim().is_empty()
            || !row.debt.trim().is_empty()
            || !row.debts.trim().is_empty();
        let borrowed = parse_f64(&row.borrow)
            .or_else(|| parse_f64(&row.debt))
            .or_else(|| parse_f64(&row.debts))
            .unwrap_or(0.0);
        let debt_total = parse_f64(&row.debts)
            .or_else(|| parse_f64(&row.debt))
            .unwrap_or(borrowed);
        let interest = (debt_total - borrowed).max(0.0);
        if borrowed > 0.0 || interest > 0.0 || has_liability_fields {
            out.push(BasicBorrowInterestMsg::create(ts, coin, borrowed, interest).to_bytes());
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::basic_account_msg::{
        BasicAccountEventType, BasicBalanceMsg, BasicBorrowInterestMsg,
    };

    #[test]
    fn parses_bitget_account_balance_snapshot() {
        let json = r#"{
            "code":"00000",
            "data":{
                "assets":[
                    {"coin":"USDT","equity":"950","balance":"1000","debt":"50"},
                    {"coin":"BTC","balance":"1.25","debt":"0"}
                ]
            }
        }"#;
        let msgs = parse_bitget_account_balance_snapshot(json).expect("parse ok");
        assert_eq!(msgs.len(), 4);

        let bal = BasicBalanceMsg::from_bytes(&msgs[0]).expect("balance");
        assert_eq!(
            bal.msg_type as u32,
            BasicAccountEventType::BalanceUpdate as u32
        );
        assert_eq!(bal.symbol, "USDT");
        assert!((bal.wallet - 1000.0).abs() < 1e-12);

        let borrow = BasicBorrowInterestMsg::from_bytes(&msgs[1]).expect("borrow");
        assert_eq!(
            borrow.msg_type as u32,
            BasicAccountEventType::BorrowInterest as u32
        );
        assert_eq!(borrow.symbol, "USDT");
        assert!((borrow.borrowed - 50.0).abs() < 1e-12);

        let btc_clear = BasicBorrowInterestMsg::from_bytes(&msgs[3]).expect("btc clear");
        assert_eq!(btc_clear.symbol, "BTC");
        assert_eq!(btc_clear.borrowed, 0.0);
        assert_eq!(btc_clear.interest, 0.0);
    }
}
