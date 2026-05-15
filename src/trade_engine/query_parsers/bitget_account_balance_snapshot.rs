use crate::common::basic_account_msg::{
    BasicAccountRiskMsg, BasicBalanceMsg, BasicBorrowInterestMsg,
};
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
    #[serde(default, rename = "accountEquity")]
    account_equity: String,
    #[serde(default, rename = "totalEquity")]
    total_equity: String,
    #[serde(default, rename = "usdtEquity")]
    usdt_equity: String,
    #[serde(default, rename = "effEquity")]
    eff_equity: String,
    #[serde(default)]
    mmr: String,
    #[serde(default)]
    imr: String,
    #[serde(default, rename = "mgnRatio")]
    mgn_ratio: String,
    #[serde(default, rename = "totalLiabilities")]
    total_liabilities: String,
    #[serde(default, rename = "notionalUsd")]
    notional_usd: String,
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
    locked: String,
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

    let data = resp.data?;
    let ts = chrono::Utc::now().timestamp_millis();
    let mut out = Vec::new();
    let has_risk_fields = [
        &data.account_equity,
        &data.total_equity,
        &data.usdt_equity,
        &data.eff_equity,
        &data.mmr,
        &data.imr,
        &data.mgn_ratio,
        &data.total_liabilities,
        &data.notional_usd,
    ]
    .iter()
    .any(|value| !value.trim().is_empty());
    if has_risk_fields {
        let actual_equity_usd = parse_f64(&data.total_equity)
            .or_else(|| parse_f64(&data.account_equity))
            .or_else(|| parse_f64(&data.usdt_equity))
            .unwrap_or(0.0);
        let adj_equity_usd = parse_f64(&data.eff_equity).unwrap_or(actual_equity_usd);
        let maintenance_margin_usd = parse_f64(&data.mmr).unwrap_or(0.0);
        let initial_margin_usd = parse_f64(&data.imr).unwrap_or(0.0);
        let margin_ratio = if maintenance_margin_usd.abs() > f64::EPSILON {
            adj_equity_usd / maintenance_margin_usd
        } else {
            0.0
        };
        out.push(
            BasicAccountRiskMsg::create(
                ts,
                adj_equity_usd,
                actual_equity_usd,
                maintenance_margin_usd,
                initial_margin_usd,
                margin_ratio,
                parse_f64(&data.total_liabilities).unwrap_or(0.0).abs(),
                parse_f64(&data.notional_usd).unwrap_or(0.0),
            )
            .to_bytes(),
        );
    }

    for row in data.assets {
        let coin = row.coin.to_ascii_uppercase();
        if coin.is_empty() {
            continue;
        }

        let wallet = parse_f64(&row.balance)
            .map(|balance| balance + parse_f64(&row.locked).unwrap_or(0.0))
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
        get_basic_event_type, BasicAccountEventType, BasicAccountRiskMsg, BasicBalanceMsg,
        BasicBorrowInterestMsg,
    };

    #[test]
    fn parses_bitget_account_balance_snapshot() {
        let json = r#"{
            "code":"00000",
            "data":{
                "assets":[
                    {"coin":"USDT","equity":"950","balance":"1000","locked":"25","debt":"50"},
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
        assert!((bal.wallet - 1025.0).abs() < 1e-12);

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

    #[test]
    fn emits_bitget_account_risk_from_top_level_snapshot() {
        let json = r#"{
            "code":"00000",
            "data":{
                "accountEquity":"100028.20011143",
                "usdtEquity":"100054.66537698",
                "effEquity":"96513.01171034",
                "mmr":"186.3",
                "imr":"385.88",
                "mgnRatio":"0.0019",
                "assets":[
                    {"coin":"USDT","equity":"91264.36927005","balance":"91057.40610131","debt":"0"}
                ]
            }
        }"#;
        let msgs = parse_bitget_account_balance_snapshot(json).expect("parse ok");
        assert_eq!(msgs.len(), 3);
        assert_eq!(
            get_basic_event_type(&msgs[0]),
            BasicAccountEventType::AccountRisk
        );
        let risk = BasicAccountRiskMsg::from_bytes(&msgs[0]).expect("risk");
        assert!((risk.actual_equity_usd - 100028.20011143).abs() < 1e-9);
        assert!((risk.adj_equity_usd - 96513.01171034).abs() < 1e-9);
        assert!((risk.maintenance_margin_usd - 186.3).abs() < 1e-12);
        assert!((risk.initial_margin_usd - 385.88).abs() < 1e-12);
        assert!(risk.margin_ratio > 500.0);
    }

    #[test]
    fn adds_locked_to_bitget_balance_snapshot_wallet() {
        let json = r#"{
            "code":"00000",
            "data":{
                "assets":[
                    {"coin":"USDT","equity":"120","balance":"100","locked":"15.5","debt":"0"}
                ]
            }
        }"#;
        let msgs = parse_bitget_account_balance_snapshot(json).expect("parse ok");
        let bal = BasicBalanceMsg::from_bytes(&msgs[0]).expect("balance");
        assert_eq!(bal.symbol, "USDT");
        assert!((bal.wallet - 115.5).abs() < 1e-12);
    }
}
