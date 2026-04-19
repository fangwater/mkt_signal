use crate::common::basic_account_msg::{BasicBalanceMsg, BasicBorrowInterestMsg};
use bytes::Bytes;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct BybitWalletBalanceResponse {
    #[serde(default, rename = "retCode")]
    ret_code: i32,
    #[serde(default)]
    result: Option<BybitWalletBalanceResult>,
}

#[derive(Debug, Deserialize)]
struct BybitWalletBalanceResult {
    #[serde(default)]
    list: Vec<BybitWalletBalanceAccount>,
}

#[derive(Debug, Deserialize)]
struct BybitWalletBalanceAccount {
    #[serde(default)]
    coin: Vec<BybitWalletBalanceCoin>,
}

#[derive(Debug, Deserialize)]
struct BybitWalletBalanceCoin {
    #[serde(default)]
    coin: String,
    #[serde(default, rename = "walletBalance")]
    wallet_balance: String,
    #[serde(default, rename = "borrowAmount")]
    borrow_amount: String,
    #[serde(default, rename = "spotBorrow")]
    spot_borrow: String,
    #[serde(default, rename = "accruedInterest")]
    accrued_interest: String,
    #[serde(default, rename = "borrowInterest")]
    borrow_interest: String,
}

fn parse_f64(v: &str) -> Option<f64> {
    let s = v.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<f64>().ok()
}

pub fn parse_bybit_account_balance_snapshot(json: &str) -> Option<Vec<Bytes>> {
    let resp: BybitWalletBalanceResponse = serde_json::from_str(json).ok()?;
    if resp.ret_code != 0 {
        return None;
    }
    let account = resp.result?.list.into_iter().next()?;
    let ts = chrono::Utc::now().timestamp_millis();

    let mut out = Vec::new();
    for coin in account.coin {
        let symbol = coin.coin.to_ascii_uppercase();
        if symbol.is_empty() {
            continue;
        }

        let balance = parse_f64(&coin.wallet_balance).unwrap_or(0.0);
        out.push(BasicBalanceMsg::create(ts, symbol.clone(), balance).to_bytes());

        let borrowed = parse_f64(&coin.borrow_amount)
            .or_else(|| parse_f64(&coin.spot_borrow))
            .unwrap_or(0.0);
        let interest = parse_f64(&coin.accrued_interest)
            .or_else(|| parse_f64(&coin.borrow_interest))
            .unwrap_or(0.0);
        if borrowed > 0.0 || interest > 0.0 {
            out.push(BasicBorrowInterestMsg::create(ts, symbol, borrowed, interest).to_bytes());
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
    fn parses_bybit_wallet_balance_snapshot() {
        let json = r#"{
            "retCode":0,
            "result":{"list":[{"coin":[
                {"coin":"USDT","walletBalance":"1000","borrowAmount":"50","accruedInterest":"2"},
                {"coin":"BTC","walletBalance":"1.25","borrowAmount":"0","accruedInterest":"0"}
            ]}]}
        }"#;
        let msgs = parse_bybit_account_balance_snapshot(json).expect("parse ok");
        assert_eq!(msgs.len(), 3);
        let bal = BasicBalanceMsg::from_bytes(&msgs[0]).expect("bal ok");
        assert_eq!(
            bal.msg_type as u32,
            BasicAccountEventType::BalanceUpdate as u32
        );
        let borrow = BasicBorrowInterestMsg::from_bytes(&msgs[1]).expect("borrow ok");
        assert_eq!(
            borrow.msg_type as u32,
            BasicAccountEventType::BorrowInterest as u32
        );
    }
}
