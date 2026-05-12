use crate::common::basic_account_msg::{BasicBalanceMsg, BasicBorrowInterestMsg};
use bytes::Bytes;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct OkexBalanceResponse {
    #[serde(default)]
    data: Vec<OkexBalanceAccount>,
}

#[derive(Debug, Deserialize)]
struct OkexBalanceAccount {
    #[serde(default)]
    details: Vec<OkexBalanceDetail>,
    #[serde(default, rename = "uTime")]
    u_time: String,
}

#[derive(Debug, Deserialize)]
struct OkexBalanceDetail {
    #[serde(default)]
    ccy: String,
    // OKX eq 是净权益口径；wallet 由 eq + liab 推导。
    #[serde(default)]
    eq: String,
    #[serde(default)]
    liab: String,
    #[serde(default)]
    interest: String,
    #[serde(default, rename = "uTime")]
    u_time: String,
}

fn parse_i64(v: &str) -> Option<i64> {
    let s = v.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<i64>().ok()
}

fn parse_f64(v: &str) -> Option<f64> {
    let s = v.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<f64>().ok()
}

pub fn parse_okex_account_balance_snapshot(json: &str) -> Option<Vec<Bytes>> {
    let resp: OkexBalanceResponse = serde_json::from_str(json).ok()?;
    let account = resp.data.first()?;
    let fallback_ts = parse_i64(&account.u_time).unwrap_or(chrono::Utc::now().timestamp_millis());

    let mut out = Vec::new();
    for d in &account.details {
        if d.ccy.is_empty() {
            continue;
        }
        let Some(eq) = parse_f64(&d.eq) else {
            continue;
        };
        let ts = parse_i64(&d.u_time).unwrap_or(fallback_ts);
        let symbol = d.ccy.to_ascii_uppercase();
        let liab = parse_f64(&d.liab).map(f64::abs).unwrap_or(0.0);
        let interest = parse_f64(&d.interest).map(f64::abs).unwrap_or(0.0);
        let wallet = eq + liab;
        out.push(BasicBalanceMsg::create(ts, symbol.clone(), wallet).to_bytes());

        let has_liability_fields = !d.liab.trim().is_empty() || !d.interest.trim().is_empty();
        let principal = (liab - interest).max(0.0);
        if principal > 0.0 || interest > 0.0 || has_liability_fields {
            out.push(BasicBorrowInterestMsg::create(ts, symbol, principal, interest).to_bytes());
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::basic_account_msg::{
        get_basic_event_type, BasicAccountEventType, BasicBalanceMsg, BasicBorrowInterestMsg,
    };

    #[test]
    fn parses_okex_balance_snapshot_to_basic_balance_msgs() {
        let json = r#"{
            "code": "0",
            "data": [{
                "uTime": "1705474164160",
                "details": [{
                    "ccy": "USDT",
                    "cashBal": "4992.890093622894",
                    "eq": "4990.5",
                    "uTime": "1705449605015"
                }]
            }],
            "msg": ""
        }"#;
        let msgs = parse_okex_account_balance_snapshot(json).expect("parse ok");
        assert_eq!(msgs.len(), 1);
        let bal = BasicBalanceMsg::from_bytes(&msgs[0]).expect("bal ok");
        assert_eq!(
            bal.msg_type as u32,
            BasicAccountEventType::BalanceUpdate as u32
        );
        assert_eq!(bal.symbol, "USDT");
        assert!((bal.wallet - 4990.5).abs() < 1e-10);
    }

    #[test]
    fn okex_usdt_balance_snapshot_uses_gross_wallet() {
        let json = r#"{
            "code": "0",
            "data": [{
                "uTime": "1705474164160",
                "details": [{
                    "ccy": "USDT",
                    "cashBal": "56243.85926211993",
                    "eq": "53443.166831427916",
                    "upl": "-2800.6924306920096",
                    "uTime": "1705449605015"
                }]
            }],
            "msg": ""
        }"#;
        let msgs = parse_okex_account_balance_snapshot(json).expect("parse ok");
        assert_eq!(msgs.len(), 1);
        let bal = BasicBalanceMsg::from_bytes(&msgs[0]).expect("bal ok");
        assert_eq!(bal.symbol, "USDT");
        assert!((bal.wallet - 53443.166831427916).abs() < 1e-10);
    }

    #[test]
    fn emits_borrow_interest_when_liab_and_interest_present() {
        let json = r#"{
            "code": "0",
            "data": [{
                "uTime": "1705474164160",
                "details": [
                    {
                        "ccy": "USDT",
                        "cashBal": "1000",
                        "eq": "950",
                        "liab": "-50.5",
                        "interest": "-0.5",
                        "uTime": "1705449605015"
                    },
                    {
                        "ccy": "BTC",
                        "cashBal": "1.25",
                        "eq": "1.25",
                        "liab": "0",
                        "interest": "0",
                        "uTime": "1705449605015"
                    }
                ]
            }],
            "msg": ""
        }"#;
        let msgs = parse_okex_account_balance_snapshot(json).expect("parse ok");
        // USDT: wallet + borrow; BTC: wallet + zero-borrow clear.
        assert_eq!(msgs.len(), 4);

        let mut usdt_wallet = None;
        let mut usdt_borrow = None;
        let mut btc_borrow = None;
        for msg in &msgs {
            match get_basic_event_type(msg) {
                BasicAccountEventType::BalanceUpdate => {
                    let bal = BasicBalanceMsg::from_bytes(msg).expect("bal ok");
                    if bal.symbol == "USDT" {
                        usdt_wallet = Some(bal.wallet);
                    }
                }
                BasicAccountEventType::BorrowInterest => {
                    let bi = BasicBorrowInterestMsg::from_bytes(msg).expect("bi ok");
                    if bi.symbol == "USDT" {
                        usdt_borrow = Some((bi.borrowed, bi.interest));
                    } else if bi.symbol == "BTC" {
                        btc_borrow = Some((bi.borrowed, bi.interest));
                    }
                }
                _ => {}
            }
        }
        assert!((usdt_wallet.expect("usdt wallet") - 1000.5).abs() < 1e-9);
        let (principal, interest) = usdt_borrow.expect("usdt borrow event");
        assert!((principal - 50.0).abs() < 1e-9);
        assert!((interest - 0.5).abs() < 1e-9);
        assert_eq!(btc_borrow, Some((0.0, 0.0)));
    }
}
