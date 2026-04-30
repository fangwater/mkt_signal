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
    let account = resp.data.get(0)?;
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
        out.push(BasicBalanceMsg::create(ts, symbol.clone(), eq).to_bytes());

        // OKX 跨保证金负债：liab 是当前借入本金 + 利息合计（绝对值），interest 单独给出已计利息。
        // 仅 liab>0 或 interest>0 时才发 borrow 事件，避免无负债币种刷无效消息。
        let liab = parse_f64(&d.liab).map(f64::abs).unwrap_or(0.0);
        let interest = parse_f64(&d.interest).map(f64::abs).unwrap_or(0.0);
        if liab > 0.0 || interest > 0.0 {
            let principal = (liab - interest).max(0.0);
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
                    "eq": "4992.890093622894",
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
        assert!((bal.balance - 4992.890093622894).abs() < 1e-10);
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
                        "eq": "1000",
                        "liab": "-50.5",
                        "interest": "-0.5",
                        "uTime": "1705449605015"
                    },
                    {
                        "ccy": "BTC",
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
        // USDT: balance + borrow; BTC: balance only.
        assert_eq!(msgs.len(), 3);

        let mut usdt_borrow = None;
        let mut saw_btc_borrow = false;
        for msg in &msgs {
            if get_basic_event_type(msg) == BasicAccountEventType::BorrowInterest {
                let bi = BasicBorrowInterestMsg::from_bytes(msg).expect("bi ok");
                if bi.symbol == "USDT" {
                    usdt_borrow = Some((bi.borrowed, bi.interest));
                } else if bi.symbol == "BTC" {
                    saw_btc_borrow = true;
                }
            }
        }
        let (principal, interest) = usdt_borrow.expect("usdt borrow event");
        assert!((principal - 50.0).abs() < 1e-9);
        assert!((interest - 0.5).abs() < 1e-9);
        assert!(!saw_btc_borrow);
    }
}
