use crate::common::basic_account_msg::BasicBalanceMsg;
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
        out.push(BasicBalanceMsg::create(ts, d.ccy.to_ascii_uppercase(), eq).to_bytes());
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::basic_account_msg::BasicAccountEventType;

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
}
