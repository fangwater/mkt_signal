use crate::common::basic_account_msg::{BasicBalanceMsg, BasicBorrowInterestMsg};
use bytes::Bytes;
use serde_json::Value;

fn parse_f64_value(v: &Value) -> Option<f64> {
    if let Some(n) = v.as_f64() {
        return Some(n);
    }
    if let Some(s) = v.as_str() {
        if s.trim().is_empty() {
            return None;
        }
        return s.parse::<f64>().ok();
    }
    None
}

fn parse_i64_value(v: &Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    if let Some(s) = v.as_str() {
        if s.trim().is_empty() {
            return None;
        }
        return s.parse::<i64>().ok();
    }
    None
}

fn extract_rows(value: &Value) -> Option<&Vec<Value>> {
    if let Some(arr) = value.as_array() {
        return Some(arr);
    }
    if let Some(arr) = value.get("data").and_then(|v| v.as_array()) {
        return Some(arr);
    }
    if let Some(arr) = value.get("result").and_then(|v| v.as_array()) {
        return Some(arr);
    }
    if let Some(arr) = value.get("accounts").and_then(|v| v.as_array()) {
        return Some(arr);
    }
    if let Some(arr) = value
        .get("data")
        .and_then(|v| v.get("accounts"))
        .and_then(|v| v.as_array())
    {
        return Some(arr);
    }
    None
}

fn extract_balance_map<'a>(value: &'a Value) -> Option<&'a serde_json::Map<String, Value>> {
    value.get("balances")?.as_object()
}

fn find_str<'a>(row: &'a Value, keys: &[&str]) -> Option<&'a str> {
    for key in keys {
        if let Some(s) = row.get(*key).and_then(|v| v.as_str()) {
            if !s.trim().is_empty() {
                return Some(s);
            }
        }
    }
    None
}

fn find_time_ms(row: &Value) -> Option<i64> {
    let keys = [
        "update_time_ms",
        "updateTimeMs",
        "update_time",
        "updateTime",
        "u_time",
        "uTime",
        "time_ms",
        "time",
    ];
    for key in keys {
        if let Some(v) = row.get(key).and_then(parse_i64_value) {
            if v <= 0 {
                continue;
            }
            if v < 1_000_000_000_000 {
                return Some(v * 1000);
            }
            return Some(v);
        }
    }
    None
}

fn find_balance(row: &Value) -> Option<f64> {
    let keys = [
        "total",
        "equity",
        "eq",
        "balance",
        "available_balance",
        "available",
        "avail",
    ];
    for key in keys {
        if let Some(v) = row.get(key).and_then(parse_f64_value) {
            return Some(v);
        }
    }
    let available = row
        .get("available")
        .and_then(parse_f64_value)
        .or_else(|| row.get("avail").and_then(parse_f64_value));
    let locked = row
        .get("locked")
        .and_then(parse_f64_value)
        .or_else(|| row.get("frozen").and_then(parse_f64_value))
        .or_else(|| row.get("freeze").and_then(parse_f64_value));
    match (available, locked) {
        (Some(a), Some(l)) => Some(a + l),
        _ => None,
    }
}

fn find_f64(row: &Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Some(v) = row.get(*key).and_then(parse_f64_value) {
            return Some(v);
        }
    }
    None
}

pub fn parse_gate_unified_balance_snapshot(json: &str) -> Option<Vec<Bytes>> {
    let value: Value = serde_json::from_str(json).ok()?;
    let now_ts = chrono::Utc::now().timestamp_millis();
    let mut out = Vec::new();

    if let Some(rows) = extract_rows(&value) {
        for row in rows {
            let symbol = find_str(row, &["currency", "ccy", "asset", "symbol", "coin"])?;
            let balance = find_balance(row)?;
            let ts = find_time_ms(row).unwrap_or(now_ts);
            out.push(BasicBalanceMsg::create(ts, symbol.to_ascii_uppercase(), balance).to_bytes());
        }
    }

    if !out.is_empty() {
        return Some(out);
    }

    if let Some(balances) = extract_balance_map(&value) {
        for (symbol, details) in balances {
            let available = find_f64(details, &["available"]).unwrap_or(0.0);
            let freeze = find_f64(details, &["freeze", "frozen"]).unwrap_or(0.0);
            let total_freeze = find_f64(details, &["total_freeze"]).unwrap_or(0.0);
            let spot_in_use = find_f64(details, &["spot_in_use"]).unwrap_or(0.0);
            let equity = find_f64(details, &["equity"]);

            let mut balance = available + freeze + spot_in_use;
            if total_freeze > freeze {
                balance += total_freeze - freeze;
            }
            if balance == 0.0 {
                if let Some(eq) = equity {
                    balance = eq;
                }
            }

            let ts = now_ts;
            out.push(BasicBalanceMsg::create(ts, symbol.to_ascii_uppercase(), balance).to_bytes());

            let total_liab = find_f64(details, &["total_liab"]).unwrap_or(0.0);
            let borrowed = find_f64(details, &["borrowed"]).unwrap_or(0.0);
            let negative_liab = find_f64(details, &["negative_liab"]).unwrap_or(0.0);
            let futures_liab = find_f64(details, &["futures_pos_liab"]).unwrap_or(0.0);
            let liab = if total_liab > 0.0 {
                total_liab
            } else {
                borrowed + negative_liab + futures_liab
            };
            out.push(
                BasicBorrowInterestMsg::create(ts, symbol.to_ascii_uppercase(), liab, 0.0)
                    .to_bytes(),
            );
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
    fn parses_gate_balance_array() {
        let json = r#"[{
            "currency": "USDT",
            "total": "100.5",
            "update_time": 1716796364
        }]"#;
        let msgs = parse_gate_unified_balance_snapshot(json).expect("parse ok");
        assert_eq!(msgs.len(), 1);
        let bal = BasicBalanceMsg::from_bytes(&msgs[0]).expect("bal ok");
        assert_eq!(bal.symbol, "USDT");
        assert!((bal.balance - 100.5).abs() < 1e-12);
        assert_eq!(bal.timestamp, 1716796364000);
    }

    #[test]
    fn parses_gate_balance_data_object() {
        let json = r#"{
            "data": [{
                "asset": "BTC",
                "balance": 0.25,
                "update_time_ms": 1716796364000
            }]
        }"#;
        let msgs = parse_gate_unified_balance_snapshot(json).expect("parse ok");
        assert_eq!(msgs.len(), 1);
        let bal = BasicBalanceMsg::from_bytes(&msgs[0]).expect("bal ok");
        assert_eq!(bal.symbol, "BTC");
        assert!((bal.balance - 0.25).abs() < 1e-12);
        assert_eq!(bal.timestamp, 1716796364000);
    }

    #[test]
    fn parses_gate_unified_balance_map() {
        let json = r#"{
            "balances": {
                "ETH": {
                    "available": "0",
                    "freeze": "0",
                    "borrowed": "0.075",
                    "negative_liab": "0",
                    "futures_pos_liab": "0",
                    "equity": "1016.1",
                    "total_freeze": "0",
                    "total_liab": "0",
                    "spot_in_use": "1.111"
                },
                "USDT": {
                    "available": "1.5",
                    "freeze": "0.5",
                    "borrowed": "0",
                    "negative_liab": "0",
                    "futures_pos_liab": "0",
                    "equity": "2.0",
                    "total_freeze": "0",
                    "total_liab": "0",
                    "spot_in_use": "0"
                }
            }
        }"#;
        let msgs = parse_gate_unified_balance_snapshot(json).expect("parse ok");
        assert!(msgs.len() >= 4);

        let mut eth_balance = None;
        let mut eth_borrowed = None;
        for msg in msgs {
            match get_basic_event_type(&msg) {
                BasicAccountEventType::BalanceUpdate => {
                    let bal = BasicBalanceMsg::from_bytes(&msg).expect("bal ok");
                    if bal.symbol == "ETH" {
                        eth_balance = Some(bal.balance);
                    }
                }
                BasicAccountEventType::BorrowInterest => {
                    let bi = BasicBorrowInterestMsg::from_bytes(&msg).expect("bi ok");
                    if bi.symbol == "ETH" {
                        eth_borrowed = Some(bi.borrowed);
                    }
                }
                _ => {}
            }
        }

        assert_eq!(eth_balance, Some(1.111));
        assert_eq!(eth_borrowed, Some(0.075));
    }
}
