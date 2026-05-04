//! Gate REST helpers for unified-account borrowing and interest state.

use crate::common::basic_account_msg::BasicBorrowInterestMsg;
use crate::portfolio_margin::gate_auth::GateCredentials;
use anyhow::{Context, Result};
use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::Value;
use sha2::{Digest, Sha512};
use std::collections::{HashMap, HashSet};

type HmacSha512 = Hmac<Sha512>;

const GATE_REST_BASE: &str = "https://api.gateio.ws";
const GATE_UNIFIED_LOANS_PATH: &str = "/api/v4/unified/loans";
const GATE_UNIFIED_INTEREST_PATH: &str = "/api/v4/unified/interest_records";

fn sign_gate_request(
    secret: &str,
    method: &str,
    path: &str,
    query: &str,
    body: &str,
    timestamp: i64,
) -> String {
    let body_hash = hex::encode(Sha512::digest(body.as_bytes()));
    let to_sign = format!(
        "{}\n{}\n{}\n{}\n{}",
        method.to_uppercase(),
        path,
        query,
        body_hash,
        timestamp
    );
    let mut mac = HmacSha512::new_from_slice(secret.as_bytes()).expect("invalid secret");
    mac.update(to_sign.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

async fn gate_rest_get(
    client: &Client,
    credentials: &GateCredentials,
    path: &str,
    query: &str,
) -> Result<(u16, String)> {
    let ts = Utc::now().timestamp();
    let sign = sign_gate_request(&credentials.secret_key, "GET", path, query, "", ts);

    let mut url = format!("{}{}", GATE_REST_BASE, path);
    if !query.is_empty() {
        url.push('?');
        url.push_str(query);
    }

    let resp = client
        .get(&url)
        .header("Accept", "application/json")
        .header("Content-Type", "application/json")
        .header("KEY", &credentials.api_key)
        .header("Timestamp", ts.to_string())
        .header("SIGN", sign)
        .send()
        .await?;

    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    Ok((status, body))
}

fn parse_f64_value(v: Option<&Value>) -> Option<f64> {
    v.and_then(|val| {
        if let Some(n) = val.as_f64() {
            Some(n)
        } else if let Some(n) = val.as_i64() {
            Some(n as f64)
        } else if let Some(n) = val.as_u64() {
            Some(n as f64)
        } else if let Some(s) = val.as_str() {
            s.trim().parse::<f64>().ok()
        } else {
            None
        }
    })
}

fn parse_i64_value(v: Option<&Value>) -> Option<i64> {
    v.and_then(|val| {
        if let Some(n) = val.as_i64() {
            Some(n)
        } else if let Some(n) = val.as_u64() {
            Some(n as i64)
        } else if let Some(s) = val.as_str() {
            s.trim().parse::<i64>().ok()
        } else {
            val.as_f64().map(|f| f as i64)
        }
    })
}

fn parse_json_rows(body: &str) -> Result<Vec<Value>> {
    let value: Value = serde_json::from_str(body).with_context(|| "parse gate rest json body")?;
    value
        .as_array()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("gate rest response is not a json array"))
}

/// Fetch unified-account borrow balances plus latest interest deduction records.
pub async fn fetch_borrow_interest(
    client: &Client,
    credentials: &GateCredentials,
) -> Result<Vec<BasicBorrowInterestMsg>> {
    let (loan_status, loan_body) = gate_rest_get(
        client,
        credentials,
        GATE_UNIFIED_LOANS_PATH,
        "type=margin&limit=100",
    )
    .await?;
    if loan_status != 200 {
        anyhow::bail!("gate unified loans http {} body={}", loan_status, loan_body);
    }
    let loan_rows = parse_json_rows(&loan_body).with_context(|| "parse gate unified loans body")?;

    let mut borrowed_by_symbol: HashMap<String, (f64, i64)> = HashMap::new();
    for row in loan_rows.iter() {
        let symbol = row
            .get("currency")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_ascii_uppercase();
        if symbol.is_empty() {
            continue;
        }
        let borrowed = parse_f64_value(row.get("amount")).unwrap_or(0.0).abs();
        let ts = parse_i64_value(row.get("update_time"))
            .or_else(|| parse_i64_value(row.get("change_time")))
            .or_else(|| parse_i64_value(row.get("create_time")))
            .unwrap_or_else(|| Utc::now().timestamp_millis());
        let entry = borrowed_by_symbol.entry(symbol).or_insert((0.0, ts));
        entry.0 += borrowed;
        entry.1 = entry.1.max(ts);
    }

    let (interest_status, interest_body) = gate_rest_get(
        client,
        credentials,
        GATE_UNIFIED_INTEREST_PATH,
        "type=margin&limit=100",
    )
    .await?;
    if interest_status != 200 {
        anyhow::bail!(
            "gate unified interest_records http {} body={}",
            interest_status,
            interest_body
        );
    }
    let interest_rows = parse_json_rows(&interest_body)
        .with_context(|| "parse gate unified interest_records body")?;

    let mut interest_by_symbol: HashMap<String, (f64, i64)> = HashMap::new();
    for row in interest_rows.iter() {
        let status = parse_i64_value(row.get("status")).unwrap_or(1);
        if status == 0 {
            continue;
        }
        let symbol = row
            .get("currency")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_ascii_uppercase();
        if symbol.is_empty() {
            continue;
        }
        let interest = parse_f64_value(row.get("interest")).unwrap_or(0.0).abs();
        let ts = parse_i64_value(row.get("create_time"))
            .unwrap_or_else(|| Utc::now().timestamp_millis());
        match interest_by_symbol.get(&symbol) {
            Some((_, existing_ts)) if *existing_ts > ts => {}
            _ => {
                interest_by_symbol.insert(symbol, (interest, ts));
            }
        }
    }

    let mut symbols: HashSet<String> = borrowed_by_symbol.keys().cloned().collect();
    symbols.extend(interest_by_symbol.keys().cloned());

    let mut out = Vec::new();
    let now_ms = Utc::now().timestamp_millis();
    for symbol in symbols {
        let (borrowed, borrow_ts) = borrowed_by_symbol
            .get(&symbol)
            .copied()
            .unwrap_or((0.0, now_ms));
        let (interest, interest_ts) = interest_by_symbol
            .get(&symbol)
            .copied()
            .unwrap_or((0.0, now_ms));
        if borrowed <= 0.0 && interest == 0.0 {
            continue;
        }
        out.push(BasicBorrowInterestMsg::create(
            borrow_ts.max(interest_ts),
            symbol,
            borrowed,
            interest,
        ));
    }
    Ok(out)
}
