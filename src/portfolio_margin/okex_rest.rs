//! OKEx REST 辅助（签名、借贷利息拉取）

use crate::common::basic_account_msg::BasicBorrowInterestMsg;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chrono::{SecondsFormat, Utc};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::Deserialize;
use sha2::Sha256;

use crate::portfolio_margin::okex_auth::OkexCredentials;

type HmacSha256 = Hmac<Sha256>;

const OKEX_INTEREST_PATH: &str = "/api/v5/account/interest-accrued";
const OKEX_REST_BASE: &str = "https://openapi.okx.com";

#[derive(Debug, Deserialize)]
struct InterestAccruedItem {
    #[serde(default)]
    ccy: String,
    #[serde(default)]
    liab: String,
    #[serde(default)]
    interest: String,
    #[serde(default)]
    ts: String,
}

#[derive(Debug, Deserialize)]
struct InterestAccruedResponse {
    #[serde(default)]
    code: String,
    #[serde(default)]
    msg: String,
    #[serde(default)]
    data: Vec<InterestAccruedItem>,
}

fn build_okex_sign(timestamp: &str, method: &str, path: &str, body: &str, secret: &str) -> String {
    let payload = format!("{}{}{}{}", timestamp, method, path, body);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take any size");
    mac.update(payload.as_bytes());
    let result = mac.finalize();
    BASE64.encode(result.into_bytes())
}

/// 拉取 OKX 借贷利息，返回基础事件列表（不写出）
pub async fn fetch_borrow_interest(
    client: &Client,
    credentials: &OkexCredentials,
) -> Result<Vec<BasicBorrowInterestMsg>> {
    let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let sign = build_okex_sign(
        &timestamp,
        "GET",
        OKEX_INTEREST_PATH,
        "",
        &credentials.secret_key,
    );

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("OK-ACCESS-KEY", credentials.api_key.parse()?);
    headers.insert("OK-ACCESS-SIGN", sign.parse()?);
    headers.insert("OK-ACCESS-TIMESTAMP", timestamp.parse()?);
    headers.insert("OK-ACCESS-PASSPHRASE", credentials.passphrase.parse()?);
    headers.insert(reqwest::header::CONTENT_TYPE, "application/json".parse()?);

    let url = format!("{}{}", OKEX_REST_BASE, OKEX_INTEREST_PATH);
    let resp = client.get(&url).headers(headers).send().await?;
    let status = resp.status();
    let body = resp.text().await?;

    if !status.is_success() {
        anyhow::bail!("okx interest-accrued http {} body={}", status, body);
    }

    let parsed: InterestAccruedResponse = serde_json::from_str(&body)?;
    if parsed.code != "0" {
        anyhow::bail!(
            "okx interest-accrued error code={} msg={}",
            parsed.code,
            parsed.msg
        );
    }

    let mut msgs = Vec::new();
    for item in parsed.data {
        let symbol = item.ccy;
        let borrowed = item.liab.parse::<f64>().unwrap_or(0.0);
        let interest = item.interest.parse::<f64>().unwrap_or(0.0);
        let ts = item
            .ts
            .parse::<i64>()
            .unwrap_or_else(|_| Utc::now().timestamp_millis());
        msgs.push(BasicBorrowInterestMsg::create(
            ts, symbol, borrowed, interest,
        ));
    }
    Ok(msgs)
}
