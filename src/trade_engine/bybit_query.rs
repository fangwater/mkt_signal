use crate::portfolio_margin::bybit_auth::BybitCredentials;
use anyhow::Result;
use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::Client;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const BYBIT_REST_BASE: &str = "https://api.bybit.com";
const BYBIT_RECV_WINDOW_MS: i64 = 5_000;

fn build_bybit_sign(
    timestamp_ms: i64,
    api_key: &str,
    recv_window_ms: i64,
    query_string: &str,
    secret: &str,
) -> String {
    let payload = format!("{timestamp_ms}{api_key}{recv_window_ms}{query_string}");
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take any size");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub async fn bybit_rest_get(
    client: &Client,
    credentials: &BybitCredentials,
    path: &str,
    query: &str,
) -> Result<(u16, String)> {
    let timestamp_ms = Utc::now().timestamp_millis();
    let sign = build_bybit_sign(
        timestamp_ms,
        &credentials.api_key,
        BYBIT_RECV_WINDOW_MS,
        query,
        &credentials.secret_key,
    );

    let mut url = format!("{}{}", BYBIT_REST_BASE, path);
    if !query.is_empty() {
        url.push('?');
        url.push_str(query);
    }

    let resp = client
        .get(&url)
        .header("X-BAPI-API-KEY", &credentials.api_key)
        .header("X-BAPI-SIGN", sign)
        .header("X-BAPI-SIGN-TYPE", "2")
        .header("X-BAPI-TIMESTAMP", timestamp_ms.to_string())
        .header("X-BAPI-RECV-WINDOW", BYBIT_RECV_WINDOW_MS.to_string())
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .send()
        .await?;

    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    Ok((status, body))
}
