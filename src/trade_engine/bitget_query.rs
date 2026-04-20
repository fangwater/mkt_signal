use crate::portfolio_margin::bitget_auth::BitgetCredentials;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::Client;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const BITGET_REST_BASE: &str = "https://api.bitget.com";

fn build_bitget_sign(
    timestamp_ms: i64,
    method: &str,
    path_with_query: &str,
    secret: &str,
) -> String {
    let payload = format!("{}{}{}", timestamp_ms, method.to_uppercase(), path_with_query);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take any size");
    mac.update(payload.as_bytes());
    BASE64.encode(mac.finalize().into_bytes())
}

pub async fn bitget_rest_get(
    client: &Client,
    credentials: &BitgetCredentials,
    path: &str,
    query: &str,
) -> Result<(u16, String)> {
    let timestamp_ms = Utc::now().timestamp_millis();
    let mut path_with_query = path.to_string();
    if !query.is_empty() {
        path_with_query.push('?');
        path_with_query.push_str(query);
    }
    let sign = build_bitget_sign(
        timestamp_ms,
        "GET",
        &path_with_query,
        &credentials.secret_key,
    );
    let url = format!("{}{}", BITGET_REST_BASE, path_with_query);

    let resp = client
        .get(&url)
        .header("ACCESS-KEY", &credentials.api_key)
        .header("ACCESS-SIGN", sign)
        .header("ACCESS-TIMESTAMP", timestamp_ms.to_string())
        .header("ACCESS-PASSPHRASE", &credentials.passphrase)
        .header("locale", "zh-CN")
        .send()
        .await?;

    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    Ok((status, body))
}
