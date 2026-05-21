use crate::portfolio_margin::okex_auth::OkexCredentials;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chrono::{SecondsFormat, Utc};
use hmac::{Hmac, Mac};
use reqwest::Client;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const OKEX_REST_BASE: &str = "https://openapi.okx.com";

fn build_okex_sign(timestamp: &str, method: &str, path: &str, body: &str, secret: &str) -> String {
    let payload = format!("{}{}{}{}", timestamp, method, path, body);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take any size");
    mac.update(payload.as_bytes());
    let result = mac.finalize();
    BASE64.encode(result.into_bytes())
}

pub async fn okex_rest_get(
    client: &Client,
    credentials: &OkexCredentials,
    path_with_query: &str,
) -> Result<(u16, String)> {
    let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let sign = build_okex_sign(
        &timestamp,
        "GET",
        path_with_query,
        "",
        &credentials.secret_key,
    );

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("OK-ACCESS-KEY", credentials.api_key.parse()?);
    headers.insert("OK-ACCESS-SIGN", sign.parse()?);
    headers.insert("OK-ACCESS-TIMESTAMP", timestamp.parse()?);
    headers.insert("OK-ACCESS-PASSPHRASE", credentials.passphrase.parse()?);
    headers.insert(reqwest::header::CONTENT_TYPE, "application/json".parse()?);

    let url = format!("{}{}", OKEX_REST_BASE, path_with_query);
    let resp = client.get(&url).headers(headers).send().await?;
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    Ok((status, body))
}
