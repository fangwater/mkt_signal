use crate::portfolio_margin::gate_auth::GateCredentials;
use anyhow::Result;
use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::Client;
use sha2::{Digest, Sha512};

type HmacSha512 = Hmac<Sha512>;

const GATE_REST_BASE: &str = "https://api.gateio.ws";

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

pub async fn gate_rest_get(
    client: &Client,
    credentials: &GateCredentials,
    path: &str,
    query: &str,
) -> Result<(u16, String)> {
    let ts = Utc::now().timestamp();
    let sign = sign_gate_request(
        &credentials.secret_key,
        "GET",
        path,
        query,
        "",
        ts,
    );

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
