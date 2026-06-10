use account_common::bybit_auth::BybitCredentials;
use anyhow::Result;
use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::Value;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const BYBIT_REST_BASE: &str = "https://api.bybit.com";
const BYBIT_RECV_WINDOW_MS: i64 = 5_000;
const BYBIT_POSITION_PAGE_LIMIT: &str = "200";
const BYBIT_POSITION_MAX_PAGES: usize = 20;

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

fn truncate_for_error(body: &str, max_len: usize) -> String {
    if body.len() <= max_len {
        body.to_string()
    } else {
        format!("{}...<truncated {} bytes>", &body[..max_len], body.len())
    }
}

fn query_has_key(query: &str, key: &str) -> bool {
    query.split('&').any(|part| {
        let candidate = part.split_once('=').map(|(k, _)| k).unwrap_or(part);
        candidate == key
    })
}

fn append_query_param(query: &str, key: &str, value: &str) -> String {
    let mut out = String::from(query);
    if !out.is_empty() {
        out.push('&');
    }
    out.push_str(key);
    out.push('=');
    out.push_str(value);
    out
}

fn ensure_position_limit(query: &str) -> String {
    if query_has_key(query, "limit") {
        query.to_string()
    } else {
        append_query_param(query, "limit", BYBIT_POSITION_PAGE_LIMIT)
    }
}

fn bybit_next_page_cursor(body: &str) -> Option<String> {
    let v: Value = serde_json::from_str(body).ok()?;
    v.get("result")
        .and_then(|r| r.get("nextPageCursor"))
        .and_then(|c| c.as_str())
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .map(ToOwned::to_owned)
}

pub async fn bybit_rest_get_position_list_pages(
    client: &Client,
    credentials: &BybitCredentials,
    path: &str,
    query: &str,
) -> Result<Vec<String>> {
    let base_query = ensure_position_limit(query);
    let mut cursor = String::new();
    let mut bodies = Vec::new();

    for page_idx in 0..BYBIT_POSITION_MAX_PAGES {
        let page_query = if cursor.is_empty() {
            base_query.clone()
        } else {
            append_query_param(&base_query, "cursor", &cursor)
        };
        let (status, body) = bybit_rest_get(client, credentials, path, &page_query).await?;
        if status != 200 {
            anyhow::bail!(
                "Bybit position list page {} returned non-200 status={} query={} body={}",
                page_idx + 1,
                status,
                page_query,
                truncate_for_error(&body, 512)
            );
        }

        let next_cursor = bybit_next_page_cursor(&body).unwrap_or_default();
        bodies.push(body);
        if next_cursor.is_empty() {
            return Ok(bodies);
        }
        if next_cursor == cursor {
            anyhow::bail!(
                "Bybit position list pagination cursor did not advance on page {} cursor={}",
                page_idx + 1,
                next_cursor
            );
        }
        cursor = next_cursor;
    }

    anyhow::bail!(
        "Bybit position list exceeded max pages={} last_cursor={}",
        BYBIT_POSITION_MAX_PAGES,
        cursor
    );
}

/// Bybit V5 POST：签名 payload 用 `body_json` 替换 GET 里的 `query_string`。
pub async fn bybit_rest_post(
    client: &Client,
    credentials: &BybitCredentials,
    path: &str,
    body: &str,
) -> Result<(u16, String)> {
    let timestamp_ms = Utc::now().timestamp_millis();
    let sign = build_bybit_sign(
        timestamp_ms,
        &credentials.api_key,
        BYBIT_RECV_WINDOW_MS,
        body,
        &credentials.secret_key,
    );

    let url = format!("{}{}", BYBIT_REST_BASE, path);

    let resp = client
        .post(&url)
        .header("X-BAPI-API-KEY", &credentials.api_key)
        .header("X-BAPI-SIGN", sign)
        .header("X-BAPI-SIGN-TYPE", "2")
        .header("X-BAPI-TIMESTAMP", timestamp_ms.to_string())
        .header("X-BAPI-RECV-WINDOW", BYBIT_RECV_WINDOW_MS.to_string())
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body.to_string())
        .send()
        .await?;

    let status = resp.status().as_u16();
    let resp_body = resp.text().await.unwrap_or_default();
    Ok((status, resp_body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn appends_limit_when_missing() {
        assert_eq!(
            ensure_position_limit("category=linear&settleCoin=USDT"),
            "category=linear&settleCoin=USDT&limit=200"
        );
    }

    #[test]
    fn preserves_existing_limit() {
        assert_eq!(
            ensure_position_limit("category=linear&settleCoin=USDT&limit=50"),
            "category=linear&settleCoin=USDT&limit=50"
        );
    }

    #[test]
    fn extracts_next_page_cursor() {
        let body = r#"{"retCode":0,"result":{"list":[],"nextPageCursor":"abc%3D"}}"#;
        assert_eq!(bybit_next_page_cursor(body).as_deref(), Some("abc%3D"));
    }

    #[test]
    fn appends_cursor_without_double_encoding() {
        assert_eq!(
            append_query_param("category=linear&limit=200", "cursor", "abc%3D"),
            "category=linear&limit=200&cursor=abc%3D"
        );
    }
}
