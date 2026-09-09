//! Bitget 下架公告拉取。列表用于发现，支持页用于补齐正文。
//!
//! 官方 REST：`GET /api/v2/public/annoucements`（拼写就是 annoucements）
//! `annType=symbol_delisting`，近一个月，cursor 用上一页最后一条 `annId`。
//! 列表接口没有正文，只有 title + annUrl；正文来自 annUrl 的 SSR JSON。

use anyhow::{anyhow, bail, Context, Result};
use reqwest::Client;
use scraper::Html;
use serde::Deserialize;
use serde_json::{json, Value};
use signal_common::public_api::bitget_public_api_url;

use crate::common::announcement_watch::{RawAnnouncement, SeenStore};

pub const ANNOUNCEMENTS_PATH: &str = "/api/v2/public/annoucements";
pub const ANN_TYPE_DELISTING: &str = "symbol_delisting";

#[derive(Debug, Deserialize)]
struct BitgetEnvelope<T> {
    code: String,
    msg: String,
    data: T,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BitgetNotice {
    ann_id: String,
    #[serde(default)]
    ann_title: String,
    #[serde(default)]
    ann_desc: String,
    #[serde(default)]
    ann_type: String,
    #[serde(default)]
    ann_sub_type: String,
    #[serde(default)]
    language: String,
    #[serde(default)]
    ann_url: String,
    #[serde(default)]
    c_time: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SpotSymbol {
    symbol: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    off_time: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MixContract {
    symbol: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    off_time: Option<String>,
    #[serde(default)]
    delivery_time: Option<String>,
}

pub async fn fetch_delist_notices(
    client: &Client,
    language: &str,
    limit: u32,
    max_pages: u32,
    store: &SeenStore,
) -> Result<Vec<RawAnnouncement>> {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    let mut page = 0u32;
    while page < max_pages {
        page += 1;
        let announcements_url = bitget_public_api_url(ANNOUNCEMENTS_PATH);
        let mut req = client.get(&announcements_url).query(&[
            ("language", language),
            ("annType", ANN_TYPE_DELISTING),
            ("limit", &limit.min(10).to_string()),
        ]);
        if let Some(cursor) = cursor.as_deref() {
            req = req.query(&[("cursor", cursor)]);
        }
        let response = req
            .send()
            .await
            .context("request Bitget announcements failed")?;
        let status = response.status();
        let body = response
            .text()
            .await
            .context("read Bitget announcements body failed")?;
        if !status.is_success() {
            bail!("Bitget announcements failed: status={status} body={body}");
        }
        let parsed: BitgetEnvelope<Vec<BitgetNotice>> =
            serde_json::from_str(&body).context("parse Bitget announcements JSON failed")?;
        if parsed.code != "00000" {
            bail!(
                "Bitget announcements rejected: code={} msg={}",
                parsed.code,
                parsed.msg
            );
        }
        if parsed.data.is_empty() {
            break;
        }
        let last_id = parsed.data.last().map(|item| item.ann_id.clone());
        let mut hit_seen_tail = !store.seen.is_empty();
        for notice in parsed.data {
            let item = to_announcement(notice);
            if store.seen.contains_key(&item.key()) {
                continue;
            }
            hit_seen_tail = false;
            out.push(item);
        }
        if hit_seen_tail {
            break;
        }
        cursor = last_id;
        if cursor.is_none() {
            break;
        }
    }
    Ok(out)
}

fn to_announcement(notice: BitgetNotice) -> RawAnnouncement {
    let published_ms = notice.c_time.parse::<i64>().unwrap_or(0);
    RawAnnouncement {
        extra: Some(json!({
            "annType": notice.ann_type,
            "annSubType": notice.ann_sub_type,
            "annDesc": notice.ann_desc,
            "language": notice.language,
        })),
        exchange: "bitget".to_string(),
        id: notice.ann_id,
        title: notice.ann_title,
        url: notice.ann_url,
        published_ms,
        source: "bitget_public_announcements".to_string(),
    }
}

pub async fn hydrate_notice_body(client: &Client, item: &mut RawAnnouncement) -> Result<()> {
    let parsed_url = reqwest::Url::parse(&item.url)
        .with_context(|| format!("invalid Bitget article URL for {}", item.id))?;
    if parsed_url.scheme() != "https"
        || !matches!(parsed_url.host_str(), Some("www.bitget.com" | "bitget.com"))
    {
        bail!(
            "unexpected Bitget article URL for {}: {}",
            item.id,
            item.url
        );
    }
    let response = client
        .get(parsed_url)
        .send()
        .await
        .with_context(|| format!("request Bitget article {} failed", item.id))?;
    let status = response.status();
    let page = response
        .text()
        .await
        .with_context(|| format!("read Bitget article {} failed", item.id))?;
    if !status.is_success() {
        bail!(
            "Bitget article {} failed: status={status} body={}",
            item.id,
            page.chars().take(300).collect::<String>()
        );
    }
    let body = parse_article_body(&page, &item.id)?;
    let extra = item.extra.get_or_insert_with(|| json!({}));
    let object = extra
        .as_object_mut()
        .ok_or_else(|| anyhow!("Bitget announcement extra is not an object for {}", item.id))?;
    object.insert("body".to_string(), Value::String(body));
    object.insert(
        "bodySource".to_string(),
        Value::String("bitget_support_article".to_string()),
    );
    Ok(())
}

pub fn has_article_body(item: &RawAnnouncement) -> bool {
    item.extra
        .as_ref()
        .and_then(|extra| extra.get("body"))
        .and_then(Value::as_str)
        .is_some_and(|body| !body.trim().is_empty())
}

pub fn article_body_processed(item: &RawAnnouncement) -> bool {
    item.extra
        .as_ref()
        .and_then(|extra| extra.get("bodyProcessed"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

pub fn mark_article_body_processed(item: &mut RawAnnouncement) -> Result<()> {
    let extra = item.extra.get_or_insert_with(|| json!({}));
    let object = extra
        .as_object_mut()
        .ok_or_else(|| anyhow!("Bitget announcement extra is not an object for {}", item.id))?;
    object.insert("bodyProcessed".to_string(), Value::Bool(true));
    Ok(())
}

fn parse_article_body(page: &str, article_id: &str) -> Result<String> {
    const STATE_MARKER: &str = "window.__ZEUS_REACT_QUERY_STATE__";
    let state_start = page
        .find(STATE_MARKER)
        .ok_or_else(|| anyhow!("Bitget article {article_id} missing SSR state"))?;
    let after_marker = &page[state_start + STATE_MARKER.len()..];
    let json_start = after_marker
        .find('=')
        .map(|offset| offset + 1)
        .ok_or_else(|| anyhow!("Bitget article {article_id} has invalid SSR state"))?;
    let script_end = after_marker[json_start..]
        .find("</script>")
        .map(|offset| json_start + offset)
        .ok_or_else(|| anyhow!("Bitget article {article_id} has unterminated SSR state"))?;
    let raw_state = after_marker[json_start..script_end]
        .trim()
        .trim_end_matches(';')
        .trim();
    let state: Value = serde_json::from_str(raw_state)
        .with_context(|| format!("parse Bitget article {article_id} SSR state failed"))?;
    let content = find_article_content(&state, article_id)
        .ok_or_else(|| anyhow!("Bitget article {article_id} missing articleDetails.content"))?;
    let fragment = Html::parse_fragment(content);
    let text = fragment
        .root_element()
        .text()
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if text.is_empty() {
        bail!("Bitget article {article_id} body is empty");
    }
    Ok(text)
}

fn find_article_content<'a>(value: &'a Value, article_id: &str) -> Option<&'a str> {
    match value {
        Value::Object(map) => {
            if let Some(details) = map.get("articleDetails").and_then(Value::as_object) {
                let id_matches = details
                    .get("contentId")
                    .and_then(Value::as_str)
                    .map(|id| id == article_id)
                    .unwrap_or(true);
                if id_matches {
                    if let Some(content) = details.get("content").and_then(Value::as_str) {
                        if !content.trim().is_empty() {
                            return Some(content);
                        }
                    }
                }
            }
            map.values()
                .find_map(|nested| find_article_content(nested, article_id))
        }
        Value::Array(items) => items
            .iter()
            .find_map(|nested| find_article_content(nested, article_id)),
        _ => None,
    }
}

pub async fn fetch_offtime_snapshot(client: &Client) -> Result<serde_json::Value> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let spot = upcoming_spot(client, now_ms).await?;
    let usdt_futures = upcoming_mix(client, "USDT-FUTURES", now_ms).await?;
    let coin_futures = upcoming_mix(client, "COIN-FUTURES", now_ms).await?;
    Ok(json!({
        "source": "bitget_instrument_offtime",
        "spot": spot,
        "usdt_futures": usdt_futures,
        "coin_futures": coin_futures,
    }))
}

async fn upcoming_spot(client: &Client, now_ms: i64) -> Result<Vec<serde_json::Value>> {
    let url = bitget_public_api_url("/api/v2/spot/public/symbols");
    let parsed: BitgetEnvelope<Vec<SpotSymbol>> =
        get_json(client, &url, "Bitget spot symbols").await?;
    Ok(parsed
        .data
        .into_iter()
        .filter_map(|item| {
            future_off(item.off_time.as_deref(), now_ms).map(|off| {
                json!({
                    "symbol": item.symbol,
                    "status": item.status,
                    "offTime": off,
                })
            })
        })
        .collect())
}

async fn upcoming_mix(
    client: &Client,
    category: &str,
    now_ms: i64,
) -> Result<Vec<serde_json::Value>> {
    let url = bitget_public_api_url(&format!("/api/v3/market/instruments?category={category}"));
    let parsed: BitgetEnvelope<Vec<MixContract>> =
        get_json(client, &url, "Bitget mix contracts").await?;
    Ok(parsed
        .data
        .into_iter()
        .filter_map(|item| {
            let off = future_off(item.off_time.as_deref(), now_ms)
                .or_else(|| future_off(item.delivery_time.as_deref(), now_ms))?;
            Some(json!({
                "symbol": item.symbol,
                "status": item.status,
                "offTime": off,
            }))
        })
        .collect())
}

async fn get_json<T: serde::de::DeserializeOwned>(
    client: &Client,
    url: &str,
    label: &str,
) -> Result<BitgetEnvelope<T>> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("request {label} failed"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("read {label} body failed"))?;
    if !status.is_success() {
        bail!("{label} failed: status={status} body={body}");
    }
    let parsed: BitgetEnvelope<T> =
        serde_json::from_str(&body).with_context(|| format!("parse {label} JSON failed"))?;
    if parsed.code != "00000" {
        bail!("{label} rejected: code={} msg={}", parsed.code, parsed.msg);
    }
    Ok(parsed)
}

fn future_off(raw: Option<&str>, now_ms: i64) -> Option<i64> {
    let trimmed = raw?.trim();
    if trimmed.is_empty() || trimmed == "0" || trimmed == "-1" {
        return None;
    }
    let ms = trimmed.parse::<i64>().ok()?;
    (ms > now_ms).then_some(ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_notice_without_parsing_title() {
        let notice = BitgetNotice {
            ann_id: "12560603892891".into(),
            ann_title: "[Important] Bitget to delist ICXUSDT, SCRTUSDT, STORJUSDT futures and related services".into(),
            ann_desc: "symbol_delisting".into(),
            ann_type: "symbol_delisting".into(),
            ann_sub_type: "trading_pair_delisting".into(),
            language: "en_US".into(),
            ann_url: "https://www.bitget.com/en/support/articles/12560603892891".into(),
            c_time: "1787300702000".into(),
        };
        let item = to_announcement(notice);
        assert_eq!(item.exchange, "bitget");
        assert_eq!(item.id, "12560603892891");
        assert_eq!(item.published_ms, 1787300702000);
        assert!(item.title.contains("ICXUSDT"));
    }

    #[test]
    fn parses_article_body_from_ssr_state() {
        let page = r#"<html><script >window.__ZEUS_REACT_QUERY_STATE__ = {"queries":[{"state":{"data":{"articleDetails":{"contentId":"123","content":"<div>Disable&nbsp;<strong>BAN, ALT</strong><br>at 10:00 UTC</div>"}}}}]};</script></html>"#;
        let body = parse_article_body(page, "123").unwrap();
        assert_eq!(body, "Disable BAN, ALT at 10:00 UTC");
    }

    #[test]
    fn rejects_ssr_state_for_another_article() {
        let page = r#"<script>window.__ZEUS_REACT_QUERY_STATE__={"articleDetails":{"contentId":"other","content":"wrong"}};</script>"#;
        assert!(parse_article_body(page, "123").is_err());
    }

    #[test]
    fn future_off_skips_placeholders() {
        assert!(future_off(Some("0"), 1).is_none());
        assert!(future_off(Some("-1"), 1).is_none());
        assert_eq!(future_off(Some("100"), 1), Some(100));
        assert!(future_off(Some("1"), 100).is_none());
        assert!(future_off(None, 1).is_none());
    }
}
