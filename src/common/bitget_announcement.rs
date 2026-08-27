//! Bitget 下架公告拉取。只转发官方字段，不做 parser。
//!
//! 官方 REST：`GET /api/v2/public/annoucements`（拼写就是 annoucements）
//! `annType=symbol_delisting`，近一个月，cursor 用上一页最后一条 `annId`。
//! 接口没有正文，只有 title + annUrl。

use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;

use crate::common::announcement_watch::{RawAnnouncement, SeenStore};

pub const ANNOUNCEMENTS_URL: &str = "https://api.bitget.com/api/v2/public/annoucements";
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
        let mut req = client.get(ANNOUNCEMENTS_URL).query(&[
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
    let parsed: BitgetEnvelope<Vec<SpotSymbol>> = get_json(
        client,
        "https://api.bitget.com/api/v2/spot/public/symbols",
        "Bitget spot symbols",
    )
    .await?;
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
    let url = format!("https://api.bitget.com/api/v3/market/instruments?category={category}");
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
    fn future_off_skips_placeholders() {
        assert!(future_off(Some("0"), 1).is_none());
        assert!(future_off(Some("-1"), 1).is_none());
        assert_eq!(future_off(Some("100"), 1), Some(100));
        assert!(future_off(Some("1"), 100).is_none());
        assert!(future_off(None, 1).is_none());
    }
}
