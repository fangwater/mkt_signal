//! Gate 下架公告拉取。只转发官方字段，不做 parser。
//!
//! 官方没有历史 REST。增量走
//! `wss://api.gateio.ws/ws/v4/ann` / `announcement.summary_delisting`。
//! 当前有效状态走公开市场接口：`delisting_time` / `in_delisting`。

use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::common::announcement_watch::RawAnnouncement;

pub const ANN_WS_URL: &str = "wss://api.gateio.ws/ws/v4/ann";
pub const DELIST_CHANNEL: &str = "announcement.summary_delisting";

#[derive(Debug, Deserialize)]
struct GateWsFrame {
    #[serde(default)]
    channel: String,
    #[serde(default)]
    event: String,
    #[serde(default)]
    result: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct GateDelistPush {
    #[serde(default)]
    lang: String,
    #[serde(default)]
    origin_url: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    brief: String,
    #[serde(default)]
    published_at: i64,
}

#[derive(Debug, Deserialize)]
struct SpotPair {
    id: String,
    #[serde(default)]
    trade_status: String,
    #[serde(default)]
    delisting_time: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct FuturesContract {
    name: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    in_delisting: bool,
}

pub fn subscribe_frame(lang: &str) -> String {
    json!({
        "time": chrono::Utc::now().timestamp(),
        "channel": DELIST_CHANNEL,
        "event": "subscribe",
        "payload": [lang],
    })
    .to_string()
}

pub fn ping_frame() -> String {
    json!({
        "time": chrono::Utc::now().timestamp(),
        "channel": "announcement.ping",
    })
    .to_string()
}

pub fn parse_ws_text(raw: &str) -> Result<Option<RawAnnouncement>> {
    let frame: GateWsFrame =
        serde_json::from_str(raw).context("parse Gate announcement WS failed")?;
    if frame.channel != DELIST_CHANNEL || frame.event != "update" {
        return Ok(None);
    }
    let Some(result) = frame.result else {
        return Ok(None);
    };
    let push: GateDelistPush =
        serde_json::from_value(result).context("parse Gate delist push failed")?;
    if push.title.is_empty() && push.origin_url.is_empty() {
        return Ok(None);
    }
    let published_ms = if push.published_at > 1_000_000_000_000 {
        push.published_at
    } else {
        push.published_at.saturating_mul(1000)
    };
    let id = if push.origin_url.is_empty() {
        format!("{}-{}", published_ms, slug(&push.title))
    } else {
        push.origin_url.clone()
    };
    Ok(Some(RawAnnouncement {
        extra: Some(json!({
            "lang": push.lang,
            "brief": push.brief,
        })),
        exchange: "gate".to_string(),
        id,
        title: push.title,
        url: push.origin_url,
        published_ms,
        source: "gate_announcement_ws".to_string(),
    }))
}

fn slug(title: &str) -> String {
    title
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(40)
        .collect()
}

pub async fn fetch_market_snapshot(client: &Client) -> Result<Value> {
    let now_sec = chrono::Utc::now().timestamp();
    Ok(json!({
        "source": "gate_market_delist_status",
        "spot_pairs": upcoming_spot_pairs(client, now_sec).await?,
        "usdt_futures_in_delisting": futures_in_delisting(
            client,
            "https://api.gateio.ws/api/v4/futures/usdt/contracts",
        ).await?,
        "btc_futures_in_delisting": futures_in_delisting(
            client,
            "https://api.gateio.ws/api/v4/futures/btc/contracts",
        ).await?,
    }))
}

async fn upcoming_spot_pairs(client: &Client, now_sec: i64) -> Result<Vec<Value>> {
    let response = client
        .get("https://api.gateio.ws/api/v4/spot/currency_pairs")
        .header("Accept", "application/json")
        .send()
        .await
        .context("request Gate spot currency_pairs failed")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("read Gate spot currency_pairs failed")?;
    if !status.is_success() {
        bail!("Gate spot currency_pairs failed: status={status} body={body}");
    }
    let pairs: Vec<SpotPair> =
        serde_json::from_str(&body).context("parse Gate spot currency_pairs JSON failed")?;
    Ok(pairs
        .into_iter()
        .filter_map(|pair| {
            let ts = pair.delisting_time.filter(|ts| *ts > now_sec)?;
            Some(json!({
                "id": pair.id,
                "trade_status": pair.trade_status,
                "delisting_time": ts,
            }))
        })
        .collect())
}

async fn futures_in_delisting(client: &Client, url: &str) -> Result<Vec<Value>> {
    let response = client
        .get(url)
        .header("Accept", "application/json")
        .send()
        .await
        .with_context(|| format!("request {url} failed"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("read {url} failed"))?;
    if !status.is_success() {
        bail!("Gate futures contracts failed: status={status} body={body}");
    }
    let contracts: Vec<FuturesContract> =
        serde_json::from_str(&body).context("parse Gate futures contracts JSON failed")?;
    Ok(contracts
        .into_iter()
        .filter(|contract| contract.in_delisting)
        .map(|contract| {
            json!({
                "name": contract.name,
                "status": contract.status,
                "in_delisting": true,
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_delist_update() {
        let raw = r#"{
            "time": 1690365913,
            "channel": "announcement.summary_delisting",
            "event": "update",
            "result": {
                "lang": "en",
                "origin_url": "https://www.gate.com/announcements/article/100677",
                "title": "Gate Will Delist AAA",
                "brief": "brief text",
                "published_at": 1690365913
            }
        }"#;
        let item = parse_ws_text(raw).unwrap().unwrap();
        assert_eq!(item.exchange, "gate");
        assert_eq!(item.title, "Gate Will Delist AAA");
        assert_eq!(item.published_ms, 1690365913000);
        assert_eq!(
            item.url,
            "https://www.gate.com/announcements/article/100677"
        );
    }

    #[test]
    fn ignores_subscribe_ack() {
        let raw = r#"{"channel":"announcement.summary_delisting","event":"subscribe","result":{"status":"success"}}"#;
        assert!(parse_ws_text(raw).unwrap().is_none());
    }
}
