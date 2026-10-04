//! Bybit public delisting announcements, including the official article body.

use anyhow::{anyhow, bail, Context, Result};
use reqwest::Client;
use scraper::{Html, Selector};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::common::announcement_watch::RawAnnouncement;

const ANNOUNCEMENTS_URL: &str = "https://api.bybit.com/v5/announcements/index";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AnnouncementResponse {
    ret_code: i64,
    ret_msg: String,
    result: AnnouncementList,
}

#[derive(Debug, Deserialize)]
struct AnnouncementList {
    total: usize,
    list: Vec<Value>,
}

pub async fn fetch_delist_notices(
    client: &Client,
    lookback_days: i64,
) -> Result<Vec<RawAnnouncement>> {
    let cutoff =
        chrono::Utc::now().timestamp_millis() - lookback_days.max(1).saturating_mul(86_400_000);
    let mut out = Vec::new();
    for page in 1..=10 {
        let response = client
            .get(ANNOUNCEMENTS_URL)
            .query(&[
                ("locale", "en-US"),
                ("type", "delistings"),
                ("page", &page.to_string()),
                ("limit", "20"),
            ])
            .send()
            .await
            .context("request Bybit delisting announcements")?
            .error_for_status()
            .context("Bybit announcement HTTP status")?;
        let response: AnnouncementResponse =
            response.json().await.context("parse Bybit announcements")?;
        if response.ret_code != 0 {
            bail!(
                "Bybit announcements: code={} msg={}",
                response.ret_code,
                response.ret_msg
            );
        }
        let count = response.result.list.len();
        let mut old = false;
        for notice in response.result.list {
            let item = notice_from_value(notice)?;
            if item.published_ms < cutoff {
                old = true;
                continue;
            }
            if !out.iter().any(|seen: &RawAnnouncement| seen.id == item.id) {
                out.push(item);
            }
        }
        if old || count < 20 || page as usize * 20 >= response.result.total {
            return Ok(out);
        }
    }
    bail!("Bybit announcements exceed 10 pages within the requested lookback")
}

fn notice_from_value(notice: Value) -> Result<RawAnnouncement> {
    let url = notice
        .get("url")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .context("Bybit announcement missing URL")?
        .to_string();
    let title = notice
        .get("title")
        .and_then(Value::as_str)
        .context("Bybit announcement missing title")?
        .to_string();
    let published_ms = notice
        .get("publishTime")
        .or_else(|| notice.get("dateTimestamp"))
        .and_then(Value::as_i64)
        .context("Bybit announcement missing publication time")?;
    Ok(RawAnnouncement {
        exchange: "bybit".into(),
        id: url.clone(),
        title,
        url,
        published_ms,
        source: "bybit_announcements".into(),
        extra: Some(notice),
    })
}

pub async fn hydrate_notice_body(client: &Client, item: &mut RawAnnouncement) -> Result<()> {
    let url = reqwest::Url::parse(&item.url).context("parse Bybit announcement URL")?;
    if url.scheme() != "https" || url.host_str() != Some("announcements.bybit.com") {
        bail!("unexpected Bybit announcement URL: {}", item.url);
    }
    let page = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("fetch Bybit article {}", item.id))?
        .error_for_status()
        .context("Bybit article HTTP status")?
        .text()
        .await
        .context("read Bybit article")?;
    let body = parse_article_body(&page)?;
    let extra = item
        .extra
        .get_or_insert_with(|| json!({}))
        .as_object_mut()
        .context("Bybit announcement extra must be an object")?;
    extra.insert("body".into(), Value::String(body));
    Ok(())
}

fn parse_article_body(page: &str) -> Result<String> {
    let document = Html::parse_document(page);
    let selector = Selector::parse("script#__NEXT_DATA__")
        .map_err(|err| anyhow!("Bybit article selector: {err}"))?;
    let script = document
        .select(&selector)
        .next()
        .context("Bybit article missing SSR data")?;
    let data: Value =
        serde_json::from_str(&script.inner_html()).context("parse Bybit article SSR")?;
    let detail = data
        .pointer("/props/pageProps/articleDetail")
        .context("Bybit article missing detail")?;
    let mut parts = Vec::new();
    if let Some(html) = detail.get("content_html").and_then(Value::as_str) {
        parts.push(
            Html::parse_fragment(html)
                .root_element()
                .text()
                .collect::<Vec<_>>()
                .join(" "),
        );
    }
    if parts.iter().all(|part| part.trim().is_empty()) {
        if let Some(content) = detail.pointer("/content/json") {
            rich_text(content, &mut parts);
        }
    }
    let body = parts
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if body.is_empty() {
        bail!("Bybit article body is empty");
    }
    Ok(body)
}

fn rich_text(value: &Value, parts: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            if let Some(text) = object.get("text").and_then(Value::as_str) {
                parts.push(text.to_string());
            }
            if let Some(children) = object.get("children") {
                rich_text(children, parts);
            }
        }
        Value::Array(children) => {
            for child in children {
                rich_text(child, parts);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn article_body_retains_pairs_and_each_cutoff() {
        let detail = json!({"props":{"pageProps":{"articleDetail":{"content_html":
            "<p>SCORUSDT,AGIUSDT</p><p>Spot trading ends Oct 8, 2026, 8:00AM UTC.</p><p>Deposits close Oct 7, 2026, 8:00AM UTC.</p>"}}}});
        let page = format!("<script id=\"__NEXT_DATA__\">{detail}</script>");
        let body = parse_article_body(&page).unwrap();
        assert!(body.contains("SCORUSDT,AGIUSDT"));
        assert!(body.contains("Spot trading ends Oct 8"));
        assert!(body.contains("Deposits close Oct 7"));
    }

    #[test]
    fn rich_article_body_and_missing_body() {
        let detail = json!({"props":{"pageProps":{"articleDetail":{"content":{"json":{
            "children":[{"children":[{"text":"BLASTUSDT"},{"text":"settles at 07:30 UTC"}]}]}}}}}});
        assert_eq!(
            parse_article_body(&format!("<script id=\"__NEXT_DATA__\">{detail}</script>")).unwrap(),
            "BLASTUSDT settles at 07:30 UTC"
        );
        assert!(parse_article_body("<html>Access denied</html>").is_err());
    }
}
