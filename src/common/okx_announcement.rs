//! Official OKX public announcement API and help-center article bodies.
use crate::common::announcement_watch::RawAnnouncement;
use anyhow::{anyhow, bail, ensure, Context, Result};
use reqwest::Client;
use scraper::{Html, Selector};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeSet;

const CATEGORY: &str = "announcements-delistings";
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Page {
    details: Vec<Value>,
    total_page: String,
}

fn official_url(raw: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(raw).context("parse OKX article URL")?;
    ensure!(
        url.scheme() == "https"
            && url.host_str() == Some("www.okx.com")
            && url.port().is_none()
            && url.username().is_empty()
            && url.password().is_none()
            && url.path().starts_with("/help/"),
        "unexpected OKX article URL"
    );
    Ok(url)
}
fn parse_page(body: &str) -> Result<(Vec<RawAnnouncement>, usize)> {
    #[derive(Deserialize)]
    struct Response {
        code: String,
        #[serde(default)]
        msg: String,
        data: Vec<Page>,
    }
    let mut response: Response = serde_json::from_str(body).context("parse OKX announcements")?;
    crate::common::okx_public::check_code(&response.code, &response.msg)?;
    ensure!(response.data.len() == 1, "OKX announcements missing page");
    let page = response.data.remove(0);
    let total = page.total_page.parse::<usize>().context("OKX totalPage")?;
    ensure!(
        total > 0 && !page.details.is_empty() && page.details.len() <= 20,
        "OKX empty/incomplete announcements page"
    );
    let mut items = Vec::new();
    for notice in page.details {
        ensure!(
            notice.get("annType").and_then(Value::as_str) == Some(CATEGORY),
            "OKX wrong announcement category"
        );
        let url = notice
            .get("url")
            .and_then(Value::as_str)
            .context("OKX announcement missing URL")?
            .to_string();
        official_url(&url)?;
        let title = notice
            .get("title")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .context("OKX announcement missing title")?
            .to_string();
        let published_ms = notice
            .get("pTime")
            .and_then(Value::as_str)
            .context("OKX missing pTime")?
            .parse::<i64>()
            .context("OKX invalid pTime")?;
        ensure!(published_ms > 0, "OKX invalid publication time");
        items.push(RawAnnouncement {
            exchange: "okex".into(),
            id: url.clone(),
            title,
            url,
            published_ms,
            source: "okex_announcements".into(),
            extra: Some(notice),
        });
    }
    Ok((items, total))
}

pub async fn fetch_delist_notices(
    client: &Client,
    lookback_days: i64,
) -> Result<Vec<RawAnnouncement>> {
    let cutoff =
        chrono::Utc::now().timestamp_millis() - lookback_days.max(1).saturating_mul(86_400_000);
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for page in 1..=10 {
        // Below the documented 5 requests / 2s; no immediate retries on 429/business errors.
        if page > 1 {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        let body = crate::common::okx_public::public_get(
            client
                .get("https://www.okx.com/api/v5/support/announcements")
                .header("Accept-Language", "en-US")
                .query(&[("annType", CATEGORY), ("page", &page.to_string())]),
        )
        .await?
        .text()
        .await
        .context("read OKX announcements")?;
        let (items, total) = parse_page(&body)?;
        ensure!(
            page >= total || items.len() == 20,
            "OKX truncated non-final announcement page"
        );
        let mut old = false;
        let mut new = 0;
        for item in items {
            if seen.insert(item.id.clone()) {
                new += 1;
                if item.published_ms < cutoff {
                    old = true;
                } else {
                    out.push(item);
                }
            }
        }
        ensure!(new > 0, "OKX repeated announcement page");
        if old || page >= total {
            return Ok(out);
        }
    }
    bail!("OKX announcements exceed ten pages within lookback")
}

pub async fn hydrate_notice_body(_client: &Client, item: &mut RawAnnouncement) -> Result<()> {
    let url = official_url(&item.url)?;
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("OKX article client")?;
    let response =
        crate::common::okx_public::public_get(client.get(url).header("Accept-Language", "en-US"))
            .await?;
    ensure!(
        response.status().is_success(),
        "OKX article redirect or invalid status"
    );
    official_url(response.url().as_str())?;
    let body = parse_article_body(&response.text().await.context("read OKX article")?)?;
    item.extra
        .get_or_insert_with(|| json!({}))
        .as_object_mut()
        .context("OKX extra must be object")?
        .insert("body".into(), Value::String(body));
    Ok(())
}
fn parse_article_body(page: &str) -> Result<String> {
    let doc = Html::parse_document(page);
    // The help center renders Contentful rich text in its SSR article; exclude nav/footer.
    let selector = Selector::parse("article div[class*='richTextContent']")
        .map_err(|e| anyhow!("OKX article selector: {e}"))?;
    let body = doc
        .select(&selector)
        .map(|node| node.text().collect::<Vec<_>>().join(" "))
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    ensure!(body.len() >= 80, "OKX article body missing or incomplete");
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn official_samples_preserve_market_and_cutoffs() {
        let (items, pages) =
            parse_page(include_str!("../../tests/fixtures/okx/announcements.json")).unwrap();
        assert_eq!(pages, 7);
        assert_eq!(items.len(), 20);
        assert_eq!(items[0].exchange, "okex");
        let body =
            parse_article_body(include_str!("../../tests/fixtures/okx/spot_article.html")).unwrap();
        assert!(body.contains("DORA/USDT"));
        assert!(body.contains("September 30, 2026"));
        assert!(body.contains("October 3, 2026"));
        let swap =
            parse_article_body(include_str!("../../tests/fixtures/okx/swap_article.html")).unwrap();
        assert!(swap.contains("ICXUSDT"));
        assert!(swap.contains("September 17, 2026, 08:00 UTC"));
        assert!(swap.contains("all open orders will be canceled"));
        assert!(parse_article_body("<html>Access denied</html>").is_err());
    }
    #[test]
    fn rejects_invalid_pages_and_untrusted_links() {
        for body in [
            r#"{"code":"0","data":[]}"#,
            r#"{"code":"0","data":[{"details":[],"totalPage":"1"}]}"#,
            r#"{"code":"50011","data":[]}"#,
        ] {
            assert!(parse_page(body).is_err());
        }
        for url in [
            "http://www.okx.com/help/test",
            "https://evil.com/help/a",
            "https://www.okx.com.evil.com/help/a",
        ] {
            assert!(official_url(url).is_err());
        }
    }
}
