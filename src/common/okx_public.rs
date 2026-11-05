//! OKX's complete, unpaginated public SPOT/SWAP instrument catalog.
use anyhow::{bail, ensure, Context, Result};
use reqwest::Client;
use serde::Deserialize;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicI64, Ordering};
static RETRY_AFTER_MS: AtomicI64 = AtomicI64::new(0);

pub(crate) async fn public_get(request: reqwest::RequestBuilder) -> Result<reqwest::Response> {
    let now = chrono::Utc::now().timestamp_millis();
    ensure!(
        now >= RETRY_AFTER_MS.load(Ordering::Relaxed),
        "OKX public source cooling down after throttling"
    );
    let response = request
        .header(reqwest::header::USER_AGENT, "mkt_signal/delist-risk")
        .send()
        .await
        .context("request OKX public source")?;
    if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
        || response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE
    {
        let delay = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| {
                s.parse::<i64>()
                    .ok()
                    .map(|secs| secs.saturating_mul(1000))
                    .or_else(|| {
                        chrono::DateTime::parse_from_rfc2822(s)
                            .ok()
                            .map(|date| date.timestamp_millis().saturating_sub(now))
                    })
            })
            .unwrap_or(60_000)
            .max(60_000);
        RETRY_AFTER_MS.fetch_max(now.saturating_add(delay), Ordering::Relaxed);
    }
    response
        .error_for_status()
        .context("OKX public HTTP status")
}

pub(crate) fn check_code(code: &str, msg: &str) -> Result<()> {
    if matches!(code, "50011" | "50013") {
        RETRY_AFTER_MS.fetch_max(
            chrono::Utc::now().timestamp_millis().saturating_add(60_000),
            Ordering::Relaxed,
        );
    }
    ensure!(code == "0", "OKX public API code={code} msg={msg}");
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Instrument {
    pub inst_id: String,
    pub inst_type: String,
    #[serde(default)]
    pub ct_type: String,
    #[serde(default)]
    pub settle_ccy: String,
    #[serde(default)]
    pub quote_ccy: String,
    pub exp_time: String,
    pub state: String,
}
impl Instrument {
    pub fn target_market(&self) -> bool {
        match self.inst_type.as_str() {
            "SPOT" => self.quote_ccy == "USDT",
            "SWAP" => self.ct_type == "linear" && self.settle_ccy == "USDT",
            _ => false,
        }
    }
    pub fn expiry_ms(&self) -> Result<Option<i64>> {
        if self.exp_time.is_empty() {
            return Ok(None);
        }
        let ms: i64 = self.exp_time.parse().context("OKX invalid expTime")?;
        ensure!(ms > 0, "OKX nonpositive expTime");
        Ok(Some(ms))
    }
}

pub(crate) async fn fetch_instruments(client: &Client, inst_type: &str) -> Result<Vec<Instrument>> {
    let body = public_get(
        client
            .get("https://www.okx.com/api/v5/public/instruments")
            .query(&[("instType", inst_type)]),
    )
    .await?
    .text()
    .await
    .context("read OKX instruments")?;
    parse_instruments(&body, inst_type)
}

pub(crate) fn parse_instruments(body: &str, inst_type: &str) -> Result<Vec<Instrument>> {
    #[derive(Deserialize)]
    struct Response {
        code: String,
        #[serde(default)]
        msg: String,
        data: Vec<Instrument>,
    }
    let response: Response = serde_json::from_str(body).context("parse OKX instruments")?;
    check_code(&response.code, &response.msg)?;
    ensure!(!response.data.is_empty(), "OKX empty instrument catalog");
    let mut ids = BTreeSet::new();
    let mut targets = 0;
    for item in &response.data {
        ensure!(
            item.inst_type == inst_type,
            "OKX mismatched instType for {}",
            item.inst_id
        );
        ensure!(
            !item.inst_id.is_empty() && !item.state.is_empty(),
            "OKX incomplete instrument row"
        );
        ensure!(
            ids.insert(&item.inst_id),
            "OKX duplicate instrument {}",
            item.inst_id
        );
        item.expiry_ms()?;
        if item.target_market() {
            let suffix = if inst_type == "SWAP" {
                "-USDT-SWAP"
            } else {
                "-USDT"
            };
            if !item.inst_id.ends_with(suffix) {
                bail!("OKX inconsistent instrument {}", item.inst_id);
            }
            targets += 1;
        }
    }
    ensure!(targets > 0, "OKX catalog contains no target USDT market");
    Ok(response.data)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn official_swap_sample_filters_inverse_market() {
        let rows = parse_instruments(
            include_str!("../../tests/fixtures/okx/swap_instruments.json"),
            "SWAP",
        )
        .unwrap();
        assert!(rows[0].target_market());
        assert!(!rows[1].target_market());
        assert!(parse_instruments(
            include_str!("../../tests/fixtures/okx/swap_instruments.json"),
            "SPOT"
        )
        .is_err());
    }
    #[test]
    fn rejects_empty_error_truncated_and_wrong_market() {
        for body in [
            r#"{"code":"0","data":[]}"#,
            r#"{"code":"50011","data":[]}"#,
            r#"{"code":"0","data":[{"instId":"BTC-USDT"}]}"#,
        ] {
            assert!(parse_instruments(body, "SPOT").is_err());
        }
    }
}
