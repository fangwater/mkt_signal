//! 下架公告 LLM 抽取。走 OpenAI Responses API（可换兼容代理）。
//!
//! 发现 / 去重仍由各所 watcher 完成。这里只对**新公告**抽标准化 JSON。
//! 主失败切备；都失败只打日志，不拦拉取。
//!
//! ```text
//! DELIST_LLM_API_URL=http://127.0.0.1:8080/v1
//! DELIST_LLM_API_KEY=sk-...
//! DELIST_LLM_MODEL=gpt-5.6-luna
//! DELIST_LLM_REASONING_EFFORT=xhigh
//! DELIST_LLM_BACKUP_API_URL=http://127.0.0.1:8080/v1
//! DELIST_LLM_BACKUP_API_KEY=sk-...
//! DELIST_LLM_BACKUP_MODEL=gpt-5.6-luna
//! ```
//!
//! 也认 `OPENAI_API_KEY` / `OPENAI_BASE_URL` 作为主端点回退。

use anyhow::{anyhow, bail, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;

use crate::common::announcement_watch::RawAnnouncement;
use crate::common::binance_announcement::ParsedAnnouncement;

const DEFAULT_MODEL: &str = "gpt-5.6-luna";
const DEFAULT_EFFORT: &str = "xhigh";
const DEFAULT_HEADER: &str = "x-openai-actor-authorization: local-image-extension";
const BODY_CHAR_LIMIT: usize = 12_000;

const SYSTEM_PROMPT: &str = r#"Extract risk events from one exchange delist announcement.
This is a risk hint, not a full product timetable. Ignore gift card, pay, convert, mining, earn, copy-trading, bots unless they are the only event.
Return JSON only. Do not invent tickers, venues, or times that are not in the text.
If a field is unknown, use an empty string or empty array.
utc must be ISO-8601 UTC like 2026-09-03T03:00:00Z, or "".
exchange must be one of: binance, bitget, gate, unknown.
venue is only the trading book:
- {exchange}-futures or {exchange}-coin-futures for perpetual / delivery / futures contracts
- {exchange}-margin for EVERYTHING else: spot, margin, loan, borrow, isolated, cross, portfolio margin
Never leave venue unknown when exchange_hint is known. Spot delist = {exchange}-margin.
action:
- delist = trading of that book will stop (spot/margin/futures)
- disable_margin / disable_loan = borrow or loan stops before full delist
- monitoring = watchlist / monitoring tag, not a confirmed delist
- other = only if none of the above
Emit at most one action per (venue, action, utc).
assets = ticker codes only (ICX, SCRT), not full names (ICON).
symbols = tradable pairs if stated (ICXUSDT); otherwise empty.
Use the exchange hint if the text does not name the venue."#;

#[derive(Debug, Clone)]
pub struct LlmEndpoint {
    pub label: String,
    pub api_url: String,
    pub api_key: String,
    pub model: String,
    pub reasoning_effort: String,
    pub extra_header: Option<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct LlmConfig {
    pub primary: LlmEndpoint,
    pub backup: LlmEndpoint,
}

#[derive(Debug, Clone)]
pub struct LlmExtractInput {
    pub exchange: String,
    pub id: String,
    pub title: String,
    pub url: String,
    pub published_ms: i64,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LlmExtract {
    pub relevant: bool,
    pub actions: Vec<LlmAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LlmAction {
    pub action: String,
    pub venue: String,
    pub exchange: String,
    pub utc: String,
    pub assets: Vec<String>,
    pub symbols: Vec<String>,
    #[serde(default)]
    pub note: String,
}

#[derive(Debug, Clone)]
pub struct LlmBudget {
    pub max: usize,
    pub used: usize,
}

impl LlmBudget {
    pub fn new(max: usize) -> Self {
        Self { max, used: 0 }
    }

    pub fn allow(&mut self) -> bool {
        if self.max == 0 || self.used < self.max {
            self.used += 1;
            true
        } else {
            false
        }
    }
}

impl LlmExtractInput {
    pub fn from_raw(item: &RawAnnouncement) -> Self {
        Self {
            exchange: item.exchange.clone(),
            id: item.id.clone(),
            title: item.title.clone(),
            url: item.url.clone(),
            published_ms: item.published_ms,
            body: body_from_raw(item),
        }
    }

    pub fn from_parsed(item: &ParsedAnnouncement) -> Self {
        Self {
            exchange: "binance".to_string(),
            id: item.code.clone(),
            title: item.title.clone(),
            url: item.url.clone(),
            published_ms: item.release_date_ms,
            body: item.body_text.clone(),
        }
    }
}

impl LlmConfig {
    pub fn from_env() -> Option<Self> {
        let api_url = first_env(&["DELIST_LLM_API_URL", "OPENAI_BASE_URL", "OPENAI_API_URL"])?;
        let api_key = first_env(&["DELIST_LLM_API_KEY", "OPENAI_API_KEY"])?;
        if api_url.trim().is_empty() || api_key.trim().is_empty() {
            return None;
        }
        let model = env_or("DELIST_LLM_MODEL", DEFAULT_MODEL);
        let effort = env_or("DELIST_LLM_REASONING_EFFORT", DEFAULT_EFFORT);
        let header = parse_header(
            &std::env::var("DELIST_LLM_HTTP_HEADER").unwrap_or_else(|_| DEFAULT_HEADER.to_string()),
        );
        let primary = LlmEndpoint {
            label: "primary".to_string(),
            api_url: api_url.clone(),
            api_key: api_key.clone(),
            model: model.clone(),
            reasoning_effort: effort.clone(),
            extra_header: header.clone(),
        };
        let backup = LlmEndpoint {
            label: "backup".to_string(),
            api_url: std::env::var("DELIST_LLM_BACKUP_API_URL").unwrap_or(api_url),
            api_key: std::env::var("DELIST_LLM_BACKUP_API_KEY").unwrap_or(api_key),
            model: env_or("DELIST_LLM_BACKUP_MODEL", &model),
            reasoning_effort: env_or("DELIST_LLM_BACKUP_REASONING_EFFORT", &effort),
            extra_header: header,
        };
        Some(Self { primary, backup })
    }

    pub fn http_client() -> Result<Client> {
        Client::builder()
            .timeout(Duration::from_secs(180))
            .user_agent("mkt_signal-delist-llm/0.1")
            .build()
            .context("build LLM HTTP client failed")
    }

    pub async fn extract(&self, client: &Client, input: &LlmExtractInput) -> Result<Value> {
        match extract_with(client, &self.primary, input).await {
            Ok(value) => Ok(value),
            Err(primary_err) => {
                log::warn!(
                    "llm primary failed id={} model={}: {primary_err:#}",
                    input.id,
                    self.primary.model
                );
                extract_with(client, &self.backup, input)
                    .await
                    .with_context(|| format!("llm backup also failed id={}", input.id))
            }
        }
    }
}

pub async fn emit_llm_extract(client: &Client, llm: &LlmConfig, input: &LlmExtractInput) {
    let _ = extract_for_emit(client, llm, input).await;
}

pub async fn extract_for_emit(
    client: &Client,
    llm: &LlmConfig,
    input: &LlmExtractInput,
) -> Result<Value> {
    match llm.extract(client, input).await {
        Ok(value) => {
            let relevant = value
                .get("relevant")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let n = value
                .get("actions")
                .and_then(|v| v.as_array())
                .map(|v| v.len())
                .unwrap_or(0);
            log::info!(
                "llm extract exchange={} id={} relevant={relevant} actions={n} title={}",
                input.exchange,
                input.id,
                input.title
            );
            println!("{value}");
            Ok(value)
        }
        Err(err) => {
            log::warn!("llm extract skipped id={}: {err:#}", input.id);
            Err(err)
        }
    }
}

fn body_from_raw(item: &RawAnnouncement) -> String {
    let Some(extra) = item.extra.as_ref() else {
        return String::new();
    };
    for key in ["brief", "annDesc", "body"] {
        if let Some(text) = extra.get(key).and_then(Value::as_str) {
            if !text.trim().is_empty() {
                return text.to_string();
            }
        }
    }
    extra.to_string()
}

async fn extract_with(
    client: &Client,
    endpoint: &LlmEndpoint,
    input: &LlmExtractInput,
) -> Result<Value> {
    let url = responses_url(&endpoint.api_url);
    let mut req = client
        .post(&url)
        .bearer_auth(&endpoint.api_key)
        .header("Content-Type", "application/json");
    if let Some((name, value)) = &endpoint.extra_header {
        req = req.header(name, value);
    }
    let payload = json!({
        "model": endpoint.model,
        "store": false,
        "reasoning": { "effort": endpoint.reasoning_effort },
        "text": {
            "format": {
                "type": "json_schema",
                "name": "delist_extract",
                "strict": true,
                "schema": extract_schema(),
            }
        },
        "input": build_prompt(input),
    });
    let response = req
        .json(&payload)
        .send()
        .await
        .with_context(|| format!("request {} {url} failed", endpoint.label))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("read {} body failed", endpoint.label))?;
    if !status.is_success() {
        bail!(
            "{} responses failed: status={status} body={}",
            endpoint.label,
            clip(&body, 400)
        );
    }
    let parsed: Value =
        serde_json::from_str(&body).context("parse Responses JSON envelope failed")?;
    if parsed.get("status").and_then(|v| v.as_str()) == Some("failed") {
        bail!(
            "{} responses status=failed error={}",
            endpoint.label,
            parsed.get("error").cloned().unwrap_or(Value::Null)
        );
    }
    let text = output_text(&parsed).ok_or_else(|| {
        anyhow!(
            "{} responses had no output_text: {}",
            endpoint.label,
            clip(&body, 400)
        )
    })?;
    let mut extract: LlmExtract = serde_json::from_str(&text)
        .with_context(|| format!("parse extract JSON failed: {text}"))?;
    normalize_extract(input, &mut extract);
    Ok(json!({
        "source": "llm_extract",
        "provider": endpoint.label,
        "model": endpoint.model,
        "announcement_id": input.id,
        "title": input.title,
        "url": input.url,
        "published_ms": input.published_ms,
        "relevant": extract.relevant,
        "actions": extract.actions,
    }))
}

pub fn responses_url(base: &str) -> String {
    let trimmed = base.trim().trim_end_matches('/');
    if trimmed.ends_with("/responses") {
        trimmed.to_string()
    } else if trimmed.ends_with("/v1") {
        format!("{trimmed}/responses")
    } else {
        format!("{trimmed}/v1/responses")
    }
}

fn extract_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "relevant": { "type": "boolean" },
            "actions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": ["delist", "disable_margin", "disable_loan", "monitoring", "other"]
                        },
                        "venue": { "type": "string" },
                        "exchange": { "type": "string" },
                        "utc": { "type": "string" },
                        "assets": { "type": "array", "items": { "type": "string" } },
                        "symbols": { "type": "array", "items": { "type": "string" } },
                        "note": { "type": "string" }
                    },
                    "required": ["action", "venue", "exchange", "utc", "assets", "symbols", "note"]
                }
            }
        },
        "required": ["relevant", "actions"]
    })
}

fn build_prompt(input: &LlmExtractInput) -> String {
    format!(
        "{SYSTEM_PROMPT}\n\nexchange_hint={}\nannouncement_id={}\nurl={}\npublished_ms={}\ntitle={}\nbody:\n{}",
        input.exchange,
        input.id,
        input.url,
        input.published_ms,
        input.title,
        clip(&input.body, BODY_CHAR_LIMIT)
    )
}

fn output_text(envelope: &Value) -> Option<String> {
    if let Some(text) = envelope.get("output_text").and_then(|v| v.as_str()) {
        if !text.is_empty() {
            return Some(text.to_string());
        }
    }
    for item in envelope.get("output")?.as_array()? {
        if item.get("type").and_then(|v| v.as_str()) != Some("message") {
            continue;
        }
        for content in item.get("content")?.as_array()? {
            let ty = content.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if ty == "output_text" || ty == "text" {
                if let Some(text) = content.get("text").and_then(|v| v.as_str()) {
                    if !text.is_empty() {
                        return Some(text.to_string());
                    }
                }
            }
        }
    }
    None
}

fn normalize_extract(input: &LlmExtractInput, extract: &mut LlmExtract) {
    for action in &mut extract.actions {
        action.exchange = normalize_exchange(&action.exchange, &input.exchange);
        action.action = normalize_action(&action.action);
        action.venue = normalize_venue(&action.exchange, &action.venue, &action.action);
        let (assets, symbols) = split_assets_and_symbols(&action.assets, &action.symbols);
        action.assets = assets;
        action.symbols = symbols;
        action.utc = action.utc.trim().to_string();
    }
}

pub fn normalize_exchange(raw: &str, hint: &str) -> String {
    let key = raw.trim().to_ascii_lowercase();
    if key.contains("binance") {
        "binance".into()
    } else if key.contains("bitget") {
        "bitget".into()
    } else if key.contains("gate") {
        "gate".into()
    } else {
        match hint.trim().to_ascii_lowercase().as_str() {
            "binance" | "bitget" | "gate" => hint.trim().to_ascii_lowercase(),
            _ => "unknown".into(),
        }
    }
}

pub fn normalize_venue(exchange: &str, raw: &str, action: &str) -> String {
    let key = raw.trim().to_ascii_lowercase().replace('_', "-");
    let compact: String = key.chars().filter(|c| c.is_ascii_alphanumeric()).collect();
    if compact.contains("coinm") || compact.contains("coinfutures") || key.contains("coin-futures")
    {
        return format!("{exchange}-coin-futures");
    }
    if compact.contains("futures") || compact.contains("swap") || compact.contains("perpetual") {
        return format!("{exchange}-futures");
    }
    if compact.contains("margin") || compact.contains("spot") || compact.contains("loan") {
        return format!("{exchange}-margin");
    }
    match action {
        "disable_margin" | "disable_loan" | "delist" | "monitoring" | "other" => {
            if matches!(
                key.as_str(),
                "binance-margin"
                    | "binance-futures"
                    | "binance-coin-futures"
                    | "bitget-margin"
                    | "bitget-futures"
                    | "bitget-coin-futures"
                    | "gate-margin"
                    | "gate-futures"
            ) {
                key
            } else if exchange == "unknown" {
                "unknown".into()
            } else {
                // spot / loan / unnamed book → margin risk
                format!("{exchange}-margin")
            }
        }
        _ => {
            if exchange == "unknown" {
                "unknown".into()
            } else {
                format!("{exchange}-margin")
            }
        }
    }
}

fn normalize_action(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().as_str() {
        "delist" | "disable_margin" | "disable_loan" | "monitoring" | "other" => {
            raw.trim().to_ascii_lowercase()
        }
        other => other.to_string(),
    }
}

fn split_assets_and_symbols(assets: &[String], symbols: &[String]) -> (Vec<String>, Vec<String>) {
    let mut out_assets = Vec::new();
    let mut out_symbols = Vec::new();
    for item in assets.iter().chain(symbols.iter()) {
        let raw = item.trim();
        if raw.is_empty() || raw.chars().any(|c| c.is_ascii_whitespace()) {
            continue;
        }
        // Keep ticker-like tokens (ICX). Drop full names (ICON/Secret).
        let has_lower = raw.chars().any(|c| c.is_ascii_lowercase());
        let value = raw.to_ascii_uppercase();
        if value.contains('/') || value.contains('-') || looks_like_pair(&value) {
            push_unique(&mut out_symbols, value);
        } else if !has_lower
            && value.chars().all(|c| c.is_ascii_alphanumeric())
            && (2..=10).contains(&value.len())
        {
            push_unique(&mut out_assets, value);
        }
    }
    (out_assets, out_symbols)
}

fn looks_like_pair(token: &str) -> bool {
    token.len() >= 6
        && ["USDT", "USDC", "BUSD", "FDUSD", "BTC", "ETH"]
            .iter()
            .any(|quote| token.ends_with(quote) && token.len() > quote.len())
}

fn push_unique(out: &mut Vec<String>, value: String) {
    if !out.iter().any(|seen| seen == &value) {
        out.push(value);
    }
}

fn first_env(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        std::env::var(name)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    })
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn parse_header(raw: &str) -> Option<(String, String)> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let (name, value) = trimmed.split_once(':')?;
    let name = name.trim();
    let value = value.trim();
    if name.is_empty() || value.is_empty() {
        None
    } else {
        Some((name.to_string(), value.to_string()))
    }
}

fn clip(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    text.chars().take(max_chars).collect::<String>() + "..."
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn joins_responses_url() {
        assert_eq!(
            responses_url("http://127.0.0.1:8080"),
            "http://127.0.0.1:8080/v1/responses"
        );
        assert_eq!(
            responses_url("http://127.0.0.1:8080/v1/"),
            "http://127.0.0.1:8080/v1/responses"
        );
        assert_eq!(
            responses_url("http://127.0.0.1:8080/v1/responses"),
            "http://127.0.0.1:8080/v1/responses"
        );
    }

    #[test]
    fn normalizes_bitget_futures_alias() {
        assert_eq!(
            normalize_venue("bitget", "Bitget Futures", "delist"),
            "bitget-futures"
        );
        assert_eq!(
            normalize_venue("binance", "USD-M", "delist"),
            "binance-margin"
        );
        assert_eq!(
            normalize_venue("binance", "usd-m perpetual", "delist"),
            "binance-futures"
        );
        assert_eq!(
            normalize_venue("binance", "unknown", "delist"),
            "binance-margin"
        );
        assert_eq!(normalize_exchange("Bitget", "gate"), "bitget");
    }

    #[test]
    fn splits_pair_tokens_from_assets() {
        let (assets, symbols) = split_assets_and_symbols(
            &["ICON".into(), "ICX".into(), "Secret".into()],
            &["ICXUSDT".into(), "SCRT".into()],
        );
        assert_eq!(assets, vec!["ICON", "ICX", "SCRT"]);
        assert_eq!(symbols, vec!["ICXUSDT"]);
    }

    #[test]
    fn reads_message_output_text() {
        let envelope = json!({
            "output": [{
                "type": "reasoning"
            }, {
                "type": "message",
                "content": [{
                    "type": "output_text",
                    "text": "{\"relevant\":true,\"actions\":[]}"
                }]
            }]
        });
        assert_eq!(
            output_text(&envelope).as_deref(),
            Some("{\"relevant\":true,\"actions\":[]}")
        );
    }

    #[test]
    fn parse_header_splits_name_value() {
        assert_eq!(
            parse_header("x-openai-actor-authorization: local-image-extension"),
            Some((
                "x-openai-actor-authorization".into(),
                "local-image-extension".into()
            ))
        );
        assert!(parse_header("").is_none());
    }
}
