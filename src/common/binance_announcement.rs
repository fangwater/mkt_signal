//! Binance 下架 / Monitoring Tag 公告：CMS 回补 + 官方快照 + 公告 WS。
//!
//! 官方 Announcements 产品只有推送、没有历史回放。列表接口也没有正文，
//! 必须再打 `article/detail/query?articleCode=` 拿 JSON AST 再展平。

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use hmac::{Hmac, Mac};
use log::{debug, info, warn};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

type HmacSha256 = Hmac<Sha256>;

pub const CMS_LIST_URL: &str =
    "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query";
pub const CMS_DETAIL_URL: &str =
    "https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query";
pub const ANNOUNCEMENT_WS_BASE: &str = "wss://api.binance.com/sapi/wss";
pub const ANNOUNCEMENT_TOPIC: &str = "com_announcement_en";
pub const CATALOG_DELISTING: i64 = 161;
pub const CATALOG_LATEST_NEWS: i64 = 49;
pub const ARTICLE_URL_PREFIX: &str = "https://www.binance.com/en/support/announcement/detail/";

const DEFAULT_USER_AGENT: &str =
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

const STOP_TOKENS: &[&str] = &[
    "AND",
    "THE",
    "FOR",
    "FROM",
    "WITH",
    "THIS",
    "THAT",
    "WILL",
    "NOTICE",
    "REMOVAL",
    "SPOT",
    "MARGIN",
    "FUTURES",
    "TRADING",
    "PAIRS",
    "PAIR",
    "PERPETUAL",
    "CONTRACT",
    "CONTRACTS",
    "TOKEN",
    "TOKENS",
    "LOAN",
    "EARN",
    "SIMPLE",
    "COPY",
    "NEWS",
    "LATEST",
    "INCLUDE",
    "EXTEND",
    "REMOVE",
    "MONITORING",
    "TAG",
    "SEED",
    "DELIST",
    "DELISTING",
    "BINANCE",
    "FELLOW",
    "BINANCIANS",
    "UTC",
    "GMT",
    "USD",
    "USDS",
    "COIN",
    "IMPORTANT",
    "UPDATE",
    "UPDATES",
    "MULTIPLE",
    "SELECTED",
    "VIA",
    "NEW",
    "ON",
    "OF",
    "TO",
    "OR",
    "AT",
    "IN",
    "IS",
    "AN",
    "NO",
    "NOT",
    "ARE",
    "AS",
    "UP",
    "AI",
    "PER",
    "LAST",
    "APPLY",
    "SIZE",
    "TICK",
    "RATE",
    "OWN",
    "DATE",
    "LIST",
    "CML",
    "TIERS",
    "ALPHA",
    "NOTE",
    "UNIMMR",
    "MMR",
    "ERC20",
    "ADL",
    "IOCO",
    "FAQ",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnnouncementKind {
    TokenDelist,
    SpotPairRemoval,
    MarginPairRemoval,
    MarginTokenDelist,
    FuturesDelist,
    MonitoringTagExtend,
    MonitoringTagRemove,
    SeedTagChange,
    OtherDelist,
    OtherNews,
}

impl AnnouncementKind {
    pub fn is_watch_relevant(self) -> bool {
        !matches!(self, Self::OtherNews)
    }
}

impl std::fmt::Display for AnnouncementKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::TokenDelist => "token_delist",
            Self::SpotPairRemoval => "spot_pair_removal",
            Self::MarginPairRemoval => "margin_pair_removal",
            Self::MarginTokenDelist => "margin_token_delist",
            Self::FuturesDelist => "futures_delist",
            Self::MonitoringTagExtend => "monitoring_tag_extend",
            Self::MonitoringTagRemove => "monitoring_tag_remove",
            Self::SeedTagChange => "seed_tag_change",
            Self::OtherDelist => "other_delist",
            Self::OtherNews => "other_news",
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CmsArticleRef {
    pub id: i64,
    pub code: String,
    pub title: String,
    pub catalog_id: i64,
    pub catalog_name: String,
    pub release_date_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParsedAnnouncement {
    pub code: String,
    pub title: String,
    pub catalog_id: i64,
    pub catalog_name: String,
    pub release_date_ms: i64,
    pub kind: AnnouncementKind,
    pub assets: Vec<String>,
    pub symbols: Vec<String>,
    pub dates: Vec<String>,
    pub url: String,
    pub body_text: String,
    pub source: String,
}

impl ParsedAnnouncement {
    pub fn article_url(code: &str) -> String {
        format!("{ARTICLE_URL_PREFIX}{code}")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WatchState {
    pub seen: BTreeMap<String, SeenArticle>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeenArticle {
    pub code: String,
    pub title: String,
    pub catalog_id: i64,
    pub release_date_ms: i64,
    pub kind: AnnouncementKind,
    pub assets: Vec<String>,
    pub symbols: Vec<String>,
    pub first_seen_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OfficialSnapshot {
    pub source: String,
    pub items: Vec<OfficialItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OfficialItem {
    pub key: String,
    pub when_ms: Option<i64>,
    pub detail: String,
}

#[derive(Debug, Deserialize)]
struct CmsEnvelope<T> {
    code: String,
    success: Option<bool>,
    message: Option<String>,
    data: Option<T>,
}

#[derive(Debug, Deserialize)]
struct CmsListData {
    catalogs: Vec<CmsCatalog>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CmsCatalog {
    catalog_id: i64,
    catalog_name: String,
    total: Option<i64>,
    articles: Vec<CmsListArticle>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CmsListArticle {
    id: i64,
    code: String,
    title: String,
    #[serde(default)]
    release_date: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CmsDetailData {
    #[serde(default)]
    body: Option<Value>,
    #[serde(default)]
    publish_date: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct WsFrame {
    #[serde(rename = "type")]
    frame_type: Option<String>,
    data: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WsAnnouncement {
    catalog_id: Option<i64>,
    catalog_name: Option<String>,
    publish_date: Option<i64>,
    title: Option<String>,
    body: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SpotDelistGroup {
    delist_time: i64,
    symbols: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarginDelistGroup {
    delist_time: i64,
    #[serde(default)]
    cross_margin_assets: Vec<String>,
    #[serde(default)]
    isolated_margin_symbols: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssetTagRow {
    asset_code: String,
    #[serde(default)]
    tags: Vec<String>,
}

pub fn http_client() -> Result<Client> {
    Client::builder()
        .timeout(Duration::from_secs(20))
        .user_agent(DEFAULT_USER_AGENT)
        .build()
        .context("build binance announcement HTTP client failed")
}

pub fn classify_title(title: &str, catalog_id: i64) -> AnnouncementKind {
    let lower = title.to_ascii_lowercase();
    if lower.contains("extend the monitoring tag") || lower.contains("monitoring tag to include") {
        return AnnouncementKind::MonitoringTagExtend;
    }
    if lower.contains("remove the monitoring tag")
        || (lower.contains("monitoring tag") && lower.contains("remove"))
    {
        return AnnouncementKind::MonitoringTagRemove;
    }
    if lower.contains("seed tag") {
        return AnnouncementKind::SeedTagChange;
    }
    if lower.contains("futures will delist")
        || (lower.contains("will delist")
            && (lower.contains("futures") || lower.contains("perpetual")))
    {
        return AnnouncementKind::FuturesDelist;
    }
    if lower.contains("removal of spot trading pairs") {
        return AnnouncementKind::SpotPairRemoval;
    }
    if lower.contains("removal of margin trading pairs") {
        return AnnouncementKind::MarginPairRemoval;
    }
    if lower.contains("margin and loan will delist")
        || lower.contains("margin will delist")
        || (lower.contains("margin") && lower.contains("will delist"))
    {
        return AnnouncementKind::MarginTokenDelist;
    }
    if lower.contains("will delist") || lower.contains("binance will delist") {
        return AnnouncementKind::TokenDelist;
    }
    if catalog_id == CATALOG_DELISTING {
        AnnouncementKind::OtherDelist
    } else {
        AnnouncementKind::OtherNews
    }
}

pub fn extract_iso_dates(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i + 10 <= bytes.len() {
        if is_digit(bytes[i])
            && is_digit(bytes[i + 1])
            && is_digit(bytes[i + 2])
            && is_digit(bytes[i + 3])
            && bytes[i + 4] == b'-'
            && is_digit(bytes[i + 5])
            && is_digit(bytes[i + 6])
            && bytes[i + 7] == b'-'
            && is_digit(bytes[i + 8])
            && is_digit(bytes[i + 9])
        {
            let mut end = i + 10;
            if end + 6 <= bytes.len()
                && bytes[end] == b' '
                && is_digit(bytes[end + 1])
                && is_digit(bytes[end + 2])
                && bytes[end + 3] == b':'
                && is_digit(bytes[end + 4])
                && is_digit(bytes[end + 5])
            {
                end += 6;
            }
            let token = text[i..end].to_string();
            if !out.contains(&token) {
                out.push(token);
            }
            i = end;
            continue;
        }
        i += 1;
    }
    out
}

fn is_digit(b: u8) -> bool {
    b.is_ascii_digit()
}

pub fn flatten_cms_text(value: &Value) -> String {
    let mut out = String::new();
    flatten_cms_text_into(value, &mut out);
    collapse_ws(&out)
}

fn flatten_cms_text_into(value: &Value, out: &mut String) {
    match value {
        Value::String(s) => {
            if let Ok(nested) = serde_json::from_str::<Value>(s) {
                flatten_cms_text_into(&nested, out);
            } else {
                push_text(out, s);
            }
        }
        Value::Array(items) => {
            for item in items {
                flatten_cms_text_into(item, out);
            }
        }
        Value::Object(map) => {
            if let Some(Value::String(text)) = map.get("text") {
                push_text(out, text);
            }
            if let Some(children) = map.get("child") {
                flatten_cms_text_into(children, out);
            }
            if matches!(
                map.get("tag").and_then(Value::as_str),
                Some("p" | "li" | "h1" | "h2" | "h3" | "br" | "div")
            ) && !out.ends_with('\n')
            {
                out.push('\n');
            }
        }
        _ => {}
    }
}

fn push_text(out: &mut String, text: &str) {
    if text.is_empty() {
        return;
    }
    if !out.is_empty() && !out.ends_with([' ', '\n']) && !text.starts_with([' ', ',', '.', ')']) {
        out.push(' ');
    }
    out.push_str(text);
}

fn collapse_ws(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut prev_space = false;
    for ch in text.chars() {
        if ch == '\n' {
            if !out.ends_with('\n') {
                out.push('\n');
            }
            prev_space = true;
            continue;
        }
        if ch.is_whitespace() {
            if !prev_space {
                out.push(' ');
                prev_space = true;
            }
            continue;
        }
        out.push(ch);
        prev_space = false;
    }
    out.trim().to_string()
}

pub fn extract_assets_and_symbols(text: &str) -> (Vec<String>, Vec<String>) {
    extract_named_tokens(text, true)
}

fn extract_named_tokens(text: &str, include_listed_assets: bool) -> (Vec<String>, Vec<String>) {
    let mut assets = BTreeSet::new();
    let mut symbols = BTreeSet::new();

    extract_parenthetical_tickers(text, &mut assets);
    if include_listed_assets {
        extract_listed_segment(text, &mut assets, &mut symbols);
    }
    extract_pair_tokens(text, &mut symbols);

    for symbol in &symbols {
        if let Some(base) = split_pair_base(symbol) {
            if !is_stop_token(base) {
                assets.insert(base.to_string());
            }
        }
    }

    (assets.into_iter().collect(), symbols.into_iter().collect())
}

fn extract_parenthetical_tickers(text: &str, assets: &mut BTreeSet<String>) {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'(' {
            let start = i + 1;
            let mut j = start;
            while j < bytes.len() && bytes[j] != b')' && j - start <= 12 {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b')' {
                let raw = &text[start..j];
                // 只要全大写 ticker：ICX / BTTC。跳过 uniMMR 这类术语。
                if raw.chars().any(|c| c.is_ascii_lowercase()) {
                    i += 1;
                    continue;
                }
                if let Some(token) = normalize_ticker(raw) {
                    if token.len() >= 2
                        && token.len() <= 8
                        && !is_stop_token(&token)
                        && !is_quote_asset(&token)
                        && token.chars().any(|c| c.is_ascii_alphabetic())
                    {
                        assets.insert(token);
                    }
                }
            }
        }
        i += 1;
    }
}

fn extract_listed_segment(
    text: &str,
    assets: &mut BTreeSet<String>,
    symbols: &mut BTreeSet<String>,
) {
    let lower = text.to_ascii_lowercase();
    let markers = [
        "will delist ",
        "to include ",
        "monitoring tag from ",
        "seed tag from ",
        "will remove the monitoring tag from ",
        "will remove the seed tag from ",
    ];
    for marker in markers {
        if let Some(pos) = lower.find(marker) {
            let rest = &text[pos + marker.len()..];
            let rest_lower = rest.to_ascii_lowercase();
            let cut = rest_lower
                .find(" on 20")
                .or_else(|| rest_lower.find(" perpetual"))
                .or_else(|| rest_lower.find(". "))
                .unwrap_or(rest.len());
            for raw in split_name_list(&rest[..cut]) {
                if let Some(token) = last_ticker_in(&raw) {
                    if is_trading_pair(&token) {
                        symbols.insert(token);
                    } else if token.len() >= 2
                        && token.len() <= 8
                        && !is_stop_token(&token)
                        && !is_quote_asset(&token)
                        && token.chars().any(|c| c.is_ascii_alphabetic())
                    {
                        assets.insert(token);
                    }
                }
            }
        }
    }
}

fn split_name_list(raw: &str) -> Vec<String> {
    raw.replace(" and ", ",")
        .replace(" AND ", ",")
        .replace('&', ",")
        .replace('，', ",")
        .split(',')
        .map(|part| part.trim().to_string())
        .filter(|part| !part.is_empty())
        .collect()
}

fn extract_pair_tokens(text: &str, symbols: &mut BTreeSet<String>) {
    let upper = text.to_ascii_uppercase();
    let bytes = upper.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if !is_ticker_char(bytes[i]) {
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len() && is_ticker_char(bytes[i]) {
            i += 1;
        }
        let token = &upper[start..i];
        if is_trading_pair(token) {
            symbols.insert(token.to_string());
        }
        if i < bytes.len() && (bytes[i] == b'/' || bytes[i] == b'-') && i + 3 < bytes.len() {
            let mut j = i + 1;
            while j < bytes.len() && is_ticker_char(bytes[j]) {
                j += 1;
            }
            let joined = format!("{}{}", &upper[start..i], &upper[i + 1..j]);
            if is_trading_pair(&joined) {
                symbols.insert(joined);
            }
        }
    }
}

fn is_ticker_char(b: u8) -> bool {
    b.is_ascii_uppercase() || b.is_ascii_digit()
}

fn last_ticker_in(raw: &str) -> Option<String> {
    let upper = raw.to_ascii_uppercase();
    let mut last = None;
    let bytes = upper.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if !is_ticker_char(bytes[i]) {
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len() && is_ticker_char(bytes[i]) {
            i += 1;
        }
        let token = &upper[start..i];
        if token.len() >= 2 && token.len() <= 20 {
            last = Some(token.to_string());
        }
    }
    last.or_else(|| normalize_ticker(raw))
}

fn normalize_ticker(raw: &str) -> Option<String> {
    let cleaned: String = raw
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_uppercase())
        .collect();
    if cleaned.len() < 2 || cleaned.len() > 20 {
        return None;
    }
    Some(cleaned)
}

fn is_stop_token(token: &str) -> bool {
    STOP_TOKENS.iter().any(|stop| *stop == token)
}

fn is_quote_asset(token: &str) -> bool {
    matches!(
        token,
        "USDT" | "USDC" | "BUSD" | "FDUSD" | "TUSD" | "BTC" | "ETH" | "BNB" | "EUR" | "USD"
    )
}

fn is_trading_pair(token: &str) -> bool {
    token.len() >= 6
        && [
            "USDT", "USDC", "BUSD", "FDUSD", "TUSD", "BTC", "ETH", "BNB", "EUR",
        ]
        .iter()
        .any(|quote| token.ends_with(quote) && token.len() > quote.len() + 1)
}

fn split_pair_base(symbol: &str) -> Option<&str> {
    for quote in [
        "USDT", "USDC", "BUSD", "FDUSD", "TUSD", "BTC", "ETH", "BNB", "EUR",
    ] {
        if let Some(base) = symbol.strip_suffix(quote) {
            if base.len() >= 2 {
                return Some(base);
            }
        }
    }
    None
}

pub fn parse_announcement(
    code: &str,
    title: &str,
    catalog_id: i64,
    catalog_name: &str,
    release_date_ms: i64,
    body: Option<&Value>,
    source: &str,
) -> ParsedAnnouncement {
    let kind = classify_title(title, catalog_id);
    let body_text = body.map(flatten_cms_text).unwrap_or_default();
    let mut blob = title.to_string();
    if !body_text.is_empty() {
        blob.push('\n');
        blob.push_str(&body_text);
    }
    let (mut assets, mut symbols) = extract_named_tokens(title, true);
    if !body_text.is_empty() {
        let (body_assets, body_symbols) = extract_named_tokens(&body_text, false);
        assets.extend(body_assets);
        symbols.extend(body_symbols);
    }
    assets.sort();
    assets.dedup();
    symbols.sort();
    symbols.dedup();
    let mut dates = extract_iso_dates(&blob);
    dates.sort();
    dates.dedup();
    ParsedAnnouncement {
        code: code.to_string(),
        title: title.to_string(),
        catalog_id,
        catalog_name: catalog_name.to_string(),
        release_date_ms,
        kind,
        assets,
        symbols,
        dates,
        url: ParsedAnnouncement::article_url(code),
        body_text,
        source: source.to_string(),
    }
}

pub fn announcement_is_interesting(title: &str, catalog_id: i64) -> bool {
    classify_title(title, catalog_id).is_watch_relevant()
}

pub async fn fetch_cms_page(
    client: &Client,
    catalog_id: i64,
    page_no: u32,
    page_size: u32,
) -> Result<(Vec<CmsArticleRef>, i64)> {
    let response = client
        .get(CMS_LIST_URL)
        .query(&[
            ("type", "1".to_string()),
            ("catalogId", catalog_id.to_string()),
            ("pageNo", page_no.to_string()),
            ("pageSize", page_size.to_string()),
        ])
        .header("Origin", "https://www.binance.com")
        .header("Referer", "https://www.binance.com/en/support/announcement")
        .send()
        .await
        .context("request Binance CMS article list failed")?;
    let status = response.status();
    let body = response.text().await.context("read CMS list body failed")?;
    if !status.is_success() {
        bail!("CMS list failed: status={status} body={body}");
    }
    let envelope: CmsEnvelope<CmsListData> =
        serde_json::from_str(&body).context("parse CMS list JSON failed")?;
    if envelope.code != "000000" && envelope.success != Some(true) {
        bail!(
            "CMS list rejected: code={} message={:?}",
            envelope.code,
            envelope.message
        );
    }
    let data = envelope
        .data
        .ok_or_else(|| anyhow!("CMS list missing data"))?;
    let catalog = data
        .catalogs
        .into_iter()
        .find(|catalog| catalog.catalog_id == catalog_id)
        .ok_or_else(|| anyhow!("CMS list missing catalog {catalog_id}"))?;
    let total = catalog.total.unwrap_or(catalog.articles.len() as i64);
    let articles = catalog
        .articles
        .into_iter()
        .map(|article| CmsArticleRef {
            id: article.id,
            code: article.code,
            title: article.title,
            catalog_id: catalog.catalog_id,
            catalog_name: catalog.catalog_name.clone(),
            release_date_ms: article.release_date,
        })
        .collect();
    Ok((articles, total))
}

pub async fn fetch_cms_detail(client: &Client, article_code: &str) -> Result<(Option<Value>, i64)> {
    let response = client
        .get(CMS_DETAIL_URL)
        .query(&[("articleCode", article_code)])
        .header("Origin", "https://www.binance.com")
        .header("Referer", "https://www.binance.com/en/support/announcement")
        .send()
        .await
        .context("request Binance CMS article detail failed")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("read CMS detail body failed")?;
    if !status.is_success() {
        bail!("CMS detail failed: status={status} body={body}");
    }
    let envelope: CmsEnvelope<CmsDetailData> =
        serde_json::from_str(&body).context("parse CMS detail JSON failed")?;
    if envelope.code != "000000" && envelope.success != Some(true) {
        bail!(
            "CMS detail rejected: code={} message={:?}",
            envelope.code,
            envelope.message
        );
    }
    let data = envelope
        .data
        .ok_or_else(|| anyhow!("CMS detail missing data for {article_code}"))?;
    Ok((data.body, data.publish_date.unwrap_or(0)))
}

pub async fn hydrate_article(
    client: &Client,
    article: &CmsArticleRef,
) -> Result<ParsedAnnouncement> {
    let (body, publish_date) = fetch_cms_detail(client, &article.code).await?;
    let release_date_ms = if article.release_date_ms > 0 {
        article.release_date_ms
    } else {
        publish_date
    };
    Ok(parse_announcement(
        &article.code,
        &article.title,
        article.catalog_id,
        &article.catalog_name,
        release_date_ms,
        body.as_ref(),
        "cms_detail",
    ))
}

pub async fn backfill_catalog(
    client: &Client,
    catalog_id: i64,
    page_size: u32,
    max_pages: u32,
    state: &WatchState,
    fetch_body: bool,
) -> Result<Vec<ParsedAnnouncement>> {
    let mut out = Vec::new();
    let mut page_no = 1u32;
    let mut total = i64::MAX;
    let mut fetched = 0i64;
    while page_no <= max_pages && fetched < total {
        let (articles, page_total) = fetch_cms_page(client, catalog_id, page_no, page_size).await?;
        total = page_total;
        if articles.is_empty() {
            break;
        }
        fetched += articles.len() as i64;
        info!(
            "CMS catalog={catalog_id} page={page_no} got={} total={total}",
            articles.len()
        );
        let mut hit_seen_tail = true;
        for article in articles {
            if state.seen.contains_key(&article.code) {
                continue;
            }
            hit_seen_tail = false;
            if catalog_id == CATALOG_LATEST_NEWS
                && !announcement_is_interesting(&article.title, catalog_id)
            {
                continue;
            }
            let parsed = if fetch_body {
                match hydrate_article(client, &article).await {
                    Ok(parsed) => parsed,
                    Err(err) => {
                        warn!(
                            "CMS detail failed for {} ({}): {err:#}",
                            article.code, article.title
                        );
                        parse_announcement(
                            &article.code,
                            &article.title,
                            article.catalog_id,
                            &article.catalog_name,
                            article.release_date_ms,
                            None,
                            "cms_list",
                        )
                    }
                }
            } else {
                parse_announcement(
                    &article.code,
                    &article.title,
                    article.catalog_id,
                    &article.catalog_name,
                    article.release_date_ms,
                    None,
                    "cms_list",
                )
            };
            out.push(parsed);
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if hit_seen_tail && !state.seen.is_empty() {
            debug!("CMS catalog={catalog_id} page={page_no} already seen, stop");
            break;
        }
        page_no += 1;
    }
    Ok(out)
}

pub fn parse_ws_frame(raw: &str) -> Result<Option<ParsedAnnouncement>> {
    let frame: WsFrame = serde_json::from_str(raw).context("parse announcement WS frame failed")?;
    if frame.frame_type.as_deref() != Some("DATA") {
        return Ok(None);
    }
    let data = match frame.data {
        Some(Value::String(s)) => serde_json::from_str::<WsAnnouncement>(&s)
            .context("parse announcement WS data string failed")?,
        Some(Value::Object(_)) => serde_json::from_value(frame.data.unwrap())
            .context("parse announcement WS data object failed")?,
        _ => return Ok(None),
    };
    let title = data.title.unwrap_or_default();
    if title.is_empty() {
        return Ok(None);
    }
    let catalog_id = data.catalog_id.unwrap_or(0);
    if !announcement_is_interesting(&title, catalog_id) {
        return Ok(None);
    }
    let code = slug_from_title_and_date(&title, data.publish_date.unwrap_or(0));
    Ok(Some(parse_announcement(
        &code,
        &title,
        catalog_id,
        data.catalog_name.as_deref().unwrap_or(""),
        data.publish_date.unwrap_or(0),
        data.body.as_ref(),
        "announcement_ws",
    )))
}

fn slug_from_title_and_date(title: &str, publish_date_ms: i64) -> String {
    let mut slug = String::new();
    for ch in title.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
        } else if !slug.ends_with('-') {
            slug.push('-');
        }
    }
    format!("{}-{}", slug.trim_matches('-'), publish_date_ms)
}

pub fn sign_query(params: &BTreeMap<&str, String>, secret: &str) -> Result<String> {
    let payload = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    sign_payload(&payload, secret)
}

pub fn sign_payload(payload: &str, secret: &str) -> Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| anyhow!("invalid binance secret"))?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn signed_announcement_ws_url(api_secret: &str, recv_window: u64) -> Result<String> {
    let timestamp = Utc::now().timestamp_millis();
    let random = uuid::Uuid::new_v4().simple().to_string();
    let mut params = BTreeMap::new();
    params.insert("random", random);
    params.insert("recvWindow", recv_window.to_string());
    params.insert("timestamp", timestamp.to_string());
    params.insert("topic", ANNOUNCEMENT_TOPIC.to_string());
    let payload = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let signature = sign_payload(&payload, api_secret)?;
    Ok(format!(
        "{ANNOUNCEMENT_WS_BASE}?{payload}&signature={signature}"
    ))
}

async fn signed_sapi_get(client: &Client, path: &str) -> Result<String> {
    let api_key = std::env::var("BINANCE_API_KEY").context("BINANCE_API_KEY is required")?;
    let api_secret =
        std::env::var("BINANCE_API_SECRET").context("BINANCE_API_SECRET is required")?;
    if api_key.trim().is_empty() || api_secret.trim().is_empty() {
        bail!("BINANCE_API_KEY/BINANCE_API_SECRET must not be empty");
    }
    let timestamp = Utc::now().timestamp_millis();
    let query = format!("timestamp={timestamp}");
    let signature = sign_payload(&query, &api_secret)?;
    let url = format!("https://api.binance.com{path}?{query}&signature={signature}");
    let response = client
        .get(url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .with_context(|| format!("request {path} failed"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("read {path} body failed"))?;
    if !status.is_success() {
        bail!("{path} failed: status={status} body={body}");
    }
    Ok(body)
}

pub async fn fetch_spot_delist_snapshot(client: &Client) -> Result<OfficialSnapshot> {
    let body = signed_sapi_get(client, "/sapi/v1/spot/delist-schedule").await?;
    let groups: Vec<SpotDelistGroup> =
        serde_json::from_str(&body).context("parse spot delist-schedule JSON failed")?;
    let mut items = Vec::new();
    for group in groups {
        for symbol in group.symbols {
            items.push(OfficialItem {
                key: symbol,
                when_ms: Some(group.delist_time),
                detail: "spot_delist_schedule".to_string(),
            });
        }
    }
    Ok(OfficialSnapshot {
        source: "binance_spot_delist_schedule".to_string(),
        items,
    })
}

pub async fn fetch_margin_delist_snapshot(client: &Client) -> Result<OfficialSnapshot> {
    let body = signed_sapi_get(client, "/sapi/v1/margin/delist-schedule").await?;
    let groups: Vec<MarginDelistGroup> =
        serde_json::from_str(&body).context("parse margin delist-schedule JSON failed")?;
    let mut items = Vec::new();
    for group in groups {
        for asset in group.cross_margin_assets {
            items.push(OfficialItem {
                key: asset,
                when_ms: Some(group.delist_time),
                detail: "cross_margin".to_string(),
            });
        }
        for symbol in group.isolated_margin_symbols {
            items.push(OfficialItem {
                key: symbol,
                when_ms: Some(group.delist_time),
                detail: "isolated_margin".to_string(),
            });
        }
    }
    Ok(OfficialSnapshot {
        source: "binance_margin_delist_schedule".to_string(),
        items,
    })
}

pub async fn fetch_monitoring_tag_snapshot(client: &Client) -> Result<OfficialSnapshot> {
    match fetch_asset_tags(client, Some("Monitoring")).await {
        Ok(items) if !items.is_empty() => {
            return Ok(OfficialSnapshot {
                source: "binance_spot_asset_tags".to_string(),
                items,
            });
        }
        Ok(_) => {
            warn!("asset/tags?tag=Monitoring returned empty, falling back to full tag scan");
        }
        Err(err) => {
            warn!("asset/tags?tag=Monitoring failed ({err:#}), falling back to full tag scan");
        }
    }
    let items = fetch_asset_tags(client, None)
        .await?
        .into_iter()
        .filter(|item| item.detail.to_ascii_lowercase().contains("monitor"))
        .collect();
    Ok(OfficialSnapshot {
        source: "binance_spot_asset_tags".to_string(),
        items,
    })
}

async fn fetch_asset_tags(client: &Client, tag: Option<&str>) -> Result<Vec<OfficialItem>> {
    let api_key = std::env::var("BINANCE_API_KEY").context("BINANCE_API_KEY is required")?;
    let api_secret =
        std::env::var("BINANCE_API_SECRET").context("BINANCE_API_SECRET is required")?;
    let timestamp = Utc::now().timestamp_millis();
    let mut params = BTreeMap::new();
    params.insert("timestamp", timestamp.to_string());
    if let Some(tag) = tag {
        params.insert("tag", tag.to_string());
    }
    let query = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let signature = sign_payload(&query, &api_secret)?;
    let url =
        format!("https://api.binance.com/sapi/v1/spot/asset/tags?{query}&signature={signature}");
    let response = client
        .get(url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .context("request spot asset tags failed")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("read spot asset tags body failed")?;
    if !status.is_success() {
        bail!("spot asset tags failed: status={status} body={body}");
    }
    let rows: Vec<AssetTagRow> =
        serde_json::from_str(&body).context("parse spot asset tags JSON failed")?;
    Ok(rows
        .into_iter()
        .map(|row| OfficialItem {
            key: row.asset_code,
            when_ms: None,
            detail: row.tags.join(","),
        })
        .collect())
}

pub fn load_state(path: &Path) -> Result<WatchState> {
    if !path.exists() {
        return Ok(WatchState::default());
    }
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read state file {} failed", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("parse state file {} failed", path.display()))
}

pub fn save_state(path: &Path, state: &WatchState) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create state dir {} failed", parent.display()))?;
        }
    }
    let tmp = PathBuf::from(format!("{}.tmp", path.display()));
    let raw = serde_json::to_string_pretty(state).context("serialize watch state failed")?;
    std::fs::write(&tmp, raw).with_context(|| format!("write {} failed", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {} failed", tmp.display(), path.display()))?;
    Ok(())
}

pub fn remember(state: &mut WatchState, parsed: &ParsedAnnouncement, now_ms: i64) -> bool {
    if state.seen.contains_key(&parsed.code) {
        return false;
    }
    state.seen.insert(
        parsed.code.clone(),
        SeenArticle {
            code: parsed.code.clone(),
            title: parsed.title.clone(),
            catalog_id: parsed.catalog_id,
            release_date_ms: parsed.release_date_ms,
            kind: parsed.kind,
            assets: parsed.assets.clone(),
            symbols: parsed.symbols.clone(),
            first_seen_ms: now_ms,
        },
    );
    true
}

pub fn format_event_line(parsed: &ParsedAnnouncement) -> String {
    serde_json::json!({
        "kind": parsed.kind,
        "code": parsed.code,
        "title": parsed.title,
        "catalog_id": parsed.catalog_id,
        "catalog_name": parsed.catalog_name,
        "release_date_ms": parsed.release_date_ms,
        "assets": parsed.assets,
        "symbols": parsed.symbols,
        "dates": parsed.dates,
        "url": parsed.url,
        "source": parsed.source,
        "body_preview": preview_body(&parsed.body_text, 240),
    })
    .to_string()
}

pub fn format_snapshot_line(snapshot: &OfficialSnapshot) -> String {
    serde_json::json!({
        "source": snapshot.source,
        "count": snapshot.items.len(),
        "items": snapshot.items,
    })
    .to_string()
}

fn preview_body(body: &str, max_chars: usize) -> String {
    let flattened: String = body.split_whitespace().collect::<Vec<_>>().join(" ");
    if flattened.chars().count() <= max_chars {
        return flattened;
    }
    flattened.chars().take(max_chars).collect::<String>() + "..."
}

pub fn datetime_from_millis(ms: i64) -> Option<DateTime<Utc>> {
    Utc.timestamp_millis_opt(ms).single()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn list_item_has_no_body_fields() {
        let raw = r#"{
            "id": 282887,
            "code": "d72915ed7a60473b92f0818d959a227a",
            "title": "Binance Will Delist ICX, SCRT, STORJ on 2026-09-03",
            "type": 1,
            "releaseDate": 1787205606608
        }"#;
        let article: CmsListArticle = serde_json::from_str(raw).unwrap();
        assert_eq!(article.code, "d72915ed7a60473b92f0818d959a227a");
        assert_eq!(article.release_date, 1787205606608);
    }

    #[test]
    fn classify_common_titles() {
        assert_eq!(
            classify_title(
                "Binance Will Delist ICX, SCRT, STORJ on 2026-09-03",
                CATALOG_DELISTING
            ),
            AnnouncementKind::TokenDelist
        );
        assert_eq!(
            classify_title(
                "Notice of Removal of Spot Trading Pairs - 2026-08-21",
                CATALOG_DELISTING
            ),
            AnnouncementKind::SpotPairRemoval
        );
        assert_eq!(
            classify_title(
                "Notice of Removal of Margin Trading Pairs - 2026-08-21",
                CATALOG_DELISTING
            ),
            AnnouncementKind::MarginPairRemoval
        );
        assert_eq!(
            classify_title(
                "Binance Futures Will Delist USDⓈ-M RVVUSDT and YALAUSDT Perpetual Contracts",
                CATALOG_DELISTING
            ),
            AnnouncementKind::FuturesDelist
        );
        assert_eq!(
            classify_title(
                "Binance Will Extend the Monitoring Tag to Include GLMR, ICX, MOVR, RARE & SOPH on 2026-08-11",
                CATALOG_LATEST_NEWS
            ),
            AnnouncementKind::MonitoringTagExtend
        );
        assert_eq!(
            classify_title("Binance Earn Yield Arena", CATALOG_LATEST_NEWS),
            AnnouncementKind::OtherNews
        );
        assert_eq!(
            classify_title(
                "Updates on Tick Size for Multiple USDⓈ-M Perpetual Futures Contracts (2026-08-24)",
                CATALOG_LATEST_NEWS
            ),
            AnnouncementKind::OtherNews
        );
        assert_eq!(
            classify_title(
                "Update on the Margin Tiers of USDⓈ-M Perpetual Contracts (2026-08-28)",
                CATALOG_LATEST_NEWS
            ),
            AnnouncementKind::OtherNews
        );
    }

    #[test]
    fn parse_token_delist_title() {
        let parsed = parse_announcement(
            "d72915ed7a60473b92f0818d959a227a",
            "Binance Will Delist ICX, SCRT, STORJ on 2026-09-03",
            CATALOG_DELISTING,
            "Delisting",
            1787205606608,
            None,
            "cms_list",
        );
        assert_eq!(parsed.kind, AnnouncementKind::TokenDelist);
        assert_eq!(parsed.assets, vec!["ICX", "SCRT", "STORJ"]);
        assert_eq!(parsed.dates, vec!["2026-09-03"]);
        assert!(parsed.body_text.is_empty());
    }

    #[test]
    fn flatten_cms_ast_and_extract_parentheticals() {
        let ast = json!({
            "node": "root",
            "child": [
                {
                    "node": "element",
                    "tag": "p",
                    "child": [{"node": "text", "text": "Binance will delist ICON (ICX), Secret (SCRT) and Storj (STORJ)."}]
                },
                {
                    "node": "element",
                    "tag": "p",
                    "child": [{"node": "text", "text": "Spot trading ceases 2026-09-03 03:00 (UTC)."}]
                }
            ]
        });
        let text = flatten_cms_text(&ast);
        assert!(text.contains("ICON (ICX)"));
        assert!(text.contains("2026-09-03 03:00"));
        let parsed = parse_announcement(
            "d72915ed7a60473b92f0818d959a227a",
            "Binance Will Delist ICX, SCRT, STORJ on 2026-09-03",
            CATALOG_DELISTING,
            "Delisting",
            1,
            Some(&ast),
            "cms_detail",
        );
        assert_eq!(parsed.assets, vec!["ICX", "SCRT", "STORJ"]);
        assert!(parsed.dates.iter().any(|d| d.starts_with("2026-09-03")));
        assert!(!parsed.body_text.is_empty());
    }

    #[test]
    fn ignores_unimmr_parenthetical() {
        let ast = json!({
            "node": "root",
            "child": [{"node": "text", "text": "monitor the Unified Maintenance Margin Ratio (uniMMR) closely"}]
        });
        let parsed = parse_announcement(
            "x",
            "Binance Will Delist ICX on 2026-09-03",
            CATALOG_DELISTING,
            "Delisting",
            1,
            Some(&ast),
            "cms_detail",
        );
        assert_eq!(parsed.assets, vec!["ICX"]);
        assert!(!parsed.assets.iter().any(|a| a == "UNIMMR"));
    }

    #[test]
    fn flatten_accepts_stringified_ast() {
        let ast =
            json!("{\"node\":\"root\",\"child\":[{\"node\":\"text\",\"text\":\"hello ICX\"}]}");
        assert!(flatten_cms_text(&ast).contains("hello ICX"));
    }

    #[test]
    fn parse_futures_title_symbols() {
        let parsed = parse_announcement(
            "abc",
            "Binance Futures Will Delist USDⓈ-M RVVUSDT and YALAUSDT Perpetual Contracts",
            CATALOG_DELISTING,
            "Delisting",
            1,
            None,
            "cms_list",
        );
        assert_eq!(parsed.kind, AnnouncementKind::FuturesDelist);
        assert_eq!(parsed.symbols, vec!["RVVUSDT", "YALAUSDT"]);
        assert!(parsed.assets.contains(&"RVV".to_string()));
        assert!(parsed.assets.contains(&"YALA".to_string()));
    }

    #[test]
    fn parse_ws_data_string() {
        let frame = r#"{
            "type":"DATA",
            "topic":"com_announcement_en",
            "data":"{\"catalogId\":161,\"catalogName\":\"Delisting\",\"publishDate\":1753257631403,\"title\":\"Binance Will Delist ICX, SCRT, STORJ on 2026-09-03\",\"body\":\"Spot trading ends 2026-09-03 03:00 (UTC)\"}"
        }"#;
        let parsed = parse_ws_frame(frame).unwrap().unwrap();
        assert_eq!(parsed.kind, AnnouncementKind::TokenDelist);
        assert_eq!(parsed.assets, vec!["ICX", "SCRT", "STORJ"]);
        assert_eq!(parsed.source, "announcement_ws");
    }

    #[test]
    fn parse_ws_ignores_command_and_unrelated_news() {
        assert!(parse_ws_frame(r#"{"type":"COMMAND","data":"SUCCESS"}"#)
            .unwrap()
            .is_none());
        let frame = r#"{
            "type":"DATA",
            "data":"{\"catalogId\":49,\"catalogName\":\"Latest Binance News\",\"publishDate\":1,\"title\":\"Binance Earn Yield Arena\"}"
        }"#;
        assert!(parse_ws_frame(frame).unwrap().is_none());
    }

    #[test]
    fn hmac_matches_openssl_announcement_sample() {
        let payload = "random=56724ac693184379ae23ffe5e910063c&topic=topic1&recvWindow=30000&timestamp=1753244327210";
        let secret = "Avqz4IQjoZSJOowMFSo3QZEd4ovfwLH7Kie8ZliTtP8ktDnqcX8bpCP7WluFtrfn";
        assert_eq!(
            sign_payload(payload, secret).unwrap(),
            "8346d214e0da7165a0093043395f67e08c63f61b5d6e25779d513c11450e691b"
        );
    }

    #[test]
    fn remember_is_idempotent() {
        let mut state = WatchState::default();
        let parsed = parse_announcement(
            "abc",
            "Binance Will Delist ICX on 2026-09-03",
            CATALOG_DELISTING,
            "Delisting",
            1,
            None,
            "cms_list",
        );
        assert!(remember(&mut state, &parsed, 10));
        assert!(!remember(&mut state, &parsed, 11));
        assert_eq!(state.seen.len(), 1);
    }

    #[test]
    fn extract_slash_pairs_from_body() {
        let (_, symbols) = extract_assets_and_symbols("Removal of ADA/USDT, SOL-BTC and XYZUSDT");
        assert!(symbols.contains(&"ADAUSDT".to_string()));
        assert!(symbols.contains(&"SOLBTC".to_string()));
        assert!(symbols.contains(&"XYZUSDT".to_string()));
    }
}
