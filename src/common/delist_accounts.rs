//! Redis online-symbol universes for NAV-configured strategies, intersected with /risk.

use anyhow::{Context, Result};
use redis::AsyncCommands;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use crate::common::delist_risk::{normalize_symbol, RiskEventView, RiskQueryResponse};
use crate::common::exchange_info::ListingIndex;

const FR_LISTS: [&str; 5] = [
    "dump_symbols",
    "pos_dump_symbols",
    "fwd_trade_symbols",
    "bwd_trade_symbols",
    "unimmr_close_symbols",
];
const INTRA_LISTS: [&str; 3] = ["dump_symbols", "fwd_trade_symbols", "bwd_trade_symbols"];
const QUOTES: [&str; 7] = ["USDT", "USDC", "BUSD", "FDUSD", "BTC", "ETH", "BNB"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisSite {
    Jp,
    Sg,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSpec {
    pub slug: String,
    pub alias: String,
    pub exchange: String,
    pub kind: String,
    pub host: String,
    pub site: RedisSite,
    /// NAV-provided per-env console/config URL (`/…/config`, absolute for SG).
    pub config_url: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NavStrategy {
    slug: String,
    #[serde(default)]
    alias: String,
    #[serde(default)]
    display_name: String,
    host: String,
    strategy_kind: String,
    exchange: String,
    #[serde(default)]
    config_url: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountHitEvent {
    pub venue: String,
    pub action: String,
    pub utc: String,
    pub status: String,
    pub listing: String,
    pub title: String,
    pub url: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountHit {
    pub symbol: String,
    pub listing: String,
    pub tone: String,
    pub events: Vec<AccountHitEvent>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountRiskView {
    pub slug: String,
    pub alias: String,
    pub exchange: String,
    pub kind: String,
    pub host: String,
    pub venues: Vec<String>,
    pub universe_n: usize,
    pub redis_ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redis_error: Option<String>,
    pub tone: String,
    pub risk_n: usize,
    pub hits: Vec<AccountHit>,
    pub symbols: Vec<String>,
    /// FR 盘 `fr_unimmr_close_symbols` 解析后的币对数；非 FR 或读取失败为 None。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unimmr_close_n: Option<usize>,
    /// 已确认 `fr_unimmr_close_symbols` 为空（key 缺失或列表无币对）。
    #[serde(default)]
    pub unimmr_empty: bool,
    /// Entry point to the env's viz dashboard (`/{ns}/<env>/` on that host's :4191).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub viz_url: Option<String>,
    /// Entry point to the env's config/console endpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_url: Option<String>,
}

/// Public front base for SG-hosted envs; JP envs stay origin-relative.
const SG_LINK_BASE: &str = "http://47.131.162.78:4191";

fn link_base(site: RedisSite) -> &'static str {
    match site {
        RedisSite::Sg => SG_LINK_BASE,
        _ => "",
    }
}

fn kind_ns(kind: &str) -> Option<&'static str> {
    match kind {
        "funding_rate" => Some("fr"),
        "intra_exchange" => Some("intra"),
        "market_making" => Some("mm"),
        "cross_exchange" => Some("cross"),
        _ => None,
    }
}

/// Entry points for one account env. The NAV `config_url` is authoritative
/// (handles SG absolute URLs and nonstandard consoles like the CTA manager);
/// when it is missing the standard `/{ns}/<env>` layout is derived.
pub fn env_urls(spec: &AccountSpec) -> (Option<String>, Option<String>) {
    let base = link_base(spec.site);
    let mut config = (!spec.config_url.is_empty()).then(|| spec.config_url.clone());
    let mut viz = config
        .as_deref()
        .and_then(|c| c.strip_suffix("/config"))
        .map(|c| format!("{c}/"));
    if config.is_none() {
        if spec.kind == "cta" {
            config = Some(format!("{base}/manager/account/?source={}", spec.slug));
        } else if let Some(ns) = kind_ns(&spec.kind) {
            config = Some(format!("{base}/{ns}/{}/config", spec.slug));
            viz = Some(format!("{base}/{ns}/{}/", spec.slug));
        }
    }
    (viz, config)
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountRiskResponse {
    pub ok: bool,
    pub as_of_ms: i64,
    pub nav_accounts_current: bool,
    pub redis: BTreeMap<String, bool>,
    pub summary: BTreeMap<String, usize>,
    pub accounts: Vec<AccountRiskView>,
    /// Most recent LLM extraction attempt (`last_attempt_ms` is the check time).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub llm: Option<crate::common::delist_store::LlmRunStatus>,
}

/// Marker returned inside the error chain when NAV rejects the session.
pub const NAV_UNAUTHORIZED: &str = "NAV strategies unauthorized";

/// Logs in to the NAV API and returns the `nav_session` token.
/// `login_url` is the `/api/auth/login` endpoint (e.g. via `/nav-api/`).
pub async fn nav_login(
    client: &Client,
    login_url: &str,
    username: &str,
    password: &str,
) -> Result<String> {
    let response = client
        .post(login_url)
        .json(&serde_json::json!({"username": username, "password": password}))
        .send()
        .await
        .with_context(|| format!("fetch NAV login {login_url}"))?
        .error_for_status()
        .with_context(|| format!("NAV login rejected {login_url}"))?;
    for value in response.headers().get_all(reqwest::header::SET_COOKIE) {
        let value = value.to_str().context("decode NAV login cookie")?;
        let Some((name, token)) = value
            .split(';')
            .next()
            .and_then(|pair| pair.split_once('='))
        else {
            continue;
        };
        if name.trim() == "nav_session" && !token.trim().is_empty() {
            return Ok(token.trim().to_string());
        }
    }
    anyhow::bail!("NAV login response missing nav_session cookie")
}

pub async fn fetch_nav_accounts(
    client: &Client,
    url: &str,
    session: Option<&str>,
) -> Result<Vec<AccountSpec>> {
    let mut request = client.get(url);
    if let Some(token) = session {
        request = request.header(reqwest::header::COOKIE, format!("nav_session={token}"));
    }
    let response = request
        .send()
        .await
        .with_context(|| format!("fetch NAV strategies {url}"))?;
    if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        anyhow::bail!(NAV_UNAUTHORIZED);
    }
    let strategies: Vec<NavStrategy> = response
        .error_for_status()
        .with_context(|| format!("NAV strategies returned an error {url}"))?
        .json()
        .await
        .with_context(|| format!("decode NAV strategies {url}"))?;
    if strategies.is_empty() {
        anyhow::bail!("NAV strategy catalog is empty");
    }
    let mut seen = BTreeSet::new();
    let mut accounts = Vec::with_capacity(strategies.len());
    for strategy in strategies {
        validate_nav_identifier("slug", &strategy.slug)?;
        let kind = strategy.strategy_kind.trim().to_ascii_lowercase();
        validate_nav_identifier("strategyKind", &kind)?;
        if !seen.insert(strategy.slug.clone()) {
            anyhow::bail!("duplicate NAV strategy slug {}", strategy.slug);
        }
        let host = strategy.host.trim().to_ascii_lowercase();
        let site = match host.as_str() {
            "local" | "jp" | "jp-meta-elvpn" => RedisSite::Jp,
            "sg" => RedisSite::Sg,
            _ => RedisSite::Unsupported,
        };
        let exchange = match strategy.exchange.trim().to_ascii_lowercase().as_str() {
            "okx" => "okex".to_string(),
            exchange => exchange.to_string(),
        };
        validate_nav_identifier("exchange", &exchange)?;
        let alias = if !strategy.alias.trim().is_empty() {
            strategy.alias
        } else if !strategy.display_name.trim().is_empty() {
            strategy.display_name
        } else {
            strategy.slug.clone()
        };
        accounts.push(AccountSpec {
            slug: strategy.slug,
            alias,
            exchange,
            kind,
            host,
            site,
            config_url: strategy.config_url,
        });
    }
    Ok(accounts)
}

fn validate_nav_identifier(field: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_'))
    {
        anyhow::bail!("invalid NAV strategy {field}: {value:?}");
    }
    Ok(())
}

pub fn redis_keys(spec: &AccountSpec) -> (Vec<String>, Vec<String>) {
    let ex = spec.exchange.as_str();
    match spec.kind.as_str() {
        "funding_rate" => {
            let suffix = format!("{ex}-margin_{ex}-futures");
            let keys = FR_LISTS
                .iter()
                .map(|list| format!("{}:fr_{list}:{suffix}", spec.slug))
                .collect();
            (keys, vec![format!("{ex}-margin"), format!("{ex}-futures")])
        }
        "intra_exchange" => {
            let keys = INTRA_LISTS
                .iter()
                .map(|list| format!("{}:intra_{list}:{ex}", spec.slug))
                .collect();
            (keys, vec![format!("{ex}-margin"), format!("{ex}-futures")])
        }
        "market_making" => (
            vec![format!("mm_trade_symbols:{ex}-futures")],
            vec![format!("{ex}-futures")],
        ),
        "cta" if ex == "binance" => (
            vec![format!("{}:binance-futures:exec:max_pos_u", spec.slug)],
            vec!["binance-futures".to_string()],
        ),
        _ => (Vec::new(), Vec::new()),
    }
}

pub fn pair_and_base(token: &str) -> (Option<String>, Option<String>) {
    let compact = normalize_symbol(token);
    if compact.is_empty() {
        return (None, None);
    }
    for quote in QUOTES {
        if compact.len() > quote.len() && compact.ends_with(quote) {
            let base = compact[..compact.len() - quote.len()].to_string();
            if !base.is_empty() {
                return (Some(compact), Some(base));
            }
        }
    }
    (None, Some(compact))
}

pub fn parse_universe(raw: &str) -> BTreeSet<String> {
    let parsed: serde_json::Value = match serde_json::from_str(raw) {
        Ok(value) => value,
        Err(_) => return BTreeSet::new(),
    };
    let items: Vec<String> = if let Some(map) = parsed.as_object() {
        map.keys().cloned().collect()
    } else if let Some(list) = parsed.as_array() {
        list.iter()
            .filter_map(|item| {
                item.as_str()
                    .map(ToOwned::to_owned)
                    .or_else(|| item.as_i64().map(|n| n.to_string()))
            })
            .collect()
    } else {
        return BTreeSet::new();
    };
    let mut out = BTreeSet::new();
    for item in items {
        match pair_and_base(&item) {
            (Some(pair), _) => {
                out.insert(pair);
            }
            (None, Some(base)) => {
                out.insert(format!("{base}USDT"));
            }
            _ => {}
        }
    }
    out
}

pub fn matched_symbols(event: &RiskEventView, universe: &BTreeSet<String>) -> Vec<String> {
    let mut pairs = Vec::new();
    for symbol in &event.symbols {
        if let (Some(pair), _) = pair_and_base(symbol) {
            if !pairs.iter().any(|seen| seen == &pair) {
                pairs.push(pair);
            }
        }
    }
    for asset in &event.assets {
        if let (Some(pair), _) = pair_and_base(asset) {
            if !pairs.iter().any(|seen| seen == &pair) {
                pairs.push(pair);
            }
        }
    }
    if !pairs.is_empty() {
        return pairs
            .into_iter()
            .filter(|pair| universe.contains(pair))
            .collect();
    }
    let mut hits = Vec::new();
    for asset in &event.assets {
        if let (_, Some(base)) = pair_and_base(asset) {
            let pair = format!("{base}USDT");
            if universe.contains(&pair) && !hits.iter().any(|seen| seen == &pair) {
                hits.push(pair);
            }
        }
    }
    hits
}

fn hit_tone(listing: &str, status: &str) -> &'static str {
    if listing == "pending" || listing == "delisted" {
        return "risk";
    }
    if matches!(status, "upcoming" | "due" | "unknown") {
        return "risk";
    }
    "ok"
}

pub fn build_account_views(
    accounts: &[AccountSpec],
    risk: &RiskQueryResponse,
    listings: &ListingIndex,
    universes: &BTreeMap<String, Result<BTreeSet<String>, String>>,
) -> Vec<AccountRiskView> {
    let mut out = Vec::new();
    for spec in accounts {
        let (_keys, venues) = redis_keys(spec);
        let host = match spec.site {
            RedisSite::Jp => "jp",
            RedisSite::Sg => "sg",
            RedisSite::Unsupported => spec.host.as_str(),
        };
        let (redis_ok, redis_error, universe) = match universes.get(&spec.slug) {
            Some(Ok(set)) => (true, None, set.clone()),
            Some(Err(err)) => (false, Some(err.clone()), BTreeSet::new()),
            None => (false, Some("redis not queried".into()), BTreeSet::new()),
        };
        let covered =
            !venues.is_empty() && matches!(spec.exchange.as_str(), "binance" | "bitget" | "gate");
        let mut hits_by_symbol: BTreeMap<String, AccountHit> = BTreeMap::new();
        if redis_ok && covered {
            if let Some(bucket) = risk.exchanges.get(&spec.exchange) {
                for event in &bucket.items {
                    if !venues.iter().any(|venue| venue == &event.venue) {
                        continue;
                    }
                    for symbol in matched_symbols(event, &universe) {
                        let listing =
                            listings.listing_for(&event.venue, std::slice::from_ref(&symbol), &[]);
                        let entry = hits_by_symbol.entry(symbol.clone()).or_insert(AccountHit {
                            symbol: symbol.clone(),
                            listing: listing.clone(),
                            tone: hit_tone(&listing, &event.status).to_string(),
                            events: Vec::new(),
                        });
                        if listing == "pending" || listing == "delisted" {
                            entry.listing = listing.clone();
                            entry.tone = "risk".to_string();
                        }
                        entry.events.push(AccountHitEvent {
                            venue: event.venue.clone(),
                            action: event.action.clone(),
                            utc: event.utc.clone(),
                            status: event.status.clone(),
                            listing,
                            title: event.title.clone(),
                            url: event.url.clone(),
                        });
                    }
                }
            }

            // The risk book only contains known delist notices. Also compare every
            // live Redis symbol with its venue catalog so an already-removed pair
            // is visible even when no announcement was captured.
            for symbol in &universe {
                for venue in &venues {
                    let listing = listings.listing_for(venue, std::slice::from_ref(symbol), &[]);
                    if listing != "delisted" {
                        continue;
                    }
                    let entry = hits_by_symbol.entry(symbol.clone()).or_insert(AccountHit {
                        symbol: symbol.clone(),
                        listing: listing.clone(),
                        tone: "risk".to_string(),
                        events: Vec::new(),
                    });
                    entry.listing = listing.clone();
                    entry.tone = "risk".to_string();
                    if entry
                        .events
                        .iter()
                        .any(|event| event.venue == *venue && event.listing == "delisted")
                    {
                        continue;
                    }
                    entry.events.push(AccountHitEvent {
                        venue: venue.clone(),
                        action: "catalog_removed".to_string(),
                        utc: String::new(),
                        status: "delisted".to_string(),
                        listing,
                        title: "not in current exchange catalog".to_string(),
                        url: String::new(),
                    });
                }
            }
        }
        let hits: Vec<AccountHit> = hits_by_symbol.into_values().collect();
        let risk_n = hits.iter().filter(|hit| hit.tone == "risk").count();
        let tone = if !redis_ok {
            "error"
        } else if !covered {
            "uncovered"
        } else if risk_n > 0 {
            "risk"
        } else {
            "ok"
        };
        let (viz_url, config_url) = env_urls(spec);
        out.push(AccountRiskView {
            slug: spec.slug.to_string(),
            alias: spec.alias.to_string(),
            exchange: spec.exchange.to_string(),
            kind: spec.kind.to_string(),
            host: host.to_string(),
            venues,
            universe_n: universe.len(),
            redis_ok,
            redis_error,
            tone: tone.to_string(),
            risk_n,
            hits,
            symbols: universe.into_iter().collect(),
            unimmr_close_n: None,
            unimmr_empty: false,
            viz_url,
            config_url,
        });
    }
    out
}

pub fn summarize(accounts: &[AccountRiskView]) -> BTreeMap<String, usize> {
    let mut summary = BTreeMap::from([
        ("accounts".into(), accounts.len()),
        ("risk".into(), 0),
        ("error".into(), 0),
        ("ok".into(), 0),
        ("uncovered".into(), 0),
    ]);
    for account in accounts {
        *summary.entry(account.tone.clone()).or_insert(0) += 1;
    }
    summary
}

pub async fn mget_strings(url: &str, keys: &[String]) -> Result<Vec<Option<String>>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let client = redis::Client::open(url).with_context(|| format!("open redis {url}"))?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .with_context(|| format!("connect redis {url}"))?;
    let values: Vec<Option<String>> = conn
        .mget(keys)
        .await
        .with_context(|| format!("mget redis {url}"))?;
    Ok(values)
}

pub async fn load_universes(
    accounts: &[AccountSpec],
    jp_url: &str,
    sg_url: Option<&str>,
) -> BTreeMap<String, Result<BTreeSet<String>, String>> {
    let mut jp_keys = Vec::new();
    let mut sg_keys = Vec::new();
    let mut owners: Vec<(String, RedisSite, Vec<String>)> = Vec::new();
    for spec in accounts {
        let (keys, _venues) = redis_keys(spec);
        match spec.site {
            RedisSite::Jp => jp_keys.extend(keys.iter().cloned()),
            RedisSite::Sg => sg_keys.extend(keys.iter().cloned()),
            RedisSite::Unsupported => {}
        }
        owners.push((spec.slug.clone(), spec.site, keys));
    }
    let jp = mget_strings(jp_url, &jp_keys).await;
    let sg = match sg_url {
        Some(url) if !url.trim().is_empty() => mget_strings(url, &sg_keys).await,
        _ if sg_keys.is_empty() => Ok(Vec::new()),
        _ => Err(anyhow::anyhow!("sg redis not configured")),
    };
    let jp_map = to_map(&jp_keys, jp);
    let sg_map = to_map(&sg_keys, sg);
    let mut out = BTreeMap::new();
    for (slug, site, keys) in owners {
        if keys.is_empty() {
            out.insert(slug, Ok(BTreeSet::new()));
            continue;
        }
        let source = match site {
            RedisSite::Jp => &jp_map,
            RedisSite::Sg => &sg_map,
            RedisSite::Unsupported => {
                out.insert(
                    slug,
                    Err("NAV strategy host has no Redis mapping".to_string()),
                );
                continue;
            }
        };
        match source {
            Err(err) => {
                out.insert(slug, Err(err.clone()));
            }
            Ok(values) => {
                let mut universe = BTreeSet::new();
                for key in keys {
                    if let Some(Some(raw)) = values.get(&key) {
                        universe.extend(parse_universe(raw));
                    }
                }
                out.insert(slug, Ok(universe));
            }
        }
    }
    out
}

/// One `fr_*` symbol list on a funding-rate account, e.g.
/// `{slug}:fr_unimmr_close_symbols:{exchange}-margin_{exchange}-futures`.
#[derive(Debug, Clone)]
pub struct FrSymbolListState {
    pub key: String,
    /// The Redis key existed; a missing key still yields an empty `symbols`.
    pub present: bool,
    pub symbols: BTreeSet<String>,
}

/// Loads one `fr_{list}` key for every `funding_rate` NAV account from its
/// site's Redis. A site-level read failure marks every account on that site;
/// a missing key yields `present=false` with an empty set.
pub async fn load_fr_symbol_list(
    accounts: &[AccountSpec],
    list: &str,
    jp_url: &str,
    sg_url: Option<&str>,
) -> BTreeMap<String, Result<FrSymbolListState, String>> {
    let mut jp_keys = Vec::new();
    let mut sg_keys = Vec::new();
    let mut owners: Vec<(String, RedisSite, String)> = Vec::new();
    for spec in accounts.iter().filter(|spec| spec.kind == "funding_rate") {
        let suffix = format!("{}-margin_{}-futures", spec.exchange, spec.exchange);
        let key = format!("{}:fr_{list}:{suffix}", spec.slug);
        match spec.site {
            RedisSite::Jp => jp_keys.push(key.clone()),
            RedisSite::Sg => sg_keys.push(key.clone()),
            RedisSite::Unsupported => {}
        }
        owners.push((spec.slug.clone(), spec.site, key));
    }
    let jp = mget_strings(jp_url, &jp_keys).await;
    let sg = match sg_url {
        Some(url) if !url.trim().is_empty() => mget_strings(url, &sg_keys).await,
        _ if sg_keys.is_empty() => Ok(Vec::new()),
        _ => Err(anyhow::anyhow!("sg redis not configured")),
    };
    let jp_map = to_map(&jp_keys, jp);
    let sg_map = to_map(&sg_keys, sg);
    let mut out = BTreeMap::new();
    for (slug, site, key) in owners {
        let source = match site {
            RedisSite::Jp => &jp_map,
            RedisSite::Sg => &sg_map,
            RedisSite::Unsupported => {
                out.insert(
                    slug,
                    Err("NAV strategy host has no Redis mapping".to_string()),
                );
                continue;
            }
        };
        let state = match source {
            Err(err) => Err(err.clone()),
            Ok(values) => {
                let raw = values.get(&key).and_then(|raw| raw.as_deref());
                Ok(FrSymbolListState {
                    key,
                    present: raw.is_some(),
                    symbols: raw.map(parse_universe).unwrap_or_default(),
                })
            }
        };
        out.insert(slug, state);
    }
    out
}

pub async fn load_fr_dump_symbols(
    accounts: &[AccountSpec],
    jp_url: &str,
    sg_url: Option<&str>,
) -> BTreeMap<String, Result<BTreeSet<String>, String>> {
    load_fr_symbol_list(accounts, "dump_symbols", jp_url, sg_url)
        .await
        .into_iter()
        .map(|(slug, state)| (slug, state.map(|state| state.symbols)))
        .collect()
}

fn to_map(
    keys: &[String],
    result: Result<Vec<Option<String>>, anyhow::Error>,
) -> Result<BTreeMap<String, Option<String>>, String> {
    match result {
        Ok(values) => Ok(keys.iter().cloned().zip(values).collect()),
        Err(err) => Err(format!("{err:#}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::delist_risk::RiskEventView;
    use axum::{routing::get, Json, Router};

    fn event(symbols: &[&str], assets: &[&str]) -> RiskEventView {
        RiskEventView {
            exchange: "binance".into(),
            venue: "binance-margin".into(),
            action: "delist".into(),
            utc: "2026-08-21T03:00:00Z".into(),
            status: "due".into(),
            assets: assets.iter().map(|s| s.to_string()).collect(),
            symbols: symbols.iter().map(|s| s.to_string()).collect(),
            note: String::new(),
            source: "llm_extract".into(),
            title: "t".into(),
            url: String::new(),
            announcement_id: "x".into(),
            listing: String::new(),
        }
    }

    #[test]
    fn pair_only_notice_does_not_flag_usdt_book() {
        let universe = BTreeSet::from(["SUIUSDT".to_string()]);
        let hits = matched_symbols(&event(&["SUIBNB", "HIVEUSDC"], &[]), &universe);
        assert!(hits.is_empty());
    }

    #[test]
    fn token_delist_matches_usdt_book() {
        let universe = BTreeSet::from(["ICXUSDT".to_string(), "BTCUSDT".to_string()]);
        let hits = matched_symbols(&event(&[], &["ICX", "SCRT"]), &universe);
        assert_eq!(hits, vec!["ICXUSDT"]);
    }

    #[test]
    fn usdc_margin_pairs_do_not_lift_usdt() {
        let universe = BTreeSet::from(["BEAMXUSDT".to_string()]);
        let hits = matched_symbols(&event(&["BEAMXUSDC", "CETUSUSDC"], &["BEAMX"]), &universe);
        assert!(hits.is_empty());
    }

    #[test]
    fn parses_max_pos_u_object() {
        let raw = r#"{"BNBUSDT":10000.0,"BTCUSDT":20000.0}"#;
        let set = parse_universe(raw);
        assert!(set.contains("BNBUSDT"));
        assert!(set.contains("BTCUSDT"));
    }

    #[tokio::test]
    async fn loads_accounts_from_nav_and_normalizes_local_metadata() {
        let app = Router::new().route(
            "/strategies",
            get(|| async {
                Json(serde_json::json!([
                    {
                        "slug": "bitget_fr_arb01",
                        "alias": "bitget arb01",
                        "displayName": "Bitget FR 01",
                        "host": "local",
                        "strategyKind": "funding_rate",
                        "exchange": "bitget"
                    },
                    {
                        "slug": "okex_mm_alpha",
                        "alias": "",
                        "displayName": "OKX MM",
                        "host": "sg",
                        "strategyKind": "market_making",
                        "exchange": "okx"
                    }
                ]))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let accounts = fetch_nav_accounts(
            &Client::new(),
            &format!("http://{address}/strategies"),
            None,
        )
        .await
        .unwrap();

        assert_eq!(accounts.len(), 2);
        assert_eq!(accounts[0].slug, "bitget_fr_arb01");
        assert_eq!(accounts[0].site, RedisSite::Jp);
        assert_eq!(accounts[1].alias, "OKX MM");
        assert_eq!(accounts[1].exchange, "okex");
        assert_eq!(accounts[1].site, RedisSite::Sg);
    }

    #[tokio::test]
    async fn rejects_empty_nav_catalog() {
        let app = Router::new().route("/strategies", get(|| async { Json(serde_json::json!([])) }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let error = fetch_nav_accounts(
            &Client::new(),
            &format!("http://{address}/strategies"),
            None,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("catalog is empty"));
    }
}
