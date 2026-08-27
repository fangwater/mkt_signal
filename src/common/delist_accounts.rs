//! Redis online-symbol universes for mounted books, intersected with /risk.

use anyhow::{Context, Result};
use redis::AsyncCommands;
use serde::Serialize;
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

#[derive(Debug, Clone, Copy)]
pub enum RedisSite {
    Jp,
    Sg,
}

#[derive(Debug, Clone)]
pub struct AccountSpec {
    pub slug: &'static str,
    pub alias: &'static str,
    pub exchange: &'static str,
    pub kind: &'static str,
    pub site: RedisSite,
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
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountRiskResponse {
    pub ok: bool,
    pub as_of_ms: i64,
    pub redis: BTreeMap<String, bool>,
    pub summary: BTreeMap<String, usize>,
    pub accounts: Vec<AccountRiskView>,
}

pub fn mounted_accounts() -> &'static [AccountSpec] {
    &[
        AccountSpec {
            slug: "binance_exec_trade01",
            alias: "binance CTA trade01",
            exchange: "binance",
            kind: "cta",
            site: RedisSite::Jp,
        },
        AccountSpec {
            slug: "okex_mm_alpha",
            alias: "okex 做市",
            exchange: "okex",
            kind: "market_making",
            site: RedisSite::Jp,
        },
        AccountSpec {
            slug: "binance-intra-arb01",
            alias: "binance mt",
            exchange: "binance",
            kind: "intra_exchange",
            site: RedisSite::Jp,
        },
        AccountSpec {
            slug: "binance_fr_arb04",
            alias: "binance 外部资金",
            exchange: "binance",
            kind: "funding_rate",
            site: RedisSite::Jp,
        },
        AccountSpec {
            slug: "binance_fr_arb03",
            alias: "binance 资费自营",
            exchange: "binance",
            kind: "funding_rate",
            site: RedisSite::Jp,
        },
        AccountSpec {
            slug: "gate_fr_arb02",
            alias: "gate资费外部资金",
            exchange: "gate",
            kind: "funding_rate",
            site: RedisSite::Jp,
        },
        AccountSpec {
            slug: "bitget_fr_arb02",
            alias: "bitget 资费自营",
            exchange: "bitget",
            kind: "funding_rate",
            site: RedisSite::Jp,
        },
        AccountSpec {
            slug: "gate_fr_arb01",
            alias: "gate 资费自营",
            exchange: "gate",
            kind: "funding_rate",
            site: RedisSite::Jp,
        },
        AccountSpec {
            slug: "bybit_mm_alpha",
            alias: "bybit做市",
            exchange: "bybit",
            kind: "market_making",
            site: RedisSite::Sg,
        },
        AccountSpec {
            slug: "bybit-intra-arb01",
            alias: "bybit mt",
            exchange: "bybit",
            kind: "intra_exchange",
            site: RedisSite::Sg,
        },
        AccountSpec {
            slug: "bybit-intra-arb02",
            alias: "bybit cta",
            exchange: "bybit",
            kind: "intra_exchange",
            site: RedisSite::Sg,
        },
    ]
}

pub fn redis_keys(spec: &AccountSpec) -> (Vec<String>, Vec<String>) {
    let ex = spec.exchange;
    match spec.kind {
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
        "cta" => (
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
    risk: &RiskQueryResponse,
    listings: &ListingIndex,
    universes: &BTreeMap<String, Result<BTreeSet<String>, String>>,
) -> Vec<AccountRiskView> {
    let mut out = Vec::new();
    for spec in mounted_accounts() {
        let (_keys, venues) = redis_keys(spec);
        let host = match spec.site {
            RedisSite::Jp => "jp",
            RedisSite::Sg => "sg",
        };
        let (redis_ok, redis_error, universe) = match universes.get(spec.slug) {
            Some(Ok(set)) => (true, None, set.clone()),
            Some(Err(err)) => (false, Some(err.clone()), BTreeSet::new()),
            None => (false, Some("redis not queried".into()), BTreeSet::new()),
        };
        let covered = matches!(spec.exchange, "binance" | "bitget" | "gate");
        let mut hits_by_symbol: BTreeMap<String, AccountHit> = BTreeMap::new();
        if redis_ok && covered {
            if let Some(bucket) = risk.exchanges.get(spec.exchange) {
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
    jp_url: &str,
    sg_url: Option<&str>,
) -> BTreeMap<String, Result<BTreeSet<String>, String>> {
    let mut jp_keys = Vec::new();
    let mut sg_keys = Vec::new();
    let mut owners: Vec<(String, RedisSite, Vec<String>)> = Vec::new();
    for spec in mounted_accounts() {
        let (keys, _venues) = redis_keys(spec);
        match spec.site {
            RedisSite::Jp => jp_keys.extend(keys.iter().cloned()),
            RedisSite::Sg => sg_keys.extend(keys.iter().cloned()),
        }
        owners.push((spec.slug.to_string(), spec.site, keys));
    }
    let jp = mget_strings(jp_url, &jp_keys).await;
    let sg = match sg_url {
        Some(url) if !url.trim().is_empty() => mget_strings(url, &sg_keys).await,
        _ => Err(anyhow::anyhow!("sg redis not configured")),
    };
    let jp_map = to_map(&jp_keys, jp);
    let sg_map = to_map(&sg_keys, sg);
    let mut out = BTreeMap::new();
    for (slug, site, keys) in owners {
        let source = match site {
            RedisSite::Jp => &jp_map,
            RedisSite::Sg => &sg_map,
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
}
