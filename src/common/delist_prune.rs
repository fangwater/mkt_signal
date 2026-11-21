//! Confirmed delist candidates and audited, compare-and-set Redis removal.

use anyhow::{bail, Context, Result};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

use crate::common::delist_accounts::{
    matched_symbols, pair_and_base, redis_keys, AccountSpec, RedisSite,
};
use crate::common::delist_risk::{normalize_symbol, RiskQueryResponse};
use crate::common::exchange_info::ListingIndex;

/// Which of the account's Redis lists a confirmed removal may touch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RemovalScope {
    /// Every account venue's catalog dropped the symbol: remove it everywhere.
    AllLists,
    /// Only one leg is gone (catalog removal, or a margin-only delist that a
    /// spot catalog never shows). The pair can no longer be arbitraged, but a
    /// catalog gap must not stop live trading, so a symbol still in any trade
    /// list is retained; otherwise it leaves the close-only lists.
    CloseOnly,
}

#[derive(Debug, Clone, Serialize)]
pub struct RedisRemovalCandidate {
    pub account_slug: String,
    pub exchange: String,
    pub redis_site: String,
    pub symbol: String,
    pub venues: Vec<String>,
    pub redis_keys: Vec<String>,
    pub scope: RemovalScope,
}

/// Outcome of reading an account's lists for one candidate.
#[derive(Debug)]
pub enum RemovalPreparation {
    Plan(RedisRemovalPlan),
    /// No list holds the symbol any more.
    Absent,
    /// A close-only candidate that a trade list still holds; never removed.
    StillTraded {
        key: String,
    },
}

/// Lists the strategy opens positions from. Every other list (dump,
/// delist_dump, pos_dump, unimmr_close) only closes or tracks positions.
fn is_trade_list(key: &str) -> bool {
    key.split(':').any(|part| part.ends_with("_trade_symbols"))
}

#[derive(Debug, Clone, Serialize)]
pub struct RedisKeyChange {
    pub key: String,
    pub before_count: usize,
    pub after_count: usize,
    pub removed_count: usize,
}

#[derive(Debug, Clone)]
pub struct RedisRemovalPlan {
    mutations: Vec<RedisMutation>,
    pub changes: Vec<RedisKeyChange>,
}

#[derive(Debug, Clone)]
struct RedisMutation {
    key: String,
    expected: String,
    replacement: String,
}

fn supports_removal(spec: &AccountSpec) -> bool {
    (matches!(
        spec.kind.as_str(),
        "funding_rate" | "intra_exchange" | "market_making"
    ) && matches!(spec.exchange.as_str(), "binance" | "bitget" | "gate"))
        || (spec.kind == "funding_rate" && spec.exchange == "bybit")
}

/// Symbols whose delist is confirmed for an account. `risk` must include past
/// events (`include_past`): a `delist` that has taken effect on one of the
/// account's venues confirms that leg even when the venue's catalog still lists
/// the symbol (Bybit margin delists, where the spot pair keeps trading).
/// Position flatness is checked by the caller before any removal.
pub fn confirmed_removal_candidates(
    accounts: &[AccountSpec],
    listings: &ListingIndex,
    risk: &RiskQueryResponse,
    universes: &BTreeMap<String, Result<BTreeSet<String>, String>>,
) -> Vec<RedisRemovalCandidate> {
    let mut out = Vec::new();
    for spec in accounts {
        if !supports_removal(spec) {
            continue;
        }
        let Some(Ok(symbols)) = universes.get(&spec.slug) else {
            continue;
        };
        let (keys, venues) = redis_keys(spec);
        let effective_delists: BTreeSet<String> = risk
            .exchanges
            .get(&spec.exchange)
            .into_iter()
            .flat_map(|bucket| &bucket.items)
            .filter(|event| {
                event.action == "delist"
                    && matches!(event.status.as_str(), "due" | "past")
                    && venues.contains(&event.venue)
            })
            .flat_map(|event| matched_symbols(event, symbols))
            .collect();
        for symbol in symbols {
            let catalog: Vec<String> = venues
                .iter()
                .map(|venue| listings.listing_for(venue, std::slice::from_ref(symbol), &[]))
                .collect();
            // An unavailable or expired catalog confirms nothing.
            if catalog.iter().any(|listing| listing == "unknown") {
                continue;
            }
            let scope = if catalog.iter().all(|listing| listing == "delisted") {
                RemovalScope::AllLists
            } else if catalog.iter().any(|listing| listing == "delisted")
                || effective_delists.contains(symbol)
            {
                RemovalScope::CloseOnly
            } else {
                continue;
            };
            out.push(RedisRemovalCandidate {
                account_slug: spec.slug.to_string(),
                exchange: spec.exchange.to_string(),
                redis_site: match spec.site {
                    RedisSite::Jp => "jp",
                    RedisSite::Sg => "sg",
                    RedisSite::Unsupported => continue,
                }
                .to_string(),
                symbol: symbol.clone(),
                venues: venues.clone(),
                redis_keys: keys.clone(),
                scope,
            });
        }
    }
    out
}

pub fn removal_universe_errors(
    accounts: &[AccountSpec],
    universes: &BTreeMap<String, Result<BTreeSet<String>, String>>,
) -> Vec<String> {
    let mut errors = Vec::new();
    for spec in accounts {
        if !supports_removal(spec) {
            continue;
        }
        match universes.get(&spec.slug) {
            Some(Ok(_)) => {}
            Some(Err(err)) => errors.push(format!("{}: {err}", spec.slug)),
            None => errors.push(format!("{}: Redis universe missing", spec.slug)),
        }
    }
    errors
}

pub async fn prepare_redis_removal(
    redis_url: &str,
    candidate: &RedisRemovalCandidate,
) -> Result<RemovalPreparation> {
    if candidate.redis_keys.is_empty() {
        return Ok(RemovalPreparation::Absent);
    }
    let client = redis::Client::open(redis_url).context("open Redis for delist removal")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("connect Redis for delist removal")?;
    let values: Vec<Option<String>> = redis::cmd("MGET")
        .arg(&candidate.redis_keys)
        .query_async(&mut conn)
        .await
        .context("read Redis symbol lists for delist removal")?;

    plan_removal(candidate, candidate.redis_keys.iter().zip(values))
}

/// Builds the compare-and-set plan from the lists as read.
fn plan_removal<'a>(
    candidate: &RedisRemovalCandidate,
    lists: impl Iterator<Item = (&'a String, Option<String>)>,
) -> Result<RemovalPreparation> {
    let mut mutations = Vec::new();
    let mut changes = Vec::new();
    for (key, raw) in lists {
        let Some(raw) = raw else {
            continue;
        };
        let Some((replacement, before_count, after_count)) =
            remove_symbol_from_json(&raw, &candidate.symbol)
                .with_context(|| format!("parse Redis symbol list key={key}"))?
        else {
            continue;
        };
        if candidate.scope == RemovalScope::CloseOnly && is_trade_list(key) {
            return Ok(RemovalPreparation::StillTraded { key: key.clone() });
        }
        changes.push(RedisKeyChange {
            key: key.clone(),
            before_count,
            after_count,
            removed_count: before_count.saturating_sub(after_count),
        });
        mutations.push(RedisMutation {
            key: key.clone(),
            expected: raw,
            replacement,
        });
    }
    if mutations.is_empty() {
        Ok(RemovalPreparation::Absent)
    } else {
        Ok(RemovalPreparation::Plan(RedisRemovalPlan {
            mutations,
            changes,
        }))
    }
}

pub async fn apply_redis_removal(redis_url: &str, plan: &RedisRemovalPlan) -> Result<()> {
    const CAS_SCRIPT: &str = r#"
for i = 1, #KEYS do
  if redis.call('GET', KEYS[i]) ~= ARGV[(i - 1) * 2 + 1] then
    return 0
  end
end
for i = 1, #KEYS do
  redis.call('SET', KEYS[i], ARGV[(i - 1) * 2 + 2])
end
return #KEYS
"#;
    let client = redis::Client::open(redis_url).context("open Redis for delist removal")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("connect Redis for delist removal")?;
    let mut command = redis::cmd("EVAL");
    command.arg(CAS_SCRIPT).arg(plan.mutations.len());
    for mutation in &plan.mutations {
        command.arg(&mutation.key);
    }
    for mutation in &plan.mutations {
        command.arg(&mutation.expected).arg(&mutation.replacement);
    }
    let changed: i64 = command
        .query_async(&mut conn)
        .await
        .context("atomically update Redis symbol lists")?;
    if changed != plan.mutations.len() as i64 {
        bail!("Redis symbol lists changed concurrently; retry on next catalog refresh");
    }
    Ok(())
}

fn remove_symbol_from_json(raw: &str, target: &str) -> Result<Option<(String, usize, usize)>> {
    let mut value: Value = serde_json::from_str(raw).context("invalid JSON")?;
    let normalized_target = normalize_symbol(target);
    let (before, after) = match &mut value {
        Value::Array(items) => {
            let before = items.len();
            items.retain(|item| {
                !item
                    .as_str()
                    .is_some_and(|item| symbol_key(item) == normalized_target)
            });
            (before, items.len())
        }
        Value::Object(items) => {
            let before = items.len();
            items.retain(|key, _| symbol_key(key) != normalized_target);
            (before, items.len())
        }
        _ => bail!("expected a JSON array or object"),
    };
    if before == after {
        return Ok(None);
    }
    Ok(Some((
        serde_json::to_string(&value).context("serialize updated Redis list")?,
        before,
        after,
    )))
}

fn symbol_key(raw: &str) -> String {
    match pair_and_base(raw) {
        (Some(pair), _) => pair,
        (None, Some(base)) => format!("{base}USDT"),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::delist_risk::{ExchangeRisk, RiskEventView};

    fn binance_intra_account() -> AccountSpec {
        AccountSpec {
            slug: "binance-intra-arb01".to_string(),
            alias: "binance mt".to_string(),
            exchange: "binance".to_string(),
            kind: "intra_exchange".to_string(),
            host: "local".to_string(),
            site: RedisSite::Jp,
            config_url: String::new(),
        }
    }

    #[test]
    fn removes_pair_and_base_from_array() {
        let raw = r#"["BTCUSDT","hei","HEI-USDT","ETHUSDT"]"#;
        let (updated, before, after) = remove_symbol_from_json(raw, "HEIUSDT").unwrap().unwrap();
        assert_eq!(before, 4);
        assert_eq!(after, 2);
        assert_eq!(updated, r#"["BTCUSDT","ETHUSDT"]"#);
    }

    #[test]
    fn removes_symbol_from_object() {
        let raw = r#"{"BTCUSDT":1,"HEIUSDT":2}"#;
        let (updated, before, after) = remove_symbol_from_json(raw, "HEIUSDT").unwrap().unwrap();
        assert_eq!((before, after), (2, 1));
        assert_eq!(updated, r#"{"BTCUSDT":1}"#);
    }

    #[test]
    fn leaves_unrelated_json_unchanged() {
        assert!(remove_symbol_from_json(r#"["BTCUSDT"]"#, "HEIUSDT")
            .unwrap()
            .is_none());
    }

    fn risk(events: Vec<(&str, &str, &str, &str)>) -> RiskQueryResponse {
        let mut exchanges: BTreeMap<String, ExchangeRisk> = BTreeMap::new();
        for (venue, action, status, asset) in events {
            let exchange = venue.split('-').next().unwrap().to_string();
            exchanges
                .entry(exchange.clone())
                .or_default()
                .items
                .push(RiskEventView {
                    exchange,
                    venue: venue.into(),
                    action: action.into(),
                    utc: "2026-11-18T12:00:00Z".into(),
                    status: status.into(),
                    assets: vec![asset.into()],
                    symbols: vec![],
                    note: String::new(),
                    source: String::new(),
                    title: String::new(),
                    url: String::new(),
                    announcement_id: String::new(),
                    listing: String::new(),
                });
        }
        RiskQueryResponse {
            ok: true,
            as_of_ms: 0,
            abnormal: false,
            count: 0,
            exchanges,
        }
    }

    fn candidate(scope: RemovalScope) -> RedisRemovalCandidate {
        RedisRemovalCandidate {
            account_slug: "gate_fr_arb01".into(),
            exchange: "gate".into(),
            redis_site: "jp".into(),
            symbol: "BGSCUSDT".into(),
            venues: vec!["gate-margin".into(), "gate-futures".into()],
            redis_keys: vec![],
            scope,
        }
    }

    #[test]
    fn one_gone_leg_is_close_only_and_every_gone_leg_is_all_lists() {
        let accounts = vec![binance_intra_account()];
        let universes = BTreeMap::from([(
            "binance-intra-arb01".to_string(),
            Ok(BTreeSet::from([
                "HEIUSDT".to_string(),
                "BTCUSDT".to_string(),
            ])),
        )]);
        let mut listings = ListingIndex::default();
        listings.insert_test("binance-margin", "BTCUSDT");
        listings.insert_test("binance-futures", "BTCUSDT");
        listings.insert_test("binance-futures", "HEIUSDT");
        let candidates =
            confirmed_removal_candidates(&accounts, &listings, &risk(vec![]), &universes);
        assert_eq!(candidates.len(), 1, "BTCUSDT is listed on both legs");
        assert_eq!(candidates[0].symbol, "HEIUSDT");
        assert_eq!(candidates[0].scope, RemovalScope::CloseOnly);

        let mut listings = ListingIndex::default();
        listings.insert_test("binance-margin", "BTCUSDT");
        listings.insert_test("binance-futures", "BTCUSDT");
        let candidates =
            confirmed_removal_candidates(&accounts, &listings, &risk(vec![]), &universes);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].scope, RemovalScope::AllLists);
        assert_eq!(
            candidates[0].venues,
            vec!["binance-margin", "binance-futures"]
        );
    }

    #[test]
    fn effective_margin_only_delist_confirms_a_leg_its_spot_catalog_still_lists() {
        let mut account = binance_intra_account();
        account.slug = "bybit_fr_arb01".into();
        account.exchange = "bybit".into();
        account.kind = "funding_rate".into();
        let universes = BTreeMap::from([(
            account.slug.clone(),
            Ok(BTreeSet::from(["BILLUSDT".to_string()])),
        )]);
        let mut listings = ListingIndex::default();
        listings.insert_test("bybit-margin", "BILLUSDT");
        listings.insert_test("bybit-futures", "BILLUSDT");
        let accounts = [account];
        let find =
            |events| confirmed_removal_candidates(&accounts, &listings, &risk(events), &universes);
        assert!(find(vec![]).is_empty(), "listed everywhere, no notice");
        assert!(
            find(vec![("bybit-margin", "delist", "upcoming", "BILL")]).is_empty(),
            "a scheduled delist is the dump path's job"
        );
        assert!(find(vec![("bybit-margin", "disable_loan", "past", "BILL")]).is_empty());
        assert!(find(vec![("gate-margin", "delist", "past", "BILL")]).is_empty());
        for status in ["due", "past"] {
            let candidates = find(vec![("bybit-margin", "delist", status, "BILL")]);
            assert_eq!(candidates.len(), 1);
            assert_eq!(candidates[0].scope, RemovalScope::CloseOnly);
        }
    }

    #[test]
    fn close_only_removal_never_touches_a_symbol_still_traded() {
        let key = |list: &str| format!("gate_fr_arb01:fr_{list}:gate-margin_gate-futures");
        let lists = |trade: &str| {
            vec![
                (
                    key("dump_symbols"),
                    Some(r#"["BGSCUSDT","ETHUSDT"]"#.to_string()),
                ),
                (
                    key("unimmr_close_symbols"),
                    Some(r#"["BGSCUSDT"]"#.to_string()),
                ),
                (key("fwd_trade_symbols"), Some(trade.to_string())),
            ]
        };
        let plan = |scope, trade: &str| {
            let rows = lists(trade);
            plan_removal(&candidate(scope), rows.iter().map(|(k, v)| (k, v.clone()))).unwrap()
        };
        match plan(RemovalScope::CloseOnly, r#"["BGSCUSDT"]"#) {
            RemovalPreparation::StillTraded { key: traded } => {
                assert_eq!(traded, key("fwd_trade_symbols"))
            }
            other => panic!("expected StillTraded, got {other:?}"),
        }
        let RemovalPreparation::Plan(close_only) = plan(RemovalScope::CloseOnly, r#"["ETHUSDT"]"#)
        else {
            panic!("close-only lists should change");
        };
        assert_eq!(
            close_only
                .changes
                .iter()
                .map(|c| c.key.clone())
                .collect::<Vec<_>>(),
            vec![key("dump_symbols"), key("unimmr_close_symbols")]
        );
        let RemovalPreparation::Plan(all) = plan(RemovalScope::AllLists, r#"["BGSCUSDT"]"#) else {
            panic!("all-lists removal should change");
        };
        assert_eq!(
            all.changes.len(),
            3,
            "a fully delisted symbol leaves trade lists too"
        );
        assert!(matches!(
            plan(RemovalScope::CloseOnly, "[]"),
            RemovalPreparation::Plan(_)
        ));
        assert!(is_trade_list("mm_trade_symbols:binance-futures"));
        assert!(is_trade_list("x:intra_bwd_trade_symbols:binance"));
        assert!(!is_trade_list(
            "x:fr_unimmr_close_symbols:gate-margin_gate-futures"
        ));
    }

    #[test]
    fn bybit_fr_removal_requires_both_catalogs_and_preserves_intra() {
        let mut account = binance_intra_account();
        account.slug = "bybit_fr_arb01".into();
        account.exchange = "bybit".into();
        account.kind = "funding_rate".into();
        account.site = RedisSite::Sg;
        let universes =
            BTreeMap::from([(account.slug.clone(), Ok(BTreeSet::from(["HEIUSDT".into()])))]);
        let mut listings = ListingIndex::default();
        listings.insert_test("bybit-margin", "BTCUSDT");
        let none = risk(vec![]);
        assert!(
            confirmed_removal_candidates(&[account.clone()], &listings, &none, &universes)
                .is_empty(),
            "an unavailable futures catalog confirms nothing"
        );
        listings.insert_test("bybit-futures", "BTCUSDT");
        let candidates =
            confirmed_removal_candidates(&[account.clone()], &listings, &none, &universes);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].scope, RemovalScope::AllLists);
        assert_eq!(candidates[0].redis_site, "sg");
        assert_eq!(candidates[0].redis_keys.len(), 6);
        assert!(candidates[0]
            .redis_keys
            .iter()
            .all(|key| key.starts_with("bybit_fr_arb01:fr_")));
        assert_eq!(
            removal_universe_errors(&[account.clone()], &BTreeMap::new()).len(),
            1
        );
        account.kind = "intra_exchange".into();
        assert!(confirmed_removal_candidates(&[account], &listings, &none, &universes).is_empty());
    }

    #[test]
    fn supported_redis_error_blocks_pruning() {
        let accounts = vec![binance_intra_account()];
        let universes = BTreeMap::from([(
            "binance-intra-arb01".to_string(),
            Err("connection refused".to_string()),
        )]);
        let errors = removal_universe_errors(&accounts, &universes);
        assert!(errors
            .iter()
            .any(|error| error.contains("binance-intra-arb01: connection refused")));
    }
}
