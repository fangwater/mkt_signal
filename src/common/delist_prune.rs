//! Confirmed delist candidates and audited, compare-and-set Redis removal.

use anyhow::{bail, Context, Result};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

use crate::common::delist_accounts::{mounted_accounts, pair_and_base, redis_keys, RedisSite};
use crate::common::delist_risk::normalize_symbol;
use crate::common::exchange_info::ListingIndex;

#[derive(Debug, Clone, Serialize)]
pub struct RedisRemovalCandidate {
    pub account_slug: String,
    pub exchange: String,
    pub redis_site: String,
    pub symbol: String,
    pub venues: Vec<String>,
    pub redis_keys: Vec<String>,
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

pub fn confirmed_removal_candidates(
    listings: &ListingIndex,
    universes: &BTreeMap<String, Result<BTreeSet<String>, String>>,
) -> Vec<RedisRemovalCandidate> {
    let mut out = Vec::new();
    for spec in mounted_accounts() {
        if !matches!(
            spec.kind,
            "funding_rate" | "intra_exchange" | "market_making"
        ) || !matches!(spec.exchange, "binance" | "bitget" | "gate")
        {
            continue;
        }
        let Some(Ok(symbols)) = universes.get(spec.slug) else {
            continue;
        };
        let (keys, venues) = redis_keys(spec);
        for symbol in symbols {
            let removed_venues: Vec<String> = venues
                .iter()
                .filter(|venue| {
                    listings.listing_for(venue, std::slice::from_ref(symbol), &[]) == "delisted"
                })
                .cloned()
                .collect();
            if !removed_venues.is_empty() {
                out.push(RedisRemovalCandidate {
                    account_slug: spec.slug.to_string(),
                    exchange: spec.exchange.to_string(),
                    redis_site: match spec.site {
                        RedisSite::Jp => "jp",
                        RedisSite::Sg => "sg",
                    }
                    .to_string(),
                    symbol: symbol.clone(),
                    venues: removed_venues,
                    redis_keys: keys.clone(),
                });
            }
        }
    }
    out
}

pub fn removal_universe_errors(
    universes: &BTreeMap<String, Result<BTreeSet<String>, String>>,
) -> Vec<String> {
    let mut errors = Vec::new();
    for spec in mounted_accounts() {
        if !matches!(
            spec.kind,
            "funding_rate" | "intra_exchange" | "market_making"
        ) || !matches!(spec.exchange, "binance" | "bitget" | "gate")
        {
            continue;
        }
        match universes.get(spec.slug) {
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
) -> Result<Option<RedisRemovalPlan>> {
    if candidate.redis_keys.is_empty() {
        return Ok(None);
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

    let mut mutations = Vec::new();
    let mut changes = Vec::new();
    for (key, raw) in candidate.redis_keys.iter().zip(values) {
        let Some(raw) = raw else {
            continue;
        };
        let Some((replacement, before_count, after_count)) =
            remove_symbol_from_json(&raw, &candidate.symbol)
                .with_context(|| format!("parse Redis symbol list key={key}"))?
        else {
            continue;
        };
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
        Ok(None)
    } else {
        Ok(Some(RedisRemovalPlan { mutations, changes }))
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

    #[test]
    fn candidate_requires_at_least_one_account_venue_to_be_gone() {
        let mut listings = ListingIndex::default();
        listings.insert_test("binance-margin", "BTCUSDT");
        listings.insert_test("binance-futures", "BTCUSDT");
        listings.insert_test("binance-futures", "HEIUSDT");
        let universes = BTreeMap::from([(
            "binance-intra-arb01".to_string(),
            Ok(BTreeSet::from(["HEIUSDT".to_string()])),
        )]);
        let candidates = confirmed_removal_candidates(&listings, &universes);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].venues, vec!["binance-margin"]);

        let mut listings = ListingIndex::default();
        listings.insert_test("binance-margin", "HEIUSDT");
        listings.insert_test("binance-futures", "HEIUSDT");
        assert!(confirmed_removal_candidates(&listings, &universes).is_empty());
    }

    #[test]
    fn supported_redis_error_blocks_pruning() {
        let universes = BTreeMap::from([(
            "binance-intra-arb01".to_string(),
            Err("connection refused".to_string()),
        )]);
        let errors = removal_universe_errors(&universes);
        assert!(errors
            .iter()
            .any(|error| error.contains("binance-intra-arb01: connection refused")));
    }
}
