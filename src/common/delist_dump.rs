//! Position-aware FR delist candidates and atomic Redis open-to-dump updates.

use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

use crate::common::delist_accounts::{
    matched_symbols, mounted_accounts, redis_keys, AccountHitEvent, RedisSite,
};
use crate::common::delist_risk::{normalize_symbol, RiskQueryResponse};

#[derive(Debug, Clone, Serialize)]
pub struct PositionDumpCandidate {
    pub account_slug: String,
    pub exchange: String,
    pub redis_site: String,
    pub symbol: String,
    pub event: AccountHitEvent,
    pub snapshot_ms: i64,
    pub open_usdt: f64,
    pub hedge_usdt: f64,
    pub impacted_position_usdt: f64,
    pub threshold_usdt: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RedisDumpChange {
    pub key: String,
    pub before_count: usize,
    pub after_count: usize,
    pub added_count: usize,
    pub removed_count: usize,
}

#[derive(Debug, Clone)]
pub struct RedisDumpPlan {
    mutations: Vec<RedisMutation>,
    pub changes: Vec<RedisDumpChange>,
}

#[derive(Debug, Clone)]
struct RedisMutation {
    key: String,
    expected: String,
    replacement: String,
}

#[derive(Debug, Deserialize)]
struct DashboardSnapshot {
    entries: Vec<DashboardEntry>,
}

#[derive(Debug, Deserialize)]
struct DashboardEntry {
    channel: String,
    entry: Value,
}

#[derive(Debug, Clone, Copy)]
struct PositionRow {
    snapshot_ms: i64,
    open_usdt: f64,
    hedge_usdt: f64,
}

pub async fn position_dump_candidates(
    client: &Client,
    snapshot_base_url: &str,
    risk: &RiskQueryResponse,
    threshold_usdt: f64,
    max_snapshot_age_ms: i64,
) -> (Vec<PositionDumpCandidate>, Vec<String>) {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut candidates = Vec::new();
    let mut errors = Vec::new();

    for spec in mounted_accounts().iter().filter(|spec| {
        spec.kind == "funding_rate" && matches!(spec.exchange, "binance" | "bitget" | "gate")
    }) {
        let Some(bucket) = risk.exchanges.get(spec.exchange) else {
            continue;
        };
        let (_keys, venues) = redis_keys(spec);
        let actionable_events = bucket.items.iter().filter(|event| {
            venues.iter().any(|venue| venue == &event.venue)
                && matches!(event.action.as_str(), "delist" | "disable_open")
                && matches!(event.status.as_str(), "upcoming" | "due")
                && !event.utc.is_empty()
        });
        let actionable_events: Vec<_> = actionable_events.collect();
        if actionable_events.is_empty() {
            continue;
        }
        let url = format!(
            "{}/fr/{}/snapshot",
            snapshot_base_url.trim_end_matches('/'),
            spec.slug
        );
        let snapshot = match fetch_snapshot(client, &url).await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                errors.push(format!("{}: {err:#}", spec.slug));
                continue;
            }
        };
        let Some((snapshot_ms, positions)) = positions_from_snapshot(&snapshot) else {
            errors.push(format!("{}: pre_trade_exposure missing", spec.slug));
            continue;
        };
        let age_ms = now_ms.saturating_sub(snapshot_ms);
        if age_ms < 0 || age_ms > max_snapshot_age_ms {
            errors.push(format!(
                "{}: stale position snapshot age_ms={age_ms}",
                spec.slug
            ));
            continue;
        }
        let position_symbols = positions.keys().cloned().collect::<BTreeSet<_>>();
        let mut events_by_symbol: BTreeMap<String, Vec<AccountHitEvent>> = BTreeMap::new();
        for event in actionable_events {
            for symbol in matched_symbols(event, &position_symbols) {
                events_by_symbol
                    .entry(symbol)
                    .or_default()
                    .push(AccountHitEvent {
                        venue: event.venue.clone(),
                        action: event.action.clone(),
                        utc: event.utc.clone(),
                        status: event.status.clone(),
                        listing: event.listing.clone(),
                        title: event.title.clone(),
                        url: event.url.clone(),
                    });
            }
        }

        for (symbol, mut actionable) in events_by_symbol {
            actionable.sort_by(|left, right| left.utc.cmp(&right.utc));
            let event = actionable.first().expect("group is non-empty");
            let position = positions.get(&symbol).expect("matched position symbol");
            let impacted_position_usdt = actionable.iter().fold(0.0_f64, |max, event| {
                let value = if event.venue.ends_with("-margin") {
                    position.open_usdt.abs()
                } else if event.venue.ends_with("-futures") {
                    position.hedge_usdt.abs()
                } else {
                    position.open_usdt.abs().max(position.hedge_usdt.abs())
                };
                max.max(value)
            });
            if !impacted_position_usdt.is_finite() || impacted_position_usdt < threshold_usdt {
                continue;
            }
            candidates.push(PositionDumpCandidate {
                account_slug: spec.slug.to_string(),
                exchange: spec.exchange.to_string(),
                redis_site: match spec.site {
                    RedisSite::Jp => "jp",
                    RedisSite::Sg => "sg",
                }
                .to_string(),
                symbol,
                event: event.clone(),
                snapshot_ms: position.snapshot_ms,
                open_usdt: position.open_usdt,
                hedge_usdt: position.hedge_usdt,
                impacted_position_usdt,
                threshold_usdt,
            });
        }
    }
    (candidates, errors)
}

async fn fetch_snapshot(client: &Client, url: &str) -> Result<DashboardSnapshot> {
    client
        .get(url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .with_context(|| format!("fetch position snapshot failed: {url}"))?
        .error_for_status()
        .with_context(|| format!("position snapshot returned error: {url}"))?
        .json()
        .await
        .with_context(|| format!("decode position snapshot failed: {url}"))
}

fn positions_from_snapshot(
    snapshot: &DashboardSnapshot,
) -> Option<(i64, BTreeMap<String, PositionRow>)> {
    let exposure = snapshot
        .entries
        .iter()
        .find(|entry| entry.channel == "pre_trade_exposure")?;
    let snapshot_ms = exposure.entry.get("ts_ms")?.as_i64()?;
    let mut positions = BTreeMap::new();
    for row in exposure.entry.get("rows")?.as_array()? {
        let Some(asset) = row.get("asset").and_then(Value::as_str) else {
            continue;
        };
        let asset = normalize_symbol(asset);
        if asset.is_empty() || asset == "TOTAL" {
            continue;
        }
        let Some(open_usdt) = row.get("open_usdt").and_then(Value::as_f64) else {
            continue;
        };
        let Some(hedge_usdt) = row.get("hedge_usdt").and_then(Value::as_f64) else {
            continue;
        };
        if !open_usdt.is_finite() || !hedge_usdt.is_finite() {
            continue;
        }
        positions.insert(
            format!("{asset}USDT"),
            PositionRow {
                snapshot_ms,
                open_usdt,
                hedge_usdt,
            },
        );
    }
    Some((snapshot_ms, positions))
}

pub async fn prepare_redis_dump(
    redis_url: &str,
    candidate: &PositionDumpCandidate,
) -> Result<Option<RedisDumpPlan>> {
    let suffix = format!(
        "{}-margin_{}-futures",
        candidate.exchange, candidate.exchange
    );
    let keys = [
        format!("{}:fr_dump_symbols:{suffix}", candidate.account_slug),
        format!("{}:fr_fwd_trade_symbols:{suffix}", candidate.account_slug),
        format!("{}:fr_bwd_trade_symbols:{suffix}", candidate.account_slug),
    ];
    let client = redis::Client::open(redis_url).context("open Redis for delist dump")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("connect Redis for delist dump")?;
    let values: Vec<Option<String>> = redis::cmd("MGET")
        .arg(&keys)
        .query_async(&mut conn)
        .await
        .context("read Redis FR symbol lists for delist dump")?;
    if values.iter().any(Option::is_none) {
        bail!("one or more FR dump/fwd/bwd Redis keys are missing");
    }

    let mut mutations = Vec::new();
    let mut changes = Vec::new();
    for (index, (key, raw)) in keys.into_iter().zip(values).enumerate() {
        let raw = raw.expect("checked above");
        let add = index == 0;
        let Some((replacement, before_count, after_count)) =
            update_symbol_list(&raw, &candidate.symbol, add)
                .with_context(|| format!("parse Redis symbol list key={key}"))?
        else {
            continue;
        };
        changes.push(RedisDumpChange {
            key: key.clone(),
            before_count,
            after_count,
            added_count: after_count.saturating_sub(before_count),
            removed_count: before_count.saturating_sub(after_count),
        });
        mutations.push(RedisMutation {
            key,
            expected: raw,
            replacement,
        });
    }
    if mutations.is_empty() {
        Ok(None)
    } else {
        Ok(Some(RedisDumpPlan { mutations, changes }))
    }
}

pub async fn apply_redis_dump(redis_url: &str, plan: &RedisDumpPlan) -> Result<()> {
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
    let client = redis::Client::open(redis_url).context("open Redis for delist dump")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("connect Redis for delist dump")?;
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
        .context("atomically move FR symbol from open lists to dump")?;
    if changed != plan.mutations.len() as i64 {
        bail!("Redis symbol lists changed concurrently; retry on next position scan");
    }
    Ok(())
}

fn update_symbol_list(
    raw: &str,
    target: &str,
    add: bool,
) -> Result<Option<(String, usize, usize)>> {
    let mut value: Value = serde_json::from_str(raw).context("invalid JSON")?;
    let items = value
        .as_array_mut()
        .context("expected a JSON array for FR symbol list")?;
    let before = items.len();
    let normalized_target = normalize_symbol(target);
    if add {
        if items.iter().any(|item| {
            item.as_str()
                .is_some_and(|item| normalize_symbol(item) == normalized_target)
        }) {
            return Ok(None);
        }
        items.push(Value::String(normalized_target));
    } else {
        items.retain(|item| {
            !item
                .as_str()
                .is_some_and(|item| normalize_symbol(item) == normalized_target)
        });
        if before == items.len() {
            return Ok(None);
        }
    }
    let after = items.len();
    Ok(Some((serde_json::to_string(&value)?, before, after)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::delist_risk::{ExchangeRisk, RiskEventView};
    use axum::{routing::get, Json, Router};

    #[test]
    fn parses_position_by_symbol_base() {
        let snapshot: DashboardSnapshot = serde_json::from_value(serde_json::json!({
            "entries": [{
                "channel": "pre_trade_exposure",
                "entry": {"ts_ms": 123, "rows": [{
                    "asset": "TSLAX", "open_usdt": 1227.0, "hedge_usdt": -1232.0
                }]}
            }]
        }))
        .unwrap();
        let (_, positions) = positions_from_snapshot(&snapshot).unwrap();
        let row = positions.get("TSLAXUSDT").unwrap();
        assert_eq!(row.snapshot_ms, 123);
        assert_eq!(row.open_usdt, 1227.0);
        assert_eq!(row.hedge_usdt, -1232.0);
    }

    #[test]
    fn adds_to_dump_and_removes_from_open() {
        let (dump, before, after) = update_symbol_list(r#"["BTCUSDT"]"#, "TSLAXUSDT", true)
            .unwrap()
            .unwrap();
        assert_eq!((before, after), (1, 2));
        assert_eq!(dump, r#"["BTCUSDT","TSLAXUSDT"]"#);

        let (open, before, after) =
            update_symbol_list(r#"["BTCUSDT","TSLAX-USDT"]"#, "TSLAXUSDT", false)
                .unwrap()
                .unwrap();
        assert_eq!((before, after), (2, 1));
        assert_eq!(open, r#"["BTCUSDT"]"#);
    }

    #[tokio::test]
    async fn finds_positioned_delist_without_a_redis_universe() {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let app = Router::new().fallback(get(move || async move {
            Json(serde_json::json!({
                "entries": [{
                    "channel": "pre_trade_exposure",
                    "entry": {"ts_ms": now_ms, "rows": [{
                        "asset": "TSLAX", "open_usdt": 1227.0, "hedge_usdt": -1232.0
                    }]}
                }]
            }))
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let event = RiskEventView {
            exchange: "gate".to_string(),
            venue: "gate-futures".to_string(),
            action: "delist".to_string(),
            utc: "2026-09-09T08:00:00Z".to_string(),
            status: "upcoming".to_string(),
            assets: Vec::new(),
            symbols: vec!["TSLAXUSDT".to_string()],
            note: String::new(),
            source: "test".to_string(),
            title: "test delist".to_string(),
            url: String::new(),
            announcement_id: "test".to_string(),
            listing: "listed".to_string(),
        };
        let risk = RiskQueryResponse {
            ok: true,
            as_of_ms: now_ms,
            abnormal: true,
            count: 1,
            exchanges: BTreeMap::from([(
                "gate".to_string(),
                ExchangeRisk {
                    exchange: "gate".to_string(),
                    abnormal: true,
                    count: 1,
                    items: vec![event],
                },
            )]),
        };
        let (candidates, errors) = position_dump_candidates(
            &Client::new(),
            &format!("http://{address}"),
            &risk,
            50.0,
            120_000,
        )
        .await;
        assert!(errors.is_empty());
        assert_eq!(candidates.len(), 2);
        assert!(candidates
            .iter()
            .all(|candidate| candidate.symbol == "TSLAXUSDT"));
        assert!(candidates
            .iter()
            .all(|candidate| candidate.impacted_position_usdt == 1232.0));
    }
}
