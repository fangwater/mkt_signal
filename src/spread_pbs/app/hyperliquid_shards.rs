use super::*;
use crate::spread_pbs::hyperliquid::HyperliquidSubscriptionBudget;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

const SOURCES_ENV: &str = "SPREAD_PBS_HYPERLIQUID_EGRESS_SHARDS";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Sources {
    primary_local_ip: String,
    secondary_local_ip: String,
}

pub(super) fn sources_from_env() -> Result<Option<Vec<Sources>>> {
    env::var(SOURCES_ENV)
        .ok()
        .map(|raw| parse_sources(&raw))
        .transpose()
}

fn parse_sources(raw: &str) -> Result<Vec<Sources>> {
    let sources: Vec<Sources> =
        serde_json::from_str(raw).with_context(|| format!("parse {SOURCES_ENV} JSON array"))?;
    if sources.is_empty() || sources.len() > 16 {
        bail!("{SOURCES_ENV} requires 1..=16 source pairs");
    }
    let mut unique = HashSet::new();
    for pair in &sources {
        for address in [&pair.primary_local_ip, &pair.secondary_local_ip] {
            let ip = address
                .parse::<std::net::IpAddr>()
                .context("invalid Hyperliquid shard source IP")?;
            if ip.is_unspecified() || !unique.insert(ip) {
                bail!("Hyperliquid shards require distinct explicitly bound source IPs");
            }
        }
    }
    Ok(sources)
}

fn partition(subscriptions: &[Value], count: usize) -> Result<Vec<Vec<Value>>> {
    if count == 0 {
        bail!("Hyperliquid shard count must be positive");
    }
    let mut shards = vec![Vec::new(); count];
    for request in subscriptions {
        let coin = request
            .get("subscription")
            .and_then(|value| value.get("coin"))
            .and_then(Value::as_str)
            .context("Hyperliquid sharded subscription missing coin")?;
        // Stable coin ownership keeps BBO/trades/book/context together and
        // does not move existing symbols when metadata gains another asset.
        let digest = Sha256::digest(coin.as_bytes());
        let hash = u64::from_be_bytes(digest[..8].try_into().expect("fixed SHA-256 prefix"));
        shards[(hash % count as u64) as usize].push(request.clone());
    }
    Ok(shards)
}

fn spawn(ctx: &LegCtx, sources: &[Sources], slot: usize, subscriptions: &[Value]) -> Option<WsLeg> {
    if subscriptions.is_empty() {
        return None;
    }
    let shard = slot / 2;
    let (role, ip) = if slot % 2 == 0 {
        ("primary", &sources[shard].primary_local_ip)
    } else {
        ("secondary", &sources[shard].secondary_local_ip)
    };
    Some(spawn_leg(
        format!("hl-shard-{shard}-{role}"),
        ip.clone(),
        ctx.url.clone(),
        MarketSource::Other,
        subscriptions.to_vec(),
        ctx,
        ctx.publisher
            .as_ref()
            .expect("Hyperliquid BBO publisher is always enabled")
            .clone(),
    ))
}

pub(super) async fn run(
    ctx: LegCtx,
    config: &Config,
    subscriptions: Vec<Value>,
    mut symbols: Vec<String>,
    sources: Vec<Sources>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut groups = partition(&subscriptions, sources.len())?;
    let mut budgets = Vec::with_capacity(sources.len());
    // Reserve every group before opening any socket. All venue apps in both
    // share this process-wide ledger, while retaining one IPC publisher each.
    for (source, group) in sources.iter().zip(&groups) {
        budgets.push(HyperliquidSubscriptionBudget::reserve(
            group.len(),
            &source.primary_local_ip,
            &source.secondary_local_ip,
        )?);
    }
    let mut legs = (0..sources.len() * 2)
        .map(|slot| spawn(&ctx, &sources, slot, &groups[slot / 2]))
        .collect::<Vec<_>>();
    log::info!("spread_pbs[{}] Hyperliquid egress shards={} symbols={} subscriptions={} per-leg={:?}; public egress independence must be verified by ops", config.venue.data_pub_slug(), sources.len(), symbols.len(), subscriptions.len(), groups.iter().map(Vec::len).collect::<Vec<_>>());
    let interval = Duration::from_secs(config.restart_duration_secs.max(2)) / legs.len() as u32;
    let mut rolling = tokio::time::interval(interval.max(Duration::from_secs(1)));
    rolling.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    rolling.tick().await;
    let mut health = tokio::time::interval(Duration::from_secs(30));
    health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    health.tick().await;
    let mut slot = 1_usize;
    let mut recovery_slot = 0_usize;
    let mut restarted_at = vec![Instant::now(); legs.len()];
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            _ = rolling.tick() => {
                let refresh = async {
                    let refreshed = apply_symbol_filter(get_symbols_for_role(config, BinanceFuturesRole::Full).await?, config.venue.data_pub_slug());
                    if refreshed.is_empty() { bail!("Hyperliquid shard refresh returned no symbols"); }
                    let requests = build_market_subscribe(&ctx.adapter, &refreshed, true, ctx.trade_publisher.is_some(), ctx.incremental_publisher.is_some(), ctx.derivatives_publisher.is_some());
                    if requests.is_empty() { bail!("Hyperliquid shard subscription build failed"); }
                    let regrouped = partition(&requests, sources.len())?;
                    for (budget, group) in budgets.iter().zip(&regrouped) { budget.grow(group.len())?; }
                    ctx.publisher.as_ref().context("missing Hyperliquid BBO publisher")?.seed_symbols(&refreshed)?;
                    if let Some(publisher) = &ctx.trade_publisher { publisher.seed_symbols(&refreshed)?; }
                    if let Some(publisher) = &ctx.incremental_publisher { publisher.seed_symbols(&refreshed)?; }
                    if let Some(publisher) = &ctx.derivatives_publisher { publisher.seed_symbols(&refreshed)?; }
                    ctx.adapter.seed_symbols(&refreshed);
                    ctx.state.borrow_mut().symbol_state.ensure_symbols(&refreshed);
                    Ok::<_, anyhow::Error>((refreshed, regrouped))
                }.await;
                match refresh {
                    Ok((refreshed, regrouped)) => { symbols = refreshed; groups = regrouped; }
                    Err(err) => {
                        log::error!("Hyperliquid shard refresh rejected; retaining active subscriptions: {err:#}");
                        continue;
                    }
                }
                if let Some(mut leg) = legs[slot].take() {
                    let _ = leg.shutdown_tx.send(true);
                    let _ = (&mut leg.handle).await;
                }
                legs[slot] = spawn(&ctx, &sources, slot, &groups[slot/2]);
                restarted_at[slot] = Instant::now();
                slot = (slot + 1) % legs.len();
            }
            _ = health.tick() => {
                let stale = {
                    let state = ctx.state.borrow();
                    let refs = symbols.iter().map(String::as_str).collect::<Vec<_>>();
                    let stale = if ctx.incremental_publisher.is_some() { state.symbol_state.stale_incremental_symbols(&refs, Instant::now(), INCREMENTAL_CRITICAL_STALE) } else { Vec::new() };
                    log::info!("Hyperliquid sharded market stats venue={} bbo={} trades={} books={} derivatives={} stale_books={:?}", config.venue.data_pub_slug(), state.published, state.trades_published, state.incremental_published, state.derivatives_published, stale);
                    stale
                };
                // A busy shard can mask a stalled single-symbol subscription
                // from the connection-wide idle timer. Recover one affected
                // leg per health tick, retaining its redundant peer.
                if !stale.is_empty() {
                    let requests = ctx.adapter.build_subscribe(&stale);
                    if let Ok(affected) = partition(&requests, sources.len()) {
                        if let Some(target) = recovery_target(&affected, &restarted_at, recovery_slot, Instant::now()) {
                            log::warn!("Hyperliquid shard={} leg={} restarting for stale books", target / 2, target % 2);
                            if let Some(mut leg) = legs[target].take() {
                                let _ = leg.shutdown_tx.send(true);
                                let _ = (&mut leg.handle).await;
                            }
                            legs[target] = spawn(&ctx, &sources, target, &groups[target / 2]);
                            restarted_at[target] = Instant::now();
                            recovery_slot = (target + 1) % legs.len();
                        }
                    }
                }
            }
        }
    }
    for leg in legs.iter().flatten() {
        let _ = leg.shutdown_tx.send(true);
    }
    for leg in legs.iter_mut().flatten() {
        let _ = (&mut leg.handle).await;
    }
    Ok(())
}

fn recovery_target(
    affected: &[Vec<Value>],
    restarted: &[Instant],
    next: usize,
    now: Instant,
) -> Option<usize> {
    (0..restarted.len())
        .map(|offset| (next + offset) % restarted.len())
        .find(|slot| {
            !affected[*slot / 2].is_empty()
                && now.saturating_duration_since(restarted[*slot])
                    >= HYPERLIQUID_STALE_RESTART_COOLDOWN
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hyperliquid_shard_recovery_targets_only_stale_shards_and_respects_cooldown() {
        let now = Instant::now();
        let old = now - HYPERLIQUID_STALE_RESTART_COOLDOWN;
        let affected = vec![Vec::new(), vec![serde_json::json!({})]];
        assert_eq!(recovery_target(&affected, &[old; 4], 0, now), Some(2));
        assert_eq!(
            recovery_target(&affected, &[old, old, now, old], 2, now),
            Some(3)
        );
        assert_eq!(
            recovery_target(&affected, &[old, old, now, now], 0, now),
            None
        );
    }

    #[test]
    fn hyperliquid_shards_partition_every_stream_once_with_stable_coin_ownership() {
        let requests = (0..400).flat_map(|coin| ["bbo","trades","l2Book","activeAssetCtx"].map(move |kind| serde_json::json!({"subscription":{"coin":format!("COIN{coin}"),"type":kind}}))).collect::<Vec<_>>();
        let groups = partition(&requests, 3).unwrap();
        assert_eq!(groups.iter().map(Vec::len).sum::<usize>(), requests.len());
        assert!(groups.iter().all(|group| group.len() < 1_000));
        for group in &groups {
            let mut counts = std::collections::HashMap::new();
            for row in group {
                *counts
                    .entry(row["subscription"]["coin"].as_str().unwrap())
                    .or_insert(0) += 1;
            }
            assert!(counts.values().all(|count| *count == 4));
        }
        let mut extended = requests.clone();
        extended.push(serde_json::json!({"subscription":{"coin":"NEW","type":"bbo"}}));
        let added = partition(&extended, 3).unwrap();
        for (old, new) in groups.iter().zip(added) {
            assert!(old.iter().all(|request| new.contains(request)));
        }
    }

    #[test]
    fn hyperliquid_shards_reject_unbound_reused_or_malformed_sources() {
        for raw in [
            "[]",
            r#"[{"primary_local_ip":"0.0.0.0","secondary_local_ip":"10.0.0.2"}]"#,
            r#"[{"primary_local_ip":"10.0.0.1","secondary_local_ip":"10.0.0.1"}]"#,
        ] {
            assert!(parse_sources(raw).is_err());
        }
        assert!(parse_sources(
            r#"[{"primary_local_ip":"10.0.0.1","secondary_local_ip":"10.0.0.2"}]"#
        )
        .is_ok());
    }
}
