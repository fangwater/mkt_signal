use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use iceoryx2::{prelude::*, service::ipc};
use mkt_parsers::msg::mkt_msg::{AskBidSpreadMsg, MktMsgType};

use crate::{
    checks::{Check, Issue},
    config::{Config, Monitor, Source},
    now_ms,
};

#[derive(Clone, Default)]
pub struct MarketFeed(Arc<RwLock<BTreeMap<(String, String), (i64, i64)>>>);

pub fn normalize_symbol(s: &str) -> String {
    s.chars()
        .filter(|c| !matches!(c, '-' | '_'))
        .flat_map(char::to_uppercase)
        .collect()
}

/// Validate bounds before using the shared zero-copy getters (which assume valid input).
pub fn parse_quote(payload: &[u8]) -> Option<(String, i64)> {
    if payload.len() < 8 {
        return None;
    }
    if u32::from_le_bytes(payload[0..4].try_into().ok()?) != MktMsgType::AskBidSpread as u32 {
        return None;
    }
    let len = u32::from_le_bytes(payload[4..8].try_into().ok()?) as usize;
    let end = 8usize.checked_add(len)?.checked_add(40)?;
    if len == 0 || end > payload.len() {
        return None;
    }
    std::str::from_utf8(&payload[8..8 + len]).ok()?;
    let bid = AskBidSpreadMsg::get_bid_price(payload);
    let ask = AskBidSpreadMsg::get_ask_price(payload);
    if !bid.is_finite() || !ask.is_finite() || bid <= 0.0 || ask < bid {
        return None;
    }
    Some((
        normalize_symbol(AskBidSpreadMsg::get_symbol(payload)),
        AskBidSpreadMsg::get_timestamp(payload) / 1000,
    ))
}

impl MarketFeed {
    pub fn spawn(cfg: &Config) -> Result<Self> {
        // Initialize once before multiple subscriber threads use the global IPC config.
        let _ = iceoryx2::config::Config::global_config();
        let feed = Self::default();
        let mut venues = BTreeMap::<String, BTreeSet<String>>::new();
        for s in cfg.sources.iter().filter(|s| s.enabled) {
            for m in &s.markets {
                venues
                    .entry(m.venue.clone())
                    .or_default()
                    .extend(m.symbols.iter().map(|s| normalize_symbol(s)));
            }
        }
        for (venue, symbols) in venues {
            let state = feed.clone();
            thread::Builder::new()
                .name(format!("fr-market-{venue}"))
                .spawn(move || loop {
                    // Failures surface as missing/stale data, with throttled notices in the main loop.
                    let _ = state.subscribe(&venue, &symbols);
                    thread::sleep(Duration::from_secs(1));
                })
                .context("spawn FR market subscriber")?;
        }
        Ok(feed)
    }

    fn subscribe(&self, venue: &str, symbols: &BTreeSet<String>) -> Result<()> {
        let node = NodeBuilder::new().create::<ipc::Service>()?;
        let service = node
            .service_builder(&ServiceName::new(&format!(
                "spread_pbs/{venue}/ask_bid_spread"
            ))?)
            .publish_subscribe::<[u8; 128]>()
            .max_publishers(1)
            .max_subscribers(64)
            .history_size(100)
            .subscriber_max_buffer_size(8192)
            .open()?;
        let subscriber = service.subscriber_builder().buffer_size(8192).create()?;
        let mut last_any = Instant::now();
        // A timestamp-less Binance spot quote in retained history must not be
        // mistaken for a live update when reconnecting to a stopped publisher.
        let mut bootstrap_samples = 100usize;
        loop {
            if let Some(sample) = subscriber.receive()? {
                last_any = Instant::now();
                let retained = bootstrap_samples > 0;
                bootstrap_samples = bootstrap_samples.saturating_sub(1);
                if let Some((symbol, exchange_ms)) = parse_quote(sample.payload()) {
                    if exchange_ms == 0 && retained {
                        continue;
                    }
                    if symbols.contains(&symbol) {
                        if let Ok(mut data) = self.0.write() {
                            data.insert((venue.to_string(), symbol), (now_ms(), exchange_ms));
                        }
                    }
                }
            } else {
                bootstrap_samples = 0;
                // Reopen a replaced/stopped service rather than staying attached forever.
                if last_any.elapsed() > Duration::from_secs(30) {
                    return Ok(());
                }
                thread::sleep(Duration::from_millis(10));
            }
        }
    }

    pub fn check(&self, s: &Source, m: &Monitor, now: i64) -> Check {
        let scope = format!("{}/market", s.id);
        let Ok(data) = self.0.read() else {
            return Check::failed(scope, "行情状态锁不可读");
        };
        let mut issues = Vec::new();
        for market in &s.markets {
            for symbol in &market.symbols {
                let key = (market.venue.clone(), normalize_symbol(symbol));
                let healthy = data.get(&key).is_some_and(|(received, exchange)| {
                    now.saturating_sub(*received) <= m.market_stale_secs as i64 * 1000
                        && (*exchange == 0
                            || (*exchange <= now + 5000
                                && now.saturating_sub(*exchange)
                                    <= m.market_stale_secs as i64 * 1000))
                });
                if !healthy {
                    issues.push(Issue::warning(
                        format!("{}-{}", key.0, key.1),
                        format!(
                            "{} {} 行情缺失或超过 {}s 未更新",
                            key.0, key.1, m.market_stale_secs
                        ),
                    ));
                }
            }
        }
        Check::ok(scope, issues)
    }
}
