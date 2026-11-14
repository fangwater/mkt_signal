use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use iceoryx2::{prelude::*, service::ipc};
use mkt_parsers::msg::mkt_msg::{AskBidSpreadMsg, MktMsgType};
use serde::Serialize;

use crate::{
    checks::{Check, Issue},
    config::{Market, Monitor, Source},
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

/// One watch per venue. Source declarations express dependencies, not separate checks.
#[derive(Clone, Debug)]
pub struct MarketWatch {
    pub venue: String,
    pub symbols: BTreeMap<String, BTreeSet<String>>,
}

impl MarketWatch {
    pub fn from_sources(sources: &[Source], shared_markets: &[Market]) -> Vec<Self> {
        let mut venues = BTreeMap::<String, BTreeMap<String, BTreeSet<String>>>::new();
        for market in shared_markets {
            for symbol in &market.symbols {
                venues
                    .entry(market.venue.clone())
                    .or_default()
                    .entry(normalize_symbol(symbol))
                    .or_default();
            }
        }
        for source in sources.iter().filter(|s| s.enabled) {
            for market in &source.markets {
                for symbol in &market.symbols {
                    venues
                        .entry(market.venue.clone())
                        .or_default()
                        .entry(normalize_symbol(symbol))
                        .or_default()
                        .insert(source.id.clone());
                }
            }
        }
        venues
            .into_iter()
            .map(|(venue, symbols)| Self { venue, symbols })
            .collect()
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct MarketSymbolView {
    pub symbol: String,
    pub source_ids: Vec<String>,
    pub received_ms: Option<i64>,
    pub exchange_ms: Option<i64>,
    pub healthy: Option<bool>,
}

#[derive(Clone, Debug, Serialize)]
pub struct MarketView {
    pub venue: String,
    pub ts_ms: i64,
    pub healthy: Option<bool>,
    pub source_ids: Vec<String>,
    pub affected_sources: Vec<String>,
    pub symbols: Vec<MarketSymbolView>,
}

impl MarketFeed {
    pub fn spawn(watches: &[MarketWatch]) -> Result<Self> {
        // Initialize before multiple subscriber threads use the global IPC config.
        let _ = iceoryx2::config::Config::global_config();
        let feed = Self::default();
        for watch in watches {
            let venue = watch.venue.clone();
            let symbols = watch.symbols.keys().cloned().collect();
            let state = feed.clone();
            thread::Builder::new()
                .name(format!("fr-market-{venue}"))
                .spawn(move || loop {
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

    pub fn check(&self, watch: &MarketWatch, m: &Monitor, now: i64) -> (MarketView, Check) {
        let scope = format!("market/{}", watch.venue);
        let data = self.0.read().ok();
        let mut all_sources = BTreeSet::new();
        let mut affected_sources = BTreeSet::new();
        let mut stale = Vec::new();
        let symbols = watch
            .symbols
            .iter()
            .map(|(symbol, sources)| {
                let times = data
                    .as_ref()
                    .and_then(|d| d.get(&(watch.venue.clone(), symbol.clone())));
                let healthy = data.as_ref().map(|_| {
                    times.is_some_and(|(received, exchange)| {
                        *received > 0
                            && *received <= now + 5000
                            && now.saturating_sub(*received) <= m.market_stale_secs as i64 * 1000
                            && (*exchange == 0
                                || (*exchange > 0
                                    && *exchange <= now + 5000
                                    && now.saturating_sub(*exchange)
                                        <= m.market_stale_secs as i64 * 1000))
                    })
                });
                all_sources.extend(sources.iter().cloned());
                if healthy != Some(true) {
                    affected_sources.extend(sources.iter().cloned());
                    stale.push(symbol.clone());
                }
                MarketSymbolView {
                    symbol: symbol.clone(),
                    source_ids: sources.iter().cloned().collect(),
                    received_ms: times.map(|t| t.0),
                    exchange_ms: times.map(|t| t.1).filter(|t| *t > 0),
                    healthy,
                }
            })
            .collect();
        let affected_sources: Vec<_> = affected_sources.into_iter().collect();
        let healthy = data.as_ref().map(|_| stale.is_empty());
        let check = if data.is_none() {
            Check::failed(scope, "行情状态锁不可读")
        } else {
            let issues =
                if stale.is_empty() {
                    Vec::new()
                } else {
                    // One stable incident per shared feed. Full per-symbol facts remain on the board.
                    let summary = stale.iter().take(8).cloned().collect::<Vec<_>>().join(", ");
                    let affected = affected_sources
                        .iter()
                        .take(8)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(" / ");
                    vec![Issue::warning(
                        "bbo",
                        format!(
                    "{} BBO：{}/{} 个受监控币种缺失或超过 {}s 未更新（{}）；影响 {} 个盘子：{}",
                    watch.venue, stale.len(), watch.symbols.len(), m.market_stale_secs, summary,
                    affected_sources.len(), affected),
                    )]
                };
            Check::ok(scope, issues)
        };
        (
            MarketView {
                venue: watch.venue.clone(),
                ts_ms: now,
                healthy,
                source_ids: all_sources.into_iter().collect(),
                affected_sources,
                symbols,
            },
            check,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    const NOW: i64 = 1_790_000_000_000;

    fn sources() -> Vec<Source> {
        let cfg: Config = toml::from_str(include_str!("../../../config/fr_monitor.toml")).unwrap();
        cfg.sources
    }

    #[test]
    fn nine_accounts_share_six_venue_checks() {
        let sources = sources();
        let watches = MarketWatch::from_sources(&sources, &[]);
        assert_eq!(watches.len(), 6);
        let feed = MarketFeed::default();
        for watch in &watches {
            let (view, check) = feed.check(watch, &Monitor::default(), NOW);
            assert_eq!(view.source_ids.len(), 3);
            assert_eq!(view.affected_sources.len(), 3);
            assert_eq!(check.issues.len(), 1);
            assert!(check.scope.starts_with("market/"));
            assert!(!view.source_ids.contains(&"binance_fr_arb01".to_string()));
        }
    }

    #[test]
    fn symbol_dependencies_are_unioned_and_normalized_once() {
        let mut sources = sources();
        sources.truncate(2);
        sources[0].enabled = true;
        sources[0].markets.truncate(1);
        sources[1].markets.truncate(1);
        sources[0].markets[0].symbols = vec!["BTC_USDT".into(), "BTCUSDT".into()];
        sources[1].markets[0].symbols = vec!["btc-usdt".into(), "ETHUSDT".into()];
        let watches = MarketWatch::from_sources(&sources, &[]);
        assert_eq!(watches.len(), 1);
        assert_eq!(watches[0].symbols.len(), 2);
        assert_eq!(watches[0].symbols["BTCUSDT"].len(), 2);
        assert_eq!(watches[0].symbols["ETHUSDT"].len(), 1);
    }

    #[test]
    fn shared_markets_preserve_coverage_without_inventing_account_dependencies() {
        let sources = sources();
        let shared = vec![
            Market {
                venue: "okex-futures".into(),
                symbols: vec!["BTCUSDT".into()],
            },
            Market {
                venue: "binance-futures".into(),
                symbols: vec!["BTCUSDT".into(), "SOLUSDT".into()],
            },
        ];
        let watches = MarketWatch::from_sources(&sources, &shared);
        assert_eq!(watches.len(), 7);
        let okx = watches.iter().find(|w| w.venue == "okex-futures").unwrap();
        let (view, check) = MarketFeed::default().check(okx, &Monitor::default(), NOW);
        assert_eq!(view.healthy, Some(false));
        assert!(view.source_ids.is_empty());
        assert!(view.affected_sources.is_empty());
        assert_eq!(check.issues.len(), 1);
        let binance = watches
            .iter()
            .find(|w| w.venue == "binance-futures")
            .unwrap();
        assert_eq!(binance.symbols["BTCUSDT"].len(), 3);
        assert!(binance.symbols["SOLUSDT"].is_empty());
    }

    #[test]
    fn partial_outage_only_marks_sources_depending_on_that_symbol() {
        let watch = MarketWatch {
            venue: "gate-futures".into(),
            symbols: BTreeMap::from([
                ("BTCUSDT".into(), BTreeSet::from(["gate_fr_arb01".into()])),
                ("ETHUSDT".into(), BTreeSet::from(["gate_fr_arb02".into()])),
            ]),
        };
        let feed = MarketFeed::default();
        feed.0
            .write()
            .unwrap()
            .insert((watch.venue.clone(), "ETHUSDT".into()), (NOW, NOW));
        let (view, check) = feed.check(&watch, &Monitor::default(), NOW);
        assert_eq!(view.affected_sources, vec!["gate_fr_arb01"]);
        assert_eq!(check.issues.len(), 1);
        assert!(check.issues[0].message.contains("BTCUSDT"));
        assert!(!check.issues[0].message.contains("gate_fr_arb02"));
        assert_eq!(view.symbols[1].healthy, Some(true));
    }

    #[test]
    fn receive_time_cannot_hide_stale_exchange_time_or_another_venue() {
        let watches = MarketWatch::from_sources(&sources(), &[]);
        let watch = &watches[0];
        let feed = MarketFeed::default();
        for symbol in watch.symbols.keys() {
            feed.0
                .write()
                .unwrap()
                .insert((watch.venue.clone(), symbol.clone()), (NOW, NOW - 40_000));
        }
        assert_eq!(
            feed.check(watch, &Monitor::default(), NOW).0.healthy,
            Some(false)
        );
        for symbol in watch.symbols.keys() {
            feed.0
                .write()
                .unwrap()
                .insert((watch.venue.clone(), symbol.clone()), (NOW, NOW));
        }
        assert!(feed
            .check(watch, &Monitor::default(), NOW)
            .1
            .issues
            .is_empty());
        assert!(!feed
            .check(&watches[1], &Monitor::default(), NOW)
            .1
            .issues
            .is_empty());
    }

    #[test]
    fn one_venue_incident_recovers_only_after_all_its_symbols_recover() {
        use crate::notices::Tracker;
        let watches = MarketWatch::from_sources(&sources(), &[]);
        let watch = &watches[0];
        let feed = MarketFeed::default();
        let m = Monitor {
            alert_delay_secs: 0,
            ..Monitor::default()
        };
        let mut tracker = Tracker::default();
        tracker.update(&[feed.check(watch, &m, NOW).1], NOW);
        let notice = tracker.claim(&m, NOW, true);
        assert_eq!(notice.len(), 1);
        tracker.acknowledge(&notice, NOW);
        for (i, symbol) in watch.symbols.keys().enumerate() {
            feed.0.write().unwrap().insert(
                (watch.venue.clone(), symbol.clone()),
                (NOW + 1000, NOW + 1000),
            );
            tracker.update(&[feed.check(watch, &m, NOW + 1000).1], NOW + 1000);
            let pending = tracker.pending(&m, NOW + 1000);
            if i + 1 < watch.symbols.len() {
                assert!(pending.is_empty());
            } else {
                assert_eq!(pending.len(), 1);
                assert!(pending[0].recovery);
            }
        }
    }
}
