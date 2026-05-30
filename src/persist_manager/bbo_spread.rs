use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use parking_lot::RwLock;

use crate::common::mkt_msg::{AskBidSpreadMsg, MktMsgType};
use crate::common::redis_client::{RedisClient, RedisSettings};
use crate::signal::common::TradingVenue;
use crate::spread_pbs::publisher::SPREAD_PAYLOAD_BYTES;
use crate::symbol_match::normalize_symbol_for_whitelist;

const DEFAULT_RING_LEN: usize = 8192;
const DEFAULT_ENRICH_DELAY_MS: u64 = 500;
const DEFAULT_SYMBOL_RELOAD_SECS: u64 = 10;
const SPREAD_HISTORY_SIZE: usize = 100;
const SPREAD_SUBSCRIBER_BUFFER: usize = 8192;

#[derive(Debug, Clone)]
pub(crate) struct BboSpreadRuntime {
    pub(crate) store: Arc<BboSpreadStore>,
    pub(crate) enrich_delay: Duration,
}

#[derive(Debug, Clone)]
struct BboSpreadConfig {
    enabled: bool,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    namespace: String,
    key_suffix: String,
    key_prefix: Option<String>,
    ring_len: usize,
    enrich_delay: Duration,
    symbol_reload_interval: Duration,
    redis: RedisSettings,
}

#[derive(Debug, Clone, Copy, Default)]
struct BboSample {
    tp_us: i64,
    bid_px: f64,
    bid_qty: f64,
    ask_px: f64,
    ask_qty: f64,
}

#[derive(Debug)]
pub(crate) struct BboSpreadStore {
    ring_len: usize,
    open_venue_slug: String,
    hedge_venue_slug: String,
    online_symbols: RwLock<HashSet<String>>,
    rings: RwLock<HashMap<(String, String), SymbolBboRing>>,
}

#[derive(Debug)]
struct SymbolBboRing {
    cap: usize,
    next: usize,
    len: usize,
    buf: Vec<BboSample>,
}

struct SpreadBboSubscriber {
    _node: Node<ipc::Service>,
    subscribers: Vec<(
        String,
        Subscriber<ipc::Service, [u8; SPREAD_PAYLOAD_BYTES], ()>,
    )>,
}

impl BboSpreadRuntime {
    pub(crate) async fn start_from_env() -> Option<Self> {
        let config = BboSpreadConfig::from_env();
        if !config.enabled {
            info!("bbo_spread enrichment disabled by env");
            return None;
        }

        let store = Arc::new(BboSpreadStore::new(
            config.ring_len,
            config.open_venue.data_pub_slug(),
            config.hedge_venue.data_pub_slug(),
        ));

        match load_online_symbols(&config).await {
            Ok(symbols) => {
                let count = symbols.len();
                store.set_online_symbols(symbols);
                info!(
                    "bbo_spread online symbols loaded ns={} suffix={} prefix={:?} count={}",
                    config.namespace, config.key_suffix, config.key_prefix, count
                );
            }
            Err(err) => {
                warn!(
                    "bbo_spread initial online symbol load failed ns={} suffix={}: {err:#}",
                    config.namespace, config.key_suffix
                );
            }
        }

        spawn_symbol_reload_task(store.clone(), config.clone());
        spawn_bbo_collector_task(store.clone(), config.clone());

        info!(
            "bbo_spread enrichment started open={} hedge={} ring_len={} delay_ms={} reload_secs={}",
            config.open_venue.data_pub_slug(),
            config.hedge_venue.data_pub_slug(),
            config.ring_len,
            config.enrich_delay.as_millis(),
            config.symbol_reload_interval.as_secs()
        );

        Some(Self {
            store,
            enrich_delay: config.enrich_delay,
        })
    }
}

impl BboSpreadConfig {
    fn from_env() -> Self {
        let (inferred_ns, inferred_suffix) = infer_namespace_and_key_suffix_from_cwd()
            .unwrap_or_else(|| ("intra".to_string(), "bybit".to_string()));

        let namespace = std::env::var("PERSIST_BBO_NAMESPACE")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(inferred_ns)
            .trim()
            .trim_end_matches(['_', '-', ':'])
            .to_ascii_lowercase();
        let key_suffix = std::env::var("PERSIST_BBO_KEY_SUFFIX")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(inferred_suffix)
            .trim()
            .to_ascii_lowercase();

        let inferred_venues = infer_venues(&namespace, &key_suffix)
            .unwrap_or((TradingVenue::BybitMargin, TradingVenue::BybitFutures));
        let open_venue = std::env::var("PERSIST_BBO_OPEN_VENUE")
            .ok()
            .and_then(|s| venue_from_slug(&s))
            .unwrap_or(inferred_venues.0);
        let hedge_venue = std::env::var("PERSIST_BBO_HEDGE_VENUE")
            .ok()
            .and_then(|s| venue_from_slug(&s))
            .unwrap_or(inferred_venues.1);

        let key_prefix = std::env::var("PERSIST_BBO_SYMBOL_KEY_PREFIX")
            .ok()
            .map(|s| s.trim().trim_end_matches(':').to_ascii_lowercase())
            .filter(|s| !s.is_empty())
            .or_else(|| {
                if namespace == "fr" {
                    infer_env_dir_from_cwd()
                } else {
                    None
                }
            });

        Self {
            enabled: env_flag_default_true("PERSIST_BBO_ENABLED"),
            open_venue,
            hedge_venue,
            namespace,
            key_suffix,
            key_prefix,
            ring_len: env_usize("PERSIST_BBO_RING_LEN").unwrap_or(DEFAULT_RING_LEN),
            enrich_delay: Duration::from_millis(
                env_u64("PERSIST_BBO_ENRICH_DELAY_MS").unwrap_or(DEFAULT_ENRICH_DELAY_MS),
            ),
            symbol_reload_interval: Duration::from_secs(
                env_u64("PERSIST_BBO_SYMBOL_RELOAD_SECS").unwrap_or(DEFAULT_SYMBOL_RELOAD_SECS),
            ),
            redis: RedisSettings {
                host: std::env::var("REDIS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
                port: env_u16("REDIS_PORT").unwrap_or(6379),
                db: env_i64("REDIS_DB").unwrap_or(0),
                username: std::env::var("REDIS_USERNAME")
                    .ok()
                    .filter(|s| !s.is_empty()),
                password: std::env::var("REDIS_PASSWORD")
                    .ok()
                    .filter(|s| !s.is_empty()),
                prefix: std::env::var("REDIS_PREFIX").ok().filter(|s| !s.is_empty()),
            },
        }
    }
}

impl BboSpreadStore {
    fn new(ring_len: usize, open_venue_slug: &str, hedge_venue_slug: &str) -> Self {
        Self {
            ring_len: ring_len.max(1),
            open_venue_slug: open_venue_slug.to_string(),
            hedge_venue_slug: hedge_venue_slug.to_string(),
            online_symbols: RwLock::new(HashSet::new()),
            rings: RwLock::new(HashMap::new()),
        }
    }

    fn set_online_symbols(&self, symbols: HashSet<String>) {
        {
            let mut rings = self.rings.write();
            rings.retain(|(_, symbol), _| symbols.contains(symbol));
        }
        *self.online_symbols.write() = symbols;
    }

    fn push(&self, venue_slug: &str, symbol: &str, sample: BboSample) {
        if sample.tp_us <= 0 || sample.bid_px <= 0.0 || sample.ask_px <= 0.0 {
            return;
        }

        let symbol_key = normalize_symbol(symbol);
        if !self.online_symbols.read().contains(&symbol_key) {
            return;
        }

        let mut rings = self.rings.write();
        let ring = rings
            .entry((venue_slug.to_string(), symbol_key))
            .or_insert_with(|| SymbolBboRing::new(self.ring_len));
        ring.push(sample);
    }

    pub(crate) fn format_spread_for_symbol(&self, symbol: &str, tp_us: i64) -> String {
        if tp_us <= 0 {
            return String::new();
        }

        let symbol_key = normalize_symbol(symbol);
        let rings = self.rings.read();
        let open = rings
            .get(&(self.open_venue_slug.clone(), symbol_key.clone()))
            .and_then(|ring| ring.find_le(tp_us));
        let hedge = rings
            .get(&(self.hedge_venue_slug.clone(), symbol_key))
            .and_then(|ring| ring.find_le(tp_us));

        if open.is_none() && hedge.is_none() {
            return String::new();
        }

        let open = open.unwrap_or_default();
        let hedge = hedge.unwrap_or_default();
        format_bbo_spread(open, hedge)
    }
}

impl SymbolBboRing {
    fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            next: 0,
            len: 0,
            buf: Vec::with_capacity(cap.max(1)),
        }
    }

    fn push(&mut self, sample: BboSample) {
        if let Some(last) = self.last() {
            if sample.tp_us < last.tp_us {
                debug!(
                    "drop out-of-order bbo sample tp={} last_tp={}",
                    sample.tp_us, last.tp_us
                );
                return;
            }
        }

        if self.buf.len() < self.cap {
            self.buf.push(sample);
            self.len += 1;
            self.next = self.len % self.cap;
        } else {
            self.buf[self.next] = sample;
            self.next = (self.next + 1) % self.cap;
        }
    }

    fn find_le(&self, tp_us: i64) -> Option<BboSample> {
        if self.len == 0 {
            return None;
        }

        let oldest = self.get_logical(0)?;
        if tp_us < oldest.tp_us {
            return None;
        }

        let mut lo = 0usize;
        let mut hi = self.len;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let sample = self.get_logical(mid)?;
            if sample.tp_us <= tp_us {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        if lo == 0 {
            None
        } else {
            self.get_logical(lo - 1)
        }
    }

    fn last(&self) -> Option<BboSample> {
        if self.len == 0 {
            return None;
        }
        self.get_logical(self.len - 1)
    }

    fn get_logical(&self, idx: usize) -> Option<BboSample> {
        if idx >= self.len {
            return None;
        }
        let physical = if self.buf.len() < self.cap {
            idx
        } else {
            (self.next + idx) % self.cap
        };
        self.buf.get(physical).copied()
    }
}

impl SpreadBboSubscriber {
    fn new(open_venue_slug: &str, hedge_venue_slug: &str) -> Result<Self> {
        let node_name = format!(
            "persist_bbo_spread_{}_{}",
            sanitize_suffix(open_venue_slug),
            sanitize_suffix(hedge_venue_slug)
        );
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()
            .with_context(|| format!("failed to create iceoryx node {}", node_name))?;

        let mut subscribers = Vec::new();
        let mut seen = HashSet::new();
        for venue_slug in [open_venue_slug, hedge_venue_slug] {
            if !seen.insert(venue_slug.to_string()) {
                continue;
            }
            let service_name = format!("spread_pbs/{}/ask_bid_spread", venue_slug);
            let service = node
                .service_builder(&ServiceName::new(&service_name)?)
                .publish_subscribe::<[u8; SPREAD_PAYLOAD_BYTES]>()
                .max_publishers(1)
                .max_subscribers(64)
                .history_size(SPREAD_HISTORY_SIZE)
                .subscriber_max_buffer_size(SPREAD_SUBSCRIBER_BUFFER)
                .open_or_create()
                .with_context(|| format!("failed to open spread service {}", service_name))?;

            let subscriber = service
                .subscriber_builder()
                .create()
                .with_context(|| format!("failed to create spread subscriber {}", service_name))?;
            subscribers.push((venue_slug.to_string(), subscriber));
            info!("bbo_spread subscribed to {}", service_name);
        }

        Ok(Self {
            _node: node,
            subscribers,
        })
    }

    fn poll_msgs(&self, max_per_channel: usize) -> Vec<(String, Bytes)> {
        let mut out = Vec::new();
        for (venue_slug, subscriber) in &self.subscribers {
            for _ in 0..max_per_channel {
                match subscriber.receive() {
                    Ok(Some(sample)) => {
                        let payload = sample.payload();
                        if payload.iter().all(|&b| b == 0) {
                            continue;
                        }
                        out.push((venue_slug.clone(), Bytes::copy_from_slice(payload)));
                    }
                    Ok(None) => break,
                    Err(err) => {
                        warn!("bbo_spread receive error venue={}: {err}", venue_slug);
                        break;
                    }
                }
            }
        }
        out
    }
}

fn spawn_bbo_collector_task(store: Arc<BboSpreadStore>, config: BboSpreadConfig) {
    tokio::task::spawn_local(async move {
        let subscriber = match SpreadBboSubscriber::new(
            config.open_venue.data_pub_slug(),
            config.hedge_venue.data_pub_slug(),
        ) {
            Ok(s) => s,
            Err(err) => {
                warn!("bbo_spread subscriber disabled: {err:#}");
                return;
            }
        };

        loop {
            let messages = subscriber.poll_msgs(512);
            if messages.is_empty() {
                tokio::time::sleep(Duration::from_millis(2)).await;
                continue;
            }

            for (venue_slug, payload) in messages {
                if let Some((symbol, sample)) = decode_bbo_payload(&payload) {
                    store.push(&venue_slug, &symbol, sample);
                }
            }
            tokio::task::yield_now().await;
        }
    });
}

fn spawn_symbol_reload_task(store: Arc<BboSpreadStore>, config: BboSpreadConfig) {
    tokio::task::spawn_local(async move {
        let mut interval = tokio::time::interval(config.symbol_reload_interval);
        loop {
            interval.tick().await;
            match load_online_symbols(&config).await {
                Ok(symbols) => {
                    let count = symbols.len();
                    store.set_online_symbols(symbols);
                    debug!(
                        "bbo_spread online symbols reloaded ns={} suffix={} count={}",
                        config.namespace, config.key_suffix, count
                    );
                }
                Err(err) => {
                    warn!(
                        "bbo_spread online symbol reload failed ns={} suffix={}: {err:#}",
                        config.namespace, config.key_suffix
                    );
                }
            }
        }
    });
}

async fn load_online_symbols(config: &BboSpreadConfig) -> Result<HashSet<String>> {
    let mut client = RedisClient::connect(config.redis.clone()).await?;
    let mut symbols = HashSet::new();

    for list_name in [
        "dump_symbols",
        "fwd_trade_symbols",
        "bwd_trade_symbols",
        "unimmr_close_symbols",
    ] {
        let key = symbol_list_redis_key(
            config.key_prefix.as_deref(),
            &config.namespace,
            list_name,
            &config.key_suffix,
        );
        let value = client.get_json::<Vec<String>>(&key).await?;
        if let Some(list) = value {
            for symbol in list {
                let normalized = normalize_symbol(&symbol);
                if !normalized.is_empty() {
                    symbols.insert(normalized);
                }
            }
        }
    }

    Ok(symbols)
}

fn decode_bbo_payload(payload: &[u8]) -> Option<(String, BboSample)> {
    if payload.len() < 8 {
        return None;
    }
    let msg_type = u32::from_le_bytes(payload.get(0..4)?.try_into().ok()?);
    if msg_type != MktMsgType::AskBidSpread as u32 {
        return None;
    }
    let symbol_len = u32::from_le_bytes(payload.get(4..8)?.try_into().ok()?) as usize;
    let required = 8usize
        .checked_add(symbol_len)?
        .checked_add(8)?
        .checked_add(32)?;
    if payload.len() < required {
        return None;
    }

    let symbol = AskBidSpreadMsg::get_symbol(payload).to_string();
    Some((
        symbol,
        BboSample {
            tp_us: AskBidSpreadMsg::get_timestamp(payload),
            bid_px: AskBidSpreadMsg::get_bid_price(payload),
            bid_qty: AskBidSpreadMsg::get_bid_amount(payload),
            ask_px: AskBidSpreadMsg::get_ask_price(payload),
            ask_qty: AskBidSpreadMsg::get_ask_amount(payload),
        },
    ))
}

fn format_bbo_spread(open: BboSample, hedge: BboSample) -> String {
    let mut out = String::with_capacity(160);
    let _ = write!(
        out,
        "{},{},{},{},{},{},{},{},{},{}",
        open.tp_us,
        fmt_f64(open.bid_px),
        fmt_f64(open.bid_qty),
        fmt_f64(open.ask_px),
        fmt_f64(open.ask_qty),
        hedge.tp_us,
        fmt_f64(hedge.bid_px),
        fmt_f64(hedge.bid_qty),
        fmt_f64(hedge.ask_px),
        fmt_f64(hedge.ask_qty),
    );
    out
}

fn fmt_f64(value: f64) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    let mut s = format!("{value:.12}");
    while s.contains('.') && s.ends_with('0') {
        s.pop();
    }
    if s.ends_with('.') {
        s.pop();
    }
    s
}

fn normalize_symbol(symbol: &str) -> String {
    normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures)
}

fn symbol_list_redis_key(
    prefix: Option<&str>,
    namespace: &str,
    list_name: &str,
    suffix: &str,
) -> String {
    let base = format!("{namespace}_{list_name}:{suffix}");
    match prefix {
        Some(prefix) if !prefix.is_empty() => format!("{prefix}:{base}"),
        _ => base,
    }
}

fn infer_namespace_and_key_suffix_from_cwd() -> Option<(String, String)> {
    let cwd: PathBuf = std::env::current_dir().ok()?;
    let name = cwd.file_name()?.to_string_lossy().to_ascii_lowercase();
    parse_namespace_and_key_suffix(&name)
}

fn infer_env_dir_from_cwd() -> Option<String> {
    let cwd = std::env::current_dir().ok()?;
    let leaf = cwd.file_name()?.to_string_lossy().trim().to_string();
    if leaf.is_empty() {
        return None;
    }
    Some(leaf.to_ascii_lowercase())
}

fn parse_namespace_and_key_suffix(name: &str) -> Option<(String, String)> {
    fn split_last_segment(input: &str) -> Option<(&str, &str)> {
        let idx = input.rfind(['-', '_'])?;
        let (head, tail_with_sep) = input.split_at(idx);
        let tail = tail_with_sep.get(1..)?;
        if head.is_empty() || tail.is_empty() {
            return None;
        }
        Some((head, tail))
    }

    let (base, _env_tag) = split_last_segment(name)?;
    let (prefix, ns) = split_last_segment(base)?;
    if prefix.is_empty() || ns.is_empty() {
        return None;
    }
    Some((ns.to_string(), prefix.to_string()))
}

fn infer_venues(namespace: &str, key_suffix: &str) -> Option<(TradingVenue, TradingVenue)> {
    match namespace {
        "fr" => infer_fr_venues_from_key_suffix(key_suffix),
        "intra" => infer_intra_venues_from_key_suffix(key_suffix),
        "cross" => infer_cross_venues_from_key_suffix(key_suffix),
        _ => None,
    }
}

fn infer_fr_venues_from_key_suffix(key_suffix: &str) -> Option<(TradingVenue, TradingVenue)> {
    let suffix = key_suffix.trim().to_ascii_lowercase();
    let mut parts = suffix.split('_');
    let open = venue_from_slug(parts.next()?)?;
    let hedge = venue_from_slug(parts.next()?)?;
    if parts.next().is_some() {
        return None;
    }
    Some((open, hedge))
}

fn infer_intra_venues_from_key_suffix(key_suffix: &str) -> Option<(TradingVenue, TradingVenue)> {
    let suffix = key_suffix.trim().to_ascii_lowercase();
    if suffix.contains('-') || suffix.contains('_') {
        return None;
    }
    let ex = normalize_exchange_str(&suffix);
    Some((
        margin_venue_for_exchange(ex)?,
        futures_venue_for_exchange(ex)?,
    ))
}

fn infer_cross_venues_from_key_suffix(key_suffix: &str) -> Option<(TradingVenue, TradingVenue)> {
    let suffix = key_suffix.trim().to_ascii_lowercase();
    let mut parts = suffix.split('-');
    let open_ex = normalize_exchange_str(parts.next()?);
    let hedge_ex = normalize_exchange_str(parts.next()?);
    if parts.next().is_some() {
        return None;
    }
    let open = futures_venue_for_exchange(open_ex)?;
    let hedge = futures_venue_for_exchange(hedge_ex)?;
    if open == hedge {
        return None;
    }
    Some((open, hedge))
}

fn normalize_exchange_str(raw: &str) -> &str {
    match raw {
        "okx" => "okex",
        other => other,
    }
}

fn venue_from_slug(raw: &str) -> Option<TradingVenue> {
    let slug = raw.trim().to_ascii_lowercase().replace('_', "-");
    match slug.as_str() {
        "binance-margin" => Some(TradingVenue::BinanceMargin),
        "binance-futures" => Some(TradingVenue::BinanceFutures),
        "okex-margin" => Some(TradingVenue::OkexMargin),
        "okex-futures" => Some(TradingVenue::OkexFutures),
        "bybit-margin" => Some(TradingVenue::BybitMargin),
        "bybit-futures" => Some(TradingVenue::BybitFutures),
        "bitget-margin" => Some(TradingVenue::BitgetMargin),
        "bitget-futures" => Some(TradingVenue::BitgetFutures),
        "gate-margin" => Some(TradingVenue::GateMargin),
        "gate-futures" => Some(TradingVenue::GateFutures),
        "aster-margin" => Some(TradingVenue::AsterMargin),
        "aster-futures" => Some(TradingVenue::AsterFutures),
        "hyperliquid-margin" => Some(TradingVenue::HyperliquidMargin),
        "hyperliquid-futures" => Some(TradingVenue::HyperliquidFutures),
        _ => None,
    }
}

fn futures_venue_for_exchange(exchange: &str) -> Option<TradingVenue> {
    match exchange {
        "binance" => Some(TradingVenue::BinanceFutures),
        "okex" | "okx" => Some(TradingVenue::OkexFutures),
        "bybit" => Some(TradingVenue::BybitFutures),
        "bitget" => Some(TradingVenue::BitgetFutures),
        "gate" => Some(TradingVenue::GateFutures),
        "aster" => Some(TradingVenue::AsterFutures),
        "hyperliquid" => Some(TradingVenue::HyperliquidFutures),
        _ => None,
    }
}

fn margin_venue_for_exchange(exchange: &str) -> Option<TradingVenue> {
    match exchange {
        "binance" => Some(TradingVenue::BinanceMargin),
        "okex" | "okx" => Some(TradingVenue::OkexMargin),
        "bybit" => Some(TradingVenue::BybitMargin),
        "bitget" => Some(TradingVenue::BitgetMargin),
        "gate" => Some(TradingVenue::GateMargin),
        "aster" => Some(TradingVenue::AsterMargin),
        "hyperliquid" => Some(TradingVenue::HyperliquidMargin),
        _ => None,
    }
}

fn sanitize_suffix(raw: &str) -> String {
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn env_flag_default_true(name: &str) -> bool {
    match std::env::var(name) {
        Ok(value) => !matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off"
        ),
        Err(_) => true,
    }
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.trim().parse().ok()
}

fn env_u16(name: &str) -> Option<u16> {
    std::env::var(name).ok()?.trim().parse().ok()
}

fn env_i64(name: &str) -> Option<i64> {
    std::env::var(name).ok()?.trim().parse().ok()
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_find_le_across_wrap() {
        let mut ring = SymbolBboRing::new(3);
        for tp in [10, 20, 30, 40] {
            ring.push(BboSample {
                tp_us: tp,
                bid_px: tp as f64,
                ..BboSample::default()
            });
        }

        assert_eq!(ring.find_le(19).map(|s| s.tp_us), None);
        assert_eq!(ring.find_le(20).map(|s| s.tp_us), Some(20));
        assert_eq!(ring.find_le(35).map(|s| s.tp_us), Some(30));
        assert_eq!(ring.find_le(99).map(|s| s.tp_us), Some(40));
    }

    #[test]
    fn bbo_spread_has_ten_numbers() {
        let open = BboSample {
            tp_us: 1,
            bid_px: 100.1,
            bid_qty: 2.0,
            ask_px: 100.2,
            ask_qty: 3.0,
        };
        let hedge = BboSample {
            tp_us: 2,
            bid_px: 99.9,
            bid_qty: 4.0,
            ask_px: 100.0,
            ask_qty: 5.0,
        };
        let s = format_bbo_spread(open, hedge);
        assert_eq!(s.split(',').count(), 10);
        assert_eq!(s, "1,100.1,2,100.2,3,2,99.9,4,100,5");
    }
}
