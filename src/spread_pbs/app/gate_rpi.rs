//! Gate RPI order book (`futures.rpi_obu` / `spot.rpi_obu`) →
//! `dat_pbs/gate-futures-rpi/incremental_depth_{50,400}` (USDT futures) and
//! `dat_pbs/gate-margin-rpi/incremental_depth_{50,400}` (spot).
//!
//! The same legs also subscribe the normal `{market}.obu` at the same depth and
//! publish the RPI-only remainder (RPI size minus normal size per price) as
//! snapshots to `.../pure_rpi_depth_{50,400}`; see [`pure`].
//!
//! The RPI book is the normal book plus RPI maker orders, which only RPI-enabled
//! takers can fill. Gate pushes it only as Order Book V2 snapshots and deltas over
//! SBE (templateId=3). Each depth gets its own WebSocket batches, A/B replicas,
//! de-dup state and IPC service, so it never touches the main BBO connection or
//! the normal `incremental` stream.
//!
//! Disabled unless `SPREAD_PBS_GATE_RPI_DEPTHS` lists depths (`50`, `400` or `50,400`).
//! `SPREAD_PBS_GATE_RPI_SYMBOLS` optionally restricts the contracts (default: all
//! spread_pbs symbols). `SPREAD_PBS_GATE_RPI_{FUTURES,SPOT}_{DEPTHS,SYMBOLS}` override
//! the shared value for one market, e.g. to enable only futures in a `gate-both` process.

use super::*;
use crate::spread_pbs::adapter::KeepaliveSpec;
use crate::spread_pbs::adapter::SubscriptionAckPolicy;
use crate::spread_pbs::gate_sbe::{GateSbeAdapter, GateSpotSbeAdapter};
use mkt_parsers::gate as gate_codec;
use std::cell::Cell;

mod pure;

pub(super) const ENV_GATE_RPI_DEPTHS: &str = "SPREAD_PBS_GATE_RPI_DEPTHS";
pub(super) const ENV_GATE_RPI_SYMBOLS: &str = "SPREAD_PBS_GATE_RPI_SYMBOLS";
/// Gate only accepts 50 (20ms) and 400 (100ms). Other depths are acknowledged but never pushed.
const SUPPORTED_DEPTHS: [u32; 2] = [50, 400];
const RPI_SUBSCRIBE_CHUNK: usize = 100;
/// `INCREMENTAL_PAYLOAD_BYTES`=2048 fits about 120 levels; cap chunks below that.
const MAX_LEVELS_PER_MSG_CAP: usize = 100;
/// A snapshot older than the published high-water is normally dropped so a
/// reconnecting replica cannot rewind the downstream book. If the stream has been
/// silent this long, Gate may have reset its ids; accept the snapshot as a new baseline.
const STALE_SNAPSHOT_RESET_AFTER: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RpiMarket {
    Futures,
    Spot,
}

impl RpiMarket {
    pub(super) fn for_venue(venue: TradingVenue) -> Option<Self> {
        match venue {
            TradingVenue::GateFutures => Some(Self::Futures),
            TradingVenue::GateMargin => Some(Self::Spot),
            _ => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Futures => "futures",
            Self::Spot => "spot",
        }
    }

    fn venue_slug(self) -> &'static str {
        match self {
            Self::Futures => "gate-futures-rpi",
            Self::Spot => "gate-margin-rpi",
        }
    }

    fn channel(self) -> &'static str {
        match self {
            Self::Futures => "futures.rpi_obu",
            Self::Spot => "spot.rpi_obu",
        }
    }

    /// Normal Order Book V2 channel, subscribed alongside for the pure RPI remainder.
    fn std_channel(self) -> &'static str {
        match self {
            Self::Futures => "futures.obu",
            Self::Spot => "spot.obu",
        }
    }

    /// Same SBE endpoints (schemaId=1) and keepalive as the main Gate adapters.
    fn ws_url(self) -> String {
        match self {
            Self::Futures => GateSbeAdapter::new().ws_url(),
            Self::Spot => GateSpotSbeAdapter::new().ws_url(),
        }
    }

    fn keepalive(self) -> Option<KeepaliveSpec> {
        match self {
            Self::Futures => GateSbeAdapter::new().keepalive(),
            Self::Spot => GateSpotSbeAdapter::new().keepalive(),
        }
    }

    fn pong_channel(self) -> &'static str {
        match self {
            Self::Futures => "\"futures.pong\"",
            Self::Spot => "\"spot.pong\"",
        }
    }

    fn env_override(self, shared: &str) -> String {
        shared.replacen(
            "SPREAD_PBS_GATE_RPI_",
            &format!("SPREAD_PBS_GATE_RPI_{}_", self.label().to_ascii_uppercase()),
            1,
        )
    }

    /// Market-specific env var if set, else the shared one.
    fn env_value(self, shared: &str) -> Option<(String, String)> {
        let specific = self.env_override(shared);
        env::var(&specific)
            .ok()
            .map(|value| (specific, value))
            .or_else(|| {
                env::var(shared)
                    .ok()
                    .map(|value| (shared.to_string(), value))
            })
    }
}

pub(super) struct RpiStream {
    market: RpiMarket,
    depth: u32,
    service_name: String,
    state: Rc<RefCell<SharedState>>,
    gap_reconnects: Rc<Cell<u64>>,
    stale_snapshot_resets: Rc<Cell<u64>>,
    pure: Rc<pure::PureSink>,
}

/// Requested depths for a market and the env var they came from (for logging).
pub(super) fn depths_from_env(market: RpiMarket) -> Result<(Vec<u32>, Option<String>)> {
    match market.env_value(ENV_GATE_RPI_DEPTHS) {
        Some((name, raw)) => Ok((parse_depths(&name, &raw)?, Some(format!("{name}={raw}")))),
        None => Ok((Vec::new(), None)),
    }
}

/// Depths requested by any Gate RPI env var, used to warn on non-Gate venues.
pub(super) fn any_depths_requested() -> Option<String> {
    [RpiMarket::Futures, RpiMarket::Spot]
        .into_iter()
        .filter_map(|market| market.env_value(ENV_GATE_RPI_DEPTHS))
        .find(|(_, raw)| !raw.trim().is_empty())
        .map(|(name, raw)| format!("{name}={raw}"))
}

fn parse_depths(env_name: &str, raw: &str) -> Result<Vec<u32>> {
    let mut depths = Vec::new();
    for item in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let depth: u32 = item
            .parse()
            .with_context(|| format!("{env_name}: invalid depth {item:?}"))?;
        if !SUPPORTED_DEPTHS.contains(&depth) {
            bail!("{env_name}: depth {depth} not supported by Gate (use 50 and/or 400)");
        }
        if !depths.contains(&depth) {
            depths.push(depth);
        }
    }
    depths.sort_unstable();
    Ok(depths)
}

fn symbols_from_env(market: RpiMarket, default: &[String]) -> Vec<String> {
    market
        .env_value(ENV_GATE_RPI_SYMBOLS)
        .and_then(|(_, raw)| parse_symbols(&raw))
        .unwrap_or_else(|| default.to_vec())
}

fn parse_symbols(raw: &str) -> Option<Vec<String>> {
    let mut symbols: Vec<String> = Vec::new();
    for symbol in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let symbol = symbol.to_ascii_uppercase();
        if !symbols.contains(&symbol) {
            symbols.push(symbol);
        }
    }
    (!symbols.is_empty()).then_some(symbols)
}

fn channel_name(depth: u32) -> String {
    format!("incremental_depth_{depth}")
}

fn pure_channel_name(depth: u32) -> String {
    format!("pure_rpi_depth_{depth}")
}

/// One subscribe batch per chunk: `[normal obu, rpi_obu]` for the same contracts.
fn build_subscribe(
    market: RpiMarket,
    symbols: &[String],
    depth: u32,
) -> Vec<Vec<serde_json::Value>> {
    let message = |channel: &str, prefix: &str, chunk: &[String]| {
        let streams: Vec<String> = chunk
            .iter()
            .map(|symbol| format!("{prefix}.{symbol}.{depth}"))
            .collect();
        serde_json::json!({
            "time": get_timestamp_us() / 1_000_000,
            "channel": channel,
            "event": "subscribe",
            "payload": streams,
        })
    };
    symbols
        .chunks(RPI_SUBSCRIBE_CHUNK)
        .map(|chunk| {
            vec![
                message(market.std_channel(), "ob", chunk),
                message(market.channel(), "rpi_ob", chunk),
            ]
        })
        .collect()
}

fn new_state(symbols: &[String]) -> SharedState {
    SharedState {
        symbol_state: SymbolSeqState::with_symbols(symbols),
        bbo_dedup_policy: BboDedupPolicy::MonotonicSequence,
        sbe_symbols: fast_hash_map_with_capacity(0),
        publication_latency: PublicationLatency::default(),
        binance_feed_latency: None,
        trade_publish_not_before_us: None,
        published: 0,
        trades_published: 0,
        incremental_published: 0,
        incremental_dropped_by_seq: 0,
        incremental_gap_warnings: 0,
        derivatives_published: 0,
        derivatives_dropped_duplicate: 0,
        derivatives_recent: fast_hash_map_with_capacity(0),
        derivatives_since_prune: 0,
        recent_bbo_identities: Vec::new(),
        recent_trade_identities: Vec::new(),
        recent_incremental_identities: Vec::new(),
        selected_whitelist: 0,
        selected_normal: 0,
        selected_other: 0,
        dropped_by_seq: 0,
        trades_dropped_by_seq: 0,
        last_dedup_reset_us: get_timestamp_us(),
    }
}

/// Per-connection `U == last_u + 1` check required by Gate Order Book V2.
/// A/B replicas are merged downstream of this check, so each leg must be
/// internally contiguous for the merged stream to stay contiguous.
#[derive(Default)]
struct LegContinuity {
    last_u: FastHashMap<String, i64>,
}

#[derive(Debug, PartialEq, Eq)]
enum Continuity {
    Accept,
    /// Already covered by this connection (u <= last u).
    Stale,
    Gap {
        last_u: Option<i64>,
    },
}

impl LegContinuity {
    fn check(&mut self, book: &gate_codec::Book) -> Continuity {
        if book.is_snapshot {
            self.last_u
                .insert(book.symbol.clone(), book.final_update_id);
            return Continuity::Accept;
        }
        match self.last_u.get_mut(&book.symbol) {
            Some(last) if book.first_update_id == *last + 1 => {
                *last = book.final_update_id;
                Continuity::Accept
            }
            Some(last) if book.final_update_id <= *last => Continuity::Stale,
            Some(last) => Continuity::Gap {
                last_u: Some(*last),
            },
            None => Continuity::Gap { last_u: None },
        }
    }

    fn clear(&mut self) {
        self.last_u.clear();
    }
}

/// Snapshots are only published when newer than the merged high-water, except
/// after a long silence (possible venue-side id reset).
fn dedup_policy_for(
    state: &SharedState,
    slot: &SymbolSlot,
    book: &gate_codec::Book,
    now: Instant,
) -> IncrementalDedupPolicy {
    let stale_baseline = book.is_snapshot
        && slot.prev != i64::MIN
        && book.final_update_id <= slot.prev
        && state.symbol_state.incremental_last_seen_at[slot.idx]
            .is_none_or(|seen| now.duration_since(seen) >= STALE_SNAPSHOT_RESET_AFTER);
    if stale_baseline {
        IncrementalDedupPolicy::SnapshotCanReset
    } else {
        IncrementalDedupPolicy::MonotonicIncludingSnapshots
    }
}

/// Subscribe acks are consumed by the ws loop and `*.pong` answers our own
/// keepalive; anything else (e.g. `futures.system` notices) is worth a warning.
fn log_text_notice(market: RpiMarket, label: &str, raw: &[u8]) {
    let text = String::from_utf8_lossy(&raw[..raw.len().min(512)]);
    if text.contains(market.pong_channel()) {
        return;
    }
    log::warn!("spread_pbs ws[{label}] gate rpi unexpected text frame: {text}");
}

#[allow(clippy::too_many_arguments)]
fn make_handler(
    market: RpiMarket,
    label: String,
    depth: u32,
    publisher: Rc<SpreadIncrementalPublisher>,
    state: Rc<RefCell<SharedState>>,
    max_levels: usize,
    gap_reconnects: Rc<Cell<u64>>,
    stale_snapshot_resets: Rc<Cell<u64>>,
    pure_sink: Rc<pure::PureSink>,
) -> FrameHandler {
    let rpi_continuity = RefCell::new(LegContinuity::default());
    let std_continuity = RefCell::new(LegContinuity::default());
    let leg_books = RefCell::new(pure::LegBooks::default());
    Rc::new(move |_recv_us: i64, raw: &[u8]| {
        if looks_like_json(raw) {
            log_text_notice(market, &label, raw);
            return Ok(());
        }
        let Some(frame) = gate_codec::parse_sbe_obu(raw) else {
            return Ok(());
        };
        if frame.depth != depth {
            return Ok(());
        }
        let is_rpi = if frame.channel == market.channel() {
            true
        } else if frame.channel == market.std_channel() {
            false
        } else {
            return Ok(());
        };
        let continuity = if is_rpi {
            &rpi_continuity
        } else {
            &std_continuity
        };
        let verdict = continuity.borrow_mut().check(&frame.book);
        match verdict {
            Continuity::Accept => {}
            Continuity::Stale => return Ok(()),
            Continuity::Gap { last_u } => {
                rpi_continuity.borrow_mut().clear();
                std_continuity.borrow_mut().clear();
                leg_books.borrow_mut().clear();
                gap_reconnects.set(gap_reconnects.get() + 1);
                bail!(
                    "gate rpi obu gap stream={} local_u={:?} U={} u={}; resubscribe for a fresh snapshot",
                    frame.stream,
                    last_u,
                    frame.book.first_update_id,
                    frame.book.final_update_id,
                );
            }
        }

        if let Some(pure) = leg_books.borrow_mut().apply(is_rpi, &frame.book) {
            pure_sink.publish(&label, pure);
        }
        if !is_rpi {
            return Ok(());
        }

        let mut s = state.borrow_mut();
        let slot = s.symbol_state.incremental_slot(&frame.book.symbol);
        let policy = dedup_policy_for(&s, &slot, &frame.book, Instant::now());
        if policy == IncrementalDedupPolicy::SnapshotCanReset {
            stale_snapshot_resets.set(stale_snapshot_resets.get() + 1);
            log::warn!(
                "spread_pbs ws[{}] gate rpi {} snapshot u={} <= published u={} after >= {}s silence; accepting as new baseline",
                label,
                frame.stream,
                frame.book.final_update_id,
                slot.prev,
                STALE_SNAPSHOT_RESET_AFTER.as_secs(),
            );
        }
        process_incremental_frame(
            &mut s,
            &publisher,
            Some(slot.idx),
            policy,
            crate::spread_pbs::gate::book_to_incremental(frame.book),
            Some(max_levels),
            false,
        );
        Ok(())
    })
}

/// Spawn A/B legs per subscribe batch for every requested depth.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn(
    market: RpiMarket,
    publishers: &SpreadPbsPublisherPool,
    dat_root: &str,
    default_symbols: &[String],
    depths: &[u32],
    local_ip: &str,
    max_levels_per_msg: Option<usize>,
    restart_duration: Duration,
) -> Result<(Vec<WsLeg>, Vec<RpiStream>)> {
    let symbols = symbols_from_env(market, default_symbols);
    let max_levels = max_levels_per_msg
        .filter(|max| *max > 0)
        .unwrap_or(MAX_LEVELS_PER_MSG_CAP)
        .min(MAX_LEVELS_PER_MSG_CAP);
    let slug = market.venue_slug();
    let mut legs = Vec::new();
    let mut streams = Vec::new();
    for &depth in depths {
        let channel = channel_name(depth);
        let publisher = Rc::new(
            publishers
                .incremental_channel_for(slug, dat_root, &channel)
                .with_context(|| format!("create gate rpi publisher {slug}/{channel}"))?,
        );
        publisher
            .seed_symbols(&symbols)
            .with_context(|| format!("seed gate rpi payload prefixes for {channel}"))?;
        let state = Rc::new(RefCell::new(new_state(&symbols)));
        let pure_channel = pure_channel_name(depth);
        let pure_publisher = Rc::new(
            publishers
                .incremental_channel_for(slug, dat_root, &pure_channel)
                .with_context(|| format!("create gate rpi publisher {slug}/{pure_channel}"))?,
        );
        pure_publisher
            .seed_symbols(&symbols)
            .with_context(|| format!("seed gate rpi payload prefixes for {pure_channel}"))?;
        let pure_sink = Rc::new(pure::PureSink::new(
            pure_publisher,
            Rc::new(RefCell::new(new_state(&symbols))),
            max_levels,
        ));
        let gap_reconnects = Rc::new(Cell::new(0));
        let stale_snapshot_resets = Rc::new(Cell::new(0));
        let subscribe_msgs = build_subscribe(market, &symbols, depth);
        log::info!(
            "spread_pbs gate rpi market={} depth={} service={} pure_service={} symbols={} batches={} max_levels_per_msg={}",
            market.label(),
            depth,
            publisher.service_name(),
            pure_sink.service_name,
            symbols.len(),
            subscribe_msgs.len(),
            max_levels,
        );
        for (batch_index, batch_msgs) in subscribe_msgs.into_iter().enumerate() {
            for (replica_index, replica) in ["a", "b"].into_iter().enumerate() {
                let label = format!(
                    "gate-rpi-{}-{depth}-{batch_index}-{replica}",
                    market.label()
                );
                let handler = make_handler(
                    market,
                    label.clone(),
                    depth,
                    publisher.clone(),
                    state.clone(),
                    max_levels,
                    gap_reconnects.clone(),
                    stale_snapshot_resets.clone(),
                    pure_sink.clone(),
                );
                let first_after = if replica_index == 0 {
                    restart_duration
                } else {
                    restart_duration / 2
                };
                let (tx, rx) = watch::channel(false);
                let url = market.ws_url();
                let handle = tokio::task::spawn_local(run_public_ws_with_ack_policy(
                    WsLoopParams {
                        label: label.clone(),
                        url: url.clone(),
                        local_ip: local_ip.to_string(),
                        remote_ip: None,
                        headers: Vec::new(),
                        subscribe_msgs: batch_msgs.clone(),
                        keepalive: market.keepalive(),
                        parse_okex_notices: false,
                        business_idle_timeout: Some(WS_BUSINESS_IDLE_TIMEOUT),
                        rolling_restart: Some(RollingRestartSpec {
                            interval: restart_duration,
                            first_after,
                        }),
                    },
                    handler,
                    rx,
                    SubscriptionAckPolicy::None,
                ));
                legs.push(WsLeg {
                    label,
                    local_ip: local_ip.to_string(),
                    url,
                    source: MarketSource::Other,
                    shutdown_tx: tx,
                    handle,
                });
            }
        }
        streams.push(RpiStream {
            market,
            depth,
            service_name: publisher.service_name().to_string(),
            state,
            gap_reconnects,
            stale_snapshot_resets,
            pure: pure_sink,
        });
    }
    Ok((legs, streams))
}

pub(super) fn log_stats(streams: &[RpiStream]) {
    for stream in streams {
        let s = stream.state.borrow();
        log::info!(
            "spread_pbs[{}] gate rpi market={} depth={} stats incremental_published={} incremental_dropped_by_seq={} symbols_seen={} gap_reconnects={} stale_snapshot_resets={}",
            stream.service_name,
            stream.market.label(),
            stream.depth,
            s.incremental_published,
            s.incremental_dropped_by_seq,
            s.symbol_state.incremental_seen(),
            stream.gap_reconnects.get(),
            stream.stale_snapshot_resets.get(),
        );
        let pure = &stream.pure;
        let ps = pure.state.borrow();
        log::info!(
            "spread_pbs[{}] gate pure rpi market={} depth={} stats paired={} published={} unchanged_skipped={} dropped_by_seq={} negative_levels={} stale_snapshot_resets={}",
            pure.service_name,
            stream.market.label(),
            stream.depth,
            pure.paired.get(),
            ps.incremental_published,
            pure.unchanged_skipped.get(),
            ps.incremental_dropped_by_seq,
            pure.negative_levels.get(),
            pure.stale_snapshot_resets.get(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book(symbol: &str, snapshot: bool, first: i64, last: i64) -> gate_codec::Book {
        gate_codec::Book {
            symbol: symbol.to_string(),
            timestamp_us: 1,
            seq_id: last,
            prev_seq_id: first - 1,
            first_update_id: first,
            final_update_id: last,
            gap_check: false,
            is_snapshot: snapshot,
            bids: vec![gate_codec::Level {
                price: 100.0,
                amount: 1.0,
            }],
            asks: vec![gate_codec::Level {
                price: 101.0,
                amount: 1.0,
            }],
        }
    }

    #[test]
    fn depths_default_off_and_only_accept_gate_supported_levels() {
        let env = ENV_GATE_RPI_DEPTHS;
        assert!(parse_depths(env, "").unwrap().is_empty());
        assert!(parse_depths(env, " , ").unwrap().is_empty());
        assert_eq!(parse_depths(env, "400, 50,50").unwrap(), vec![50, 400]);
        assert!(parse_depths(env, "1").is_err());
        assert!(parse_depths(env, "20").is_err());
        assert!(parse_depths(env, "abc").is_err());
    }

    #[test]
    fn symbols_override_is_optional_and_normalized() {
        assert_eq!(parse_symbols(""), None);
        assert_eq!(
            parse_symbols(" btc_usdt,ETH_USDT,BTC_USDT "),
            Some(vec!["BTC_USDT".to_string(), "ETH_USDT".to_string()])
        );
    }

    #[test]
    fn subscribe_batches_rpi_streams_and_names_depth_channels() {
        let symbols: Vec<String> = (0..150).map(|i| format!("S{i}_USDT")).collect();
        let batches = build_subscribe(RpiMarket::Futures, &symbols, 400);
        assert_eq!(batches.len(), 2);
        let [std, rpi] = &batches[0][..] else {
            panic!("each batch subscribes normal + rpi");
        };
        assert_eq!(std["channel"], "futures.obu");
        assert_eq!(std["payload"][0], "ob.S0_USDT.400");
        assert_eq!(rpi["channel"], "futures.rpi_obu");
        assert_eq!(rpi["event"], "subscribe");
        assert_eq!(rpi["payload"].as_array().unwrap().len(), 100);
        assert_eq!(rpi["payload"][0], "rpi_ob.S0_USDT.400");
        assert_eq!(batches[1][1]["payload"].as_array().unwrap().len(), 50);
        assert_eq!(channel_name(50), "incremental_depth_50");
        assert_eq!(channel_name(400), "incremental_depth_400");
        assert_eq!(pure_channel_name(50), "pure_rpi_depth_50");

        let spot = build_subscribe(RpiMarket::Spot, &symbols[..1], 50);
        assert_eq!(spot[0][0]["channel"], "spot.obu");
        assert_eq!(spot[0][0]["payload"][0], "ob.S0_USDT.50");
        assert_eq!(spot[0][1]["channel"], "spot.rpi_obu");
        assert_eq!(spot[0][1]["payload"][0], "rpi_ob.S0_USDT.50");
    }

    #[test]
    fn markets_map_gate_venues_to_rpi_services_and_endpoints() {
        assert_eq!(
            RpiMarket::for_venue(TradingVenue::GateFutures),
            Some(RpiMarket::Futures)
        );
        assert_eq!(
            RpiMarket::for_venue(TradingVenue::GateMargin),
            Some(RpiMarket::Spot)
        );
        assert_eq!(RpiMarket::for_venue(TradingVenue::BinanceFutures), None);
        assert_eq!(RpiMarket::Futures.venue_slug(), "gate-futures-rpi");
        assert_eq!(RpiMarket::Spot.venue_slug(), "gate-margin-rpi");
        assert!(RpiMarket::Futures.ws_url().contains("fx-ws.gateio.ws"));
        assert!(RpiMarket::Spot.ws_url().contains("/spot/sbe"));
        assert!(RpiMarket::Spot.ws_url().contains("sbe_schema_id=1"));
        assert_eq!(
            RpiMarket::Spot.env_override(ENV_GATE_RPI_DEPTHS),
            "SPREAD_PBS_GATE_RPI_SPOT_DEPTHS"
        );
        assert_eq!(
            RpiMarket::Futures.env_override(ENV_GATE_RPI_SYMBOLS),
            "SPREAD_PBS_GATE_RPI_FUTURES_SYMBOLS"
        );
    }

    #[test]
    fn leg_continuity_requires_snapshot_then_contiguous_ids() {
        let mut c = LegContinuity::default();
        assert_eq!(
            c.check(&book("BTC_USDT", false, 5, 6)),
            Continuity::Gap { last_u: None },
            "delta before snapshot"
        );
        assert_eq!(c.check(&book("BTC_USDT", true, 10, 10)), Continuity::Accept);
        assert_eq!(
            c.check(&book("BTC_USDT", false, 11, 14)),
            Continuity::Accept
        );
        assert_eq!(c.check(&book("BTC_USDT", false, 12, 14)), Continuity::Stale);
        assert_eq!(
            c.check(&book("BTC_USDT", false, 16, 18)),
            Continuity::Gap { last_u: Some(14) }
        );
        // A new snapshot (e.g. after Gate re-sends one) re-baselines the leg.
        assert_eq!(c.check(&book("BTC_USDT", true, 30, 30)), Continuity::Accept);
        assert_eq!(
            c.check(&book("BTC_USDT", false, 31, 31)),
            Continuity::Accept
        );
    }

    #[test]
    fn stale_replica_snapshot_is_dropped_unless_stream_went_silent() {
        let mut state = new_state(&["BTC_USDT".to_string()]);
        let now = Instant::now();
        let slot = state.symbol_state.incremental_slot("BTC_USDT");
        state.symbol_state.set_incremental_slot_at(slot, 100, now);
        let slot = state.symbol_state.incremental_slot("BTC_USDT");

        let older = book("BTC_USDT", true, 90, 90);
        assert_eq!(
            dedup_policy_for(&state, &slot, &older, now + Duration::from_millis(50)),
            IncrementalDedupPolicy::MonotonicIncludingSnapshots
        );
        assert!(should_drop_incremental(
            &slot,
            older.final_update_id,
            true,
            IncrementalDedupPolicy::MonotonicIncludingSnapshots
        ));
        assert_eq!(
            dedup_policy_for(
                &state,
                &slot,
                &older,
                now + STALE_SNAPSHOT_RESET_AFTER + Duration::from_millis(1)
            ),
            IncrementalDedupPolicy::SnapshotCanReset
        );
        let newer = book("BTC_USDT", true, 120, 120);
        assert_eq!(
            dedup_policy_for(&state, &slot, &newer, now),
            IncrementalDedupPolicy::MonotonicIncludingSnapshots
        );
    }
}
