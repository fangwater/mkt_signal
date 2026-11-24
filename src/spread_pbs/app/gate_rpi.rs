//! Gate USDT futures RPI order book (`futures.rpi_obu`) → `dat_pbs/gate-futures-rpi/incremental_depth_{50,400}`.
//!
//! The RPI book is the normal book plus RPI maker orders, which only RPI-enabled
//! takers can fill. Gate pushes it only as Order Book V2 snapshots and deltas over
//! SBE (templateId=3). Each depth gets its own WebSocket batches, A/B replicas,
//! de-dup state and IPC service, so it never touches the main BBO connection or
//! the normal `incremental` stream.
//!
//! Disabled unless `SPREAD_PBS_GATE_RPI_DEPTHS` lists depths (`50`, `400` or `50,400`).
//! `SPREAD_PBS_GATE_RPI_SYMBOLS` optionally restricts the contracts (default: all
//! spread_pbs symbols).

use super::*;
use crate::spread_pbs::adapter::SubscriptionAckPolicy;
use crate::spread_pbs::gate_sbe::GateSbeAdapter;
use mkt_parsers::gate as gate_codec;
use std::cell::Cell;

pub(super) const ENV_GATE_RPI_DEPTHS: &str = "SPREAD_PBS_GATE_RPI_DEPTHS";
pub(super) const ENV_GATE_RPI_SYMBOLS: &str = "SPREAD_PBS_GATE_RPI_SYMBOLS";
const RPI_VENUE_SLUG: &str = "gate-futures-rpi";
const RPI_CHANNEL: &str = "futures.rpi_obu";
/// Gate only accepts 50 (20ms) and 400 (100ms). Other depths are acknowledged but never pushed.
const SUPPORTED_DEPTHS: [u32; 2] = [50, 400];
const RPI_SUBSCRIBE_CHUNK: usize = 100;
/// `INCREMENTAL_PAYLOAD_BYTES`=2048 fits about 120 levels; cap chunks below that.
const MAX_LEVELS_PER_MSG_CAP: usize = 100;
/// A snapshot older than the published high-water is normally dropped so a
/// reconnecting replica cannot rewind the downstream book. If the stream has been
/// silent this long, Gate may have reset its ids; accept the snapshot as a new baseline.
const STALE_SNAPSHOT_RESET_AFTER: Duration = Duration::from_secs(5);

pub(super) struct RpiStream {
    depth: u32,
    service_name: String,
    state: Rc<RefCell<SharedState>>,
    gap_reconnects: Rc<Cell<u64>>,
    stale_snapshot_resets: Rc<Cell<u64>>,
}

pub(super) fn depths_from_env() -> Result<Vec<u32>> {
    match env::var(ENV_GATE_RPI_DEPTHS) {
        Ok(raw) => parse_depths(&raw),
        Err(_) => Ok(Vec::new()),
    }
}

fn parse_depths(raw: &str) -> Result<Vec<u32>> {
    let mut depths = Vec::new();
    for item in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let depth: u32 = item
            .parse()
            .with_context(|| format!("{ENV_GATE_RPI_DEPTHS}: invalid depth {item:?}"))?;
        if !SUPPORTED_DEPTHS.contains(&depth) {
            bail!("{ENV_GATE_RPI_DEPTHS}: depth {depth} not supported by Gate (use 50 and/or 400)");
        }
        if !depths.contains(&depth) {
            depths.push(depth);
        }
    }
    depths.sort_unstable();
    Ok(depths)
}

fn symbols_from_env(default: &[String]) -> Vec<String> {
    let raw = env::var(ENV_GATE_RPI_SYMBOLS).unwrap_or_default();
    parse_symbols(&raw).unwrap_or_else(|| default.to_vec())
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

fn build_subscribe(symbols: &[String], depth: u32) -> Vec<serde_json::Value> {
    symbols
        .chunks(RPI_SUBSCRIBE_CHUNK)
        .map(|chunk| {
            let streams: Vec<String> = chunk
                .iter()
                .map(|symbol| format!("rpi_ob.{symbol}.{depth}"))
                .collect();
            serde_json::json!({
                "time": get_timestamp_us() / 1_000_000,
                "channel": RPI_CHANNEL,
                "event": "subscribe",
                "payload": streams,
            })
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

/// Subscribe acks are consumed by the ws loop and `futures.pong` answers our own
/// keepalive; anything else (e.g. `futures.system` notices) is worth a warning.
fn log_text_notice(label: &str, raw: &[u8]) {
    let text = String::from_utf8_lossy(&raw[..raw.len().min(512)]);
    if text.contains("\"futures.pong\"") {
        return;
    }
    log::warn!("spread_pbs ws[{label}] gate rpi unexpected text frame: {text}");
}

fn make_handler(
    label: String,
    depth: u32,
    publisher: Rc<SpreadIncrementalPublisher>,
    state: Rc<RefCell<SharedState>>,
    max_levels: usize,
    gap_reconnects: Rc<Cell<u64>>,
    stale_snapshot_resets: Rc<Cell<u64>>,
) -> FrameHandler {
    let continuity = RefCell::new(LegContinuity::default());
    Rc::new(move |_recv_us: i64, raw: &[u8]| {
        if looks_like_json(raw) {
            log_text_notice(&label, raw);
            return Ok(());
        }
        let Some(frame) = gate_codec::parse_futures_sbe_obu(raw) else {
            return Ok(());
        };
        if frame.channel != RPI_CHANNEL || frame.depth != depth {
            return Ok(());
        }
        let verdict = continuity.borrow_mut().check(&frame.book);
        match verdict {
            Continuity::Accept => {}
            Continuity::Stale => return Ok(()),
            Continuity::Gap { last_u } => {
                continuity.borrow_mut().clear();
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
    publishers: &SpreadPbsPublisherPool,
    dat_root: &str,
    default_symbols: &[String],
    depths: &[u32],
    local_ip: &str,
    max_levels_per_msg: Option<usize>,
    restart_duration: Duration,
) -> Result<(Vec<WsLeg>, Vec<RpiStream>)> {
    let symbols = symbols_from_env(default_symbols);
    let max_levels = max_levels_per_msg
        .filter(|max| *max > 0)
        .unwrap_or(MAX_LEVELS_PER_MSG_CAP)
        .min(MAX_LEVELS_PER_MSG_CAP);
    let gate = GateSbeAdapter::new();
    let mut legs = Vec::new();
    let mut streams = Vec::new();
    for &depth in depths {
        let channel = channel_name(depth);
        let publisher = Rc::new(
            publishers
                .incremental_channel_for(RPI_VENUE_SLUG, dat_root, &channel)
                .with_context(|| format!("create gate rpi publisher {RPI_VENUE_SLUG}/{channel}"))?,
        );
        publisher
            .seed_symbols(&symbols)
            .with_context(|| format!("seed gate rpi payload prefixes for {channel}"))?;
        let state = Rc::new(RefCell::new(new_state(&symbols)));
        let gap_reconnects = Rc::new(Cell::new(0));
        let stale_snapshot_resets = Rc::new(Cell::new(0));
        let subscribe_msgs = build_subscribe(&symbols, depth);
        log::info!(
            "spread_pbs gate rpi depth={} service={} symbols={} batches={} max_levels_per_msg={}",
            depth,
            publisher.service_name(),
            symbols.len(),
            subscribe_msgs.len(),
            max_levels,
        );
        for (batch_index, subscribe_msg) in subscribe_msgs.into_iter().enumerate() {
            for (replica_index, replica) in ["a", "b"].into_iter().enumerate() {
                let label = format!("gate-rpi-{depth}-{batch_index}-{replica}");
                let handler = make_handler(
                    label.clone(),
                    depth,
                    publisher.clone(),
                    state.clone(),
                    max_levels,
                    gap_reconnects.clone(),
                    stale_snapshot_resets.clone(),
                );
                let first_after = if replica_index == 0 {
                    restart_duration
                } else {
                    restart_duration / 2
                };
                let (tx, rx) = watch::channel(false);
                let url = gate.ws_url();
                let handle = tokio::task::spawn_local(run_public_ws_with_ack_policy(
                    WsLoopParams {
                        label: label.clone(),
                        url: url.clone(),
                        local_ip: local_ip.to_string(),
                        remote_ip: None,
                        headers: Vec::new(),
                        subscribe_msgs: vec![subscribe_msg.clone()],
                        keepalive: gate.keepalive(),
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
            depth,
            service_name: publisher.service_name().to_string(),
            state,
            gap_reconnects,
            stale_snapshot_resets,
        });
    }
    Ok((legs, streams))
}

pub(super) fn log_stats(streams: &[RpiStream]) {
    for stream in streams {
        let s = stream.state.borrow();
        log::info!(
            "spread_pbs[{}] gate rpi depth={} stats incremental_published={} incremental_dropped_by_seq={} symbols_seen={} gap_reconnects={} stale_snapshot_resets={}",
            stream.service_name,
            stream.depth,
            s.incremental_published,
            s.incremental_dropped_by_seq,
            s.symbol_state.incremental_seen(),
            stream.gap_reconnects.get(),
            stream.stale_snapshot_resets.get(),
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
        assert!(parse_depths("").unwrap().is_empty());
        assert!(parse_depths(" , ").unwrap().is_empty());
        assert_eq!(parse_depths("400, 50,50").unwrap(), vec![50, 400]);
        assert!(parse_depths("1").is_err());
        assert!(parse_depths("20").is_err());
        assert!(parse_depths("abc").is_err());
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
        let msgs = build_subscribe(&symbols, 400);
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0]["channel"], RPI_CHANNEL);
        assert_eq!(msgs[0]["event"], "subscribe");
        assert_eq!(msgs[0]["payload"].as_array().unwrap().len(), 100);
        assert_eq!(msgs[0]["payload"][0], "rpi_ob.S0_USDT.400");
        assert_eq!(msgs[1]["payload"].as_array().unwrap().len(), 50);
        assert_eq!(channel_name(50), "incremental_depth_50");
        assert_eq!(channel_name(400), "incremental_depth_400");
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
