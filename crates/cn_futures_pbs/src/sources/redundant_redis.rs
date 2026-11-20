//! Two independent futures L2 subscriptions with one ordered output.
//! Keep the standby tail in receive order. An exact overlap at the last
//! published quote proves the join without assuming consecutive MDL SeqNo.
use super::{redis::RedisSource, Source};
use crate::msg::{MarketDataMsg, SOURCE_RESET, UNKNOWN_QUANTITY, VENUES};
use anyhow::{Context, Result};
use runtime_common::fast_hash::{fast_hash_map_with_capacity, FastHashMap};
use std::{
    collections::VecDeque,
    net::SocketAddrV4,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
        Arc,
    },
    time::{Duration, Instant},
};

const STANDBY_CAPACITY: usize = 32_768;
const INGRESS_CAPACITY: usize = 32_768;
const JOIN_TIMEOUT: Duration = Duration::from_millis(750);
const STALL: Duration = Duration::from_secs(1);

type Identity = (u32, u32, u64, [u8; 64]);

fn identity(q: &MarketDataMsg) -> Identity {
    (q.trading_day, q.action_day, q.source_sequence, q.symbol)
}

fn same_payload(a: &MarketDataMsg, b: &MarketDataMsg) -> bool {
    let equal = |a: f64, b: f64| a == b || a.is_nan() && b.is_nan();
    a.exchange_ts_us == b.exchange_ts_us
        && a.volume == b.volume
        && equal(a.last_price, b.last_price)
        && equal(a.turnover, b.turnover)
        && equal(a.open_interest, b.open_interest)
        && a.bid
            .iter()
            .chain(&a.ask)
            .zip(b.bid.iter().chain(&b.ask))
            .all(|(a, b)| a.quantity == b.quantity && equal(a.price, b.price))
}

struct Standby {
    quotes: VecDeque<MarketDataMsg>,
    index: FastHashMap<Identity, usize>,
    front: usize,
}

impl Standby {
    fn new() -> Self {
        Self {
            quotes: VecDeque::new(),
            index: fast_hash_map_with_capacity(512),
            front: 0,
        }
    }

    fn clear(&mut self) {
        self.quotes.clear();
        self.index.clear();
        self.front = 0;
    }

    fn push(&mut self, quote: MarketDataMsg) -> bool {
        let key = identity(&quote);
        if let Some(&offset) = self.index.get(&key) {
            return same_payload(&self.quotes[offset - self.front], &quote);
        }
        if self.quotes.len() == STANDBY_CAPACITY {
            let oldest = self.quotes.pop_front().expect("full standby queue");
            self.index.remove(&identity(&oldest));
            self.front += 1;
        }
        self.index.insert(key, self.front + self.quotes.len());
        self.quotes.push_back(quote);
        true
    }

    fn offset(&self, quote: &MarketDataMsg) -> Option<usize> {
        self.index
            .get(&identity(quote))
            .map(|&offset| offset - self.front)
    }

    fn conflicts(&self, quote: &MarketDataMsg) -> bool {
        self.offset(quote)
            .is_some_and(|offset| !same_payload(&self.quotes[offset], quote))
    }
}

struct Handoff {
    lane: usize,
    deadline: Instant,
}

struct Merge {
    primary: Option<usize>,
    last_quote_at: Instant,
    last: [Option<MarketDataMsg>; VENUES.len()],
    standby: [Standby; VENUES.len()],
    handoff: Option<Handoff>,
    output: VecDeque<MarketDataMsg>,
    generation: u64,
}

impl Merge {
    fn new() -> Self {
        Self {
            primary: None,
            last_quote_at: Instant::now(),
            last: [None; VENUES.len()],
            standby: std::array::from_fn(|_| Standby::new()),
            handoff: None,
            output: VecDeque::from([MarketDataMsg::reset(1)]),
            generation: 1,
        }
    }

    fn clear_standby(&mut self) {
        for buffer in &mut self.standby {
            buffer.clear();
        }
    }

    fn reset(&mut self, reason: &str) {
        eprintln!("cn-futures-pbs: Redis L2 continuity lost: {reason}");
        self.primary = None;
        self.handoff = None;
        self.last.fill(None);
        self.clear_standby();
        self.generation = self.generation.saturating_add(1);
        self.output.push_back(MarketDataMsg::reset(self.generation));
    }

    fn switch_to(&mut self, lane: usize, now: Instant) {
        self.handoff = Some(Handoff {
            lane,
            deadline: now + JOIN_TIMEOUT,
        });
    }

    fn lane_reset(&mut self, lane: usize, now: Instant) {
        if let Some(handoff) = &self.handoff {
            if lane == handoff.lane {
                self.reset("standby also lost continuity during takeover");
            }
        } else if self.primary == Some(lane) {
            self.switch_to(1 - lane, now);
        } else {
            self.clear_standby();
        }
    }

    fn emit(&mut self, venue: usize, mut quote: MarketDataMsg, now: Instant) {
        quote.source_generation = self.generation;
        self.last[venue] = Some(quote);
        self.last_quote_at = now;
        self.output.push_back(quote);
    }

    fn quote(&mut self, lane: usize, quote: MarketDataMsg, now: Instant) {
        let Some(venue) = VENUES.iter().position(|v| v.as_bytes() == quote.venue()) else {
            return;
        };
        if quote.source_sequence == UNKNOWN_QUANTITY || quote.action_day == 0 {
            // A snapshot without a vendor identity cannot prove an overlap.
            self.lane_reset(lane, now);
            return;
        }
        if let Some(handoff) = &self.handoff {
            if lane == handoff.lane && !self.standby[venue].push(quote) {
                self.reset("conflicting quote in the standby window");
            }
            return;
        }
        let primary = *self.primary.get_or_insert(lane);
        if lane == primary {
            let standby = &self.standby[venue];
            if standby.conflicts(&quote) {
                self.reset("primary and standby disagree for the same quote");
                return;
            }
            if let Some(last) = self.last[venue].as_ref() {
                if standby.conflicts(last) {
                    self.reset("primary and standby disagree at the last published quote");
                    return;
                }
                if let Some((start, end)) = standby.offset(last).zip(standby.offset(&quote)) {
                    // A short hole can be repaired before the primary's next
                    // quote goes out, if the standby already saw both endpoints.
                    for offset in start + 1..end {
                        let recovered = self.standby[venue].quotes[offset];
                        self.emit(venue, recovered, now);
                    }
                }
            }
            self.emit(venue, quote, now);
            return;
        }
        let last = self.last[venue].as_ref();
        if last
            .is_some_and(|last| identity(last) == identity(&quote) && !same_payload(last, &quote))
            || !self.standby[venue].push(quote)
        {
            self.reset("primary and standby disagree for the same quote");
        } else if now.duration_since(self.last_quote_at) >= STALL
            && last.is_some_and(|last| identity(last) < identity(&quote))
        {
            // A TCP socket can answer heartbeats while its quote stream stops.
            // The shadow must have newer data before a quiet primary is suspect.
            self.switch_to(lane, now);
        }
    }

    fn advance(&mut self, now: Instant) {
        let Some(handoff) = &self.handoff else { return };
        let mut skip = [0usize; VENUES.len()];
        for (venue, last) in self.last.iter().enumerate() {
            if let Some(last) = last {
                let buffer = &self.standby[venue];
                let Some(offset) = buffer.offset(last) else {
                    if now >= handoff.deadline {
                        self.reset("standby did not cover the last published quote");
                    }
                    return;
                };
                if !same_payload(&buffer.quotes[offset], last) {
                    self.reset("standby boundary fields differ");
                    return;
                }
                skip[venue] = offset + 1;
            }
        }

        let lane = handoff.lane;
        self.primary = Some(lane);
        self.handoff = None;
        self.last_quote_at = now;
        let mut forwarded = 0;
        // No ordering contract links different exchanges. Within each venue,
        // drain exactly the standby socket's receipt order after the overlap.
        for (venue, skip) in skip.into_iter().enumerate() {
            let pending = std::mem::take(&mut self.standby[venue].quotes);
            self.standby[venue].clear();
            for quote in pending.into_iter().skip(skip) {
                self.emit(venue, quote, now);
                forwarded += 1;
            }
        }
        eprintln!("cn-futures-pbs: Redis L2 switched to subscription {}; buffered_quotes={forwarded}, generation={}", lane + 1, self.generation);
    }
}

/// The endpoints can designate the same Redis service for single-socket
/// failure tolerance, or two equivalent Redis ingress addresses.
pub struct RedundantRedisSource {
    inputs: Receiver<(usize, MarketDataMsg)>,
    running: Arc<AtomicBool>,
    merge: Merge,
}

impl RedundantRedisSource {
    pub fn new(primary: SocketAddrV4, standby: SocketAddrV4) -> Result<Self> {
        let (tx, inputs) = mpsc::sync_channel(INGRESS_CAPACITY);
        let running = Arc::new(AtomicBool::new(true));
        for (lane, endpoint) in [primary, standby].into_iter().enumerate() {
            let tx = tx.clone();
            let runner = running.clone();
            if let Err(error) = std::thread::Builder::new()
                .name(format!("cn-redis-l2-{}", lane + 1))
                .spawn(move || read_lane(lane, endpoint, tx, runner))
            {
                running.store(false, Ordering::Relaxed);
                return Err(error).context("start Redis L2 subscription thread");
            }
        }
        Ok(Self {
            inputs,
            running,
            merge: Merge::new(),
        })
    }
}

impl Drop for RedundantRedisSource {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

fn read_lane(
    lane: usize,
    endpoint: SocketAddrV4,
    tx: SyncSender<(usize, MarketDataMsg)>,
    running: Arc<AtomicBool>,
) {
    let mut source = RedisSource::new(endpoint);
    while running.load(Ordering::Relaxed) {
        let quote = match source.poll() {
            Ok(Some(quote)) => quote,
            Ok(None) => continue,
            Err(error) => {
                eprintln!(
                    "cn-futures-pbs: Redis L2 subscription {} failed: {error:#}",
                    lane + 1
                );
                if tx.send((lane, MarketDataMsg::reset(0))).is_err() {
                    return;
                }
                source = RedisSource::new(endpoint);
                continue;
            }
        };
        match tx.try_send((lane, quote)) {
            Ok(()) => {}
            Err(TrySendError::Disconnected(_)) => return,
            Err(TrySendError::Full(_)) => {
                // Signal the lost interval in FIFO order before resubscribing.
                eprintln!(
                    "cn-futures-pbs: Redis L2 subscription {} ingress queue full",
                    lane + 1
                );
                if tx.send((lane, MarketDataMsg::reset(0))).is_err() {
                    return;
                }
                source = RedisSource::new(endpoint);
            }
        }
    }
}

impl Source for RedundantRedisSource {
    fn poll(&mut self) -> Result<Option<MarketDataMsg>> {
        loop {
            if let Some(quote) = self.merge.output.pop_front() {
                return Ok(Some(quote));
            }
            let now = Instant::now();
            self.merge.advance(now);
            if let Some(quote) = self.merge.output.pop_front() {
                return Ok(Some(quote));
            }
            match self.inputs.try_recv() {
                Ok((lane, quote)) if quote.flags & SOURCE_RESET != 0 => {
                    self.merge.lane_reset(lane, now)
                }
                Ok((lane, quote)) => self.merge.quote(lane, quote, now),
                Err(mpsc::TryRecvError::Empty) => return Ok(None),
                Err(mpsc::TryRecvError::Disconnected) => {
                    anyhow::bail!("Redis L2 subscription threads exited")
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quote(venue: &str, symbol: &[u8], seq: u64) -> MarketDataMsg {
        let mut quote = MarketDataMsg {
            trading_day: 20261120,
            action_day: 20261120,
            source_sequence: seq,
            last_price: 100.0,
            ..MarketDataMsg::default()
        };
        quote.set_venue(venue).unwrap();
        quote.set_symbol(symbol).unwrap();
        quote
    }

    #[test]
    fn one_lane_disconnect_releases_only_the_standby_tail_in_exchange_order() {
        let mut merge = Merge::new();
        let now = Instant::now();
        let shfe = quote("SHFE", b"sn2701", 100);
        let dce = quote("DCE", b"m2701", 800);
        merge.output.clear();
        merge.quote(0, shfe, now);
        merge.quote(0, dce, now);
        merge.output.clear();
        merge.quote(1, quote("SHFE", b"rb2701", 99), now);
        merge.quote(1, shfe, now);
        merge.quote(1, quote("SHFE", b"rb2701", 102), now);
        merge.quote(1, quote("SHFE", b"sn2701", 109), now);
        merge.quote(1, dce, now);
        merge.quote(1, quote("DCE", b"m2701", 804), now);

        merge.lane_reset(0, now);
        merge.advance(now);
        assert_eq!(merge.primary, Some(1));
        assert!(merge
            .output
            .iter()
            .all(|q| q.flags & SOURCE_RESET == 0 && q.source_generation == 1));
        assert_eq!(
            merge
                .output
                .iter()
                .map(|q| q.source_sequence)
                .collect::<Vec<_>>(),
            [102, 109, 804]
        );

        merge.output.clear();
        merge.lane_reset(0, now);
        merge.quote(1, quote("SHFE", b"sn2701", 114), now);
        assert_eq!(merge.output.pop_front().unwrap().source_generation, 1);
    }

    #[test]
    fn standby_must_observe_the_same_boundary_before_switching() {
        let mut merge = Merge::new();
        let now = Instant::now();
        merge.output.clear();
        merge.quote(0, quote("SHFE", b"sn2701", 10), now);
        merge.output.clear();
        merge.quote(1, quote("SHFE", b"sn2701", 12), now);
        merge.lane_reset(0, now);
        merge.advance(now);
        assert!(merge.output.is_empty());
        merge.advance(now + JOIN_TIMEOUT);
        let reset = merge.output.pop_front().unwrap();
        assert_ne!(reset.flags & SOURCE_RESET, 0);
        assert_eq!(reset.source_generation, 2);
        assert!(merge.output.is_empty());
    }

    #[test]
    fn fills_a_primary_hole_when_standby_already_has_both_endpoints() {
        let mut merge = Merge::new();
        let now = Instant::now();
        let before = quote("SHFE", b"sn2701", 10);
        let after = quote("SHFE", b"sn2701", 30);
        merge.output.clear();
        merge.quote(0, before, now);
        merge.output.clear();
        merge.quote(1, before, now);
        merge.quote(1, quote("SHFE", b"rb2701", 18), now);
        merge.quote(1, after, now);
        merge.quote(0, after, now);
        assert_eq!(
            merge
                .output
                .iter()
                .map(|q| q.source_sequence)
                .collect::<Vec<_>>(),
            [18, 30]
        );
        assert_eq!(merge.primary, Some(0));
        assert_eq!(merge.generation, 1);
    }

    #[test]
    fn missing_both_subscriptions_forces_a_new_generation() {
        let mut merge = Merge::new();
        let now = Instant::now();
        let anchor = quote("SHFE", b"sn2701", 10);
        merge.output.clear();
        merge.quote(0, anchor, now);
        merge.quote(1, anchor, now);
        merge.output.clear();
        merge.lane_reset(0, now);
        merge.lane_reset(1, now);
        assert_ne!(merge.output.pop_front().unwrap().flags & SOURCE_RESET, 0);
        assert!(merge.output.is_empty());
    }

    #[test]
    fn healthy_heartbeats_do_not_hide_a_stalled_market_stream() {
        let mut merge = Merge::new();
        let now = Instant::now();
        let anchor = quote("SHFE", b"sn2701", 10);
        merge.output.clear();
        merge.quote(0, anchor, now);
        merge.quote(1, anchor, now);
        merge.output.clear();
        let later = now + STALL;
        merge.quote(1, quote("SHFE", b"sn2701", 12), later);
        merge.advance(later);
        let quote = merge.output.pop_front().unwrap();
        assert_eq!(quote.source_sequence, 12);
        assert_eq!(quote.source_generation, 1);
        assert_eq!(merge.primary, Some(1));
    }

    #[test]
    fn conflicting_copies_never_join() {
        let mut merge = Merge::new();
        let now = Instant::now();
        let anchor = quote("SHFE", b"sn2701", 10);
        merge.output.clear();
        merge.quote(0, anchor, now);
        merge.output.clear();
        merge.quote(
            1,
            MarketDataMsg {
                last_price: 101.0,
                ..anchor
            },
            now,
        );
        assert_ne!(merge.output.pop_front().unwrap().flags & SOURCE_RESET, 0);
    }
}
