//! Pure RPI depth: `rpi_obu` size minus `obu` size per price, paired by update id.
//!
//! Gate pushes `{market}.obu` and `{market}.rpi_obu` for the same contract and depth
//! as one frame pair sharing `t`, `U` and `u`. Each leg keeps both local books and,
//! once both sides reach the same `u`, emits the RPI-only remainder of every level
//! in the RPI book. The RPI book is a superset of the normal book, so the normal
//! book's top N always covers the price range of the RPI book's top N.

use super::*;
use std::collections::BTreeMap;

/// Price → integer key; both books decode the same mantissa/exponent, so the
/// scaled-and-rounded value is stable even when the raw f64 bits differ.
const PRICE_KEY_SCALE: f64 = 1e12;
/// Sizes come from integer mantissas; treat sub-ppb residue as float noise.
const QTY_EPS_REL: f64 = 1e-9;

fn price_key(price: f64) -> i64 {
    (price * PRICE_KEY_SCALE).round() as i64
}

#[derive(Default)]
struct LocalBook {
    bids: BTreeMap<i64, (f64, f64)>,
    asks: BTreeMap<i64, (f64, f64)>,
    u: i64,
    timestamp_us: i64,
}

impl LocalBook {
    fn apply(&mut self, book: &gate_codec::Book) {
        if book.is_snapshot {
            self.bids.clear();
            self.asks.clear();
        }
        for (side, levels) in [(&mut self.bids, &book.bids), (&mut self.asks, &book.asks)] {
            for level in levels {
                let key = price_key(level.price);
                if level.amount == 0.0 {
                    side.remove(&key);
                } else {
                    side.insert(key, (level.price, level.amount));
                }
            }
        }
        self.u = book.final_update_id;
        self.timestamp_us = book.timestamp_us;
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct PureBook {
    pub(super) symbol: String,
    pub(super) u: i64,
    pub(super) timestamp_us: i64,
    /// Best first: bids descending, asks ascending.
    pub(super) bids: Vec<gate_codec::Level>,
    pub(super) asks: Vec<gate_codec::Level>,
    /// RPI size below the normal size at a price (should never happen).
    pub(super) negative_levels: u64,
}

type PureFingerprint = (Vec<(i64, u64)>, Vec<(i64, u64)>);

impl PureBook {
    fn fingerprint(&self) -> PureFingerprint {
        let side = |levels: &[gate_codec::Level]| {
            levels
                .iter()
                .map(|l| (price_key(l.price), l.amount.to_bits()))
                .collect::<Vec<_>>()
        };
        (side(&self.bids), side(&self.asks))
    }

    fn into_snapshot(self) -> gate_codec::Book {
        gate_codec::Book {
            symbol: self.symbol,
            timestamp_us: self.timestamp_us,
            seq_id: self.u,
            prev_seq_id: i64::MIN,
            first_update_id: self.u,
            final_update_id: self.u,
            gap_check: false,
            is_snapshot: true,
            bids: self.bids,
            asks: self.asks,
        }
    }
}

fn pure_side<'a>(
    rpi: impl Iterator<Item = (&'a i64, &'a (f64, f64))>,
    std: &BTreeMap<i64, (f64, f64)>,
    negative: &mut u64,
) -> Vec<gate_codec::Level> {
    let mut out = Vec::new();
    for (key, &(price, rpi_qty)) in rpi {
        let std_qty = std.get(key).map_or(0.0, |&(_, qty)| qty);
        let pure = rpi_qty - std_qty;
        let eps = QTY_EPS_REL * rpi_qty.abs().max(std_qty.abs());
        if pure > eps {
            out.push(gate_codec::Level {
                price,
                amount: pure,
            });
        } else if pure < -eps {
            *negative += 1;
        }
    }
    out
}

fn compute_pure(symbol: &str, rpi: &LocalBook, std: &LocalBook) -> PureBook {
    let mut negative_levels = 0;
    let bids = pure_side(rpi.bids.iter().rev(), &std.bids, &mut negative_levels);
    let asks = pure_side(rpi.asks.iter(), &std.asks, &mut negative_levels);
    PureBook {
        symbol: symbol.to_string(),
        u: rpi.u,
        timestamp_us: rpi.timestamp_us,
        bids,
        asks,
        negative_levels,
    }
}

/// One connection's normal and RPI books. Only fed frames that passed that
/// connection's continuity check.
#[derive(Default)]
pub(super) struct LegBooks {
    std: FastHashMap<String, LocalBook>,
    rpi: FastHashMap<String, LocalBook>,
}

impl LegBooks {
    /// Apply one side; returns the pure book when both sides now sit at the same `u`.
    pub(super) fn apply(&mut self, is_rpi: bool, book: &gate_codec::Book) -> Option<PureBook> {
        let (mine, other) = if is_rpi {
            (&mut self.rpi, &self.std)
        } else {
            (&mut self.std, &self.rpi)
        };
        let mine = mine.entry(book.symbol.clone()).or_default();
        mine.apply(book);
        let other = other.get(&book.symbol)?;
        if other.u != mine.u {
            return None;
        }
        Some(if is_rpi {
            compute_pure(&book.symbol, mine, other)
        } else {
            compute_pure(&book.symbol, other, mine)
        })
    }

    pub(super) fn clear(&mut self) {
        self.std.clear();
        self.rpi.clear();
    }
}

/// Merged A/B output for pure RPI snapshots: de-dup by `u`, publish only on change.
pub(super) struct PureSink {
    pub(super) service_name: String,
    pub(super) publisher: Rc<SpreadIncrementalPublisher>,
    pub(super) state: Rc<RefCell<SharedState>>,
    last_published: RefCell<FastHashMap<String, PureFingerprint>>,
    max_levels: usize,
    pub(super) paired: Cell<u64>,
    pub(super) unchanged_skipped: Cell<u64>,
    pub(super) negative_levels: Cell<u64>,
    pub(super) stale_snapshot_resets: Cell<u64>,
}

impl PureSink {
    pub(super) fn new(
        publisher: Rc<SpreadIncrementalPublisher>,
        state: Rc<RefCell<SharedState>>,
        max_levels: usize,
    ) -> Self {
        Self {
            service_name: publisher.service_name().to_string(),
            publisher,
            state,
            last_published: RefCell::new(fast_hash_map_with_capacity(0)),
            max_levels,
            paired: Cell::new(0),
            unchanged_skipped: Cell::new(0),
            negative_levels: Cell::new(0),
            stale_snapshot_resets: Cell::new(0),
        }
    }

    pub(super) fn publish(&self, label: &str, pure: PureBook) {
        self.paired.set(self.paired.get() + 1);
        if pure.negative_levels > 0 {
            self.negative_levels
                .set(self.negative_levels.get() + pure.negative_levels);
        }
        let mut s = self.state.borrow_mut();
        let slot = s.symbol_state.incremental_slot(&pure.symbol);
        let fingerprint = pure.fingerprint();
        let snapshot = pure.into_snapshot();
        let policy = dedup_policy_for(&s, &slot, &snapshot, Instant::now());
        if should_drop_incremental(&slot, snapshot.final_update_id, true, policy) {
            s.incremental_dropped_by_seq += 1;
            return;
        }
        if policy == IncrementalDedupPolicy::SnapshotCanReset {
            self.stale_snapshot_resets
                .set(self.stale_snapshot_resets.get() + 1);
            log::warn!(
                "spread_pbs ws[{}] gate pure rpi {} u={} <= published u={} after silence; accepting as new baseline",
                label,
                snapshot.symbol,
                snapshot.final_update_id,
                slot.prev,
            );
        }
        let mut last = self.last_published.borrow_mut();
        if last.get(&snapshot.symbol) == Some(&fingerprint) {
            s.symbol_state
                .set_incremental_slot(slot, snapshot.final_update_id);
            self.unchanged_skipped.set(self.unchanged_skipped.get() + 1);
            return;
        }
        last.insert(snapshot.symbol.clone(), fingerprint);
        process_incremental_frame(
            &mut s,
            &self.publisher,
            Some(slot.idx),
            policy,
            crate::spread_pbs::gate::book_to_incremental(snapshot),
            Some(self.max_levels),
            false,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lv(price: f64, amount: f64) -> gate_codec::Level {
        gate_codec::Level { price, amount }
    }

    fn frame(
        snapshot: bool,
        first: i64,
        last: i64,
        bids: Vec<gate_codec::Level>,
        asks: Vec<gate_codec::Level>,
    ) -> gate_codec::Book {
        gate_codec::Book {
            symbol: "BTC_USDT".to_string(),
            timestamp_us: last * 10,
            seq_id: last,
            prev_seq_id: first - 1,
            first_update_id: first,
            final_update_id: last,
            gap_check: false,
            is_snapshot: snapshot,
            bids,
            asks,
        }
    }

    #[test]
    fn pairs_by_update_id_and_subtracts_normal_size() {
        let mut legs = LegBooks::default();
        let rpi = frame(
            true,
            0,
            10,
            vec![lv(100.2, 5.0), lv(100.1, 3.0), lv(100.0, 4.0)],
            vec![lv(100.3, 2.0), lv(100.4, 7.0)],
        );
        let std = frame(
            true,
            0,
            10,
            vec![lv(100.1, 3.0), lv(100.0, 1.5), lv(99.9, 9.0)],
            vec![lv(100.3, 0.5), lv(100.5, 1.0)],
        );
        assert_eq!(legs.apply(true, &rpi), None, "normal side not yet at u=10");
        let pure = legs.apply(false, &std).expect("paired at u=10");
        assert_eq!(pure.u, 10);
        assert_eq!(pure.timestamp_us, 100);
        // 100.2 is RPI-only; 100.1 fully normal; 100.0 has 2.5 RPI; 99.9 outside the RPI book.
        assert_eq!(pure.bids, vec![lv(100.2, 5.0), lv(100.0, 2.5)]);
        assert_eq!(pure.asks, vec![lv(100.3, 1.5), lv(100.4, 7.0)]);
        assert_eq!(pure.negative_levels, 0);

        // Delta pair at u=11: RPI-only bid at 100.2 is filled, normal 100.0 grows.
        let rpi = frame(false, 11, 11, vec![lv(100.2, 0.0), lv(100.0, 5.0)], vec![]);
        let std = frame(false, 11, 11, vec![lv(100.0, 2.5)], vec![]);
        assert_eq!(legs.apply(false, &std), None, "RPI side still at u=10");
        let pure = legs.apply(true, &rpi).expect("paired at u=11");
        assert_eq!(pure.bids, vec![lv(100.0, 2.5)]);
        assert_eq!(pure.asks, vec![lv(100.3, 1.5), lv(100.4, 7.0)]);
    }

    #[test]
    fn float_residue_is_zero_and_negative_remainders_are_counted() {
        let mut legs = LegBooks::default();
        legs.apply(
            false,
            &frame(true, 0, 1, vec![lv(1688.0, 5677.4)], vec![lv(1688.1, 2.0)]),
        );
        let pure = legs
            .apply(
                true,
                &frame(
                    true,
                    0,
                    1,
                    vec![lv(1688.0, 5677.400000000001)],
                    vec![lv(1688.1, 1.0)],
                ),
            )
            .expect("paired");
        assert!(pure.bids.is_empty(), "sub-ppb residue is not RPI size");
        assert!(pure.asks.is_empty());
        assert_eq!(pure.negative_levels, 1);
    }

    #[test]
    fn clearing_leg_books_requires_a_new_pair() {
        let mut legs = LegBooks::default();
        legs.apply(false, &frame(true, 0, 5, vec![lv(1.0, 1.0)], vec![]));
        legs.clear();
        assert_eq!(
            legs.apply(true, &frame(true, 0, 5, vec![lv(1.0, 2.0)], vec![])),
            None
        );
    }

    #[test]
    fn fingerprint_ignores_update_id_but_detects_size_changes() {
        let base = PureBook {
            symbol: "BTC_USDT".to_string(),
            u: 1,
            timestamp_us: 1,
            bids: vec![lv(100.0, 1.0)],
            asks: vec![],
            negative_levels: 0,
        };
        let mut same_levels_new_u = base.clone();
        same_levels_new_u.u = 2;
        assert_eq!(base.fingerprint(), same_levels_new_u.fingerprint());
        let mut size_changed = base.clone();
        size_changed.bids[0].amount = 2.0;
        assert_ne!(base.fingerprint(), size_changed.fingerprint());
    }
}
