//! Full-book baseline metrics and the 23 depth formulas supplied by the operator.
use crate::depth_pub::orderbook::{key_to_price, price_to_key, OrderBook};
use mkt_parsers::msg::mkt_msg::Level;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Changes of absolute displayed quantity, valued at the updated level price.
#[derive(Debug, Clone, Default)]
pub struct DepthBookFlow {
    added: [f64; 2],
    removed: [f64; 2],
    touch_added: [f64; 2],
    touch_removed: [f64; 2],
}

impl DepthBookFlow {
    pub fn observe(&mut self, book: &OrderBook, bids: &[Level], asks: &[Level]) {
        for (side, levels) in [bids, asks].into_iter().enumerate() {
            let order_side = if side == 0 { &book.bids } else { &book.asks };
            let touch = order_side.best_level(side == 0).map(|level| level.0);
            // Match OrderBookSide's same-update-id rule: a stored level cannot
            // be overwritten twice, but a deletion permits a later insertion.
            let mut previous_in_packet = HashMap::new();
            for level in levels {
                let (price, amount) = (level.price, level.amount);
                if !price.is_finite() || price <= 0.0 || !amount.is_finite() || amount < 0.0 {
                    continue;
                }
                let key = price_to_key(price);
                if previous_in_packet
                    .get(&key)
                    .is_some_and(|amount| *amount != 0.0)
                {
                    continue;
                }
                let previous = previous_in_packet
                    .insert(key, amount)
                    .unwrap_or_else(|| order_side.amount_at_price_key(key).unwrap_or(0.0));
                let delta = (amount - previous) * price;
                let at_touch = touch.is_some_and(|best| {
                    if side == 0 {
                        price >= best
                    } else {
                        price <= best
                    }
                });
                if delta > 0.0 {
                    self.added[side] += delta;
                    if at_touch {
                        self.touch_added[side] += delta;
                    }
                } else if delta < 0.0 {
                    self.removed[side] -= delta;
                    if at_touch {
                        self.touch_removed[side] -= delta;
                    }
                }
            }
        }
    }
}

impl DepthBookBar {
    pub fn from_book(book: &OrderBook, flow: DepthBookFlow) -> Self {
        let mut row = Self::default();
        let Some(bid) = book.best_bid_price() else {
            return row;
        };
        let Some(ask) = book.best_ask_price() else {
            return row;
        };
        if !book.is_valid() || !bid.is_finite() || !ask.is_finite() || bid <= 0.0 || ask < bid {
            return row;
        }
        let mid = (bid + ask) * 0.5;
        let bids = side_metrics(book.bids.levels_keys().into_iter().rev(), mid, true);
        let asks = side_metrics(book.asks.levels_keys().into_iter(), mid, false);
        [
            row.depth_bid_notional_bps1,
            row.depth_bid_notional_bps5,
            row.depth_bid_notional_bps10,
            row.depth_bid_notional_bps20,
            row.depth_bid_notional_bps50,
            row.depth_bid_notional_bps100,
        ] = bids.0;
        [
            row.depth_ask_notional_bps1,
            row.depth_ask_notional_bps5,
            row.depth_ask_notional_bps10,
            row.depth_ask_notional_bps20,
            row.depth_ask_notional_bps50,
            row.depth_ask_notional_bps100,
        ] = asks.0;
        row.depth_bid_notional_all = bids.1;
        row.depth_ask_notional_all = asks.1;
        [
            row.depth_bid_bps_to_10k,
            row.depth_bid_bps_to_50k,
            row.depth_bid_bps_to_100k,
        ] = bids.2;
        [
            row.depth_ask_bps_to_10k,
            row.depth_ask_bps_to_50k,
            row.depth_ask_bps_to_100k,
        ] = asks.2;
        row.depth_n_bid = bids.3 as f64;
        row.depth_n_ask = asks.3 as f64;
        [row.depth_bid_add_notional, row.depth_ask_add_notional] = flow.added;
        [row.depth_bid_sub_notional, row.depth_ask_sub_notional] = flow.removed;
        [
            row.depth_touch_bid_add_notional,
            row.depth_touch_ask_add_notional,
        ] = flow.touch_added;
        [
            row.depth_touch_bid_sub_notional,
            row.depth_touch_ask_sub_notional,
        ] = flow.touch_removed;
        row
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::factor_pub::fusion_factor_pub::{
        app::BaselineReplayState, plan::ExtraFactorId, SymbolFactorPlan,
    };

    #[test]
    fn formulas_match_the_operator_python_reference() {
        let fixtures: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/depth_book_factors.json"))
                .unwrap();
        for case in fixtures["cases"].as_array().unwrap() {
            let row: DepthBookBar = serde_json::from_value(case["input"].clone()).unwrap();
            for id in DepthFactorId::ALL {
                let actual = row.factor(id);
                let expected = &case["expected"][id.name()];
                if expected == "NaN" {
                    assert!(actual.is_nan(), "{} {}: {actual}", case["name"], id.name());
                } else {
                    let expected = expected.as_f64().unwrap();
                    assert!(
                        (actual - expected).abs() <= 1e-12 * expected.abs().max(1.0),
                        "{} {}: {actual} != {expected}",
                        case["name"],
                        id.name()
                    );
                }
            }
        }
        // These less restrictive guards intentionally mirror the supplied Python.
        assert_eq!(log_ba(f64::INFINITY, 1.0), f64::INFINITY);
        assert_eq!(log_ba(1.0, f64::INFINITY), f64::NEG_INFINITY);
        assert!(far_share(1.0, f64::INFINITY).is_nan());
        assert!(imbalance(f64::INFINITY, 1.0).is_nan());
        assert_eq!(imbalance(-2.0, 6.0), -2.0);
    }

    #[test]
    fn full_book_metrics_include_deeper_than_twenty_levels() {
        let mut book = OrderBook::new();
        let bids: Vec<_> = (0..25).map(|i| (99.99 - i as f64 * 0.01, 100.0)).collect();
        let asks: Vec<_> = (0..30).map(|i| (100.01 + i as f64 * 0.01, 50.0)).collect();
        book.replace_snapshot(&bids, &asks, 1, 1);
        let row = DepthBookBar::from_book(&book, DepthBookFlow::default());
        assert_eq!(row.depth_n_bid, 25.0);
        assert_eq!(row.depth_n_ask, 30.0);
        assert!((row.depth_bid_notional_all - 249675.0).abs() < 1e-6);
        assert!((row.depth_ask_notional_all - 150232.5).abs() < 1e-6);
        assert_eq!(row.depth_bid_notional_bps100, row.depth_bid_notional_all);
        assert!((row.depth_bid_bps_to_10k - 2.0).abs() < 1e-9);
        assert!((row.depth_ask_bps_to_10k - 2.0).abs() < 1e-9);
        assert!((row.depth_bid_bps_to_100k - 11.0).abs() < 1e-9);
        assert!(row.depth_bid_notional_bps5 < row.depth_bid_notional_all);
        let mut shallow = OrderBook::new();
        shallow.replace_snapshot(&[(99.0, 1.0)], &[(101.0, 1.0)], 1, 1);
        assert!(DepthBookBar::from_book(&shallow, DepthBookFlow::default())
            .depth_bid_bps_to_10k
            .is_nan());
        assert!(
            DepthBookBar::from_book(&OrderBook::new(), DepthBookFlow::default())
                .factor(DepthFactorId::ImbAll)
                .is_nan()
        );
    }

    #[test]
    fn flows_match_absolute_quantity_updates_and_orderbook_duplicate_rules() {
        let mut book = OrderBook::new();
        book.replace_snapshot(
            &[(99.0, 2.0), (98.0, 3.0)],
            &[(101.0, 4.0), (102.0, 2.0)],
            1,
            1,
        );
        let bids = [
            Level::from_values(99.0, 3.0),
            Level::from_values(99.0, 9.0),
            Level::from_values(98.0, 0.0),
            Level::from_values(99.5, 2.0),
        ];
        let asks = [
            Level::from_values(101.0, 2.0),
            Level::from_values(102.0, 3.0),
        ];
        let mut flow = DepthBookFlow::default();
        flow.observe(&book, &bids, &asks);
        assert_eq!(flow.added, [298.0, 102.0]);
        assert_eq!(flow.removed, [294.0, 202.0]);
        assert_eq!(flow.touch_added, [298.0, 0.0]);
        assert_eq!(flow.touch_removed, [0.0, 202.0]);
        let bids: Vec<_> = bids.iter().map(|v| (v.price, v.amount)).collect();
        let asks: Vec<_> = asks.iter().map(|v| (v.price, v.amount)).collect();
        book.apply_update(&bids, &asks, 2, 2);
        assert_eq!(book.bids.amount_at_price(99.0), Some(3.0));
        let row = DepthBookBar::from_book(&book, flow);
        assert_eq!(row.depth_bid_add_notional, 298.0);
        assert_eq!(row.depth_ask_sub_notional, 202.0);
    }

    #[test]
    fn all_depth_bindings_round_trip_without_changing_existing_indexes() {
        let plan = SymbolFactorPlan::from_factor_names(
            "depth-test",
            DEPTH_FACTOR_NAMES.iter().map(|n| n.to_string()).collect(),
        )
        .unwrap();
        BaselineReplayState::validate_factor_plan(&plan).unwrap();
        for id in DepthFactorId::ALL {
            let binding = ExtraFactorId::from_name(id.name()).unwrap();
            assert_eq!(ExtraFactorId::from_index(binding.as_index()), Some(binding));
            assert_eq!(
                ExtraFactorId::index_to_name(binding.as_index()),
                Some(id.name())
            );
        }
        assert_eq!(ExtraFactorId::AvgPrice.as_index(), 1000);
        assert_eq!(ExtraFactorId::Ask0Volume1.as_index(), 1038);
    }
}

fn side_metrics(
    levels: impl Iterator<Item = (i64, f64)>,
    mid: f64,
    bid: bool,
) -> ([f64; 6], f64, [f64; 3], usize) {
    let mut bands = [0.0; 6];
    let mut total = 0.0;
    let mut distances = [f64::NAN; 3];
    let mut count = 0;
    for (key, amount) in levels {
        let price = key_to_price(key);
        if !price.is_finite() || price <= 0.0 || !amount.is_finite() || amount <= 0.0 {
            continue;
        }
        let notional = price * amount;
        let distance = (if bid { mid - price } else { price - mid }) / mid * 10_000.0;
        total += notional;
        count += 1;
        for (i, width) in [1.0, 5.0, 10.0, 20.0, 50.0, 100.0].into_iter().enumerate() {
            if distance <= width {
                bands[i] += notional;
            }
        }
        for (i, target) in [10_000.0, 50_000.0, 100_000.0].into_iter().enumerate() {
            if distances[i].is_nan() && total >= target {
                distances[i] = distance;
            }
        }
    }
    (bands, total, distances, count)
}

pub const DEPTH_FACTOR_NAMES: [&str; 23] = [
    "depth_imb_bps1",
    "depth_imb_bps5",
    "depth_imb_bps10",
    "depth_imb_bps20",
    "depth_imb_bps50",
    "depth_imb_bps100",
    "depth_imb_all",
    "depth_log_ba_bps5",
    "depth_log_ba_bps20",
    "depth_log_ba_bps100",
    "depth_near_far_bid",
    "depth_near_far_ask",
    "depth_far_share_bid",
    "depth_far_share_ask",
    "depth_gap_bps_10k",
    "depth_gap_bps_50k",
    "depth_gap_bps_100k",
    "depth_flow_add_imb",
    "depth_flow_sub_imb",
    "depth_flow_net",
    "depth_touch_add_imb",
    "depth_touch_net",
    "depth_level_imb",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DepthFactorId {
    ImbBps1,
    ImbBps5,
    ImbBps10,
    ImbBps20,
    ImbBps50,
    ImbBps100,
    ImbAll,
    LogBaBps5,
    LogBaBps20,
    LogBaBps100,
    NearFarBid,
    NearFarAsk,
    FarShareBid,
    FarShareAsk,
    GapBps10k,
    GapBps50k,
    GapBps100k,
    FlowAddImb,
    FlowSubImb,
    FlowNet,
    TouchAddImb,
    TouchNet,
    LevelImb,
}

impl DepthFactorId {
    pub const ALL: [Self; 23] = [
        Self::ImbBps1,
        Self::ImbBps5,
        Self::ImbBps10,
        Self::ImbBps20,
        Self::ImbBps50,
        Self::ImbBps100,
        Self::ImbAll,
        Self::LogBaBps5,
        Self::LogBaBps20,
        Self::LogBaBps100,
        Self::NearFarBid,
        Self::NearFarAsk,
        Self::FarShareBid,
        Self::FarShareAsk,
        Self::GapBps10k,
        Self::GapBps50k,
        Self::GapBps100k,
        Self::FlowAddImb,
        Self::FlowSubImb,
        Self::FlowNet,
        Self::TouchAddImb,
        Self::TouchNet,
        Self::LevelImb,
    ];
    pub fn from_name(name: &str) -> Option<Self> {
        DEPTH_FACTOR_NAMES
            .iter()
            .position(|candidate| *candidate == name)
            .map(|i| Self::ALL[i])
    }
    pub fn from_index(index: usize) -> Option<Self> {
        Self::ALL.get(index).copied()
    }
    pub const fn index(self) -> usize {
        self as usize
    }
    pub fn name(self) -> &'static str {
        DEPTH_FACTOR_NAMES[self.index()]
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DepthBookBar {
    pub depth_bid_notional_bps1: f64,
    pub depth_bid_notional_bps5: f64,
    pub depth_bid_notional_bps10: f64,
    pub depth_bid_notional_bps20: f64,
    pub depth_bid_notional_bps50: f64,
    pub depth_bid_notional_bps100: f64,
    pub depth_ask_notional_bps1: f64,
    pub depth_ask_notional_bps5: f64,
    pub depth_ask_notional_bps10: f64,
    pub depth_ask_notional_bps20: f64,
    pub depth_ask_notional_bps50: f64,
    pub depth_ask_notional_bps100: f64,
    pub depth_bid_notional_all: f64,
    pub depth_ask_notional_all: f64,
    pub depth_bid_bps_to_10k: f64,
    pub depth_bid_bps_to_50k: f64,
    pub depth_bid_bps_to_100k: f64,
    pub depth_ask_bps_to_10k: f64,
    pub depth_ask_bps_to_50k: f64,
    pub depth_ask_bps_to_100k: f64,
    pub depth_bid_add_notional: f64,
    pub depth_bid_sub_notional: f64,
    pub depth_ask_add_notional: f64,
    pub depth_ask_sub_notional: f64,
    pub depth_touch_bid_add_notional: f64,
    pub depth_touch_bid_sub_notional: f64,
    pub depth_touch_ask_add_notional: f64,
    pub depth_touch_ask_sub_notional: f64,
    pub depth_n_bid: f64,
    pub depth_n_ask: f64,
}

impl Default for DepthBookBar {
    fn default() -> Self {
        Self {
            depth_bid_notional_bps1: f64::NAN,
            depth_bid_notional_bps5: f64::NAN,
            depth_bid_notional_bps10: f64::NAN,
            depth_bid_notional_bps20: f64::NAN,
            depth_bid_notional_bps50: f64::NAN,
            depth_bid_notional_bps100: f64::NAN,
            depth_ask_notional_bps1: f64::NAN,
            depth_ask_notional_bps5: f64::NAN,
            depth_ask_notional_bps10: f64::NAN,
            depth_ask_notional_bps20: f64::NAN,
            depth_ask_notional_bps50: f64::NAN,
            depth_ask_notional_bps100: f64::NAN,
            depth_bid_notional_all: f64::NAN,
            depth_ask_notional_all: f64::NAN,
            depth_bid_bps_to_10k: f64::NAN,
            depth_bid_bps_to_50k: f64::NAN,
            depth_bid_bps_to_100k: f64::NAN,
            depth_ask_bps_to_10k: f64::NAN,
            depth_ask_bps_to_50k: f64::NAN,
            depth_ask_bps_to_100k: f64::NAN,
            depth_bid_add_notional: f64::NAN,
            depth_bid_sub_notional: f64::NAN,
            depth_ask_add_notional: f64::NAN,
            depth_ask_sub_notional: f64::NAN,
            depth_touch_bid_add_notional: f64::NAN,
            depth_touch_bid_sub_notional: f64::NAN,
            depth_touch_ask_add_notional: f64::NAN,
            depth_touch_ask_sub_notional: f64::NAN,
            depth_n_bid: f64::NAN,
            depth_n_ask: f64::NAN,
        }
    }
}

fn imbalance(bid: f64, ask: f64) -> f64 {
    if bid.is_finite() && ask.is_finite() && bid + ask > 0.0 {
        (bid - ask) / (bid + ask)
    } else {
        f64::NAN
    }
}
fn ratio(num: f64, den: f64) -> f64 {
    if num.is_finite() && den.is_finite() && den > 0.0 {
        num / den
    } else {
        f64::NAN
    }
}
fn log_ba(bid: f64, ask: f64) -> f64 {
    if bid > 0.0 && ask > 0.0 {
        (bid / ask).ln()
    } else {
        f64::NAN
    }
}
fn far_share(near: f64, far: f64) -> f64 {
    if near.is_finite() && far > 0.0 {
        (far - near) / far
    } else {
        f64::NAN
    }
}

impl DepthBookBar {
    pub fn factor(&self, id: DepthFactorId) -> f64 {
        match id {
            DepthFactorId::ImbBps1 => {
                imbalance(self.depth_bid_notional_bps1, self.depth_ask_notional_bps1)
            }
            DepthFactorId::ImbBps5 => {
                imbalance(self.depth_bid_notional_bps5, self.depth_ask_notional_bps5)
            }
            DepthFactorId::ImbBps10 => {
                imbalance(self.depth_bid_notional_bps10, self.depth_ask_notional_bps10)
            }
            DepthFactorId::ImbBps20 => {
                imbalance(self.depth_bid_notional_bps20, self.depth_ask_notional_bps20)
            }
            DepthFactorId::ImbBps50 => {
                imbalance(self.depth_bid_notional_bps50, self.depth_ask_notional_bps50)
            }
            DepthFactorId::ImbBps100 => imbalance(
                self.depth_bid_notional_bps100,
                self.depth_ask_notional_bps100,
            ),
            DepthFactorId::ImbAll => {
                imbalance(self.depth_bid_notional_all, self.depth_ask_notional_all)
            }
            DepthFactorId::LogBaBps5 => {
                log_ba(self.depth_bid_notional_bps5, self.depth_ask_notional_bps5)
            }
            DepthFactorId::LogBaBps20 => {
                log_ba(self.depth_bid_notional_bps20, self.depth_ask_notional_bps20)
            }
            DepthFactorId::LogBaBps100 => log_ba(
                self.depth_bid_notional_bps100,
                self.depth_ask_notional_bps100,
            ),
            DepthFactorId::NearFarBid => {
                ratio(self.depth_bid_notional_bps5, self.depth_bid_notional_bps50)
            }
            DepthFactorId::NearFarAsk => {
                ratio(self.depth_ask_notional_bps5, self.depth_ask_notional_bps50)
            }
            DepthFactorId::FarShareBid => far_share(
                self.depth_bid_notional_bps10,
                self.depth_bid_notional_bps100,
            ),
            DepthFactorId::FarShareAsk => far_share(
                self.depth_ask_notional_bps10,
                self.depth_ask_notional_bps100,
            ),
            DepthFactorId::GapBps10k => self.depth_ask_bps_to_10k - self.depth_bid_bps_to_10k,
            DepthFactorId::GapBps50k => self.depth_ask_bps_to_50k - self.depth_bid_bps_to_50k,
            DepthFactorId::GapBps100k => self.depth_ask_bps_to_100k - self.depth_bid_bps_to_100k,
            DepthFactorId::FlowAddImb => {
                imbalance(self.depth_bid_add_notional, self.depth_ask_add_notional)
            }
            DepthFactorId::FlowSubImb => {
                imbalance(self.depth_bid_sub_notional, self.depth_ask_sub_notional)
            }
            DepthFactorId::FlowNet => imbalance(
                self.depth_bid_add_notional - self.depth_bid_sub_notional,
                self.depth_ask_add_notional - self.depth_ask_sub_notional,
            ),
            DepthFactorId::TouchAddImb => imbalance(
                self.depth_touch_bid_add_notional,
                self.depth_touch_ask_add_notional,
            ),
            DepthFactorId::TouchNet => imbalance(
                self.depth_touch_bid_add_notional - self.depth_touch_bid_sub_notional,
                self.depth_touch_ask_add_notional - self.depth_touch_ask_sub_notional,
            ),
            DepthFactorId::LevelImb => imbalance(self.depth_n_bid, self.depth_n_ask),
        }
    }

    /// State fields take the last closed sub-bar; event flows add across sub-bars.
    pub fn merge(&mut self, source: &Self) {
        let depth_bid_add_notional = self.depth_bid_add_notional + source.depth_bid_add_notional;
        let depth_bid_sub_notional = self.depth_bid_sub_notional + source.depth_bid_sub_notional;
        let depth_ask_add_notional = self.depth_ask_add_notional + source.depth_ask_add_notional;
        let depth_ask_sub_notional = self.depth_ask_sub_notional + source.depth_ask_sub_notional;
        let depth_touch_bid_add_notional =
            self.depth_touch_bid_add_notional + source.depth_touch_bid_add_notional;
        let depth_touch_bid_sub_notional =
            self.depth_touch_bid_sub_notional + source.depth_touch_bid_sub_notional;
        let depth_touch_ask_add_notional =
            self.depth_touch_ask_add_notional + source.depth_touch_ask_add_notional;
        let depth_touch_ask_sub_notional =
            self.depth_touch_ask_sub_notional + source.depth_touch_ask_sub_notional;
        *self = source.clone();
        self.depth_bid_add_notional = depth_bid_add_notional;
        self.depth_bid_sub_notional = depth_bid_sub_notional;
        self.depth_ask_add_notional = depth_ask_add_notional;
        self.depth_ask_sub_notional = depth_ask_sub_notional;
        self.depth_touch_bid_add_notional = depth_touch_bid_add_notional;
        self.depth_touch_bid_sub_notional = depth_touch_bid_sub_notional;
        self.depth_touch_ask_add_notional = depth_touch_ask_add_notional;
        self.depth_touch_ask_sub_notional = depth_touch_ask_sub_notional;
    }
}
