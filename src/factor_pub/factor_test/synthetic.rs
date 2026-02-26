//! Deterministic synthetic data generator for factor testing.

use crate::common::trade_flow_feature_msg::{TradeFlowFeatureMsg, TRADE_FLOW_FEATURE_DIM};
use crate::factor_pub::fusion_factor_pub::app::{DepthLevel, DepthSnapshot};

const NUM_BARS: usize = 600;
const DEPTH_LEVELS: usize = 20;

/// Simple xorshift64 PRNG for deterministic generation (no rand dependency).
struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// Uniform f64 in [0, 1)
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Uniform f64 in [lo, hi)
    fn uniform(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.next_f64()
    }
}

pub struct ScenarioData {
    pub name: String,
    pub trade_flow_msgs: Vec<TradeFlowFeatureMsg>,
    pub depth_snapshots: Vec<DepthSnapshot>,
}

pub fn generate_all_scenarios() -> Vec<ScenarioData> {
    vec![
        generate_normal(42),
        generate_edge_flat(123),
        generate_edge_extreme(999),
    ]
}

fn make_trade_flow_msg(ts: i64, values: Vec<f64>) -> TradeFlowFeatureMsg {
    TradeFlowFeatureMsg {
        msg_type: 0,
        symbol_length: 7,
        symbol: "BTCUSDT".to_string(),
        venue: 0,
        ts,
        values,
    }
}

fn make_depth(bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) -> DepthSnapshot {
    DepthSnapshot {
        bids: bids
            .into_iter()
            .map(|(p, a)| DepthLevel { price: p, amount: a })
            .collect(),
        asks: asks
            .into_iter()
            .map(|(p, a)| DepthLevel { price: p, amount: a })
            .collect(),
    }
}

/// Embed depth snapshot into values array (after TRADE_FLOW_FEATURE_DIM fields).
fn embed_depth_into_values(values: &mut Vec<f64>, depth: &DepthSnapshot) {
    // bids: 20 levels × (price, amount)
    for i in 0..DEPTH_LEVELS {
        if let Some(level) = depth.bids.get(i) {
            values.push(level.price);
            values.push(level.amount);
        } else {
            values.push(0.0);
            values.push(0.0);
        }
    }
    // asks: 20 levels × (price, amount)
    for i in 0..DEPTH_LEVELS {
        if let Some(level) = depth.asks.get(i) {
            values.push(level.price);
            values.push(level.amount);
        } else {
            values.push(0.0);
            values.push(0.0);
        }
    }
}

/// Normal market scenario: BTC-like price ~40000 with random walk, normal volumes, gradient depth.
fn generate_normal(seed: u64) -> ScenarioData {
    let mut rng = Rng::new(seed);
    let mut msgs = Vec::with_capacity(NUM_BARS);
    let mut depths = Vec::with_capacity(NUM_BARS);
    let mut price = 40000.0;

    for i in 0..NUM_BARS {
        let ts = (i as i64 + 1) * 5_000_000; // 5s intervals in microseconds
        // Random walk
        price += rng.uniform(-50.0, 50.0);
        if price < 30000.0 {
            price = 30000.0;
        }

        let high = price + rng.uniform(0.0, 30.0);
        let low = price - rng.uniform(0.0, 30.0);
        let open = price + rng.uniform(-20.0, 20.0);
        let close = price + rng.uniform(-10.0, 10.0);
        let volume = rng.uniform(1.0, 100.0);
        let amount = volume * close;
        let buy_count = rng.uniform(10.0, 200.0).floor();
        let sell_count = rng.uniform(10.0, 200.0).floor();
        let buy_amount = rng.uniform(0.5, 0.7) * amount;
        let sell_amount = amount - buy_amount;
        let buy_volume = rng.uniform(0.4, 0.6) * volume;
        let sell_volume = volume - buy_volume;
        let large_order = rng.uniform(0.0, 20.0);
        let medium_order = rng.uniform(5.0, 30.0);
        let small_order = rng.uniform(10.0, 50.0);
        let large_buy = rng.uniform(0.0, 10.0);
        let large_sell = rng.uniform(0.0, 10.0);
        let medium_buy = rng.uniform(2.0, 15.0);
        let medium_sell = rng.uniform(2.0, 15.0);
        let small_buy = rng.uniform(5.0, 25.0);
        let small_sell = rng.uniform(5.0, 25.0);
        let vwap = (open + close + high + low) / 4.0;
        let buy_vwap = vwap + rng.uniform(-5.0, 5.0);
        let sell_vwap = vwap + rng.uniform(-5.0, 5.0);
        let net_buy_amount = buy_amount - sell_amount;
        let net_buy_volume = buy_volume - sell_volume;
        let net_buy_pct = if amount.abs() > 1e-12 {
            net_buy_amount / amount
        } else {
            0.0
        };
        let net_buy_large = large_buy - large_sell;
        let net_buy_medium = medium_buy - medium_sell;
        let net_buy_small = small_buy - small_sell;

        // 32 trade flow fields
        let mut values = vec![
            open,
            high,
            low,
            close,
            volume,
            amount,
            if buy_count + sell_count > 0.0 {
                amount / (buy_count + sell_count)
            } else {
                0.0
            }, // avg_amount
            buy_count + sell_count, // count
            buy_count,
            sell_count,
            buy_amount,
            sell_amount,
            buy_volume,
            sell_volume,
            large_order,
            medium_order,
            small_order,
            large_buy,
            large_sell,
            medium_buy,
            medium_sell,
            small_buy,
            small_sell,
            vwap,
            buy_vwap,
            sell_vwap,
            net_buy_amount,
            net_buy_volume,
            net_buy_pct,
            net_buy_large,
            net_buy_medium,
            net_buy_small,
        ];
        assert_eq!(values.len(), TRADE_FLOW_FEATURE_DIM);

        // Depth: gradient book around mid price
        let mid = (close + open) / 2.0;
        let mut bids = Vec::with_capacity(DEPTH_LEVELS);
        let mut asks = Vec::with_capacity(DEPTH_LEVELS);
        for j in 0..DEPTH_LEVELS {
            let offset = (j as f64 + 1.0) * 0.5;
            let bid_price = mid - offset;
            let ask_price = mid + offset;
            let bid_amount = rng.uniform(0.5, 5.0) * (1.0 + 0.1 * j as f64);
            let ask_amount = rng.uniform(0.5, 5.0) * (1.0 + 0.1 * j as f64);
            bids.push((bid_price, bid_amount));
            asks.push((ask_price, ask_amount));
        }

        let depth = make_depth(bids, asks);
        embed_depth_into_values(&mut values, &depth);

        msgs.push(make_trade_flow_msg(ts, values));
        depths.push(depth);
    }

    ScenarioData {
        name: "normal".to_string(),
        trade_flow_msgs: msgs,
        depth_snapshots: depths,
    }
}

/// Edge case: all closes identical (std=0), constant volume, uniform depth.
fn generate_edge_flat(seed: u64) -> ScenarioData {
    let mut rng = Rng::new(seed);
    let mut msgs = Vec::with_capacity(NUM_BARS);
    let mut depths = Vec::with_capacity(NUM_BARS);
    let price = 40000.0;

    for i in 0..NUM_BARS {
        let ts = (i as i64 + 1) * 5_000_000;

        let volume = 50.0;
        let amount = volume * price;
        let buy_count = 100.0;
        let sell_count = 100.0;
        let buy_amount = 0.5 * amount;
        let sell_amount = 0.5 * amount;
        let buy_volume = 25.0;
        let sell_volume = 25.0;
        let large_order = 10.0;
        let medium_order = 20.0;
        let small_order = 30.0;
        let large_buy = 5.0;
        let large_sell = 5.0;
        let medium_buy = 10.0;
        let medium_sell = 10.0;
        let small_buy = 15.0;
        let small_sell = 15.0;
        let vwap = price;
        let buy_vwap = price;
        let sell_vwap = price;

        let mut values = vec![
            price,        // open
            price,        // high
            price,        // low
            price,        // close
            volume,       // volume
            amount,       // amount
            amount / 200.0, // avg_amount
            200.0,        // count
            buy_count,
            sell_count,
            buy_amount,
            sell_amount,
            buy_volume,
            sell_volume,
            large_order,
            medium_order,
            small_order,
            large_buy,
            large_sell,
            medium_buy,
            medium_sell,
            small_buy,
            small_sell,
            vwap,
            buy_vwap,
            sell_vwap,
            0.0, // net_buy_amount
            0.0, // net_buy_volume
            0.0, // net_buy_pct
            0.0, // net_buy_large
            0.0, // net_buy_medium
            0.0, // net_buy_small
        ];
        assert_eq!(values.len(), TRADE_FLOW_FEATURE_DIM);

        // Uniform depth: all levels same amount
        let mut bids = Vec::with_capacity(DEPTH_LEVELS);
        let mut asks = Vec::with_capacity(DEPTH_LEVELS);
        for j in 0..DEPTH_LEVELS {
            let offset = (j as f64 + 1.0) * 0.5;
            bids.push((price - offset, 10.0));
            asks.push((price + offset, 10.0));
        }

        let depth = make_depth(bids, asks);
        embed_depth_into_values(&mut values, &depth);

        msgs.push(make_trade_flow_msg(ts, values));
        depths.push(depth);
        let _ = rng.next_u64(); // consume rng to keep deterministic
    }

    ScenarioData {
        name: "edge_flat".to_string(),
        trade_flow_msgs: msgs,
        depth_snapshots: depths,
    }
}

/// Edge case: contains zeros, very small values, division-prone values.
fn generate_edge_extreme(seed: u64) -> ScenarioData {
    let mut rng = Rng::new(seed);
    let mut msgs = Vec::with_capacity(NUM_BARS);
    let mut depths = Vec::with_capacity(NUM_BARS);
    let mut price = 40000.0;

    for i in 0..NUM_BARS {
        let ts = (i as i64 + 1) * 5_000_000;

        // Alternate between normal and extreme bars
        let is_extreme = i % 10 == 0;

        if !is_extreme {
            price += rng.uniform(-30.0, 30.0);
            if price < 30000.0 {
                price = 30000.0;
            }
        }

        let (open, high, low, close, volume, buy_count, sell_count) = if is_extreme {
            // Extreme: zero volume, zero counts
            (price, price, price, price, 0.0, 0.0, 0.0)
        } else {
            let h = price + rng.uniform(0.0, 20.0);
            let l = price - rng.uniform(0.0, 20.0);
            let o = price + rng.uniform(-10.0, 10.0);
            let c = price + rng.uniform(-5.0, 5.0);
            let v = rng.uniform(1.0, 80.0);
            let bc = rng.uniform(10.0, 150.0).floor();
            let sc = rng.uniform(10.0, 150.0).floor();
            (o, h, l, c, v, bc, sc)
        };

        let amount = volume * close;
        let buy_amount = if is_extreme { 0.0 } else { rng.uniform(0.3, 0.7) * amount };
        let sell_amount = if is_extreme { 0.0 } else { amount - buy_amount };
        let buy_volume = if is_extreme { 0.0 } else { rng.uniform(0.3, 0.6) * volume };
        let sell_volume = if is_extreme { 0.0 } else { volume - buy_volume };
        let large_order = if is_extreme { 0.0 } else { rng.uniform(0.0, 15.0) };
        let medium_order = if is_extreme { 0.0 } else { rng.uniform(3.0, 20.0) };
        let small_order = if is_extreme { 0.0 } else { rng.uniform(5.0, 40.0) };
        let large_buy = if is_extreme { 0.0 } else { rng.uniform(0.0, 8.0) };
        let large_sell = if is_extreme { 0.0 } else { rng.uniform(0.0, 8.0) };
        let medium_buy = if is_extreme { 0.0 } else { rng.uniform(1.0, 10.0) };
        let medium_sell = if is_extreme { 0.0 } else { rng.uniform(1.0, 10.0) };
        let small_buy = if is_extreme { 0.0 } else { rng.uniform(2.0, 20.0) };
        let small_sell = if is_extreme { 0.0 } else { rng.uniform(2.0, 20.0) };
        let vwap = if is_extreme { price } else { (open + close + high + low) / 4.0 };
        let buy_vwap = if is_extreme { price } else { vwap + rng.uniform(-3.0, 3.0) };
        let sell_vwap = if is_extreme { price } else { vwap + rng.uniform(-3.0, 3.0) };
        let net_buy_amount = buy_amount - sell_amount;
        let net_buy_volume = buy_volume - sell_volume;
        let total_count = buy_count + sell_count;
        let net_buy_pct = if amount.abs() > 1e-12 {
            net_buy_amount / amount
        } else {
            0.0
        };
        let net_buy_large = large_buy - large_sell;
        let net_buy_medium = medium_buy - medium_sell;
        let net_buy_small = small_buy - small_sell;

        let avg_amount = if total_count > 0.0 {
            amount / total_count
        } else {
            0.0
        };

        let mut values = vec![
            open,
            high,
            low,
            close,
            volume,
            amount,
            avg_amount,
            total_count,
            buy_count,
            sell_count,
            buy_amount,
            sell_amount,
            buy_volume,
            sell_volume,
            large_order,
            medium_order,
            small_order,
            large_buy,
            large_sell,
            medium_buy,
            medium_sell,
            small_buy,
            small_sell,
            vwap,
            buy_vwap,
            sell_vwap,
            net_buy_amount,
            net_buy_volume,
            net_buy_pct,
            net_buy_large,
            net_buy_medium,
            net_buy_small,
        ];
        assert_eq!(values.len(), TRADE_FLOW_FEATURE_DIM);

        // Depth: some levels have zero amount on extreme bars
        let mid = (close + open) / 2.0;
        let mut bids = Vec::with_capacity(DEPTH_LEVELS);
        let mut asks = Vec::with_capacity(DEPTH_LEVELS);
        for j in 0..DEPTH_LEVELS {
            let offset = (j as f64 + 1.0) * 0.5;
            let bid_price = mid - offset;
            let ask_price = mid + offset;
            let (bid_amount, ask_amount) = if is_extreme && j % 3 == 0 {
                (0.0, 0.0)
            } else {
                (
                    rng.uniform(0.5, 4.0) * (1.0 + 0.05 * j as f64),
                    rng.uniform(0.5, 4.0) * (1.0 + 0.05 * j as f64),
                )
            };
            bids.push((bid_price, bid_amount));
            asks.push((ask_price, ask_amount));
        }

        let depth = make_depth(bids, asks);
        embed_depth_into_values(&mut values, &depth);

        msgs.push(make_trade_flow_msg(ts, values));
        depths.push(depth);
    }

    ScenarioData {
        name: "edge_extreme".to_string(),
        trade_flow_msgs: msgs,
        depth_snapshots: depths,
    }
}
