//! Raw execution volatility owned by CTA Spread's Kafka factor publisher.
//! Each market/symbol has its own 5s closes and 720-slot sample-std window.

use anyhow::Result;
use mkt_parsers::msg::mkt_msg::{ModelMsg, MODEL_STATUS_OK};
use order_common::TradingVenue;
use period_pbs::pb::{PeriodMessage, TradeInfo};
use period_pbs::period::normalize_timestamp_ms;
use runtime_common::symbol_util::normalize_symbol_for_venue;
use std::collections::{HashMap, HashSet};

use super::app::timestamp_as_micros;
use crate::factor_pub::model_output_publisher::ModelPublisher;
use crate::factor_pub::trade_flow_feature_pub::vol_state::VolState;

pub const BAR_MS: i64 = 5_000;
pub const RETURN_BARS: usize = 24;
pub const WINDOW_RETURNS: usize = 720;

pub fn output_service_path(venue: TradingVenue) -> String {
    format!("model_output/cta_spread-{}-5s-vol", venue.data_pub_slug())
}

#[derive(Debug, Clone)]
pub(super) struct VolatilityObservation {
    pub symbol: String,
    pub cash: bool,
    /// Close of the completed [ts_ms - 5s, ts_ms) bar, in event time.
    pub ts_ms: i64,
    pub value: Option<f64>,
}

impl VolatilityObservation {
    fn message(&self, seq_no: u64, now_ms: i64) -> ModelMsg {
        ModelMsg::create(
            self.symbol.clone(),
            self.ts_ms,
            now_ms,
            seq_no,
            self.value.unwrap_or(f64::NAN),
            None,
            self.value.is_some(),
            MODEL_STATUS_OK,
            Vec::new(),
            Vec::new(),
        )
    }
}

struct SymbolState {
    vol: VolState,
    last_period_ms: i64,
    latest: Option<VolatilityObservation>,
}

impl Default for SymbolState {
    fn default() -> Self {
        Self {
            vol: VolState::new(RETURN_BARS, WINDOW_RETURNS, 1.0, BAR_MS),
            last_period_ms: 0,
            latest: None,
        }
    }
}

#[derive(Default)]
pub(super) struct VolatilityFactors {
    // Futures and cash have separate event clocks, prices and readiness.
    markets: [HashMap<String, SymbolState>; 2],
}

impl VolatilityFactors {
    pub fn consume_period(
        &mut self,
        period: &PeriodMessage,
        cash: bool,
        allowed_symbols: &HashSet<String>,
    ) -> Vec<VolatilityObservation> {
        let end_ms = normalize_timestamp_ms(period.ts);
        if end_ms <= 0 {
            return Vec::new();
        }
        // Merge repeated symbol entries before sorting, so the last trade in
        // event time determines the close even when the payload is unordered.
        let mut symbols: HashMap<String, Vec<&TradeInfo>> = HashMap::new();
        for info in &period.symbol_infos {
            let symbol = normalize_symbol_for_venue(&info.symbol, TradingVenue::BinanceFutures);
            if allowed_symbols.contains(&symbol) {
                symbols.entry(symbol).or_default().extend(&info.trades);
            }
        }
        let mut observations = Vec::new();
        for (symbol, mut trades) in symbols {
            let state = self.markets[usize::from(cash)]
                .entry(symbol.clone())
                .or_default();
            // PeriodMessage timestamps are inclusive upper bounds. Replayed
            // or reordered periods cannot alter an already observed close.
            if end_ms <= state.last_period_ms {
                continue;
            }
            trades.sort_by_key(|trade| timestamp_as_micros(trade.timestamp));
            let mut sealed = Vec::new();
            for trade in trades {
                let ts_us = timestamp_as_micros(trade.timestamp);
                if ts_us <= state.last_period_ms.saturating_mul(1_000)
                    || ts_us > end_ms.saturating_mul(1_000)
                    || !trade.amount.is_finite()
                    || trade.amount <= 0.0
                {
                    continue;
                }
                state.vol.on_trade(ts_us / 1_000, trade.price, &mut sealed);
            }
            // Advance only on this symbol's Kafka coverage, never wall time or
            // the other market's progress. Empty covered bars carry the close.
            state.vol.on_period_tick(end_ms, &mut sealed);
            state.last_period_ms = end_ms;
            for bar in sealed {
                let observation = VolatilityObservation {
                    symbol: symbol.clone(),
                    cash,
                    ts_ms: bar.bar_start_ms + BAR_MS,
                    value: bar.vol,
                };
                state.latest = Some(observation.clone());
                observations.push(observation);
            }
        }
        observations
    }

    pub fn latest(&self) -> Vec<VolatilityObservation> {
        self.markets
            .iter()
            .flat_map(|market| market.values().filter_map(|state| state.latest.clone()))
            .collect()
    }
}

pub(super) struct VolatilityOutputs {
    publishers: [ModelPublisher; 2],
    seq_no: [u64; 2],
}

impl VolatilityOutputs {
    pub fn new() -> Result<Self> {
        let futures = ModelPublisher::new(
            "cta_spread_binance_futures_vol",
            &output_service_path(TradingVenue::BinanceFutures),
        )?;
        let cash = ModelPublisher::new(
            "cta_spread_binance_cash_vol",
            &output_service_path(TradingVenue::BinanceMargin),
        )?;
        Ok(Self {
            publishers: [futures, cash],
            seq_no: [0; 2],
        })
    }

    pub fn publish(&mut self, observations: &[VolatilityObservation], now_ms: i64) -> (u64, u64) {
        let mut published = 0;
        let mut failed = 0;
        for observation in observations {
            let market = usize::from(observation.cash);
            self.seq_no[market] = self.seq_no[market].saturating_add(1);
            let success = observation
                .message(self.seq_no[market], now_ms)
                .to_bytes()
                .map(|payload| self.publishers[market].publish(&payload))
                .unwrap_or(false);
            if success {
                published += 1;
            } else {
                failed += 1;
            }
        }
        (published, failed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::factor_pub::rl_vol::compute_rl_return_volatility;
    use period_pbs::pb::SymbolInfo;
    use period_pbs::period::INIT_TP_MS;
    use std::collections::VecDeque;

    fn allowed() -> HashSet<String> {
        HashSet::from(["ETHUSDT".to_string(), "SOLUSDT".to_string()])
    }

    fn trade(ts_ms: i64, price: f64) -> TradeInfo {
        TradeInfo {
            timestamp: ts_ms * 1_000,
            side: "buy".into(),
            price,
            amount: 1.0,
        }
    }

    fn period(end_ms: i64, symbol: &str, trades: Vec<TradeInfo>) -> PeriodMessage {
        PeriodMessage {
            ts: end_ms,
            symbol_infos: vec![SymbolInfo {
                symbol: symbol.into(),
                trades,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn price(index: usize) -> f64 {
        100.0 + 7.0 * (index as f64 / 13.0).sin() + index as f64 * 0.01
    }

    #[test]
    fn full_window_matches_unscaled_24_bar_returns_and_evicts_old_returns() {
        let mut factors = VolatilityFactors::default();
        let allowed = allowed();
        let mut closes = VecDeque::new();
        for i in 0..1_600 {
            let start = INIT_TP_MS + i as i64 * BAR_MS;
            let close = price(i);
            closes.push_back(close);
            let observations = factors.consume_period(
                &period(start + BAR_MS, "ETHUSDT", vec![trade(start + 1, close)]),
                false,
                &allowed,
            );
            assert_eq!(observations.len(), 1);
            let observation = &observations[0];
            assert_eq!(observation.ts_ms, start + BAR_MS);
            let expected = compute_rl_return_volatility(&closes, 24, 720).unwrap();
            assert_eq!(observation.value.is_some(), i >= 743);
            match (observation.value, expected) {
                (Some(actual), Some(expected)) => {
                    assert!(expected > 0.0);
                    assert!((actual - expected).abs() < 1e-12, "bar {i}");
                }
                (None, None) => {}
                other => panic!("readiness mismatch: {other:?}"),
            }
        }
    }

    #[test]
    fn cash_futures_and_symbols_have_independent_prices_clocks_and_readiness() {
        let mut factors = VolatilityFactors::default();
        let allowed = allowed();
        for i in 0..744 {
            let start = INIT_TP_MS + i * BAR_MS;
            for (cash, close) in [(false, price(i as usize)), (true, 200.0)] {
                factors.consume_period(
                    &period(start + BAR_MS, "ETHUSDT", vec![trade(start + 1, close)]),
                    cash,
                    &allowed,
                );
            }
        }
        let last = factors.latest();
        assert_eq!(last.len(), 2);
        assert_eq!(last.iter().find(|v| v.cash).unwrap().value, Some(0.0));
        assert!(last.iter().find(|v| !v.cash).unwrap().value.unwrap() > 0.0);
        let start = INIT_TP_MS + 744 * BAR_MS;
        let mut message = period(start + BAR_MS, "SOLUSDT", vec![trade(start + 1, 50.0)]);
        message.symbol_infos.push(SymbolInfo {
            symbol: "BTCUSDT".into(),
            trades: vec![trade(start + 1, 30_000.0)],
            ..Default::default()
        });
        let observed = factors.consume_period(&message, false, &allowed);
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].symbol, "SOLUSDT");
        assert!(observed[0].value.is_none());
        // A different symbol/market cannot refresh ETH's timestamp.
        assert_eq!(factors.latest().len(), 3);
        for eth in factors.latest().iter().filter(|v| v.symbol == "ETHUSDT") {
            assert_eq!(eth.ts_ms, start);
        }
    }

    #[test]
    fn duplicate_and_reordered_periods_cannot_rewrite_closes() {
        let mut factors = VolatilityFactors::default();
        let allowed = allowed();
        let mut closes = VecDeque::new();
        for i in 0..760 {
            let start = INIT_TP_MS + i * BAR_MS;
            let close = price(i as usize);
            closes.push_back(close);
            let message = period(start + BAR_MS, "ETHUSDT", vec![trade(start + 1, close)]);
            let observed = factors.consume_period(&message, false, &allowed);
            assert!(factors.consume_period(&message, false, &allowed).is_empty());
            let old = period(start, "ETHUSDT", vec![trade(start - 1, 9_999.0)]);
            assert!(factors.consume_period(&old, false, &allowed).is_empty());
            let expected = compute_rl_return_volatility(&closes, 24, 720).unwrap();
            if let Some(expected) = expected {
                assert!((observed[0].value.unwrap() - expected).abs() < 1e-12);
            }
        }
    }

    #[test]
    fn microsecond_trades_after_a_period_boundary_are_not_dropped_as_duplicates() {
        let mut factors = VolatilityFactors::default();
        let allowed = allowed();
        let mut closes = VecDeque::new();
        factors.consume_period(&period(INIT_TP_MS, "ETHUSDT", vec![]), false, &allowed);
        for i in 0..744 {
            let start = INIT_TP_MS + i * BAR_MS;
            let close = price(i as usize);
            closes.push_back(close);
            let mut first_microsecond = trade(start, close);
            first_microsecond.timestamp += 1;
            let mut beyond_period = trade(start + BAR_MS, 9_999.0);
            beyond_period.timestamp += 1;
            let observed = factors.consume_period(
                &period(
                    start + BAR_MS,
                    "ETHUSDT",
                    vec![
                        trade(start, 9_999.0), // already covered by the previous period
                        first_microsecond,
                        beyond_period, // must not move the pending close forwards
                    ],
                ),
                false,
                &allowed,
            );
            assert_eq!(observed.len(), 1);
            if i == 743 {
                let expected = compute_rl_return_volatility(&closes, 24, 720)
                    .unwrap()
                    .unwrap();
                assert!((observed[0].value.unwrap() - expected).abs() < 1e-12);
            }
        }
    }

    #[test]
    fn unordered_trades_and_repeated_symbol_entries_use_the_event_time_close() {
        let mut factors = VolatilityFactors::default();
        let allowed = allowed();
        let mut closes = VecDeque::new();
        for i in 0..744 {
            let start = INIT_TP_MS + i * BAR_MS;
            let close = price(i as usize);
            closes.push_back(close);
            let mut message = period(
                start + BAR_MS,
                "ETHUSDT",
                vec![trade(start + 4_999, close), trade(start + 1, 99.0)],
            );
            message.symbol_infos.push(SymbolInfo {
                symbol: "ethusdt".into(),
                trades: vec![trade(start + 2_000, 101.0)],
                ..Default::default()
            });
            let observed = factors.consume_period(&message, false, &allowed);
            assert_eq!(observed.len(), 1);
            if i == 743 {
                let expected = compute_rl_return_volatility(&closes, 24, 720)
                    .unwrap()
                    .unwrap();
                assert!((observed[0].value.unwrap() - expected).abs() < 1e-12);
            }
        }
    }

    #[test]
    fn kafka_event_time_seals_five_second_bars_and_carries_empty_covered_bars() {
        let mut factors = VolatilityFactors::default();
        let allowed = allowed();
        assert!(factors
            .consume_period(
                &period(
                    INIT_TP_MS + 3_000,
                    "ETHUSDT",
                    vec![trade(INIT_TP_MS + 1, 100.0)]
                ),
                false,
                &allowed,
            )
            .is_empty());
        let observed = factors.consume_period(
            &period(
                INIT_TP_MS + 6_000,
                "ETHUSDT",
                vec![trade(INIT_TP_MS + 5_000, 110.0)],
            ),
            false,
            &allowed,
        );
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].ts_ms, INIT_TP_MS + 5_000);
        // Advancing a covered, empty symbol seals two more bars; no wall-clock
        // tick is involved, so replay and live consumption follow one path.
        let observed = factors.consume_period(
            &period(INIT_TP_MS + 15_000, "ETHUSDT", vec![]),
            false,
            &allowed,
        );
        assert_eq!(
            observed.iter().map(|v| v.ts_ms).collect::<Vec<_>>(),
            vec![INIT_TP_MS + 10_000, INIT_TP_MS + 15_000]
        );
        assert!(observed.iter().all(|v| v.value.is_none()));
        let no_price = factors.consume_period(
            &period(INIT_TP_MS + 15_000, "SOLUSDT", vec![]),
            false,
            &allowed,
        );
        assert!(no_price.is_empty());
    }

    #[test]
    fn invalid_and_future_trades_do_not_seed_volatility() {
        let mut factors = VolatilityFactors::default();
        let mut invalid_amount = trade(INIT_TP_MS + 2, 100.0);
        invalid_amount.amount = f64::NAN;
        let message = period(
            INIT_TP_MS + BAR_MS,
            "ETHUSDT",
            vec![
                trade(INIT_TP_MS + 1, f64::NAN),
                trade(INIT_TP_MS + 1, -1.0),
                invalid_amount,
                trade(INIT_TP_MS + BAR_MS + 1, 100.0),
            ],
        );
        assert!(factors
            .consume_period(&message, false, &allowed())
            .is_empty());
        assert!(factors.latest().is_empty());
    }

    #[test]
    fn output_carries_raw_volatility_readiness_and_close_time() {
        let mut observation = VolatilityObservation {
            symbol: "ETHUSDT".into(),
            cash: true,
            ts_ms: INIT_TP_MS + BAR_MS,
            value: None,
        };
        let bytes = observation
            .message(1, INIT_TP_MS + 6_000)
            .to_bytes()
            .unwrap();
        let decoded = ModelMsg::from_bytes(&bytes).unwrap();
        assert!(!decoded.score_ready);
        assert!(decoded.score.is_nan());
        observation.value = Some(0.002);
        let bytes = observation
            .message(2, INIT_TP_MS + 6_000)
            .to_bytes()
            .unwrap();
        let decoded = ModelMsg::from_bytes(&bytes).unwrap();
        assert!(decoded.score_ready);
        assert_eq!(decoded.score, 0.002);
        assert_eq!(decoded.ts_in_ms, observation.ts_ms);
        assert_eq!(decoded.ts_out_ms, INIT_TP_MS + 6_000);
        assert_eq!(decoded.seq_no, 2);
        assert!(decoded.score_quantile.is_none());
        assert_eq!(
            output_service_path(TradingVenue::BinanceMargin),
            "model_output/cta_spread-binance-margin-5s-vol"
        );
        assert_eq!(
            output_service_path(TradingVenue::BinanceFutures),
            "model_output/cta_spread-binance-futures-5s-vol"
        );
    }
}
