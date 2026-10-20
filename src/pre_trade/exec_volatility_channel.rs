//! Raw CTA Spread volatility, keyed by execution market and symbol.
use anyhow::Result;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use mkt_parsers::msg::mkt_msg::{ModelMsg, MODEL_STATUS_OK};
use mkt_parsers::msg::model_ipc::MODEL_PAYLOAD_MAX_BYTES;
use order_common::TradingVenue;
use runtime_common::symbol_util::normalize_symbol_for_internal;
use runtime_common::time_util::get_timestamp_us;
use std::cell::RefCell;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::factor_pub::cta_spread_factor_model_1m_pub::volatility::output_service_path;

#[derive(Default)]
struct VolatilityCache {
    values: HashMap<(TradingVenue, String), (i64, Option<f64>)>,
}

impl VolatilityCache {
    fn observe(&mut self, venue: TradingVenue, msg: ModelMsg, now_ms: i64) {
        if msg.ts_in_ms <= 0 || msg.ts_in_ms > now_ms {
            return;
        }
        let key = (venue, normalize_symbol_for_internal(&msg.symbol));
        if self
            .values
            .get(&key)
            .is_some_and(|(ts, _)| *ts >= msg.ts_in_ms)
        {
            return;
        }
        let value = (msg.score_ready
            && msg.status == MODEL_STATUS_OK
            && msg.score.is_finite()
            && msg.score >= 0.0)
            .then_some(msg.score);
        self.values.insert(key, (msg.ts_in_ms, value));
    }

    fn latest(
        &self,
        venue: TradingVenue,
        symbol: &str,
        now_us: i64,
        max_age_ms: u32,
    ) -> Option<f64> {
        let (ts, value) = self
            .values
            .get(&(venue, normalize_symbol_for_internal(symbol)))?;
        let now_ms = now_us / 1_000;
        (*ts <= now_ms && now_ms - *ts <= i64::from(max_age_ms))
            .then_some(*value)
            .flatten()
    }
}

thread_local! {
    static CACHE: RefCell<VolatilityCache> = RefCell::new(VolatilityCache::default());
}

pub fn latest(venue: TradingVenue, symbol: &str, now_us: i64, max_age_ms: u32) -> Option<f64> {
    CACHE.with(|cache| cache.borrow().latest(venue, symbol, now_us, max_age_ms))
}

fn subscribe(
    venue: TradingVenue,
) -> Result<Subscriber<ipc::Service, [u8; MODEL_PAYLOAD_MAX_BYTES], ()>> {
    let node = NodeBuilder::new()
        .name(&NodeName::new(&format!(
            "exec_grid_{}",
            venue.data_pub_slug().replace('-', "_")
        ))?)
        .create::<ipc::Service>()?;
    let service = node
        .service_builder(&ServiceName::new(&output_service_path(venue))?)
        .publish_subscribe::<[u8; MODEL_PAYLOAD_MAX_BYTES]>()
        .open()?;
    Ok(service.subscriber_builder().buffer_size(256).create()?)
}

pub fn start(venues: &[TradingVenue]) {
    for &venue in venues {
        tokio::task::spawn_local(async move {
            let mut last_warning: Option<Instant> = None;
            loop {
                let subscriber = match subscribe(venue) {
                    Ok(value) => value,
                    Err(err) => {
                        if last_warning.is_none_or(|ts| ts.elapsed() >= Duration::from_secs(60)) {
                            log::debug!(
                                "Exec grid volatility unavailable venue={venue:?}: {err:#}"
                            );
                            last_warning = Some(Instant::now());
                        }
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };
                log::info!("Exec grid subscribed to {}", output_service_path(venue));
                loop {
                    let mut failed = false;
                    for _ in 0..256 {
                        match subscriber.receive() {
                            Ok(Some(sample)) => {
                                if let Ok(msg) = ModelMsg::from_bytes(sample.payload()) {
                                    CACHE.with(|cache| {
                                        cache.borrow_mut().observe(
                                            venue,
                                            msg,
                                            get_timestamp_us() / 1_000,
                                        )
                                    });
                                }
                            }
                            Ok(None) => break,
                            Err(_) => {
                                failed = true;
                                break;
                            }
                        }
                    }
                    if failed {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(ts: i64, value: f64, ready: bool) -> ModelMsg {
        ModelMsg::create(
            "BTCUSDT".into(),
            ts,
            ts,
            1,
            value,
            None,
            ready,
            MODEL_STATUS_OK,
            Vec::new(),
            Vec::new(),
        )
    }

    #[test]
    fn volatility_is_scoped_to_market_and_expires_at_its_event_time() {
        let mut cache = VolatilityCache::default();
        cache.observe(
            TradingVenue::BinanceFutures,
            message(1000, 0.01, true),
            1100,
        );
        cache.observe(TradingVenue::BinanceMargin, message(1000, 0.02, true), 1100);
        assert_eq!(
            cache.latest(TradingVenue::BinanceFutures, "BTCUSDT", 2_000_000, 1000),
            Some(0.01)
        );
        assert_eq!(
            cache.latest(TradingVenue::BinanceMargin, "BTCUSDT", 2_000_000, 1000),
            Some(0.02)
        );
        assert_eq!(
            cache.latest(TradingVenue::OkexFutures, "BTCUSDT", 2_000_000, 1000),
            None
        );
        assert_eq!(
            cache.latest(TradingVenue::BinanceFutures, "ETHUSDT", 2_000_000, 1000),
            None
        );
        assert_eq!(
            cache.latest(TradingVenue::BinanceFutures, "BTCUSDT", 2_001_000, 1000),
            None
        );
        assert_eq!(
            cache.latest(TradingVenue::BinanceFutures, "BTCUSDT", 999_000, 1000),
            None
        );
    }

    #[test]
    fn unready_or_invalid_values_invalidate_old_ready_data_without_replay_resurrection() {
        let venue = TradingVenue::BinanceFutures;
        for (value, ready, status) in [
            (f64::NAN, true, MODEL_STATUS_OK),
            (-0.01, true, MODEL_STATUS_OK),
            (0.01, false, MODEL_STATUS_OK),
            (0.01, true, MODEL_STATUS_OK + 1),
        ] {
            let mut cache = VolatilityCache::default();
            cache.observe(venue, message(1000, 0.01, true), 1100);
            let mut invalid = message(1100, value, ready);
            invalid.status = status;
            cache.observe(venue, invalid, 1100);
            cache.observe(venue, message(1000, 0.01, true), 1100);
            cache.observe(venue, message(1200, 0.01, true), 1100); // future rejected
            assert_eq!(cache.latest(venue, "BTCUSDT", 1_100_000, 1000), None);
            cache.observe(venue, message(1200, 0.0, true), 1200); // zero vol is valid
            assert_eq!(cache.latest(venue, "BTCUSDT", 1_200_000, 1000), Some(0.0));
        }
    }
}
