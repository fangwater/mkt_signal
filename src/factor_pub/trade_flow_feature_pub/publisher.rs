//! Trade flow feature 发布器

use anyhow::{bail, Result};
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{error, info, warn};
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const TRADE_FLOW_FEATURE_MAX_BYTES: usize = 1024;
const SUBSCRIBER_MAX_BUFFER_SIZE: usize = 8192;
const HISTORY_SIZE: usize = 128;
const PUBLISH_GAP_ERROR_THRESHOLD: Duration = Duration::from_secs(6);
const EXCEED_SUMMARY_LOG_INTERVAL: Duration = Duration::from_secs(60);

pub struct TradeFlowFeaturePublisher {
    venue_slug: String,
    publisher: Publisher<ipc::Service, [u8; TRADE_FLOW_FEATURE_MAX_BYTES], ()>,
    published: u64,
    dropped: u64,
    last_publish_at_by_symbol: HashMap<String, Instant>,
    exceeded_events_in_window: HashMap<String, Vec<u128>>,
    last_exceed_summary_log_at: Instant,
}

impl TradeFlowFeaturePublisher {
    pub fn new(venue_slug: &str) -> Result<Self> {
        let node_name = format!("trade_flow_feature_pub_{}", venue_slug.replace('-', "_"));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service_path = format!("factor_pub/{}/trade_flow_feature", venue_slug);
        let service = node
            .service_builder(&ServiceName::new(&service_path)?)
            .publish_subscribe::<[u8; TRADE_FLOW_FEATURE_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(10)
            .history_size(HISTORY_SIZE)
            .subscriber_max_buffer_size(SUBSCRIBER_MAX_BUFFER_SIZE)
            .open_or_create()?;

        let actual_subscriber_buffer_size = service.static_config().subscriber_max_buffer_size();
        if actual_subscriber_buffer_size < SUBSCRIBER_MAX_BUFFER_SIZE {
            bail!(
                "trade_flow_feature service has stale subscriber_max_buffer_size: service={} expected_min={} actual={} hint=stop producer/consumers, cleanup stale iceoryx service, then restart producer first",
                service_path,
                SUBSCRIBER_MAX_BUFFER_SIZE,
                actual_subscriber_buffer_size
            );
        }

        let publisher = service.publisher_builder().create()?;

        info!(
            "TradeFlowFeaturePublisher created: venue={} service={} history_size={} subscriber_max_buffer_size={}",
            venue_slug,
            service_path,
            service.static_config().history_size(),
            service.static_config().subscriber_max_buffer_size()
        );

        Ok(Self {
            venue_slug: venue_slug.to_string(),
            publisher,
            published: 0,
            dropped: 0,
            last_publish_at_by_symbol: HashMap::new(),
            exceeded_events_in_window: HashMap::new(),
            last_exceed_summary_log_at: Instant::now(),
        })
    }

    pub fn publish(&mut self, data: &[u8], symbol: &str) -> bool {
        if data.len() > TRADE_FLOW_FEATURE_MAX_BYTES {
            warn!(
                "TradeFlowFeature payload size {} exceeds max {} for symbol={}",
                data.len(),
                TRADE_FLOW_FEATURE_MAX_BYTES,
                symbol
            );
            self.dropped += 1;
            return false;
        }

        let mut buffer = [0u8; TRADE_FLOW_FEATURE_MAX_BYTES];
        buffer[..data.len()].copy_from_slice(data);

        match self.publisher.loan_uninit() {
            Ok(sample) => {
                let sample = sample.write_payload(buffer);
                if sample.send().is_ok() {
                    let now = Instant::now();
                    self.maybe_log_exceed_summary(now);

                    if let Some(gap) = self.record_publish_gap(symbol, now) {
                        let exceed_at_ms = current_unix_time_ms();
                        self.exceeded_events_in_window
                            .entry(symbol.to_string())
                            .or_default()
                            .push(exceed_at_ms);
                        error!(
                            "TradeFlowFeature publish gap exceeded: venue={} symbol={} gap_ms={} threshold_ms={} exceed_at_ms={}",
                            self.venue_slug,
                            symbol,
                            gap.as_millis(),
                            PUBLISH_GAP_ERROR_THRESHOLD.as_millis(),
                            exceed_at_ms
                        );
                    }
                    self.published += 1;
                    return true;
                }
            }
            Err(_) => {}
        }

        self.dropped += 1;
        false
    }

    pub fn log_stats(&mut self) {
        info!(
            "TradeFlowFeaturePublisher[{}] stats: published={}, dropped={}",
            self.venue_slug, self.published, self.dropped
        );
        self.published = 0;
        self.dropped = 0;
    }

    fn record_publish_gap(&mut self, symbol: &str, now: Instant) -> Option<Duration> {
        let gap = self
            .last_publish_at_by_symbol
            .insert(symbol.to_string(), now)
            .map(|last| now.saturating_duration_since(last));

        match gap {
            Some(gap) if gap > PUBLISH_GAP_ERROR_THRESHOLD => Some(gap),
            _ => None,
        }
    }

    fn maybe_log_exceed_summary(&mut self, now: Instant) {
        if now.duration_since(self.last_exceed_summary_log_at) < EXCEED_SUMMARY_LOG_INTERVAL {
            return;
        }

        let mut exceeded_symbol_details: Vec<String> = self
            .exceeded_events_in_window
            .iter()
            .map(|(symbol, timestamps)| {
                let joined = timestamps
                    .iter()
                    .map(|ts| ts.to_string())
                    .collect::<Vec<_>>()
                    .join("|");
                format!("{symbol}:{joined}")
            })
            .collect();
        exceeded_symbol_details.sort();

        info!(
            "TradeFlowFeature publish gap summary: venue={} window_secs={} exceeded_symbol_count={} exceeded_symbol_times=[{}] summary_at_ms={}",
            self.venue_slug,
            EXCEED_SUMMARY_LOG_INTERVAL.as_secs(),
            self.exceeded_events_in_window.len(),
            exceeded_symbol_details.join(","),
            current_unix_time_ms()
        );
        self.exceeded_events_in_window.clear();
        self.last_exceed_summary_log_at = now;
    }
}

fn current_unix_time_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_gap_equal_to_threshold_is_not_reported() {
        let prev = Instant::now();
        let now = prev + PUBLISH_GAP_ERROR_THRESHOLD;
        let gap = now.saturating_duration_since(prev);
        assert!(gap <= PUBLISH_GAP_ERROR_THRESHOLD);
    }

    #[test]
    fn publish_gap_above_threshold_is_reported() {
        let prev = Instant::now();
        let now = prev + PUBLISH_GAP_ERROR_THRESHOLD + Duration::from_millis(1);
        let gap = now.saturating_duration_since(prev);
        assert!(gap > PUBLISH_GAP_ERROR_THRESHOLD);
    }

    #[test]
    fn current_unix_time_ms_is_non_zero() {
        assert!(current_unix_time_ms() > 0);
    }
}
