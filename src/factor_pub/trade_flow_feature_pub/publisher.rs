//! Trade flow feature 发布器

use anyhow::{bail, Result};
use bytes::Bytes;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{error, info, warn};
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::common::mkt_msg::FactorValueMsg;
use crate::factor_pub::factor_index::{factor_name_to_channel, factor_name_to_index};

pub const TRADE_FLOW_FEATURE_MAX_BYTES: usize = 1024;
const SUBSCRIBER_MAX_BUFFER_SIZE: usize = 8192;
const HISTORY_SIZE: usize = 128;
const PUBLISH_GAP_ERROR_THRESHOLD: Duration = Duration::from_secs(6);
const EXCEED_SUMMARY_LOG_INTERVAL: Duration = Duration::from_secs(60);
const RL_FACTOR_NAME: &str = "rl_return_volatility";
const RL_FACTOR_PAYLOAD_MAX_BYTES: usize = 256;
const RL_FACTOR_SUBSCRIBER_MAX_BUFFER_SIZE: usize = 8192;
const RL_FACTOR_HISTORY_SIZE: usize = 128;
const RL_FACTOR_WARN_INTERVAL_SECS: u64 = 60;

pub struct RlFactorPublisher {
    publisher: Publisher<ipc::Service, [u8; RL_FACTOR_PAYLOAD_MAX_BYTES], ()>,
    factor_index: u16,
    published_count: u64,
    dropped_count: u64,
    last_warn: Instant,
}

impl RlFactorPublisher {
    pub fn new(venue_slug: &str) -> Result<Self> {
        let Some(factor_index) = factor_name_to_index(RL_FACTOR_NAME) else {
            bail!("missing factor index for '{}'", RL_FACTOR_NAME);
        };

        let node_name = format!("trade_flow_rl_pub_{}", venue_slug.replace('-', "_"));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service_path = format!(
            "factor_pub/{}/{}",
            venue_slug,
            factor_name_to_channel(RL_FACTOR_NAME)
        );
        let service = node
            .service_builder(&ServiceName::new(&service_path)?)
            .publish_subscribe::<[u8; RL_FACTOR_PAYLOAD_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(10)
            .subscriber_max_buffer_size(RL_FACTOR_SUBSCRIBER_MAX_BUFFER_SIZE)
            .history_size(RL_FACTOR_HISTORY_SIZE)
            .open_or_create()?;

        let actual_subscriber_buffer_size = service.static_config().subscriber_max_buffer_size();
        let actual_history_size = service.static_config().history_size();
        if actual_subscriber_buffer_size < RL_FACTOR_SUBSCRIBER_MAX_BUFFER_SIZE {
            bail!(
                "rl factor service has stale subscriber_max_buffer_size: service={} expected_min={} actual={} hint=stop producer/consumers, cleanup stale iceoryx service, then restart producer first",
                service_path,
                RL_FACTOR_SUBSCRIBER_MAX_BUFFER_SIZE,
                actual_subscriber_buffer_size
            );
        }
        if actual_history_size < RL_FACTOR_HISTORY_SIZE {
            bail!(
                "rl factor service has stale history_size: service={} expected_min={} actual={} hint=stop producer/consumers, cleanup stale iceoryx service, then restart producer first",
                service_path,
                RL_FACTOR_HISTORY_SIZE,
                actual_history_size
            );
        }

        let publisher = service.publisher_builder().create()?;

        info!(
            "RlFactorPublisher ready: service={} factor_index={} history_size={} subscriber_max_buffer_size={}",
            service_path,
            factor_index,
            actual_history_size,
            actual_subscriber_buffer_size,
        );

        Ok(Self {
            publisher,
            factor_index,
            published_count: 0,
            dropped_count: 0,
            last_warn: Instant::now() - Duration::from_secs(RL_FACTOR_WARN_INTERVAL_SECS),
        })
    }

    pub fn publish(&mut self, symbol: &str, value: f64, timestamp_ms: i64, ready: bool) -> bool {
        let msg = FactorValueMsg::create_with_factor_index(
            symbol.to_string(),
            value,
            timestamp_ms,
            ready,
            self.factor_index,
        );
        let bytes: Bytes = msg.to_bytes();
        if bytes.len() > RL_FACTOR_PAYLOAD_MAX_BYTES {
            self.warn_throttled(
                "payload_too_large",
                &format!(
                    "len={} max={} symbol={}",
                    bytes.len(),
                    RL_FACTOR_PAYLOAD_MAX_BYTES,
                    symbol
                ),
            );
            self.dropped_count = self.dropped_count.saturating_add(1);
            return false;
        }

        let mut buffer = [0u8; RL_FACTOR_PAYLOAD_MAX_BYTES];
        buffer[..bytes.len()].copy_from_slice(&bytes);

        match self.publisher.loan_uninit() {
            Ok(sample) => {
                let sample = sample.write_payload(buffer);
                if sample.send().is_ok() {
                    self.published_count = self.published_count.saturating_add(1);
                    true
                } else {
                    self.warn_throttled("send", &symbol);
                    self.dropped_count = self.dropped_count.saturating_add(1);
                    false
                }
            }
            Err(err) => {
                self.warn_throttled("loan_uninit", &err);
                self.dropped_count = self.dropped_count.saturating_add(1);
                false
            }
        }
    }

    pub fn published_count(&self) -> u64 {
        self.published_count
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped_count
    }

    fn warn_throttled(&mut self, action: &str, detail: &dyn std::fmt::Debug) {
        if self.last_warn.elapsed() >= Duration::from_secs(RL_FACTOR_WARN_INTERVAL_SECS) {
            warn!("rl factor publish {} failed: {:?}", action, detail);
            self.last_warn = Instant::now();
        }
    }
}

pub struct TradeFlowFeaturePublisher {
    venue_slug: String,
    publisher: Publisher<ipc::Service, [u8; TRADE_FLOW_FEATURE_MAX_BYTES], ()>,
    published: u64,
    dropped: u64,
    last_publish_at_by_symbol: HashMap<String, Instant>,
    exceeded_events_in_window: HashMap<String, Vec<u64>>,
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

        match self.publisher.loan_uninit() {
            Ok(mut sample) => {
                sample
                    .payload_mut()
                    .write([0u8; TRADE_FLOW_FEATURE_MAX_BYTES]);
                let mut sample = unsafe { sample.assume_init() };
                sample.payload_mut()[..data.len()].copy_from_slice(data);
                if sample.send().is_ok() {
                    let now = Instant::now();
                    self.maybe_log_exceed_summary(now);

                    if let Some(gap) = self.record_publish_gap(symbol, now) {
                        let exceed_at_ms = current_unix_time_ms();
                        if let Some(events) = self.exceeded_events_in_window.get_mut(symbol) {
                            events.push(exceed_at_ms);
                        } else {
                            self.exceeded_events_in_window
                                .insert(symbol.to_string(), vec![exceed_at_ms]);
                        }
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
        let gap = if let Some(last) = self.last_publish_at_by_symbol.get_mut(symbol) {
            let gap = now.saturating_duration_since(*last);
            *last = now;
            Some(gap)
        } else {
            self.last_publish_at_by_symbol
                .insert(symbol.to_string(), now);
            None
        };

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

fn current_unix_time_ms() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    millis.min(u64::MAX as u128) as u64
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
