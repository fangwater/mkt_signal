//! 独立的 model_output 订阅 / 缓存 hub（从 FactorValueHub 拆出）。

use anyhow::{Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::collections::HashMap;
use std::time::Instant;

use crate::common::mkt_msg::{ModelMsg, MODEL_STATUS_OK};
use crate::common::model_ipc::MODEL_PAYLOAD_MAX_BYTES;
use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::signal::common::TradingVenue;

const MODEL_OUTPUT_HISTORY_SIZE: usize = 128;
const MODEL_OUTPUT_SUBSCRIBER_BUFFER_SIZE: usize = 256;
const MODEL_OUTPUT_POLL_MAX_PER_CHANNEL: usize = 256;
const MODEL_OUTPUT_STATS_LOG_INTERVAL_SECS: u64 = 60;

#[derive(Debug, Clone)]
pub struct ModelOutputScoreLookupResult {
    pub service_name: String,
    pub symbol_key: String,
    pub subscribed: bool,
    pub score: Option<f64>,
    pub score_quantile: Option<f64>,
    pub note: String,
}

#[derive(Debug, Clone)]
pub struct ModelOutputUpdateEvent {
    pub service_name: String,
    pub symbol_key: String,
    pub score: f64,
    pub score_quantile: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
struct ModelOutputSnapshot {
    score: f64,
    score_quantile: Option<f64>,
}

struct ModelOutputSubscriberEntry {
    service_name: String,
    subscriber: Subscriber<ipc::Service, [u8; MODEL_PAYLOAD_MAX_BYTES], ()>,
}

pub struct ModelOutputHub {
    hedge_venue: TradingVenue,
    subscribers: Vec<ModelOutputSubscriberEntry>,
    services: Vec<String>,
    latest_scores: HashMap<(String, String), ModelOutputSnapshot>,
    msg_count: u64,
    parse_err_count: u64,
    last_log: Instant,
}

impl ModelOutputHub {
    pub fn new(hedge_venue: TradingVenue) -> Self {
        Self {
            hedge_venue,
            subscribers: Vec::new(),
            services: Vec::new(),
            latest_scores: HashMap::new(),
            msg_count: 0,
            parse_err_count: 0,
            last_log: Instant::now(),
        }
    }

    pub fn update_services(
        &mut self,
        node: &Node<ipc::Service>,
        services: Vec<String>,
    ) -> usize {
        let normalized = Self::normalize_services(services);
        if normalized == self.services {
            return self.subscribers.len();
        }

        if normalized.is_empty() {
            self.services.clear();
            self.subscribers.clear();
            self.latest_scores.clear();
            info!("ModelOutputHub: subscriptions cleared");
            return 0;
        }

        let mut subscribers: Vec<ModelOutputSubscriberEntry> = Vec::new();
        for service_name in &normalized {
            match Self::create_subscriber(node, service_name) {
                Ok(subscriber) => {
                    subscribers.push(ModelOutputSubscriberEntry {
                        service_name: service_name.clone(),
                        subscriber,
                    });
                }
                Err(err) => {
                    warn!(
                        "ModelOutputHub: subscribe failed service={} err={:#}",
                        service_name, err
                    );
                }
            }
        }
        if subscribers.is_empty() {
            warn!(
                "ModelOutputHub: no subscriber created, keep previous subscriptions count={}",
                self.subscribers.len()
            );
            return self.subscribers.len();
        }

        self.services = normalized;
        self.subscribers = subscribers;
        self.latest_scores.clear();
        self.msg_count = 0;
        self.parse_err_count = 0;
        self.last_log = Instant::now();
        info!(
            "ModelOutputHub: subscriptions updated count={} services={:?} buffer_size={}",
            self.subscribers.len(),
            self.services,
            MODEL_OUTPUT_SUBSCRIBER_BUFFER_SIZE
        );
        self.subscribers.len()
    }

    pub fn poll_updates(&mut self) -> Vec<ModelOutputUpdateEvent> {
        let mut events = Vec::new();
        if self.subscribers.is_empty() {
            return events;
        }

        for entry in &mut self.subscribers {
            let mut polled = 0usize;
            while polled < MODEL_OUTPUT_POLL_MAX_PER_CHANNEL {
                match entry.subscriber.receive() {
                    Ok(Some(sample)) => {
                        polled += 1;
                        let payload = sample.payload();
                        if payload.iter().all(|&b| b == 0) {
                            continue;
                        }

                        let msg = match ModelMsg::from_bytes(payload) {
                            Ok(msg) => msg,
                            Err(err) => {
                                self.parse_err_count = self.parse_err_count.saturating_add(1);
                                warn!(
                                    "ModelOutputHub: parse payload failed service={} err={}",
                                    entry.service_name, err
                                );
                                continue;
                            }
                        };

                        if msg.status != MODEL_STATUS_OK {
                            continue;
                        }

                        let symbol_key = normalize_symbol_for_venue(&msg.symbol, self.hedge_venue);
                        let cache_key = (entry.service_name.clone(), symbol_key);
                        let event = ModelOutputUpdateEvent {
                            service_name: entry.service_name.clone(),
                            symbol_key: cache_key.1.clone(),
                            score: msg.score,
                            score_quantile: msg.score_quantile,
                        };
                        self.latest_scores.insert(
                            cache_key,
                            ModelOutputSnapshot {
                                score: msg.score,
                                score_quantile: msg.score_quantile,
                            },
                        );
                        self.msg_count = self.msg_count.saturating_add(1);
                        events.push(event);
                    }
                    Ok(None) => break,
                    Err(err) => {
                        warn!(
                            "ModelOutputHub: subscriber receive error service={} err={}",
                            entry.service_name, err
                        );
                        break;
                    }
                }
            }
        }

        if self.last_log.elapsed().as_secs() >= MODEL_OUTPUT_STATS_LOG_INTERVAL_SECS {
            info!(
                "ModelOutputHub: stats services={} latest_scores={} recv={} parse_err={}",
                self.services.len(),
                self.latest_scores.len(),
                self.msg_count,
                self.parse_err_count
            );
            self.last_log = Instant::now();
            self.msg_count = 0;
            self.parse_err_count = 0;
        }

        events
    }

    pub fn lookup_score(
        &mut self,
        model_service: &str,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
    ) -> ModelOutputScoreLookupResult {
        let _ = self.poll_updates();
        self.cached_score(model_service, hedge_symbol, hedge_venue)
    }

    pub fn cached_score(
        &self,
        model_service: &str,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
    ) -> ModelOutputScoreLookupResult {
        let Some(service_name) = Self::normalize_service_name(model_service) else {
            return ModelOutputScoreLookupResult {
                service_name: model_service.trim().to_string(),
                symbol_key: normalize_symbol_for_venue(hedge_symbol, hedge_venue),
                subscribed: false,
                score: None,
                score_quantile: None,
                note: "service_disabled".to_string(),
            };
        };
        let symbol_key = normalize_symbol_for_venue(hedge_symbol, hedge_venue);

        let subscribed = self.services.iter().any(|s| s == &service_name);
        if !subscribed {
            return ModelOutputScoreLookupResult {
                service_name,
                symbol_key,
                subscribed: false,
                score: None,
                score_quantile: None,
                note: "service_not_subscribed".to_string(),
            };
        }

        let cache_key = (service_name.clone(), symbol_key.clone());
        match self.latest_scores.get(&cache_key).copied() {
            Some(snapshot) if snapshot.score.is_finite() => ModelOutputScoreLookupResult {
                service_name,
                symbol_key,
                subscribed: true,
                score: Some(snapshot.score),
                score_quantile: snapshot.score_quantile,
                note: "ok".to_string(),
            },
            Some(_) => ModelOutputScoreLookupResult {
                service_name,
                symbol_key,
                subscribed: true,
                score: None,
                score_quantile: None,
                note: "invalid_model_score".to_string(),
            },
            None => ModelOutputScoreLookupResult {
                service_name,
                symbol_key,
                subscribed: true,
                score: None,
                score_quantile: None,
                note: "missing_model_score".to_string(),
            },
        }
    }

    pub fn subscribed_services(&self) -> &[String] {
        &self.services
    }

    fn create_subscriber(
        node: &Node<ipc::Service>,
        service_name: &str,
    ) -> Result<Subscriber<ipc::Service, [u8; MODEL_PAYLOAD_MAX_BYTES], ()>> {
        let service = node
            .service_builder(&ServiceName::new(service_name)?)
            .publish_subscribe::<[u8; MODEL_PAYLOAD_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(10)
            .history_size(MODEL_OUTPUT_HISTORY_SIZE)
            .subscriber_max_buffer_size(MODEL_OUTPUT_SUBSCRIBER_BUFFER_SIZE)
            .open_or_create()
            .with_context(|| {
                format!("failed to open/create model_output subscriber service={service_name}")
            })?;

        service
            .subscriber_builder()
            .buffer_size(MODEL_OUTPUT_SUBSCRIBER_BUFFER_SIZE)
            .create()
            .with_context(|| format!("failed to create model_output subscriber: {service_name}"))
    }

    pub fn normalize_service_name(service_name: &str) -> Option<String> {
        let trimmed = service_name.trim();
        if trimmed.is_empty() || trimmed == "-" {
            return None;
        }
        if trimmed.contains('/') {
            Some(trimmed.to_string())
        } else {
            Some(format!("model_output/{trimmed}"))
        }
    }

    fn normalize_services(services: Vec<String>) -> Vec<String> {
        let mut normalized = Vec::new();
        for raw in services {
            let Some(service) = Self::normalize_service_name(&raw) else {
                continue;
            };
            if !normalized.iter().any(|s| s == &service) {
                normalized.push(service);
            }
        }
        normalized
    }
}

#[cfg(test)]
mod tests {
    use super::ModelOutputHub;

    #[test]
    fn normalizes_bare_service_name() {
        assert_eq!(
            ModelOutputHub::normalize_service_name("binance_futures_direction_model"),
            Some("model_output/binance_futures_direction_model".to_string())
        );
        assert_eq!(
            ModelOutputHub::normalize_service_name("model_output/binance_futures_direction_model"),
            Some("model_output/binance_futures_direction_model".to_string())
        );
    }

    #[test]
    fn ignores_disabled_service_name() {
        assert_eq!(ModelOutputHub::normalize_service_name(""), None);
        assert_eq!(ModelOutputHub::normalize_service_name("-"), None);
    }
}
