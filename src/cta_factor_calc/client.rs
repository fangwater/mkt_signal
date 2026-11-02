//! Read-only factor input for independent signal/combination consumers.
use super::{
    config::{FactorId, Worker},
    engine::{FactorEvent, FactorPoint},
    runtime::service_path,
};
use anyhow::{ensure, Context, Result};
use iceoryx2::{port::subscriber::Subscriber, prelude::*, service::ipc};
use mkt_parsers::msg::model_ipc::MODEL_PAYLOAD_MAX_BYTES;
use std::collections::BTreeMap;

#[derive(Default)]
pub struct FactorCache {
    generation: Option<String>,
    latest: BTreeMap<String, FactorPoint>,
}
impl FactorCache {
    pub fn apply(&mut self, event: FactorEvent) -> Result<Option<FactorPoint>> {
        let generation = match &event {
            FactorEvent::Point { generation, point } => {
                let id = FactorId::parse(&point.factor_id)?;
                ensure!(
                    point.ts_ms > 0 && point.ts_ms % id.interval.millis() == 0,
                    "invalid factor timestamp"
                );
                let expected = if id.factor == "nq" {
                    vec!["long", "short"]
                } else {
                    vec!["value"]
                };
                ensure!(
                    point.fields.keys().map(String::as_str).collect::<Vec<_>>() == expected,
                    "invalid factor output fields"
                );
                for field in point.fields.values() {
                    ensure!(
                        field.raw.is_none_or(f64::is_finite)
                            && field.value.is_none_or(f64::is_finite)
                            && field
                                .quantile
                                .is_none_or(|q| q.is_finite() && (0.0..=1.0).contains(&q)),
                        "invalid factor value"
                    );
                    ensure!(
                        !field.ready
                            || (field.raw.is_some()
                                && field.value.is_some()
                                && field.quantile.is_some()),
                        "ready factor lacks a value"
                    );
                }
                generation
            }
            FactorEvent::Removed {
                generation,
                factor_id,
            } => {
                FactorId::parse(factor_id)?;
                generation
            }
        };
        ensure!(
            !generation.is_empty() && generation.len() <= 64,
            "invalid factor generation"
        );
        if self.generation.as_ref() != Some(generation) {
            self.latest.clear();
            self.generation = Some(generation.clone());
        }
        match event {
            FactorEvent::Removed { factor_id, .. } => {
                self.latest.remove(&factor_id);
                Ok(None)
            }
            FactorEvent::Point { point, .. } => {
                if self
                    .latest
                    .get(&point.factor_id)
                    .is_some_and(|old| point.ts_ms <= old.ts_ms)
                {
                    return Ok(None);
                }
                self.latest.insert(point.factor_id.clone(), point.clone());
                Ok(Some(point))
            }
        }
    }
    pub fn latest(&self) -> &BTreeMap<String, FactorPoint> {
        &self.latest
    }
    pub fn fresh(&self, id: &str, now_ms: i64, max_age_ms: i64) -> Option<&FactorPoint> {
        self.latest
            .get(id)
            .filter(|p| p.ready() && p.ts_ms <= now_ms && now_ms - p.ts_ms <= max_age_ms)
    }
}

pub struct FactorFeed {
    _node: Node<ipc::Service>,
    subscriber: Subscriber<ipc::Service, [u8; MODEL_PAYLOAD_MAX_BYTES], ()>,
    cache: FactorCache,
}
impl FactorFeed {
    pub fn new(worker: Worker) -> Result<Self> {
        let node = NodeBuilder::new().create::<ipc::Service>()?;
        let service = node
            .service_builder(&ServiceName::new(&service_path(worker))?)
            .publish_subscribe::<[u8; MODEL_PAYLOAD_MAX_BYTES]>()
            .open()?;
        let subscriber = service.subscriber_builder().buffer_size(8192).create()?;
        Ok(Self {
            _node: node,
            subscriber,
            cache: FactorCache::default(),
        })
    }
    pub fn poll(&mut self) -> Result<Vec<FactorPoint>> {
        let mut points = Vec::new();
        while let Some(sample) = self.subscriber.receive()? {
            let bytes = sample.payload();
            let length = bytes.iter().position(|b| *b == 0).unwrap_or(bytes.len());
            let event: FactorEvent =
                serde_json::from_slice(&bytes[..length]).context("decode factor point")?;
            if let Some(point) = self.cache.apply(event)? {
                points.push(point);
            }
        }
        // Retained IPC history may cross a replay handover or a deletion. Only
        // the current generation's final observation for an ID is actionable.
        points.retain(|point| self.cache.latest().get(&point.factor_id) == Some(point));
        let mut points = points
            .into_iter()
            .map(|p| (p.factor_id.clone(), p))
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect::<Vec<_>>();
        points.sort_by(|a, b| (a.ts_ms, &a.factor_id).cmp(&(b.ts_ms, &b.factor_id)));
        Ok(points)
    }
    pub fn latest(&self) -> &BTreeMap<String, FactorPoint> {
        self.cache.latest()
    }
    pub fn fresh(&self, id: &str, now_ms: i64, max_age_ms: i64) -> Option<&FactorPoint> {
        self.cache.fresh(id, now_ms, max_age_ms)
    }
}
