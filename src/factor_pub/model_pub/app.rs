use anyhow::Result;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::time::{Duration, Instant};

use super::cfg::ModelPubConfig;
use super::model::XgbModel;
use super::publisher::{ModelPublisher, MODEL_PAYLOAD_MAX_BYTES};
use crate::common::mkt_msg::{
    FeatureMsg, ModelMsg, MODEL_STATUS_BAD_DIM, MODEL_STATUS_DECODE_ERR, MODEL_STATUS_INFER_ERR,
    MODEL_STATUS_OK,
};

const INPUT_MAX_BYTES: usize = MODEL_PAYLOAD_MAX_BYTES;
const IDLE_SLEEP_MICROS: u64 = 200;
const LOG_INTERVAL_SECS: u64 = 60;

#[derive(Default)]
struct ModelPubStats {
    recv_total: u64,
    publish_ok: u64,
    publish_fail: u64,
    decode_err: u64,
    bad_dim: u64,
    infer_err: u64,
}

pub struct ModelPubApp {
    config: ModelPubConfig,
    model: XgbModel,
    subscriber: Subscriber<ipc::Service, [u8; INPUT_MAX_BYTES], ()>,
    publisher: ModelPublisher,
    stats: ModelPubStats,
    last_log_stats: Instant,
}

impl ModelPubApp {
    pub fn new(config_path: &str) -> Result<Self> {
        let config = ModelPubConfig::load(config_path)?;
        config.validate()?;

        let model = XgbModel::load(&config.model_path, config.n_features)?;
        let subscriber = Self::create_subscriber(&config.model_name, &config.input_service)?;

        let publisher_node = format!("model_pub_{}_out", sanitize_node_suffix(&config.model_name));
        let publisher = ModelPublisher::new(&publisher_node, &config.output_service)?;

        info!(
            "ModelPubApp started: model_name={} input_service={} output_service={} n_features={}",
            config.model_name, config.input_service, config.output_service, config.n_features
        );

        Ok(Self {
            config,
            model,
            subscriber,
            publisher,
            stats: ModelPubStats::default(),
            last_log_stats: Instant::now(),
        })
    }

    fn create_subscriber(
        model_name: &str,
        service_path: &str,
    ) -> Result<Subscriber<ipc::Service, [u8; INPUT_MAX_BYTES], ()>> {
        let node_name = format!("model_pub_{}_in", sanitize_node_suffix(model_name));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(service_path)?)
            .publish_subscribe::<[u8; INPUT_MAX_BYTES]>()
            .open()?;

        let subscriber = service.subscriber_builder().create()?;
        info!("Subscribed to feature channel: {}", service_path);
        Ok(subscriber)
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            let mut has_message = false;
            while let Some(sample) = self.subscriber.receive()? {
                has_message = true;
                self.stats.recv_total += 1;

                let data = sample.payload().to_vec();
                self.process_input(&data);
            }

            if !has_message {
                std::thread::sleep(Duration::from_micros(IDLE_SLEEP_MICROS));
            }

            if self.last_log_stats.elapsed() >= Duration::from_secs(LOG_INTERVAL_SECS) {
                self.log_stats();
                self.last_log_stats = Instant::now();
            }
        }
    }

    fn process_input(&mut self, data: &[u8]) {
        match FeatureMsg::from_bytes(data) {
            Ok(feature) => {
                if feature.feature_dim as usize != self.config.n_features
                    || feature.features.len() != self.config.n_features
                {
                    self.stats.bad_dim += 1;
                    self.emit_result(
                        &feature.symbol,
                        feature.ts_ms,
                        feature.seq_no,
                        0.0,
                        MODEL_STATUS_BAD_DIM,
                    );
                    return;
                }

                match self.model.predict_one(&feature.features) {
                    Ok(score) => {
                        self.emit_result(
                            &feature.symbol,
                            feature.ts_ms,
                            feature.seq_no,
                            score,
                            MODEL_STATUS_OK,
                        );
                    }
                    Err(err) => {
                        self.stats.infer_err += 1;
                        warn!(
                            "Model inference failed: model={} symbol={} seq={} err={}",
                            self.config.model_name, feature.symbol, feature.seq_no, err
                        );
                        self.emit_result(
                            &feature.symbol,
                            feature.ts_ms,
                            feature.seq_no,
                            0.0,
                            MODEL_STATUS_INFER_ERR,
                        );
                    }
                }
            }
            Err(err) => {
                self.stats.decode_err += 1;
                warn!(
                    "Feature message decode failed: model={} input_service={} err={}",
                    self.config.model_name, self.config.input_service, err
                );
                self.emit_result("", 0, 0, 0.0, MODEL_STATUS_DECODE_ERR);
            }
        }
    }

    fn emit_result(&mut self, symbol: &str, ts_in_ms: i64, seq_no: u64, score: f64, status: u8) {
        let msg = ModelMsg::create(
            symbol.to_string(),
            ts_in_ms,
            now_millis(),
            seq_no,
            score,
            status,
        );
        let bytes = match msg.to_bytes() {
            Ok(bytes) => bytes,
            Err(err) => {
                self.stats.publish_fail += 1;
                warn!(
                    "Model result encode failed: model={} symbol={} seq={} err={}",
                    self.config.model_name, symbol, seq_no, err
                );
                return;
            }
        };

        if self.publisher.publish(bytes.as_ref()) {
            self.stats.publish_ok += 1;
        } else {
            self.stats.publish_fail += 1;
        }
    }

    fn log_stats(&mut self) {
        info!(
            "ModelPubApp[{}] stats: recv_total={} publish_ok={} publish_fail={} decode_err={} bad_dim={} infer_err={}",
            self.config.model_name,
            self.stats.recv_total,
            self.stats.publish_ok,
            self.stats.publish_fail,
            self.stats.decode_err,
            self.stats.bad_dim,
            self.stats.infer_err,
        );
        self.stats = ModelPubStats::default();
    }
}

fn sanitize_node_suffix(raw: &str) -> String {
    let normalized = raw.trim();
    if normalized.is_empty() {
        return "default".to_string();
    }

    normalized
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn now_millis() -> i64 {
    let Ok(duration) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) else {
        return 0;
    };

    let millis = duration.as_millis();
    if millis > i64::MAX as u128 {
        i64::MAX
    } else {
        millis as i64
    }
}
