use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
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
const MODEL_FETCH_CONCURRENCY: usize = 8;

#[derive(Default)]
struct ModelPubStats {
    recv_total: u64,
    publish_ok: u64,
    publish_fail: u64,
    decode_err: u64,
    bad_dim: u64,
    infer_err: u64,
    symbol_miss: u64,
    infer_latency_sum_us: u64,
    infer_latency_max_us: u64,
    infer_count: u64,
}

struct SymbolModelRuntime {
    feature_dim: usize,
    model: XgbModel,
}

struct SymbolModelLoaded {
    symbol: String,
    feature_dim: usize,
    factor_count: usize,
    model_json_bytes: usize,
    runtime: SymbolModelRuntime,
}

#[derive(Debug, Deserialize)]
struct SymbolListResponse {
    #[serde(default)]
    items: Vec<SymbolListItem>,
}

#[derive(Debug, Deserialize)]
struct SymbolListItem {
    symbol: String,
}

#[derive(Debug, Deserialize)]
struct ModelPayloadResponse {
    payload: ModelPayloadBody,
}

#[derive(Debug, Deserialize)]
struct ModelPayloadBody {
    model_json: String,
    metadata: ModelMetadata,
    #[serde(default)]
    dim_factors: Vec<Value>,
}

#[derive(Debug, Deserialize)]
struct ModelMetadata {
    feature_dim: usize,
}

#[derive(Debug, Deserialize)]
struct LoginResp {
    token: String,
}

#[derive(Debug, Serialize)]
struct LoginReq<'a> {
    password: &'a str,
}

pub struct ModelPubApp {
    model_name: String,
    input_service: String,
    subscriber: Subscriber<ipc::Service, [u8; INPUT_MAX_BYTES], ()>,
    publisher: ModelPublisher,
    models_by_symbol: HashMap<String, SymbolModelRuntime>,
    stats: ModelPubStats,
    last_log_stats: Instant,
}

impl ModelPubApp {
    pub async fn new(config_path: &str, model_name: &str) -> Result<Self> {
        let model_name = normalize_model_name(model_name)?;

        let config = ModelPubConfig::load(config_path)?;
        config.validate()?;

        let input_service = config.render_input_service(&model_name)?;
        let output_service = config.render_output_service(&model_name)?;

        let models_by_symbol = Self::load_models_from_model_manager(&config, &model_name).await?;
        if models_by_symbol.is_empty() {
            anyhow::bail!(
                "startup model list is empty: model_name={} base_url={}",
                model_name,
                config.model_manager_base_url
            );
        }

        // Warmup: run one dummy predict per symbol to prime XGBoost internals
        for (symbol, runtime) in &models_by_symbol {
            let dummy = vec![0.0f32; runtime.feature_dim];
            match runtime.model.predict_one(&dummy) {
                Ok(_) => info!("warmup predict ok: model_name={} symbol={}", model_name, symbol),
                Err(e) => warn!("warmup predict failed: model_name={} symbol={} err={}", model_name, symbol, e),
            }
        }

        let subscriber = Self::create_subscriber(&model_name, &input_service)?;
        let publisher_node = format!("model_pub_{}_out", sanitize_node_suffix(&model_name));
        let publisher = ModelPublisher::new(&publisher_node, &output_service)?;

        info!(
            "ModelPubApp started: model_name={} input_service={} output_service={} loaded_symbols={}",
            model_name,
            input_service,
            output_service,
            models_by_symbol.len()
        );

        Ok(Self {
            model_name,
            input_service,
            subscriber,
            publisher,
            models_by_symbol,
            stats: ModelPubStats::default(),
            last_log_stats: Instant::now(),
        })
    }

    async fn load_models_from_model_manager(
        config: &ModelPubConfig,
        model_name: &str,
    ) -> Result<HashMap<String, SymbolModelRuntime>> {
        let base_url = config.model_manager_base_url.trim_end_matches('/');
        let client = Client::builder()
            .timeout(Duration::from_millis(
                config.model_manager_request_timeout_ms,
            ))
            .build()
            .context("build reqwest client failed")?;

        info!(
            "startup-models: loading model payloads from model_manager base_url={} model_name={}",
            base_url, model_name
        );

        let token = authenticate_model_manager(&client, config, base_url).await?;
        let symbols = fetch_model_symbols(&client, base_url, model_name, token.as_deref()).await?;
        if symbols.is_empty() {
            anyhow::bail!(
                "model_manager returned empty symbols for model={}",
                model_name
            );
        }

        info!(
            "startup-models: model_name={} symbol_count={} symbols={:?}",
            model_name,
            symbols.len(),
            symbols
        );

        let total = symbols.len();
        let fetch_parallel = MODEL_FETCH_CONCURRENCY.min(total.max(1));
        info!(
            "startup-models: concurrent loading enabled model_name={} concurrency={} total_symbols={}",
            model_name, fetch_parallel, total
        );

        let token_owned = token.clone();
        let task_stream = stream::iter(symbols.into_iter().map(|symbol| {
            let client = client.clone();
            let base_url = base_url.to_string();
            let model_name = model_name.to_string();
            let token = token_owned.clone();
            async move {
                load_single_symbol_model(&client, &base_url, &model_name, &symbol, token.as_deref())
                    .await
            }
        }))
        .buffer_unordered(fetch_parallel);

        let mut models = HashMap::with_capacity(total);
        let mut completed = 0usize;
        futures::pin_mut!(task_stream);
        while let Some(res) = task_stream.next().await {
            let loaded = res?;
            completed += 1;
            info!(
                "startup-model-loaded: model_name={} symbol={} feature_dim={} factor_count={} model_json_bytes={} progress={}/{}",
                model_name,
                loaded.symbol,
                loaded.feature_dim,
                loaded.factor_count,
                loaded.model_json_bytes,
                completed,
                total
            );
            models.insert(loaded.symbol.clone(), loaded.runtime);
        }

        Ok(models)
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
                let symbol_key = normalize_symbol_key(&feature.symbol);
                let predict_result = {
                    let Some(runtime) = self.models_by_symbol.get(&symbol_key) else {
                        self.stats.symbol_miss = self.stats.symbol_miss.saturating_add(1);
                        self.stats.infer_err = self.stats.infer_err.saturating_add(1);
                        warn!(
                            "model symbol not loaded: model_name={} symbol={} loaded_symbol_count={}",
                            self.model_name,
                            feature.symbol,
                            self.models_by_symbol.len()
                        );
                        self.emit_result(
                            &feature.symbol,
                            feature.ts_ms,
                            0.0,
                            MODEL_STATUS_INFER_ERR,
                        );
                        return;
                    };

                    if feature.feature_dim as usize != runtime.feature_dim
                        || feature.features.len() != runtime.feature_dim
                    {
                        self.stats.bad_dim = self.stats.bad_dim.saturating_add(1);
                        self.emit_result(&feature.symbol, feature.ts_ms, 0.0, MODEL_STATUS_BAD_DIM);
                        return;
                    }

                    let f32_features: Vec<f32> = feature.features.iter().map(|&v| v as f32).collect();
                    let infer_start = Instant::now();
                    let result = runtime.model.predict_one(&f32_features);
                    let elapsed_us = infer_start.elapsed().as_micros() as u64;
                    self.stats.infer_count += 1;
                    self.stats.infer_latency_sum_us += elapsed_us;
                    if elapsed_us > self.stats.infer_latency_max_us {
                        self.stats.infer_latency_max_us = elapsed_us;
                    }
                    result
                };

                match predict_result {
                    Ok(score) => {
                        self.emit_result(&feature.symbol, feature.ts_ms, score, MODEL_STATUS_OK);
                    }
                    Err(err) => {
                        self.stats.infer_err = self.stats.infer_err.saturating_add(1);
                        warn!(
                            "Model inference failed: model_name={} symbol={} err={}",
                            self.model_name, feature.symbol, err
                        );
                        self.emit_result(
                            &feature.symbol,
                            feature.ts_ms,
                            0.0,
                            MODEL_STATUS_INFER_ERR,
                        );
                    }
                }
            }
            Err(err) => {
                self.stats.decode_err = self.stats.decode_err.saturating_add(1);
                warn!(
                    "Feature message decode failed: model_name={} input_service={} err={}",
                    self.model_name, self.input_service, err
                );
                self.emit_result("", 0, 0.0, MODEL_STATUS_DECODE_ERR);
            }
        }
    }

    fn emit_result(&mut self, symbol: &str, ts_in_ms: i64, score: f64, status: u8) {
        let msg = ModelMsg::create(symbol.to_string(), ts_in_ms, now_millis(), 0, score, status);
        let bytes = match msg.to_bytes() {
            Ok(bytes) => bytes,
            Err(err) => {
                self.stats.publish_fail = self.stats.publish_fail.saturating_add(1);
                warn!(
                    "Model result encode failed: model_name={} symbol={} err={}",
                    self.model_name, symbol, err
                );
                return;
            }
        };

        if self.publisher.publish(bytes.as_ref()) {
            self.stats.publish_ok = self.stats.publish_ok.saturating_add(1);
        } else {
            self.stats.publish_fail = self.stats.publish_fail.saturating_add(1);
        }
    }

    fn log_stats(&mut self) {
        let avg_latency_us = if self.stats.infer_count > 0 {
            self.stats.infer_latency_sum_us / self.stats.infer_count
        } else {
            0
        };
        info!(
            "ModelPubApp[{}] stats: recv={} pub_ok={} pub_fail={} decode_err={} bad_dim={} infer_err={} symbol_miss={} infer_count={} latency_avg={}us latency_max={}us",
            self.model_name,
            self.stats.recv_total,
            self.stats.publish_ok,
            self.stats.publish_fail,
            self.stats.decode_err,
            self.stats.bad_dim,
            self.stats.infer_err,
            self.stats.symbol_miss,
            self.stats.infer_count,
            avg_latency_us,
            self.stats.infer_latency_max_us,
        );
        self.stats = ModelPubStats::default();
    }
}

async fn fetch_model_symbols(
    client: &Client,
    base_url: &str,
    model_name: &str,
    token: Option<&str>,
) -> Result<Vec<String>> {
    let url = format!(
        "{}/api/models/{}/symbols",
        base_url,
        urlencoding::encode(model_name)
    );

    let mut req = client.get(&url);
    if let Some(token) = token {
        req = req.bearer_auth(token);
    }

    let resp = req
        .send()
        .await
        .with_context(|| format!("GET {} failed", url))?
        .error_for_status()
        .with_context(|| format!("GET {} returned error status", url))?;

    let payload: SymbolListResponse = resp
        .json()
        .await
        .with_context(|| format!("decode symbols response failed: {}", url))?;

    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for item in payload.items {
        let symbol = normalize_symbol_key(&item.symbol);
        if symbol.is_empty() {
            continue;
        }
        if seen.insert(symbol.clone()) {
            out.push(symbol);
        }
    }
    out.sort();
    Ok(out)
}

async fn fetch_symbol_model_payload(
    client: &Client,
    base_url: &str,
    model_name: &str,
    symbol: &str,
    token: Option<&str>,
) -> Result<ModelPayloadBody> {
    let url = format!(
        "{}/api/models/{}/model/{}",
        base_url,
        urlencoding::encode(model_name),
        urlencoding::encode(symbol)
    );

    let mut req = client.get(&url);
    if let Some(token) = token {
        req = req.bearer_auth(token);
    }

    let resp = req
        .send()
        .await
        .with_context(|| format!("GET {} failed", url))?
        .error_for_status()
        .with_context(|| format!("GET {} returned error status", url))?;

    let payload: ModelPayloadResponse = resp
        .json()
        .await
        .with_context(|| format!("decode model payload response failed: {}", url))?;

    Ok(payload.payload)
}

async fn authenticate_model_manager(
    client: &Client,
    config: &ModelPubConfig,
    base_url: &str,
) -> Result<Option<String>> {
    if let Some(token) = config
        .model_manager_bearer_token
        .as_ref()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
    {
        return Ok(Some(token));
    }

    let Some(password) = config
        .model_manager_password
        .as_ref()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
    else {
        return Ok(None);
    };

    let login_url = format!("{}/api/auth/login", base_url);
    let resp = client
        .post(&login_url)
        .json(&LoginReq {
            password: password.as_str(),
        })
        .send()
        .await
        .with_context(|| format!("POST {} failed", login_url))?
        .error_for_status()
        .with_context(|| format!("POST {} returned error status", login_url))?;

    let payload: LoginResp = resp
        .json()
        .await
        .with_context(|| format!("decode login response failed: {}", login_url))?;
    Ok(Some(payload.token))
}

fn normalize_model_name(raw: &str) -> Result<String> {
    let normalized = raw.trim();
    if normalized.is_empty() {
        anyhow::bail!("model_name must not be empty");
    }
    Ok(normalized.to_string())
}

fn normalize_symbol_key(raw: &str) -> String {
    raw.trim().to_uppercase()
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

async fn load_single_symbol_model(
    client: &Client,
    base_url: &str,
    model_name: &str,
    symbol: &str,
    token: Option<&str>,
) -> Result<SymbolModelLoaded> {
    let payload = fetch_symbol_model_payload(client, base_url, model_name, symbol, token)
        .await
        .with_context(|| {
            format!(
                "fetch symbol model payload failed: model_name={} symbol={}",
                model_name, symbol
            )
        })?;

    if payload.model_json.trim().is_empty() {
        anyhow::bail!(
            "model payload has empty model_json: model_name={} symbol={}",
            model_name,
            symbol
        );
    }

    let feature_dim = payload.metadata.feature_dim;
    if feature_dim == 0 {
        anyhow::bail!(
            "model payload has invalid feature_dim=0: model_name={} symbol={}",
            model_name,
            symbol
        );
    }

    let factor_count = payload.dim_factors.len();
    let model_json_bytes = payload.model_json.len();
    let model = XgbModel::load_from_bytes(payload.model_json.as_bytes(), feature_dim)
        .with_context(|| {
            format!(
                "load xgboost model from payload failed: model_name={} symbol={} feature_dim={}",
                model_name, symbol, feature_dim
            )
        })?;

    Ok(SymbolModelLoaded {
        symbol: symbol.to_string(),
        feature_dim,
        factor_count,
        model_json_bytes,
        runtime: SymbolModelRuntime { feature_dim, model },
    })
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
