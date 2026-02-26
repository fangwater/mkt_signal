use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use reqwest::{header::HeaderName, Client};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use super::cfg::ModelPubConfig;
use super::model::OnnxModel;
use super::publisher::ModelPublisher;
use crate::common::mkt_msg::{FeatureMsg, FeatureStatus, ModelMsg, MODEL_STATUS_OK};
use crate::common::rolling_welford::RollingWelford;
use crate::factor_pub::fusion_factor_pub::app::ExtraFactorId;
use crate::factor_pub::fusion_factor_pub::publisher::FUSION_FACTOR_PAYLOAD_MAX_BYTES;
use crate::factor_pub::fusion_factor_pub::FusionFactorId;

const INPUT_MAX_BYTES: usize = FUSION_FACTOR_PAYLOAD_MAX_BYTES;
const IDLE_SLEEP_MICROS: u64 = 200;
const LOG_INTERVAL_SECS: u64 = 60;
const MODEL_FETCH_CONCURRENCY: usize = 8;

#[derive(Default)]
struct ModelPubStats {
    recv_total: u64,
    publish_ok: u64,
    publish_fail: u64,
    infer_latency_sum_us: u64,
    infer_latency_max_us: u64,
    zscore_latency_sum_us: u64,
    zscore_latency_max_us: u64,
    predict_latency_sum_us: u64,
    predict_latency_max_us: u64,
    marshal_latency_sum_us: u64,
    marshal_latency_max_us: u64,
    onnx_predict_latency_sum_us: u64,
    onnx_predict_latency_max_us: u64,
    infer_count: u64,
    cold_start_suppressed: u64,
    reload_only: u64,
}

struct SymbolNormState {
    welford_vec: Vec<RollingWelford>,
    sample_count: u64,
}

struct SymbolModelRuntime {
    feature_dim: usize,
    model: OnnxModel,
    factor_indices: Vec<u16>,
}

struct SymbolModelLoaded {
    symbol: String,
    model_onnx_bytes: usize,
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

struct ModelOnnxPayload {
    model_onnx: Vec<u8>,
    feature_dim: usize,
}

#[derive(Debug, Deserialize)]
struct SymbolDetailResp {
    #[serde(default)]
    factors: Vec<String>,
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
    norm_states: HashMap<String, SymbolNormState>,
    window_size: usize,
    min_samples: u64,
    zscore_cap: f64,
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

        // Warmup one dummy predict per symbol to catch ONNX/runtime issues early.
        let mut warmup_ok = 0usize;
        let mut warmup_fail: Vec<String> = Vec::new();
        for (symbol, runtime) in &models_by_symbol {
            let dummy = vec![0.0f32; runtime.feature_dim];
            match runtime.model.predict_one(&dummy) {
                Ok(_) => warmup_ok += 1,
                Err(e) => {
                    warn!(
                        "warmup predict failed: model_name={} symbol={} err={}",
                        model_name, symbol, e
                    );
                    warmup_fail.push(symbol.clone());
                }
            }
        }
        if warmup_fail.is_empty() {
            info!(
                "warmup predict done: model_name={} all {}/{} ok",
                model_name,
                warmup_ok,
                models_by_symbol.len()
            );
        } else {
            warn!(
                "warmup predict done: model_name={} ok={} failed={} failed_symbols={:?}",
                model_name,
                warmup_ok,
                warmup_fail.len(),
                warmup_fail
            );
        }

        let subscriber = Self::create_subscriber(&model_name, &input_service)?;
        let publisher_node = format!("model_pub_{}_out", sanitize_node_suffix(&model_name));
        let publisher = ModelPublisher::new(&publisher_node, &output_service)?;

        info!(
            "ModelPubApp started: model_name={} input={} output={} symbols={} window_size={} min_samples={} zscore_cap={}",
            model_name,
            input_service,
            output_service,
            models_by_symbol.len(),
            config.window_size,
            config.min_samples,
            config.zscore_cap,
        );

        Ok(Self {
            model_name,
            input_service,
            subscriber,
            publisher,
            models_by_symbol,
            norm_states: HashMap::new(),
            window_size: config.window_size,
            min_samples: config.min_samples,
            zscore_cap: config.zscore_cap,
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
            "startup-models: loading ONNX model artifacts from model_manager base_url={} model_name={}",
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
        let config_owned = config.clone();
        let task_stream = stream::iter(symbols.into_iter().map(|symbol| {
            let client = client.clone();
            let base_url = base_url.to_string();
            let model_name = model_name.to_string();
            let token = token_owned.clone();
            let config = config_owned.clone();
            async move {
                load_single_symbol_model(
                    &client,
                    &base_url,
                    &config,
                    &model_name,
                    &symbol,
                    token.as_deref(),
                )
                .await
            }
        }))
        .buffer_unordered(fetch_parallel);

        let mut models = HashMap::with_capacity(total);
        let mut completed = 0usize;
        let mut total_model_bytes = 0usize;
        futures::pin_mut!(task_stream);
        while let Some(res) = task_stream.next().await {
            let loaded = res?;
            completed += 1;
            total_model_bytes += loaded.model_onnx_bytes;
            models.insert(loaded.symbol.clone(), loaded.runtime);
        }

        info!(
            "startup-models-loaded: model_name={} loaded={}/{} total_model_bytes={}",
            model_name, completed, total, total_model_bytes
        );

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
            .max_publishers(1)
            .max_subscribers(10)
            .subscriber_max_buffer_size(8192)
            .history_size(128)
            .open_or_create()?;

        let subscriber = service.subscriber_builder().buffer_size(8192).create()?;
        info!("Subscribed to feature channel: {}", service_path);
        Ok(subscriber)
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            let mut has_message = false;
            while let Some(sample) = self.subscriber.receive()? {
                has_message = true;
                self.stats.recv_total += 1;
                if self.stats.recv_total % 500 == 1 {
                    info!(
                        "ModelPub[{}] received: recv_total={} bytes={}",
                        self.model_name,
                        self.stats.recv_total,
                        sample.payload().len()
                    );
                }

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

                // --- z-score normalization ---
                let dim = feature.features.len();
                let window_size = self.window_size;
                let norm_state = self
                    .norm_states
                    .entry(symbol_key.clone())
                    .or_insert_with(|| SymbolNormState {
                        welford_vec: (0..dim).map(|_| RollingWelford::new(window_size)).collect(),
                        sample_count: 0,
                    });

                // Always push into Welford (heartbeat), even during cold start
                if norm_state.welford_vec.len() != dim {
                    // dimension changed, reset
                    norm_state.welford_vec =
                        (0..dim).map(|_| RollingWelford::new(window_size)).collect();
                    norm_state.sample_count = 0;
                }
                for (i, &val) in feature.features.iter().enumerate() {
                    norm_state.welford_vec[i].push(val);
                }
                norm_state.sample_count += 1;

                // Cold-start gating: suppress publishing until min_samples reached
                if norm_state.sample_count < self.min_samples {
                    self.stats.cold_start_suppressed += 1;
                    return;
                }

                // Reload: only update rolling z-score, skip inference
                if feature.status == FeatureStatus::Reload as u8 {
                    self.stats.reload_only += 1;
                    return;
                }

                // Compute z-scores
                let zscore_start = Instant::now();
                let normalized: Vec<f64> = norm_state
                    .welford_vec
                    .iter()
                    .map(|w| w.zscore_capped(self.zscore_cap).unwrap_or(0.0))
                    .collect();
                let zscore_us = zscore_start.elapsed().as_micros() as u64;

                // --- inference ---
                let Some(runtime) = self.models_by_symbol.get(&symbol_key) else {
                    panic!(
                        "model symbol not loaded: model_name={} symbol={} loaded_symbol_count={}",
                        self.model_name,
                        feature.symbol,
                        self.models_by_symbol.len()
                    );
                };

                if normalized.len() != runtime.feature_dim {
                    panic!(
                        "feature dim mismatch: model_name={} symbol={} expected={} got={}",
                        self.model_name,
                        feature.symbol,
                        runtime.feature_dim,
                        normalized.len()
                    );
                }

                let f32_features: Vec<f32> = normalized.iter().map(|&v| v as f32).collect();
                let factor_indices = runtime.factor_indices.clone();
                let predict_start = Instant::now();
                let result = runtime.model.predict_one_timed(&f32_features);
                let predict_us = predict_start.elapsed().as_micros() as u64;

                match result {
                    Ok(output) => {
                        let elapsed_us = zscore_us + predict_us;
                        self.stats.infer_count += 1;
                        self.stats.infer_latency_sum_us += elapsed_us;
                        if elapsed_us > self.stats.infer_latency_max_us {
                            self.stats.infer_latency_max_us = elapsed_us;
                        }
                        self.stats.zscore_latency_sum_us += zscore_us;
                        if zscore_us > self.stats.zscore_latency_max_us {
                            self.stats.zscore_latency_max_us = zscore_us;
                        }
                        self.stats.predict_latency_sum_us += predict_us;
                        if predict_us > self.stats.predict_latency_max_us {
                            self.stats.predict_latency_max_us = predict_us;
                        }
                        self.stats.marshal_latency_sum_us += output.marshal_us;
                        if output.marshal_us > self.stats.marshal_latency_max_us {
                            self.stats.marshal_latency_max_us = output.marshal_us;
                        }
                        self.stats.onnx_predict_latency_sum_us += output.onnx_predict_us;
                        if output.onnx_predict_us > self.stats.onnx_predict_latency_max_us {
                            self.stats.onnx_predict_latency_max_us = output.onnx_predict_us;
                        }

                        if symbol_key.contains("BTC") {
                            log_btc_heartbeat(
                                &self.model_name,
                                &feature.symbol,
                                &feature.features,
                                &normalized,
                                output.score,
                                elapsed_us,
                                zscore_us,
                                predict_us,
                                output.marshal_us,
                                output.onnx_predict_us,
                            );
                        }

                        self.emit_result(
                            &feature.symbol,
                            feature.ts_ms,
                            output.score,
                            MODEL_STATUS_OK,
                            &factor_indices,
                            &f32_features,
                        );
                    }
                    Err(err) => {
                        panic!(
                            "Model inference failed: model_name={} symbol={} err={}",
                            self.model_name, feature.symbol, err
                        );
                    }
                }
            }
            Err(err) => {
                panic!(
                    "Feature message decode failed: model_name={} input_service={} err={}",
                    self.model_name, self.input_service, err
                );
            }
        }
    }

    fn emit_result(
        &mut self,
        symbol: &str,
        ts_in_ms: i64,
        score: f64,
        status: u8,
        factor_indices: &[u16],
        factor_values: &[f32],
    ) {
        let msg = ModelMsg::create(
            symbol.to_string(),
            ts_in_ms,
            now_millis(),
            0,
            score,
            status,
            factor_indices.to_vec(),
            factor_values.to_vec(),
        );
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
        let avg_zscore_us = if self.stats.infer_count > 0 {
            self.stats.zscore_latency_sum_us / self.stats.infer_count
        } else {
            0
        };
        let avg_predict_us = if self.stats.infer_count > 0 {
            self.stats.predict_latency_sum_us / self.stats.infer_count
        } else {
            0
        };
        let avg_marshal_us = if self.stats.infer_count > 0 {
            self.stats.marshal_latency_sum_us / self.stats.infer_count
        } else {
            0
        };
        let avg_onnx_predict_us = if self.stats.infer_count > 0 {
            self.stats.onnx_predict_latency_sum_us / self.stats.infer_count
        } else {
            0
        };
        let norm_symbols = self.norm_states.len();
        let warmed_symbols = self
            .norm_states
            .values()
            .filter(|s| s.sample_count >= self.min_samples)
            .count();

        let mut extra = String::new();
        if self.stats.publish_fail > 0 {
            extra.push_str(&format!(" pub_fail={}", self.stats.publish_fail));
        }
        if self.stats.cold_start_suppressed > 0 {
            extra.push_str(&format!(
                " cold_suppressed={}",
                self.stats.cold_start_suppressed
            ));
        }
        if self.stats.reload_only > 0 {
            extra.push_str(&format!(" reload_only={}", self.stats.reload_only));
        }

        info!(
            "ModelPubApp[{}] recv={} pub={} infer={} lat(avg/max): total={}us/{}us zscore={}us/{}us predict={}us/{}us marshal={}us/{}us onnx_predict={}us/{}us warmed={}/{}{}",
            self.model_name,
            self.stats.recv_total,
            self.stats.publish_ok,
            self.stats.infer_count,
            avg_latency_us,
            self.stats.infer_latency_max_us,
            avg_zscore_us,
            self.stats.zscore_latency_max_us,
            avg_predict_us,
            self.stats.predict_latency_max_us,
            avg_marshal_us,
            self.stats.marshal_latency_max_us,
            avg_onnx_predict_us,
            self.stats.onnx_predict_latency_max_us,
            warmed_symbols,
            norm_symbols,
            extra,
        );

        self.stats = ModelPubStats::default();
    }
}

fn log_btc_heartbeat(
    model_name: &str,
    symbol: &str,
    raw_features: &[f64],
    zscore_features: &[f64],
    score: f64,
    infer_latency_us: u64,
    zscore_latency_us: u64,
    predict_latency_us: u64,
    marshal_latency_us: u64,
    onnx_predict_latency_us: u64,
) {
    let raw_str: Vec<String> = raw_features.iter().map(|v| format!("{:.6}", v)).collect();
    let zscore_str: Vec<String> = zscore_features
        .iter()
        .map(|v| format!("{:.4}", v))
        .collect();
    let total_ms = infer_latency_us as f64 / 1000.0;
    let zscore_ms = zscore_latency_us as f64 / 1000.0;
    let predict_ms = predict_latency_us as f64 / 1000.0;
    let marshal_ms = marshal_latency_us as f64 / 1000.0;
    let onnx_predict_ms = onnx_predict_latency_us as f64 / 1000.0;
    info!(
        "ModelPubApp[{}] {} heartbeat: raw=[{}] zscore=[{}] score={:.6} lat: total={:.2}ms zscore={:.2}ms predict={:.2}ms marshal={:.2}ms onnx_predict={:.2}ms",
        model_name,
        symbol,
        raw_str.join(", "),
        zscore_str.join(", "),
        score,
        total_ms,
        zscore_ms,
        predict_ms,
        marshal_ms,
        onnx_predict_ms,
    );
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

async fn fetch_symbol_model_onnx(
    client: &Client,
    base_url: &str,
    config: &ModelPubConfig,
    model_name: &str,
    symbol: &str,
    token: Option<&str>,
) -> Result<ModelOnnxPayload> {
    let relative_path = config
        .render_model_onnx_path(model_name, symbol)
        .with_context(|| {
            format!(
                "render model ONNX artifact path failed: model_name={} symbol={}",
                model_name, symbol
            )
        })?;

    let url = build_model_url(base_url, &relative_path);

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

    let feature_dim_header = parse_feature_dim_header_name(config)?;
    let feature_dim = parse_feature_dim_value(resp.headers(), &feature_dim_header, &url)?;
    if feature_dim == 0 {
        anyhow::bail!(
            "invalid model feature dim from response header '{}': 0, url={}",
            feature_dim_header.as_str(),
            url
        );
    }

    let model_onnx = resp
        .bytes()
        .await
        .with_context(|| format!("read ONNX model response failed: {}", url))?
        .to_vec();
    if model_onnx.is_empty() {
        anyhow::bail!("empty ONNX model payload: {}", url);
    }

    Ok(ModelOnnxPayload {
        model_onnx,
        feature_dim,
    })
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
    config: &ModelPubConfig,
    model_name: &str,
    symbol: &str,
    token: Option<&str>,
) -> Result<SymbolModelLoaded> {
    let payload = fetch_symbol_model_onnx(client, base_url, config, model_name, symbol, token)
        .await
        .with_context(|| {
            format!(
                "fetch symbol model ONNX artifact failed: model_name={} symbol={}",
                model_name, symbol
            )
        })?;

    let feature_dim = payload.feature_dim;
    if feature_dim == 0 {
        anyhow::bail!(
            "model ONNX artifact payload has invalid feature_dim=0: model_name={} symbol={}",
            model_name,
            symbol
        );
    }

    let model_onnx_bytes = payload.model_onnx.len();
    let model_path = persist_model_onnx_to_cache(
        &config.model_onnx_cache_dir,
        model_name,
        symbol,
        &payload.model_onnx,
    )
    .with_context(|| {
        format!(
            "persist model ONNX artifact to cache failed: model_name={} symbol={}",
            model_name, symbol
        )
    })?;

    let model = OnnxModel::load_from_path(&model_path, Some(feature_dim))
        .with_context(|| {
            format!(
                "load ONNX model from cache failed: model_name={} symbol={} feature_dim={} path={}",
                model_name,
                symbol,
                feature_dim,
                model_path.display()
            )
        })?;

    let factor_indices = fetch_symbol_factor_indices(client, base_url, model_name, symbol, token)
        .await
        .with_context(|| {
            format!(
                "fetch symbol factor indices failed: model_name={} symbol={}",
                model_name, symbol
            )
        })?;

    if factor_indices.len() != feature_dim {
        anyhow::bail!(
            "factor indices count mismatch with feature_dim: model_name={} symbol={} indices={} feature_dim={}",
            model_name, symbol, factor_indices.len(), feature_dim
        );
    }

    Ok(SymbolModelLoaded {
        symbol: symbol.to_string(),
        model_onnx_bytes,
        runtime: SymbolModelRuntime {
            feature_dim,
            model,
            factor_indices,
        },
    })
}

/// Map a factor name to its u16 index.
/// FusionFactorId: 0..631, ExtraFactorId: 1000..1005.
fn factor_name_to_index(name: &str) -> Option<u16> {
    if let Some(fid) = FusionFactorId::from_name(name) {
        return Some(fid.as_index());
    }
    if let Some(eid) = ExtraFactorId::from_name(name) {
        return Some(eid.as_index());
    }
    None
}

async fn fetch_symbol_factor_indices(
    _client: &Client,
    base_url: &str,
    model_name: &str,
    symbol: &str,
    token: Option<&str>,
) -> Result<Vec<u16>> {
    // Use a dedicated short-timeout client — symbol detail is a fast JSON lookup.
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("build factor-indices client failed")?;

    let url = format!(
        "{}/api/models/{}/symbols/{}",
        base_url,
        urlencoding::encode(model_name),
        urlencoding::encode(symbol)
    );

    let mut req = client.get(&url);
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }

    let resp = req
        .send()
        .await
        .with_context(|| format!("GET {} failed", url))?
        .error_for_status()
        .with_context(|| format!("GET {} returned error status", url))?;

    let detail: SymbolDetailResp = resp
        .json()
        .await
        .with_context(|| format!("decode symbol detail failed: {}", url))?;

    let mut indices = Vec::with_capacity(detail.factors.len());
    for name in &detail.factors {
        match factor_name_to_index(name) {
            Some(idx) => indices.push(idx),
            None => {
                warn!(
                    "unknown factor name in model plan, using u16::MAX: model_name={} symbol={} factor={}",
                    model_name, symbol, name
                );
                indices.push(u16::MAX);
            }
        }
    }

    Ok(indices)
}

fn build_model_url(base_url: &str, path_or_url: &str) -> String {
    let trimmed = path_or_url.trim();
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return trimmed.to_string();
    }
    format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        trimmed.trim_start_matches('/')
    )
}

fn parse_feature_dim_header_name(config: &ModelPubConfig) -> Result<HeaderName> {
    HeaderName::from_bytes(config.model_onnx_feature_dim_header.trim().as_bytes()).with_context(
        || {
            format!(
                "invalid feature dim header name: {}",
                config.model_onnx_feature_dim_header
            )
        },
    )
}

fn parse_feature_dim_value(
    headers: &reqwest::header::HeaderMap,
    header_name: &HeaderName,
    url: &str,
) -> Result<usize> {
    let value = headers.get(header_name).ok_or_else(|| {
        anyhow::anyhow!(
            "missing feature dim header '{}' in ONNX model response: {}",
            header_name.as_str(),
            url
        )
    })?;

    let raw = value.to_str().with_context(|| {
        format!(
            "feature dim header '{}' is not valid utf-8, url={}",
            header_name.as_str(),
            url
        )
    })?;

    let parsed = raw.trim().parse::<usize>().with_context(|| {
        format!(
            "feature dim header '{}' is not a valid usize: value='{}' url={}",
            header_name.as_str(),
            raw,
            url
        )
    })?;
    Ok(parsed)
}

fn persist_model_onnx_to_cache(
    cache_root: &str,
    model_name: &str,
    symbol: &str,
    model_onnx: &[u8],
) -> Result<PathBuf> {
    if model_onnx.is_empty() {
        anyhow::bail!("model_onnx must not be empty");
    }

    let root = Path::new(cache_root.trim());
    if root.as_os_str().is_empty() {
        anyhow::bail!("model_onnx_cache_dir must not be empty");
    }

    let model_dir = root.join(sanitize_node_suffix(model_name));
    fs::create_dir_all(&model_dir)
        .with_context(|| format!("create model cache dir failed: {}", model_dir.display()))?;

    let mut hasher = Sha256::new();
    hasher.update(model_onnx);
    let digest = format!("{:x}", hasher.finalize());
    let symbol_safe = sanitize_node_suffix(symbol);
    let target = model_dir.join(format!("{}.{}.onnx", symbol_safe, &digest[..16]));
    if target.exists() {
        return Ok(target);
    }

    let tmp = model_dir.join(format!("{}.{}.tmp", symbol_safe, now_millis()));
    fs::write(&tmp, model_onnx)
        .with_context(|| format!("write tmp model ONNX artifact failed: {}", tmp.display()))?;
    match fs::rename(&tmp, &target) {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(&tmp);
            return Ok(target);
        }
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "atomic rename model ONNX artifact failed: from={} to={}",
                    tmp.display(),
                    target.display()
                )
            });
        }
    }
    Ok(target)
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
