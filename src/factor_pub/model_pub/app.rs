use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use reqwest::{header::HeaderName, Client};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use super::cfg::ModelPubConfig;
use super::factor_pool::{
    build_extract_indices, build_factor_indices, build_factor_position_map,
    load_symbol_factor_names_from_tlen_server, parse_venue_slug_from_input_service,
};
use super::model::OnnxModel;
use super::publisher::ModelPublisher;
use super::score_rolling::InlineScoreRolling;
use crate::common::mkt_msg::{FeatureMsg, FeatureStatus, ModelMsg, MODEL_STATUS_OK};
use crate::factor_pub::fusion_factor_pub::publisher::FUSION_FACTOR_PAYLOAD_MAX_BYTES;

const INPUT_MAX_BYTES: usize = FUSION_FACTOR_PAYLOAD_MAX_BYTES;
const IDLE_SLEEP_MICROS: u64 = 200;
const LOG_INTERVAL_SECS: u64 = 60;
const MODEL_FETCH_CONCURRENCY: usize = 1;
const MODEL_ONNX_PATH_TEMPLATE: &str = "/api/models/{model_name}/model_onnx/{symbol}";
const MODEL_ONNX_FEATURE_DIM_HEADER: &str = "x-model-feature-dim";
const MODEL_ONNX_CACHE_DIR: &str = "/tmp/mkt_signal_model_pub_onnx";
const NEUTRAL_SCORE_QUANTILE: f64 = 0.5;

#[derive(Default)]
struct ModelPubStats {
    recv_total: u64,
    publish_ok: u64,
    publish_fail: u64,
    infer_latency_sum_us: u64,
    infer_latency_max_us: u64,
    predict_latency_sum_us: u64,
    predict_latency_max_us: u64,
    marshal_latency_sum_us: u64,
    marshal_latency_max_us: u64,
    onnx_predict_latency_sum_us: u64,
    onnx_predict_latency_max_us: u64,
    infer_count: u64,
    reload_only: u64,
    empty_feature_drop: u64,
}

struct SymbolModelRuntime {
    input_feature_dim: usize,
    feature_dim: usize,
    model: OnnxModel,
    factor_indices: Vec<u16>,
    extract_indices: Vec<usize>,
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

pub struct ModelPubApp {
    model_name: String,
    input_service: String,
    subscriber: Subscriber<ipc::Service, [u8; INPUT_MAX_BYTES], ()>,
    publisher: ModelPublisher,
    models_by_symbol: HashMap<String, SymbolModelRuntime>,
    stats: ModelPubStats,
    last_log_stats: Instant,
    score_rolling: InlineScoreRolling,
}

impl ModelPubApp {
    pub async fn new(
        config_path: &str,
        model_name: &str,
        warming_dir: Option<&Path>,
    ) -> Result<Self> {
        let model_name = normalize_model_name(model_name)?;

        let config = ModelPubConfig::load(config_path)?;
        config.validate()?;

        let input_service = config.input_service.trim().to_string();
        let output_service = config.output_service.trim().to_string();
        let venue_slug = parse_venue_slug_from_input_service(&input_service)?;
        let symbol_factor_names = load_symbol_factor_names_from_tlen_server(&config, &venue_slug)
            .await
            .with_context(|| {
                format!(
                    "load symbol factor plans from tlen_server failed: venue={}",
                    venue_slug
                )
            })?;

        let models_by_symbol =
            Self::load_models_from_model_manager(&config, &model_name, &symbol_factor_names)
                .await?;
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
        let mut score_rolling = InlineScoreRolling::new(&model_name, &config.score_rolling).await?;
        if let Some(dir) = warming_dir {
            score_rolling.warm_from_dir(dir).await.with_context(|| {
                format!(
                    "warm score_rolling from dir failed: model={} dir={}",
                    model_name,
                    dir.display()
                )
            })?;
        }

        info!(
            "ModelPubApp started: input={} output={} symbols={} venue={} symbol_factor_plans={} warming_dir={}",
            input_service,
            output_service,
            models_by_symbol.len(),
            venue_slug,
            symbol_factor_names.len(),
            warming_dir
                .map(|dir| dir.display().to_string())
                .unwrap_or_else(|| "-".to_string()),
        );

        Ok(Self {
            model_name,
            input_service,
            subscriber,
            publisher,
            models_by_symbol,
            stats: ModelPubStats::default(),
            last_log_stats: Instant::now(),
            score_rolling,
        })
    }

    async fn load_models_from_model_manager(
        config: &ModelPubConfig,
        model_name: &str,
        symbol_factor_names: &HashMap<String, Vec<String>>,
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

        let symbols = fetch_model_symbols(&client, base_url, model_name).await?;
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

        let task_stream = stream::iter(symbols.into_iter().map(|symbol| {
            let client = client.clone();
            let base_url = base_url.to_string();
            let model_name = model_name.to_string();
            let symbol_factor_names = symbol_factor_names.clone();
            async move {
                load_single_symbol_model(
                    &client,
                    &base_url,
                    &model_name,
                    &symbol,
                    &symbol_factor_names,
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
            .open_or_create()
            .with_context(|| {
                format!(
                    "open_or_create feature channel failed: service_path={}",
                    service_path
                )
            })?;

        let subscriber = service.subscriber_builder().buffer_size(8192).create()?;
        info!("Subscribed to feature channel: {}", service_path);
        Ok(subscriber)
    }

    pub async fn run(&mut self) -> Result<()> {
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
                self.process_input(&data).await?;
            }

            if !has_message {
                tokio::time::sleep(Duration::from_micros(IDLE_SLEEP_MICROS)).await;
            }

            if self.last_log_stats.elapsed() >= Duration::from_secs(LOG_INTERVAL_SECS) {
                self.log_stats();
                self.last_log_stats = Instant::now();
            }
        }
    }

    async fn process_input(&mut self, data: &[u8]) -> Result<()> {
        match FeatureMsg::from_bytes(data) {
            Ok(feature) => {
                let symbol_key = normalize_symbol_key(&feature.symbol);
                let normalized = feature.features;
                let dim = normalized.len();
                let runtime = self.models_by_symbol.get(&symbol_key);

                // Empty feature vector means fusion_factor tracked the symbol but no factor plan
                // was configured for it. Skip inference and let upstream RL-only flow stand.
                if dim == 0 {
                    if runtime.is_some() {
                        panic!(
                            "empty feature vector for loaded model symbol: model_name={} symbol={}",
                            self.model_name, feature.symbol
                        );
                    }
                    self.stats.empty_feature_drop = self.stats.empty_feature_drop.saturating_add(1);
                    self.emit_zero_result_for_missing_model(&feature.symbol, feature.ts_ms)
                        .await?;
                    return Ok(());
                }

                // Reload payloads come from fusion_factor bootstrap. They are already normalized,
                // but model inference should still wait for live data.
                if feature.status == FeatureStatus::Reload as u8 {
                    if let Some(runtime) = runtime {
                        if dim != runtime.input_feature_dim {
                            warn!(
                                "reload feature dim mismatch, emit zero score: model_name={} symbol={} expected={} got={}",
                                self.model_name,
                                feature.symbol,
                                runtime.input_feature_dim,
                                dim
                            );
                            self.emit_zero_result_for_missing_model(&feature.symbol, feature.ts_ms)
                                .await?;
                        }
                    }
                    self.stats.reload_only += 1;
                    return Ok(());
                }

                // --- inference ---
                let Some(runtime) = runtime else {
                    warn!(
                        "model symbol not loaded, emit zero score: model_name={} symbol={} loaded_symbol_count={}",
                        self.model_name,
                        feature.symbol,
                        self.models_by_symbol.len()
                    );
                    self.emit_zero_result_for_missing_model(&feature.symbol, feature.ts_ms)
                        .await?;
                    return Ok(());
                };

                if dim != runtime.input_feature_dim {
                    warn!(
                        "symbol feature dim mismatch, emit zero score: model_name={} symbol={} expected={} got={}",
                        self.model_name,
                        feature.symbol,
                        runtime.input_feature_dim,
                        dim
                    );
                    self.emit_zero_result_for_missing_model(&feature.symbol, feature.ts_ms)
                        .await?;
                    return Ok(());
                }

                let extracted: Vec<f64> = runtime
                    .extract_indices
                    .iter()
                    .map(|&idx| normalized[idx])
                    .collect();
                if extracted.len() != runtime.feature_dim {
                    warn!(
                        "extracted feature dim mismatch, emit zero score: model_name={} symbol={} expected={} got={}",
                        self.model_name,
                        feature.symbol,
                        runtime.feature_dim,
                        extracted.len()
                    );
                    self.emit_zero_result_for_missing_model(&feature.symbol, feature.ts_ms)
                        .await?;
                    return Ok(());
                }

                let f32_features: Vec<f32> = extracted.iter().map(|&v| v as f32).collect();
                let factor_indices = runtime.factor_indices.clone();
                let predict_start = Instant::now();
                let result = runtime.model.predict_one_timed(&f32_features);
                let predict_us = predict_start.elapsed().as_micros() as u64;

                match result {
                    Ok(output) => {
                        let elapsed_us = predict_us;
                        self.stats.infer_count += 1;
                        self.stats.infer_latency_sum_us += elapsed_us;
                        if elapsed_us > self.stats.infer_latency_max_us {
                            self.stats.infer_latency_max_us = elapsed_us;
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
                                &extracted,
                                output.score,
                                elapsed_us,
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
                        )
                        .await?;
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
        Ok(())
    }

    async fn emit_zero_result_for_missing_model(
        &mut self,
        symbol: &str,
        ts_in_ms: i64,
    ) -> Result<()> {
        self.publish_result(
            symbol,
            ts_in_ms,
            0.0,
            Some(NEUTRAL_SCORE_QUANTILE),
            MODEL_STATUS_OK,
            &[],
            &[],
            false,
        )
        .await
    }

    async fn emit_result(
        &mut self,
        symbol: &str,
        ts_in_ms: i64,
        score: f64,
        status: u8,
        factor_indices: &[u16],
        factor_values: &[f32],
    ) -> Result<()> {
        let score_quantile = self.score_rolling.preview_score_quantile(symbol, score);
        self.publish_result(
            symbol,
            ts_in_ms,
            score,
            score_quantile,
            status,
            factor_indices,
            factor_values,
            true,
        )
        .await
    }

    async fn publish_result(
        &mut self,
        symbol: &str,
        ts_in_ms: i64,
        score: f64,
        score_quantile: Option<f64>,
        status: u8,
        factor_indices: &[u16],
        factor_values: &[f32],
        update_score_rolling: bool,
    ) -> Result<()> {
        let ts_out_ms = now_millis();
        let msg = ModelMsg::create(
            symbol.to_string(),
            ts_in_ms,
            ts_out_ms,
            0,
            score,
            score_quantile,
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
                return Ok(());
            }
        };

        if self.publisher.publish(bytes.as_ref()) {
            self.stats.publish_ok = self.stats.publish_ok.saturating_add(1);
            if update_score_rolling {
                self.score_rolling
                    .post_process_score(symbol, score, ts_out_ms)
                    .await?;
            }
        } else {
            self.stats.publish_fail = self.stats.publish_fail.saturating_add(1);
        }
        Ok(())
    }

    fn log_stats(&mut self) {
        let avg_latency_us = if self.stats.infer_count > 0 {
            self.stats.infer_latency_sum_us / self.stats.infer_count
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
        let mut extra = String::new();
        if self.stats.publish_fail > 0 {
            extra.push_str(&format!(" pub_fail={}", self.stats.publish_fail));
        }
        if self.stats.reload_only > 0 {
            extra.push_str(&format!(" reload_only={}", self.stats.reload_only));
        }
        if self.stats.empty_feature_drop > 0 {
            extra.push_str(&format!(" empty_drop={}", self.stats.empty_feature_drop));
        }

        info!(
            "ModelPubApp[{}] recv={} pub={} infer={} lat(avg/max): total={}us/{}us predict={}us/{}us marshal={}us/{}us onnx_predict={}us/{}us{}",
            self.model_name,
            self.stats.recv_total,
            self.stats.publish_ok,
            self.stats.infer_count,
            avg_latency_us,
            self.stats.infer_latency_max_us,
            avg_predict_us,
            self.stats.predict_latency_max_us,
            avg_marshal_us,
            self.stats.marshal_latency_max_us,
            avg_onnx_predict_us,
            self.stats.onnx_predict_latency_max_us,
            extra,
        );

        self.stats = ModelPubStats::default();
    }
}

fn log_btc_heartbeat(
    model_name: &str,
    symbol: &str,
    normalized_features: &[f64],
    score: f64,
    infer_latency_us: u64,
    predict_latency_us: u64,
    marshal_latency_us: u64,
    onnx_predict_latency_us: u64,
) {
    let normalized_str: Vec<String> = normalized_features
        .iter()
        .map(|v| format!("{:.4}", v))
        .collect();
    let total_ms = infer_latency_us as f64 / 1000.0;
    let predict_ms = predict_latency_us as f64 / 1000.0;
    let marshal_ms = marshal_latency_us as f64 / 1000.0;
    let onnx_predict_ms = onnx_predict_latency_us as f64 / 1000.0;
    info!(
        "ModelPubApp[{}] {} heartbeat: features=[{}] score={:.6} lat: total={:.2}ms predict={:.2}ms marshal={:.2}ms onnx_predict={:.2}ms",
        model_name,
        symbol,
        normalized_str.join(", "),
        score,
        total_ms,
        predict_ms,
        marshal_ms,
        onnx_predict_ms,
    );
}

async fn fetch_model_symbols(
    client: &Client,
    base_url: &str,
    model_name: &str,
) -> Result<Vec<String>> {
    let url = format!(
        "{}/api/models/{}/symbols",
        base_url,
        urlencoding::encode(model_name)
    );

    let resp = client
        .get(&url)
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
    model_name: &str,
    symbol: &str,
) -> Result<ModelOnnxPayload> {
    let relative_path = render_model_onnx_path(model_name, symbol).with_context(|| {
        format!(
            "render model ONNX artifact path failed: model_name={} symbol={}",
            model_name, symbol
        )
    })?;

    let url = build_model_url(base_url, &relative_path);

    let resp = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {} failed", url))?
        .error_for_status()
        .with_context(|| format!("GET {} returned error status", url))?;

    let feature_dim_header = parse_feature_dim_header_name()?;
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
    symbol_factor_names: &HashMap<String, Vec<String>>,
) -> Result<SymbolModelLoaded> {
    let payload = fetch_symbol_model_onnx(client, base_url, model_name, symbol)
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
    let model_path = persist_model_onnx_to_cache(model_name, symbol, &payload.model_onnx)
        .with_context(|| {
            format!(
                "persist model ONNX artifact to cache failed: model_name={} symbol={}",
                model_name, symbol
            )
        })?;

    let model = OnnxModel::load_from_path(&model_path, Some(feature_dim)).with_context(|| {
        format!(
            "load ONNX model from cache failed: model_name={} symbol={} feature_dim={} path={}",
            model_name,
            symbol,
            feature_dim,
            model_path.display()
        )
    })?;

    let factor_names = fetch_symbol_factor_names(base_url, model_name, symbol)
        .await
        .with_context(|| {
            format!(
                "fetch symbol factor names failed: model_name={} symbol={}",
                model_name, symbol
            )
        })?;
    let symbol_plan = symbol_factor_names.get(symbol).ok_or_else(|| {
        anyhow::anyhow!(
            "missing symbol factor plan from tlen_server: model_name={} symbol={}",
            model_name,
            symbol
        )
    })?;
    let symbol_factor_positions =
        build_factor_position_map(symbol, symbol_plan).with_context(|| {
            format!(
                "build symbol factor position map failed: model_name={} symbol={} factors={}",
                model_name,
                symbol,
                symbol_plan.len()
            )
        })?;
    let factor_indices = build_factor_indices(model_name, symbol, &factor_names);
    let extract_indices =
        build_extract_indices(model_name, symbol, &factor_names, &symbol_factor_positions);

    if factor_indices.len() != feature_dim {
        anyhow::bail!(
            "factor indices count mismatch with feature_dim: model_name={} symbol={} indices={} feature_dim={}",
            model_name, symbol, factor_indices.len(), feature_dim
        );
    }
    if extract_indices.len() != feature_dim {
        anyhow::bail!(
            "extract indices count mismatch with feature_dim: model_name={} symbol={} indices={} feature_dim={}",
            model_name, symbol, extract_indices.len(), feature_dim
        );
    }

    Ok(SymbolModelLoaded {
        symbol: symbol.to_string(),
        model_onnx_bytes,
        runtime: SymbolModelRuntime {
            input_feature_dim: symbol_plan.len(),
            feature_dim,
            model,
            factor_indices,
            extract_indices,
        },
    })
}

async fn fetch_symbol_factor_names(
    base_url: &str,
    model_name: &str,
    symbol: &str,
) -> Result<Vec<String>> {
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

    let resp = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {} failed", url))?
        .error_for_status()
        .with_context(|| format!("GET {} returned error status", url))?;

    let detail: SymbolDetailResp = resp
        .json()
        .await
        .with_context(|| format!("decode symbol detail failed: {}", url))?;

    Ok(detail.factors)
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

fn render_model_onnx_path(model_name: &str, symbol: &str) -> Result<String> {
    let trimmed_model = model_name.trim();
    if trimmed_model.is_empty() {
        anyhow::bail!("model_name must not be empty");
    }
    let trimmed_symbol = symbol.trim();
    if trimmed_symbol.is_empty() {
        anyhow::bail!("symbol must not be empty");
    }

    let rendered = MODEL_ONNX_PATH_TEMPLATE
        .replace("{model_name}", &urlencoding::encode(trimmed_model))
        .replace("{symbol}", &urlencoding::encode(trimmed_symbol));
    if rendered.trim().is_empty() {
        anyhow::bail!("model ONNX path renders to empty value");
    }
    Ok(rendered)
}

fn parse_feature_dim_header_name() -> Result<HeaderName> {
    HeaderName::from_bytes(MODEL_ONNX_FEATURE_DIM_HEADER.as_bytes()).with_context(|| {
        format!(
            "invalid feature dim header name: {}",
            MODEL_ONNX_FEATURE_DIM_HEADER
        )
    })
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
    model_name: &str,
    symbol: &str,
    model_onnx: &[u8],
) -> Result<PathBuf> {
    if model_onnx.is_empty() {
        anyhow::bail!("model_onnx must not be empty");
    }

    let root = Path::new(MODEL_ONNX_CACHE_DIR);
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
