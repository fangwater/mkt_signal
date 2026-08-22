//! Aggregate tree-model IPC and NN-model ZMQ predictions into sampled CSV files.

use anyhow::{bail, Context, Result};
use axum::extract::{Query, State};
use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};
use chrono::{TimeZone, Utc};
use clap::Parser;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use ipc_common::iceoryx_subscriber::{ChannelType, MultiChannelSubscriber, SubscribeParams};
use mkt_parsers::msg::mkt_msg::{AskBidSpreadMsg, ModelMsg, MODEL_STATUS_OK};
use mkt_parsers::msg::model_ipc::MODEL_PAYLOAD_MAX_BYTES;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const HISTORY_SIZE: usize = 128;
const SUBSCRIBER_BUFFER_SIZE: usize = 256;
const IPC_RECONNECT_INTERVAL: Duration = Duration::from_secs(5);
const ZMQ_RECONNECT_INTERVAL: Duration = Duration::from_secs(3);
const CSV_HEADER: &str = "timestamp_ms,prediction,mid_price\n";
const DASHBOARD_HTML: &str = r#"<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script><style>body{margin:0;background:#f4f6f8;color:#17212b;font:14px Arial,sans-serif}header{height:52px;background:#fff;border-bottom:1px solid #d8dee5;display:flex;align-items:center;padding:0 20px;gap:16px}select{height:30px;border:1px solid #b9c5d1;background:#fff;padding:0 8px;color:#17212b}.label{color:#526170;font-size:12px}#chart{height:calc(100vh - 53px);width:100%}</style></head><body><header><span class="label">Model</span><select id="model"></select><span class="label">Symbol</span><select id="symbol"></select><span class="label" id="status"></span></header><div id="chart"></div><script>const model=document.querySelector('#model'),symbol=document.querySelector('#symbol'),status=document.querySelector('#status'),chart=echarts.init(document.querySelector('#chart'));let options={};function fill(el,items){el.replaceChildren(...items.map(x=>{const o=document.createElement('option');o.value=x;o.textContent=x;return o}))}async function loadSeries(){if(!model.value||!symbol.value)return;const d=await(await fetch(`/api/series?model=${encodeURIComponent(model.value)}&symbol=${encodeURIComponent(symbol.value)}`)).json(),p=d.points||[];chart.setOption({animation:false,grid:{left:64,right:72,top:28,bottom:42},tooltip:{trigger:'axis'},xAxis:{type:'time'},yAxis:[{type:'value',name:'Model'},{type:'value',name:'Mid',position:'right',scale:true}],series:[{name:'Model',type:'line',showSymbol:false,data:p.map(x=>[x.timestamp_ms,x.value]),lineStyle:{width:1.5,color:'#1769aa'}},{name:'Mid',type:'line',yAxisIndex:1,showSymbol:false,connectNulls:false,data:p.map(x=>[x.timestamp_ms,x.mid_price]),lineStyle:{width:1.2,color:'#c55a11'}}]});status.textContent=`${p.length} points`}async function loadOptions(){const d=await(await fetch('/api/options')).json();options=d.symbols||{};fill(model,d.models||[]);fill(symbol,options[model.value]||[]);await loadSeries()}model.addEventListener('change',async()=>{fill(symbol,options[model.value]||[]);await loadSeries()});symbol.addEventListener('change',loadSeries);window.addEventListener('resize',()=>chart.resize());loadOptions();setInterval(loadSeries,5000);</script></body></html>"#;

const BINANCE_FUTURES_VENUE: &str = "binance-futures";

#[derive(Debug, Parser)]
#[command(name = "predict_file")]
#[command(about = "Sample model predictions from Iceoryx/ZMQ and write UTC CSV files")]
struct Args {
    #[arg(long)]
    config: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
struct Config {
    instance: String,
    out_dir: PathBuf,
    flush_every: u64,
    flush_interval_ms: u64,
    poll_interval_ms: u64,
    max_drain_per_source: usize,
    stats_secs: u64,
    cache_minutes: u64,
    cache_cleanup_ms: u64,
    http_bind: String,
    http_port: u16,
    models: Vec<ModelConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            instance: "default".to_string(),
            out_dir: PathBuf::from("predict_file"),
            flush_every: 256,
            flush_interval_ms: 1_000,
            poll_interval_ms: 10,
            max_drain_per_source: 256,
            stats_secs: 60,
            cache_minutes: 30,
            cache_cleanup_ms: 30_000,
            http_bind: "127.0.0.1".to_string(),
            http_port: 8818,
            models: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default)]
struct ModelConfig {
    #[serde(alias = "name")]
    model_name: String,
    group: String,
    source: String,
    service: Option<String>,
    endpoint: Option<String>,
    topic: String,
    interval_ms: u64,
    max_subscribers: usize,
}

impl Default for ModelConfig {
    fn default() -> Self {
        Self {
            model_name: String::new(),
            group: String::new(),
            source: "ipc".to_string(),
            service: None,
            endpoint: None,
            topic: "model_output/".to_string(),
            interval_ms: 1_000,
            max_subscribers: 10,
        }
    }
}

impl Config {
    fn load(path: &Path) -> Result<Self> {
        let text =
            fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
        let cfg: Self =
            toml::from_str(&text).with_context(|| format!("parse config {}", path.display()))?;
        cfg.validate(path)?;
        Ok(cfg)
    }

    fn validate(&self, path: &Path) -> Result<()> {
        if self.instance.trim().is_empty() || self.out_dir.as_os_str().is_empty() {
            bail!(
                "config {} requires non-empty instance and out_dir",
                path.display()
            );
        }
        if self.flush_interval_ms == 0
            || self.poll_interval_ms == 0
            || self.max_drain_per_source == 0
            || self.cache_minutes == 0
            || self.cache_cleanup_ms == 0
        {
            bail!(
                "config {} has an invalid positive interval setting",
                path.display()
            );
        }
        let _: SocketAddr = format!("{}:{}", self.http_bind.trim(), self.http_port)
            .parse()
            .with_context(|| format!("config {} has invalid HTTP bind/port", path.display()))?;
        if self.http_port == 0 {
            bail!("config {} has HTTP port 0", path.display());
        }
        if self.models.is_empty() {
            bail!("config {} has no [[models]] entries", path.display());
        }
        let mut names = HashSet::new();
        for model in &self.models {
            let name = model.model_name.trim();
            if name.is_empty() || safe_component(name).is_empty() || !names.insert(name) {
                bail!(
                    "config {} has an empty, unsafe, or duplicate model_name: {name:?}",
                    path.display()
                );
            }
            let group = model.group.trim();
            if group.is_empty() || safe_component(group).is_empty() {
                bail!(
                    "config {} model {name} requires a non-empty, safe group",
                    path.display()
                );
            }
            if model.interval_ms == 0 {
                bail!("config {} model {name} has interval_ms=0", path.display());
            }
            if model.max_subscribers == 0 {
                bail!(
                    "config {} model {name} has max_subscribers=0",
                    path.display()
                );
            }
            match model.source.trim().to_ascii_lowercase().as_str() {
                "ipc" if normalize_service(model.service.as_deref()).is_some() => {}
                "ipc" => bail!(
                    "config {} IPC model {name} requires service",
                    path.display()
                ),
                "zmq"
                    if model
                        .endpoint
                        .as_deref()
                        .is_some_and(|value| !value.trim().is_empty()) => {}
                "zmq" => bail!(
                    "config {} ZMQ model {name} requires endpoint",
                    path.display()
                ),
                other => bail!(
                    "config {} model {name} source must be ipc or zmq, got {other:?}",
                    path.display()
                ),
            }
        }
        Ok(())
    }
}

enum SubscriberKind {
    Ipc(Subscriber<ipc::Service, [u8; MODEL_PAYLOAD_MAX_BYTES], ()>),
    Zmq {
        _context: zmq::Context,
        socket: zmq::Socket,
        topic_prefix: String,
    },
}

enum SubscriberConfig {
    Ipc {
        service_name: String,
        max_subscribers: usize,
    },
    Zmq {
        endpoint: String,
        topic_prefix: String,
    },
}

impl SubscriberConfig {
    fn reconnect_interval(&self) -> Duration {
        match self {
            Self::Ipc { .. } => IPC_RECONNECT_INTERVAL,
            Self::Zmq { .. } => ZMQ_RECONNECT_INTERVAL,
        }
    }

    fn connect(&self, node: &Node<ipc::Service>, model_name: &str) -> Result<SubscriberKind> {
        match self {
            Self::Ipc {
                service_name,
                max_subscribers,
            } => {
                let service = node
                    .service_builder(&ServiceName::new(service_name)?)
                    .publish_subscribe::<[u8; MODEL_PAYLOAD_MAX_BYTES]>()
                    .max_publishers(1)
                    .max_subscribers(*max_subscribers)
                    .history_size(HISTORY_SIZE)
                    .subscriber_max_buffer_size(SUBSCRIBER_BUFFER_SIZE)
                    .open_or_create()
                    .with_context(|| format!("open IPC service {service_name}"))?;
                let subscriber = service
                    .subscriber_builder()
                    .buffer_size(SUBSCRIBER_BUFFER_SIZE)
                    .create()
                    .with_context(|| format!("create IPC subscriber {service_name}"))?;
                log::info!(
                    "predict_file subscribe model={model_name} source=ipc service={service_name}"
                );
                Ok(SubscriberKind::Ipc(subscriber))
            }
            Self::Zmq {
                endpoint,
                topic_prefix,
            } => {
                let context = zmq::Context::new();
                let socket = context.socket(zmq::SUB).context("create ZMQ SUB socket")?;
                socket.set_linger(0).context("set ZMQ linger")?;
                socket
                    .set_rcvhwm(SUBSCRIBER_BUFFER_SIZE as i32)
                    .context("set ZMQ receive HWM")?;
                socket
                    .set_reconnect_ivl(ZMQ_RECONNECT_INTERVAL.as_millis() as i32)
                    .context("set ZMQ reconnect interval")?;
                socket
                    .set_subscribe(topic_prefix.as_bytes())
                    .with_context(|| format!("subscribe ZMQ topic {topic_prefix:?}"))?;
                socket
                    .connect(endpoint)
                    .with_context(|| format!("connect ZMQ endpoint {endpoint}"))?;
                log::info!(
                    "predict_file subscribe model={model_name} source=zmq endpoint={endpoint} topic={topic_prefix:?} reconnect_interval_secs={}",
                    ZMQ_RECONNECT_INTERVAL.as_secs()
                );
                Ok(SubscriberKind::Zmq {
                    _context: context,
                    socket,
                    topic_prefix: topic_prefix.clone(),
                })
            }
        }
    }
}

impl SubscriberKind {
    fn try_receive(&self) -> Result<Option<ModelMsg>> {
        match self {
            Self::Ipc(subscriber) => match subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = sample.payload();
                    if payload.iter().all(|byte| *byte == 0) {
                        return Ok(None);
                    }
                    ModelMsg::from_bytes(payload)
                        .context("decode IPC ModelMsg")
                        .map(Some)
                }
                Ok(None) => Ok(None),
                Err(err) => Err(err).context("receive IPC ModelMsg"),
            },
            Self::Zmq {
                socket,
                topic_prefix,
                ..
            } => {
                let frames = match socket.recv_multipart(zmq::DONTWAIT) {
                    Ok(frames) => frames,
                    Err(zmq::Error::EAGAIN) => return Ok(None),
                    Err(err) => return Err(err).context("receive ZMQ ModelMsg"),
                };
                if frames.len() != 2 {
                    bail!("ZMQ ModelMsg expected 2 frames, got {}", frames.len());
                }
                let topic = std::str::from_utf8(&frames[0]).context("ZMQ topic is not UTF-8")?;
                let msg = ModelMsg::from_bytes(&frames[1]).context("decode ZMQ ModelMsg")?;
                let expected_topic = format!("{topic_prefix}{}", msg.symbol);
                if topic != expected_topic {
                    bail!("ZMQ topic/symbol mismatch: topic={topic:?} expected={expected_topic:?}");
                }
                Ok(Some(msg))
            }
        }
    }
}

struct Source {
    model_name: String,
    group: String,
    subscription: SubscriberConfig,
    subscriber: Option<SubscriberKind>,
    reconnect_at: Option<Instant>,
    interval: Duration,
    last_written: HashMap<String, Instant>,
    received: u64,
    persisted: u64,
    skipped: u64,
}

impl Source {
    fn reconnect_if_due(&mut self, node: &Node<ipc::Service>) {
        if self
            .reconnect_at
            .is_some_and(|reconnect_at| Instant::now() < reconnect_at)
        {
            return;
        }

        let reconnect_interval = self.subscription.reconnect_interval();
        match self.subscription.connect(node, &self.model_name) {
            Ok(subscriber) => {
                self.subscriber = Some(subscriber);
                self.reconnect_at = None;
                log::info!("predict_file reconnected model={}", self.model_name);
            }
            Err(err) => {
                self.reconnect_at = Some(Instant::now() + reconnect_interval);
                log::warn!(
                    "predict_file reconnect failed model={}, retry_in_secs={}: {err:#}",
                    self.model_name,
                    reconnect_interval.as_secs()
                );
            }
        }
    }

    fn schedule_reconnect(&mut self, err: &anyhow::Error) {
        let reconnect_interval = self.subscription.reconnect_interval();
        self.subscriber = None;
        self.reconnect_at = Some(Instant::now() + reconnect_interval);
        log::warn!(
            "predict_file receive error model={}, reconnect_in_secs={}: {err:#}",
            self.model_name,
            reconnect_interval.as_secs()
        );
    }
}

#[derive(Clone, Serialize)]
struct CachePoint {
    timestamp_ms: i64,
    value: f64,
    mid_price: Option<f64>,
}

#[derive(Clone)]
struct MidSnapshot {
    timestamp_ms: i64,
    price: f64,
}

struct CacheStore {
    ttl_ms: i64,
    points: HashMap<(String, String), VecDeque<CachePoint>>,
    tracked_symbols: HashSet<String>,
    latest_mid: HashMap<String, MidSnapshot>,
}

impl CacheStore {
    fn new(cache_minutes: u64) -> Self {
        Self {
            ttl_ms: i64::try_from(cache_minutes.saturating_mul(60_000)).unwrap_or(i64::MAX),
            points: HashMap::new(),
            tracked_symbols: HashSet::new(),
            latest_mid: HashMap::new(),
        }
    }

    fn record(&mut self, model: &str, symbol: &str, timestamp_ms: i64, value: f64) -> Option<f64> {
        self.tracked_symbols.insert(symbol.to_string());
        let cutoff = now_ms().saturating_sub(self.ttl_ms);
        let mid_price = self
            .latest_mid
            .get(symbol)
            .filter(|snapshot| snapshot.timestamp_ms >= cutoff)
            .map(|snapshot| snapshot.price);
        self.points
            .entry((model.to_string(), symbol.to_string()))
            .or_default()
            .push_back(CachePoint {
                timestamp_ms,
                value,
                mid_price,
            });
        mid_price
    }

    fn observe_mid(&mut self, symbol: &str, timestamp_ms: i64, bid: f64, ask: f64) {
        if !self.tracked_symbols.contains(symbol) || !bid.is_finite() || !ask.is_finite() {
            return;
        }
        let price = (bid + ask) * 0.5;
        if price.is_finite() && price > 0.0 {
            self.latest_mid.insert(
                symbol.to_string(),
                MidSnapshot {
                    timestamp_ms,
                    price,
                },
            );
        }
    }

    fn cleanup(&mut self, now_ms: i64) {
        let cutoff = now_ms.saturating_sub(self.ttl_ms);
        self.points.retain(|_, queue| {
            while queue
                .front()
                .is_some_and(|point| point.timestamp_ms < cutoff)
            {
                queue.pop_front();
            }
            !queue.is_empty()
        });
        self.tracked_symbols.clear();
        self.tracked_symbols
            .extend(self.points.keys().map(|(_, symbol)| symbol.clone()));
        self.latest_mid.retain(|symbol, snapshot| {
            self.tracked_symbols.contains(symbol) && snapshot.timestamp_ms >= cutoff
        });
    }

    fn options(&self) -> OptionsResponse {
        let mut symbols: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (model, symbol) in self.points.keys() {
            symbols
                .entry(model.clone())
                .or_default()
                .push(symbol.clone());
        }
        for values in symbols.values_mut() {
            values.sort();
            values.dedup();
        }
        let models = symbols.keys().cloned().collect();
        OptionsResponse { models, symbols }
    }

    fn series(&self, model: &str, symbol: &str) -> Vec<CachePoint> {
        self.points
            .get(&(model.to_string(), symbol.to_string()))
            .map(|queue| queue.iter().cloned().collect())
            .unwrap_or_default()
    }
}

#[derive(Clone)]
struct AppState {
    cache: Arc<RwLock<CacheStore>>,
}

#[derive(Serialize)]
struct OptionsResponse {
    models: Vec<String>,
    symbols: BTreeMap<String, Vec<String>>,
}

#[derive(Deserialize)]
struct SeriesQuery {
    model: String,
    symbol: String,
}

#[derive(Serialize)]
struct SeriesResponse {
    model: String,
    symbol: String,
    points: Vec<CachePoint>,
}

fn dashboard_html() -> Html<String> {
    let base_script = "const basePath=location.pathname.endsWith('/')?location.pathname:location.pathname+'/';const apiPath=(path)=>basePath+path;";
    Html(
        DASHBOARD_HTML
            .replace("chart.setOption({animation:false,", "chart.setOption({animation:false,legend:{top:0},")
            .replace("lineStyle:{width:1.2,color:'#c55a11'}", "lineStyle:{width:2,color:'#c55a11'}")
            .replace("const model=", &format!("{base_script}const model="))
            .replace("fetch(`/api/series?", "fetch(`${apiPath('api/series')}?")
            .replace("fetch('/api/options')", "fetch(apiPath('api/options'))")
            .replace(
                "function fill(el,items){el.replaceChildren(...items.map(x=>{",
                "function fill(el,items){const placeholder=el===model?'Select model':'Select symbol';el.replaceChildren(new Option(placeholder,''),...items.map(x=>{",
            ),
    )
}

async fn options_route(State(state): State<AppState>) -> Json<OptionsResponse> {
    Json(state.cache.read().options())
}

async fn series_route(
    State(state): State<AppState>,
    Query(query): Query<SeriesQuery>,
) -> Json<SeriesResponse> {
    let points = state.cache.read().series(&query.model, &query.symbol);
    Json(SeriesResponse {
        model: query.model,
        symbol: query.symbol,
        points,
    })
}

fn start_http(state: AppState, bind: &str, port: u16) -> Result<()> {
    let addr: SocketAddr = format!("{bind}:{port}")
        .parse()
        .context("parse HTTP address")?;
    let listener =
        std::net::TcpListener::bind(addr).with_context(|| format!("bind HTTP {addr}"))?;
    listener.set_nonblocking(true)?;
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build predict_file HTTP runtime");
        runtime.block_on(async move {
            let listener =
                tokio::net::TcpListener::from_std(listener).expect("adopt HTTP listener");
            let app = Router::new()
                .route("/", get(|| async { dashboard_html() }))
                .route(
                    "/healthz",
                    get(|| async { Json(serde_json::json!({"ok": true})) }),
                )
                .route("/api/options", get(options_route))
                .route("/api/series", get(series_route))
                .with_state(state);
            if let Err(err) = axum::serve(listener, app).await {
                log::error!("predict_file HTTP server stopped: {err}");
            }
        });
    });
    log::info!("predict_file dashboard listening at http://{addr}");
    Ok(())
}

impl Source {
    fn should_persist(&mut self, symbol: &str, now: Instant) -> bool {
        if self
            .last_written
            .get(symbol)
            .is_some_and(|last| now.duration_since(*last) < self.interval)
        {
            self.skipped = self.skipped.saturating_add(1);
            return false;
        }
        self.last_written.insert(symbol.to_string(), now);
        true
    }
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let cfg = Config::load(&args.config)?;
    fs::create_dir_all(&cfg.out_dir)
        .with_context(|| format!("create {}", cfg.out_dir.display()))?;

    let node_name = format!("predict_file_{}", safe_component(&cfg.instance));
    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;
    let mut sources = build_sources(&node, &cfg)?;
    let mut writer = CsvWriter::new(cfg.out_dir.clone());
    let mut last_flush = Instant::now();
    let mut last_stats = Instant::now();
    let cache = Arc::new(RwLock::new(CacheStore::new(cfg.cache_minutes)));
    let app_state = AppState {
        cache: cache.clone(),
    };
    start_http(app_state, &cfg.http_bind, cfg.http_port)?;
    let mut mid_subscriber = MultiChannelSubscriber::new(&format!(
        "predict_file_mid_{}",
        safe_component(&cfg.instance)
    ))?;
    mid_subscriber.subscribe_channels(vec![SubscribeParams {
        service_root: Some("spread_pbs".to_string()),
        topic_prefix: BINANCE_FUTURES_VENUE.to_string(),
        channel: ChannelType::AskBidSpread,
    }])?;
    let mut received = 0u64;
    let mut persisted = 0u64;
    let mut errors = 0u64;

    log::info!(
        "predict_file started config={} instance={} out_dir={} sources={} flush_every={} flush_interval_ms={}",
        args.config.display(), cfg.instance, cfg.out_dir.display(), sources.len(), cfg.flush_every, cfg.flush_interval_ms
    );

    let mut last_cleanup = Instant::now();
    loop {
        let mut had_message = false;
        if poll_mid_prices(&mut mid_subscriber, &cache, cfg.max_drain_per_source) > 0 {
            had_message = true;
        }

        for source in &mut sources {
            if source.subscriber.is_none() {
                source.reconnect_if_due(&node);
                continue;
            }
            for _ in 0..cfg.max_drain_per_source {
                let received_msg = source
                    .subscriber
                    .as_ref()
                    .expect("subscriber checked above")
                    .try_receive();
                let msg = match received_msg {
                    Ok(Some(msg)) => msg,
                    Ok(None) => break,
                    Err(err) => {
                        errors = errors.saturating_add(1);
                        source.schedule_reconnect(&err);
                        break;
                    }
                };
                had_message = true;
                received = received.saturating_add(1);
                source.received = source.received.saturating_add(1);
                if msg.status != MODEL_STATUS_OK
                    || !msg.score.is_finite()
                    || msg.symbol.trim().is_empty()
                {
                    source.skipped = source.skipped.saturating_add(1);
                    continue;
                }
                let symbol = msg.symbol.trim().to_uppercase();
                if !source.should_persist(&symbol, Instant::now()) {
                    continue;
                }
                let timestamp_ms = if msg.ts_out_ms > 0 {
                    msg.ts_out_ms
                } else {
                    Utc::now().timestamp_millis()
                };
                let mid_price =
                    cache
                        .write()
                        .record(&source.model_name, &symbol, timestamp_ms, msg.score);
                writer.write(
                    &source.group,
                    &symbol,
                    &source.model_name,
                    timestamp_ms,
                    msg.score,
                    mid_price,
                )?;
                persisted = persisted.saturating_add(1);
                source.persisted = source.persisted.saturating_add(1);
                if cfg.flush_every > 0 && writer.pending_rows >= cfg.flush_every {
                    writer.flush()?;
                    last_flush = Instant::now();
                }
            }
        }
        if last_flush.elapsed() >= Duration::from_millis(cfg.flush_interval_ms) {
            writer.flush()?;
            last_flush = Instant::now();
        }
        if cfg.stats_secs > 0 && last_stats.elapsed() >= Duration::from_secs(cfg.stats_secs) {
            log::info!(
                "predict_file stats received={} persisted={} errors={} open_csv={} pending_rows={}",
                received,
                persisted,
                errors,
                writer.writers.len(),
                writer.pending_rows
            );
            last_stats = Instant::now();
        }
        if last_cleanup.elapsed() >= Duration::from_millis(cfg.cache_cleanup_ms) {
            cache.write().cleanup(now_ms());
            last_cleanup = Instant::now();
        }
        if !had_message {
            thread::sleep(Duration::from_millis(cfg.poll_interval_ms));
        }
    }
}

fn poll_mid_prices(
    subscriber: &mut MultiChannelSubscriber,
    cache: &Arc<RwLock<CacheStore>>,
    limit: usize,
) -> usize {
    let payloads = subscriber.poll_channel_from(
        "spread_pbs",
        BINANCE_FUTURES_VENUE,
        &ChannelType::AskBidSpread,
        Some(limit),
    );
    let mut accepted = 0usize;
    let mut store = cache.write();
    for payload in payloads {
        if payload.len() < 8 {
            continue;
        }
        let symbol = AskBidSpreadMsg::get_symbol(&payload).to_uppercase();
        let bid = AskBidSpreadMsg::get_bid_price(&payload);
        let ask = AskBidSpreadMsg::get_ask_price(&payload);
        store.observe_mid(&symbol, now_ms(), bid, ask);
        accepted += 1;
    }
    accepted
}

fn build_sources(node: &Node<ipc::Service>, cfg: &Config) -> Result<Vec<Source>> {
    cfg.models
        .iter()
        .map(|model| {
            let model_name = model.model_name.trim().to_string();
            let group = model.group.trim().to_string();
            let subscription = match model.source.trim().to_ascii_lowercase().as_str() {
                "ipc" => SubscriberConfig::Ipc {
                    service_name: normalize_service(model.service.as_deref())
                        .expect("validated IPC service"),
                    max_subscribers: model.max_subscribers,
                },
                "zmq" => SubscriberConfig::Zmq {
                    endpoint: model
                        .endpoint
                        .as_deref()
                        .expect("validated ZMQ endpoint")
                        .trim()
                        .to_string(),
                    topic_prefix: model.topic.clone(),
                },
                _ => unreachable!("validated source"),
            };
            let subscriber = subscription.connect(node, &model_name)?;
            Ok(Source {
                model_name,
                group,
                subscription,
                subscriber: Some(subscriber),
                reconnect_at: None,
                interval: Duration::from_millis(model.interval_ms),
                last_written: HashMap::new(),
                received: 0,
                persisted: 0,
                skipped: 0,
            })
        })
        .collect()
}

struct CsvWriter {
    out_dir: PathBuf,
    writers: HashMap<FileKey, BufWriter<File>>,
    pending_rows: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct FileKey {
    group: String,
    symbol: String,
    date: String,
    model_name: String,
}

impl CsvWriter {
    fn new(out_dir: PathBuf) -> Self {
        Self {
            out_dir,
            writers: HashMap::new(),
            pending_rows: 0,
        }
    }

    fn write(
        &mut self,
        group: &str,
        symbol: &str,
        model_name: &str,
        timestamp_ms: i64,
        prediction: f64,
        mid_price: Option<f64>,
    ) -> Result<()> {
        let date = Utc
            .timestamp_millis_opt(timestamp_ms)
            .single()
            .ok_or_else(|| anyhow::anyhow!("invalid prediction timestamp_ms={timestamp_ms}"))?
            .format("%Y-%m-%d")
            .to_string();
        let key = FileKey {
            group: safe_component(group),
            symbol: safe_component(symbol),
            date,
            model_name: safe_component(model_name),
        };
        if !self.writers.contains_key(&key) {
            let path = self.path(&key);
            fs::create_dir_all(path.parent().expect("CSV has parent"))
                .with_context(|| format!("create CSV directory for {}", path.display()))?;
            let is_new = !path.exists() || path.metadata()?.len() == 0;
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("open CSV {}", path.display()))?;
            let mut writer = BufWriter::new(file);
            if is_new {
                writer.write_all(CSV_HEADER.as_bytes())?;
            }
            log::info!("predict_file writing {}", path.display());
            self.writers.insert(key.clone(), writer);
        }
        let writer = self.writers.get_mut(&key).expect("writer inserted");
        match mid_price {
            Some(mid_price) => writeln!(writer, "{timestamp_ms},{prediction},{mid_price}")?,
            None => writeln!(writer, "{timestamp_ms},{prediction},")?,
        }
        self.pending_rows = self.pending_rows.saturating_add(1);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        for writer in self.writers.values_mut() {
            writer.flush()?;
        }
        self.pending_rows = 0;
        Ok(())
    }

    fn path(&self, key: &FileKey) -> PathBuf {
        self.out_dir
            .join(&key.group)
            .join(&key.symbol)
            .join("data")
            .join(&key.date)
            .join(format!("{}.csv", key.model_name))
    }
}

fn normalize_service(raw: Option<&str>) -> Option<String> {
    let service = raw?.trim();
    if service.is_empty() || service == "-" {
        return None;
    }
    Some(if service.contains('/') {
        service.to_string()
    } else {
        format!("model_output/{service}")
    })
}

fn safe_component(raw: &str) -> String {
    raw.trim()
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => ch,
            _ => '_',
        })
        .collect()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{normalize_service, now_ms, safe_component, Config, CsvWriter};
    use std::{fs, path::Path};

    #[test]
    fn normalizes_bare_ipc_service() {
        assert_eq!(
            normalize_service(Some("direction")),
            Some("model_output/direction".to_string())
        );
    }

    #[test]
    fn sanitizes_csv_path_components() {
        assert_eq!(safe_component("BTC/USDT"), "BTC_USDT");
    }

    #[test]
    fn rejects_config_without_models() {
        let cfg: Config = toml::from_str("instance = 'test'").unwrap();
        assert!(cfg.validate(Path::new("test.toml")).is_err());
    }

    #[test]
    fn csv_writer_groups_by_group_and_writes_mid_price() {
        let out_dir = std::env::temp_dir().join(format!(
            "predict_file_test_{}_{}",
            std::process::id(),
            now_ms()
        ));
        let mut writer = CsvWriter::new(out_dir.clone());
        let timestamp_ms = 1_704_067_200_000;
        writer
            .write(
                "rpan",
                "BTCUSDT",
                "predict_rnn_layer",
                timestamp_ms,
                0.25,
                Some(101.5),
            )
            .unwrap();
        writer
            .write(
                "rpan",
                "BTCUSDT",
                "predict_rnn_layer",
                timestamp_ms + 1,
                0.5,
                None,
            )
            .unwrap();
        writer.flush().unwrap();

        let path = out_dir
            .join("rpan")
            .join("BTCUSDT")
            .join("data")
            .join("2024-01-01")
            .join("predict_rnn_layer.csv");
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            "timestamp_ms,prediction,mid_price\n1704067200000,0.25,101.5\n1704067200001,0.5,\n"
        );
        fs::remove_dir_all(out_dir).unwrap();
    }
}
#[test]
fn cache_attaches_latest_mid_only_to_model_points_and_expires() {
    let mut cache = CacheStore::new(30);
    let now = 1_800_000_000_000_i64;
    cache.record("model", "BTCUSDT", now, 0.1);
    cache.observe_mid("BTCUSDT", now, 100.0, 102.0);
    cache.record("model", "BTCUSDT", now + 1, 0.2);
    let points = cache.series("model", "BTCUSDT");
    assert_eq!(points.len(), 2);
    assert_eq!(points[0].mid_price, None);
    assert_eq!(points[1].mid_price, Some(101.0));

    cache.cleanup(now + 30 * 60_000 + 2);
    assert!(cache.series("model", "BTCUSDT").is_empty());
    assert!(cache.latest_mid.is_empty());
    assert!(cache.tracked_symbols.is_empty());
}
