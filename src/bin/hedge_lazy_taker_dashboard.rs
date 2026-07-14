use anyhow::{Context, Result};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};
use clap::Parser;
use csv::{ReaderBuilder, StringRecord};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::net::SocketAddr;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const INDEX_HTML: &str = include_str!("../hedge_lazy_taker_dashboard.html");

#[derive(Debug, Parser)]
#[command(name = "hedge_lazy_taker_dashboard")]
#[command(about = "Dashboard for hedge_lazy_taker_eval events.csv output")]
struct Args {
    #[arg(long, default_value = "data/hedge_lazy_taker_eval/events.csv")]
    events_csv: PathBuf,

    #[arg(long, default_value = "0.0.0.0")]
    bind: String,

    #[arg(long, default_value_t = 8821)]
    port: u16,

    #[arg(long, default_value_t = 1000)]
    refresh_ms: u64,

    #[arg(long, default_value_t = 200_000)]
    max_points: usize,
}

#[derive(Debug, Clone)]
struct EventRow {
    take_tp_us: i64,
    hold_us: i64,
    symbol: String,
    model_name: String,
    direction: i8,
    category: String,
    hold_count: usize,
    return_rate: Option<f64>,
    position: f64,
    pnl: Option<f64>,
    status: String,
}

#[derive(Debug, Clone, Default)]
struct Accumulator {
    events: u64,
    held_events: u64,
    no_hold_events: u64,
    evaluated: u64,
    wins: u64,
    losses: u64,
    flat: u64,
    missing_bbo: u64,
    cumulative_pnl: f64,
    hold_total_us: i128,
}

impl Accumulator {
    fn add(&mut self, event: &EventRow) {
        self.events += 1;
        if event.category == "held" {
            self.held_events += 1;
            self.hold_total_us += i128::from(event.hold_us.max(0));
            if event.status == "missing_bbo" {
                self.missing_bbo += 1;
                return;
            }
            if let Some(pnl) = event.pnl.filter(|value| value.is_finite()) {
                self.evaluated += 1;
                self.cumulative_pnl += pnl;
                if pnl > 1e-12 {
                    self.wins += 1;
                } else if pnl < -1e-12 {
                    self.losses += 1;
                } else {
                    self.flat += 1;
                }
            }
        } else {
            self.no_hold_events += 1;
        }
    }

    fn snapshot(&self) -> EffectStats {
        let decided = self.wins + self.losses;
        EffectStats {
            events: self.events,
            held_events: self.held_events,
            no_hold_events: self.no_hold_events,
            evaluated: self.evaluated,
            wins: self.wins,
            losses: self.losses,
            flat: self.flat,
            missing_bbo: self.missing_bbo,
            cumulative_pnl: self.cumulative_pnl,
            average_pnl: if self.evaluated == 0 {
                0.0
            } else {
                self.cumulative_pnl / self.evaluated as f64
            },
            win_rate: if decided == 0 {
                0.0
            } else {
                self.wins as f64 / decided as f64
            },
            average_hold_ms: if self.held_events == 0 {
                0.0
            } else {
                self.hold_total_us as f64 / self.held_events as f64 / 1000.0
            },
        }
    }
}

#[derive(Debug, Clone)]
struct RawPoint {
    take_tp_us: i64,
    hold_us: i64,
    symbol: String,
    direction: i8,
    hold_count: usize,
    return_rate: f64,
    position: f64,
    pnl: f64,
}

#[derive(Debug, Clone, Default)]
struct ModelData {
    stats: Accumulator,
    symbols: BTreeMap<String, Accumulator>,
    points: VecDeque<RawPoint>,
}

#[derive(Debug)]
struct DataStore {
    source: PathBuf,
    max_points: usize,
    models: BTreeMap<String, ModelData>,
    rows_loaded: u64,
    parse_errors: u64,
    file_size: u64,
    available: bool,
    last_event_tp_us: Option<i64>,
    last_reload_ms: i64,
}

impl DataStore {
    fn new(source: PathBuf, max_points: usize) -> Self {
        Self {
            source,
            max_points: max_points.max(1),
            models: BTreeMap::new(),
            rows_loaded: 0,
            parse_errors: 0,
            file_size: 0,
            available: false,
            last_event_tp_us: None,
            last_reload_ms: now_ms(),
        }
    }

    fn reset(&mut self) {
        self.models.clear();
        self.rows_loaded = 0;
        self.parse_errors = 0;
        self.file_size = 0;
        self.last_event_tp_us = None;
        self.last_reload_ms = now_ms();
    }

    fn add_event(&mut self, event: EventRow) {
        let model = self.models.entry(event.model_name.clone()).or_default();
        model.stats.add(&event);
        model
            .symbols
            .entry(event.symbol.clone())
            .or_default()
            .add(&event);
        if event.category == "held" {
            if let Some(pnl) = event.pnl.filter(|value| value.is_finite()) {
                model.points.push_back(RawPoint {
                    take_tp_us: event.take_tp_us,
                    hold_us: event.hold_us,
                    symbol: event.symbol,
                    direction: event.direction,
                    hold_count: event.hold_count,
                    return_rate: event.return_rate.unwrap_or_default(),
                    position: event.position,
                    pnl,
                });
                while model.points.len() > self.max_points {
                    model.points.pop_front();
                }
            }
        }
        self.rows_loaded += 1;
        self.last_event_tp_us = Some(
            self.last_event_tp_us
                .map_or(event.take_tp_us, |current| current.max(event.take_tp_us)),
        );
        self.last_reload_ms = now_ms();
    }
}

#[derive(Clone)]
struct AppState {
    data: Arc<RwLock<DataStore>>,
}

#[derive(Debug, Clone, Serialize)]
struct EffectStats {
    events: u64,
    held_events: u64,
    no_hold_events: u64,
    evaluated: u64,
    wins: u64,
    losses: u64,
    flat: u64,
    missing_bbo: u64,
    cumulative_pnl: f64,
    average_pnl: f64,
    win_rate: f64,
    average_hold_ms: f64,
}

#[derive(Debug, Serialize)]
struct SourceStatus {
    path: String,
    available: bool,
    file_size: u64,
    rows_loaded: u64,
    parse_errors: u64,
    last_event_tp_us: Option<i64>,
    last_reload_ms: i64,
}

#[derive(Debug, Serialize)]
struct ModelSummary {
    name: String,
    symbol_count: usize,
    stats: EffectStats,
}

#[derive(Debug, Serialize)]
struct OverviewResponse {
    source: SourceStatus,
    models: Vec<ModelSummary>,
}

#[derive(Debug, Deserialize)]
struct ModelQuery {
    model: String,
    symbol: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct ChartPoint {
    take_tp_us: i64,
    symbol: String,
    direction: i8,
    hold_ms: f64,
    hold_count: usize,
    return_rate: f64,
    position: f64,
    pnl: f64,
    cumulative_pnl: f64,
}

#[derive(Debug, Serialize)]
struct SymbolSummary {
    symbol: String,
    stats: EffectStats,
}

#[derive(Debug, Serialize)]
struct ModelResponse {
    model: String,
    selected_symbol: Option<String>,
    stats: EffectStats,
    points: Vec<ChartPoint>,
    symbols: Vec<SymbolSummary>,
}

type ApiError = (StatusCode, Json<serde_json::Value>);

struct HeaderIndex {
    take_tp_us: usize,
    hold_us: usize,
    symbol: usize,
    model_name: usize,
    direction: usize,
    category: usize,
    hold_count: usize,
    return_rate: usize,
    position: usize,
    pnl: usize,
    status: usize,
}

impl HeaderIndex {
    fn from_record(record: &StringRecord) -> Result<Self> {
        let index = |name: &str| {
            record
                .iter()
                .position(|field| field.trim() == name)
                .with_context(|| format!("CSV header is missing required column '{name}'"))
        };
        Ok(Self {
            take_tp_us: index("take_tp_us")?,
            hold_us: index("hold_us")?,
            symbol: index("symbol")?,
            model_name: index("model_name")?,
            direction: index("direction")?,
            category: index("category")?,
            hold_count: index("hold_count")?,
            return_rate: index("return_rate")?,
            position: index("position")?,
            pnl: index("pnl")?,
            status: index("status")?,
        })
    }
}

struct CsvTailer {
    source: PathBuf,
    data: Arc<RwLock<DataStore>>,
    offset: u64,
    file_id: Option<u64>,
    pending: Vec<u8>,
    header: Option<HeaderIndex>,
}

impl CsvTailer {
    fn new(source: PathBuf, data: Arc<RwLock<DataStore>>) -> Self {
        Self {
            source,
            data,
            offset: 0,
            file_id: None,
            pending: Vec::new(),
            header: None,
        }
    }

    fn poll(&mut self) -> Result<()> {
        let metadata = match std::fs::metadata(&self.source) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                self.data.write().available = false;
                return Ok(());
            }
            Err(err) => return Err(err).context("read CSV metadata failed"),
        };
        let current_file_id = metadata.ino();
        if self.file_id.is_some_and(|id| id != current_file_id) || metadata.len() < self.offset {
            self.reset();
        }
        self.file_id = Some(current_file_id);
        {
            let mut data = self.data.write();
            data.available = true;
            data.file_size = metadata.len();
        }
        if metadata.len() == self.offset {
            return Ok(());
        }

        let mut file = File::open(&self.source)
            .with_context(|| format!("open CSV failed: {}", self.source.display()))?;
        file.seek(SeekFrom::Start(self.offset))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        self.offset += bytes.len() as u64;
        self.pending.extend_from_slice(&bytes);
        self.process_complete_lines()
    }

    fn reset(&mut self) {
        self.offset = 0;
        self.pending.clear();
        self.header = None;
        self.data.write().reset();
    }

    fn process_complete_lines(&mut self) -> Result<()> {
        while let Some(record_end) = complete_csv_record_end(&self.pending) {
            let rest = self.pending.split_off(record_end);
            let mut line = std::mem::replace(&mut self.pending, rest);
            while matches!(line.last(), Some(b'\n' | b'\r')) {
                line.pop();
            }
            if line.is_empty() {
                continue;
            }
            if self.header.is_none() {
                let record = parse_csv_record(&line)?;
                self.header = Some(HeaderIndex::from_record(&record)?);
                continue;
            }
            let result = parse_csv_record(&line)
                .and_then(|record| parse_event(&record, self.header.as_ref().unwrap()));
            match result {
                Ok(event) => self.data.write().add_event(event),
                Err(err) => {
                    self.data.write().parse_errors += 1;
                    log::warn!("skip malformed hedge lazy taker CSV row: {err:#}");
                }
            }
        }
        Ok(())
    }
}

fn complete_csv_record_end(bytes: &[u8]) -> Option<usize> {
    let mut in_quotes = false;
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'"' if in_quotes && bytes.get(index + 1) == Some(&b'"') => {
                index += 2;
                continue;
            }
            b'"' => in_quotes = !in_quotes,
            b'\n' if !in_quotes => return Some(index + 1),
            _ => {}
        }
        index += 1;
    }
    None
}

fn parse_csv_record(line: &[u8]) -> Result<StringRecord> {
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(line);
    reader
        .records()
        .next()
        .context("empty CSV record")?
        .context("parse CSV record failed")
}

fn parse_event(record: &StringRecord, header: &HeaderIndex) -> Result<EventRow> {
    let field = |index: usize, name: &str| {
        record
            .get(index)
            .with_context(|| format!("CSV row is missing '{name}'"))
    };
    let take_tp_us = field(header.take_tp_us, "take_tp_us")?
        .parse()
        .context("invalid take_tp_us")?;
    let hold_us = field(header.hold_us, "hold_us")?
        .parse()
        .context("invalid hold_us")?;
    let direction = field(header.direction, "direction")?
        .parse()
        .context("invalid direction")?;
    let hold_count = field(header.hold_count, "hold_count")?
        .parse()
        .context("invalid hold_count")?;
    let return_rate_text = field(header.return_rate, "return_rate")?.trim();
    let return_rate = if return_rate_text.is_empty() {
        None
    } else {
        Some(return_rate_text.parse().context("invalid return_rate")?)
    };
    let position = field(header.position, "position")?
        .parse()
        .context("invalid position")?;
    let pnl_text = field(header.pnl, "pnl")?.trim();
    let pnl = if pnl_text.is_empty() {
        None
    } else {
        Some(pnl_text.parse().context("invalid pnl")?)
    };
    let symbol = field(header.symbol, "symbol")?.trim().to_ascii_uppercase();
    let model_name = field(header.model_name, "model_name")?.trim().to_string();
    anyhow::ensure!(!symbol.is_empty(), "empty symbol");
    anyhow::ensure!(!model_name.is_empty(), "empty model_name");
    Ok(EventRow {
        take_tp_us,
        hold_us,
        symbol,
        model_name,
        direction,
        category: field(header.category, "category")?.trim().to_string(),
        hold_count,
        return_rate,
        position,
        pnl,
        status: field(header.status, "status")?.trim().to_string(),
    })
}

async fn overview(State(state): State<AppState>) -> Json<OverviewResponse> {
    let data = state.data.read();
    Json(OverviewResponse {
        source: SourceStatus {
            path: data.source.display().to_string(),
            available: data.available,
            file_size: data.file_size,
            rows_loaded: data.rows_loaded,
            parse_errors: data.parse_errors,
            last_event_tp_us: data.last_event_tp_us,
            last_reload_ms: data.last_reload_ms,
        },
        models: data
            .models
            .iter()
            .map(|(name, model)| ModelSummary {
                name: name.clone(),
                symbol_count: model.symbols.len(),
                stats: model.stats.snapshot(),
            })
            .collect(),
    })
}

async fn model_data(
    State(state): State<AppState>,
    Query(query): Query<ModelQuery>,
) -> Result<Json<ModelResponse>, ApiError> {
    let data = state.data.read();
    let model = data.models.get(&query.model).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "unknown model"})),
        )
    })?;
    let selected_symbol = query
        .symbol
        .as_deref()
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .map(str::to_ascii_uppercase);
    let stats = match &selected_symbol {
        Some(symbol) => model.symbols.get(symbol).ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "unknown symbol for model"})),
            )
        })?,
        None => &model.stats,
    }
    .snapshot();

    let limit = query.limit.unwrap_or(4000).clamp(100, 20_000);
    let filtered = model
        .points
        .iter()
        .filter(|point| {
            selected_symbol
                .as_ref()
                .is_none_or(|symbol| point.symbol == *symbol)
        })
        .collect::<Vec<_>>();
    let start = filtered.len().saturating_sub(limit);
    let mut cumulative_pnl = filtered[..start].iter().map(|point| point.pnl).sum();
    let points = filtered[start..]
        .iter()
        .map(|point| {
            cumulative_pnl += point.pnl;
            ChartPoint {
                take_tp_us: point.take_tp_us,
                symbol: point.symbol.clone(),
                direction: point.direction,
                hold_ms: point.hold_us as f64 / 1000.0,
                hold_count: point.hold_count,
                return_rate: point.return_rate,
                position: point.position,
                pnl: point.pnl,
                cumulative_pnl,
            }
        })
        .collect();
    let symbols = model
        .symbols
        .iter()
        .map(|(symbol, stats)| SymbolSummary {
            symbol: symbol.clone(),
            stats: stats.snapshot(),
        })
        .collect();

    Ok(Json(ModelResponse {
        model: query.model,
        selected_symbol,
        stats,
        points,
        symbols,
    }))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn spawn_csv_watcher(
    source: PathBuf,
    data: Arc<RwLock<DataStore>>,
    refresh_ms: u64,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut tailer = CsvTailer::new(source, data);
        while !stop.load(Ordering::Relaxed) {
            if let Err(err) = tailer.poll() {
                log::error!("hedge lazy taker CSV refresh failed: {err:#}");
            }
            thread::sleep(Duration::from_millis(refresh_ms.max(100)));
        }
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    anyhow::ensure!(args.refresh_ms > 0, "refresh_ms must be > 0");
    anyhow::ensure!(args.max_points > 0, "max_points must be > 0");

    let source = absolute_path(&args.events_csv)?;
    let data = Arc::new(RwLock::new(DataStore::new(source.clone(), args.max_points)));
    let stop = Arc::new(AtomicBool::new(false));
    let watcher = spawn_csv_watcher(source.clone(), data.clone(), args.refresh_ms, stop.clone());
    let state = AppState { data };
    let app = Router::new()
        .route("/", get(|| async { Html(INDEX_HTML) }))
        .route(
            "/healthz",
            get(|| async { Json(serde_json::json!({"ok": true, "ts_ms": now_ms()})) }),
        )
        .route("/api/overview", get(overview))
        .route("/api/model", get(model_data))
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", args.bind, args.port)
        .parse()
        .context("invalid bind address")?;
    log::info!(
        "hedge_lazy_taker_dashboard listening at http://{} source={}",
        addr,
        source.display()
    );
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    stop.store(true, Ordering::Relaxed);
    let _ = watcher.join();
    Ok(())
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &[u8] = b"take_tp_us,direct_tp_us,hold_us,symbol,model_name,venue,direction,category,direct_target_us,lazy_target_us,direct_price,lazy_price,hold_count,return_rate,position,pnl,status";

    #[test]
    fn parses_named_columns_and_quoted_model() {
        let header_record = parse_csv_record(HEADER).unwrap();
        let header = HeaderIndex::from_record(&header_record).unwrap();
        let row = parse_csv_record(
            b"100,90,10,BTCUSDT,\"model,one\",2,-1,held,92,102,10,9.5,2,0.25,2,0.5,ok",
        )
        .unwrap();
        let event = parse_event(&row, &header).unwrap();
        assert_eq!(event.model_name, "model,one");
        assert_eq!(event.symbol, "BTCUSDT");
        assert_eq!(event.hold_count, 2);
        assert_eq!(event.return_rate, Some(0.25));
        assert_eq!(event.position, 2.0);
        assert_eq!(event.pnl, Some(0.5));
    }

    #[test]
    fn finds_newline_after_complete_quoted_record() {
        let bytes = b"1,\"model\nname\",ok\nnext";
        assert_eq!(complete_csv_record_end(bytes), Some(18));
    }

    #[test]
    fn aggregates_models_and_symbols_separately() {
        let mut store = DataStore::new(PathBuf::from("events.csv"), 10);
        for (model, symbol, pnl) in [
            ("a", "BTCUSDT", 1.0),
            ("a", "ETHUSDT", -0.5),
            ("b", "BTCUSDT", 3.0),
        ] {
            store.add_event(EventRow {
                take_tp_us: 1,
                hold_us: 2_000,
                symbol: symbol.to_string(),
                model_name: model.to_string(),
                direction: 1,
                category: "held".to_string(),
                hold_count: 1,
                return_rate: Some(pnl),
                position: 1.0,
                pnl: Some(pnl),
                status: "ok".to_string(),
            });
        }
        assert_eq!(store.models["a"].symbols.len(), 2);
        assert_eq!(store.models["a"].stats.snapshot().cumulative_pnl, 0.5);
        assert_eq!(store.models["b"].stats.snapshot().cumulative_pnl, 3.0);
    }
}
