//! 下架风险查询 HTTP。
//!
//! 官方快照 + 公告 LLM 抽取写入同一本风险簿，全量扁平 JSON 查询。
//! 公告默认 1h 拉取，官方市场/offTime/schedule 默认 3h。
//! 原始公告与拉取/LLM 状态写入 Postgres，失败原因可查。
//!
//! ```text
//! cargo run --bin delist_risk_server -- --bind 0.0.0.0:8787
//! curl 'http://127.0.0.1:8787/v1/risk'
//! curl 'http://127.0.0.1:8787/v1/status'
//! ```

use anyhow::{Context, Result};
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use mkt_signal::common::announcement_llm::{
    extract_for_emit, LlmBudget, LlmConfig, LlmExtractInput,
};
use mkt_signal::common::announcement_watch::{
    http_client as public_http_client, RawAnnouncement, SeenStore,
};
use mkt_signal::common::binance_announcement::{
    backfill_catalog, fetch_margin_delist_snapshot, fetch_monitoring_tag_snapshot,
    fetch_spot_delist_snapshot, http_client as binance_http_client, ParsedAnnouncement, WatchState,
    CATALOG_DELISTING,
};
use mkt_signal::common::bitget_announcement::{fetch_delist_notices, fetch_offtime_snapshot};
use mkt_signal::common::delist_risk::{
    announcement_from_raw, events_from_bitget_offtime, events_from_delist_schedule,
    events_from_gate_snapshot, events_from_official_snapshot, RiskBook, RiskQuery,
};
use mkt_signal::common::delist_schedule::{provider_for_venue, DelistScheduleQuery};
use mkt_signal::common::delist_store::{DelistStore, StatusBook};
use mkt_signal::common::gate_announcement::{
    fetch_market_snapshot, parse_ws_text, ping_frame, subscribe_frame, ANN_WS_URL,
};
use order_common::TradingVenue;
use parking_lot::Mutex;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time;
use tokio_tungstenite::tungstenite::Message;

#[derive(Parser)]
#[command(name = "delist_risk_server")]
#[command(about = "HTTP query for exchange delist / margin / futures risk")]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8787")]
    bind: String,

    #[arg(long, default_value = "data/delist_risk.json")]
    book: PathBuf,

    #[arg(long, default_value_t = 30)]
    days: i64,

    #[arg(long)]
    skip_llm: bool,

    #[arg(long)]
    skip_announcements: bool,

    #[arg(long)]
    skip_official: bool,

    #[arg(long)]
    skip_ws: bool,

    #[arg(long)]
    skip_postgres: bool,

    /// Official market / offTime / schedule snapshot interval. Default 3h.
    #[arg(long, default_value_t = 10_800)]
    official_interval_secs: u64,

    /// Announcement poll interval. Default 1h.
    #[arg(long, default_value_t = 3_600)]
    announcement_interval_secs: u64,

    #[arg(long, default_value_t = 0)]
    llm_max: usize,

    /// Postgres URL. Falls back to DELIST_PG_URL.
    #[arg(long)]
    postgres: Option<String>,
}

#[derive(Clone)]
struct AppState {
    book: Arc<RwLock<RiskBook>>,
    status: Arc<RwLock<StatusBook>>,
    store: Option<Arc<DelistStore>>,
    book_path: PathBuf,
    default_days: i64,
}

#[derive(Debug, Deserialize)]
struct RiskParams {
    venue: Option<String>,
    exchange: Option<String>,
    days: Option<i64>,
    #[serde(default)]
    include_past: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let mut book = RiskBook::load(&args.book).unwrap_or_else(|err| {
        warn!("load risk book failed, start empty: {err:#}");
        RiskBook::default()
    });

    let pg_url = args
        .postgres
        .clone()
        .or_else(|| std::env::var("DELIST_PG_URL").ok())
        .filter(|url| !url.trim().is_empty());
    let store = if args.skip_postgres {
        None
    } else if let Some(url) = pg_url {
        match DelistStore::connect(&url).await {
            Ok(store) => {
                info!("postgres connected");
                Some(store)
            }
            Err(err) => {
                warn!("postgres unavailable, continue without persist: {err:#}");
                None
            }
        }
    } else {
        info!("postgres disabled (no --postgres / DELIST_PG_URL)");
        None
    };

    let mut status = StatusBook::default();
    if let Some(store) = store.as_ref() {
        match store.load_announcements().await {
            Ok(items) => {
                info!("restore announcements from postgres count={}", items.len());
                for item in items {
                    book.remember_announcement(&announcement_from_raw(&item));
                }
            }
            Err(err) => warn!("restore announcements failed: {err:#}"),
        }
        match store.load_sources().await {
            Ok(rows) => status.replace_sources(rows),
            Err(err) => warn!("restore source_status failed: {err:#}"),
        }
        match store.load_llm().await {
            Ok(rows) => status.replace_llm(rows),
            Err(err) => warn!("restore llm_status failed: {err:#}"),
        }
    }

    let state = AppState {
        book: Arc::new(RwLock::new(book)),
        status: Arc::new(RwLock::new(status)),
        store,
        book_path: args.book.clone(),
        default_days: args.days,
    };

    let refresh = state.clone();
    let refresh_args = RefreshArgs {
        skip_llm: args.skip_llm,
        skip_announcements: args.skip_announcements,
        skip_official: args.skip_official,
        skip_ws: args.skip_ws,
        official_interval_secs: args.official_interval_secs,
        announcement_interval_secs: args.announcement_interval_secs,
        days: args.days,
        llm_max: args.llm_max,
    };
    tokio::spawn(async move {
        if let Err(err) = run_refresh(refresh, refresh_args).await {
            warn!("delist refresh loop exited: {err:#}");
        }
    });

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/risk", get(query_risk))
        .route("/v1/venues", get(query_venues))
        .route("/v1/announcements", get(query_announcements))
        .route("/v1/status", get(query_status))
        .with_state(state);

    let addr: SocketAddr = args.bind.parse().context("invalid --bind")?;
    info!(
        "delist_risk_server listening at http://{addr} official={}s announcements={}s",
        args.official_interval_secs, args.announcement_interval_secs
    );
    axum::serve(
        tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("bind {addr} failed"))?,
        app,
    )
    .await
    .context("delist_risk_server exited")
}

async fn healthz(State(state): State<AppState>) -> impl IntoResponse {
    let book = state.book.read().await;
    let snap = state.status.read().await.snapshot(state.store.is_some());
    Json(json!({
        "ok": true,
        "updated_ms": book.updated_ms,
        "events": book.events.len(),
        "announcements": book.announcements.len(),
        "degraded": snap.degraded,
        "postgres": snap.postgres,
    }))
}

async fn query_risk(
    State(state): State<AppState>,
    Query(params): Query<RiskParams>,
) -> impl IntoResponse {
    let book = state.book.read().await;
    Json(book.query(&to_query(&params, state.default_days)))
}

async fn query_venues(
    State(state): State<AppState>,
    Query(params): Query<RiskParams>,
) -> impl IntoResponse {
    let book = state.book.read().await;
    Json(json!({
        "ok": true,
        "as_of_ms": chrono::Utc::now().timestamp_millis(),
        "venues": book.venue_summaries(&to_query(&params, state.default_days)),
    }))
}

async fn query_announcements(State(state): State<AppState>) -> impl IntoResponse {
    let book = state.book.read().await;
    Json(json!({
        "ok": true,
        "count": book.announcements.len(),
        "items": book.announcements,
    }))
}

async fn query_status(State(state): State<AppState>) -> impl IntoResponse {
    let snap = state.status.read().await.snapshot(state.store.is_some());
    Json(snap)
}

fn to_query(params: &RiskParams, default_days: i64) -> RiskQuery {
    RiskQuery {
        venue: params.venue.clone(),
        exchange: params.exchange.clone(),
        days: Some(params.days.unwrap_or(default_days)),
        include_past: params.include_past,
    }
}

struct RefreshArgs {
    skip_llm: bool,
    skip_announcements: bool,
    skip_official: bool,
    skip_ws: bool,
    official_interval_secs: u64,
    announcement_interval_secs: u64,
    days: i64,
    llm_max: usize,
}

async fn run_refresh(state: AppState, args: RefreshArgs) -> Result<()> {
    let public = public_http_client()?;
    let binance = binance_http_client()?;
    let llm = if args.skip_llm {
        None
    } else {
        LlmConfig::from_env()
    };
    let llm_client = if llm.is_some() {
        LlmConfig::http_client().ok()
    } else {
        None
    };
    if llm.is_some() {
        info!("llm extract enabled");
    } else {
        info!("llm extract disabled");
    }
    let llm_budget = Arc::new(Mutex::new(LlmBudget::new(args.llm_max)));

    if !args.skip_official {
        refresh_official(&state, &public, &binance, args.days).await;
        persist(&state).await;
    }
    if !args.skip_announcements {
        refresh_announcements(
            &state,
            &public,
            &binance,
            llm.as_ref(),
            llm_client.as_ref(),
            &llm_budget,
        )
        .await;
        persist(&state).await;
    }
    if llm.is_some() {
        let backfill_state = state.clone();
        let backfill_llm = llm.clone();
        let backfill_client = llm_client.clone();
        let backfill_budget = llm_budget.clone();
        tokio::spawn(async move {
            backfill_pending_llm(
                &backfill_state,
                backfill_llm.as_ref(),
                backfill_client.as_ref(),
                &backfill_budget,
            )
            .await;
            persist(&backfill_state).await;
        });
    }

    let mut official = time::interval(Duration::from_secs(args.official_interval_secs.max(60)));
    official.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    official.tick().await;
    let mut announcements =
        time::interval(Duration::from_secs(args.announcement_interval_secs.max(60)));
    announcements.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    announcements.tick().await;

    loop {
        tokio::select! {
            _ = official.tick(), if !args.skip_official => {
                refresh_official(&state, &public, &binance, args.days).await;
                persist(&state).await;
            }
            _ = announcements.tick(), if !args.skip_announcements => {
                refresh_announcements(
                    &state,
                    &public,
                    &binance,
                    llm.as_ref(),
                    llm_client.as_ref(),
                    &llm_budget,
                ).await;
                persist(&state).await;
            }
            result = gate_ws_session(
                &state,
                llm.as_ref(),
                llm_client.as_ref(),
                &llm_budget,
            ), if !args.skip_ws => {
                match result {
                    Ok(()) => mark_ok(&state, "gate_ws", "ws").await,
                    Err(err) => {
                        warn!("Gate announcement ws session ended: {err:#}");
                        mark_err(&state, "gate_ws", "ws", &format!("{err:#}")).await;
                    }
                }
                persist(&state).await;
                time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
}

async fn refresh_official(state: &AppState, public: &Client, binance: &Client, days: i64) {
    match fetch_market_snapshot(public).await {
        Ok(snapshot) => {
            let events = events_from_gate_snapshot(&snapshot);
            let n = events.len();
            state
                .book
                .write()
                .await
                .replace_source("gate_market", events);
            info!("official gate_market events={n}");
            mark_ok(state, "gate_market", "fetch").await;
        }
        Err(err) => {
            warn!("gate market snapshot failed: {err:#}");
            mark_err(state, "gate_market", "fetch", &format!("{err:#}")).await;
        }
    }

    match fetch_offtime_snapshot(public).await {
        Ok(snapshot) => {
            let events = events_from_bitget_offtime(&snapshot);
            let n = events.len();
            state
                .book
                .write()
                .await
                .replace_source("bitget_instrument_offtime", events);
            info!("official bitget offtime events={n}");
            mark_ok(state, "bitget_instrument_offtime", "fetch").await;
        }
        Err(err) => {
            warn!("bitget offtime snapshot failed: {err:#}");
            mark_err(
                state,
                "bitget_instrument_offtime",
                "fetch",
                &format!("{err:#}"),
            )
            .await;
        }
    }

    ingest_binance_official(state, binance).await;
    ingest_schedule_venues(state, days).await;
}

async fn ingest_binance_official(state: &AppState, client: &Client) {
    match fetch_spot_delist_snapshot(client).await {
        Ok(snapshot) => {
            let events =
                events_from_official_snapshot("binance-margin", "binance", "delist", &snapshot);
            let n = events.len();
            let source = snapshot.source.clone();
            state.book.write().await.replace_source(&source, events);
            info!("official binance spot delist events={n}");
            mark_ok(state, "binance_spot_delist", "fetch").await;
        }
        Err(err) => {
            warn!("binance spot delist snapshot skipped: {err:#}");
            mark_err(state, "binance_spot_delist", "fetch", &format!("{err:#}")).await;
        }
    }
    match fetch_margin_delist_snapshot(client).await {
        Ok(snapshot) => {
            let events =
                events_from_official_snapshot("binance-margin", "binance", "delist", &snapshot);
            let n = events.len();
            let source = snapshot.source.clone();
            state.book.write().await.replace_source(&source, events);
            info!("official binance margin delist events={n}");
            mark_ok(state, "binance_margin_delist", "fetch").await;
        }
        Err(err) => {
            warn!("binance margin delist snapshot skipped: {err:#}");
            mark_err(state, "binance_margin_delist", "fetch", &format!("{err:#}")).await;
        }
    }
    match fetch_monitoring_tag_snapshot(client).await {
        Ok(snapshot) => {
            let events =
                events_from_official_snapshot("binance-margin", "binance", "monitoring", &snapshot);
            let n = events.len();
            let source = snapshot.source.clone();
            state.book.write().await.replace_source(&source, events);
            info!("official binance monitoring events={n}");
            mark_ok(state, "binance_monitoring", "fetch").await;
        }
        Err(err) => {
            warn!("binance monitoring snapshot skipped: {err:#}");
            mark_err(state, "binance_monitoring", "fetch", &format!("{err:#}")).await;
        }
    }
}

async fn ingest_schedule_venues(state: &AppState, days: i64) {
    let query = DelistScheduleQuery::next_days(days.max(1));
    for venue in [
        TradingVenue::BinanceFutures,
        TradingVenue::BinanceCoinFutures,
        TradingVenue::GateFutures,
        TradingVenue::BitgetFutures,
        TradingVenue::BitgetCoinFutures,
    ] {
        let provider = provider_for_venue(venue);
        let source_name = format!("schedule:{}", venue.data_pub_slug());
        match provider.future_delist_events(&query).await {
            Ok(events) => {
                let source = source_name.clone();
                let mut mapped = events_from_delist_schedule(&events);
                for event in &mut mapped {
                    event.source = source.clone();
                    event.announcement_id = source.clone();
                }
                let n = mapped.len();
                state.book.write().await.replace_source(&source, mapped);
                info!("official {} events={n}", venue.data_pub_slug());
                mark_ok(state, &source_name, "fetch").await;
            }
            Err(err) => {
                warn!("schedule {} skipped: {err:#}", venue.data_pub_slug());
                mark_err(state, &source_name, "fetch", &format!("{err:#}")).await;
            }
        }
    }
}

async fn refresh_announcements(
    state: &AppState,
    public: &Client,
    binance: &Client,
    llm: Option<&LlmConfig>,
    llm_client: Option<&Client>,
    llm_budget: &Mutex<LlmBudget>,
) {
    let mut watch = WatchState::default();
    {
        let book = state.book.read().await;
        for item in &book.announcements {
            if item.exchange == "binance" {
                watch.seen.insert(
                    item.id.clone(),
                    mkt_signal::common::binance_announcement::SeenArticle {
                        code: item.id.clone(),
                        title: item.title.clone(),
                        catalog_id: 0,
                        release_date_ms: item.published_ms,
                        kind:
                            mkt_signal::common::binance_announcement::AnnouncementKind::OtherDelist,
                        assets: Vec::new(),
                        symbols: Vec::new(),
                        first_seen_ms: item.published_ms,
                    },
                );
            }
        }
    }
    match backfill_catalog(binance, CATALOG_DELISTING, 10, 1, &watch, true).await {
        Ok(items) => {
            info!("binance cms new={}", items.len());
            mark_ok(state, "binance_cms", "fetch").await;
            for item in items {
                let input = LlmExtractInput::from_parsed(&item);
                remember_raw(state, &raw_from_parsed(&item)).await;
                maybe_extract(state, llm, llm_client, llm_budget, &input).await;
            }
        }
        Err(err) => {
            warn!("binance cms backfill failed: {err:#}");
            mark_err(state, "binance_cms", "fetch", &format!("{err:#}")).await;
        }
    }

    let mut seen = SeenStore::default();
    {
        let book = state.book.read().await;
        for item in &book.announcements {
            if item.exchange == "bitget" {
                seen.seen.insert(
                    format!("bitget:{}", item.id),
                    mkt_signal::common::announcement_watch::SeenItem {
                        id: item.id.clone(),
                        title: item.title.clone(),
                        url: item.url.clone(),
                        published_ms: item.published_ms,
                        first_seen_ms: item.published_ms,
                    },
                );
            }
        }
    }
    match fetch_delist_notices(public, "en_US", 10, 2, &seen).await {
        Ok(items) => {
            info!("bitget announcements new={}", items.len());
            mark_ok(state, "bitget_announcements", "fetch").await;
            for item in items {
                let input = LlmExtractInput::from_raw(&item);
                remember_raw(state, &item).await;
                maybe_extract(state, llm, llm_client, llm_budget, &input).await;
            }
        }
        Err(err) => {
            warn!("bitget announcements failed: {err:#}");
            mark_err(state, "bitget_announcements", "fetch", &format!("{err:#}")).await;
        }
    }
}

async fn backfill_pending_llm(
    state: &AppState,
    llm: Option<&LlmConfig>,
    llm_client: Option<&Client>,
    llm_budget: &Mutex<LlmBudget>,
) {
    let Some(store) = state.store.as_ref() else {
        return;
    };
    let items = match store.load_announcements().await {
        Ok(items) => items,
        Err(err) => {
            warn!("llm backfill load announcements failed: {err:#}");
            return;
        }
    };
    let mut pending = 0usize;
    for item in items {
        let already_ok = state
            .status
            .read()
            .await
            .llm(&item.exchange, &item.id)
            .is_some_and(|row| row.ok);
        if already_ok {
            continue;
        }
        pending += 1;
        let input = LlmExtractInput::from_raw(&item);
        maybe_extract(state, llm, llm_client, llm_budget, &input).await;
    }
    info!("llm backfill pending={pending}");
}

async fn maybe_extract(
    state: &AppState,
    llm: Option<&LlmConfig>,
    llm_client: Option<&Client>,
    llm_budget: &Mutex<LlmBudget>,
    input: &LlmExtractInput,
) {
    let (Some(llm), Some(client)) = (llm, llm_client) else {
        return;
    };
    if !llm_budget.lock().allow() {
        return;
    }
    match extract_for_emit(client, llm, input).await {
        Ok(value) => {
            state.book.write().await.ingest_llm_value(input, &value);
            mark_llm(state, input, true, None).await;
        }
        Err(err) => {
            mark_llm(state, input, false, Some(&format!("{err:#}"))).await;
        }
    }
}

async fn remember_raw(state: &AppState, item: &RawAnnouncement) {
    state
        .book
        .write()
        .await
        .remember_announcement(&announcement_from_raw(item));
    if let Some(store) = state.store.as_ref() {
        if let Err(err) = store.upsert_announcement(item).await {
            warn!(
                "persist announcement {}/{} failed: {err:#}",
                item.exchange, item.id
            );
        }
    }
}

async fn persist(state: &AppState) {
    let book = state.book.read().await;
    if let Err(err) = book.save(&state.book_path) {
        warn!("save risk book failed: {err:#}");
    }
}

async fn mark_ok(state: &AppState, source: &str, kind: &str) {
    let row = {
        let mut status = state.status.write().await;
        status.mark_ok(source, kind);
        status.source(source).cloned()
    };
    if let Some(row) = row {
        persist_source(state, &row).await;
    }
}

async fn mark_err(state: &AppState, source: &str, kind: &str, err: &str) {
    let row = {
        let mut status = state.status.write().await;
        status.mark_err(source, kind, err);
        status.source(source).cloned()
    };
    if let Some(row) = row {
        persist_source(state, &row).await;
    }
}

async fn mark_llm(state: &AppState, input: &LlmExtractInput, ok: bool, err: Option<&str>) {
    let (llm_row, source_row) = {
        let mut status = state.status.write().await;
        status.mark_llm(&input.exchange, &input.id, &input.title, ok, err);
        (
            status.llm(&input.exchange, &input.id).cloned(),
            status.source("llm").cloned(),
        )
    };
    if let (Some(store), Some(row)) = (state.store.as_ref(), llm_row) {
        if let Err(err) = store.upsert_llm(&row).await {
            warn!("persist llm_status failed: {err:#}");
        }
    }
    if let Some(row) = source_row {
        persist_source(state, &row).await;
    }
}

async fn persist_source(state: &AppState, row: &mkt_signal::common::delist_store::SourceStatus) {
    if let Some(store) = state.store.as_ref() {
        if let Err(err) = store.upsert_source(row).await {
            warn!("persist source_status {} failed: {err:#}", row.source);
        }
    }
}

fn raw_from_parsed(item: &ParsedAnnouncement) -> RawAnnouncement {
    RawAnnouncement {
        extra: Some(json!({
            "catalogId": item.catalog_id,
            "catalogName": item.catalog_name,
            "body": item.body_text,
            "kind": item.kind.to_string(),
        })),
        exchange: "binance".to_string(),
        id: item.code.clone(),
        title: item.title.clone(),
        url: item.url.clone(),
        published_ms: item.release_date_ms,
        source: item.source.clone(),
    }
}

async fn gate_ws_session(
    state: &AppState,
    llm: Option<&LlmConfig>,
    llm_client: Option<&Client>,
    llm_budget: &Mutex<LlmBudget>,
) -> Result<()> {
    info!("connecting Gate announcement ws {ANN_WS_URL}");
    let (mut ws, _) = tokio_tungstenite::connect_async(ANN_WS_URL)
        .await
        .context("connect Gate announcement ws failed")?;
    ws.send(Message::Text(subscribe_frame("en")))
        .await
        .context("send Gate delist subscribe failed")?;
    mark_ok(state, "gate_ws", "ws").await;

    let mut ping = time::interval(Duration::from_secs(20));
    ping.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ping.tick() => {
                ws.send(Message::Text(ping_frame()))
                    .await
                    .context("Gate announcement ping failed")?;
            }
            incoming = ws.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        handle_gate_text(state, &text, llm, llm_client, llm_budget).await;
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        if let Ok(text) = String::from_utf8(bytes) {
                            handle_gate_text(state, &text, llm, llm_client, llm_budget).await;
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        ws.send(Message::Pong(payload)).await.ok();
                    }
                    Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        warn!("Gate announcement ws closed: {frame:?}");
                        return Ok(());
                    }
                    Some(Err(err)) => return Err(err.into()),
                    None => {
                        warn!("Gate announcement ws stream ended");
                        return Ok(());
                    }
                }
            }
        }
    }
}

async fn handle_gate_text(
    state: &AppState,
    text: &str,
    llm: Option<&LlmConfig>,
    llm_client: Option<&Client>,
    llm_budget: &Mutex<LlmBudget>,
) {
    match parse_ws_text(text) {
        Ok(Some(item)) => {
            let input = LlmExtractInput::from_raw(&item);
            remember_raw(state, &item).await;
            maybe_extract(state, llm, llm_client, llm_budget, &input).await;
            persist(state).await;
        }
        Ok(None) => {}
        Err(err) => warn!("skip Gate announcement ws frame: {err:#}"),
    }
}
