//! 下架风险查询 HTTP。
//!
//! 官方快照 + 公告 LLM 抽取写入同一本风险簿，全量扁平 JSON 查询。
//! 公告、官方市场/offTime/schedule 与完整产品目录默认每天拉取一次。
//! 每天 00:00 UTC 将完整产品目录快照写入 Postgres。
//! 原始公告与拉取/LLM 状态写入 Postgres，失败原因可查。
//!
//! ```text
//! cargo run --bin delist_risk_server -- --bind 0.0.0.0:8787
//! curl 'http://127.0.0.1:8787/risk'
//! curl 'http://127.0.0.1:8787/status'
//! ```

use anyhow::{Context, Result};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, NaiveDate, Utc};
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
    backfill_catalog, fetch_margin_delist_snapshot, fetch_spot_delist_snapshot,
    http_client as binance_http_client, ParsedAnnouncement, WatchState, CATALOG_DELISTING,
};
use mkt_signal::common::bitget_announcement::{
    article_body_processed, fetch_delist_notices, fetch_offtime_snapshot, has_article_body,
    hydrate_notice_body, mark_article_body_processed,
};
use mkt_signal::common::delist_accounts::{
    build_account_views, load_universes, summarize, AccountRiskResponse,
};
use mkt_signal::common::delist_prune::{
    apply_redis_removal, confirmed_removal_candidates, prepare_redis_removal,
    removal_universe_errors, RedisRemovalCandidate,
};
use mkt_signal::common::delist_risk::{
    announcement_from_raw, events_from_bitget_offtime, events_from_delist_schedule,
    events_from_gate_snapshot, events_from_official_snapshot, RiskBook, RiskQuery,
};
use mkt_signal::common::delist_schedule::{provider_for_venue, DelistScheduleQuery};
use mkt_signal::common::delist_store::{DelistStore, StatusBook};
use mkt_signal::common::exchange_info::{fetch_listing_index, ListingIndex};
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

    /// Official market / offTime / schedule snapshot interval. Default 24h.
    #[arg(long, default_value_t = 86_400)]
    official_interval_secs: u64,

    /// Complete public product-catalog interval. Default 24h.
    #[arg(long, default_value_t = 86_400)]
    listing_interval_secs: u64,

    /// Remove symbols from Redis after any account venue confirms removal.
    #[arg(long)]
    auto_remove_redis: bool,

    /// Announcement poll interval. Default 24h.
    #[arg(long, default_value_t = 86_400)]
    announcement_interval_secs: u64,

    #[arg(long, default_value_t = 0)]
    llm_max: usize,

    /// Re-extract these announcements even if a previous LLM run succeeded.
    /// Comma-separated `id` or `exchange:id`. Default re-runs the 2026-08-21
    /// Binance spot-pair notice that previously promoted SUI/BNB to asset SUI.
    #[arg(long)]
    force_llm_ids: Option<String>,

    /// Postgres URL. Falls back to DELIST_PG_URL.
    #[arg(long)]
    postgres: Option<String>,

    /// Local Redis for jp-hosted books. Falls back to DELIST_REDIS_URL.
    #[arg(long)]
    redis: Option<String>,

    /// Optional Redis for sg-hosted books. Falls back to DELIST_SG_REDIS_URL.
    #[arg(long)]
    sg_redis: Option<String>,

    #[arg(long, default_value = "web/delist_risk")]
    web_dir: PathBuf,
}

#[derive(Clone)]
struct AppState {
    book: Arc<RwLock<RiskBook>>,
    status: Arc<RwLock<StatusBook>>,
    listings: Arc<RwLock<ListingIndex>>,
    store: Option<Arc<DelistStore>>,
    book_path: PathBuf,
    web_dir: PathBuf,
    default_days: i64,
    jp_redis: String,
    sg_redis: Option<String>,
    auto_remove_redis: bool,
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
    book.retain_delist_events();

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
            Ok(rows) => status.replace_sources(
                rows.into_iter()
                    .filter(|row| row.source != "binance_monitoring")
                    .collect(),
            ),
            Err(err) => warn!("restore source_status failed: {err:#}"),
        }
        match store.load_llm().await {
            Ok(rows) => status.replace_llm(rows),
            Err(err) => warn!("restore llm_status failed: {err:#}"),
        }
    }

    let jp_redis = args
        .redis
        .clone()
        .or_else(|| std::env::var("DELIST_REDIS_URL").ok())
        .filter(|url| !url.trim().is_empty())
        .unwrap_or_else(|| "redis://127.0.0.1:6379/0".to_string());
    let sg_redis = args
        .sg_redis
        .clone()
        .or_else(|| std::env::var("DELIST_SG_REDIS_URL").ok())
        .filter(|url| !url.trim().is_empty());
    if sg_redis.is_some() {
        info!("sg redis enabled for bybit books");
    } else {
        info!("sg redis disabled (no --sg-redis / DELIST_SG_REDIS_URL)");
    }

    let state = AppState {
        book: Arc::new(RwLock::new(book)),
        status: Arc::new(RwLock::new(status)),
        listings: Arc::new(RwLock::new(ListingIndex::default())),
        store,
        book_path: args.book.clone(),
        web_dir: args.web_dir.clone(),
        default_days: args.days,
        jp_redis,
        sg_redis,
        auto_remove_redis: args.auto_remove_redis,
    };

    let refresh = state.clone();
    let refresh_args = RefreshArgs {
        skip_llm: args.skip_llm,
        skip_announcements: args.skip_announcements,
        skip_official: args.skip_official,
        skip_ws: args.skip_ws,
        official_interval_secs: args.official_interval_secs,
        listing_interval_secs: args.listing_interval_secs,
        announcement_interval_secs: args.announcement_interval_secs,
        days: args.days,
        llm_max: args.llm_max,
        force_llm_ids: args.force_llm_ids.clone(),
    };
    tokio::spawn(async move {
        if let Err(err) = run_refresh(refresh, refresh_args).await {
            warn!("delist refresh loop exited: {err:#}");
        }
    });

    let app = Router::new()
        .route("/", get(index_page))
        .route("/index.html", get(index_page))
        .route("/healthz", get(healthz))
        .route("/risk", get(query_risk))
        .route("/venues", get(query_venues))
        .route("/announcements", get(query_announcements))
        .route("/status", get(query_status))
        .route("/accounts", get(query_accounts))
        .route("/removal-candidates", get(query_removal_candidates))
        .route("/removals", get(query_removals))
        .with_state(state);

    let addr: SocketAddr = args.bind.parse().context("invalid --bind")?;
    info!(
        "delist_risk_server listening at http://{addr} official={}s listings={}s announcements={}s auto_remove_redis={}",
        args.official_interval_secs,
        args.listing_interval_secs,
        args.announcement_interval_secs,
        args.auto_remove_redis,
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
    let mut resp = book.query(&to_query(&params, state.default_days));
    state.listings.read().await.decorate(&mut resp);
    Json(resp)
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

async fn index_page(State(state): State<AppState>) -> impl IntoResponse {
    let path = state.web_dir.join("index.html");
    match std::fs::read_to_string(&path) {
        Ok(body) => {
            let mut response = Html(body).into_response();
            response.headers_mut().insert(
                axum::http::header::CACHE_CONTROL,
                axum::http::HeaderValue::from_static("no-store"),
            );
            response
        }
        Err(err) => (
            axum::http::StatusCode::NOT_FOUND,
            format!("missing frontend {}: {err}", path.display()),
        )
            .into_response(),
    }
}

async fn query_accounts(
    State(state): State<AppState>,
    Query(params): Query<RiskParams>,
) -> impl IntoResponse {
    let book = state.book.read().await;
    let mut risk = book.query(&to_query(&params, state.default_days));
    let listings = state.listings.read().await.clone();
    listings.decorate(&mut risk);
    drop(book);
    let universes = load_universes(&state.jp_redis, state.sg_redis.as_deref()).await;
    let accounts = build_account_views(&risk, &listings, &universes);
    let mut redis = std::collections::BTreeMap::new();
    redis.insert(
        "jp".to_string(),
        accounts
            .iter()
            .filter(|account| account.host == "jp")
            .all(|account| account.redis_ok),
    );
    redis.insert(
        "sg".to_string(),
        accounts
            .iter()
            .filter(|account| account.host == "sg")
            .all(|account| account.redis_ok),
    );
    Json(AccountRiskResponse {
        ok: true,
        as_of_ms: risk.as_of_ms,
        redis,
        summary: summarize(&accounts),
        accounts,
    })
}

async fn query_removal_candidates(State(state): State<AppState>) -> impl IntoResponse {
    let listings = state.listings.read().await.clone();
    let universes = load_universes(&state.jp_redis, state.sg_redis.as_deref()).await;
    let redis_errors = removal_universe_errors(&universes);
    let candidates = confirmed_removal_candidates(&listings, &universes);
    let catalog_complete = state
        .status
        .read()
        .await
        .source("exchange_info")
        .is_some_and(|status| status.ok);
    Json(json!({
        "ok": true,
        "auto_remove_redis": state.auto_remove_redis,
        "catalog_complete": catalog_complete,
        "redis_errors": redis_errors,
        "count": candidates.len(),
        "items": candidates,
    }))
}

#[derive(Debug, Deserialize)]
struct RemovalParams {
    limit: Option<i64>,
}

async fn query_removals(
    State(state): State<AppState>,
    Query(params): Query<RemovalParams>,
) -> Response {
    let Some(store) = state.store.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"ok": false, "error": "postgres unavailable"})),
        )
            .into_response();
    };
    match store.load_redis_removals(params.limit.unwrap_or(200)).await {
        Ok(items) => Json(json!({
            "ok": true,
            "count": items.len(),
            "items": items,
        }))
        .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"ok": false, "error": format!("{err:#}")})),
        )
            .into_response(),
    }
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
    listing_interval_secs: u64,
    announcement_interval_secs: u64,
    days: i64,
    llm_max: usize,
    force_llm_ids: Option<String>,
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
        let snapshot_date = Utc::now().date_naive();
        match refresh_listings(&state, &public).await {
            Ok(index) => persist_symbol_snapshot(&state, &index, snapshot_date).await,
            Err(err) => record_symbol_snapshot_failure(&state, snapshot_date, &err).await,
        }
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
        let force_llm_ids = force_llm_id_set(&args.force_llm_ids);
        tokio::spawn(async move {
            backfill_pending_llm(
                &backfill_state,
                backfill_llm.as_ref(),
                backfill_client.as_ref(),
                &backfill_budget,
                &force_llm_ids,
            )
            .await;
            persist(&backfill_state).await;
        });
    }

    let mut official = time::interval(Duration::from_secs(args.official_interval_secs.max(60)));
    official.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    official.tick().await;
    let mut listings = time::interval(Duration::from_secs(args.listing_interval_secs.max(60)));
    listings.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    listings.tick().await;
    let mut announcements =
        time::interval(Duration::from_secs(args.announcement_interval_secs.max(60)));
    announcements.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    announcements.tick().await;
    let daily_snapshot = time::sleep(duration_until_next_utc_midnight(Utc::now()));
    tokio::pin!(daily_snapshot);

    loop {
        tokio::select! {
            _ = official.tick(), if !args.skip_official => {
                refresh_official(&state, &public, &binance, args.days).await;
                persist(&state).await;
            }
            _ = listings.tick(), if !args.skip_official => {
                let snapshot_date = Utc::now().date_naive();
                match refresh_listings(&state, &public).await {
                    Ok(index) => persist_symbol_snapshot(&state, &index, snapshot_date).await,
                    Err(err) => record_symbol_snapshot_failure(&state, snapshot_date, &err).await,
                }
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
            _ = &mut daily_snapshot, if !args.skip_official => {
                let snapshot_date = Utc::now().date_naive();
                match refresh_listings(&state, &public).await {
                    Ok(index) => persist_symbol_snapshot(&state, &index, snapshot_date).await,
                    Err(err) => record_symbol_snapshot_failure(&state, snapshot_date, &err).await,
                }
                persist(&state).await;
                daily_snapshot.as_mut().reset(
                    time::Instant::now() + duration_until_next_utc_midnight(Utc::now())
                );
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

async fn refresh_listings(state: &AppState, public: &Client) -> Result<ListingIndex> {
    let (index, errors) = fetch_listing_index(public).await;
    if errors.is_empty() {
        *state.listings.write().await = index.clone();
        mark_ok(state, "exchange_info", "fetch").await;
        info!("official exchange_info refreshed");
        if state.auto_remove_redis {
            match prune_confirmed_delists(state).await {
                Ok(count) => {
                    mark_ok(state, "redis_delist_prune", "mutation").await;
                    info!("Redis confirmed-delist prune completed removals={count}");
                }
                Err(err) => {
                    warn!("Redis confirmed-delist prune failed: {err:#}");
                    mark_err(state, "redis_delist_prune", "mutation", &format!("{err:#}")).await;
                }
            }
        }
        return Ok(index);
    }
    for (source, err) in &errors {
        warn!("exchange_info {source} failed: {err}");
    }
    let error = errors
        .iter()
        .map(|(source, err)| format!("{source}: {err}"))
        .collect::<Vec<_>>()
        .join("; ");
    mark_err(state, "exchange_info", "fetch", &error).await;
    anyhow::bail!(error)
}

fn scheduled_midnight_ms(snapshot_date: NaiveDate) -> i64 {
    snapshot_date
        .and_hms_opt(0, 0, 0)
        .expect("midnight is valid")
        .and_utc()
        .timestamp_millis()
}

fn duration_until_next_utc_midnight(now: DateTime<Utc>) -> Duration {
    let next_date = now.date_naive().succ_opt().expect("next UTC date is valid");
    let next = next_date
        .and_hms_opt(0, 0, 0)
        .expect("midnight is valid")
        .and_utc();
    (next - now)
        .to_std()
        .unwrap_or_else(|_| Duration::from_secs(1))
}

async fn persist_symbol_snapshot(state: &AppState, index: &ListingIndex, snapshot_date: NaiveDate) {
    let Some(store) = state.store.as_ref() else {
        mark_err(
            state,
            "exchange_symbol_snapshot",
            "persist",
            "postgres unavailable",
        )
        .await;
        return;
    };
    let rows = index.snapshot_rows();
    if rows.is_empty() {
        record_symbol_snapshot_failure(
            state,
            snapshot_date,
            &anyhow::anyhow!("complete exchange symbol catalog is empty"),
        )
        .await;
        return;
    }
    let captured_ms = Utc::now().timestamp_millis();
    match store
        .write_symbol_snapshot(
            snapshot_date,
            scheduled_midnight_ms(snapshot_date),
            captured_ms,
            &rows,
        )
        .await
    {
        Ok(created) => {
            mark_ok(state, "exchange_symbol_snapshot", "persist").await;
            if created {
                let venues = rows
                    .iter()
                    .map(|row| row.venue.as_str())
                    .collect::<std::collections::BTreeSet<_>>()
                    .len();
                info!(
                    "exchange symbol snapshot persisted date={} venues={} symbols={}",
                    snapshot_date,
                    venues,
                    rows.len()
                );
            } else {
                info!("exchange symbol snapshot already complete date={snapshot_date}");
            }
        }
        Err(err) => {
            warn!("exchange symbol snapshot persist failed: {err:#}");
            mark_err(
                state,
                "exchange_symbol_snapshot",
                "persist",
                &format!("{err:#}"),
            )
            .await;
        }
    }
}

async fn record_symbol_snapshot_failure(
    state: &AppState,
    snapshot_date: NaiveDate,
    err: &anyhow::Error,
) {
    let message = format!("{err:#}");
    mark_err(state, "exchange_symbol_snapshot", "persist", &message).await;
    let Some(store) = state.store.as_ref() else {
        return;
    };
    if let Err(store_err) = store
        .record_symbol_snapshot_failure(
            snapshot_date,
            scheduled_midnight_ms(snapshot_date),
            &message,
        )
        .await
    {
        warn!("record exchange symbol snapshot failure failed: {store_err:#}");
    }
}

async fn prune_confirmed_delists(state: &AppState) -> Result<usize> {
    let store = state
        .store
        .as_ref()
        .context("postgres is required before Redis auto-removal")?;
    let listings = state.listings.read().await.clone();
    let universes = load_universes(&state.jp_redis, state.sg_redis.as_deref()).await;
    let universe_errors = removal_universe_errors(&universes);
    if !universe_errors.is_empty() {
        anyhow::bail!("Redis universe load failed: {}", universe_errors.join("; "));
    }
    let candidates = confirmed_removal_candidates(&listings, &universes);
    let mut removed = 0usize;
    let mut errors = Vec::new();
    for candidate in candidates {
        match prune_candidate(state, store, &candidate).await {
            Ok(changed) => removed += usize::from(changed),
            Err(err) => errors.push(format!(
                "account={} symbol={}: {err:#}",
                candidate.account_slug, candidate.symbol
            )),
        }
    }
    if errors.is_empty() {
        Ok(removed)
    } else {
        anyhow::bail!(errors.join("; "))
    }
}

async fn prune_candidate(
    state: &AppState,
    store: &DelistStore,
    candidate: &RedisRemovalCandidate,
) -> Result<bool> {
    let redis_url = match candidate.redis_site.as_str() {
        "jp" => state.jp_redis.as_str(),
        "sg" => state
            .sg_redis
            .as_deref()
            .context("SG Redis is not configured")?,
        site => anyhow::bail!("unsupported Redis site {site}"),
    };
    let Some(plan) = prepare_redis_removal(redis_url, candidate).await? else {
        return Ok(false);
    };
    let audit_id = store
        .begin_redis_removal(
            &candidate.account_slug,
            &candidate.exchange,
            &candidate.redis_site,
            &candidate.symbol,
            serde_json::to_value(&candidate.venues)?,
            serde_json::to_value(&plan.changes)?,
        )
        .await?;
    info!(
        "Redis delist removal pending audit_id={} account={} symbol={} keys={}",
        audit_id,
        candidate.account_slug,
        candidate.symbol,
        plan.changes.len()
    );
    match apply_redis_removal(redis_url, &plan).await {
        Ok(()) => {
            store
                .finish_redis_removal(audit_id, "success", None)
                .await?;
            info!(
                "Redis delist removal success audit_id={} account={} symbol={} changes={}",
                audit_id,
                candidate.account_slug,
                candidate.symbol,
                serde_json::to_string(&plan.changes)?
            );
            Ok(true)
        }
        Err(err) => {
            if let Err(audit_err) = store
                .finish_redis_removal(audit_id, "failed", Some(&format!("{err:#}")))
                .await
            {
                warn!("finish failed removal audit id={audit_id}: {audit_err:#}");
            }
            Err(err)
        }
    }
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
    let mut bitget_detail_errors =
        backfill_bitget_details(state, public, llm, llm_client, llm_budget).await;
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
            for mut item in items {
                if let Err(err) = hydrate_notice_body(public, &mut item).await {
                    warn!("Bitget article detail failed id={}: {err:#}", item.id);
                    bitget_detail_errors.push(format!("{}: {err:#}", item.id));
                    continue;
                }
                let input = LlmExtractInput::from_raw(&item);
                remember_raw(state, &item).await;
                if maybe_extract(state, llm, llm_client, llm_budget, &input).await {
                    if let Err(err) = mark_article_body_processed(&mut item) {
                        warn!(
                            "Bitget article completion mark failed id={}: {err:#}",
                            item.id
                        );
                    } else {
                        remember_raw(state, &item).await;
                    }
                }
            }
        }
        Err(err) => {
            warn!("bitget announcements failed: {err:#}");
            mark_err(state, "bitget_announcements", "fetch", &format!("{err:#}")).await;
        }
    }
    if bitget_detail_errors.is_empty() {
        mark_ok(state, "bitget_article_detail", "fetch").await;
    } else {
        mark_err(
            state,
            "bitget_article_detail",
            "fetch",
            &bitget_detail_errors.join("; "),
        )
        .await;
    }
}

async fn backfill_bitget_details(
    state: &AppState,
    client: &Client,
    llm: Option<&LlmConfig>,
    llm_client: Option<&Client>,
    llm_budget: &Mutex<LlmBudget>,
) -> Vec<String> {
    let Some(store) = state.store.as_ref() else {
        return Vec::new();
    };
    let items = match store.load_announcements().await {
        Ok(items) => items,
        Err(err) => return vec![format!("load stored announcements: {err:#}")],
    };
    let mut errors = Vec::new();
    for mut item in items
        .into_iter()
        .filter(|item| item.exchange == "bitget" && !article_body_processed(item))
    {
        if !has_article_body(&item) {
            if let Err(err) = hydrate_notice_body(client, &mut item).await {
                warn!(
                    "Bitget stored article detail failed id={}: {err:#}",
                    item.id
                );
                errors.push(format!("{}: {err:#}", item.id));
                continue;
            }
            remember_raw(state, &item).await;
        }
        let input = LlmExtractInput::from_raw(&item);
        if maybe_extract(state, llm, llm_client, llm_budget, &input).await {
            if let Err(err) = mark_article_body_processed(&mut item) {
                errors.push(format!("{}: completion mark: {err:#}", item.id));
            } else {
                remember_raw(state, &item).await;
            }
        }
    }
    errors
}

const DEFAULT_FORCE_LLM_IDS: &str = "fab1676df7fb464a9e4634c6f777659e";

fn force_llm_id_set(raw: &Option<String>) -> std::collections::BTreeSet<String> {
    raw.as_deref()
        .unwrap_or(DEFAULT_FORCE_LLM_IDS)
        .split(',')
        .map(|item| {
            item.trim()
                .rsplit_once(':')
                .map(|(_, id)| id.trim())
                .unwrap_or(item.trim())
                .to_string()
        })
        .filter(|id| !id.is_empty())
        .collect()
}

async fn backfill_pending_llm(
    state: &AppState,
    llm: Option<&LlmConfig>,
    llm_client: Option<&Client>,
    llm_budget: &Mutex<LlmBudget>,
    force_ids: &std::collections::BTreeSet<String>,
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
    let mut forced = 0usize;
    for item in items {
        let already_ok = state
            .status
            .read()
            .await
            .llm(&item.exchange, &item.id)
            .is_some_and(|row| row.ok);
        let force = force_ids.contains(&item.id);
        if already_ok && !force {
            continue;
        }
        if force {
            forced += 1;
        } else {
            pending += 1;
        }
        let input = LlmExtractInput::from_raw(&item);
        maybe_extract(state, llm, llm_client, llm_budget, &input).await;
    }
    info!("llm backfill pending={pending} forced={forced}");
}

async fn maybe_extract(
    state: &AppState,
    llm: Option<&LlmConfig>,
    llm_client: Option<&Client>,
    llm_budget: &Mutex<LlmBudget>,
    input: &LlmExtractInput,
) -> bool {
    let (Some(llm), Some(client)) = (llm, llm_client) else {
        return false;
    };
    if !llm_budget.lock().allow() {
        return false;
    }
    match extract_for_emit(client, llm, input).await {
        Ok(value) => {
            state.book.write().await.ingest_llm_value(input, &value);
            mark_llm(state, input, true, None).await;
            true
        }
        Err(err) => {
            mark_llm(state, input, false, Some(&format!("{err:#}"))).await;
            false
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn next_snapshot_is_exactly_utc_midnight() {
        let before_midnight = Utc.with_ymd_and_hms(2026, 9, 7, 23, 59, 59).unwrap();
        assert_eq!(
            duration_until_next_utc_midnight(before_midnight),
            Duration::from_secs(1)
        );

        let midnight = Utc.with_ymd_and_hms(2026, 9, 7, 0, 0, 0).unwrap();
        assert_eq!(
            duration_until_next_utc_midnight(midnight),
            Duration::from_secs(86_400)
        );
        assert_eq!(
            scheduled_midnight_ms(midnight.date_naive()),
            midnight.timestamp_millis()
        );
    }
}
