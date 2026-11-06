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
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
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
    build_account_views, fetch_nav_accounts, load_fr_dump_symbols, load_fr_symbol_list,
    load_universes, nav_login, summarize, AccountRiskResponse, AccountRiskView, AccountSpec,
    FrSymbolListState, NAV_UNAUTHORIZED,
};
use mkt_signal::common::delist_dump::{
    apply_redis_dump, position_close_statuses, position_dump_candidates, prepare_redis_dump,
    snapshot_symbol_is_flat, PositionDumpCandidate, POSITION_REMOVAL_THRESHOLD_USDT,
};
use mkt_signal::common::delist_flatten::{
    audit_dedup_key, flatten_candidates, FlattenCandidate, FlattenExecutor, FlattenRunOutput,
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
use mkt_signal::common::delist_store::{DelistStore, FlattenExecutionAudit, StatusBook};
use mkt_signal::common::exchange_info::{fetch_listing_index, ListingIndex};
use mkt_signal::common::gate_announcement::{
    fetch_market_snapshot, parse_ws_text, ping_frame, subscribe_frame, ANN_WS_URL,
};
use mkt_signal::common::{bybit_announcement, okx_announcement};
use order_common::TradingVenue;
use parking_lot::Mutex;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex as AsyncMutex, RwLock, Semaphore};
use tokio::time;
use tokio_tungstenite::tungstenite::Message;

#[path = "delist_risk_server/access.rs"]
mod access;
#[path = "delist_risk_server/backup.rs"]
mod backup;
#[path = "delist_risk_server/cta_backup.rs"]
mod cta_backup;
#[path = "delist_risk_server/cta_factors.rs"]
mod cta_factors;
#[path = "delist_risk_server/cta_mapping.rs"]
mod cta_mapping;
#[path = "delist_risk_server/exposure.rs"]
mod exposure;
#[path = "delist_risk_server/notifications.rs"]
mod notifications;
#[path = "delist_risk_server/upload.rs"]
mod upload;

use mkt_signal::pre_trade::notification_client::{
    LocalNotificationClient, NotificationRequest, NotificationSeverity,
};

const POSITION_CLOSED_THRESHOLD_USDT: f64 = 100.0;

#[derive(Parser)]
#[command(name = "delist_risk_server")]
#[command(about = "HTTP query for exchange delist / margin / futures risk")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8787")]
    bind: String,

    /// Required login credentials (salted password hash), origin and FR allowlist.
    #[arg(long, default_value = "config/delist_access.json")]
    access_config: PathBuf,

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

    /// Read CTA signal/Manager mapping once and print JSON; do not start the service.
    #[arg(long)]
    cta_mapping_check: bool,

    /// Official market / offTime / schedule snapshot interval. Default 24h.
    #[arg(long, default_value_t = 86_400)]
    official_interval_secs: u64,

    /// Complete public product-catalog interval. Default 24h.
    #[arg(long, default_value_t = 86_400)]
    listing_interval_secs: u64,

    /// Remove symbols from Redis after any account venue confirms removal.
    #[arg(long)]
    auto_remove_redis: bool,

    /// Move positioned FR symbols from open lists to dump before a scheduled delist.
    #[arg(long)]
    auto_dump_position_risk: bool,

    /// Local position snapshot scan interval. This does not poll exchange delist APIs.
    #[arg(long, default_value_t = 60)]
    position_risk_interval_secs: u64,

    /// FR `fr_unimmr_close_symbols` empty-list watch interval. Redis-only read.
    #[arg(long, default_value_t = 60)]
    unimmr_list_interval_secs: u64,

    /// Minimum absolute affected-leg position required for automatic dump.
    #[arg(long, default_value_t = 50.0)]
    position_risk_threshold_usdt: f64,

    /// Reject account snapshots older than this many seconds.
    #[arg(long, default_value_t = 120)]
    position_snapshot_max_age_secs: u64,

    /// Read-only FR net exposure polling interval.
    #[arg(long, default_value_t = 10)]
    exposure_interval_secs: u64,
    #[arg(long, default_value_t = 90)]
    exposure_max_age_secs: u64,
    #[arg(long, default_value_t = 30)]
    exposure_persistence_secs: u64,
    #[arg(long, default_value_t = 60)]
    data_persistence_secs: u64,
    #[arg(long, default_value_t = 100.0)]
    exposure_asset_limit_usdt: f64,
    #[arg(long, default_value_t = 500.0)]
    exposure_account_limit_usdt: f64,

    /// Explicit bounded exceptions for operator-confirmed spot tails.
    #[arg(long, default_value = "config/fr_exposure_ignored_spot_tails.json")]
    exposure_ignored_spot_tails: PathBuf,

    /// Enable the independent FR DingTalk channel (FR_DINGTALK_* only).
    #[arg(long)]
    fr_notify: bool,

    /// Validate the FR DingTalk configuration without starting services or sending.
    #[arg(long)]
    fr_notify_check: bool,

    /// Send one explicitly requested FR channel setup test, then exit.
    #[arg(long)]
    fr_notify_send_check: bool,

    /// Clear small FR positions during the final delist window.
    #[arg(long)]
    auto_flatten_position_risk: bool,

    /// Final window before the actual delist deadline.
    #[arg(long, default_value_t = 24)]
    flatten_window_hours: u64,

    /// Positions above this value require a manual flatten action.
    #[arg(long, default_value_t = 1_000.0)]
    flatten_manual_threshold_usdt: f64,

    /// Parent directory containing account deployment directories.
    #[arg(long, default_value = "/home/ubuntu")]
    flatten_env_root: PathBuf,

    /// Maximum runtime of one flatten script.
    #[arg(long, default_value_t = 300)]
    flatten_timeout_secs: u64,

    /// SSH alias used to run SG Bybit FR flatten in its own environment.
    #[arg(long)]
    sg_flatten_ssh_host: Option<String>,

    /// Base URL serving /fr/<env>/snapshot.
    #[arg(long, default_value = "http://127.0.0.1:4191")]
    snapshot_base_url: String,

    /// NAV strategy catalog used as the delist account source of truth.
    #[arg(long, default_value = "http://127.0.0.1:4191/nav-api/strategies")]
    nav_strategies_url: String,

    /// NAV API service account; when set the server logs in and retries on 401.
    #[arg(long)]
    nav_username: Option<String>,

    /// NAV API service account password.
    #[arg(long)]
    nav_password: Option<String>,

    /// NAV strategy catalog refresh interval.
    #[arg(long, default_value_t = 60)]
    nav_strategy_interval_secs: u64,

    /// Announcement poll interval. Default 24h.
    #[arg(long, default_value_t = 86_400)]
    announcement_interval_secs: u64,

    #[arg(long, default_value_t = 0)]
    llm_max: usize,

    /// Re-extract these announcements even if a previous LLM run succeeded.
    /// Comma-separated `id` or `exchange:id`. Only explicitly requested IDs are forced.
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

    /// Directory for daily FR configuration archives.
    #[arg(long, default_value = "data/redis_backups")]
    redis_backup_dir: PathBuf,

    /// Number of daily archives retained for each FR environment.
    #[arg(long, default_value_t = 30)]
    redis_backup_keep: usize,

    /// Parent directory containing JP FR environments.
    #[arg(long, default_value = "/home/ubuntu")]
    redis_backup_env_root: PathBuf,

    /// SSH host for reading SG FR environment configuration.
    #[arg(long, default_value = "sg")]
    redis_backup_sg_ssh_host: String,

    /// Environment owning the shared cta-prep-binance-futures configuration.
    /// Defaults to <redis-backup-env-root>/cta-prep-binance-futures.
    #[arg(long)]
    cta_factor_env: Option<PathBuf>,
    /// Environment owning the independent cta-factor-spread catalog.
    #[arg(long)]
    cta_spread_factor_env: Option<PathBuf>,

    /// Local CTA configuration archive directory.
    #[arg(long, default_value = "data/cta_backups")]
    cta_backup_dir: PathBuf,

    /// COS destination for CTA archives, using the private rclone config.
    #[arg(long, requires = "redis_backup_cos_config")]
    cta_backup_cos_target: Option<String>,

    /// rclone COS destination, e.g. remote:bucket/prefix.
    #[arg(long, requires = "redis_backup_cos_config")]
    redis_backup_cos_target: Option<String>,

    /// Private rclone configuration containing the COS remote.
    #[arg(long)]
    redis_backup_cos_config: Option<PathBuf>,

    /// Disable daily FR configuration backups.
    #[arg(long)]
    skip_redis_backup: bool,

    #[arg(long, default_value = "web/delist_risk")]
    web_dir: PathBuf,
}

#[derive(Clone)]
struct AppState {
    book: Arc<RwLock<RiskBook>>,
    status: Arc<RwLock<StatusBook>>,
    listings: Arc<RwLock<ListingIndex>>,
    llm_locks: Arc<Mutex<BTreeMap<(String, String), Arc<AsyncMutex<()>>>>>,
    llm_slots: Arc<Semaphore>,
    accounts: Arc<RwLock<Vec<AccountSpec>>>,
    exposure: Arc<RwLock<exposure::Monitor>>,
    exposure_settings: exposure::Settings,
    fr_notifications: Option<Arc<notifications::Notifications>>,
    store: Option<Arc<DelistStore>>,
    book_path: PathBuf,
    web_dir: PathBuf,
    default_days: i64,
    jp_redis: String,
    sg_redis: Option<String>,
    auto_remove_redis: bool,
    auto_dump_position_risk: bool,
    auto_flatten_position_risk: bool,
    position_risk_threshold_usdt: f64,
    position_snapshot_max_age_ms: i64,
    flatten_window_ms: i64,
    flatten_manual_threshold_usdt: f64,
    flatten_executor: FlattenExecutor,
    flatten_inflight: Arc<AsyncMutex<BTreeSet<String>>>,
    notification_client: Option<LocalNotificationClient>,
    flatten_api_token: Option<String>,
    snapshot_base_url: String,
    snapshot_client: Client,
    nav_strategies_url: String,
    nav_login_url: String,
    nav_username: Option<String>,
    nav_password: Option<String>,
    nav_session: Arc<RwLock<Option<String>>>,
    cta_mapping: cta_mapping::Settings,
    backups: ManualBackupSettings,
    backup_lock: Arc<AsyncMutex<()>>,
}

#[derive(Clone)]
struct ManualBackupSettings {
    enabled: bool,
    fr_dir: PathBuf,
    cta_dir: PathBuf,
    env_root: PathBuf,
    sg_ssh_host: String,
    keep: usize,
    fr_cos: Option<upload::UploadConfig>,
    cta_cos: Option<upload::UploadConfig>,
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
    if args.fr_notify_check || args.fr_notify_send_check {
        let sender = notifications::DingTalk::from_env()?;
        if args.fr_notify_send_check {
            sender.send("[接入验证] FR 独立钉钉通道签名验证\n将接收 Luna 每次实际公告处理结果、每日扫描汇总心跳，以及 UniMMR/敞口等风险的首次异常与恢复通知。\n本条仅验证通知通道，没有交易、名单或仓位变更。").await?;
        }
        println!("Independent FR DingTalk configuration is valid");
        return Ok(());
    }
    if args.cta_mapping_check {
        let settings = cta_mapping::Settings {
            env_root: args.redis_backup_env_root.clone(),
            factor_env: args.cta_factor_env.clone().unwrap_or_else(|| {
                args.redis_backup_env_root
                    .join(cta_factors::Worker::Perp.name())
            }),
            spread_factor_env: args.cta_spread_factor_env.clone().unwrap_or_else(|| {
                args.redis_backup_env_root
                    .join(cta_factors::Worker::Spread.name())
            }),
            jp_database: std::env::var("DELIST_CTA_JP_DATABASE_URL")
                .ok()
                .filter(|s| !s.is_empty()),
        };
        let params = RiskParams {
            venue: None,
            exchange: None,
            days: None,
            include_past: false,
        };
        let risk = RiskBook::default().query(&to_query(&params, args.days));
        let result = cta_mapping::load(&settings, &risk, &ListingIndex::default()).await;
        println!("{}", serde_json::to_string_pretty(&result)?);
        return Ok(());
    }
    let access = access::Access::load(&args.access_config)?;
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
                    .filter(|row| {
                        row.source != "binance_monitoring" && row.source != "nav_strategies"
                    })
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
    if args.redis_backup_keep == 0 {
        anyhow::bail!("--redis-backup-keep must be positive");
    }
    if sg_redis.is_some() {
        info!("sg redis enabled for bybit books");
    } else {
        info!("sg redis disabled (no --sg-redis / DELIST_SG_REDIS_URL)");
    }
    if !args.flatten_manual_threshold_usdt.is_finite() || args.flatten_manual_threshold_usdt <= 0.0
    {
        anyhow::bail!("--flatten-manual-threshold-usdt must be finite and positive");
    }
    if args.flatten_window_hours == 0 {
        anyhow::bail!("--flatten-window-hours must be positive");
    }
    let notification_client = match LocalNotificationClient::from_env() {
        Ok(client) => Some(client),
        Err(err) => {
            if args.auto_flatten_position_risk {
                return Err(err)
                    .context("automatic delist flatten requires the local notification service");
            }
            info!("delist flatten notifications disabled: {err:#}");
            None
        }
    };
    if args.auto_flatten_position_risk && store.is_none() {
        anyhow::bail!("automatic delist flatten requires PostgreSQL");
    }
    let flatten_api_token = std::env::var("DELIST_FLATTEN_API_TOKEN")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if args.auto_flatten_position_risk && flatten_api_token.is_none() {
        anyhow::bail!("automatic delist flatten requires DELIST_FLATTEN_API_TOKEN");
    }

    let exposure_settings = exposure::Settings {
        interval_secs: args.exposure_interval_secs,
        max_age_secs: args.exposure_max_age_secs,
        persistence_secs: args.exposure_persistence_secs,
        data_persistence_secs: args.data_persistence_secs,
        asset_limit_usdt: args.exposure_asset_limit_usdt,
        account_limit_usdt: args.exposure_account_limit_usdt,
        catalog_max_age_secs: args.nav_strategy_interval_secs.saturating_mul(3).max(90),
        ignored_spot_tails: exposure::load_ignored_spot_tails(&args.exposure_ignored_spot_tails)?,
    };
    exposure_settings.validate()?;
    let fr_notifications = if args.fr_notify {
        Some(
            notifications::Notifications::open(
                args.book.with_file_name("fr_notifications.json"),
                notifications::DingTalk::from_env()?,
            )
            .await?,
        )
    } else {
        None
    };
    let state = AppState {
        book: Arc::new(RwLock::new(book)),
        status: Arc::new(RwLock::new(status)),
        listings: Arc::new(RwLock::new(ListingIndex::default())),
        llm_locks: Arc::new(Mutex::new(BTreeMap::new())),
        llm_slots: Arc::new(Semaphore::new(2)),
        accounts: Arc::new(RwLock::new(Vec::new())),
        exposure: Arc::new(RwLock::new(exposure::Monitor::new(
            exposure_settings.clone(),
        ))),
        exposure_settings,
        fr_notifications,
        store,
        book_path: args.book.clone(),
        web_dir: args.web_dir.clone(),
        default_days: args.days,
        jp_redis,
        sg_redis,
        auto_remove_redis: args.auto_remove_redis,
        auto_dump_position_risk: args.auto_dump_position_risk,
        auto_flatten_position_risk: args.auto_flatten_position_risk,
        position_risk_threshold_usdt: args.position_risk_threshold_usdt,
        position_snapshot_max_age_ms: (args.position_snapshot_max_age_secs as i64)
            .saturating_mul(1_000),
        flatten_window_ms: (args.flatten_window_hours as i64).saturating_mul(60 * 60 * 1_000),
        flatten_manual_threshold_usdt: args.flatten_manual_threshold_usdt,
        flatten_executor: FlattenExecutor::new(
            args.flatten_env_root.clone(),
            Duration::from_secs(args.flatten_timeout_secs.max(1)),
        )
        .with_sg_ssh_host(args.sg_flatten_ssh_host.clone())?,
        flatten_inflight: Arc::new(AsyncMutex::new(BTreeSet::new())),
        notification_client,
        flatten_api_token,
        snapshot_base_url: args.snapshot_base_url.clone(),
        snapshot_client: public_http_client()?,
        nav_strategies_url: args.nav_strategies_url.clone(),
        nav_login_url: format!(
            "{}auth/login",
            args.nav_strategies_url
                .trim_end_matches('/')
                .trim_end_matches("strategies")
        ),
        nav_username: args.nav_username.clone().filter(|u| !u.is_empty()),
        nav_password: args.nav_password.clone().filter(|p| !p.is_empty()),
        nav_session: Arc::new(RwLock::new(None)),
        cta_mapping: cta_mapping::Settings {
            env_root: args.redis_backup_env_root.clone(),
            factor_env: args.cta_factor_env.clone().unwrap_or_else(|| {
                args.redis_backup_env_root
                    .join(cta_factors::Worker::Perp.name())
            }),
            spread_factor_env: args.cta_spread_factor_env.clone().unwrap_or_else(|| {
                args.redis_backup_env_root
                    .join(cta_factors::Worker::Spread.name())
            }),
            jp_database: std::env::var("DELIST_CTA_JP_DATABASE_URL")
                .ok()
                .filter(|s| !s.is_empty()),
        },
        backups: ManualBackupSettings {
            enabled: !args.skip_redis_backup,
            fr_dir: args.redis_backup_dir.clone(),
            cta_dir: args.cta_backup_dir.clone(),
            env_root: args.redis_backup_env_root.clone(),
            sg_ssh_host: args.redis_backup_sg_ssh_host.clone(),
            keep: args.redis_backup_keep,
            fr_cos: args
                .redis_backup_cos_target
                .clone()
                .zip(args.redis_backup_cos_config.clone())
                .map(|(target, config_path)| upload::UploadConfig {
                    target,
                    config_path,
                }),
            cta_cos: args
                .cta_backup_cos_target
                .clone()
                .zip(args.redis_backup_cos_config.clone())
                .map(|(target, config_path)| upload::UploadConfig {
                    target,
                    config_path,
                }),
        },
        backup_lock: Arc::new(AsyncMutex::new(())),
    };

    if !args.skip_redis_backup {
        let backup_state = state.clone();
        let backup_dir = args.redis_backup_dir.clone();
        let env_root = args.redis_backup_env_root.clone();
        let sg_ssh_host = args.redis_backup_sg_ssh_host.clone();
        let keep = args.redis_backup_keep;
        let cos = args
            .redis_backup_cos_target
            .clone()
            .zip(args.redis_backup_cos_config.clone())
            .map(|(target, config_path)| upload::UploadConfig {
                target,
                config_path,
            });
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(300));
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            let mut completed = BTreeMap::new();
            let mut cleaned = BTreeMap::new();
            loop {
                interval.tick().await;
                if !nav_accounts_ready(&backup_state).await {
                    mark_err(
                        &backup_state,
                        "redis_fr_backup_catalog",
                        "backup",
                        "NAV strategy catalog is unavailable",
                    )
                    .await;
                    // The NAV refresh starts in a separate task. Do not delay
                    // the first backup by five minutes on a normal startup.
                    let deadline = Instant::now() + Duration::from_secs(300);
                    while Instant::now() < deadline {
                        time::sleep(Duration::from_secs(5)).await;
                        if nav_accounts_ready(&backup_state).await {
                            interval.reset_immediately();
                            break;
                        }
                    }
                    continue;
                }
                mark_ok(&backup_state, "redis_fr_backup_catalog", "backup").await;
                let accounts = backup_state.accounts.read().await.clone();
                for account in accounts.iter().filter(|a| a.kind == "funding_rate") {
                    let _backup_guard = backup_state.backup_lock.lock().await;
                    let today = Utc::now().date_naive();
                    let id = format!("{}:{}:{}", account.host, account.slug, account.exchange);
                    let source = format!("redis_fr_backup_{}", account.slug);
                    let target = match account.site {
                        mkt_signal::common::delist_accounts::RedisSite::Jp => {
                            Some(("jp", backup_state.jp_redis.as_str()))
                        }
                        mkt_signal::common::delist_accounts::RedisSite::Sg => {
                            backup_state.sg_redis.as_deref().map(|url| ("sg", url))
                        }
                        mkt_signal::common::delist_accounts::RedisSite::Unsupported => None,
                    };
                    if completed.get(&id) != Some(&today) {
                        let result = if let Some((site, url)) = target {
                            backup::backup_today(
                                &backup_dir,
                                site,
                                url,
                                account,
                                keep,
                                &env_root,
                                &sg_ssh_host,
                                cos.is_some(),
                            )
                            .await
                        } else {
                            Err(anyhow::anyhow!(
                                "FR account Redis site is unsupported or unavailable"
                            ))
                        };
                        match result {
                            Ok(backup::Outcome::Created(path)) => {
                                info!(
                                    "FR Redis backup completed env={} path={}",
                                    account.slug,
                                    path.display()
                                );
                                completed.insert(id.clone(), today);
                                mark_ok(&backup_state, &source, "backup").await;
                            }
                            Ok(backup::Outcome::AlreadyExists) => {
                                completed.insert(id.clone(), today);
                                mark_ok(&backup_state, &source, "backup").await;
                            }
                            Err(err) => {
                                let message = format!("{err:#}");
                                warn!("FR Redis backup failed env={}: {message}", account.slug);
                                mark_err(&backup_state, &source, "backup", &message).await;
                            }
                        }
                    }
                    if let Some(cos) = cos.as_ref() {
                        let source = format!("cos_fr_backup_{}", account.slug);
                        let result = if let Some((site, _)) = target {
                            upload::sync_archives(cos, &backup_dir, site, account, keep).await
                        } else {
                            Err(anyhow::anyhow!(
                                "FR account backup site is unsupported or unavailable"
                            ))
                        };
                        match result {
                            Ok(count) => {
                                if count > 0 {
                                    info!(
                                        "COS backup verified env={} uploaded={count}",
                                        account.slug
                                    );
                                }
                                mark_ok(&backup_state, &source, "upload").await;
                                if cleaned.get(&id) != Some(&today) {
                                    let cleanup_source =
                                        format!("cos_fr_retention_{}", account.slug);
                                    let result = if let Some((site, _)) = target {
                                        upload::prune_remote(cos, &backup_dir, site, account, keep)
                                            .await
                                    } else {
                                        Err(anyhow::anyhow!(
                                            "FR account backup site is unsupported or unavailable"
                                        ))
                                    };
                                    match result {
                                        Ok(deleted) => {
                                            if deleted > 0 {
                                                info!("COS retention completed env={} deleted={deleted}", account.slug);
                                            }
                                            cleaned.insert(id.clone(), today);
                                            mark_ok(&backup_state, &cleanup_source, "retention")
                                                .await;
                                        }
                                        Err(err) => {
                                            let message = format!("{err:#}");
                                            warn!(
                                                "COS retention failed env={}: {message}",
                                                account.slug
                                            );
                                            mark_err(
                                                &backup_state,
                                                &cleanup_source,
                                                "retention",
                                                &message,
                                            )
                                            .await;
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                let message = format!("{err:#}");
                                warn!("COS backup failed env={}: {message}", account.slug);
                                mark_err(&backup_state, &source, "upload", &message).await;
                            }
                        }
                    }
                }
            }
        });
    }

    if !args.skip_redis_backup {
        let backup_state = state.clone();
        let backup_dir = args.cta_backup_dir.clone();
        let env_root = args.redis_backup_env_root.clone();
        let keep = args.redis_backup_keep;
        let cos = args
            .cta_backup_cos_target
            .clone()
            .zip(args.redis_backup_cos_config.clone())
            .map(|(target, config_path)| upload::UploadConfig {
                target,
                config_path,
            });
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(300));
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            let mut completed = BTreeMap::new();
            let mut cleaned = BTreeMap::new();
            loop {
                interval.tick().await;
                for worker in [cta_factors::Worker::Perp, cta_factors::Worker::Spread] {
                    let _backup_guard = backup_state.backup_lock.lock().await;
                    let today = Utc::now().date_naive();
                    let account = cta_backup::account(worker);
                    let slug = &account.slug;
                    if completed.get(slug) != Some(&today) {
                        let result = match backup_state.cta_mapping.jp_database.as_deref() {
                            Some(url) => {
                                cta_backup::backup_today(
                                    &backup_dir,
                                    &env_root,
                                    &backup_state.jp_redis,
                                    url,
                                    backup_state.cta_mapping.factor_environment(worker),
                                    worker,
                                )
                                .await
                            }
                            None => Err(anyhow::anyhow!(
                                "CTA Manager read-only database is not configured"
                            )),
                        };
                        match result {
                            Ok(created) => {
                                if let backup::Outcome::Created(path) = created.outcome {
                                    info!(
                                        "CTA configuration backup created env={} path={}",
                                        slug,
                                        path.display()
                                    );
                                }
                                completed.insert(slug.clone(), today);
                                mark_ok(&backup_state, &format!("cta_backup_{slug}"), "backup")
                                    .await;
                                if created.warnings.is_empty() {
                                    mark_ok(
                                        &backup_state,
                                        &format!("cta_backup_quality_{slug}"),
                                        "backup",
                                    )
                                    .await;
                                } else {
                                    mark_err(
                                        &backup_state,
                                        &format!("cta_backup_quality_{slug}"),
                                        "backup",
                                        &created.warnings.join("; "),
                                    )
                                    .await;
                                }
                            }
                            Err(err) => {
                                mark_err(
                                    &backup_state,
                                    &format!("cta_backup_{slug}"),
                                    "backup",
                                    &format!("{err:#}"),
                                )
                                .await
                            }
                        }
                    }
                    if let Some(cos) = cos.as_ref() {
                        let source = format!("cos_cta_backup_{slug}");
                        match upload::sync_archives(cos, &backup_dir, "jp", &account, keep).await {
                            Ok(count) => {
                                if count > 0 {
                                    info!("CTA COS backup verified env={} uploaded={count}", slug);
                                }
                                mark_ok(&backup_state, &source, "upload").await;
                                let latest = backup::archive_paths(&backup_dir, "jp", &account)
                                    .ok()
                                    .and_then(|paths| paths.into_iter().last())
                                    .filter(|path| {
                                        path.file_name()
                                            .and_then(|name| name.to_str())
                                            .and_then(|name| name.strip_suffix(".tar.gz"))
                                            .and_then(backup::parse_archive_stem)
                                            .is_some_and(|stamp| stamp.date() == today)
                                    });
                                if let Some(path) = latest {
                                    match cta_backup::validated_warnings(&path, &account) {
                                        Ok(warnings) if warnings.is_empty() => {
                                            mark_ok(
                                                &backup_state,
                                                &format!("cta_backup_quality_{slug}"),
                                                "backup",
                                            )
                                            .await
                                        }
                                        Ok(warnings) => {
                                            mark_err(
                                                &backup_state,
                                                &format!("cta_backup_quality_{slug}"),
                                                "backup",
                                                &warnings.join("; "),
                                            )
                                            .await
                                        }
                                        Err(err) => {
                                            mark_err(
                                                &backup_state,
                                                &format!("cta_backup_quality_{slug}"),
                                                "backup",
                                                &format!("{err:#}"),
                                            )
                                            .await
                                        }
                                    }
                                }
                                if cleaned.get(slug) != Some(&today) {
                                    let cleanup = format!("cos_cta_retention_{slug}");
                                    match upload::prune_remote(
                                        cos,
                                        &backup_dir,
                                        "jp",
                                        &account,
                                        keep,
                                    )
                                    .await
                                    {
                                        Ok(deleted) => {
                                            if deleted > 0 {
                                                info!("CTA COS retention completed env={} deleted={deleted}", slug);
                                            }
                                            cleaned.insert(slug.clone(), today);
                                            mark_ok(&backup_state, &cleanup, "retention").await;
                                        }
                                        Err(err) => {
                                            mark_err(
                                                &backup_state,
                                                &cleanup,
                                                "retention",
                                                &format!("{err:#}"),
                                            )
                                            .await
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                mark_err(&backup_state, &source, "upload", &format!("{err:#}"))
                                    .await
                            }
                        }
                    } else {
                        mark_err(
                            &backup_state,
                            &format!("cos_cta_backup_{slug}"),
                            "upload",
                            "CTA COS target is not configured",
                        )
                        .await;
                    }
                }
            }
        });
    }

    let refresh = state.clone();
    let refresh_args = RefreshArgs {
        skip_llm: args.skip_llm,
        skip_announcements: args.skip_announcements,
        skip_official: args.skip_official,
        skip_ws: args.skip_ws,
        official_interval_secs: args.official_interval_secs,
        listing_interval_secs: args.listing_interval_secs,
        announcement_interval_secs: args.announcement_interval_secs,
        position_risk_interval_secs: args.position_risk_interval_secs,
        unimmr_list_interval_secs: args.unimmr_list_interval_secs,
        nav_strategy_interval_secs: args.nav_strategy_interval_secs,
        days: args.days,
        llm_max: args.llm_max,
        force_llm_ids: args.force_llm_ids.clone(),
    };
    tokio::spawn(async move {
        if let Err(err) = run_refresh(refresh, refresh_args).await {
            warn!("delist refresh loop exited: {err:#}");
        }
    });

    if let Some(notifications) = state.fr_notifications.clone() {
        tokio::spawn(notifications.run());
    }
    let exposure_state = state.clone();
    let exposure_client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()?;
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(
            exposure_state.exposure_settings.interval_secs,
        ));
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let specs = exposure_state.accounts.read().await.clone();
            let samples = exposure::collect(
                &exposure_client,
                &exposure_state.snapshot_base_url,
                &specs,
                &exposure_state.exposure_settings,
            )
            .await;
            let current = exposure_catalog_current(&exposure_state).await;
            let now = Utc::now().timestamp_millis();
            let risk_observations = samples
                .iter()
                .map(|(slug, sample)| {
                    notifications::unimmr_observation(slug, sample.as_ref().ok().map(|s| &s.risk))
                })
                .collect::<Vec<_>>();
            let report = {
                let mut monitor = exposure_state.exposure.write().await;
                monitor.apply(&specs, samples, now, current);
                monitor.report(now, current)
            };
            if !specs.is_empty() {
                if let Some(notifier) = &exposure_state.fr_notifications {
                    let mut observations = notifier.exposure_observations(&report).await;
                    observations.extend(risk_observations);
                    let sources = exposure_state
                        .status
                        .read()
                        .await
                        .snapshot(exposure_state.store.is_some());
                    let active_fr: BTreeSet<_> =
                        report.accounts.iter().map(|a| a.slug.clone()).collect();
                    observations.extend(
                        sources
                            .sources
                            .iter()
                            .filter(|s| notifications::fr_source(&s.source, &active_fr))
                            .map(|s| notifications::Observation {
                                key: format!("source/{}", s.source),
                                title: format!("监控数据源 {}", s.source),
                                problem: Some(!s.ok),
                                detail: if s.ok {
                                    "数据源检查已恢复".into()
                                } else {
                                    "数据源检查失败；错误详情见 Delist 状态页与服务日志".into()
                                },
                            }),
                    );
                    if let Err(err) = notifier.observe(observations).await {
                        warn!("queue FR observations failed: {err}");
                    }
                }
            }
            // Same report as /exposures, retained locally for operator diagnostics.
            let path = exposure_state
                .book_path
                .with_file_name("fr_exposure_check.json");
            let temporary = path.with_extension("json.tmp");
            let saved = async {
                tokio::fs::write(&temporary, serde_json::to_vec(&report)?).await?;
                tokio::fs::rename(&temporary, &path).await?;
                Ok::<_, anyhow::Error>(())
            }
            .await;
            if let Err(err) = saved {
                warn!("save FR exposure report failed: {err:#}");
            }
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
        .route("/exposures", get(query_exposures))
        .route("/notifications", get(query_notifications))
        .route("/cta-strategies", get(query_cta_strategies))
        .route("/backup-now", post(manual_backup))
        .route("/removal-candidates", get(query_removal_candidates))
        .route("/removals", get(query_removals))
        .route("/dump-candidates", get(query_dump_candidates))
        .route("/dump-transitions", get(query_dump_transitions))
        .route("/flatten-candidates", get(query_flatten_candidates))
        .route("/flatten-executions", get(query_flatten_executions))
        .route("/flatten", post(manual_flatten))
        .with_state(state);

    let app = access::protect(app, access);

    let addr: SocketAddr = args.bind.parse().context("invalid --bind")?;
    info!(
        "delist_risk_server listening at http://{addr} official={}s listings={}s announcements={}s nav_strategies={}s auto_remove_redis={} auto_dump_position_risk={} auto_flatten_position_risk={} position_scan={}s dump_threshold={}U flatten_window={}h manual_threshold={}U data_persistence={}s",
        args.official_interval_secs,
        args.listing_interval_secs,
        args.announcement_interval_secs,
        args.nav_strategy_interval_secs,
        args.auto_remove_redis,
        args.auto_dump_position_risk,
        args.auto_flatten_position_risk,
        args.position_risk_interval_secs,
        args.position_risk_threshold_usdt,
        args.flatten_window_hours,
        args.flatten_manual_threshold_usdt,
        args.data_persistence_secs,
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
    let latest_llm = state.status.read().await.llm_latest_by_exchange();
    let book = state.book.read().await;
    let latest_announcements =
        mkt_signal::common::delist_accounts::latest_announcement_views(&book, latest_llm);
    let mut risk = book.query(&to_query(&params, state.default_days));
    let listings = state.listings.read().await.clone();
    listings.decorate(&mut risk);
    drop(book);
    let account_specs = state.accounts.read().await.clone();
    let (universes, unimmr_lists) = tokio::join!(
        load_universes(&account_specs, &state.jp_redis, state.sg_redis.as_deref()),
        load_fr_symbol_list(
            &account_specs,
            "unimmr_close_symbols",
            &state.jp_redis,
            state.sg_redis.as_deref()
        )
    );
    let mut accounts = build_account_views(&account_specs, &risk, &listings, &universes);
    for account in &mut accounts {
        if account.kind == "funding_rate" {
            account.viz_url = Some(format!("/delist/open?env={}&panel=viz", account.slug));
            account.config_url = Some(format!("/delist/open?env={}&panel=config", account.slug));
        }
    }
    decorate_unimmr_lists(&mut accounts, &unimmr_lists);
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
        nav_accounts_current: nav_accounts_ready(&state).await,
        redis,
        summary: summarize(&accounts),
        accounts,
        latest_announcements,
    })
}

async fn query_cta_strategies(
    State(state): State<AppState>,
    Query(params): Query<RiskParams>,
) -> impl IntoResponse {
    let book = state.book.read().await;
    let mut risk = book.query(&to_query(&params, state.default_days));
    let listings = state.listings.read().await.clone();
    listings.decorate(&mut risk);
    drop(book);
    let mut response = cta_mapping::load(&state.cta_mapping, &risk, &listings).await;
    cta_mapping::apply_source_health(&mut response, &*state.status.read().await);
    Json(response)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManualBackupRequest {
    kind: String,
    env: String,
}

async fn manual_backup(
    State(state): State<AppState>,
    Json(request): Json<ManualBackupRequest>,
) -> Response {
    let fail = |code: StatusCode, message: String| {
        (code, Json(json!({"ok": false, "error": message}))).into_response()
    };
    if !state.backups.enabled {
        return fail(StatusCode::SERVICE_UNAVAILABLE, "备份功能已关闭".into());
    }
    let Ok(_guard) = state.backup_lock.try_lock() else {
        return fail(StatusCode::CONFLICT, "备份正在进行，请稍后重试".into());
    };
    let (account, site, dir, cos, created, warnings) = match request.kind.as_str() {
        "funding_rate" => {
            if !nav_accounts_ready(&state).await {
                return fail(StatusCode::SERVICE_UNAVAILABLE, "策略名单暂不可用".into());
            }
            let Some(account) = state
                .accounts
                .read()
                .await
                .iter()
                .find(|a| a.kind == "funding_rate" && a.slug == request.env)
                .cloned()
            else {
                return fail(StatusCode::NOT_FOUND, "未找到该 FR 策略".into());
            };
            let (site, redis) = match account.site {
                mkt_signal::common::delist_accounts::RedisSite::Jp => {
                    ("jp", Some(state.jp_redis.as_str()))
                }
                mkt_signal::common::delist_accounts::RedisSite::Sg => {
                    ("sg", state.sg_redis.as_deref())
                }
                mkt_signal::common::delist_accounts::RedisSite::Unsupported => ("", None),
            };
            let Some(redis) = redis else {
                return fail(StatusCode::SERVICE_UNAVAILABLE, "策略 Redis 不可用".into());
            };
            let Some(cos) = state.backups.fr_cos.as_ref() else {
                return fail(StatusCode::SERVICE_UNAVAILABLE, "FR 云端备份未配置".into());
            };
            let result = backup::backup_manual(
                &state.backups.fr_dir,
                site,
                redis,
                &account,
                state.backups.keep,
                &state.backups.env_root,
                &state.backups.sg_ssh_host,
                true,
            )
            .await;
            let path = match result {
                Ok(backup::Outcome::Created(path)) => path,
                Ok(backup::Outcome::AlreadyExists) => {
                    return fail(StatusCode::CONFLICT, "备份版本冲突，请重试".into())
                }
                Err(err) => {
                    return fail(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("本地备份失败：{err:#}"),
                    )
                }
            };
            (
                account,
                site,
                state.backups.fr_dir.clone(),
                cos.clone(),
                path,
                Vec::new(),
            )
        }
        "cta_perp" | "cta_spread" => {
            let Some(worker) = cta_backup::worker(&request.env) else {
                return fail(StatusCode::BAD_REQUEST, "无效的 CTA 共享进程".into());
            };
            let account = cta_backup::account(worker);
            if request.kind != account.kind {
                return fail(StatusCode::BAD_REQUEST, "CTA 备份类型与进程不一致".into());
            }
            let Some(url) = state.cta_mapping.jp_database.as_deref() else {
                mark_err(
                    &state,
                    &format!("cta_backup_{}", account.slug),
                    "backup",
                    "CTA Manager database is not configured",
                )
                .await;
                return fail(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Manager 数据库未配置".into(),
                );
            };
            let Some(cos) = state.backups.cta_cos.as_ref() else {
                mark_err(
                    &state,
                    &format!("cos_cta_backup_{}", account.slug),
                    "upload",
                    "CTA COS target is not configured",
                )
                .await;
                return fail(StatusCode::SERVICE_UNAVAILABLE, "CTA 云端备份未配置".into());
            };
            let result = cta_backup::backup_manual(
                &state.backups.cta_dir,
                &state.backups.env_root,
                &state.jp_redis,
                url,
                state.cta_mapping.factor_environment(worker),
                worker,
            )
            .await;
            let created = match result {
                Ok(created) => created,
                Err(err) => {
                    mark_err(
                        &state,
                        &format!("cta_backup_{}", account.slug),
                        "backup",
                        &format!("{err:#}"),
                    )
                    .await;
                    return fail(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("本地备份失败：{err:#}"),
                    );
                }
            };
            let path = match created.outcome {
                backup::Outcome::Created(path) => path,
                backup::Outcome::AlreadyExists => {
                    return fail(StatusCode::CONFLICT, "备份版本冲突，请重试".into())
                }
            };
            (
                account,
                "jp",
                state.backups.cta_dir.clone(),
                cos.clone(),
                path,
                created.warnings,
            )
        }
        _ => return fail(StatusCode::BAD_REQUEST, "不支持的策略类型".into()),
    };
    let filename = created
        .file_name()
        .and_then(|x| x.to_str())
        .unwrap_or("backup.tar.gz")
        .to_string();
    let is_cta = matches!(account.kind.as_str(), "cta_perp" | "cta_spread");
    let local_source = if is_cta {
        format!("cta_backup_{}", account.slug)
    } else {
        format!("redis_fr_backup_{}", account.slug)
    };
    let cloud_source = if is_cta {
        format!("cos_cta_backup_{}", account.slug)
    } else {
        format!("cos_fr_backup_{}", account.slug)
    };
    let retention_source = if is_cta {
        format!("cos_cta_retention_{}", account.slug)
    } else {
        format!("cos_fr_retention_{}", account.slug)
    };
    mark_ok(&state, &local_source, "backup").await;
    if is_cta {
        let quality = format!("cta_backup_quality_{}", account.slug);
        if warnings.is_empty() {
            mark_ok(&state, &quality, "backup").await;
        } else {
            mark_err(&state, &quality, "backup", &warnings.join("; ")).await;
        }
    }
    if let Err(err) = upload::sync_archives(&cos, &dir, site, &account, state.backups.keep).await {
        mark_err(&state, &cloud_source, "upload", &format!("{err:#}")).await;
        return fail(
            StatusCode::BAD_GATEWAY,
            format!("本地备份已保存，但云端上传或校验失败；后台会重试：{err:#}"),
        );
    }
    mark_ok(&state, &cloud_source, "upload").await;
    let retention_warning =
        match upload::prune_remote(&cos, &dir, site, &account, state.backups.keep).await {
            Ok(_) => {
                mark_ok(&state, &retention_source, "retention").await;
                None
            }
            Err(err) => {
                mark_err(&state, &retention_source, "retention", &format!("{err:#}")).await;
                Some(format!("备份已上传并校验，但旧备份清理失败：{err:#}"))
            }
        };
    Json(json!({"ok":true,"environment":account.slug,"filename":filename,"verified":true,"warnings":warnings,"retention_warning":retention_warning})).into_response()
}

async fn query_removal_candidates(State(state): State<AppState>) -> impl IntoResponse {
    let listings = state.listings.read().await.clone();
    let accounts = state.accounts.read().await.clone();
    let universes = load_universes(&accounts, &state.jp_redis, state.sg_redis.as_deref()).await;
    let redis_errors = removal_universe_errors(&accounts, &universes);
    let candidates = confirmed_removal_candidates(&accounts, &listings, &universes);
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
        "nav_accounts_current": nav_accounts_ready(&state).await,
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

async fn query_dump_candidates(State(state): State<AppState>) -> Response {
    let accounts = state.accounts.read().await.clone();
    match collect_position_candidates(&state, &accounts, state.position_risk_threshold_usdt).await {
        Ok((items, snapshot_errors)) => Json(json!({
            "ok": true,
            "auto_dump_position_risk": state.auto_dump_position_risk,
            "threshold_usdt": state.position_risk_threshold_usdt,
            "snapshot_errors": snapshot_errors,
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

async fn query_dump_transitions(
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
    match store.load_redis_dumps(params.limit.unwrap_or(200)).await {
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

#[derive(Debug, Clone, Serialize)]
struct FlattenCandidateView {
    #[serde(flatten)]
    candidate: FlattenCandidate,
    #[serde(skip_serializing_if = "Option::is_none")]
    latest: Option<FlattenExecutionAudit>,
}

async fn query_flatten_candidates(State(state): State<AppState>) -> Response {
    let accounts = state.accounts.read().await.clone();
    let (collected, dump_symbols) = tokio::join!(
        collect_flatten_candidates(&state, &accounts),
        load_fr_dump_symbols(&accounts, &state.jp_redis, state.sg_redis.as_deref())
    );
    let (items, snapshot_errors, positioned) = match collected {
        Ok(result) => result,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"ok": false, "error": format!("{err:#}")})),
            )
                .into_response();
        }
    };
    let audits = match state.store.as_ref() {
        Some(store) => match store.load_flatten_executions(500).await {
            Ok(rows) => rows,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"ok": false, "error": format!("{err:#}")})),
                )
                    .into_response();
            }
        },
        None => Vec::new(),
    };
    let dump_errors = dump_symbols
        .iter()
        .filter_map(|(account, result)| {
            result.as_ref().err().map(|err| format!("{account}: {err}"))
        })
        .collect::<Vec<_>>();
    let position_statuses =
        position_close_statuses(&positioned, &dump_symbols, POSITION_CLOSED_THRESHOLD_USDT);
    let mut latest = BTreeMap::new();
    for audit in audits {
        latest
            .entry((
                audit.account_slug.clone(),
                audit.symbol.clone(),
                audit.deadline_ms,
            ))
            .or_insert(audit);
    }
    let views = items
        .into_iter()
        .map(|candidate| {
            let key = (
                candidate.account_slug.clone(),
                candidate.symbol.clone(),
                candidate.deadline_ms,
            );
            FlattenCandidateView {
                candidate,
                latest: latest.remove(&key),
            }
        })
        .collect::<Vec<_>>();
    Json(json!({
        "ok": true,
        "auto_flatten_position_risk": state.auto_flatten_position_risk,
        "window_ms": state.flatten_window_ms,
        "manual_threshold_usdt": state.flatten_manual_threshold_usdt,
        "position_closed_threshold_usdt": POSITION_CLOSED_THRESHOLD_USDT,
        "snapshot_errors": snapshot_errors,
        "dump_errors": dump_errors,
        "position_statuses": position_statuses,
        "count": views.len(),
        "items": views,
    }))
    .into_response()
}

async fn query_flatten_executions(
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
    match store
        .load_flatten_executions(params.limit.unwrap_or(200))
        .await
    {
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

#[derive(Debug, Deserialize)]
struct ManualFlattenRequest {
    account_slug: String,
    symbol: String,
}

async fn manual_flatten(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ManualFlattenRequest>,
) -> Response {
    let Some(expected_token) = state.flatten_api_token.as_deref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"ok": false, "error": "manual flatten API token is not configured"})),
        )
            .into_response();
    };
    let supplied_token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or("");
    if !constant_time_equal(supplied_token.as_bytes(), expected_token.as_bytes()) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"ok": false, "error": "invalid manual flatten token"})),
        )
            .into_response();
    }
    let Some(store) = state.store.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"ok": false, "error": "postgres unavailable"})),
        )
            .into_response();
    };
    if !nav_accounts_ready(&state).await {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"ok": false, "error": "NAV strategy catalog is not current"})),
        )
            .into_response();
    }
    let accounts = state.accounts.read().await.clone();
    let (candidates, snapshot_errors, positioned) =
        match collect_flatten_candidates(&state, &accounts).await {
            Ok(result) => result,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"ok": false, "error": format!("{err:#}")})),
                )
                    .into_response();
            }
        };
    let Some(candidate) = candidates.into_iter().find(|candidate| {
        candidate.account_slug == request.account_slug && candidate.symbol == request.symbol
    }) else {
        return (
            StatusCode::CONFLICT,
            Json(json!({
                "ok": false,
                "error": "symbol is not a current final-24-hour positioned delist candidate",
                "snapshot_errors": snapshot_errors,
            })),
        )
            .into_response();
    };
    let Some(positioned) = positioned.into_iter().find(|positioned| {
        positioned.account_slug == candidate.account_slug
            && positioned.symbol == candidate.symbol
            && positioned.delist_utc.as_deref() == Some(candidate.delist_utc.as_str())
    }) else {
        return (
            StatusCode::CONFLICT,
            Json(json!({"ok": false, "error": "position candidate changed during validation"})),
        )
            .into_response();
    };
    match store
        .flatten_success_covers_snapshot(
            &candidate.account_slug,
            &candidate.symbol,
            candidate.deadline_ms,
            candidate.snapshot_ms,
        )
        .await
    {
        Ok(true) => {
            return (
                StatusCode::CONFLICT,
                Json(json!({
                    "ok": false,
                    "error": "latest snapshot predates a successful flatten; wait for a fresh position snapshot",
                })),
            )
                .into_response();
        }
        Ok(false) => {}
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"ok": false, "error": format!("{err:#}")})),
            )
                .into_response();
        }
    }
    let inflight_key = format!("{}:{}", candidate.account_slug, candidate.symbol);
    {
        let mut inflight = state.flatten_inflight.lock().await;
        if !inflight.insert(inflight_key.clone()) {
            return (
                StatusCode::CONFLICT,
                Json(json!({"ok": false, "error": "flatten is already running"})),
            )
                .into_response();
        }
    }
    let result = async {
        let dedup_key = format!(
            "manual:{}:{}:{}:{}",
            candidate.account_slug,
            candidate.symbol,
            candidate.deadline_ms,
            Utc::now().timestamp_micros()
        );
        execute_flatten_with_audit(&state, store, &candidate, &positioned, "manual", &dedup_key)
            .await
    }
    .await;
    state.flatten_inflight.lock().await.remove(&inflight_key);

    match result {
        Ok(Some((audit_id, output))) if output.success => Json(json!({
            "ok": true,
            "audit_id": audit_id,
            "result": output,
        }))
        .into_response(),
        Ok(Some((audit_id, output))) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({
                "ok": false,
                "audit_id": audit_id,
                "error": "flatten script failed",
                "result": output,
            })),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::CONFLICT,
            Json(json!({"ok": false, "error": "flatten request was already handled"})),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"ok": false, "error": format!("{err:#}")})),
        )
            .into_response(),
    }
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |diff, (left, right)| diff | (left ^ right))
        == 0
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
    position_risk_interval_secs: u64,
    unimmr_list_interval_secs: u64,
    nav_strategy_interval_secs: u64,
    days: i64,
    llm_max: usize,
    force_llm_ids: Option<String>,
}

async fn refresh_nav_account_catalog(state: &AppState, client: &Client) -> bool {
    let was_ready = nav_accounts_ready(state).await;
    let session = state.nav_session.read().await.clone();
    let mut result =
        fetch_nav_accounts(client, &state.nav_strategies_url, session.as_deref()).await;
    let unauthorized = result
        .as_ref()
        .err()
        .is_some_and(|err| format!("{err:#}").contains(NAV_UNAUTHORIZED));
    if unauthorized {
        *state.nav_session.write().await = None;
        if let (Some(user), Some(pass)) = (&state.nav_username, &state.nav_password) {
            match nav_login(client, &state.nav_login_url, user, pass).await {
                Ok(token) => {
                    info!("NAV login refreshed session");
                    *state.nav_session.write().await = Some(token.clone());
                    result =
                        fetch_nav_accounts(client, &state.nav_strategies_url, Some(&token)).await;
                }
                Err(err) => {
                    warn!("NAV login failed: {err:#}");
                }
            }
        }
    }
    match result {
        Ok(accounts) => {
            let count = accounts.len();
            let changed = state.accounts.read().await.as_slice() != accounts.as_slice();
            *state.accounts.write().await = accounts;
            mark_ok(state, "nav_strategies", "fetch").await;
            info!("NAV strategy catalog refreshed accounts={count}");
            !was_ready || changed
        }
        Err(err) => {
            warn!("NAV strategy catalog refresh failed; retaining cached accounts: {err:#}");
            mark_err(state, "nav_strategies", "fetch", &format!("{err:#}")).await;
            false
        }
    }
}

async fn nav_accounts_ready(state: &AppState) -> bool {
    let source_ok = state
        .status
        .read()
        .await
        .source("nav_strategies")
        .is_some_and(|status| status.ok);
    source_ok && !state.accounts.read().await.is_empty()
}

async fn exposure_catalog_current(state: &AppState) -> bool {
    let now = Utc::now().timestamp_millis();
    state
        .status
        .read()
        .await
        .source("nav_strategies")
        .is_some_and(|s| {
            s.ok && s.last_success_ms.is_some_and(|ts| {
                ts > 0
                    && ts <= now + 5_000
                    && now.saturating_sub(ts)
                        <= state.exposure_settings.catalog_max_age_secs as i64 * 1_000
            })
        })
}

async fn query_exposures(State(state): State<AppState>) -> Json<exposure::Report> {
    let current = exposure_catalog_current(&state).await;
    Json(
        state
            .exposure
            .read()
            .await
            .report(Utc::now().timestamp_millis(), current),
    )
}

async fn query_notifications(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(match &state.fr_notifications {
        Some(notifier) => notifier.status().await,
        None => json!({"enabled":false}),
    })
}

async fn fr_event(state: &AppState, kind: &str, message: String) -> bool {
    if let Some(notifier) = &state.fr_notifications {
        if let Err(err) = notifier.event(kind, message).await {
            warn!("queue FR event failed kind={kind}: {err}");
            return false;
        }
    }
    true
}

async fn fr_heartbeat(state: &AppState) {
    let Some(notifier) = &state.fr_notifications else {
        return;
    };
    let current = exposure_catalog_current(state).await;
    let report = state
        .exposure
        .read()
        .await
        .report(Utc::now().timestamp_millis(), current);
    let sources = state.status.read().await.snapshot(state.store.is_some());
    let active_fr: BTreeSet<_> = report.accounts.iter().map(|a| a.slug.clone()).collect();
    let failures = sources
        .sources
        .iter()
        .filter(|s| !s.ok && notifications::fr_source(&s.source, &active_fr))
        .map(|s| s.source.as_str())
        .collect::<Vec<_>>();
    let summary = format!("FR 覆盖 {} 个：敞口告警 {}，数据不完整 {}\n公告及监控源：{}\n本轮按现有配置检查；没有新增策略、手工下单或调仓。自动风控实际操作列在下方日志中。", report.accounts.len(), report.accounts.iter().filter(|a| a.risk_status == "alert").count(), report.accounts.iter().filter(|a| !a.complete).count(), if failures.is_empty() { "全部已报告成功".into() } else { format!("存在失败/未知：{}", failures.join(", ")) });
    if let Err(err) = notifier.heartbeat(&summary).await {
        warn!("queue FR heartbeat failed: {err}");
    }
}

async fn run_refresh(state: AppState, args: RefreshArgs) -> Result<()> {
    let public = public_http_client()?;
    let nav_client = Client::builder()
        .timeout(Duration::from_secs(15))
        .no_proxy()
        .build()
        .context("build NAV HTTP client")?;
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

    if !args.skip_ws {
        let ws_state = state.clone();
        let ws_llm = llm.clone();
        let ws_client = llm_client.clone();
        let ws_budget = llm_budget.clone();
        tokio::spawn(async move {
            loop {
                let result =
                    gate_ws_session(&ws_state, ws_llm.as_ref(), ws_client.as_ref(), &ws_budget)
                        .await;
                match result {
                    Ok(()) => mark_ok(&ws_state, "gate_ws", "ws").await,
                    Err(err) => {
                        warn!("Gate announcement ws session ended: {err:#}");
                        mark_err(&ws_state, "gate_ws", "ws", &format!("{err:#}")).await;
                    }
                }
                time::sleep(Duration::from_secs(3)).await;
            }
        });
    }

    refresh_nav_account_catalog(&state, &nav_client).await;

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
        let ann_state = state.clone();
        let ann_llm = llm.clone();
        let ann_client = llm_client.clone();
        let ann_budget = llm_budget.clone();
        let public = public.clone();
        let binance = binance.clone();
        let interval_secs = args.announcement_interval_secs.max(60);
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(interval_secs));
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                refresh_announcements(
                    &ann_state,
                    &public,
                    &binance,
                    ann_llm.as_ref(),
                    ann_client.as_ref(),
                    &ann_budget,
                )
                .await;
                backfill_pending_llm(
                    &ann_state,
                    ann_llm.as_ref(),
                    ann_client.as_ref(),
                    &ann_budget,
                    &BTreeSet::new(),
                )
                .await;
                persist(&ann_state).await;
                fr_heartbeat(&ann_state).await;
            }
        });
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
    if state.auto_dump_position_risk || state.auto_flatten_position_risk {
        run_position_risk_scan(&state).await;
    }
    let mut unimmr_watch = UnimmrListWatch::default();
    run_unimmr_list_scan(&state, &mut unimmr_watch).await;

    let mut official = time::interval(Duration::from_secs(args.official_interval_secs.max(60)));
    official.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    official.tick().await;
    let mut listings = time::interval(Duration::from_secs(args.listing_interval_secs.max(60)));
    listings.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    listings.tick().await;
    let mut position_risk = time::interval(Duration::from_secs(
        args.position_risk_interval_secs.max(10),
    ));
    position_risk.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    position_risk.tick().await;
    let mut unimmr_list =
        time::interval(Duration::from_secs(args.unimmr_list_interval_secs.max(10)));
    unimmr_list.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    unimmr_list.tick().await;
    let mut nav_strategies =
        time::interval(Duration::from_secs(args.nav_strategy_interval_secs.max(10)));
    nav_strategies.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    nav_strategies.tick().await;
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
            _ = position_risk.tick(), if state.auto_dump_position_risk || state.auto_flatten_position_risk => {
                run_position_risk_scan(&state).await;
            }
            _ = unimmr_list.tick() => {
                run_unimmr_list_scan(&state, &mut unimmr_watch).await;
            }
            _ = nav_strategies.tick() => {
                if refresh_nav_account_catalog(&state, &nav_client).await {
                    if state.auto_remove_redis {
                        run_confirmed_delist_prune(&state).await;
                    }
                    if state.auto_dump_position_risk || state.auto_flatten_position_risk {
                        run_position_risk_scan(&state).await;
                    }
                }
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

#[derive(Debug)]
struct ListingRefreshFailure(Vec<(String, String)>);
impl std::fmt::Display for ListingRefreshFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.0
                .iter()
                .map(|(source, error)| format!("{source}: {error}"))
                .collect::<Vec<_>>()
                .join("; ")
        )
    }
}
impl std::error::Error for ListingRefreshFailure {}

fn snapshot_failure_source(error: &anyhow::Error) -> &'static str {
    if error
        .downcast_ref::<ListingRefreshFailure>()
        .is_some_and(|failure| {
            !failure.0.is_empty()
                && failure
                    .0
                    .iter()
                    .all(|(source, _)| source.starts_with("okex_"))
        })
    {
        "okex_exchange_symbol_snapshot"
    } else {
        "exchange_symbol_snapshot"
    }
}

async fn refresh_listings(state: &AppState, public: &Client) -> Result<ListingIndex> {
    let (index, errors) = fetch_listing_index(public).await;
    // A failed source cannot freeze healthy exchange catalogs or certify an old one.
    let mut current = state.listings.write().await;
    current.merge_refresh(index.clone());
    drop(current);
    for venue in ["okex-margin", "okex-futures"] {
        let source = if venue == "okex-margin" {
            "okex_spot_exchange_info"
        } else {
            "okex_futures_exchange_info"
        };
        if let Some((_, error)) = errors.iter().find(|(name, _)| name == source) {
            mark_err(state, source, "fetch", error).await;
        } else {
            mark_ok(state, source, "fetch").await;
        }
    }
    if let Some(notifier) = &state.fr_notifications {
        let rows = index
            .snapshot_rows()
            .into_iter()
            .filter(|row| row.exchange != "okex")
            .collect::<Vec<_>>();
        if let Err(err) = notifier.listings(&rows).await {
            warn!("queue listing changes failed: {err}");
        }
    }
    let legacy_errors = errors
        .iter()
        .filter(|(source, _)| !source.starts_with("okex_"))
        .collect::<Vec<_>>();
    if legacy_errors.is_empty() {
        mark_ok(state, "exchange_info", "fetch").await;
        if state.auto_remove_redis {
            run_confirmed_delist_prune(state).await;
        }
    } else {
        let error = legacy_errors
            .iter()
            .map(|(source, err)| format!("{source}: {err}"))
            .collect::<Vec<_>>()
            .join("; ");
        warn!("partial exchange_info refresh: {error}");
        mark_err(state, "exchange_info", "fetch", &error).await;
    }
    if !errors.is_empty() {
        return Err(ListingRefreshFailure(errors).into());
    }
    // Only current successful venues are persisted; failed books stay unavailable in memory.
    Ok(index)
}

async fn run_confirmed_delist_prune(state: &AppState) {
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
            if index.has_venue("okex-margin") && index.has_venue("okex-futures") {
                mark_ok(state, "okex_exchange_symbol_snapshot", "persist").await;
            }
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
    mark_err(state, snapshot_failure_source(err), "persist", &message).await;
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

async fn collect_position_candidates(
    state: &AppState,
    accounts: &[AccountSpec],
    threshold_usdt: f64,
) -> Result<(Vec<PositionDumpCandidate>, Vec<String>)> {
    let book = state.book.read().await;
    let mut risk = book.query(&RiskQuery {
        venue: None,
        exchange: None,
        days: Some(state.default_days),
        include_past: false,
    });
    let listings = state.listings.read().await.clone();
    listings.decorate(&mut risk);
    drop(book);
    Ok(position_dump_candidates(
        &state.snapshot_client,
        &state.snapshot_base_url,
        accounts,
        &risk,
        threshold_usdt,
        state.position_snapshot_max_age_ms,
    )
    .await)
}

async fn collect_flatten_candidates(
    state: &AppState,
    accounts: &[AccountSpec],
) -> Result<(
    Vec<FlattenCandidate>,
    Vec<String>,
    Vec<PositionDumpCandidate>,
)> {
    let (positioned, snapshot_errors) = collect_position_candidates(state, accounts, 0.0).await?;
    let candidates = flatten_candidates(
        &positioned,
        Utc::now().timestamp_millis(),
        state.flatten_window_ms,
        state.flatten_manual_threshold_usdt,
    );
    Ok((candidates, snapshot_errors, positioned))
}

async fn run_position_risk_scan(state: &AppState) {
    let Some(store) = state.store.as_ref() else {
        let message = "postgres is required before automatic FR position handling";
        if state.auto_dump_position_risk {
            mark_err(state, "redis_delist_dump", "mutation", message).await;
        }
        if state.auto_flatten_position_risk {
            mark_err(state, "delist_flatten", "execution", message).await;
        }
        return;
    };
    if !nav_accounts_ready(state).await {
        let message = "NAV strategy catalog is not current; automatic position handling paused";
        if state.auto_dump_position_risk {
            mark_err(state, "redis_delist_dump", "mutation", message).await;
        }
        if state.auto_flatten_position_risk {
            mark_err(state, "delist_flatten", "execution", message).await;
        }
        return;
    }
    let accounts = state.accounts.read().await.clone();
    let (positioned, snapshot_errors) =
        match collect_position_candidates(state, &accounts, 0.0).await {
            Ok(result) => result,
            Err(err) => {
                let message = format!("{err:#}");
                if state.auto_dump_position_risk {
                    mark_err(state, "redis_delist_dump", "mutation", &message).await;
                }
                if state.auto_flatten_position_risk {
                    mark_err(state, "delist_flatten", "execution", &message).await;
                }
                return;
            }
        };
    let flatten = flatten_candidates(
        &positioned,
        Utc::now().timestamp_millis(),
        state.flatten_window_ms,
        state.flatten_manual_threshold_usdt,
    );

    let mut errors = snapshot_errors.clone();
    let mut changed = 0usize;
    let mut dump_keys = BTreeSet::new();
    let flatten_keys = flatten
        .iter()
        .map(|candidate| (candidate.account_slug.as_str(), candidate.symbol.as_str()))
        .collect::<BTreeSet<_>>();
    for candidate in &positioned {
        let normal_dump = state.auto_dump_position_risk
            && candidate.impacted_position_usdt > state.position_risk_threshold_usdt;
        let final_window_dump = state.auto_flatten_position_risk
            && flatten_keys.contains(&(candidate.account_slug.as_str(), candidate.symbol.as_str()));
        if !normal_dump && !final_window_dump {
            continue;
        }
        if !dump_keys.insert((candidate.account_slug.clone(), candidate.symbol.clone())) {
            continue;
        }
        let mut candidate = candidate.clone();
        candidate.threshold_usdt = if normal_dump {
            state.position_risk_threshold_usdt
        } else {
            0.0
        };
        match dump_position_candidate(state, store, &candidate).await {
            Ok(did_change) => changed += usize::from(did_change),
            Err(err) => errors.push(format!(
                "account={} symbol={}: {err:#}",
                candidate.account_slug, candidate.symbol
            )),
        }
    }
    if state.auto_dump_position_risk || state.auto_flatten_position_risk {
        if errors.is_empty() {
            mark_ok(state, "redis_delist_dump", "mutation").await;
        } else {
            let message = errors.join("; ");
            warn!("FR positioned-delist scan degraded: {message}");
            mark_err(state, "redis_delist_dump", "mutation", &message).await;
        }
        info!(
            "FR positioned-delist scan candidates={} flatten_candidates={} changes={changed}",
            positioned
                .iter()
                .filter(|candidate| candidate.impacted_position_usdt
                    > state.position_risk_threshold_usdt)
                .count(),
            flatten.len(),
        );
    }

    if state.auto_flatten_position_risk {
        let mut flatten_errors = snapshot_errors;
        for candidate in &flatten {
            let result = if candidate.disposition == "manual" {
                record_manual_flatten_required(state, store, candidate).await
            } else {
                match positioned.iter().find(|positioned| {
                    positioned.account_slug == candidate.account_slug
                        && positioned.symbol == candidate.symbol
                        && positioned.delist_utc.as_deref() == Some(candidate.delist_utc.as_str())
                }) {
                    Some(positioned) => {
                        auto_flatten_candidate(state, store, candidate, positioned).await
                    }
                    None => Err(anyhow::anyhow!("position candidate changed during scan")),
                }
            };
            if let Err(err) = result {
                flatten_errors.push(format!(
                    "account={} symbol={}: {err:#}",
                    candidate.account_slug, candidate.symbol
                ));
            }
        }
        if flatten_errors.is_empty() {
            mark_ok(state, "delist_flatten", "execution").await;
        } else {
            let message = flatten_errors.join("; ");
            warn!("FR delist flatten scan degraded: {message}");
            mark_err(state, "delist_flatten", "execution", &message).await;
        }
    }
}

async fn ensure_positioned_symbol_dumped(
    state: &AppState,
    store: &DelistStore,
    positioned: &PositionDumpCandidate,
) -> Result<()> {
    let mut positioned = positioned.clone();
    positioned.threshold_usdt = 0.0;
    dump_position_candidate(state, store, &positioned).await?;
    Ok(())
}

async fn current_flatten_account(
    state: &AppState,
    candidate: &FlattenCandidate,
) -> Result<AccountSpec> {
    if !nav_accounts_ready(state).await {
        anyhow::bail!("NAV strategy catalog is not current; flatten paused");
    }
    state
        .accounts
        .read()
        .await
        .iter()
        .find(|account| account.slug == candidate.account_slug)
        .cloned()
        .context("flatten account is not in the current NAV strategy catalog")
}

async fn record_manual_flatten_required(
    state: &AppState,
    store: &DelistStore,
    candidate: &FlattenCandidate,
) -> Result<()> {
    if store
        .flatten_success_covers_snapshot(
            &candidate.account_slug,
            &candidate.symbol,
            candidate.deadline_ms,
            candidate.snapshot_ms,
        )
        .await?
    {
        return Ok(());
    }
    let dedup_key = audit_dedup_key(candidate, "manual-required");
    let account = current_flatten_account(state, candidate).await?;
    let command = serde_json::to_value(state.flatten_executor.command(candidate, &account)?)?;
    let claimed = store
        .claim_flatten_execution(
            &dedup_key,
            "manual_required",
            &candidate.account_slug,
            &candidate.exchange,
            &candidate.symbol,
            &candidate.delist_utc,
            candidate.deadline_ms,
            candidate.snapshot_ms,
            candidate.open_usdt,
            candidate.hedge_usdt,
            candidate.position_usdt,
            candidate.manual_threshold_usdt,
            command,
            "notifying",
        )
        .await?;
    let audit_id = match claimed {
        Some(audit_id) => audit_id,
        None => {
            let Some((audit_id, status, notification_status)) =
                store.flatten_execution_state(&dedup_key).await?
            else {
                anyhow::bail!("manual-required audit disappeared after claim");
            };
            if status == "manual_required" && notification_status.as_deref() == Some("accepted") {
                return Ok(());
            }
            audit_id
        }
    };
    let notify = send_flatten_notification(
        state,
        candidate,
        NotificationSeverity::Critical,
        "下架前24小时大仓位需人工清仓",
        &format!(
            "{} | {}\n持仓 {:.2}U，大于自动清仓阈值 {:.2}U\n下架时间 {}\n请在下架风险页面确认后人工执行 flatten --symbol {} --mode clear",
            candidate.account_slug,
            candidate.symbol,
            candidate.position_usdt,
            candidate.manual_threshold_usdt,
            candidate.delist_utc,
            candidate.symbol,
        ),
    )
    .await;
    let notification_status = if notify.is_ok() { "accepted" } else { "failed" };
    let notify_error = notify.as_ref().err().map(|err| format!("{err:#}"));
    store
        .finish_flatten_execution(
            audit_id,
            "manual_required",
            None,
            None,
            None,
            notify_error.as_deref(),
            Some(notification_status),
        )
        .await?;
    notify
}

async fn auto_flatten_candidate(
    state: &AppState,
    store: &DelistStore,
    candidate: &FlattenCandidate,
    positioned: &PositionDumpCandidate,
) -> Result<()> {
    let inflight_key = format!("{}:{}", candidate.account_slug, candidate.symbol);
    {
        let mut inflight = state.flatten_inflight.lock().await;
        if !inflight.insert(inflight_key.clone()) {
            return Ok(());
        }
    }
    let result = async {
        let dedup_key = audit_dedup_key(candidate, "auto");
        match execute_flatten_with_audit(state, store, candidate, positioned, "auto", &dedup_key)
            .await?
        {
            Some((_audit_id, output)) if !output.success => {
                anyhow::bail!("automatic flatten script failed")
            }
            Some(_) => Ok(()),
            None => match store.flatten_execution_state(&dedup_key).await? {
                Some((_id, status, _)) if status == "success" => {
                    if store
                        .flatten_success_covers_snapshot(
                            &candidate.account_slug,
                            &candidate.symbol,
                            candidate.deadline_ms,
                            candidate.snapshot_ms,
                        )
                        .await?
                    {
                        Ok(())
                    } else {
                        anyhow::bail!(
                            "position remains in a snapshot captured after automatic flatten"
                        )
                    }
                }
                Some((_id, status, _)) => anyhow::bail!(
                    "automatic flatten already attempted with unresolved status={status}"
                ),
                None => anyhow::bail!("automatic flatten audit disappeared after claim"),
            },
        }
    }
    .await;
    state.flatten_inflight.lock().await.remove(&inflight_key);
    result
}

async fn execute_flatten_with_audit(
    state: &AppState,
    store: &DelistStore,
    candidate: &FlattenCandidate,
    positioned: &PositionDumpCandidate,
    trigger: &str,
    dedup_key: &str,
) -> Result<Option<(i64, FlattenRunOutput)>> {
    let account = current_flatten_account(state, candidate).await?;
    let command = serde_json::to_value(state.flatten_executor.command(candidate, &account)?)?;
    let claimed = store
        .claim_flatten_execution(
            dedup_key,
            trigger,
            &candidate.account_slug,
            &candidate.exchange,
            &candidate.symbol,
            &candidate.delist_utc,
            candidate.deadline_ms,
            candidate.snapshot_ms,
            candidate.open_usdt,
            candidate.hedge_usdt,
            candidate.position_usdt,
            candidate.manual_threshold_usdt,
            command,
            "running",
        )
        .await;
    let Some(audit_id) = (match claimed {
        Ok(claimed) => claimed,
        Err(err) => {
            let _ = send_flatten_notification(
                state,
                candidate,
                NotificationSeverity::Critical,
                "下架清仓审计异常",
                &format!(
                    "{} | {}\n持仓 {:.2}U\n{err:#}",
                    candidate.account_slug, candidate.symbol, candidate.position_usdt
                ),
            )
            .await;
            return Err(err);
        }
    }) else {
        return Ok(None);
    };
    info!(
        "delist flatten started audit_id={} trigger={} account={} symbol={} position={}U",
        audit_id, trigger, candidate.account_slug, candidate.symbol, candidate.position_usdt
    );
    if let Err(err) = ensure_positioned_symbol_dumped(state, store, positioned).await {
        let message = format!("prepare dump before flatten failed: {err:#}");
        let notify = send_flatten_notification(
            state,
            candidate,
            NotificationSeverity::Critical,
            "下架清仓准备失败",
            &format!(
                "{} | {}\n持仓 {:.2}U\n{}",
                candidate.account_slug, candidate.symbol, candidate.position_usdt, message
            ),
        )
        .await;
        store
            .finish_flatten_execution(
                audit_id,
                "failed",
                None,
                None,
                None,
                Some(&message),
                Some(if notify.is_ok() { "accepted" } else { "failed" }),
            )
            .await?;
        return Err(err);
    }
    let output = match state.flatten_executor.run(candidate, &account).await {
        Ok(output) => output,
        Err(err) => {
            let message = format!("{err:#}");
            let notify = send_flatten_notification(
                state,
                candidate,
                NotificationSeverity::Critical,
                "下架自动清仓执行异常",
                &format!(
                    "{} | {}\n持仓 {:.2}U\n{}",
                    candidate.account_slug, candidate.symbol, candidate.position_usdt, message
                ),
            )
            .await;
            store
                .finish_flatten_execution(
                    audit_id,
                    "failed",
                    None,
                    None,
                    None,
                    Some(&message),
                    Some(if notify.is_ok() { "accepted" } else { "failed" }),
                )
                .await?;
            return Err(err);
        }
    };
    let status = if output.success { "success" } else { "failed" };
    let mut notification_status = None;
    let mut error = None;
    if !output.success {
        let message = format!(
            "flatten script exited with code {:?}: {}",
            output.exit_code,
            output.stderr.lines().next().unwrap_or("no stderr")
        );
        let notify = send_flatten_notification(
            state,
            candidate,
            NotificationSeverity::Critical,
            "下架清仓脚本失败",
            &format!(
                "{} | {}\n持仓 {:.2}U\n{}",
                candidate.account_slug, candidate.symbol, candidate.position_usdt, message
            ),
        )
        .await;
        notification_status = Some(if notify.is_ok() { "accepted" } else { "failed" });
        error = Some(message);
    }
    store
        .finish_flatten_execution(
            audit_id,
            status,
            output.exit_code,
            Some(&output.stdout),
            Some(&output.stderr),
            error.as_deref(),
            notification_status,
        )
        .await?;
    info!(
        "delist flatten finished audit_id={} trigger={} account={} symbol={} status={}",
        audit_id, trigger, candidate.account_slug, candidate.symbol, status
    );
    fr_event(
        state,
        "operation",
        format!(
            "[操作日志] 下架清仓脚本执行结果\n{} {} | 触发方式 {} | 脚本状态 {} | 审计 {}",
            candidate.account_slug, candidate.symbol, trigger, status, audit_id
        ),
    )
    .await;
    Ok(Some((audit_id, output)))
}

async fn send_flatten_notification(
    state: &AppState,
    candidate: &FlattenCandidate,
    severity: NotificationSeverity,
    title: &str,
    message: &str,
) -> Result<()> {
    let client = state
        .notification_client
        .clone()
        .context("local notification client is unavailable")?;
    let mut fields = BTreeMap::new();
    fields.insert("账户".to_string(), candidate.account_slug.clone());
    fields.insert("币对".to_string(), candidate.symbol.clone());
    fields.insert(
        "持仓".to_string(),
        format!("{:.2}U", candidate.position_usdt),
    );
    fields.insert("下架时间".to_string(), candidate.delist_utc.clone());
    let request = NotificationRequest {
        source: "delist_risk_server".to_string(),
        title: title.to_string(),
        message: message.to_string(),
        severity,
        fields,
        dedup_key: Some(audit_dedup_key(candidate, title)),
    };
    tokio::task::spawn_blocking(move || client.send(&request))
        .await
        .context("join notification task")?
}

/// Marks FR accounts whose `fr_unimmr_close_symbols` list is confirmed empty.
/// Read failures leave the fields unset; an empty list upgrades tone to `risk`.
fn decorate_unimmr_lists(
    accounts: &mut [AccountRiskView],
    lists: &BTreeMap<String, Result<FrSymbolListState, String>>,
) {
    for account in accounts
        .iter_mut()
        .filter(|account| account.kind == "funding_rate")
    {
        let Some(Ok(list)) = lists.get(&account.slug) else {
            continue;
        };
        account.unimmr_close_n = Some(list.symbols.len());
        account.unimmr_empty = list.symbols.is_empty();
        if account.unimmr_empty && matches!(account.tone.as_str(), "ok" | "uncovered") {
            account.tone = "risk".to_string();
        }
    }
}

/// Per-scan dedup for the FR `fr_unimmr_close_symbols` empty-list watch.
#[derive(Default)]
struct UnimmrListWatch {
    /// slug -> last accepted/attempted alert time for a currently empty list.
    empty_notified: BTreeMap<String, Instant>,
}

/// Watches every NAV `funding_rate` account's `fr_unimmr_close_symbols` Redis
/// list and alerts while it is empty. An empty list means a UniMMR breach has
/// no algorithmic close candidates on that env.
async fn run_unimmr_list_scan(state: &AppState, watch: &mut UnimmrListWatch) {
    if !nav_accounts_ready(state).await {
        return;
    }
    let accounts = state.accounts.read().await.clone();
    if !accounts
        .iter()
        .any(|account| account.kind == "funding_rate")
    {
        return;
    }
    let lists = load_fr_symbol_list(
        &accounts,
        "unimmr_close_symbols",
        &state.jp_redis,
        state.sg_redis.as_deref(),
    )
    .await;

    let now = Instant::now();
    if let Some(notifier) = &state.fr_notifications {
        let mut observations: Vec<_> = lists
            .iter()
            .map(|(slug, result)| notifications::Observation {
                key: format!("unimmr-list/{slug}"),
                title: format!("{slug} UniMMR 平仓列表"),
                problem: result.as_ref().ok().map(|list| list.symbols.is_empty()),
                detail: match result {
                    Ok(list) if list.symbols.is_empty() => {
                        "fr_unimmr_close_symbols 为空或未配置，风险触发时没有算法平仓候选".into()
                    }
                    Ok(list) => format!("列表已有 {} 个币对", list.symbols.len()),
                    Err(_) => "列表读取失败，不能确认恢复".into(),
                },
            })
            .collect();
        observations.extend(
            lists
                .iter()
                .map(|(slug, result)| notifications::Observation {
                    key: format!("unimmr-list-data/{slug}"),
                    title: format!("{slug} UniMMR 列表数据"),
                    problem: Some(result.is_err()),
                    detail: if result.is_ok() {
                        "列表读取已恢复".into()
                    } else {
                        "Redis列表读取失败；无法确认原列表风险已恢复".into()
                    },
                }),
        );
        if let Err(err) = notifier.observe(observations).await {
            warn!("queue UniMMR list observation failed: {err}");
        }
    }
    let mut empty: Vec<(&String, &FrSymbolListState)> = Vec::new();
    let mut read_errors = Vec::new();
    for (slug, result) in &lists {
        match result {
            Ok(list) if list.symbols.is_empty() => empty.push((slug, list)),
            Ok(list) => {
                if watch.empty_notified.remove(slug).is_some() && state.fr_notifications.is_none() {
                    if let Err(err) = send_unimmr_list_notification(state, slug, list, true).await {
                        warn!("UniMMR list recovery notification failed account={slug}: {err:#}");
                    }
                }
            }
            Err(err) => read_errors.push(format!("{slug}: {err}")),
        }
    }
    for (slug, list) in &empty {
        let due = state.fr_notifications.is_none() && !watch.empty_notified.contains_key(*slug);
        if !due {
            continue;
        }
        match send_unimmr_list_notification(state, slug, list, false).await {
            Ok(()) => {
                info!("UniMMR empty-list alert sent account={slug}");
                watch.empty_notified.insert((*slug).clone(), now);
            }
            Err(err) => {
                warn!("UniMMR empty-list alert failed account={slug}: {err:#}");
            }
        }
    }
    watch
        .empty_notified
        .retain(|slug, _| lists.contains_key(slug));

    let mut problems = Vec::new();
    if !empty.is_empty() {
        problems.push(format!(
            "empty fr_unimmr_close_symbols: {}",
            empty
                .iter()
                .map(|(slug, _)| slug.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    problems.extend(read_errors.iter().map(|err| format!("list read: {err}")));
    if problems.is_empty() {
        mark_ok(state, "fr_unimmr_close_list", "check").await;
    } else {
        let message = problems.join("; ");
        warn!("FR UniMMR list scan degraded: {message}");
        mark_err(state, "fr_unimmr_close_list", "check", &message).await;
    }
}

async fn send_unimmr_list_notification(
    state: &AppState,
    account_slug: &str,
    list: &FrSymbolListState,
    recovered: bool,
) -> Result<()> {
    let client = state
        .notification_client
        .clone()
        .context("local notification client is unavailable")?;
    let (title, severity, message) = if recovered {
        (
            "UniMMR平仓列表恢复",
            NotificationSeverity::Info,
            format!(
                "{account_slug}｜fr_unimmr_close_symbols 已配置 {} 个币对",
                list.symbols.len()
            ),
        )
    } else {
        (
            "UniMMR平仓列表为空",
            NotificationSeverity::Critical,
            format!(
                "{account_slug}｜fr_unimmr_close_symbols {}\nkey: {}\nUniMMR 跌破触发线时将没有可算法平仓的币对",
                if list.present { "列表为空" } else { "key 未配置" },
                list.key
            ),
        )
    };
    let mut fields = BTreeMap::new();
    fields.insert("账户".to_string(), account_slug.to_string());
    fields.insert("Redis Key".to_string(), list.key.clone());
    let request = NotificationRequest {
        source: "delist_risk_server".to_string(),
        title: title.to_string(),
        message,
        severity,
        fields,
        dedup_key: Some(format!(
            "delist_risk_server:unimmr_close_list:{account_slug}"
        )),
    };
    tokio::task::spawn_blocking(move || client.send(&request))
        .await
        .context("join notification task")?
}

async fn dump_position_candidate(
    state: &AppState,
    store: &DelistStore,
    candidate: &PositionDumpCandidate,
) -> Result<bool> {
    let redis_url = match candidate.redis_site.as_str() {
        "jp" => state.jp_redis.as_str(),
        "sg" => state
            .sg_redis
            .as_deref()
            .context("SG Redis is not configured")?,
        site => anyhow::bail!("unsupported Redis site {site}"),
    };
    let Some(plan) = prepare_redis_dump(redis_url, candidate).await? else {
        return Ok(false);
    };
    let audit_id = store
        .begin_redis_dump(
            &candidate.account_slug,
            &candidate.exchange,
            &candidate.redis_site,
            &candidate.symbol,
            serde_json::to_value(&candidate.event)?,
            candidate.snapshot_ms,
            candidate.open_usdt,
            candidate.hedge_usdt,
            candidate.impacted_position_usdt,
            candidate.threshold_usdt,
            serde_json::to_value(&plan.changes)?,
        )
        .await?;
    info!(
        "Redis delist dump pending audit_id={} account={} symbol={} position={}U",
        audit_id, candidate.account_slug, candidate.symbol, candidate.impacted_position_usdt
    );
    match apply_redis_dump(redis_url, &plan).await {
        Ok(()) => {
            store.finish_redis_dump(audit_id, "success", None).await?;
            fr_event(state, "operation", format!("[操作日志] 下架仓位转入 dump\n{} {} | 影响持仓 {:.2}U | 审计 {}\n已更新该币对的开仓/dump名单，未在此步骤下单。", candidate.account_slug, candidate.symbol, candidate.impacted_position_usdt, audit_id)).await;
            info!(
                "Redis delist dump success audit_id={} account={} symbol={} changes={}",
                audit_id,
                candidate.account_slug,
                candidate.symbol,
                serde_json::to_string(&plan.changes)?
            );
            Ok(true)
        }
        Err(err) => {
            if let Err(audit_err) = store
                .finish_redis_dump(audit_id, "failed", Some(&format!("{err:#}")))
                .await
            {
                warn!("finish failed dump audit id={audit_id}: {audit_err:#}");
            }
            Err(err)
        }
    }
}

async fn prune_confirmed_delists(state: &AppState) -> Result<usize> {
    if !nav_accounts_ready(state).await {
        anyhow::bail!("NAV strategy catalog is not current; automatic removal paused");
    }
    let store = state
        .store
        .as_ref()
        .context("postgres is required before Redis auto-removal")?;
    let listings = state.listings.read().await.clone();
    let accounts = state.accounts.read().await.clone();
    let universes = load_universes(&accounts, &state.jp_redis, state.sg_redis.as_deref()).await;
    let universe_errors = removal_universe_errors(&accounts, &universes);
    if !universe_errors.is_empty() {
        anyhow::bail!("Redis universe load failed: {}", universe_errors.join("; "));
    }
    let candidates = confirmed_removal_candidates(&accounts, &listings, &universes);
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
    if !snapshot_symbol_is_flat(
        &state.snapshot_client,
        &state.snapshot_base_url,
        &candidate.account_slug,
        &candidate.symbol,
        state.position_snapshot_max_age_ms,
        POSITION_REMOVAL_THRESHOLD_USDT,
    )
    .await?
    {
        info!(
            "Redis delist removal retained positioned symbol account={} symbol={}",
            candidate.account_slug, candidate.symbol
        );
        return Ok(false);
    }
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
            fr_event(state, "operation", format!("[操作日志] 确认下架后移出策略名单\n{} {} | 审计 {}\n该币对仓位已通过现有清空校验；已更新Redis名单。", candidate.account_slug, candidate.symbol, audit_id)).await;
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
        TradingVenue::OkexMargin,
        TradingVenue::OkexFutures,
        TradingVenue::BinanceFutures,
        TradingVenue::BinanceCoinFutures,
        TradingVenue::GateFutures,
        TradingVenue::BybitFutures,
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
    match bybit_announcement::fetch_delist_notices(public, state.default_days.max(30) + 7).await {
        Ok(items) => {
            mark_ok(state, "bybit_announcements", "fetch").await;
            let mut errors = Vec::new();
            for mut item in items {
                if let Err(err) = bybit_announcement::hydrate_notice_body(public, &mut item).await {
                    errors.push(format!("{}: {err:#}", item.id));
                    continue;
                }
                let input = LlmExtractInput::from_raw(&item);
                remember_raw(state, &item).await;
                maybe_extract(state, llm, llm_client, llm_budget, &input).await;
            }
            if errors.is_empty() {
                mark_ok(state, "bybit_article_detail", "fetch").await;
            } else {
                mark_err(state, "bybit_article_detail", "fetch", &errors.join("; ")).await;
            }
        }
        Err(err) => mark_err(state, "bybit_announcements", "fetch", &format!("{err:#}")).await,
    }
    match okx_announcement::fetch_delist_notices(public, state.default_days.max(90) + 7).await {
        Ok(items) => {
            mark_ok(state, "okex_announcements", "fetch").await;
            let mut errors = Vec::new();
            for mut item in items {
                if let Err(err) = okx_announcement::hydrate_notice_body(public, &mut item).await {
                    errors.push(format!("{}: {err:#}", item.id));
                    continue;
                }
                let input = LlmExtractInput::from_raw(&item);
                remember_raw(state, &item).await;
                if !maybe_extract(state, llm, llm_client, llm_budget, &input).await {
                    errors.push(format!(
                        "{}: extraction unavailable or pending retry",
                        item.id
                    ));
                }
            }
            if errors.is_empty() {
                mark_ok(state, "okex_article_detail", "fetch").await;
            } else {
                mark_err(state, "okex_article_detail", "fetch", &errors.join("; ")).await;
            }
        }
        Err(err) => mark_err(state, "okex_announcements", "fetch", &format!("{err:#}")).await,
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

fn force_llm_id_set(raw: &Option<String>) -> std::collections::BTreeSet<String> {
    raw.as_deref()
        .unwrap_or("")
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
        extract_announcement(state, llm, llm_client, llm_budget, &input, force).await;
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
    extract_announcement(state, llm, llm_client, llm_budget, input, false).await
}

async fn extraction_event(
    input: &LlmExtractInput,
    state: &AppState,
    kind: &str,
    message: String,
) -> bool {
    // OKX is a CTA risk source, outside the existing FR delivery channel.
    if input.exchange == "okex" {
        return true;
    }
    fr_event(state, kind, message).await
}

async fn extract_announcement(
    state: &AppState,
    llm: Option<&LlmConfig>,
    llm_client: Option<&Client>,
    llm_budget: &Mutex<LlmBudget>,
    input: &LlmExtractInput,
    force: bool,
) -> bool {
    let (Some(llm), Some(client)) = (llm, llm_client) else {
        return false;
    };
    let key = (input.exchange.clone(), input.id.clone());
    let gate = {
        let mut locks = state.llm_locks.lock();
        locks.entry(key).or_default().clone()
    };
    // Serialize each article so an edit arriving during extraction is not lost or
    // overwritten by an older response. Waiting duplicates reuse the saved result.
    let _claim = gate.lock().await;
    if let Some(previous) = state.status.read().await.llm(&input.exchange, &input.id) {
        if !previous.should_extract(input, Utc::now().timestamp_millis(), force) {
            return previous.ok;
        }
    }
    let Ok(_slot) = state.llm_slots.acquire().await else {
        return false;
    };
    if !llm_budget.lock().allow() {
        return false;
    }
    match extract_for_emit(client, llm, input).await {
        Ok(value) => {
            state.book.write().await.ingest_llm_value(input, &value);
            // Save events before the durable success marker so restart dedup cannot lose a result.
            if !persist(state).await {
                mark_llm(state, input, false, Some("save extracted risk book failed")).await;
                extraction_event(
                    input,
                    state,
                    "luna_failed",
                    notifications::luna_message(input, Some(&value), false),
                )
                .await;
                return false;
            }
            if !extraction_event(
                input,
                state,
                "luna_ok",
                notifications::luna_message(input, Some(&value), true),
            )
            .await
            {
                mark_llm(
                    state,
                    input,
                    false,
                    Some("FR notification outbox persistence failed"),
                )
                .await;
                return false;
            }
            mark_llm(state, input, true, None).await;
            true
        }
        Err(err) => {
            mark_llm(state, input, false, Some(&format!("{err:#}"))).await;
            extraction_event(
                input,
                state,
                "luna_failed",
                notifications::luna_message(input, None, false),
            )
            .await;
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

async fn persist(state: &AppState) -> bool {
    // Serialize the shared .tmp write/rename across the independent refresh workers.
    let book = state.book.write().await;
    if let Err(err) = book.save(&state.book_path) {
        warn!("save risk book failed: {err:#}");
        return false;
    }
    true
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
        status.mark_llm(
            &input.exchange,
            &input.id,
            &input.title,
            ok,
            err,
            input.fingerprint(),
        );
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
    // Observe backup outcomes directly so CTA alerts also work without a NAV/FR
    // account catalog, and a short failure cannot disappear between scans.
    if let (Some(notifier), Some(observation)) = (
        &state.fr_notifications,
        notifications::cta_backup_observation(row),
    ) {
        if let Err(err) = notifier.observe(vec![observation]).await {
            warn!("queue CTA backup notification failed: {err}");
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
    llm_budget: &Arc<Mutex<LlmBudget>>,
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
    llm_budget: &Arc<Mutex<LlmBudget>>,
) {
    match parse_ws_text(text) {
        Ok(Some(item)) => {
            let input = LlmExtractInput::from_raw(&item);
            remember_raw(state, &item).await;
            let state = state.clone();
            let llm = llm.cloned();
            let client = llm_client.cloned();
            let budget = llm_budget.clone();
            tokio::spawn(async move {
                maybe_extract(&state, llm.as_ref(), client.as_ref(), &budget, &input).await;
            });
        }
        Ok(None) => {}
        Err(err) => warn!("skip Gate announcement ws frame: {err:#}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[tokio::test]
    async fn repeated_announcements_make_one_request_and_changed_body_reextracts() {
        use mkt_signal::common::announcement_llm::LlmEndpoint;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let app = Router::new().route(
            "/v1/responses",
            post(move || {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    time::sleep(Duration::from_millis(50)).await;
                    Json(json!({"output_text":json!({"relevant":true,"actions":[{
                    "exchange":"gate","venue":"gate-margin","action":"delist",
                    "utc":"2026-10-08T08:00:00Z","assets":["CDL"],"symbols":[],"note":""
                }]}).to_string()}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = LlmEndpoint {
            label: "test".into(),
            api_url: format!("http://{}", listener.local_addr().unwrap()),
            api_key: "test".into(),
            model: "test".into(),
            reasoning_effort: "low".into(),
            extra_header: None,
        };
        let llm = LlmConfig {
            primary: endpoint.clone(),
            backup: endpoint,
        };
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = std::env::temp_dir().join(format!("delist-llm-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let fr_notifier = notifications::Notifications::open(
            dir.join("notifications.json"),
            notifications::DingTalk::test_sender(
                reqwest::Url::parse("http://127.0.0.1:1/unused").unwrap(),
            ),
        )
        .await
        .unwrap();
        let state = AppState {
            book: Arc::new(RwLock::new(RiskBook::default())),
            status: Arc::new(RwLock::new(StatusBook::default())),
            listings: Arc::new(RwLock::new(ListingIndex::default())),
            accounts: Arc::new(RwLock::new(Vec::new())),
            exposure: Arc::new(RwLock::new(exposure::Monitor::new(
                exposure::Settings::default(),
            ))),
            exposure_settings: exposure::Settings::default(),
            fr_notifications: Some(fr_notifier.clone()),
            llm_locks: Arc::new(Mutex::new(BTreeMap::new())),
            llm_slots: Arc::new(Semaphore::new(2)),
            store: None,
            book_path: dir.join("book.json"),
            web_dir: dir.clone(),
            default_days: 30,
            jp_redis: String::new(),
            sg_redis: None,
            auto_remove_redis: false,
            auto_dump_position_risk: false,
            auto_flatten_position_risk: false,
            position_risk_threshold_usdt: 50.0,
            position_snapshot_max_age_ms: 120_000,
            flatten_window_ms: 86_400_000,
            flatten_manual_threshold_usdt: 1000.0,
            flatten_executor: FlattenExecutor::new(dir.clone(), Duration::from_secs(1)),
            flatten_inflight: Arc::new(AsyncMutex::new(BTreeSet::new())),
            notification_client: None,
            flatten_api_token: None,
            snapshot_base_url: String::new(),
            snapshot_client: Client::new(),
            nav_strategies_url: String::new(),
            nav_login_url: String::new(),
            nav_username: None,
            nav_password: None,
            nav_session: Arc::new(RwLock::new(None)),
            cta_mapping: cta_mapping::Settings {
                env_root: dir.clone(),
                factor_env: dir.join(cta_factors::Worker::Perp.name()),
                spread_factor_env: dir.join(cta_factors::Worker::Spread.name()),
                jp_database: None,
            },
            backups: ManualBackupSettings {
                enabled: false,
                fr_dir: dir.clone(),
                cta_dir: dir.clone(),
                env_root: dir.clone(),
                sg_ssh_host: "sg".into(),
                keep: 30,
                fr_cos: None,
                cta_cos: None,
            },
            backup_lock: Arc::new(AsyncMutex::new(())),
        };
        let client = Client::builder().no_proxy().build().unwrap();
        let budget = Mutex::new(LlmBudget::new(0));
        let mut input = LlmExtractInput {
            exchange: "gate".into(),
            id: "101657".into(),
            title: "Delist CDL".into(),
            url: String::new(),
            published_ms: 1,
            body: "CDL trading ends at 08:00 UTC".into(),
        };
        let (first, second) = tokio::join!(
            maybe_extract(&state, Some(&llm), Some(&client), &budget, &input),
            maybe_extract(&state, Some(&llm), Some(&client), &budget, &input),
        );
        assert!(first);
        assert!(second);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(fr_notifier.status().await["queued"], 1);
        input.published_ms = 2;
        assert!(maybe_extract(&state, Some(&llm), Some(&client), &budget, &input).await);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        input.body = "CDL trading ends at 09:00 UTC".into();
        assert!(maybe_extract(&state, Some(&llm), Some(&client), &budget, &input).await);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let mut revised = input.clone();
        revised.body = "CDL trading ends at 10:00 UTC".into();
        let mut latest = input.clone();
        latest.body = "CDL trading ends at 11:00 UTC".into();
        let (revised_ok, latest_ok) = tokio::join!(
            maybe_extract(&state, Some(&llm), Some(&client), &budget, &revised),
            maybe_extract(&state, Some(&llm), Some(&client), &budget, &latest),
        );
        assert!(revised_ok && latest_ok);
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert_eq!(fr_notifier.status().await["queued"], 4);
        assert_eq!(
            state
                .status
                .read()
                .await
                .llm("gate", "101657")
                .unwrap()
                .input_fingerprint
                .as_deref(),
            Some(latest.fingerprint().as_str())
        );
        assert_eq!(RiskBook::load(&state.book_path).unwrap().events.len(), 1);
        server.abort();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn okx_only_snapshot_failure_stays_outside_fr_sources() {
        let okx: anyhow::Error =
            ListingRefreshFailure(vec![("okex_spot_exchange_info".into(), "throttled".into())])
                .into();
        assert_eq!(
            snapshot_failure_source(&okx),
            "okex_exchange_symbol_snapshot"
        );
        assert!(!notifications::fr_source(
            snapshot_failure_source(&okx),
            &BTreeSet::new()
        ));
        let mixed: anyhow::Error = ListingRefreshFailure(vec![
            ("okex_spot_exchange_info".into(), "failed".into()),
            ("binance_spot_exchange_info".into(), "failed".into()),
        ])
        .into();
        assert_eq!(snapshot_failure_source(&mixed), "exchange_symbol_snapshot");
        assert_eq!(
            snapshot_failure_source(&anyhow::anyhow!("database unavailable")),
            "exchange_symbol_snapshot"
        );
    }

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

    #[test]
    fn manual_token_comparison_requires_exact_bytes() {
        assert!(constant_time_equal(b"correct-token", b"correct-token"));
        assert!(!constant_time_equal(b"correct-token", b"wrong-token"));
        assert!(!constant_time_equal(
            b"correct-token",
            b"correct-token-longer"
        ));
    }
}
