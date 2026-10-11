use anyhow::{Context, Result};
use axum::{
    extract::State,
    http::{header, StatusCode},
    response::Html,
    routing::get,
    Json, Router,
};
use clap::Parser;
use fr_monitor::{
    checks::{self, Check},
    config::{Config, Monitor, Source},
    market::{MarketFeed, MarketView, MarketWatch},
    notices::{ActiveIssue, DingTalk, Notice, Tracker},
    now_ms,
};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{sync::RwLock, task::JoinSet};

#[derive(Parser)]
#[command(
    about = "Read-only FR health monitor and unified dashboard; notifications require --execute"
)]
struct Args {
    #[arg(long, default_value = "config/fr_monitor.toml")]
    config: PathBuf,
    #[arg(long)]
    once: bool,
    #[arg(long, conflicts_with = "execute")]
    dry_run: bool,
    #[arg(long)]
    execute: bool,
}

#[derive(Clone, Serialize)]
struct SourceView {
    id: String,
    namespace: String,
    checked_ts_ms: i64,
    markets: Vec<String>,
    market_symbols: BTreeMap<String, Vec<String>>,
    max_asset_exposure_usdt: f64,
    max_total_exposure_usdt: f64,
    snapshot: Option<Value>,
}

impl SourceView {
    fn new(s: &Source) -> Self {
        Self {
            id: s.id.clone(),
            namespace: s.namespace.clone(),
            checked_ts_ms: 0,
            markets: s.markets.iter().map(|m| m.venue.clone()).collect(),
            market_symbols: s
                .markets
                .iter()
                .map(|m| (m.venue.clone(), m.symbols.clone()))
                .collect(),
            max_asset_exposure_usdt: s.max_asset_exposure_usdt,
            max_total_exposure_usdt: s.max_total_exposure_usdt,
            snapshot: None,
        }
    }
}

#[derive(Default, Clone, Serialize)]
struct Board {
    ts_ms: i64,
    poll_secs: u64,
    market_poll_secs: u64,
    market_ts_ms: i64,
    markets: Vec<MarketView>,
    stale_secs: u64,
    notifications_enabled: bool,
    notification_error: Option<String>,
    sources: Vec<SourceView>,
    issues: Vec<ActiveIssue>,
    history: VecDeque<History>,
    events: VecDeque<Notice>,
}

#[derive(Clone, Serialize)]
struct History {
    ts_ms: i64,
    equity: Option<f64>,
    exposure: Option<f64>,
}

#[derive(Default)]
struct Runtime {
    started_ms: i64,
    board: Board,
    tracker: Tracker,
    delivery_errors: [Option<String>; 2],
}
type SharedRuntime = Arc<RwLock<Runtime>>;

impl Runtime {
    fn apply_markets(&mut self, feed: &MarketFeed, watches: &[MarketWatch], m: &Monitor, now: i64) {
        let (views, checks): (Vec<_>, Vec<_>) =
            watches.iter().map(|w| feed.check(w, m, now)).unzip();
        self.tracker.update(&checks, now);
        self.board.markets = views;
        self.board.market_ts_ms = now;
        self.board.ts_ms = now;
        self.board.issues = self.tracker.active();
    }

    fn apply_source(&mut self, view: SourceView, checks: Vec<Check>) {
        let now = now_ms();
        self.tracker.update(&checks, now);
        if let Some(previous) = self.board.sources.iter_mut().find(|s| s.id == view.id) {
            *previous = view;
        }
        self.board.ts_ms = now;
        self.board.issues = self.tracker.active();
    }

    fn sample(&mut self, m: &Monitor, now: i64) {
        // A blocked RocksDB worker must not keep repeating unverified old risks.
        let checks: Vec<_> = self
            .board
            .sources
            .iter()
            .filter(|s| {
                !poll_fresh(
                    if s.checked_ts_ms > 0 {
                        s.checked_ts_ms
                    } else {
                        self.started_ms
                    },
                    now,
                    m.poll_secs,
                )
            })
            .flat_map(|s| {
                ["risk", "exposure", "orders"].map(|kind| {
                    Check::failed(
                        format!("{}/{kind}", s.id),
                        "盘子巡检未完成或已停更，等待数据恢复",
                    )
                })
            })
            .collect();
        self.tracker.update(&checks, now);
        self.board.issues = self.tracker.active();
        self.board.history.push_back(History {
            ts_ms: now,
            equity: total(&self.board.sources, "total_equity", now, m),
            exposure: total(&self.board.sources, "total_exposure", now, m),
        });
        while self.board.history.len() > 360 {
            self.board.history.pop_front();
        }
    }
}

fn poll_fresh(ts: i64, now: i64, interval: u64) -> bool {
    ts > 0 && ts <= now + 5000 && now.saturating_sub(ts) <= (interval * 3 + 30) as i64 * 1000
}

async fn status(State(runtime): State<SharedRuntime>) -> impl axum::response::IntoResponse {
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(runtime.read().await.board.clone()),
    )
}

async fn health(State(runtime): State<SharedRuntime>) -> (StatusCode, Json<Value>) {
    let state = runtime.read().await;
    let b = &state.board;
    let now = now_ms();
    let market_ok = poll_fresh(b.market_ts_ms, now, b.market_poll_secs);
    let sources_ok = b
        .sources
        .iter()
        .all(|s| poll_fresh(s.checked_ts_ms, now, b.poll_secs));
    let ok = market_ok && sources_ok;
    (
        if ok {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(
            serde_json::json!({"ok":ok,"market_ok":market_ok,"sources_ok":sources_ok,"ts_ms":b.ts_ms}),
        ),
    )
}

#[derive(Default)]
struct OrderState {
    book: checks::OrderBook,
    scanned_at_us: Option<i64>,
}

async fn collect(
    s: &Source,
    m: &Monitor,
    http: &reqwest::Client,
    state: &mut OrderState,
) -> (SourceView, Vec<Check>) {
    let started_ms = now_ms();
    let mut checks = Vec::new();
    let path = s.rocksdb_path.clone();
    let since = state
        .scanned_at_us
        .map(|ts| ts.saturating_sub(60_000_000).max(0))
        .unwrap_or_else(|| (started_ms - m.order_lookback_secs as i64 * 1000).max(0) * 1000);
    let cap = m.max_order_records;
    let orders = tokio::task::spawn_blocking(move || {
        persist_manager::parquet::read_order_health_observations(&path, since, cap)
    });
    let snapshot: Result<Value> = async {
        let response = http
            .get(format!("{}/snapshot", s.viz_url.trim_end_matches('/')))
            .send()
            .await
            .context("Viz snapshot request failed")?
            .error_for_status()
            .context("Viz HTTP error")?;
        let bytes = response.bytes().await.context("read Viz snapshot")?;
        anyhow::ensure!(bytes.len() <= 4_000_000, "Viz snapshot too large");
        Ok(serde_json::from_slice(&bytes).context("invalid Viz JSON")?)
    }
    .await;
    let scope = format!("{}/orders", s.id);
    checks.push(match orders.await {
        Ok(Ok(records)) => {
            let issues = state.book.observe(&records, m, now_ms());
            state.scanned_at_us = Some(started_ms * 1000);
            Check::ok(scope, issues)
        }
        Ok(Err(e)) => Check::failed(scope, format!("订单只读检查失败: {e:#}")),
        Err(_) => Check::failed(scope, "订单检查任务失败"),
    });
    let now = now_ms();
    let snapshot = match snapshot {
        Ok(value) => {
            checks.extend(checks::check_snapshot(&value, s, m, now));
            Some(value)
        }
        Err(_) => {
            for kind in ["risk", "exposure"] {
                checks.push(Check::failed(
                    format!("{}/{kind}", s.id),
                    "Viz 数据不可读，请检查对应进程和端口",
                ));
            }
            None
        }
    };
    (
        SourceView {
            snapshot,
            checked_ts_ms: now,
            ..SourceView::new(s)
        },
        checks,
    )
}

fn total(views: &[SourceView], key: &str, now: i64, m: &Monitor) -> Option<f64> {
    views.iter().try_fold(0.0, |sum, s| {
        if !poll_fresh(s.checked_ts_ms, now, m.poll_secs) {
            return None;
        }
        let v = s.snapshot.as_ref()?["entries"]
            .as_array()?
            .iter()
            .find(|v| v["type"] == "pre_trade_risk" && v["namespace"] == s.namespace)?;
        let entry = &v["entry"];
        let ts = entry["ts_ms"].as_i64()?;
        if ts <= 0 || ts > now + 5000 || now - ts > m.stale_secs as i64 * 1000 {
            return None;
        }
        let accounts = entry["account_risks"].as_array()?;
        if accounts.is_empty()
            || !accounts.iter().all(|a| {
                a["ts_ms"].as_i64().is_some_and(|t| {
                    t > 0 && t <= now + 5000 && now - t <= m.stale_secs as i64 * 1000
                })
            })
        {
            return None;
        }
        Some(sum + entry[key].as_f64().filter(|n| n.is_finite())?)
    })
}

fn interval(seconds: u64) -> tokio::time::Interval {
    let mut timer = tokio::time::interval(Duration::from_secs(seconds));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    timer
}

async fn market_loop(
    runtime: SharedRuntime,
    feed: MarketFeed,
    watches: Vec<MarketWatch>,
    m: Monitor,
) {
    let mut timer = interval(m.market_poll_secs);
    loop {
        timer.tick().await;
        runtime
            .write()
            .await
            .apply_markets(&feed, &watches, &m, now_ms());
    }
}

async fn source_loop(runtime: SharedRuntime, source: Source, m: Monitor, http: reqwest::Client) {
    let mut timer = interval(m.poll_secs);
    let mut state = OrderState::default();
    loop {
        timer.tick().await;
        let (view, checks) = collect(&source, &m, &http, &mut state).await;
        runtime.write().await.apply_source(view, checks);
    }
}

/// Webhooks run outside both collection paths, with a separate worker per channel.
async fn notify_once(
    runtime: &SharedRuntime,
    m: &Monitor,
    sender: Option<&DingTalk>,
    market: bool,
) -> Result<()> {
    let notices = runtime.write().await.tracker.claim(m, now_ms(), market);
    if notices.is_empty() {
        return Ok(());
    }
    if let Some(sender) = sender {
        if let Err(error) = sender.send(&notices, market).await {
            runtime.write().await.tracker.release(&notices);
            return Err(error);
        }
    }
    let mut state = runtime.write().await;
    state.tracker.acknowledge(&notices, now_ms());
    state.board.issues = state.tracker.active();
    for notice in notices {
        state.board.events.push_front(notice);
    }
    while state.board.events.len() > 100 {
        state.board.events.pop_back();
    }
    Ok(())
}

async fn notification_loop(
    runtime: SharedRuntime,
    m: Monitor,
    sender: Option<Arc<DingTalk>>,
    market: bool,
) {
    let index = usize::from(!market);
    let mut failures = 0u32;
    loop {
        let result = notify_once(&runtime, &m, sender.as_deref(), market).await;
        let delay = match result {
            Ok(()) => {
                failures = 0;
                runtime.write().await.delivery_errors[index] = None;
                Duration::from_secs(m.poll_secs)
            }
            Err(error) => {
                failures = failures.saturating_add(1);
                runtime.write().await.delivery_errors[index] = Some(error.to_string());
                eprintln!("FR monitor: notification delivery failed; retry scheduled");
                Duration::from_millis(1000 << failures.min(8))
            }
        };
        {
            let mut state = runtime.write().await;
            state.board.notification_error = state.delivery_errors.iter().flatten().next().cloned();
        }
        tokio::time::sleep(delay).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let cfg = Config::load(&args.config)?;
    let notifier = if args.execute {
        Some(Arc::new(DingTalk::from_env(&cfg.monitor)?))
    } else {
        None
    };
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(cfg.monitor.request_timeout_secs))
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()?;
    let watches = MarketWatch::from_sources(&cfg.sources, &cfg.markets);
    let feed = MarketFeed::spawn(&watches)?;
    let sources: Vec<_> = cfg.sources.iter().filter(|s| s.enabled).cloned().collect();
    let runtime = SharedRuntime::default();
    {
        let mut state = runtime.write().await;
        state.started_ms = now_ms();
        state.board.sources = sources.iter().map(SourceView::new).collect();
        state.board.sources.sort_by(|a, b| a.id.cmp(&b.id));
        state.board.poll_secs = cfg.monitor.poll_secs;
        state.board.market_poll_secs = cfg.monitor.market_poll_secs;
        state.board.stale_secs = cfg.monitor.stale_secs;
        state.board.notifications_enabled = args.execute;
    }
    if args.once {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut jobs = JoinSet::new();
        for source in sources {
            let m = cfg.monitor.clone();
            let http = http.clone();
            jobs.spawn(
                async move { collect(&source, &m, &http, &mut OrderState::default()).await },
            );
        }
        while let Some(result) = jobs.join_next().await {
            let (view, checks) = result.context("FR source check failed")?;
            runtime.write().await.apply_source(view, checks);
        }
        {
            let mut state = runtime.write().await;
            state.apply_markets(&feed, &watches, &cfg.monitor, now_ms());
            state.sample(&cfg.monitor, now_ms());
        }
        for market in [true, false] {
            notify_once(&runtime, &cfg.monitor, notifier.as_deref(), market).await?;
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&runtime.read().await.board)?
        );
        return Ok(());
    }

    let listener = tokio::net::TcpListener::bind(cfg.monitor.bind)
        .await
        .context("bind FR dashboard")?;
    let app = Router::new()
        .route(
            "/",
            get(|| async { Html(include_str!("../web/index.html")) }),
        )
        .route("/api/status", get(status))
        .route("/healthz", get(health))
        .with_state(runtime.clone());
    println!(
        "FR monitor dashboard: http://{} (notifications={})",
        cfg.monitor.bind, args.execute
    );
    let mut jobs = JoinSet::<Result<()>>::new();
    jobs.spawn(async move {
        axum::serve(listener, app)
            .await
            .context("FR dashboard stopped")
    });
    let r = runtime.clone();
    let m = cfg.monitor.clone();
    jobs.spawn(async move {
        market_loop(r, feed, watches, m).await;
        Ok(())
    });
    for source in sources {
        let r = runtime.clone();
        let m = cfg.monitor.clone();
        let http = http.clone();
        jobs.spawn(async move {
            source_loop(r, source, m, http).await;
            Ok(())
        });
    }
    for market in [true, false] {
        let r = runtime.clone();
        let m = cfg.monitor.clone();
        let notifier = notifier.clone();
        jobs.spawn(async move {
            notification_loop(r, m, notifier, market).await;
            Ok(())
        });
    }
    let r = runtime.clone();
    let m = cfg.monitor.clone();
    jobs.spawn(async move {
        let mut timer = interval(m.poll_secs);
        loop {
            timer.tick().await;
            r.write().await.sample(&m, now_ms());
        }
    });
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        result = jobs.join_next() => {
            result.context("monitor worker missing")?.context("monitor worker panicked")??;
            anyhow::bail!("monitor worker stopped unexpectedly");
        }
    }
    jobs.abort_all();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(id: &str, origin: &str) -> Source {
        Source {
            id: id.into(),
            namespace: id.into(),
            viz_url: format!("{origin}/{id}"),
            rocksdb_path: std::env::temp_dir()
                .join(format!("fr-monitor-missing-{}", std::process::id()))
                .join(id),
            markets: vec![fr_monitor::config::Market {
                venue: "gate-futures".into(),
                symbols: vec!["BTCUSDT".into()],
            }],
            max_asset_exposure_usdt: 1000.0,
            max_total_exposure_usdt: 5000.0,
            enabled: true,
        }
    }

    #[test]
    fn stalled_source_becomes_unknown_without_repeating_old_risk() {
        let now = now_ms();
        let m = Monitor::default();
        let mut runtime = Runtime {
            started_ms: now,
            ..Runtime::default()
        };
        runtime
            .board
            .sources
            .push(SourceView::new(&source("slow", "http://127.0.0.1")));
        runtime.sample(&m, now);
        assert!(runtime.board.issues.is_empty());
        runtime.tracker.update(
            &[Check::ok(
                "slow/risk".into(),
                vec![checks::Issue::warning("risk", "old risk")],
            )],
            now,
        );
        runtime.sample(&m, now + 61_000);
        assert_eq!(runtime.board.issues.len(), 4);
        let old = runtime
            .board
            .issues
            .iter()
            .find(|i| i.issue.key == "risk")
            .unwrap();
        assert!(!old.observed);
        assert_eq!(runtime.tracker.pending(&m, now + 92_000).len(), 3);
        assert!(runtime.board.history.back().unwrap().equity.is_none());
    }

    #[tokio::test]
    async fn slow_account_does_not_block_market_or_other_accounts() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route(
                "/slow/snapshot",
                get(|| async {
                    std::future::pending::<()>().await;
                    Json(serde_json::json!({"entries": []}))
                }),
            )
            .route(
                "/fast/snapshot",
                get(|| async { Json(serde_json::json!({"entries": []})) }),
            );
        // JoinSet aborts every worker even if an assertion fails.
        let mut jobs = JoinSet::new();
        jobs.spawn(async move { axum::serve(listener, app).await.unwrap() });
        let sources = vec![source("slow", &origin), source("fast", &origin)];
        let watches = MarketWatch::from_sources(&sources, &[]);
        let m = Monitor {
            poll_secs: 1,
            market_poll_secs: 1,
            ..Monitor::default()
        };
        let runtime = SharedRuntime::default();
        {
            let mut state = runtime.write().await;
            state.started_ms = now_ms();
            state.board.poll_secs = m.poll_secs;
            state.board.market_poll_secs = m.market_poll_secs;
            state.board.sources = sources.iter().map(SourceView::new).collect();
        }
        let http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();
        jobs.spawn(market_loop(
            runtime.clone(),
            MarketFeed::default(),
            watches,
            m.clone(),
        ));
        for source in sources {
            jobs.spawn(source_loop(
                runtime.clone(),
                source,
                m.clone(),
                http.clone(),
            ));
        }
        tokio::time::timeout(Duration::from_secs(4), async {
            let mut initial = None;
            loop {
                {
                    let state = runtime.read().await;
                    let fast = state.board.sources[1].checked_ts_ms;
                    let market = state.board.market_ts_ms;
                    if fast > 0 && market > 0 {
                        if let Some((old_fast, old_market)) = initial {
                            if fast > old_fast && market > old_market {
                                break;
                            }
                        } else {
                            initial = Some((fast, market));
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("market and fast account must advance while the slow request is pending");
        let state = runtime.read().await;
        assert_eq!(state.board.sources[0].checked_ts_ms, 0);
        assert_eq!(state.board.markets.len(), 1);
        assert_eq!(state.board.markets[0].affected_sources.len(), 2);
        drop(state);
        let (status, Json(health)) = health(State(runtime)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(health["market_ok"], true);
        assert_eq!(health["sources_ok"], false);
    }
}
