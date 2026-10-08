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
    config::{Config, Source},
    market::MarketFeed,
    notices::{ActiveIssue, DingTalk, Notice, Tracker},
    now_ms,
};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex},
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
    markets: Vec<String>,
    snapshot: Option<Value>,
}

#[derive(Default, Clone, Serialize)]
struct Board {
    ts_ms: i64,
    poll_secs: u64,
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

type SharedBoard = Arc<RwLock<Board>>;

async fn status(State(board): State<SharedBoard>) -> impl axum::response::IntoResponse {
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(board.read().await.clone()),
    )
}

async fn health(State(board): State<SharedBoard>) -> (StatusCode, Json<Value>) {
    let b = board.read().await;
    let ok = b.ts_ms > 0 && now_ms() - b.ts_ms <= (b.poll_secs * 3 + 30) as i64 * 1000;
    (
        if ok {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(serde_json::json!({"ok":ok,"ts_ms":b.ts_ms})),
    )
}

#[derive(Default)]
struct OrderState {
    book: checks::OrderBook,
    scanned_at_us: Option<i64>,
}
type OrderBooks = Arc<Mutex<BTreeMap<String, OrderState>>>;

async fn collect(
    s: Source,
    m: fr_monitor::config::Monitor,
    http: reqwest::Client,
    market: MarketFeed,
    books: OrderBooks,
) -> (SourceView, Vec<Check>) {
    let now = now_ms();
    let mut checks = vec![market.check(&s, &m, now)];
    let path = s.rocksdb_path.clone();
    // Bootstrap from the configured window, then scan incrementally with an
    // overlap for delayed writes. A failed scan never advances the cursor.
    let since = books
        .lock()
        .ok()
        .and_then(|b| b.get(&s.id).and_then(|s| s.scanned_at_us))
        .map(|ts| ts.saturating_sub(60_000_000).max(0))
        .unwrap_or_else(|| (now - m.order_lookback_secs as i64 * 1000).max(0) * 1000);
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
    let snapshot = match snapshot {
        Ok(value) => {
            checks.extend(checks::check_snapshot(&value, &s, &m, now));
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
    let scope = format!("{}/orders", s.id);
    checks.push(match orders.await {
        Ok(Ok(records)) => match books.lock() {
            Ok(mut books) => {
                let state = books.entry(s.id.clone()).or_default();
                let issues = state.book.observe(&records, &m, now);
                state.scanned_at_us = Some(now * 1000);
                Check::ok(scope, issues)
            }
            Err(_) => Check::failed(scope, "订单巡检状态不可读"),
        },
        Ok(Err(e)) => Check::failed(scope, format!("订单只读检查失败: {e:#}")),
        Err(_) => Check::failed(scope, "订单检查任务失败"),
    });
    (
        SourceView {
            id: s.id,
            namespace: s.namespace,
            markets: s.markets.iter().map(|m| m.venue.clone()).collect(),
            snapshot,
        },
        checks,
    )
}

fn total(views: &[SourceView], key: &str, now: i64, stale_secs: u64) -> Option<f64> {
    views.iter().try_fold(0.0, |sum, s| {
        let v = s.snapshot.as_ref()?["entries"]
            .as_array()?
            .iter()
            .find(|v| v["type"] == "pre_trade_risk" && v["namespace"] == s.namespace)?;
        let entry = &v["entry"];
        let ts = entry["ts_ms"].as_i64()?;
        if ts <= 0 || ts > now + 5000 || now - ts > stale_secs as i64 * 1000 {
            return None;
        }
        let accounts = entry["account_risks"].as_array()?;
        if accounts.is_empty()
            || !accounts.iter().all(|a| {
                a["ts_ms"].as_i64().is_some_and(|t| {
                    t > 0 && t <= now + 5000 && now - t <= stale_secs as i64 * 1000
                })
            })
        {
            return None;
        }
        Some(sum + entry[key].as_f64().filter(|n| n.is_finite())?)
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let cfg = Config::load(&args.config)?;
    let notifier = if args.execute {
        Some(DingTalk::from_env(&cfg.monitor)?)
    } else {
        None
    };
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(cfg.monitor.request_timeout_secs))
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()?;
    let market = MarketFeed::spawn(&cfg)?;
    let board = SharedBoard::default();
    let mut server = if !args.once {
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
            .with_state(board.clone());
        println!(
            "FR monitor dashboard: http://{} (notifications={})",
            cfg.monitor.bind, args.execute
        );
        Some(tokio::spawn(
            async move { axum::serve(listener, app).await },
        ))
    } else {
        None
    };
    // Allow retained IPC messages and the first live BBO update to arrive.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut tracker = Tracker::default();
    let books = OrderBooks::default();
    let mut retry_after = [0_i64; 2];
    let mut failures = [0_u32; 2];
    let mut delivery_errors: [Option<String>; 2] = [None, None];
    loop {
        if server.as_ref().is_some_and(|s| s.is_finished()) {
            server
                .take()
                .unwrap()
                .await
                .context("dashboard task failed")??;
            anyhow::bail!("dashboard stopped unexpectedly");
        }
        let started = tokio::time::Instant::now();
        let mut jobs = JoinSet::new();
        for s in cfg.sources.iter().filter(|s| s.enabled) {
            jobs.spawn(collect(
                s.clone(),
                cfg.monitor.clone(),
                http.clone(),
                market.clone(),
                books.clone(),
            ));
        }
        let mut views = Vec::new();
        let mut checks = Vec::new();
        while let Some(result) = jobs.join_next().await {
            let (view, mut checked) = result.context("FR source check task failed")?;
            views.push(view);
            checks.append(&mut checked);
        }
        views.sort_by(|a, b| a.id.cmp(&b.id));
        let now = now_ms();
        tracker.update(&checks, now);
        let pending = tracker.pending(&cfg.monitor, now);
        {
            let mut b = board.write().await;
            b.ts_ms = now;
            b.poll_secs = cfg.monitor.poll_secs;
            b.stale_secs = cfg.monitor.stale_secs;
            b.notifications_enabled = args.execute;
            b.issues = tracker.active();
            b.history.push_back(History {
                ts_ms: now,
                equity: total(&views, "total_equity", now, cfg.monitor.stale_secs),
                exposure: total(&views, "total_exposure", now, cfg.monitor.stale_secs),
            });
            while b.history.len() > 360 {
                b.history.pop_front();
            }
            b.sources = views;
        }
        if args.once {
            println!("{}", serde_json::to_string_pretty(&*board.read().await)?);
        }
        for (index, market_channel) in [true, false].into_iter().enumerate() {
            if now < retry_after[index] {
                continue;
            }
            let notices: Vec<_> = pending
                .iter()
                .filter(|n| n.market() == market_channel)
                .cloned()
                .collect();
            // Bound each poll's delivery time; unacknowledged notices stay pending.
            for batch in notices.chunks(4).take(2) {
                if let Some(sender) = &notifier {
                    if let Err(error) = sender.send(batch, market_channel).await {
                        failures[index] = failures[index].saturating_add(1);
                        retry_after[index] = now + (1000_i64 << failures[index].min(8));
                        delivery_errors[index] = Some(error.to_string());
                        eprintln!("FR monitor: notification delivery failed; retry scheduled");
                        break;
                    }
                    delivery_errors[index] = None;
                }
                failures[index] = 0;
                tracker.acknowledge(batch, now);
                if !args.once {
                    println!("{}", serde_json::to_string(batch)?);
                }
                let mut b = board.write().await;
                for notice in batch {
                    b.events.push_front(notice.clone());
                }
                while b.events.len() > 100 {
                    b.events.pop_back();
                }
            }
        }
        board.write().await.notification_error = delivery_errors.iter().flatten().next().cloned();
        if args.once {
            return Ok(());
        }
        let delay = Duration::from_secs(cfg.monitor.poll_secs).saturating_sub(started.elapsed());
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = tokio::time::sleep(delay) => {}
        }
    }
    Ok(())
}
