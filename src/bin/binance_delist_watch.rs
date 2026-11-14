//! 币安下架 / Monitoring Tag 观察进程。
//!
//! 列表接口只有 `id/code/title/type/releaseDate`，**没有正文**。
//! 正文走 `article/detail/query?articleCode=`，是 JSON AST，需要展平。
//! 官方公告 WS 没有历史回放：启动先 CMS 回补 + `delist-schedule` / `asset/tags` 快照，
//! 再订 `com_announcement_en` 收增量。
//!
//! ```text
//! cargo run --bin binance_delist_watch -- --once --max-pages 1
//! cargo run --bin binance_delist_watch -- --state data/binance_delist_watch.json
//! ```

use anyhow::{Context, Result};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
use mkt_signal::common::announcement_llm::{
    emit_llm_extract, LlmBudget, LlmConfig, LlmExtractInput,
};
use mkt_signal::common::binance_announcement::{
    announcement_is_interesting, backfill_catalog, fetch_margin_delist_snapshot,
    fetch_monitoring_tag_snapshot, fetch_spot_delist_snapshot, format_event_line,
    format_snapshot_line, http_client, load_state, parse_ws_frame, remember, save_state,
    signed_announcement_ws_url, AnnouncementKind, OfficialSnapshot, ParsedAnnouncement, WatchState,
    ANNOUNCEMENT_TOPIC, CATALOG_DELISTING, CATALOG_LATEST_NEWS,
};
use reqwest::Client;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

#[derive(Parser)]
#[command(name = "binance_delist_watch")]
#[command(about = "Watch Binance delist / monitoring-tag announcements")]
struct Args {
    /// Seen-article state file (article.code 去重).
    #[arg(long, default_value = "data/binance_delist_watch.json")]
    state: PathBuf,

    /// CMS page size.
    #[arg(long, default_value_t = 20)]
    page_size: u32,

    /// Max CMS pages per catalog on each backfill.
    #[arg(long, default_value_t = 3)]
    max_pages: u32,

    /// Skip fetching CMS article body (title-only parse).
    #[arg(long)]
    no_body: bool,

    /// Skip unofficial CMS backfill.
    #[arg(long)]
    skip_cms: bool,

    /// Skip official delist-schedule / asset-tags snapshot.
    #[arg(long)]
    skip_official: bool,

    /// Skip announcement websocket (backfill/snapshot only).
    #[arg(long)]
    skip_ws: bool,

    /// Run one backfill + snapshot pass, then exit.
    #[arg(long)]
    once: bool,

    /// CMS backfill interval while watching.
    #[arg(long, default_value_t = 300)]
    cms_interval_secs: u64,

    /// Official snapshot interval while watching.
    #[arg(long, default_value_t = 600)]
    official_interval_secs: u64,

    /// Announcement WS recvWindow (ms).
    #[arg(long, default_value_t = 30_000)]
    recv_window: u64,

    /// Skip optional LLM structured extract.
    #[arg(long)]
    skip_llm: bool,

    /// Max new announcements to extract this process (0 = unlimited).
    #[arg(long, default_value_t = 0)]
    llm_max: usize,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let client = http_client()?;
    let llm = load_llm(&args);
    let llm_client = llm.as_ref().and_then(|_| LlmConfig::http_client().ok());
    let mut state = load_state(&args.state).unwrap_or_else(|err| {
        warn!("load state failed, start empty: {err:#}");
        WatchState::default()
    });

    let mut llm_budget = LlmBudget::new(args.llm_max);
    run_backfill_pass(
        &args,
        &client,
        &mut state,
        llm.as_ref(),
        llm_client.as_ref(),
        &mut llm_budget,
    )
    .await?;
    persist(&args.state, &state)?;

    if args.once || args.skip_ws {
        return Ok(());
    }

    let api_key = std::env::var("BINANCE_API_KEY")
        .context("BINANCE_API_KEY is required for announcement websocket")?;
    let api_secret = std::env::var("BINANCE_API_SECRET")
        .context("BINANCE_API_SECRET is required for announcement websocket")?;
    if api_key.trim().is_empty() || api_secret.trim().is_empty() {
        anyhow::bail!("BINANCE_API_KEY/BINANCE_API_SECRET must not be empty");
    }

    let mut cms_tick = time::interval(Duration::from_secs(args.cms_interval_secs.max(30)));
    cms_tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    cms_tick.tick().await;
    let mut official_tick =
        time::interval(Duration::from_secs(args.official_interval_secs.max(60)));
    official_tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    official_tick.tick().await;

    loop {
        tokio::select! {
            _ = cms_tick.tick() => {
                if let Err(err) = cms_backfill(&args, &client, &mut state, llm.as_ref(), llm_client.as_ref(), &mut llm_budget).await {
                    warn!("CMS backfill failed: {err:#}");
                }
                persist(&args.state, &state)?;
            }
            _ = official_tick.tick() => {
                if let Err(err) = official_snapshots(&args, &client).await {
                    warn!("official snapshot failed: {err:#}");
                }
            }
            result = announcement_ws_session(&api_key, &api_secret, args.recv_window, &mut state, llm.as_ref(), llm_client.as_ref(), &mut llm_budget) => {
                if let Err(err) = result {
                    warn!("announcement ws session ended: {err:#}");
                }
                persist(&args.state, &state)?;
                time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
}

fn load_llm(args: &Args) -> Option<LlmConfig> {
    if args.skip_llm {
        return None;
    }
    match LlmConfig::from_env() {
        Some(cfg) => {
            info!(
                "llm extract enabled primary={} backup={}",
                cfg.primary.model, cfg.backup.model
            );
            Some(cfg)
        }
        None => {
            info!("llm extract disabled: set DELIST_LLM_API_KEY/URL or OPENAI_API_KEY");
            None
        }
    }
}

async fn run_backfill_pass(
    args: &Args,
    client: &Client,
    state: &mut WatchState,
    llm: Option<&LlmConfig>,
    llm_client: Option<&Client>,
    llm_budget: &mut LlmBudget,
) -> Result<()> {
    cms_backfill(args, client, state, llm, llm_client, llm_budget).await?;
    official_snapshots(args, client).await?;
    Ok(())
}

async fn cms_backfill(
    args: &Args,
    client: &Client,
    state: &mut WatchState,
    llm: Option<&LlmConfig>,
    llm_client: Option<&Client>,
    llm_budget: &mut LlmBudget,
) -> Result<()> {
    if args.skip_cms {
        return Ok(());
    }
    let fetch_body = !args.no_body;
    let mut fresh = Vec::new();
    fresh.extend(
        backfill_catalog(
            client,
            CATALOG_DELISTING,
            args.page_size,
            args.max_pages,
            state,
            fetch_body,
        )
        .await?,
    );
    fresh.extend(
        backfill_catalog(
            client,
            CATALOG_LATEST_NEWS,
            args.page_size,
            args.max_pages,
            state,
            fetch_body,
        )
        .await?,
    );
    emit_new(state, fresh, llm, llm_client, llm_budget).await;
    Ok(())
}

async fn official_snapshots(args: &Args, client: &Client) -> Result<()> {
    if args.skip_official {
        return Ok(());
    }
    emit_snapshot(fetch_spot_delist_snapshot(client).await);
    emit_snapshot(fetch_margin_delist_snapshot(client).await);
    emit_snapshot(fetch_monitoring_tag_snapshot(client).await);
    Ok(())
}

fn emit_snapshot(result: Result<OfficialSnapshot>) {
    match result {
        Ok(snapshot) => {
            info!(
                "official snapshot source={} count={}",
                snapshot.source,
                snapshot.items.len()
            );
            println!("{}", format_snapshot_line(&snapshot));
        }
        Err(err) => warn!("official snapshot skipped: {err:#}"),
    }
}

async fn emit_new(
    state: &mut WatchState,
    events: Vec<ParsedAnnouncement>,
    llm: Option<&LlmConfig>,
    llm_client: Option<&Client>,
    llm_budget: &mut LlmBudget,
) {
    let now_ms = chrono::Utc::now().timestamp_millis();
    for event in events {
        if !event.kind.is_watch_relevant() && event.catalog_id != CATALOG_DELISTING {
            continue;
        }
        if remember(state, &event, now_ms) {
            info!(
                "announcement kind={} assets={:?} symbols={:?} title={}",
                event.kind, event.assets, event.symbols, event.title
            );
            println!("{}", format_event_line(&event));
            if let (Some(llm), Some(llm_client)) = (llm, llm_client) {
                if llm_budget.allow() {
                    emit_llm_extract(llm_client, llm, &LlmExtractInput::from_parsed(&event)).await;
                }
            }
        }
    }
}

fn persist(path: &PathBuf, state: &WatchState) -> Result<()> {
    save_state(path, state)?;
    Ok(())
}

async fn announcement_ws_session(
    api_key: &str,
    api_secret: &str,
    recv_window: u64,
    state: &mut WatchState,
    llm: Option<&LlmConfig>,
    llm_client: Option<&Client>,
    llm_budget: &mut LlmBudget,
) -> Result<()> {
    let url = signed_announcement_ws_url(api_secret, recv_window)?;
    let mut request = url
        .as_str()
        .into_client_request()
        .context("build announcement ws request failed")?;
    request.headers_mut().insert(
        "X-MBX-APIKEY",
        HeaderValue::from_str(api_key).context("invalid BINANCE_API_KEY header")?,
    );
    info!("connecting announcement ws topic={ANNOUNCEMENT_TOPIC}");
    let (mut ws, _) = tokio_tungstenite::connect_async(request)
        .await
        .context("connect announcement ws failed")?;

    let subscribe = serde_json::json!({
        "command": "SUBSCRIBE",
        "value": ANNOUNCEMENT_TOPIC,
    });
    ws.send(Message::Text(subscribe.to_string()))
        .await
        .context("send announcement SUBSCRIBE failed")?;

    let mut ping = time::interval(Duration::from_secs(30));
    ping.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let session_deadline = time::sleep(Duration::from_secs(23 * 3600));
    tokio::pin!(session_deadline);

    loop {
        tokio::select! {
            _ = &mut session_deadline => {
                info!("announcement ws 23h refresh");
                let _ = ws.close(None).await;
                return Ok(());
            }
            _ = ping.tick() => {
                if let Err(err) = ws.send(Message::Ping(Vec::new())).await {
                    error!("announcement ws ping failed: {err}");
                    return Err(err.into());
                }
            }
            incoming = ws.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        handle_ws_text(&text, state, llm, llm_client, llm_budget).await
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        if let Ok(text) = String::from_utf8(bytes) {
                            handle_ws_text(&text, state, llm, llm_client, llm_budget).await;
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        ws.send(Message::Pong(payload)).await.ok();
                    }
                    Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        warn!("announcement ws closed: {frame:?}");
                        return Ok(());
                    }
                    Some(Err(err)) => return Err(err.into()),
                    None => {
                        warn!("announcement ws stream ended");
                        return Ok(());
                    }
                }
            }
        }
    }
}

async fn handle_ws_text(
    text: &str,
    state: &mut WatchState,
    llm: Option<&LlmConfig>,
    llm_client: Option<&Client>,
    llm_budget: &mut LlmBudget,
) {
    match parse_ws_frame(text) {
        Ok(Some(event)) => {
            if announcement_is_interesting(&event.title, event.catalog_id)
                || event.kind != AnnouncementKind::OtherNews
            {
                emit_new(state, vec![event], llm, llm_client, llm_budget).await;
            }
        }
        Ok(None) => {}
        Err(err) => warn!("skip announcement ws frame: {err:#} raw={text}"),
    }
}
