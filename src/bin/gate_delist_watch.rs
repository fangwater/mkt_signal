//! Gate 下架公告拉取。只打印官方字段，不做 ticker / 标题解析。
//!
//! 官方没有历史 REST。增量走
//! `wss://api.gateio.ws/ws/v4/ann` / `announcement.summary_delisting`。
//! 当前状态：现货 `delisting_time`、合约 `in_delisting`。
//!
//! ```text
//! cargo run --bin gate_delist_watch -- --once
//! cargo run --bin gate_delist_watch -- --state data/gate_delist_watch.json
//! ```

use anyhow::{Context, Result};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
use mkt_signal::common::announcement_llm::{
    emit_llm_extract, LlmBudget, LlmConfig, LlmExtractInput,
};
use mkt_signal::common::announcement_watch::{
    emit_announcement, http_client, load_store, save_store, SeenStore,
};
use mkt_signal::common::gate_announcement::{
    fetch_market_snapshot, parse_ws_text, ping_frame, subscribe_frame, ANN_WS_URL,
};
use reqwest::Client;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time;
use tokio_tungstenite::tungstenite::Message;

#[derive(Parser)]
#[command(name = "gate_delist_watch")]
#[command(about = "Watch Gate delist announcements without parsing titles")]
struct Args {
    #[arg(long, default_value = "data/gate_delist_watch.json")]
    state: PathBuf,

    #[arg(long, default_value = "en")]
    language: String,

    #[arg(long)]
    skip_ws: bool,

    #[arg(long)]
    skip_official: bool,

    #[arg(long)]
    once: bool,

    #[arg(long, default_value_t = 600)]
    official_interval_secs: u64,

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
    let mut llm_budget = LlmBudget::new(args.llm_max);
    let mut store = load_store(&args.state).unwrap_or_else(|err| {
        warn!("load state failed, start empty: {err:#}");
        SeenStore::default()
    });

    emit_official(&args, &client).await?;
    persist(&args.state, &store)?;
    if args.once || args.skip_ws {
        return Ok(());
    }

    let mut official = time::interval(Duration::from_secs(args.official_interval_secs.max(60)));
    official.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    official.tick().await;

    loop {
        tokio::select! {
            _ = official.tick() => {
                if let Err(err) = emit_official(&args, &client).await {
                    warn!("Gate market snapshot failed: {err:#}");
                }
            }
            result = announcement_ws_session(&args.language, &mut store, llm.as_ref(), llm_client.as_ref(), &mut llm_budget) => {
                if let Err(err) = result {
                    warn!("Gate announcement ws session ended: {err:#}");
                }
                persist(&args.state, &store)?;
                time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
}

async fn emit_official(args: &Args, client: &Client) -> Result<()> {
    if args.skip_official {
        return Ok(());
    }
    let snapshot = fetch_market_snapshot(client).await?;
    let spot_n = snapshot
        .get("spot_pairs")
        .and_then(|v| v.as_array())
        .map(|v| v.len())
        .unwrap_or(0);
    let usdt_n = snapshot
        .get("usdt_futures_in_delisting")
        .and_then(|v| v.as_array())
        .map(|v| v.len())
        .unwrap_or(0);
    let btc_n = snapshot
        .get("btc_futures_in_delisting")
        .and_then(|v| v.as_array())
        .map(|v| v.len())
        .unwrap_or(0);
    info!("gate market snapshot spot={spot_n} usdt_futures={usdt_n} btc_futures={btc_n}");
    println!("{snapshot}");
    Ok(())
}

fn persist(path: &PathBuf, store: &SeenStore) -> Result<()> {
    save_store(path, store)
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

async fn announcement_ws_session(
    language: &str,
    store: &mut SeenStore,
    llm: Option<&LlmConfig>,
    llm_client: Option<&Client>,
    llm_budget: &mut LlmBudget,
) -> Result<()> {
    info!("connecting Gate announcement ws {ANN_WS_URL}");
    let (mut ws, _) = tokio_tungstenite::connect_async(ANN_WS_URL)
        .await
        .context("connect Gate announcement ws failed")?;
    ws.send(Message::Text(subscribe_frame(language)))
        .await
        .context("send Gate delist subscribe failed")?;

    let mut ping = time::interval(Duration::from_secs(20));
    ping.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = ping.tick() => {
                if let Err(err) = ws.send(Message::Text(ping_frame())).await {
                    error!("Gate announcement ping failed: {err}");
                    return Err(err.into());
                }
            }
            incoming = ws.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        handle_ws_text(&text, store, llm, llm_client, llm_budget).await
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        if let Ok(text) = String::from_utf8(bytes) {
                            handle_ws_text(&text, store, llm, llm_client, llm_budget).await;
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

async fn handle_ws_text(
    text: &str,
    store: &mut SeenStore,
    llm: Option<&LlmConfig>,
    llm_client: Option<&Client>,
    llm_budget: &mut LlmBudget,
) {
    match parse_ws_text(text) {
        Ok(Some(item)) => {
            if emit_announcement(store, item.clone()) {
                if let (Some(llm), Some(llm_client)) = (llm, llm_client) {
                    if llm_budget.allow() {
                        emit_llm_extract(llm_client, llm, &LlmExtractInput::from_raw(&item)).await;
                    }
                }
            }
        }
        Ok(None) => {}
        Err(err) => warn!("skip Gate announcement ws frame: {err:#} raw={text}"),
    }
}
