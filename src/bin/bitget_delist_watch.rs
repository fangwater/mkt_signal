//! Bitget 下架公告拉取。只打印官方字段，不做 ticker / 标题解析。
//!
//! 公告：`GET /api/v2/public/annoucements?annType=symbol_delisting`
//! 近一个月，无正文，只有 title + annUrl。
//! 当前状态：现货 / U 本位合约 `offTime`。
//!
//! ```text
//! cargo run --bin bitget_delist_watch -- --once
//! cargo run --bin bitget_delist_watch -- --state data/bitget_delist_watch.json
//! ```

use anyhow::Result;
use clap::Parser;
use log::{info, warn};
use mkt_signal::common::announcement_llm::{
    emit_llm_extract, LlmBudget, LlmConfig, LlmExtractInput,
};
use mkt_signal::common::announcement_watch::{
    emit_announcement, http_client, load_store, save_store, SeenStore,
};
use mkt_signal::common::bitget_announcement::{fetch_delist_notices, fetch_offtime_snapshot};
use reqwest::Client;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time;

#[derive(Parser)]
#[command(name = "bitget_delist_watch")]
#[command(about = "Fetch Bitget delist announcements without parsing titles")]
struct Args {
    #[arg(long, default_value = "data/bitget_delist_watch.json")]
    state: PathBuf,

    #[arg(long, default_value = "en_US")]
    language: String,

    #[arg(long, default_value_t = 10)]
    limit: u32,

    #[arg(long, default_value_t = 5)]
    max_pages: u32,

    #[arg(long)]
    skip_announcements: bool,

    #[arg(long)]
    skip_official: bool,

    #[arg(long)]
    once: bool,

    #[arg(long, default_value_t = 120)]
    poll_interval_secs: u64,

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
    let mut store = load_store(&args.state).unwrap_or_else(|err| {
        warn!("load state failed, start empty: {err:#}");
        SeenStore::default()
    });

    let mut llm_budget = LlmBudget::new(args.llm_max);
    run_pass(
        &args,
        &client,
        &mut store,
        llm.as_ref(),
        llm_client.as_ref(),
        &mut llm_budget,
    )
    .await?;
    persist(&args.state, &store)?;
    if args.once {
        return Ok(());
    }

    let mut poll = time::interval(Duration::from_secs(args.poll_interval_secs.max(30)));
    poll.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    poll.tick().await;
    let mut official = time::interval(Duration::from_secs(args.official_interval_secs.max(60)));
    official.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    official.tick().await;

    loop {
        tokio::select! {
            _ = poll.tick() => {
                if let Err(err) = poll_announcements(&args, &client, &mut store, llm.as_ref(), llm_client.as_ref(), &mut llm_budget).await {
                    warn!("Bitget announcement poll failed: {err:#}");
                }
                persist(&args.state, &store)?;
            }
            _ = official.tick() => {
                if let Err(err) = emit_official(&args, &client).await {
                    warn!("Bitget offtime snapshot failed: {err:#}");
                }
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

async fn run_pass(
    args: &Args,
    client: &Client,
    store: &mut SeenStore,
    llm: Option<&LlmConfig>,
    llm_client: Option<&Client>,
    llm_budget: &mut LlmBudget,
) -> Result<()> {
    poll_announcements(args, client, store, llm, llm_client, llm_budget).await?;
    emit_official(args, client).await?;
    Ok(())
}

async fn poll_announcements(
    args: &Args,
    client: &Client,
    store: &mut SeenStore,
    llm: Option<&LlmConfig>,
    llm_client: Option<&Client>,
    llm_budget: &mut LlmBudget,
) -> Result<()> {
    if args.skip_announcements {
        return Ok(());
    }
    let items =
        fetch_delist_notices(client, &args.language, args.limit, args.max_pages, store).await?;
    info!("bitget announcements fetched new={}", items.len());
    for item in items {
        if emit_announcement(store, item.clone()) {
            if let (Some(llm), Some(llm_client)) = (llm, llm_client) {
                if llm_budget.allow() {
                    emit_llm_extract(llm_client, llm, &LlmExtractInput::from_raw(&item)).await;
                }
            }
        }
    }
    Ok(())
}

async fn emit_official(args: &Args, client: &Client) -> Result<()> {
    if args.skip_official {
        return Ok(());
    }
    let snapshot = fetch_offtime_snapshot(client).await?;
    let spot_n = snapshot
        .get("spot")
        .and_then(|v| v.as_array())
        .map(|v| v.len())
        .unwrap_or(0);
    let fut_n = snapshot
        .get("usdt_futures")
        .and_then(|v| v.as_array())
        .map(|v| v.len())
        .unwrap_or(0);
    let coin_n = snapshot
        .get("coin_futures")
        .and_then(|v| v.as_array())
        .map(|v| v.len())
        .unwrap_or(0);
    info!("bitget offtime snapshot spot={spot_n} usdt_futures={fut_n} coin_futures={coin_n}");
    println!("{snapshot}");
    Ok(())
}

fn persist(path: &PathBuf, store: &SeenStore) -> Result<()> {
    save_store(path, store)
}
