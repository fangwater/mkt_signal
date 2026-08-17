//! jp-meta `depth_pub_general`：本机 8 路 depth25（BN/OKX/Bitget/Gate × margin+futures）。
//! Bybit 不在本机（在 sg），故不纳入。

use anyhow::{bail, Context, Result};
use clap::Parser;
use std::path::{Path, PathBuf};

use mkt_signal::depth_pub::general::DepthPubGeneralRunner;
use runtime_common::affinity::maybe_pin_current_thread;

#[derive(Parser)]
#[command(name = "depth_pub_general")]
#[command(about = "jp-meta 8-way depth publisher (no Bybit: that stack runs on sg)")]
struct Args {
    /// 绑定主线程到指定 CPU 核（可选）；未提供则尝试 DEPTH_PUB_CORE
    #[arg(long)]
    core: Option<usize>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    maybe_pin_current_thread(args.core, "DEPTH_PUB_CORE")?;

    let cfg_path = resolve_cfg_path()?;
    log::info!("depth_pub_general cfg path: {}", cfg_path.display());

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let mut runner = DepthPubGeneralRunner::new(&cfg_path.to_string_lossy()).await?;
            runner.run().await
        })
        .await
}

fn resolve_cfg_path() -> Result<PathBuf> {
    let current_dir = std::env::current_dir().context("resolve depth_pub_general cwd")?;
    resolve_cfg_path_from(&current_dir)
}

fn resolve_cfg_path_from(current_dir: &Path) -> Result<PathBuf> {
    let venue_config = current_dir.join("config/mkt_cfg.yaml");
    if venue_config.exists() {
        return Ok(venue_config);
    }

    let home = std::env::var("HOME")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| std::env::var("USER").ok().map(|u| format!("/home/{}", u)));
    let Some(home) = home else {
        bail!("HOME/USER not set; cannot resolve mkt_cfg.yaml");
    };

    let primary = PathBuf::from(&home).join("spread_pbs/config/mkt_cfg.yaml");
    if primary.exists() {
        return Ok(primary);
    }
    let fallback = PathBuf::from(&home).join("dat_pbs/config/mkt_cfg.yaml");
    if fallback.exists() {
        return Ok(fallback);
    }
    bail!(
        "mkt_cfg.yaml not found at {}, {}, or {}",
        venue_config.display(),
        primary.display(),
        fallback.display()
    )
}
