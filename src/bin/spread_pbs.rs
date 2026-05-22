use anyhow::{bail, Result};
use clap::Parser;
use std::path::PathBuf;

use mkt_signal::cfg::Config;
use mkt_signal::signal::common::TradingVenue;
use mkt_signal::common::affinity::pin_to_core;
use mkt_signal::spread_pbs::SpreadPbsApp;

#[derive(Parser)]
#[command(name = "spread_pbs")]
#[command(about = "Dedicated high-speed askbidspread publisher (single-venue, pinned core).")]
struct Args {
    /// Trading venue（OKex / Binance / Bybit / Gate / Bitget × spot+futures，共 10 个）
    #[arg(short, long)]
    venue: TradingVenue,

    /// 绑定到的 CPU 核心编号
    #[arg(short, long)]
    core: usize,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    pin_to_core(args.core)?;

    let config_path = resolve_cfg_path()?;
    log::info!("spread_pbs cfg path: {}", config_path.display());
    let config_str = config_path.to_string_lossy();
    let config = Config::load_config(&config_str, args.venue).await?;

    let app = SpreadPbsApp::new(config);

    // current_thread runtime + spawn_local 需要 LocalSet 上下文
    let local = tokio::task::LocalSet::new();
    local.run_until(app.run()).await
}

/// 配置文件路径优先级：
/// 1) `$HOME/spread_pbs/config/mkt_cfg.yaml`（spread_pbs 自己的部署目录）
/// 2) `$HOME/dat_pbs/config/mkt_cfg.yaml`（兜底，复用 dat_pbs 那份）
fn resolve_cfg_path() -> Result<PathBuf> {
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
        "mkt_cfg.yaml not found at {} or {}",
        primary.display(),
        fallback.display()
    );
}
