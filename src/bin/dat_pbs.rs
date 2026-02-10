use anyhow::bail;
use clap::Parser;
use mkt_signal::app::MktSignalApp;
use mkt_signal::cfg::Config;
use mkt_signal::signal::common::TradingVenue;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "dat_pbs")]
#[command(about = "using market data generate signal (dat_pbs channel)")]
struct Args {
    /// Trading venue to connect to (e.g., binance_margin, binance_futures, okex_futures)
    #[arg(short, long)]
    venue: TradingVenue,
}

#[tokio::main(worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();
    let venue = args.venue;

    // 配置文件路径优先：$HOME/dat_pbs/config/mkt_cfg.yaml
    let config_path = resolve_cfg_path()?;
    let config_path_str = config_path.to_string_lossy();
    let config = Config::load_config(&config_path_str, venue).await?;
    let config = Box::leak(Box::new(config));

    let app = MktSignalApp::new(config).await?;
    app.run().await
}

fn resolve_cfg_path() -> anyhow::Result<PathBuf> {
    let dat_pbs = home_cfg_path("dat_pbs")?;
    if dat_pbs.exists() {
        return Ok(dat_pbs);
    }

    bail!(
        "mkt_cfg.yaml not found at {}",
        home_cfg_path("dat_pbs")?.display()
    );
}

fn home_cfg_path(dir_name: &str) -> anyhow::Result<PathBuf> {
    if let Ok(home) = std::env::var("HOME") {
        if !home.trim().is_empty() {
            return Ok(PathBuf::from(home).join(format!("{dir_name}/config/mkt_cfg.yaml")));
        }
    }
    if let Ok(user) = std::env::var("USER") {
        if !user.trim().is_empty() {
            return Ok(PathBuf::from(format!(
                "/home/{}/{}/config/mkt_cfg.yaml",
                user, dir_name
            )));
        }
    }
    bail!(
        "HOME/USER not set; cannot resolve /home/<user>/{}/config/mkt_cfg.yaml",
        dir_name
    )
}
