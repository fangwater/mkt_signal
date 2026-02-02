use anyhow::bail;
use clap::Parser;
use mkt_signal::app::MktSignalApp;
use mkt_signal::common::mkt_cfg::home_mkt_cfg_path;
use mkt_signal::cfg::Config;
use mkt_signal::signal::common::TradingVenue;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "mkt_signal")]
#[command(about = "using market data generate signal")]
struct Args {
    /// Trading venue to connect to (e.g., binance_margin, binance_futures, okex_futures)
    #[arg(short, long)]
    venue: TradingVenue,
}

#[tokio::main(worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    // 解析命令行参数
    let args = Args::parse();
    let venue = args.venue;

    // 配置文件路径：$HOME/mkt_pub/config/mkt_cfg.yaml
    let config_path = resolve_cfg_path()?;
    let config_path_str = config_path.to_string_lossy();
    let config = Config::load_config(&config_path_str, venue).await?;

    // 资金费率管理器已移除：预测/借贷逻辑在策略进程内处理

    // 创建并运行应用
    let app = MktSignalApp::new(config).await?;
    app.run().await
}

fn resolve_cfg_path() -> anyhow::Result<PathBuf> {
    let home_path = home_mkt_cfg_path()?;
    if home_path.exists() {
        return Ok(home_path);
    }
    bail!(
        "mkt_cfg.yaml not found at {}",
        home_path.display()
    );
}
