mod connection;
mod sub_msg;
mod cfg;
mod forwarder;
mod mkt_msg;
mod parser;
mod proxy;
mod receiver;
mod restart_checker;
mod app;
use cfg::Config;
use app::CryptoProxyApp;
use tokio::sync::OnceCell;
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Exchange {
    Binance,
    #[value(name = "binance-futures")]
    #[serde(rename = "binance-futures")]
    BinanceFutures,
    Okex,
    #[value(name = "okex-swap")]
    #[serde(rename = "okex-swap")]
    OkexSwap,
    Bybit,
    #[value(name = "bybit-spot")]
    #[serde(rename = "bybit-spot")]
    BybitSpot,
}


#[derive(Parser)]
#[command(name = "crypto_proxy")]
#[command(about = "Cryptocurrency market data proxy")]
struct Args {
    /// Exchange to connect to
    #[arg(short, long)]
    exchange: Exchange,
}

#[tokio::main(worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "INFO");
    env_logger::init();

    // 解析命令行参数
    let args = Args::parse();
    let exchange = args.exchange;
    
    // 固定配置文件路径
    let config_path = "mkt_cfg.yaml";

    static CFG: OnceCell<Config> = OnceCell::const_new();

    async fn get_config(config_path: &str, exchange: Exchange) -> &'static Config {
        CFG.get_or_init(|| async {
            Config::load_config(config_path, exchange).await.unwrap()
        }).await
    }
    
    let config = get_config(config_path, exchange).await;
    
    // 创建并运行应用
    let app = CryptoProxyApp::new(config).await?;
    app.run().await
}