mod connection;
mod sub_msg;
mod cfg;
mod exchange;
mod iceoryx_forwarder;
mod mkt_msg;
mod parser;
mod proxy;
mod app;
mod market_state;
use cfg::Config;
use app::MktSignalApp;
use exchange::Exchange;
use tokio::sync::OnceCell;
use clap::Parser;


#[derive(Parser)]
#[command(name = "mkt_signal")]
#[command(about = "using market data generate signal")]
struct Args {
    /// Exchange to connect to
    #[arg(short, long)]
    exchange: Exchange,
}

#[tokio::main(worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "DEBUG");
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
    
    // 初始化资金费率管理器
    let funding_manager = crate::market_state::FundingRateManager::instance();
    tokio::spawn(async move {
        if let Err(e) = funding_manager.initialize().await {
            log::error!("初始化资金费率管理器失败: {}", e);
        }
    });
    
    // 创建并运行应用
    let app = MktSignalApp::new(config).await?;
    app.run().await
}
