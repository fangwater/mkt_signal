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
use clap::Parser;

#[derive(Parser)]
#[command(name = "crypto_proxy")]
#[command(about = "config file path")]
struct Args {
    /// 配置文件路径
    #[arg(short, long)]
    config: String,
}

#[tokio::main(worker_threads = 3)]
async fn main() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "INFO");
    env_logger::init();

    // 解析命令行参数
    let args = Args::parse();
    let config_path = args.config;

    static CFG: OnceCell<Config> = OnceCell::const_new();

    async fn get_config(config_path: &str) -> &'static Config {
        CFG.get_or_init(|| async {
            Config::load_config(config_path).await.unwrap()
        }).await
    }
    
    let config = get_config(&config_path).await;
    
    // 创建并运行应用
    let app = CryptoProxyApp::new(config).await?;
    app.run().await
}