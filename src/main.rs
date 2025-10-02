use clap::Parser;
use mkt_signal::app::MktSignalApp;
use mkt_signal::cfg::Config;
use mkt_signal::exchange::Exchange;
use tokio::sync::OnceCell;

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
    // 设置日志级别：默认 DEBUG，但关闭 tungstenite 和 reqwest 的 debug 日志
    std::env::set_var("RUST_LOG", "info,tungstenite=info,reqwest=info");
    env_logger::init();

    // 解析命令行参数
    let args = Args::parse();
    let exchange = args.exchange;

    // 固定配置文件路径
    let config_path = "config/mkt_cfg.yaml";

    static CFG: OnceCell<Config> = OnceCell::const_new();

    async fn get_config(config_path: &str, exchange: Exchange) -> &'static Config {
        CFG.get_or_init(|| async { Config::load_config(config_path, exchange).await.unwrap() })
            .await
    }

    let config = get_config(config_path, exchange).await;

    // 资金费率管理器已移除：预测/借贷逻辑在策略进程内处理

    // 创建并运行应用
    let app = MktSignalApp::new(config).await?;
    app.run().await
}
