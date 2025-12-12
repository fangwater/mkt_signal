use anyhow::Result;
use clap::Parser;
use mkt_signal::bridge::cfg::BridgeConfig;
use mkt_signal::bridge::BridgeApp;

#[derive(Parser, Debug)]
#[command(name = "ipc_bridge")]
#[command(about = "Bridge Iceoryx2 IPC channels across network via ZMQ")]
struct Args {
    /// Path to bridge config yaml
    #[arg(long)]
    cfg: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let log_env = env_logger::Env::default().default_filter_or("info");
    env_logger::Builder::from_env(log_env).init();

    let args = Args::parse();
    let cfg = BridgeConfig::load_from_file(&args.cfg)?;

    let app = BridgeApp::new(cfg);
    let local = tokio::task::LocalSet::new();
    local.run_until(app.run()).await
}
