use anyhow::Result;
use log::info;
use mkt_signal::pre_trade::PreTrade;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    info!("pre_trade starting (configuration via environment variables)");
    info!("Required env vars: BINANCE_API_KEY, BINANCE_API_SECRET");
    info!("Optional env vars: SPOT_ASSET_FILTER, REDIS_URL");
    SignalChannel::initialize("my_channel", Some("backward_channel"))?;
    let pre_trade = PreTrade::new();
    let local = tokio::task::LocalSet::new();
    local.run_until(pre_trade.run()).await
}
