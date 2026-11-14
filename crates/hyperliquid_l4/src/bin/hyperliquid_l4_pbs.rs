use anyhow::Result;
use clap::Parser;
use hyperliquid_l4::{
    ipc::IpcSink,
    runtime::{run, Options},
};

#[derive(Parser)]
#[command(about = "Hyperliquid node L4 -> order-level iceoryx2 batches")]
struct Args {
    #[command(flatten)]
    options: Options,
    /// Dedicated L4 service; never reuse an L2 service name.
    #[arg(long, default_value = "dat_pbs/hyperliquid-futures/l4")]
    service: String,
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    args.options.validate()?;
    let sink = IpcSink::new(&args.service)?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        if let Err(error) = hyperliquid_l4::runtime::shutdown_signal().await {
            log::error!("L4 shutdown signal: {error}");
        }
        let _ = shutdown_tx.send(true);
    });
    run(args.options, sink, shutdown_rx).await
}
