use anyhow::Result;
use clap::Parser;
use log::{error, info};
use mkt_signal::common::affinity::maybe_pin_current_thread;
use mkt_signal::latency_stable_monitor::cfg::LatencyStableMonitorConfig;
use mkt_signal::latency_stable_monitor::LatencyStableMonitorApp;
use tokio::signal;
use tokio_util::sync::CancellationToken;

#[derive(Parser, Debug)]
#[command(name = "latency_stable_monitor")]
#[command(
    about = "Monitor latency snapshot IPC services and publish JSON snapshots over one ZMQ PUB socket"
)]
struct Args {
    /// Path to latency stable monitor config yaml.
    #[arg(long)]
    cfg: String,

    /// Bind main runtime thread to a CPU core. Falls back to LATENCY_STABLE_MONITOR_CORE.
    #[arg(long)]
    core: Option<usize>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let log_env = env_logger::Env::default().default_filter_or("info");
    env_logger::Builder::from_env(log_env).init();

    let args = Args::parse();
    maybe_pin_current_thread(args.core, "LATENCY_STABLE_MONITOR_CORE")?;

    let cfg = LatencyStableMonitorConfig::load_from_file(&args.cfg)?;
    let shutdown = CancellationToken::new();
    setup_signal_handlers(&shutdown);

    let app = LatencyStableMonitorApp::new(cfg);
    info!("latency_stable_monitor using config {}", args.cfg);
    app.run(shutdown).await
}

fn setup_signal_handlers(token: &CancellationToken) {
    let ctrl_c = token.clone();
    tokio::spawn(async move {
        if let Err(e) = signal::ctrl_c().await {
            error!("latency_stable_monitor: listen ctrl_c failed: {}", e);
            return;
        }
        info!("latency_stable_monitor: received Ctrl+C");
        ctrl_c.cancel();
    });

    #[cfg(unix)]
    {
        let sigterm_token = token.clone();
        tokio::spawn(async move {
            match signal::unix::signal(signal::unix::SignalKind::terminate()) {
                Ok(mut stream) => {
                    stream.recv().await;
                    info!("latency_stable_monitor: received SIGTERM");
                    sigterm_token.cancel();
                }
                Err(e) => {
                    error!("latency_stable_monitor: listen SIGTERM failed: {}", e);
                }
            }
        });
    }
}
