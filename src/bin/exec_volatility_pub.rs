use anyhow::Result;
use clap::Parser;
use mkt_signal::factor_pub::exec_volatility_pub::app::ExecVolatilityPubApp;
use runtime_common::affinity::maybe_pin_current_thread;
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "config/exec_volatility_pub.toml")]
    config: String,
    #[arg(long, default_value = "run/exec_volatility_status.json")]
    status_path: PathBuf,
    #[arg(long)]
    core: Option<usize>,
    /// Warm up alongside the old publisher, then acquire its existing IPC services.
    #[arg(long)]
    wait_for_publishers: bool,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    maybe_pin_current_thread(args.core, "EXEC_VOLATILITY_CORE")?;
    ExecVolatilityPubApp::new(&args.config, &args.status_path, args.wait_for_publishers)?.run()
}
