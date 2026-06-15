use anyhow::Result;
use clap::Parser;
use persist_manager::PersistManager;
use runtime_common::affinity::maybe_pin_current_thread;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();

    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    maybe_pin_current_thread(args.core, "PERSIST_MANAGER_CORE")?;

    let manager = PersistManager::new();
    let local = tokio::task::LocalSet::new();
    local.run_until(manager.run()).await
}

#[derive(Parser, Debug)]
#[command(name = "persist_manager")]
struct Args {
    /// Bind the current-thread runtime to a CPU core. Falls back to PERSIST_MANAGER_CORE.
    #[arg(long)]
    core: Option<usize>,
}
