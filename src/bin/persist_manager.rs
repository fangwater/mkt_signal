use anyhow::Result;
use clap::Parser;
use mkt_signal::persist_manager::PersistManager;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    let manager = PersistManager::new();
    let local = tokio::task::LocalSet::new();
    local.run_until(manager.run()).await
}

#[derive(Parser, Debug)]
#[command(name = "persist_manager")]
struct Args {}
