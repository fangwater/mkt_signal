use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use viz_server::native::{self, config::Config};

#[derive(Parser)]
#[command(about = "Read-only domestic futures Viz for CTP/Yida stores and native market IPC")]
struct Args {
    #[arg(long, default_value = "config/cn_futures_viz.toml")]
    config: PathBuf,
    #[arg(long)]
    check_config: bool,
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let cfg = Config::load(&args.config)?;
    if args.check_config {
        return Ok(());
    }
    native::run(cfg).await
}
