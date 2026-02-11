//! Model Publisher 入口
//!
//! 使用方式: cargo run --bin model_pub -- --config config/model_pub.toml

use anyhow::Result;
use clap::Parser;
use log::info;

use mkt_signal::factor_pub::model_pub::app::ModelPubApp;

#[derive(Parser)]
#[command(name = "model_pub")]
#[command(about = "Model Publisher - vector in, model inference, vector out")]
struct Args {
    /// Path to model pub TOML config
    #[arg(short, long, default_value = "config/model_pub.toml")]
    config: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    info!("Loading config from: {}", args.config);

    let mut app = ModelPubApp::new(&args.config)?;
    app.run()
}
