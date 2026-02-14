//! Model Publisher 入口
//!
//! 使用方式: cargo run --bin model_pub -- <model_name>

use anyhow::Result;
use clap::Parser;
use log::info;

use mkt_signal::factor_pub::model_pub::app::ModelPubApp;

const DEFAULT_CONFIG_PATH: &str = "config/model_pub.toml";

#[derive(Parser)]
#[command(name = "model_pub")]
#[command(
    about = "Model Publisher - online model bootstrap, vector in, model inference, vector out"
)]
struct Args {
    /// Model name registered in model_manager
    model_name: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    info!(
        "Starting model_pub: model_name={} config={}",
        args.model_name, DEFAULT_CONFIG_PATH
    );

    let mut app = ModelPubApp::new(DEFAULT_CONFIG_PATH, &args.model_name).await?;
    app.run()
}
