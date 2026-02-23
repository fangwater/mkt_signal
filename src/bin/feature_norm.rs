//! Feature Norm 入口
//!
//! 使用方式:
//! cargo run --bin feature_norm -- --config config/feature_norm.yaml

use anyhow::Result;
use clap::Parser;
use log::info;

use mkt_signal::factor_pub::feature_norm::app::FeatureNormApp;

#[derive(Parser)]
#[command(name = "feature_norm")]
#[command(about = "Z-score normalization for FeatureMsg between fusion_factor_pub and model_pub")]
struct Args {
    /// Config path
    #[arg(short, long, default_value = "config/feature_norm.yaml")]
    config: String,
}

fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    info!("Starting feature_norm: config={}", args.config);

    let mut app = FeatureNormApp::new(&args.config)?;
    app.run()
}
