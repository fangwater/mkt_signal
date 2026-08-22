//! 1-minute Model Publisher entry point
//!
//! Usage: cargo run --bin model_1m_pub --features model-ort -- <model_name>

use anyhow::Result;
use clap::Parser;
use log::info;
use std::env;
use std::path::PathBuf;

use mkt_signal::factor_pub::model_pub::app::ModelPubApp;
use runtime_common::affinity::maybe_pin_current_thread;

const DEFAULT_CONFIG_PATH: &str = "config/model_1m_pub.toml";

#[derive(Parser)]
#[command(name = "model_1m_pub")]
#[command(about = "1-minute model publisher using fusion_factor_1m and factor_plan_1m")]
struct Args {
    /// Model name registered in model_manager
    model_name: String,

    /// Optional directory containing warming samples like btcusdt-ylabel.txt
    #[arg(long)]
    warming_dir: Option<PathBuf>,

    /// Bind main runtime thread to a CPU core. Falls back to MODEL_1M_PUB_CORE.
    #[arg(long)]
    core: Option<usize>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    maybe_pin_current_thread(args.core, "MODEL_1M_PUB_CORE")?;
    set_onnx_env_fixed();
    info!(
        "Starting model_1m_pub: model_name={} config={} backend=ort factor_plan=factor_plan_1m warming_dir={}",
        args.model_name,
        DEFAULT_CONFIG_PATH,
        args.warming_dir
            .as_ref()
            .map(|dir| dir.display().to_string())
            .unwrap_or_else(|| "-".to_string())
    );

    let mut app = ModelPubApp::new_one_minute(
        DEFAULT_CONFIG_PATH,
        &args.model_name,
        args.warming_dir.as_deref(),
    )
    .await?;
    app.run().await
}

fn set_onnx_env_fixed() {
    env::set_var("OMP_NUM_THREADS", "1");
    env::set_var("OMP_WAIT_POLICY", "PASSIVE");
    info!("model_1m_pub ONNX env fixed: OMP_NUM_THREADS=1 OMP_WAIT_POLICY=PASSIVE");
}
