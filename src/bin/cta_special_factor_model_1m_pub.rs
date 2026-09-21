use anyhow::Result;
use clap::Parser;
use mkt_signal::factor_pub::cta_special_factor_model_1m_pub::app::CtaSpecialFactorModel1mPubApp;
use order_common::TradingVenue;
use runtime_common::affinity::maybe_pin_current_thread;

#[derive(Debug, Parser)]
#[command(name = "cta_special_factor_model_1m_pub")]
struct Args {
    #[arg(long, default_value = "binance-futures")]
    venue: TradingVenue,
    #[arg(long, default_value = "config/cta_special_factor_model_1m_pub.toml")]
    config: String,
    #[arg(long)]
    core: Option<usize>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    maybe_pin_current_thread(args.core, "CTA_SPECIAL_FACTOR_MODEL_1M_CORE")?;
    CtaSpecialFactorModel1mPubApp::new(&args.config, args.venue)
        .await?
        .run()
        .await
}
