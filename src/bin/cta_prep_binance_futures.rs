#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    mkt_signal::cta_factor_calc::runtime::main(mkt_signal::cta_factor_calc::config::Worker::Perp)
        .await
}
