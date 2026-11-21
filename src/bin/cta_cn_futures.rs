#[tokio::main(worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    mkt_signal::cta_cn_futures::run().await
}
