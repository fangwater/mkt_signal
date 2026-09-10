use anyhow::{bail, Context, Result};
use clap::Parser;
use std::time::Duration;
use trade_engine::ltp_rest::LtpRestClient;

#[derive(Debug, Parser)]
#[command(about = "Inspect or cancel portfolio-scoped RapidX spot, margin, and perpetual orders")]
struct Args {
    #[arg(long, value_parser = ["binance", "okex"])]
    exchange: String,
    #[arg(long)]
    execute: bool,
    #[arg(long, default_value_t = 30)]
    timeout_secs: u64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let exchange = match args.exchange.as_str() {
        "binance" => "BINANCE",
        "okex" => "OKX",
        _ => unreachable!("clap validates exchange"),
    };
    if args.timeout_secs == 0 {
        bail!("--timeout-secs must be positive");
    }

    let client = LtpRestClient::from_env().context("build RapidX REST client")?;
    let spot_orders = client
        .open_order_ids(exchange, "SPOT")
        .await
        .context("query portfolio-scoped RapidX spot open orders")?;
    let margin_orders = client
        .open_order_ids(exchange, "MARGIN")
        .await
        .context("query portfolio-scoped RapidX margin open orders")?;
    let perp_orders = client
        .open_perp_order_ids(exchange)
        .await
        .context("query portfolio-scoped RapidX perpetual open orders")?;
    println!(
        "[plan] backend=ltp exchange={} spot_open_orders={} margin_open_orders={} perp_open_orders={} execute={}",
        args.exchange,
        spot_orders.len(),
        margin_orders.len(),
        perp_orders.len(),
        args.execute
    );
    if !args.execute {
        if spot_orders.is_empty() && margin_orders.is_empty() && perp_orders.is_empty() {
            println!("[plan] no RapidX spot, margin, or perpetual open orders found");
        } else {
            println!("[plan] rerun with --execute to cancel and verify this portfolio scope");
        }
        return Ok(());
    }

    for business_type in ["SPOT", "MARGIN", "PERP"] {
        client
            .cancel_open_orders(
                exchange,
                business_type,
                Duration::from_secs(args.timeout_secs),
            )
            .await
            .with_context(|| format!("cancel and verify RapidX {business_type} open orders"))?;
    }
    println!("[done] all RapidX spot, margin, and perpetual open orders confirmed empty");
    Ok(())
}
