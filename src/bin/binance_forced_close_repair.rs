use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use mkt_signal::pre_trade::PersistChannel;
use order_common::{OrderStatus, OrderType, Side, TradingVenue};
use persist_common::UnifiedOrderRecord;
use runtime_common::time_util::get_timestamp_us;
use std::path::Path;

#[derive(Debug, Parser)]
#[command(
    name = "binance_forced_close_repair",
    about = "Publish one verified Binance forced-close fill to persist_manager; dry-run by default"
)]
struct Args {
    #[arg(long)]
    source_id: String,

    #[arg(long)]
    symbol: String,

    #[arg(long)]
    event_ts_us: i64,

    #[arg(long)]
    order_id: u64,

    #[arg(long)]
    trade_id: u64,

    #[arg(long, value_enum)]
    side: SideArg,

    #[arg(long)]
    price: f64,

    #[arg(long)]
    qty: f64,

    #[arg(long, value_enum)]
    reason: ForcedCloseReason,

    #[arg(long, default_value_t = false)]
    execute: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum SideArg {
    Buy,
    Sell,
}

impl From<SideArg> for Side {
    fn from(value: SideArg) -> Self {
        match value {
            SideArg::Buy => Self::Buy,
            SideArg::Sell => Self::Sell,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ForcedCloseReason {
    Liquidation,
    Adl,
    Settlement,
    Delivery,
}

impl ForcedCloseReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Liquidation => "liquidation",
            Self::Adl => "adl",
            Self::Settlement => "settlement",
            Self::Delivery => "delivery",
        }
    }
}

fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "warn");
    }
    env_logger::init();

    let args = Args::parse();
    let symbol = normalized_symbol(&args.symbol)?;
    validate_args(&args)?;

    let from_key = format!(
        "exchange_forced_close:{}:order={}:trade={}",
        args.reason.as_str(),
        args.order_id,
        args.trade_id
    );
    let client_order_id = synthetic_client_order_id(args.order_id)?;
    let now_us = get_timestamp_us();
    let mut record = UnifiedOrderRecord {
        symbol_len: 0,
        symbol: symbol.as_bytes().to_vec(),
        create_ts: args.event_ts_us,
        update_ts: args.event_ts_us,
        signal_ts: 0,
        submit_ts: 0,
        local_ts: now_us,
        mkt_ts: 0,
        client_order_id,
        venue: TradingVenue::BinanceFutures.to_u8(),
        ttype: OrderType::Market.to_u8(),
        side: Side::from(args.side).to_u8(),
        price: args.price,
        price_offset: 0.0,
        amount_init: args.qty,
        amount_update: args.qty,
        status: OrderStatus::Filled.to_u8(),
        from_key_len: 0,
        from_key: from_key.as_bytes().to_vec(),
        signal_bbo: None,
    };
    record.refresh_lengths();

    print_summary(&args, &symbol, &from_key, client_order_id);
    if !args.execute {
        println!("mode=dry-run published=false");
        return Ok(());
    }

    validate_runtime_scope(&args.source_id)?;
    PersistChannel::with(|channel| {
        if !channel.is_uniform_order_publisher_available() {
            bail!("uniform order publisher is unavailable");
        }
        channel
            .try_publish_uniform_order(&record)
            .map_err(anyhow::Error::msg)
    })?;
    println!("mode=execute published=true");
    Ok(())
}

fn validate_args(args: &Args) -> Result<()> {
    validate_source_id(&args.source_id)?;
    if args.event_ts_us <= 0 {
        bail!("event-ts-us must be positive");
    }
    if args.order_id == 0 || args.trade_id == 0 {
        bail!("order-id and trade-id must be positive");
    }
    if !args.price.is_finite() || args.price <= 0.0 {
        bail!("price must be finite and positive");
    }
    if !args.qty.is_finite() || args.qty <= 0.0 {
        bail!("qty must be finite and positive");
    }
    Ok(())
}

fn validate_source_id(source_id: &str) -> Result<()> {
    let suffix = source_id
        .strip_prefix("binance_exec_trade")
        .context("source-id must match binance_exec_tradeNN")?;
    if suffix.len() != 2 || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("source-id must match binance_exec_tradeNN");
    }
    Ok(())
}

fn normalized_symbol(raw: &str) -> Result<String> {
    let symbol = raw.trim().to_ascii_uppercase();
    if symbol.is_empty()
        || symbol.len() > 32
        || !symbol
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        bail!("symbol must contain only ASCII letters and digits");
    }
    Ok(symbol)
}

fn synthetic_client_order_id(order_id: u64) -> Result<i64> {
    let order_id = i64::try_from(order_id).context("order-id exceeds i64 range")?;
    order_id
        .checked_neg()
        .context("order-id cannot be represented as a synthetic client order id")
}

fn validate_runtime_scope(source_id: &str) -> Result<()> {
    let namespace = std::env::var("IPC_NAMESPACE").context("IPC_NAMESPACE is not set")?;
    if namespace != source_id {
        bail!("IPC_NAMESPACE {namespace:?} does not match source-id {source_id:?}");
    }

    let current_dir = std::env::current_dir().context("read current directory")?;
    let runtime_name = current_dir
        .file_name()
        .and_then(|name| name.to_str())
        .context("current directory has no UTF-8 basename")?;
    if runtime_name != source_id {
        bail!(
            "current runtime directory {} does not match source-id {source_id:?}",
            Path::new(&current_dir).display()
        );
    }
    Ok(())
}

fn print_summary(args: &Args, symbol: &str, from_key: &str, client_order_id: i64) {
    println!("source_id={}", args.source_id);
    println!("venue=binance-futures");
    println!("symbol={symbol}");
    println!("event_ts_us={}", args.event_ts_us);
    println!("order_id={}", args.order_id);
    println!("trade_id={}", args.trade_id);
    println!("client_order_id={client_order_id}");
    println!("side={}", Side::from(args.side).as_str());
    println!("price={}", args.price);
    println!("qty={}", args.qty);
    println!("status=FILLED");
    println!("from_key={from_key}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_symbol_and_builds_synthetic_id() {
        assert_eq!(normalized_symbol(" storjusdt ").unwrap(), "STORJUSDT");
        assert_eq!(
            synthetic_client_order_id(10_308_963_858).unwrap(),
            -10_308_963_858
        );
    }

    #[test]
    fn rejects_invalid_scope_and_symbol() {
        assert!(validate_source_id("trade03").is_err());
        assert!(validate_source_id("binance_exec_trade3").is_err());
        assert!(normalized_symbol("STORJ-USDT").is_err());
    }
}
