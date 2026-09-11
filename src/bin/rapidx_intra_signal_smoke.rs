use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use ipc_common::iceoryx_publisher::TradeSignalPublisher;
use order_common::{OrderType, Side, TradingVenue};
use runtime_common::execution_backend::{
    rapidx_binance_cash_business_type, rapidx_portfolio_id, ExecBackend, RapidXCashBusinessType,
};
use runtime_common::{exchange::Exchange, time_util::get_timestamp_us};
use signal_common::common::{SignalBytes, TradingLeg};
use signal_common::open_signal::ArbOpenCtx;
use signal_common::trade_signal::SignalType;
use std::time::Duration;

const ABSOLUTE_MAX_NOTIONAL_USDT: f64 = 100.0;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    fn side(self) -> Side {
        match self {
            Self::Buy => Side::Buy,
            Self::Sell => Side::Sell,
        }
    }
}

#[derive(Debug, Parser)]
#[command(about = "Publish one bounded Binance MARGIN/PERP ArbOpen smoke-test signal")]
struct Args {
    #[arg(long)]
    symbol: String,
    #[arg(long, value_enum)]
    side: OrderSide,
    #[arg(long)]
    quantity: f64,
    #[arg(long)]
    price: f64,
    #[arg(long)]
    price_tick: f64,
    #[arg(long)]
    quantity_tick: f64,
    #[arg(long)]
    open_bid: f64,
    #[arg(long)]
    open_bid_qty: f64,
    #[arg(long)]
    open_ask: f64,
    #[arg(long)]
    open_ask_qty: f64,
    #[arg(long)]
    hedge_bid: f64,
    #[arg(long)]
    hedge_bid_qty: f64,
    #[arg(long)]
    hedge_ask: f64,
    #[arg(long)]
    hedge_ask_qty: f64,
    #[arg(long, default_value_t = 60)]
    ttl_secs: u64,
    #[arg(long, default_value_t = ABSOLUTE_MAX_NOTIONAL_USDT)]
    max_notional_usdt: f64,
    #[arg(long)]
    from_key: Option<String>,
    #[arg(long)]
    execute: bool,
}

fn validate(args: &Args) -> Result<()> {
    if args.symbol.is_empty()
        || args.symbol.len() > 32
        || !args
            .symbol
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        bail!("--symbol must be 1..32 uppercase ASCII letters or digits");
    }
    for (name, value) in [
        ("quantity", args.quantity),
        ("price", args.price),
        ("price-tick", args.price_tick),
        ("quantity-tick", args.quantity_tick),
        ("open-bid", args.open_bid),
        ("open-bid-qty", args.open_bid_qty),
        ("open-ask", args.open_ask),
        ("open-ask-qty", args.open_ask_qty),
        ("hedge-bid", args.hedge_bid),
        ("hedge-bid-qty", args.hedge_bid_qty),
        ("hedge-ask", args.hedge_ask),
        ("hedge-ask-qty", args.hedge_ask_qty),
    ] {
        if !value.is_finite() || value <= 0.0 {
            bail!("--{name} must be positive and finite");
        }
    }
    if args.open_bid >= args.open_ask || args.hedge_bid >= args.hedge_ask {
        bail!("each BBO must have bid < ask");
    }
    match args.side {
        OrderSide::Buy if args.price > args.open_bid => {
            bail!("BUY maker price must be at or below the opening best bid")
        }
        OrderSide::Sell if args.price < args.open_ask => {
            bail!("SELL maker price must be at or above the opening best ask")
        }
        _ => {}
    }
    if args.ttl_secs == 0 || args.ttl_secs > 300 {
        bail!("--ttl-secs must be in 1..=300");
    }
    if !args.max_notional_usdt.is_finite()
        || args.max_notional_usdt <= 0.0
        || args.max_notional_usdt > ABSOLUTE_MAX_NOTIONAL_USDT
    {
        bail!("--max-notional-usdt must be in (0, 100]");
    }
    if args.quantity * args.price > args.max_notional_usdt + 1e-9 {
        bail!(
            "order notional {:.8} exceeds configured cap {:.8}",
            args.quantity * args.price,
            args.max_notional_usdt
        );
    }
    if let Some(value) = args.from_key.as_deref() {
        if value.is_empty()
            || value.len() > 128
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'|')
            })
        {
            bail!("--from-key must be 1..128 safe ASCII characters");
        }
    }
    Ok(())
}

fn validate_runtime() -> Result<()> {
    if ExecBackend::for_exchange(Exchange::Binance)? != ExecBackend::Ltp {
        bail!("rapidx_intra_signal_smoke requires Binance execution backend ltp");
    }
    rapidx_portfolio_id()?;
    if rapidx_binance_cash_business_type()? != RapidXCashBusinessType::Margin {
        bail!("RAPIDX_BINANCE_CASH_BUSINESS_TYPE must be MARGIN");
    }
    for (name, expected) in [
        ("OPEN_VENUE", "binance-margin"),
        ("HEDGE_VENUE", "binance-futures"),
    ] {
        if let Ok(value) = std::env::var(name) {
            if !value.eq_ignore_ascii_case(expected) {
                bail!("{name} must be {expected}, got {value}");
            }
        }
    }
    Ok(())
}

fn build_context(args: &Args, now_us: i64, from_key: String) -> Result<ArbOpenCtx> {
    let mut context = ArbOpenCtx::new();
    context.opening_leg = TradingLeg::new_with_qty(
        TradingVenue::BinanceMargin,
        args.open_bid,
        args.open_bid_qty,
        args.open_ask,
        args.open_ask_qty,
        now_us,
    );
    context.hedging_leg = TradingLeg::new_with_qty(
        TradingVenue::BinanceFutures,
        args.hedge_bid,
        args.hedge_bid_qty,
        args.hedge_ask,
        args.hedge_ask_qty,
        now_us,
    );
    context.set_opening_symbol(&args.symbol);
    context.set_hedging_symbol(&args.symbol);
    context.set_side(args.side.side());
    context.set_order_type(OrderType::Limit);
    context.set_price_with_tick_floor(args.price, args.price_tick);
    context.set_amount_with_tick_floor(args.quantity, args.quantity_tick);
    if (context.price_value() - args.price).abs() > args.price_tick * 1e-6 {
        bail!("--price must already be aligned to --price-tick");
    }
    if context.amount_value() <= 0.0 {
        bail!("quantized quantity is zero");
    }
    if context.amount_value() * context.price_value() > args.max_notional_usdt + 1e-9 {
        bail!("quantized order exceeds configured notional cap");
    }
    context.create_ts = now_us;
    context.exp_time = now_us.saturating_add((args.ttl_secs as i64).saturating_mul(1_000_000));
    context.price_offset = 0.0;
    context.spread_rate = 0.0;
    context.hedge_timeout_us = 0;
    context.set_from_key(from_key.into_bytes());
    Ok(context)
}

fn main() -> Result<()> {
    let args = Args::parse();
    validate(&args)?;
    validate_runtime()?;
    let from_key = args
        .from_key
        .clone()
        .unwrap_or_else(|| format!("rapidx_intra_signal_smoke|{}", get_timestamp_us()));
    let preview = build_context(&args, get_timestamp_us(), from_key.clone())?;
    println!(
        "[plan] exchange=binance opening=MARGIN hedge=PERP symbol={} side={} order_type=LIMIT maker_only=true quantity={} price={} notional_usdt={:.8} ttl_secs={} max_notional_usdt={} from_key={} execute={}",
        args.symbol,
        args.side.side().as_str(),
        preview.amount_value(),
        preview.price_value(),
        preview.amount_value() * preview.price_value(),
        args.ttl_secs,
        args.max_notional_usdt,
        from_key,
        args.execute
    );
    if !args.execute {
        println!("[plan] dry-run only; add --execute to publish this one strategy signal");
        return Ok(());
    }

    let publisher = TradeSignalPublisher::open("trade_signal")
        .context("open the existing trade_signal IPC service")?;
    std::thread::sleep(Duration::from_millis(250));
    let generation_time = get_timestamp_us();
    let context = build_context(&args, generation_time, from_key)?;
    let bytes = context.to_bytes();
    publisher.publish_trade_signal_parts(SignalType::ArbOpen, generation_time, 0.0, &bytes)?;
    println!("[sent] one ArbOpen signal published to pre_trade");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> Args {
        Args {
            symbol: "COTIUSDT".into(),
            side: OrderSide::Sell,
            quantity: 4_000.0,
            price: 0.02,
            price_tick: 0.000001,
            quantity_tick: 1.0,
            open_bid: 0.019999,
            open_bid_qty: 1_000.0,
            open_ask: 0.02,
            open_ask_qty: 2_000.0,
            hedge_bid: 0.01998,
            hedge_bid_qty: 2_000.0,
            hedge_ask: 0.01999,
            hedge_ask_qty: 2_000.0,
            ttl_secs: 60,
            max_notional_usdt: 100.0,
            from_key: Some("smoke|1".into()),
            execute: false,
        }
    }

    #[test]
    fn enforces_cap_and_post_only_side() {
        let mut value = args();
        assert!(validate(&value).is_ok());
        value.quantity = 5_001.0;
        assert!(validate(&value).is_err());
        value = args();
        value.price = value.open_bid;
        assert!(validate(&value).is_err());
    }

    #[test]
    fn builds_quantized_margin_to_perp_context() {
        let value = args();
        let context = build_context(&value, 123, "smoke|1".into()).unwrap();
        assert_eq!(context.get_opening_symbol(), "COTIUSDT");
        assert_eq!(
            context.opening_leg.get_venue(),
            Some(TradingVenue::BinanceMargin)
        );
        assert_eq!(
            context.hedging_leg.get_venue(),
            Some(TradingVenue::BinanceFutures)
        );
        assert_eq!(context.amount_value(), 4_000.0);
        assert_eq!(context.price_value(), 0.02);
        assert_eq!(context.exp_time, 60_000_123);
    }
}
