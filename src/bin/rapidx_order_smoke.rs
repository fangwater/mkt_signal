use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use order_common::trade_error_code::rapidx::describe_error_code;
use order_common::{OrderStatus, OrderType, Side, TradeRequestType};
use runtime_common::ipc_service_name::build_service_name;
use runtime_common::time_util::get_timestamp_us;
use runtime_common::{
    exchange::Exchange,
    execution_backend::{rapidx_portfolio_id, ExecBackend},
};
use signal_common::tick_math::QuantizedValue;
use std::time::{Duration, Instant};
use trade_engine::trade_request::{
    BinanceCancelOrderParams, BinanceNewOrderParams, TradeRequestIpcPayload,
};

const RESPONSE_BYTES: usize = 64;
const DEFAULT_MAX_NOTIONAL_USDT: f64 = 10.0;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Business {
    Spot,
    Margin,
    Perp,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Action {
    Place,
    Cancel,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Kind {
    Limit,
    Market,
}

#[derive(Debug, Parser)]
#[command(about = "Send one bounded Binance RapidX order through the trade-engine IPC path")]
struct Args {
    #[arg(long, value_enum)]
    action: Action,
    #[arg(long, value_enum)]
    business: Business,
    #[arg(long)]
    symbol: String,
    #[arg(long)]
    client_order_id: i64,
    #[arg(long, value_enum, default_value = "buy")]
    side: OrderSide,
    #[arg(long, value_enum, default_value = "limit")]
    order_type: Kind,
    #[arg(long, default_value_t = 0.0)]
    quantity: f64,
    #[arg(long, default_value_t = 0.0)]
    quote_quantity: f64,
    #[arg(long, default_value_t = 0.0)]
    price: f64,
    #[arg(long, default_value_t = 0.0)]
    risk_price: f64,
    #[arg(long)]
    post_only: bool,
    #[arg(long)]
    reduce_only: bool,
    #[arg(long, default_value_t = DEFAULT_MAX_NOTIONAL_USDT)]
    max_notional_usdt: f64,
    #[arg(long, default_value_t = 15)]
    timeout_secs: u64,
    #[arg(long)]
    execute: bool,
}

fn request_types(business: Business) -> (TradeRequestType, TradeRequestType) {
    match business {
        Business::Spot => (
            TradeRequestType::BinanceLtpNewSpotOrder,
            TradeRequestType::BinanceLtpCancelSpotOrder,
        ),
        Business::Margin => (
            TradeRequestType::BinanceNewMarginOrder,
            TradeRequestType::BinanceCancelMarginOrder,
        ),
        Business::Perp => (
            TradeRequestType::BinanceNewUMOrder,
            TradeRequestType::BinanceCancelUMOrder,
        ),
    }
}

fn validate(args: &Args) -> Result<()> {
    if args.client_order_id <= 0 {
        bail!("--client-order-id must be positive");
    }
    if args.symbol.is_empty()
        || !args
            .symbol
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        bail!("--symbol must contain only uppercase ASCII letters and digits");
    }
    if args.timeout_secs == 0 {
        bail!("--timeout-secs must be positive");
    }
    if !matches!(args.action, Action::Place) {
        return Ok(());
    }
    if args.reduce_only && !matches!(args.business, Business::Perp) {
        bail!("--reduce-only is valid only for PERP orders");
    }
    let cash_market_buy = matches!(args.business, Business::Spot | Business::Margin)
        && matches!(args.order_type, Kind::Market)
        && matches!(args.side, OrderSide::Buy);
    if cash_market_buy {
        if args.quantity != 0.0 {
            bail!("cash MARKET BUY forbids --quantity; use --quote-quantity");
        }
        if !args.quote_quantity.is_finite() || args.quote_quantity <= 0.0 {
            bail!("cash MARKET BUY requires a positive finite --quote-quantity");
        }
    } else {
        if !args.quantity.is_finite() || args.quantity <= 0.0 {
            bail!("--quantity must be positive and finite");
        }
        if args.quote_quantity != 0.0 {
            bail!("--quote-quantity is only valid for SPOT/MARGIN MARKET BUY");
        }
    }
    let risk_price = match args.order_type {
        Kind::Limit => {
            if !args.price.is_finite() || args.price <= 0.0 {
                bail!("LIMIT order requires a positive finite --price");
            }
            args.price
        }
        Kind::Market => {
            if args.post_only {
                bail!("MARKET order cannot be post-only");
            }
            if !cash_market_buy && (!args.risk_price.is_finite() || args.risk_price <= 0.0) {
                bail!("MARKET order requires a positive finite --risk-price");
            }
            args.risk_price
        }
    };
    if !args.max_notional_usdt.is_finite()
        || args.max_notional_usdt <= 0.0
        || args.max_notional_usdt > DEFAULT_MAX_NOTIONAL_USDT
    {
        bail!("--max-notional-usdt must be in (0, 10]");
    }
    let notional = if cash_market_buy {
        args.quote_quantity
    } else {
        args.quantity * risk_price
    };
    if !notional.is_finite() || notional > args.max_notional_usdt {
        bail!(
            "order notional {:.8} exceeds configured cap {:.8}",
            notional,
            args.max_notional_usdt
        );
    }
    Ok(())
}

fn request_bytes(args: &Args) -> Result<bytes::Bytes> {
    let (new_type, cancel_type) = request_types(args.business);
    match args.action {
        Action::Cancel => BinanceCancelOrderParams::request_bytes_from_parts(
            cancel_type,
            get_timestamp_us(),
            args.client_order_id,
            &args.symbol,
            args.client_order_id,
        )
        .context("build cancel request"),
        Action::Place => {
            let side = match args.side {
                OrderSide::Buy => Side::Buy,
                OrderSide::Sell => Side::Sell,
            };
            let order_type = match args.order_type {
                Kind::Limit => OrderType::Limit,
                Kind::Market => OrderType::Market,
            };
            let quantity = if args.quantity == 0.0 {
                QuantizedValue::zero()
            } else {
                QuantizedValue::from_decimal(args.quantity)
                    .context("quantity cannot be represented exactly")?
            };
            let quote_quantity = if args.quote_quantity == 0.0 {
                QuantizedValue::zero()
            } else {
                QuantizedValue::from_decimal(args.quote_quantity)
                    .context("quote quantity cannot be represented exactly")?
            };
            let price = match args.order_type {
                Kind::Limit => QuantizedValue::from_decimal(args.price)
                    .context("price cannot be represented exactly")?,
                Kind::Market => QuantizedValue::from_parts(0, 0, 1),
            };
            BinanceNewOrderParams::request_bytes_from_parts_with_quote_order_qty(
                new_type,
                get_timestamp_us(),
                args.client_order_id,
                &args.symbol,
                side,
                order_type,
                quantity,
                price,
                args.reduce_only,
                matches!(args.business, Business::Margin),
                false,
                false,
                args.post_only,
                quote_quantity,
            )
            .context("build place request")
        }
    }
}

fn response_client_order_id(payload: &[u8; RESPONSE_BYTES]) -> i64 {
    i64::from_le_bytes(payload[4..12].try_into().expect("fixed response slice"))
}

fn print_response(payload: &[u8; RESPONSE_BYTES]) -> (u16, i32, Option<OrderStatus>) {
    let req_type = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    let status = u16::from_le_bytes(payload[16..18].try_into().unwrap());
    let error_code = i32::from_le_bytes(payload[18..22].try_into().unwrap());
    let order_id = i64::from_le_bytes(payload[22..30].try_into().unwrap());
    let order_status = OrderStatus::from_u8(payload[30]);
    let executed_qty = f64::from_le_bytes(payload[39..47].try_into().unwrap());
    let response_price = f64::from_le_bytes(payload[47..55].try_into().unwrap());
    println!(
        "[response] req_type={} http_status={} error_code={} error_description={} order_id={} order_status={} executed_qty={} response_price={}",
        req_type,
        status,
        error_code,
        describe_error_code(error_code).unwrap_or(if error_code == 0 { "none" } else { "unknown" }),
        order_id,
        order_status.map(|value| value.as_str()).unwrap_or("NONE"),
        executed_qty,
        response_price
    );
    (status, error_code, order_status)
}

fn main() -> Result<()> {
    let args = Args::parse();
    validate(&args)?;
    if ExecBackend::for_exchange(Exchange::Binance)? != ExecBackend::Ltp {
        bail!("rapidx_order_smoke requires Binance execution backend ltp");
    }
    rapidx_portfolio_id()?;
    let (new_type, cancel_type) = request_types(args.business);
    let req_type = match args.action {
        Action::Place => new_type,
        Action::Cancel => cancel_type,
    };
    println!(
        "[plan] exchange=binance business={:?} action={:?} symbol={} client_order_id={} req_type={} execute={}",
        args.business,
        args.action,
        args.symbol,
        args.client_order_id,
        req_type as u32,
        args.execute
    );
    if !args.execute {
        println!("[plan] dry-run only; add --execute to publish this one request");
        return Ok(());
    }

    let request = request_bytes(&args)?;
    let node = NodeBuilder::new()
        .name(&NodeName::new("rapidx_order_smoke")?)
        .create::<ipc::Service>()?;
    let request_service = node
        .service_builder(&ServiceName::new(&build_service_name(
            "order_reqs/binance",
        ))?)
        .publish_subscribe::<TradeRequestIpcPayload>()
        .subscriber_max_buffer_size(256)
        .open_or_create()?;
    let response_service = node
        .service_builder(&ServiceName::new(&build_service_name(
            "order_resps/binance",
        ))?)
        .publish_subscribe::<[u8; RESPONSE_BYTES]>()
        .subscriber_max_buffer_size(256)
        .open_or_create()?;
    let publisher = request_service.publisher_builder().create()?;
    let subscriber = response_service.subscriber_builder().create()?;

    std::thread::sleep(Duration::from_millis(250));
    let mut sample = publisher.loan_uninit()?;
    TradeRequestIpcPayload::write_to_uninit_slot(sample.payload_mut(), &request)
        .context("request exceeds IPC payload")?;
    unsafe { sample.assume_init() }.send()?;
    println!("[sent] request published to trade engine");

    let deadline = Instant::now() + Duration::from_secs(args.timeout_secs);
    while Instant::now() < deadline {
        if let Some(sample) = subscriber.receive()? {
            let payload = sample.payload();
            if response_client_order_id(payload) != args.client_order_id {
                continue;
            }
            let (status, error_code, order_status) = print_response(payload);
            if status >= 400 || error_code != 0 {
                bail!("RapidX request rejected with status {status} and error code {error_code}");
            }
            if order_status.is_some() {
                return Ok(());
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    bail!("timed out waiting for a RapidX order lifecycle response")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> Args {
        Args {
            action: Action::Place,
            business: Business::Spot,
            symbol: "XRPUSDT".into(),
            client_order_id: 1,
            side: OrderSide::Buy,
            order_type: Kind::Limit,
            quantity: 10.0,
            quote_quantity: 0.0,
            price: 0.5,
            risk_price: 0.0,
            post_only: true,
            reduce_only: false,
            max_notional_usdt: 10.0,
            timeout_secs: 15,
            execute: false,
        }
    }

    #[test]
    fn enforces_notional_cap_and_market_reference_price() {
        let mut value = args();
        assert!(validate(&value).is_ok());
        value.quantity = 21.0;
        assert!(validate(&value).is_err());
        value.quantity = 10.0;
        value.business = Business::Perp;
        value.order_type = Kind::Market;
        value.post_only = false;
        assert!(validate(&value).is_err());
        value.risk_price = 0.5;
        assert!(validate(&value).is_ok());
    }

    #[test]
    fn cash_market_buy_requires_quote_quantity_only() {
        let mut value = args();
        value.order_type = Kind::Market;
        value.post_only = false;
        assert!(validate(&value).is_err());
        value.quantity = 0.0;
        value.quote_quantity = 5.5;
        assert!(validate(&value).is_ok());
    }

    #[test]
    fn reduce_only_is_limited_to_perp() {
        let mut value = args();
        value.reduce_only = true;
        assert!(validate(&value).is_err());
        value.business = Business::Perp;
        assert!(validate(&value).is_ok());
    }

    #[test]
    fn request_types_cover_spot_margin_and_perp() {
        assert_eq!(
            request_types(Business::Spot).0,
            TradeRequestType::BinanceLtpNewSpotOrder
        );
        assert_eq!(
            request_types(Business::Margin).0,
            TradeRequestType::BinanceNewMarginOrder
        );
        assert_eq!(
            request_types(Business::Perp).0,
            TradeRequestType::BinanceNewUMOrder
        );
    }
}
