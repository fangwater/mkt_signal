use anyhow::{bail, Context, Result};
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use ipc_common::iceoryx_publisher::{
    QUERY_REQ_PAYLOAD, QUERY_RESP_PAYLOAD, QUERY_SUBSCRIBER_MAX_BUFFER_SIZE,
};
use order_common::{
    OrderExecutionStatus, QueryEngineResponse, QueryEngineResponseMessage, TimeInForce,
};
use runtime_common::ipc_service_name::build_service_name;
use runtime_common::time_util::get_timestamp_us;
use runtime_common::{
    exchange::Exchange,
    execution_backend::{rapidx_portfolio_id, ExecBackend},
};
use serde::Deserialize;
use std::time::{Duration, Instant};
use trade_engine::query_parsers::compact_order::{
    CompactOrderQueryResp, ORDER_QUERY_NOT_FOUND_MARKER,
};
use trade_engine::query_request::{GenericQueryRequest, QueryRequestType};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Business {
    Spot,
    Margin,
    Perp,
}

#[derive(Debug, Parser)]
#[command(about = "Query one Binance RapidX order through the trade-engine IPC path")]
struct Args {
    #[arg(long, value_enum)]
    business: Business,
    #[arg(long)]
    symbol: String,
    #[arg(long)]
    client_order_id: i64,
    #[arg(long)]
    client_query_id: Option<i64>,
    #[arg(long, default_value_t = 15)]
    timeout_secs: u64,
}

#[derive(Debug, Deserialize)]
struct ErrorEnvelope {
    code: Option<i32>,
    #[serde(default)]
    message: String,
    #[serde(default)]
    msg: String,
}

fn query_type(business: Business) -> QueryRequestType {
    match business {
        Business::Spot | Business::Margin => QueryRequestType::BinanceWsMarginQuery,
        Business::Perp => QueryRequestType::BinanceWsUMQuery,
    }
}

fn validate(args: &Args) -> Result<()> {
    if args.client_order_id <= 0 {
        bail!("--client-order-id must be positive");
    }
    if args.client_query_id.is_some_and(|value| value <= 0) {
        bail!("--client-query-id must be positive");
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
    Ok(())
}

fn build_request(args: &Args, client_query_id: i64) -> GenericQueryRequest {
    let params = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("symbol", &args.symbol)
        .append_pair("origClientOrderId", &args.client_order_id.to_string())
        .finish();
    GenericQueryRequest::create(
        query_type(args.business),
        get_timestamp_us(),
        client_query_id,
        Bytes::from(params),
    )
}

fn trimmed_body(body: &[u8]) -> &[u8] {
    let len = body
        .iter()
        .rposition(|byte| *byte != 0)
        .map_or(0, |index| index + 1);
    &body[..len]
}

fn handle_response(response: &QueryEngineResponseMessage) -> Result<()> {
    let body = response.body_bytes();
    let trimmed = trimmed_body(body);
    if trimmed == ORDER_QUERY_NOT_FOUND_MARKER {
        bail!("RapidX order query returned 401018: Order not found");
    }
    if trimmed.first() == Some(&b'{') {
        let error: ErrorEnvelope =
            serde_json::from_slice(trimmed).context("decode RapidX order-query error envelope")?;
        let code = error.code.unwrap_or_default();
        let server_message = if error.message.is_empty() {
            error.msg.as_str()
        } else {
            error.message.as_str()
        };
        let stable = order_common::trade_error_code::rapidx::describe_error_code(code)
            .unwrap_or("unknown RapidX error");
        bail!(
            "RapidX order query failed: code={code} description={stable} message={server_message}"
        );
    }
    if let Ok(compact) = CompactOrderQueryResp::from_bytes_prefix(body) {
        if compact.order_id > 0
            && compact.executed_qty.is_finite()
            && compact.executed_qty >= 0.0
            && compact.response_price.is_finite()
            && OrderExecutionStatus::from_u8(compact.status_u8).is_some()
            && TimeInForce::from_u8(compact.time_in_force_u8).is_some()
        {
            let status = OrderExecutionStatus::from_u8(compact.status_u8)
                .map(|value| value.as_str())
                .unwrap_or("UNKNOWN");
            let time_in_force = TimeInForce::from_u8(compact.time_in_force_u8)
                .map(|value| value.as_str())
                .unwrap_or("UNKNOWN");
            println!(
                "[response] transport=rest client_query_id={} order_id={} status={} executed_qty={} response_price={} update_time_ms={} time_in_force={}",
                response.client_query_id(),
                compact.order_id,
                status,
                compact.executed_qty,
                compact.response_price,
                compact.update_time_ms,
                time_in_force
            );
            return Ok(());
        }
    }

    let text = String::from_utf8_lossy(trimmed);
    bail!("RapidX order query returned an unrecognized body: {text}")
}

fn main() -> Result<()> {
    let args = Args::parse();
    validate(&args)?;
    if ExecBackend::for_exchange(Exchange::Binance)? != ExecBackend::Ltp {
        bail!("rapidx_query_smoke requires Binance execution backend ltp");
    }
    rapidx_portfolio_id()?;

    let client_query_id = args.client_query_id.unwrap_or_else(get_timestamp_us);
    let request = build_request(&args, client_query_id).to_bytes();
    if request.len() > QUERY_REQ_PAYLOAD {
        bail!("query request exceeds IPC payload");
    }
    println!(
        "[plan] exchange=binance business={:?} symbol={} client_order_id={} client_query_id={} req_type={} transport=rest read_only=true",
        args.business,
        args.symbol,
        args.client_order_id,
        client_query_id,
        query_type(args.business) as u32
    );

    let node = NodeBuilder::new()
        .name(&NodeName::new("rapidx_query_smoke")?)
        .create::<ipc::Service>()?;
    let request_service = node
        .service_builder(&ServiceName::new(&build_service_name(
            "query_reqs/binance",
        ))?)
        .publish_subscribe::<[u8; QUERY_REQ_PAYLOAD]>()
        .subscriber_max_buffer_size(QUERY_SUBSCRIBER_MAX_BUFFER_SIZE)
        .open_or_create()?;
    let response_service = node
        .service_builder(&ServiceName::new(&build_service_name(
            "query_resps/binance",
        ))?)
        .publish_subscribe::<[u8; QUERY_RESP_PAYLOAD]>()
        .subscriber_max_buffer_size(QUERY_SUBSCRIBER_MAX_BUFFER_SIZE)
        .open_or_create()?;
    let publisher = request_service.publisher_builder().create()?;
    let subscriber = response_service.subscriber_builder().create()?;

    std::thread::sleep(Duration::from_millis(250));
    let mut payload = [0u8; QUERY_REQ_PAYLOAD];
    payload[..request.len()].copy_from_slice(&request);
    publisher
        .loan_uninit()?
        .write_payload(payload)
        .send()
        .context("publish RapidX query request")?;
    println!("[sent] read-only query published to trade engine");

    let deadline = Instant::now() + Duration::from_secs(args.timeout_secs);
    while Instant::now() < deadline {
        if let Some(sample) = subscriber.receive()? {
            let response = QueryEngineResponseMessage::from_payload(sample.payload())
                .context("decode query response")?;
            if response.client_query_id() != client_query_id {
                continue;
            }
            if response.req_type() != query_type(args.business) as u32 {
                bail!("query response type does not match request");
            }
            return handle_response(&response);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    bail!("timed out waiting for RapidX order query response")
}

#[cfg(test)]
mod tests {
    use super::*;
    use trade_engine::query_request::QueryRequestMsg;

    fn args(business: Business) -> Args {
        Args {
            business,
            symbol: "XRPUSDT".into(),
            client_order_id: 123,
            client_query_id: Some(456),
            timeout_secs: 15,
        }
    }

    #[test]
    fn routes_cash_and_perp_through_existing_ws_query_types() {
        assert_eq!(
            query_type(Business::Spot),
            QueryRequestType::BinanceWsMarginQuery
        );
        assert_eq!(
            query_type(Business::Margin),
            QueryRequestType::BinanceWsMarginQuery
        );
        assert_eq!(
            query_type(Business::Perp),
            QueryRequestType::BinanceWsUMQuery
        );
    }

    #[test]
    fn request_keeps_lookup_and_correlation_ids_distinct() {
        let args = args(Business::Perp);
        let request = build_request(&args, 456).to_bytes();
        let parsed = QueryRequestMsg::parse(&request).unwrap();
        assert_eq!(parsed.client_query_id, 456);
        assert_eq!(parsed.req_type, QueryRequestType::BinanceWsUMQuery);
        assert_eq!(parsed.params, "symbol=XRPUSDT&origClientOrderId=123");
    }

    #[test]
    fn validation_rejects_bad_identifiers_and_symbols() {
        let mut value = args(Business::Spot);
        assert!(validate(&value).is_ok());
        value.client_order_id = 0;
        assert!(validate(&value).is_err());
        value.client_order_id = 123;
        value.symbol = "xrp-usdt".into();
        assert!(validate(&value).is_err());
    }
}
