use anyhow::{anyhow, Result};
use clap::Parser;
use iceoryx2::port::client::Client;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::info;
use std::thread;
use std::time::Duration;

use mkt_signal::common::time_util::get_timestamp_us;
use mkt_signal::depth_pub::query_msg::{
    resp_status_name, DepthQueryHeader, DepthQueryLoadTlenBatchReq, DepthQueryLoadTlenBatchResp,
    DepthQueryLoadTlenSingleReq, DepthQueryLoadTlenSingleResp, DepthQueryType, DEPTH_QUERY_PAYLOAD,
    RESP_STATUS_OK,
};
use mkt_signal::signal::common::TradingVenue;

const DEPTH_QUERY_SERVICE_PREFIX: &str = "depth_queries";
const RESPONSE_WAIT_RETRY: usize = 50;
const RESPONSE_WAIT_MS: u64 = 2;

type DepthQueryClient = Client<ipc::Service, [u8], (), [u8], ()>;

#[derive(Parser, Debug)]
#[command(name = "demo_depth_query_tlen")]
#[command(about = "Depth query demo: request load_tlen(single/batch) from depth_pub")]
struct Args {
    /// Venue, e.g. binance-futures
    #[arg(long)]
    venue: TradingVenue,

    /// Symbol for query, e.g. BTCUSDT
    #[arg(long)]
    symbol: String,

    /// Single-price request
    #[arg(long)]
    price: Option<f64>,

    /// Batch request prices in CSV, e.g. "101.0,101.5,102.0"
    #[arg(long)]
    prices: Option<String>,
}

fn create_depth_query_client(venue: TradingVenue) -> Result<DepthQueryClient> {
    let node_name = format!("demo_depth_query_tlen_{}", venue.data_pub_slug());
    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;

    let service_name = format!("{}/{}", DEPTH_QUERY_SERVICE_PREFIX, venue.data_pub_slug());
    let service = node
        .service_builder(&ServiceName::new(&service_name)?)
        .request_response::<[u8], [u8]>()
        .open()?;

    let client = service
        .client_builder()
        .initial_max_slice_len(DEPTH_QUERY_PAYLOAD)
        .create()?;

    info!("depth query client ready: {}", service_name);
    Ok(client)
}

fn parse_csv_prices(raw: &str) -> Result<Vec<f64>> {
    let values: Vec<f64> = raw
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(|token| {
            token
                .parse::<f64>()
                .map_err(|err| anyhow!("invalid price '{token}': {err}"))
        })
        .collect::<Result<Vec<_>>>()?;
    if values.is_empty() {
        return Err(anyhow!("prices is empty"));
    }
    Ok(values)
}

fn send_depth_query(
    client: &DepthQueryClient,
    req: &[u8],
    expected_type: DepthQueryType,
) -> Result<Vec<u8>> {
    let pending = client
        .loan_slice_uninit(req.len())
        .map_err(|err| anyhow!("loan depth query request failed: {err:?}"))?
        .write_from_slice(req)
        .send()
        .map_err(|err| anyhow!("send depth query request failed: {err:?}"))?;

    for _ in 0..RESPONSE_WAIT_RETRY {
        let maybe_response = pending
            .receive()
            .map_err(|err| anyhow!("receive depth query response failed: {err:?}"))?;
        if let Some(response) = maybe_response {
            let payload = response.payload();
            let header =
                DepthQueryHeader::parse(payload).map_err(|err| anyhow!(err.to_string()))?;
            if header.query_type != expected_type as u8 {
                return Err(anyhow!(
                    "unexpected response query_type={}, expected={}",
                    header.query_type,
                    expected_type as u8
                ));
            }

            let resp_payload = &payload[header.payload_offset..];
            if resp_payload.is_empty() {
                return Err(anyhow!("response payload is empty"));
            }
            let status = resp_payload[0];
            if status != RESP_STATUS_OK {
                return Err(anyhow!(
                    "depth query failed: status={}({})",
                    status,
                    resp_status_name(status)
                ));
            }
            return Ok(resp_payload[1..].to_vec());
        }

        thread::sleep(Duration::from_millis(RESPONSE_WAIT_MS));
    }

    Err(anyhow!("depth query response timeout"))
}

fn query_single(client: &DepthQueryClient, symbol: &str, price: f64) -> Result<f64> {
    let mut req_buf = [0u8; DEPTH_QUERY_PAYLOAD];
    let header_len =
        DepthQueryHeader::write(&mut req_buf, DepthQueryType::LoadTlenSingle as u8, symbol)
            .map_err(|err| anyhow!(err.to_string()))?;
    let req = DepthQueryLoadTlenSingleReq {
        timestamp_us: get_timestamp_us(),
        price,
    };
    let payload_len = req
        .write_to(&mut req_buf[header_len..])
        .map_err(|err| anyhow!(err.to_string()))?;
    let req_len = header_len + payload_len;

    let body = send_depth_query(client, &req_buf[..req_len], DepthQueryType::LoadTlenSingle)?;
    let resp = DepthQueryLoadTlenSingleResp::from_payload(&body)
        .map_err(|err| anyhow!(err.to_string()))?;
    Ok(resp.amount)
}

fn query_batch(client: &DepthQueryClient, symbol: &str, prices: &[f64]) -> Result<Vec<f64>> {
    let mut req_buf = [0u8; DEPTH_QUERY_PAYLOAD];
    let header_len =
        DepthQueryHeader::write(&mut req_buf, DepthQueryType::LoadTlenBatch as u8, symbol)
            .map_err(|err| anyhow!(err.to_string()))?;
    let payload_len = DepthQueryLoadTlenBatchReq::write_to(
        &mut req_buf[header_len..],
        get_timestamp_us(),
        prices,
    )
    .map_err(|err| anyhow!(err.to_string()))?;
    let req_len = header_len + payload_len;

    let body = send_depth_query(client, &req_buf[..req_len], DepthQueryType::LoadTlenBatch)?;
    let resp =
        DepthQueryLoadTlenBatchResp::from_payload(&body).map_err(|err| anyhow!(err.to_string()))?;
    Ok(resp.amounts)
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    if args.price.is_none() && args.prices.is_none() {
        return Err(anyhow!(
            "at least one of --price or --prices must be provided"
        ));
    }

    let symbol = args.symbol.trim().to_uppercase();
    if symbol.is_empty() {
        return Err(anyhow!("symbol must not be empty"));
    }

    let batch_prices = match args.prices.as_deref() {
        Some(raw) => Some(parse_csv_prices(raw)?),
        None => None,
    };

    let client = create_depth_query_client(args.venue)?;

    if let Some(price) = args.price {
        let amount = query_single(&client, &symbol, price)?;
        println!(
            "TLEN_SINGLE venue={} symbol={} price={} amount={}",
            args.venue.data_pub_slug(),
            symbol,
            price,
            amount
        );
    }

    if let Some(prices) = batch_prices {
        let amounts = query_batch(&client, &symbol, &prices)?;
        println!(
            "TLEN_BATCH venue={} symbol={} count={}",
            args.venue.data_pub_slug(),
            symbol,
            prices.len()
        );
        for (price, amount) in prices.iter().zip(amounts.iter()) {
            println!("  price={} amount={}", price, amount);
        }
    }

    Ok(())
}
