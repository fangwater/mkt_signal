use anyhow::Result;
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use log::info;
use mkt_signal::common::time_util::get_timestamp_us;
use mkt_signal::pre_trade::QueryEngHub;
use mkt_signal::trade_engine::query_request::{GenericQueryRequest, QueryRequestType};

#[derive(Parser, Debug)]
#[command(name = "query_client")]
#[command(about = "Send query requests and print decoded query responses")]
struct Args {
    /// Exchange (currently binance only for snapshot demo)
    #[arg(long, default_value = "binance")]
    exchange: String,

    /// Query kind
    #[arg(long, value_enum)]
    kind: QueryKind,

    /// Client query id for correlation (default: now_us)
    #[arg(long)]
    client_query_id: Option<i64>,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum QueryKind {
    BinancePmBalanceSnapshot,
    BinanceUmAccountSnapshot,
}

impl QueryKind {
    fn req_type(&self) -> QueryRequestType {
        match self {
            QueryKind::BinancePmBalanceSnapshot => QueryRequestType::BinancePmBalanceSnapshot,
            QueryKind::BinanceUmAccountSnapshot => QueryRequestType::BinanceUmAccountSnapshot,
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    let args = Args::parse();
    let exchange = args.exchange.trim().to_ascii_lowercase();

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            QueryEngHub::initialize([exchange.as_str()])?;

            let client_query_id = args.client_query_id.unwrap_or_else(get_timestamp_us);
            let req = GenericQueryRequest::create(
                args.kind.req_type(),
                get_timestamp_us(),
                client_query_id,
                Bytes::new(),
            );

            info!(
                "sending query: exchange={} type={:?} client_query_id={}",
                exchange,
                args.kind.req_type(),
                client_query_id
            );
            QueryEngHub::publish_query_request(exchange.as_str(), &req.to_bytes())?;

            // Responses are printed by QueryEngHub listener (debug logs).
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            Ok(())
        })
        .await
}

