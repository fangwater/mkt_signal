use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use mkt_parsers::msg::mkt_msg::{
    get_msg_type, FundingRateMsg, IndexPriceMsg, MarkPriceMsg, MktMsgType,
};
use order_common::TradingVenue;
use redis::Commands;
use runtime_common::time_util::get_timestamp_us;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

const DERIVATIVES_PAYLOAD: usize = 128;
const DERIVATIVES_MAX_SUBSCRIBERS: usize = 64;
const DERIVATIVES_HISTORY_SIZE: usize = 50;
const DERIVATIVES_SUBSCRIBER_MAX_BUFFER: usize = 8192;

#[derive(Debug, Parser)]
#[command(
    name = "intra_funding_trace",
    about = "Read-only trace for one symbol on dat_pbs/<venue>/derivatives funding messages"
)]
struct Args {
    #[arg(long, default_value = "bybit-futures")]
    venue: String,

    #[arg(long, default_value = "HOMEUSDT")]
    symbol: String,

    #[arg(long, default_value = "bybit-intra-arb01")]
    env_name: String,

    #[arg(long, default_value = "bybit-margin")]
    open_venue: String,

    #[arg(long, default_value = "bybit-futures")]
    hedge_venue: String,

    #[arg(long, default_value_t = 60)]
    window: usize,

    #[arg(long, value_enum, default_value_t = PeriodArg::Hours8)]
    period: PeriodArg,

    #[arg(long)]
    redis_url: Option<String>,

    #[arg(long, default_value_t = 10)]
    stats_secs: u64,

    #[arg(long)]
    max_target_msgs: Option<u64>,

    #[arg(long)]
    print_prices: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PeriodArg {
    #[value(name = "1h")]
    Hours1,
    #[value(name = "2h")]
    Hours2,
    #[value(name = "4h")]
    Hours4,
    #[value(name = "6h")]
    Hours6,
    #[value(name = "8h")]
    Hours8,
}

impl PeriodArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::Hours1 => "1h",
            Self::Hours2 => "2h",
            Self::Hours4 => "4h",
            Self::Hours6 => "6h",
            Self::Hours8 => "8h",
        }
    }
}

#[derive(Debug, Default, Clone)]
struct FundingThresholds {
    raw_key: String,
    raw_fields: HashMap<String, String>,
    expanded_fields: HashMap<String, f64>,
}

#[derive(Debug)]
struct RollingMean {
    values: VecDeque<f64>,
    sum: f64,
    capacity: usize,
}

impl RollingMean {
    fn new(capacity: usize) -> Self {
        Self {
            values: VecDeque::with_capacity(capacity.max(1)),
            sum: 0.0,
            capacity: capacity.max(1),
        }
    }

    fn push(&mut self, value: f64) -> f64 {
        if self.values.len() >= self.capacity {
            if let Some(old) = self.values.pop_front() {
                self.sum -= old;
            }
        }
        self.values.push_back(value);
        self.sum += value;
        self.mean()
    }

    fn mean(&self) -> f64 {
        if self.values.is_empty() {
            0.0
        } else {
            self.sum / self.values.len() as f64
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let venue = parse_venue(&args.venue)?;
    if !venue.is_futures() {
        bail!("venue={} is not a futures venue", args.venue);
    }
    let open_venue = parse_venue(&args.open_venue)?;
    let hedge_venue = parse_venue(&args.hedge_venue)?;
    let symbol = normalize_symbol_key(&args.symbol);
    let service_name = format!("dat_pbs/{}/derivatives", venue.data_pub_slug());
    let node_name = format!(
        "intra_funding_trace_{}_{}_{}",
        venue.data_pub_slug().replace('-', "_"),
        symbol.to_ascii_lowercase(),
        std::process::id()
    );

    let thresholds = load_thresholds(&args, open_venue, hedge_venue)?;
    let period = args.period.as_str();
    let fwd_threshold = thresholds
        .expanded_fields
        .get(&format!("{period}_forward_close"))
        .copied();
    let bwd_threshold = thresholds
        .expanded_fields
        .get(&format!("{period}_backward_close"))
        .copied();

    println!(
        "trace_start service={} node={} symbol={} window={} period={} threshold_key={} raw_fields={} expanded_fields={}",
        service_name,
        node_name,
        symbol,
        args.window.max(1),
        period,
        empty_dash(&thresholds.raw_key),
        thresholds.raw_fields.len(),
        thresholds.expanded_fields.len()
    );
    println!(
        "thresholds forward_close={:?} rule=current_fr_ma<threshold backward_close={:?} rule=current_fr_ma(+loan)>threshold loan_not_loaded=true",
        fwd_threshold, bwd_threshold
    );

    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()
        .with_context(|| format!("failed to create iceoryx node {node_name}"))?;
    let service = node
        .service_builder(&ServiceName::new(&service_name)?)
        .publish_subscribe::<[u8; DERIVATIVES_PAYLOAD]>()
        .max_publishers(1)
        .max_subscribers(DERIVATIVES_MAX_SUBSCRIBERS)
        .history_size(DERIVATIVES_HISTORY_SIZE)
        .subscriber_max_buffer_size(DERIVATIVES_SUBSCRIBER_MAX_BUFFER)
        .open_or_create()
        .with_context(|| format!("failed to open/create derivatives service {service_name}"))?;
    let subscriber: Subscriber<ipc::Service, [u8; DERIVATIVES_PAYLOAD], ()> =
        service.subscriber_builder().create()?;

    let mut rolling = RollingMean::new(args.window);
    let mut last_stats = Instant::now();
    let stats_interval = Duration::from_secs(args.stats_secs.max(1));
    let mut total: u64 = 0;
    let mut funding_total: u64 = 0;
    let mut mark_total: u64 = 0;
    let mut index_total: u64 = 0;
    let mut target_funding: u64 = 0;
    let mut target_mark: u64 = 0;
    let mut target_index: u64 = 0;
    let mut last_target_line = String::new();

    loop {
        match subscriber.receive() {
            Ok(Some(sample)) => {
                total += 1;
                let payload = sample.payload();
                match get_msg_type(payload) {
                    MktMsgType::FundingRate => {
                        funding_total += 1;
                        let raw_symbol = FundingRateMsg::get_symbol(payload);
                        let msg_symbol = normalize_symbol_key(raw_symbol);
                        if msg_symbol == symbol {
                            target_funding += 1;
                            let funding_rate = FundingRateMsg::get_funding_rate(payload);
                            let mean = rolling.push(funding_rate);
                            let msg_ts = FundingRateMsg::get_timestamp(payload);
                            let next_ts = FundingRateMsg::get_next_funding_time(payload);
                            let now_us = get_timestamp_us();
                            let age_us = if msg_ts > 0 { now_us - msg_ts } else { 0 };
                            let fwd_hit = fwd_threshold
                                .map(|threshold| mean < threshold)
                                .unwrap_or(false);
                            let bwd_hit_without_loan = bwd_threshold
                                .map(|threshold| mean > threshold)
                                .unwrap_or(false);
                            last_target_line = format!(
                                "funding symbol={} raw_symbol={} rate={:.10} fr_ma={:.10} samples={} msg_ts={} age_us={} next_funding_time={} fwd_thr={:?} fwd_hit={} bwd_thr={:?} bwd_hit_without_loan={}",
                                symbol,
                                raw_symbol,
                                funding_rate,
                                mean,
                                rolling.len(),
                                msg_ts,
                                age_us,
                                next_ts,
                                fwd_threshold,
                                fwd_hit,
                                bwd_threshold,
                                bwd_hit_without_loan
                            );
                            println!("{}", last_target_line);
                            if let Some(max) = args.max_target_msgs {
                                if target_funding >= max {
                                    break;
                                }
                            }
                        }
                    }
                    MktMsgType::MarkPrice => {
                        mark_total += 1;
                        let msg_symbol = normalize_symbol_key(MarkPriceMsg::get_symbol(payload));
                        if msg_symbol == symbol {
                            target_mark += 1;
                            if args.print_prices {
                                println!(
                                    "mark symbol={} price={:.10} msg_ts={}",
                                    symbol,
                                    MarkPriceMsg::get_mark_price(payload),
                                    MarkPriceMsg::get_timestamp(payload)
                                );
                            }
                        }
                    }
                    MktMsgType::IndexPrice => {
                        index_total += 1;
                        let msg_symbol = normalize_symbol_key(IndexPriceMsg::get_symbol(payload));
                        if msg_symbol == symbol {
                            target_index += 1;
                            if args.print_prices {
                                println!(
                                    "index symbol={} price={:.10} msg_ts={}",
                                    symbol,
                                    IndexPriceMsg::get_index_price(payload),
                                    IndexPriceMsg::get_timestamp(payload)
                                );
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(None) => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(err) => {
                eprintln!("receive_error service={} err={}", service_name, err);
                std::thread::sleep(Duration::from_secs(1));
            }
        }

        if last_stats.elapsed() >= stats_interval {
            println!(
                "stats total={} funding={} mark={} index={} target_funding={} target_mark={} target_index={} target_samples={} last_target={}",
                total,
                funding_total,
                mark_total,
                index_total,
                target_funding,
                target_mark,
                target_index,
                rolling.len(),
                empty_dash(&last_target_line)
            );
            last_stats = Instant::now();
        }
    }

    Ok(())
}

fn load_thresholds(
    args: &Args,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Result<FundingThresholds> {
    let key = format!(
        "{}:funding_rate_thresholds_{}_{}",
        args.env_name,
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    );
    let redis_url = args
        .redis_url
        .clone()
        .or_else(|| std::env::var("REDIS_URL").ok())
        .unwrap_or_else(|| "redis://127.0.0.1:6379/0".to_string());

    let client = redis::Client::open(redis_url.clone())
        .with_context(|| format!("failed to create redis client url={redis_url}"))?;
    let mut conn = client
        .get_connection()
        .with_context(|| format!("failed to connect redis url={redis_url}"))?;
    let raw_fields: HashMap<String, String> = conn
        .hgetall(&key)
        .with_context(|| format!("failed to HGETALL redis key={key}"))?;
    let expanded_fields = expand_intra_fixed_close_thresholds(&raw_fields);

    Ok(FundingThresholds {
        raw_key: key,
        raw_fields,
        expanded_fields,
    })
}

fn expand_intra_fixed_close_thresholds(raw: &HashMap<String, String>) -> HashMap<String, f64> {
    let mut parsed = HashMap::new();
    for (key, value) in raw {
        if let Ok(value) = value.parse::<f64>() {
            parsed.insert(key.clone(), value);
        }
    }

    for (fixed_key, legacy_key) in [
        ("forward_close", "4h_forward_close"),
        ("backward_close", "4h_backward_close"),
    ] {
        let Some(value) = parsed
            .get(fixed_key)
            .or_else(|| parsed.get(legacy_key))
            .copied()
        else {
            continue;
        };
        parsed.remove(fixed_key);
        for period in ["1h", "2h", "4h", "6h", "8h"] {
            parsed.insert(format!("{period}_{fixed_key}"), value);
        }
    }

    parsed
}

fn parse_venue(raw: &str) -> Result<TradingVenue> {
    let slug = raw.trim().to_ascii_lowercase().replace('_', "-");
    let venue = match slug.as_str() {
        "binance-margin" => TradingVenue::BinanceMargin,
        "binance-futures" | "binance" => TradingVenue::BinanceFutures,
        "okex-margin" | "okx-margin" => TradingVenue::OkexMargin,
        "okex-futures" | "okx-futures" | "okex" | "okx" => TradingVenue::OkexFutures,
        "bybit-margin" => TradingVenue::BybitMargin,
        "bybit-futures" | "bybit" => TradingVenue::BybitFutures,
        "bitget-margin" => TradingVenue::BitgetMargin,
        "bitget-futures" | "bitget" => TradingVenue::BitgetFutures,
        "gate-margin" => TradingVenue::GateMargin,
        "gate-futures" | "gate" => TradingVenue::GateFutures,
        "aster-margin" => TradingVenue::AsterMargin,
        "aster-futures" | "aster" => TradingVenue::AsterFutures,
        "hyperliquid-margin" => TradingVenue::HyperliquidMargin,
        "hyperliquid-futures" | "hyperliquid" => TradingVenue::HyperliquidFutures,
        _ => bail!("unknown venue slug: {raw}"),
    };
    Ok(venue)
}

fn normalize_symbol_key(symbol: &str) -> String {
    symbol
        .trim()
        .to_ascii_uppercase()
        .replace(['-', '_', '/'], "")
        .replace(":SWAP", "")
        .replace("SWAP", "")
        .replace("PERP", "")
}

fn empty_dash(value: &str) -> &str {
    if value.is_empty() {
        "-"
    } else {
        value
    }
}
