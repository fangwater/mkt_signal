use anyhow::Result;
use clap::Parser;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use mkt_signal::common::redis_client::RedisSettings;
use mkt_signal::common::symbol_util::normalize_symbol_for_venue;
use mkt_signal::common::trade_flow_feature_msg::TradeFlowFeatureMsg;
use mkt_signal::factor_pub::factor_index::factor_name_to_channel;
use mkt_signal::factor_pub::rl_vol::compute_rl_return_volatility;
use mkt_signal::factor_pub::trade_flow_feature_pub::cfg::TradeFlowFeaturePubConfig;
use mkt_signal::funding_rate::factor_value_hub::FactorValueHub;
use mkt_signal::signal::common::TradingVenue;
use std::collections::VecDeque;
use std::thread;
use std::time::Duration;

const PROCESS_NAME: &str = "inspect_rl_vol";
const TARGET_FACTOR_NAME: &str = "rl_return_volatility";
const TRADE_FLOW_CHANNEL: &str = "trade_flow_feature";
const TRADE_FLOW_MAX_BYTES: usize = 1024;
const TRADE_FLOW_SUBSCRIBER_BUFFER_SIZE: usize = 8192;
const TRADE_FLOW_SERVICE_HISTORY_SIZE: usize = 128;
const TRADE_FLOW_MAX_SUBSCRIBERS: usize = 10;
const FIELD_CLOSE: usize = 3;
const MAX_CLOSE_HISTORY: usize = 4096;
const POLL_INTERVAL_MS: u64 = 20;
const EPSILON: f64 = 1e-12;

#[derive(Parser, Debug)]
#[command(name = "inspect_rl_vol")]
#[command(
    about = "Inspect rl_return_volatility IPC and recompute raw volatility from trade_flow_feature"
)]
struct Args {
    /// Symbol to inspect, e.g. BTCUSDT
    #[arg(long)]
    symbol: String,

    /// Hedge venue used for factor IPC subscription
    #[arg(long, value_enum)]
    hedge_venue: TradingVenue,

    /// Open venue used by trade_signal context; defaults to hedge venue
    #[arg(long, value_enum)]
    open_venue: Option<TradingVenue>,

    /// Trade flow feature config path for rl_factor settings
    #[arg(long, default_value = "config/trade_flow_feature_pub.yaml")]
    config: String,

    /// Exit after the first snapshot is observed for the symbol
    #[arg(long, default_value_t = false)]
    once: bool,

    /// Poll interval in milliseconds
    #[arg(long, default_value_t = POLL_INTERVAL_MS)]
    poll_interval_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
struct RawVolState {
    ts_ms: Option<i64>,
    close_count: usize,
    required_count: usize,
    latest_close_bits: Option<u64>,
    prev_close_bits: Option<u64>,
    raw_value_bits: Option<u64>,
    scaled_value_bits: Option<u64>,
    note: String,
}

impl RawVolState {
    fn latest_close(&self) -> Option<f64> {
        self.latest_close_bits.map(f64::from_bits)
    }

    fn prev_close(&self) -> Option<f64> {
        self.prev_close_bits.map(f64::from_bits)
    }

    fn raw_value(&self) -> Option<f64> {
        self.raw_value_bits.map(f64::from_bits)
    }

    fn scaled_value(&self) -> Option<f64> {
        self.scaled_value_bits.map(f64::from_bits)
    }
}

fn default_redis_settings() -> RedisSettings {
    RedisSettings {
        host: "127.0.0.1".to_string(),
        port: 6379,
        db: 0,
        username: None,
        password: None,
        prefix: None,
    }
}

fn push_close(closes: &mut VecDeque<f64>, close: f64) {
    closes.push_back(close);
    if closes.len() > MAX_CLOSE_HISTORY {
        closes.pop_front();
    }
}

fn apply_scale(raw: f64, scale_factor: f64) -> f64 {
    raw * scale_factor
}

fn infer_raw_from_published(value: f64, scale_factor: f64) -> String {
    if !(value.is_finite() && scale_factor.is_finite() && scale_factor > 0.0) {
        return "-".to_string();
    }

    format!("{:.10}", value / scale_factor)
}

fn create_trade_flow_subscriber(
    node: &Node<ipc::Service>,
    venue_slug: &str,
) -> Result<Subscriber<ipc::Service, [u8; TRADE_FLOW_MAX_BYTES], ()>> {
    let service_name = format!("factor_pub/{}/{}", venue_slug, TRADE_FLOW_CHANNEL);
    let service = node
        .service_builder(&ServiceName::new(&service_name)?)
        .publish_subscribe::<[u8; TRADE_FLOW_MAX_BYTES]>()
        .max_publishers(1)
        .max_subscribers(TRADE_FLOW_MAX_SUBSCRIBERS)
        .subscriber_max_buffer_size(TRADE_FLOW_SUBSCRIBER_BUFFER_SIZE)
        .history_size(TRADE_FLOW_SERVICE_HISTORY_SIZE)
        .open_or_create()?;

    Ok(service.subscriber_builder().create()?)
}

fn poll_raw_vol_state(
    subscriber: &Subscriber<ipc::Service, [u8; TRADE_FLOW_MAX_BYTES], ()>,
    hedge_venue: TradingVenue,
    target_symbol: &str,
    closes: &mut VecDeque<f64>,
    pct_change_period: usize,
    rolling_window: usize,
    scale_factor: f64,
    last_state: &Option<RawVolState>,
) -> Result<Option<RawVolState>> {
    let required_count = pct_change_period + rolling_window;
    let mut newest = last_state.clone();

    loop {
        let Some(sample) = subscriber.receive()? else {
            break;
        };
        let payload = sample.payload();
        if payload.iter().all(|&b| b == 0) {
            continue;
        }

        let msg = match TradeFlowFeatureMsg::from_bytes(payload) {
            Ok(msg) => msg,
            Err(err) => {
                eprintln!("warn: decode trade_flow_feature failed: {err}");
                continue;
            }
        };

        let symbol_key = normalize_symbol_for_venue(&msg.symbol, hedge_venue);
        if symbol_key != target_symbol {
            continue;
        }

        let Some(close) = msg.values.get(FIELD_CLOSE).copied() else {
            continue;
        };
        if !close.is_finite() || close <= 0.0 {
            continue;
        }

        push_close(closes, close);
        let raw_value = compute_rl_return_volatility(closes, pct_change_period, rolling_window)?;
        let scaled_value = raw_value.map(|raw| apply_scale(raw, scale_factor));
        let latest_close = closes.back().copied();
        let prev_close = closes
            .len()
            .checked_sub(2)
            .and_then(|idx| closes.get(idx).copied());
        let note = if raw_value.is_some() {
            "ok".to_string()
        } else {
            format!("warming_up({}/{})", closes.len(), required_count)
        };

        newest = Some(RawVolState {
            ts_ms: Some(msg.ts),
            close_count: closes.len(),
            required_count,
            latest_close_bits: latest_close.map(f64::to_bits),
            prev_close_bits: prev_close.map(f64::to_bits),
            raw_value_bits: raw_value.map(f64::to_bits),
            scaled_value_bits: scaled_value.map(f64::to_bits),
            note,
        });
    }

    Ok(newest)
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let open_venue = args.open_venue.unwrap_or(args.hedge_venue);
    let hedge_venue_slug = args.hedge_venue.data_pub_slug();
    let target_symbol = normalize_symbol_for_venue(&args.symbol, args.hedge_venue);
    let poll_interval = Duration::from_millis(args.poll_interval_ms.max(1));
    let cfg = TradeFlowFeaturePubConfig::load(&args.config)?;
    let required_count = cfg.rl_factor.pct_change_period + cfg.rl_factor.rolling_window;

    let node = NodeBuilder::new()
        .name(&NodeName::new(PROCESS_NAME)?)
        .create::<ipc::Service>()?;

    let mut hub = FactorValueHub::new(
        &node,
        open_venue,
        args.hedge_venue,
        TARGET_FACTOR_NAME,
        TARGET_FACTOR_NAME,
        None,
        default_redis_settings(),
        "inspect".to_string(),
        30 * 60,
        10_000,
    )?;
    let trade_flow_subscriber = create_trade_flow_subscriber(&node, hedge_venue_slug)?;

    println!(
        "subscribed factor_service=factor_pub/{}/{} trade_flow_service=factor_pub/{}/{} symbol={} symbol_key={} open_venue={} hedge_venue={} once={} config={} rl_pct_change_period={} rl_rolling_window={} rl_scale_factor={}",
        hedge_venue_slug,
        factor_name_to_channel(TARGET_FACTOR_NAME),
        hedge_venue_slug,
        TRADE_FLOW_CHANNEL,
        args.symbol,
        target_symbol,
        open_venue.data_pub_slug(),
        hedge_venue_slug,
        args.once,
        args.config,
        cfg.rl_factor.pct_change_period,
        cfg.rl_factor.rolling_window,
        cfg.rl_factor.scale_factor,
    );

    let mut close_history = VecDeque::with_capacity(required_count.max(64));
    let mut last_factor_seen = None;
    let mut last_raw_seen: Option<RawVolState> = None;

    loop {
        let prev_raw_seen = last_raw_seen.clone();
        last_raw_seen = poll_raw_vol_state(
            &trade_flow_subscriber,
            args.hedge_venue,
            &target_symbol,
            &mut close_history,
            cfg.rl_factor.pct_change_period,
            cfg.rl_factor.rolling_window,
            cfg.rl_factor.scale_factor,
            &last_raw_seen,
        )?;

        let snapshot = hub.lookup_factor_value(&args.symbol, args.hedge_venue);
        let factor_current = (
            snapshot.ts_ms,
            snapshot.target_factor_value.map(f64::to_bits),
            snapshot.ready,
            snapshot.note.clone(),
        );
        let should_print =
            last_factor_seen.as_ref() != Some(&factor_current) || prev_raw_seen != last_raw_seen;

        if should_print {
            let inferred_raw = snapshot
                .target_factor_value
                .map(|v| infer_raw_from_published(v, cfg.rl_factor.scale_factor))
                .unwrap_or_else(|| "-".to_string());
            let latest_close = last_raw_seen.as_ref().and_then(|s| s.latest_close());
            let prev_close = last_raw_seen.as_ref().and_then(|s| s.prev_close());
            let close_delta = match (latest_close, prev_close) {
                (Some(latest), Some(prev)) => Some(latest - prev),
                _ => None,
            };
            let close_same = match close_delta {
                Some(delta) => delta.abs() <= EPSILON,
                None => false,
            };

            println!(
                "symbol={} symbol_key={} factor_ts_ms={} ready={} factor_value={} factor_index={} note={} inferred_raw={} raw_ts_ms={} raw_close_count={}/{} latest_close={} prev_close={} close_delta={} close_same={} raw_value={} scaled_from_raw={} raw_note={}",
                args.symbol,
                snapshot.symbol_key,
                snapshot
                    .ts_ms
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                snapshot
                    .ready
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                snapshot
                    .target_factor_value
                    .map(|v| format!("{v:.10}"))
                    .unwrap_or_else(|| "-".to_string()),
                snapshot
                    .factor_index
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                snapshot.note,
                inferred_raw,
                last_raw_seen
                    .as_ref()
                    .and_then(|s| s.ts_ms)
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                last_raw_seen
                    .as_ref()
                    .map(|s| s.close_count.to_string())
                    .unwrap_or_else(|| "0".to_string()),
                last_raw_seen
                    .as_ref()
                    .map(|s| s.required_count.to_string())
                    .unwrap_or_else(|| required_count.to_string()),
                latest_close
                    .map(|v| format!("{v:.10}"))
                    .unwrap_or_else(|| "-".to_string()),
                prev_close
                    .map(|v| format!("{v:.10}"))
                    .unwrap_or_else(|| "-".to_string()),
                close_delta
                    .map(|v| format!("{v:.10}"))
                    .unwrap_or_else(|| "-".to_string()),
                close_same,
                last_raw_seen
                    .as_ref()
                    .and_then(|s| s.raw_value())
                    .map(|v| format!("{v:.10}"))
                    .unwrap_or_else(|| "-".to_string()),
                last_raw_seen
                    .as_ref()
                    .and_then(|s| s.scaled_value())
                    .map(|v| format!("{v:.10}"))
                    .unwrap_or_else(|| "-".to_string()),
                last_raw_seen
                    .as_ref()
                    .map(|s| s.note.clone())
                    .unwrap_or_else(|| "waiting_trade_flow".to_string()),
            );
            last_factor_seen = Some(factor_current);
        }

        if args.once && snapshot.target_factor_value.is_some() {
            break;
        }

        thread::sleep(poll_interval);
    }

    Ok(())
}
