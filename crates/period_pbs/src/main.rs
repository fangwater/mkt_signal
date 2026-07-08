use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use ipc_common::iceoryx_subscriber::{ChannelType, MultiChannelSubscriber, SubscribeParams};
use period_pbs::collector::{CollectorConfig, CompletedPeriod, PeriodCollector};
use period_pbs::config::{
    PeriodPbsConfig, RuntimeVenueConfig, PERIOD_MS, SERVICE_ROOT, STATS_LOG_SECS,
};
use period_pbs::decode::{
    decode_market_msg_with_symbol, peek_market_msg, validate_trade_side, DecodedMarketMsg,
    MarketMsgKind,
};
use period_pbs::publisher::PeriodPublisher;
use runtime_common::affinity::pin_to_core;

#[derive(Debug, Parser)]
#[command(name = "period_pbs")]
#[command(
    about = "Cut dat_pbs trade+incremental IPC into 3s PeriodMessage protobuf and publish over ZMQ PUB."
)]
struct Args {
    /// TOML config path.
    #[arg(long, default_value = "config/period_pbs.toml")]
    config: PathBuf,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let config = PeriodPbsConfig::load_from_file(&args.config)?;
    if let Some(core) = config.core {
        pin_to_core(core).with_context(|| format!("pin period_pbs to cpu core {core}"))?;
    } else {
        log::info!("period_pbs cpu pinning skipped: core not configured");
    }
    let runtime_configs = config.runtime_venues()?;

    let node_name = format!("period_pbs_{}", std::process::id());
    let mut subscriber = MultiChannelSubscriber::new(&node_name)?;
    subscriber.subscribe_channels(build_subscriptions(&runtime_configs))?;

    let publisher = PeriodPublisher::bind(&config.zmq)?;
    let mut venues: Vec<VenueRuntime> =
        runtime_configs.into_iter().map(VenueRuntime::new).collect();

    log::info!(
        "period_pbs started config={} service_root={} period_ms={} core={} bind={} venues={} online_symbols={} stats_log_secs={}",
        args.config.display(),
        SERVICE_ROOT,
        PERIOD_MS,
        config
            .core
            .map_or_else(|| "none".to_string(), |core| core.to_string()),
        config.zmq.bind.trim(),
        venues.len(),
        config.online_symbols.len(),
        STATS_LOG_SECS,
    );
    for venue in &venues {
        log::info!(
            "period_pbs venue configured: venue={} topic={} poster_id={} poll_batch={} idle_sleep_us={} delay_ms={} mapped_symbols={}",
            venue.config.name,
            venue.config.topic,
            venue.config.poster_id,
            venue.config.poll_batch,
            venue.config.idle_sleep_us,
            venue.config.delay_ms,
            venue.config.symbols.len(),
        );
    }

    let stats_interval = Duration::from_secs(STATS_LOG_SECS);
    let mut last_stats_log = Instant::now();

    loop {
        let mut made_progress = false;
        for venue in &mut venues {
            let trade_count = poll_venue_channel(
                &mut subscriber,
                venue,
                ChannelType::Trade,
                "trade",
                &publisher,
            )?;
            let inc_count = poll_venue_channel(
                &mut subscriber,
                venue,
                ChannelType::Incremental,
                "incremental",
                &publisher,
            )?;
            if trade_count > 0 || inc_count > 0 {
                made_progress = true;
            }
        }

        if last_stats_log.elapsed() >= stats_interval {
            for venue in &venues {
                venue.log_stats();
            }
            last_stats_log = Instant::now();
        }

        if !made_progress {
            idle(&venues);
        }
    }
}

fn idle(venues: &[VenueRuntime]) {
    if venues.iter().any(|venue| venue.config.idle_sleep_us == 0) {
        thread::yield_now();
        return;
    }

    let idle_sleep_us = venues
        .iter()
        .map(|venue| venue.config.idle_sleep_us)
        .min()
        .unwrap_or(0);
    if idle_sleep_us == 0 {
        thread::yield_now();
    } else {
        thread::sleep(Duration::from_micros(idle_sleep_us));
    }
}

fn build_subscriptions(venues: &[RuntimeVenueConfig]) -> Vec<SubscribeParams> {
    let mut params = Vec::with_capacity(venues.len() * 2);
    for venue in venues {
        params.push(SubscribeParams {
            service_root: Some(SERVICE_ROOT.to_string()),
            topic_prefix: venue.name.clone(),
            channel: ChannelType::Trade,
        });
        params.push(SubscribeParams {
            service_root: Some(SERVICE_ROOT.to_string()),
            topic_prefix: venue.name.clone(),
            channel: ChannelType::Incremental,
        });
    }
    params
}

fn poll_venue_channel(
    subscriber: &mut MultiChannelSubscriber,
    venue: &mut VenueRuntime,
    channel: ChannelType,
    channel_label: &'static str,
    publisher: &PeriodPublisher,
) -> Result<usize> {
    let venue_name = venue.config.name.clone();
    let poll_batch = venue.config.poll_batch;
    subscriber
        .poll_channel_from_with(
            SERVICE_ROOT,
            &venue_name,
            &channel,
            Some(poll_batch),
            |payload| {
                venue.handle_payload(channel_label, payload, publisher);
                Ok(())
            },
        )
        .with_context(|| {
            format!("poll period_pbs channel venue={venue_name} channel={channel_label}")
        })
}

struct VenueRuntime {
    config: RuntimeVenueConfig,
    collector: PeriodCollector,
    stats: VenueStats,
}

#[derive(Default)]
struct VenueStats {
    received: u64,
    accepted: u64,
    filtered_symbols: u64,
    ignored_other: u64,
    decode_errors: u64,
    trade_msgs: u64,
    incremental_msgs: u64,
    completed_periods: u64,
    published: u64,
    publish_errors: u64,
}

impl VenueRuntime {
    fn new(config: RuntimeVenueConfig) -> Self {
        let collector = PeriodCollector::new(CollectorConfig {
            period_ms: PERIOD_MS,
            delay_ms: config.delay_ms,
            poster_id: config.poster_id.clone(),
        });
        Self {
            config,
            collector,
            stats: VenueStats::default(),
        }
    }

    fn handle_payload(
        &mut self,
        channel_label: &'static str,
        payload: &[u8],
        publisher: &PeriodPublisher,
    ) {
        self.stats.received = self.stats.received.saturating_add(1);
        if let Err(err) = self.process_payload(payload, publisher) {
            self.stats.decode_errors = self.stats.decode_errors.saturating_add(1);
            log::warn!(
                "period_pbs decode/process failed: venue={} channel={} err={:#}",
                self.config.name,
                channel_label,
                err
            );
        }
    }

    fn process_payload(&mut self, payload: &[u8], publisher: &PeriodPublisher) -> Result<()> {
        let (kind, source_symbol) = peek_market_msg(payload)?;
        if kind == MarketMsgKind::Other {
            self.stats.ignored_other = self.stats.ignored_other.saturating_add(1);
            return Ok(());
        }

        let Some(canonical_symbol) = self.config.symbols.canonical_for_source(source_symbol) else {
            self.stats.filtered_symbols = self.stats.filtered_symbols.saturating_add(1);
            return Ok(());
        };
        let completed = match decode_market_msg_with_symbol(payload, canonical_symbol)? {
            DecodedMarketMsg::Trade(trade) => {
                validate_trade_side(trade.side)?;
                self.stats.accepted = self.stats.accepted.saturating_add(1);
                self.stats.trade_msgs = self.stats.trade_msgs.saturating_add(1);
                self.collector.push_trade(trade)?
            }
            DecodedMarketMsg::Incremental(inc) => {
                self.stats.accepted = self.stats.accepted.saturating_add(1);
                self.stats.incremental_msgs = self.stats.incremental_msgs.saturating_add(1);
                self.collector.push_incremental(inc)?
            }
            DecodedMarketMsg::Other => {
                self.stats.ignored_other = self.stats.ignored_other.saturating_add(1);
                Vec::new()
            }
        };

        for period in completed {
            self.publish_period(period, publisher);
        }

        Ok(())
    }

    fn publish_period(&mut self, period: CompletedPeriod, publisher: &PeriodPublisher) {
        self.stats.completed_periods = self.stats.completed_periods.saturating_add(1);
        print_period_table(&period);

        match publisher.publish(&self.config.topic, &period.encoded) {
            Ok(()) => {
                self.stats.published = self.stats.published.saturating_add(1);
                log::info!(
                    "period_pbs published venue={} topic={} period={} ts={} symbols={} trades={} incs={} pb_bytes={}",
                    self.config.name,
                    self.config.topic,
                    period.period,
                    period.upper_bound_ms,
                    period.message.symbol_infos.len(),
                    period.trade_count,
                    period.inc_count,
                    period.encoded.len()
                );
            }
            Err(err) => {
                self.stats.publish_errors = self.stats.publish_errors.saturating_add(1);
                log::warn!(
                    "period_pbs publish failed: venue={} topic={} period={} err={:#}",
                    self.config.name,
                    self.config.topic,
                    period.period,
                    err
                );
            }
        }
    }

    fn log_stats(&self) {
        log::info!(
            "period_pbs stats: venue={} topic={} received={} accepted={} trades={} incrementals={} filtered_symbols={} ignored_other={} decode_errors={} completed_periods={} published={} publish_errors={}",
            self.config.name,
            self.config.topic,
            self.stats.received,
            self.stats.accepted,
            self.stats.trade_msgs,
            self.stats.incremental_msgs,
            self.stats.filtered_symbols,
            self.stats.ignored_other,
            self.stats.decode_errors,
            self.stats.completed_periods,
            self.stats.published,
            self.stats.publish_errors,
        );
    }
}

fn print_period_table(period: &CompletedPeriod) {
    const SYMBOL_W: usize = 24;
    const INC_W: usize = 12;
    const TRADE_W: usize = 12;
    let width = SYMBOL_W + INC_W + TRADE_W + 10;
    let line = "-".repeat(width);

    println!("+{}+", line);
    println!("|{:^width$}|", "OrderBook Archive", width = width);
    println!(
        "| {:<width$} |",
        format!(
            "period: {} ts: {} pb_bytes: {}",
            period.period,
            period.upper_bound_ms,
            period.encoded.len()
        ),
        width = width - 2
    );
    println!("+{}+", line);
    println!(
        "| {:<SYMBOL_W$} | {:>INC_W$} | {:>TRADE_W$} |",
        "Symbol", "Inc_Count", "Trade_Count"
    );
    println!("+{}+", line);
    for symbol_info in &period.message.symbol_infos {
        println!(
            "| {:<SYMBOL_W$} | {:>INC_W$} | {:>TRADE_W$} |",
            symbol_info.symbol,
            symbol_info.incs.len(),
            symbol_info.trades.len()
        );
    }
    println!("+{}+", line);
    println!(
        "| {:<SYMBOL_W$} | {:>INC_W$} | {:>TRADE_W$} |",
        "TOTAL", period.inc_count, period.trade_count
    );
    println!("+{}+", line);
}
