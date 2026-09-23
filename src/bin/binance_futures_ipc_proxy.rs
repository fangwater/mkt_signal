use anyhow::{Context, Result};
use iceoryx2::port::publisher::Publisher;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use std::time::{Duration, Instant};

use mkt_signal::spread_pbs::publisher::{DERIVATIVES_PAYLOAD_BYTES, SPREAD_PAYLOAD_BYTES};

const BBO_SOURCE: &str = "spread_pbs/binance-futures/ask_bid_spread";
const DERIVATIVES_SOURCE: &str = "dat_pbs/binance-futures/derivatives";
const BBO_PROXY: &str = "spread_pbs_proxy/binance-futures/ask_bid_spread";
const DERIVATIVES_PROXY: &str = "dat_pbs_proxy/binance-futures/derivatives";
const MAX_NODES: usize = 128;
const MAX_SUBSCRIBERS: usize = 128;
const SUBSCRIBER_MAX_BUFFER: usize = 8192;
const STATS_INTERVAL: Duration = Duration::from_secs(10);
const STALE_INTERVAL: Duration = Duration::from_secs(3);

fn open_source<const N: usize>(
    node: &Node<ipc::Service>,
    name: &str,
    history: usize,
) -> Result<Subscriber<ipc::Service, [u8; N], ()>> {
    let service = node
        .service_builder(&ServiceName::new(name)?)
        .publish_subscribe::<[u8; N]>()
        .max_publishers(1)
        .max_subscribers(64)
        .history_size(history)
        .subscriber_max_buffer_size(SUBSCRIBER_MAX_BUFFER)
        .open()
        .with_context(|| format!("open source {name}"))?;
    service
        .subscriber_builder()
        .buffer_size(SUBSCRIBER_MAX_BUFFER)
        .create()
        .with_context(|| format!("subscribe to {name}"))
}

fn create_proxy<const N: usize>(
    node: &Node<ipc::Service>,
    name: &str,
    history: usize,
) -> Result<Publisher<ipc::Service, [u8; N], ()>> {
    let service = node
        .service_builder(&ServiceName::new(name)?)
        .publish_subscribe::<[u8; N]>()
        .max_publishers(1)
        .max_nodes(MAX_NODES)
        .max_subscribers(MAX_SUBSCRIBERS)
        .history_size(history)
        .subscriber_max_buffer_size(SUBSCRIBER_MAX_BUFFER)
        .open_or_create()
        .with_context(|| format!("create proxy service {name}"))?;
    service
        .publisher_builder()
        .create()
        .with_context(|| format!("publish to {name}"))
}

fn forward<const N: usize>(
    source: &Subscriber<ipc::Service, [u8; N], ()>,
    target: &Publisher<ipc::Service, [u8; N], ()>,
    label: &str,
) -> Result<usize> {
    let mut count = 0;
    while count < 256 {
        let Some(sample) = source
            .receive()
            .with_context(|| format!("receive {label}"))?
        else {
            break;
        };
        target
            .loan_uninit()
            .with_context(|| format!("loan {label}"))?
            .write_payload(*sample.payload())
            .send()
            .with_context(|| format!("send {label}"))?;
        count += 1;
    }
    Ok(count)
}

fn main() -> Result<()> {
    env_logger::init();
    let node = NodeBuilder::new()
        .name(&NodeName::new("binance_futures_ipc_proxy")?)
        .create::<ipc::Service>()?;
    let bbo_source = open_source::<SPREAD_PAYLOAD_BYTES>(&node, BBO_SOURCE, 100)?;
    let derivatives_source =
        open_source::<DERIVATIVES_PAYLOAD_BYTES>(&node, DERIVATIVES_SOURCE, 50)?;
    let bbo_proxy = create_proxy::<SPREAD_PAYLOAD_BYTES>(&node, BBO_PROXY, 100)?;
    let derivatives_proxy =
        create_proxy::<DERIVATIVES_PAYLOAD_BYTES>(&node, DERIVATIVES_PROXY, 50)?;

    log::info!(
        "Binance Futures IPC proxy ready: bbo={} -> {} derivatives={} -> {} max_nodes={} max_subscribers={}",
        BBO_SOURCE,
        BBO_PROXY,
        DERIVATIVES_SOURCE,
        DERIVATIVES_PROXY,
        MAX_NODES,
        MAX_SUBSCRIBERS
    );
    let mut bbo_count = 0u64;
    let mut derivatives_count = 0u64;
    let mut last_bbo = Instant::now();
    let mut last_derivatives = Instant::now();
    let mut last_stats = Instant::now();
    loop {
        let bbo = forward(&bbo_source, &bbo_proxy, "BBO")?;
        let derivatives = forward(&derivatives_source, &derivatives_proxy, "derivatives")?;
        if bbo > 0 {
            last_bbo = Instant::now();
            bbo_count += bbo as u64;
        }
        if derivatives > 0 {
            last_derivatives = Instant::now();
            derivatives_count += derivatives as u64;
        }
        if last_stats.elapsed() >= STATS_INTERVAL {
            log::info!(
                "Binance Futures IPC proxy stats: bbo={} derivatives={} bbo_age_ms={} derivatives_age_ms={}",
                bbo_count,
                derivatives_count,
                last_bbo.elapsed().as_millis(),
                last_derivatives.elapsed().as_millis()
            );
            if last_bbo.elapsed() >= STALE_INTERVAL || last_derivatives.elapsed() >= STALE_INTERVAL
            {
                log::warn!("Binance Futures IPC proxy source stale");
            }
            bbo_count = 0;
            derivatives_count = 0;
            last_stats = Instant::now();
        }
        if bbo == 0 && derivatives == 0 {
            std::thread::yield_now();
        }
    }
}
