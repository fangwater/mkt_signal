use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::common::ipc_service_name::build_service_name;
use crate::signal::trade_signal::TradeSignal;

pub const SIGNAL_PAYLOAD: usize = 4_096;
pub const RESAMPLE_PAYLOAD: usize = 32 * 1024;
pub const BINANCE_MARGIN_UPDATE_PAYLOAD: usize = SIGNAL_PAYLOAD;
pub const BINANCE_UM_UPDATE_PAYLOAD: usize = SIGNAL_PAYLOAD;
pub const QUERY_REQ_PAYLOAD: usize = 256;
// Query responses now also carry standard-account snapshot events, which are
// materially larger than compact order-query payloads.
pub const QUERY_RESP_PAYLOAD: usize = 4_096;

static SIGNAL_PUBLISH_DRY_RUN: AtomicBool = AtomicBool::new(false);

pub struct GenericPublisher<const PAYLOAD: usize> {
    publisher: Publisher<ipc::Service, [u8; PAYLOAD], ()>,
    full_service: String,
    suppress_trade_signal_publish: bool,
    suppressed_publish_count: AtomicU64,
}

pub fn configure_signal_publish_dry_run(enabled: bool) {
    SIGNAL_PUBLISH_DRY_RUN.store(enabled, Ordering::Relaxed);
}

fn signal_publish_dry_run_enabled() -> bool {
    SIGNAL_PUBLISH_DRY_RUN.load(Ordering::Relaxed)
}

fn describe_signal_payload(data: &[u8]) -> String {
    let signal_type = TradeSignal::get_signal_type(data)
        .map(|v| format!("{:?}", v))
        .unwrap_or_else(|| "unknown".to_string());
    let generation_time = TradeSignal::get_generation_time(data)
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".to_string());
    format!(
        "signal_type={} generation_time={} payload_bytes={}",
        signal_type,
        generation_time,
        data.len()
    )
}

impl<const PAYLOAD: usize> GenericPublisher<PAYLOAD> {
    pub fn new(channel_name: &str) -> anyhow::Result<Self> {
        Self::new_with_prefix("signal_pubs", channel_name)
    }

    pub fn new_with_prefix(prefix: &str, channel_name: &str) -> anyhow::Result<Self> {
        let full_service = build_service_name(&format!("{}/{}", prefix, channel_name));
        let service_name = ServiceName::new(&full_service)?;

        let node_id = format!("{}_{}", prefix, channel_name);
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_id)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&service_name)
            .publish_subscribe::<[u8; PAYLOAD]>()
            .max_publishers(1)
            .max_subscribers(32)
            .history_size(128)
            .subscriber_max_buffer_size(256)
            .open_or_create()?;

        let publisher = service.publisher_builder().create()?;

        info!("IceOryx publisher created: {}", full_service);

        Ok(Self {
            publisher,
            full_service,
            suppress_trade_signal_publish: prefix == "signal_pubs"
                && channel_name == "trade_signal",
            suppressed_publish_count: AtomicU64::new(0),
        })
    }

    pub fn publish(&self, data: &[u8]) -> anyhow::Result<()> {
        if data.len() > PAYLOAD {
            anyhow::bail!("Data size exceeds {} bytes", PAYLOAD);
        }

        if self.suppress_trade_signal_publish && signal_publish_dry_run_enabled() {
            let suppressed = self
                .suppressed_publish_count
                .fetch_add(1, Ordering::Relaxed)
                + 1;
            if suppressed == 1 || suppressed % 100 == 0 {
                warn!(
                    "Signal publish suppressed (dry-run): service={} count={} {}",
                    self.full_service,
                    suppressed,
                    describe_signal_payload(data)
                );
            }
            return Ok(());
        }

        // Prepare a fixed-size buffer and copy the data
        let mut buffer = [0u8; PAYLOAD];
        buffer[..data.len()].copy_from_slice(data);

        // Send via loan + write_payload pattern (same as forwarder)
        let sample = self.publisher.loan_uninit()?;
        let sample = sample.write_payload(buffer);
        sample.send()?;

        // 生产环境去除发布详情 DEBUG 日志，避免刷屏

        Ok(())
    }
}

pub type SignalPublisher = GenericPublisher<SIGNAL_PAYLOAD>;
pub type ResamplePublisher = GenericPublisher<RESAMPLE_PAYLOAD>;
// 通用持久化发布器（支持所有交易所）
pub type TradeUpdatePublisher = GenericPublisher<SIGNAL_PAYLOAD>;
pub type OrderUpdatePublisher = GenericPublisher<SIGNAL_PAYLOAD>;
pub type UniformOrderPublisher = GenericPublisher<SIGNAL_PAYLOAD>;
