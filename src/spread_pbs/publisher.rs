use anyhow::Result;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;

/// AskBidSpreadMsg wire format 实测占用：4B msg_type + 4B symbol_len + N(symbol)
/// + 8B ts + 4×8B = 至多 ~80 字节。预留到 128 与 dat_pbs 对齐，便于
/// 未来扩展且和 forwarder.rs 的 SPREAD_MAX_BYTES 一致。
pub const SPREAD_PAYLOAD_BYTES: usize = 128;

const HISTORY_SIZE: usize = 100;
const SUBSCRIBER_MAX_BUFFER: usize = 8192;

/// `spread_pbs/<venue>/ask_bid_spread` 服务的 publisher 包装。
///
/// `max_subscribers = 64` 与 `max_publishers = 1` 与 plan 约定一致，
/// 与 dat_pbs 的同名 channel 完全独立。
pub struct SpreadPublisher {
    publisher: Publisher<ipc::Service, [u8; SPREAD_PAYLOAD_BYTES], ()>,
    service_name: String,
}

impl SpreadPublisher {
    /// `venue_slug` 直接使用 `data_pub_slug()`（如 `okex-futures`）。
    pub fn new(venue_slug: &str) -> Result<Self> {
        let service_name = format!("spread_pbs/{}/ask_bid_spread", venue_slug);
        let node_name = format!("spread_pbs_{}", venue_slug.replace('-', "_"));

        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; SPREAD_PAYLOAD_BYTES]>()
            .max_publishers(1)
            .max_subscribers(64)
            .history_size(HISTORY_SIZE)
            .subscriber_max_buffer_size(SUBSCRIBER_MAX_BUFFER)
            .open_or_create()?;

        let publisher = service.publisher_builder().create()?;

        log::info!(
            "spread_pbs publisher ready: service={} max_subscribers=64 payload={}B",
            service_name,
            SPREAD_PAYLOAD_BYTES
        );
        Ok(Self {
            publisher,
            service_name,
        })
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    /// 同步 publish。`data` 长度需 ≤ `SPREAD_PAYLOAD_BYTES`。
    pub fn publish(&self, data: &[u8]) -> Result<()> {
        anyhow::ensure!(
            data.len() <= SPREAD_PAYLOAD_BYTES,
            "spread payload {} exceeds {}",
            data.len(),
            SPREAD_PAYLOAD_BYTES
        );
        let mut buffer = [0u8; SPREAD_PAYLOAD_BYTES];
        buffer[..data.len()].copy_from_slice(data);

        let sample = self.publisher.loan_uninit()?;
        let sample = sample.write_payload(buffer);
        sample.send()?;
        Ok(())
    }
}
