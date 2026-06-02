use anyhow::Result;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;

use crate::rolling_metrics::latency_snapshot::LATENCY_SNAPSHOT_PAYLOAD_LEN;

/// AskBidSpreadMsg wire format 实测占用：4B msg_type + 4B symbol_len + N(symbol)
/// + 8B ts + 4×8B = 至多 ~80 字节。预留到 128 与 dat_pbs 对齐，便于
/// 未来扩展且和 forwarder.rs 的 SPREAD_MAX_BYTES 一致。
pub const SPREAD_PAYLOAD_BYTES: usize = 128;
pub const TRADE_PAYLOAD_BYTES: usize = 128;
pub const DERIVATIVES_PAYLOAD_BYTES: usize = 128;
pub const INCREMENTAL_PAYLOAD_BYTES: usize = 2048;

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

/// `spread_pbs/<venue>/latency` 服务的 publisher。这个 service 不经过
/// `IPC_NAMESPACE`，因为 spread_pbs 在单机上按 venue 唯一部署。
pub struct SpreadLatencyPublisher {
    publisher: Publisher<ipc::Service, [u8; LATENCY_SNAPSHOT_PAYLOAD_LEN], ()>,
    service_name: String,
}

/// OKX SBE trades 替代旧 `dat_pbs/<venue>/trade` 的 publisher。
///
/// 这个服务必须独占创建：如果同名服务已经存在，说明旧 dat_pbs trade 通道或
/// 其他 publisher 仍在占用，启动应直接失败，而不是 open 复用。
pub struct SpreadTradePublisher {
    publisher: Publisher<ipc::Service, [u8; TRADE_PAYLOAD_BYTES], ()>,
    service_name: String,
}

/// OKX SBE books-l2-tbt 替代旧 `dat_pbs/<venue>/incremental` 的 publisher。
///
/// 与 trade replacement 一样只允许创建新服务；同名服务已存在时启动直接失败。
pub struct SpreadIncrementalPublisher {
    publisher: Publisher<ipc::Service, [u8; INCREMENTAL_PAYLOAD_BYTES], ()>,
    service_name: String,
}

/// OKX derivatives metrics 替代旧 `dat_pbs/<venue>/derivatives` 的 publisher。
pub struct SpreadDerivativesPublisher {
    publisher: Publisher<ipc::Service, [u8; DERIVATIVES_PAYLOAD_BYTES], ()>,
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

impl SpreadLatencyPublisher {
    pub fn new(venue_slug: &str) -> Result<Self> {
        let service_name = format!("spread_pbs/{}/latency", venue_slug);
        let node_name = format!("spread_pbs_{}_latency", venue_slug.replace('-', "_"));

        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; LATENCY_SNAPSHOT_PAYLOAD_LEN]>()
            .max_publishers(1)
            .max_subscribers(64)
            .history_size(HISTORY_SIZE)
            .subscriber_max_buffer_size(SUBSCRIBER_MAX_BUFFER)
            .open_or_create()?;

        let publisher = service.publisher_builder().create()?;

        log::info!(
            "spread_pbs latency publisher ready: service={} max_subscribers=64 payload={}B",
            service_name,
            LATENCY_SNAPSHOT_PAYLOAD_LEN
        );
        Ok(Self {
            publisher,
            service_name,
        })
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    pub fn publish(&self, data: [u8; LATENCY_SNAPSHOT_PAYLOAD_LEN]) -> Result<()> {
        self.publisher.send_copy(data)?;
        Ok(())
    }
}

impl SpreadTradePublisher {
    pub fn new_create_only(venue_slug: &str) -> Result<Self> {
        let service_name = format!("dat_pbs/{}/trade", venue_slug);
        let node_name = format!("spread_pbs_{}_trade", venue_slug.replace('-', "_"));

        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; TRADE_PAYLOAD_BYTES]>()
            .max_publishers(1)
            .max_subscribers(64)
            .history_size(HISTORY_SIZE)
            .subscriber_max_buffer_size(SUBSCRIBER_MAX_BUFFER)
            .create()?;

        let publisher = service.publisher_builder().create()?;

        log::info!(
            "spread_pbs trade publisher ready: service={} mode=create-only max_subscribers=64 payload={}B",
            service_name,
            TRADE_PAYLOAD_BYTES
        );
        Ok(Self {
            publisher,
            service_name,
        })
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    pub fn publish(&self, data: &[u8]) -> Result<()> {
        anyhow::ensure!(
            data.len() <= TRADE_PAYLOAD_BYTES,
            "trade payload {} exceeds {}",
            data.len(),
            TRADE_PAYLOAD_BYTES
        );
        let mut buffer = [0u8; TRADE_PAYLOAD_BYTES];
        buffer[..data.len()].copy_from_slice(data);

        let sample = self.publisher.loan_uninit()?;
        let sample = sample.write_payload(buffer);
        sample.send()?;
        Ok(())
    }
}

impl SpreadIncrementalPublisher {
    pub fn new_create_only(venue_slug: &str) -> Result<Self> {
        let service_name = format!("dat_pbs/{}/incremental", venue_slug);
        let node_name = format!("spread_pbs_{}_incremental", venue_slug.replace('-', "_"));

        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; INCREMENTAL_PAYLOAD_BYTES]>()
            .max_publishers(1)
            .max_subscribers(10)
            .history_size(100)
            .subscriber_max_buffer_size(SUBSCRIBER_MAX_BUFFER)
            .create()?;

        let publisher = service.publisher_builder().create()?;

        log::info!(
            "spread_pbs incremental publisher ready: service={} mode=create-only max_subscribers=10 payload={}B",
            service_name,
            INCREMENTAL_PAYLOAD_BYTES
        );
        Ok(Self {
            publisher,
            service_name,
        })
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    pub fn publish(&self, data: &[u8]) -> Result<()> {
        anyhow::ensure!(
            data.len() <= INCREMENTAL_PAYLOAD_BYTES,
            "incremental payload {} exceeds {}",
            data.len(),
            INCREMENTAL_PAYLOAD_BYTES
        );
        let mut buffer = [0u8; INCREMENTAL_PAYLOAD_BYTES];
        buffer[..data.len()].copy_from_slice(data);

        let sample = self.publisher.loan_uninit()?;
        let sample = sample.write_payload(buffer);
        sample.send()?;
        Ok(())
    }
}

impl SpreadDerivativesPublisher {
    pub fn new_create_only(venue_slug: &str) -> Result<Self> {
        let service_name = format!("dat_pbs/{}/derivatives", venue_slug);
        let node_name = format!("spread_pbs_{}_derivatives", venue_slug.replace('-', "_"));

        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; DERIVATIVES_PAYLOAD_BYTES]>()
            .max_publishers(1)
            .max_subscribers(10)
            .history_size(50)
            .subscriber_max_buffer_size(SUBSCRIBER_MAX_BUFFER)
            .create()?;

        let publisher = service.publisher_builder().create()?;

        log::info!(
            "spread_pbs derivatives publisher ready: service={} mode=create-only max_subscribers=10 payload={}B",
            service_name,
            DERIVATIVES_PAYLOAD_BYTES
        );
        Ok(Self {
            publisher,
            service_name,
        })
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    pub fn publish(&self, data: &[u8]) -> Result<()> {
        anyhow::ensure!(
            data.len() <= DERIVATIVES_PAYLOAD_BYTES,
            "derivatives payload {} exceeds {}",
            data.len(),
            DERIVATIVES_PAYLOAD_BYTES
        );
        let mut buffer = [0u8; DERIVATIVES_PAYLOAD_BYTES];
        buffer[..data.len()].copy_from_slice(data);

        let sample = self.publisher.loan_uninit()?;
        let sample = sample.write_payload(buffer);
        sample.send()?;
        Ok(())
    }
}
