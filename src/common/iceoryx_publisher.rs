use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::info;

pub const SIGNAL_PAYLOAD: usize = 4_096;
pub const RESAMPLE_PAYLOAD: usize = 32 * 1024;
pub const BINANCE_MARGIN_UPDATE_PAYLOAD: usize = SIGNAL_PAYLOAD;
pub const BINANCE_UM_UPDATE_PAYLOAD: usize = SIGNAL_PAYLOAD;

pub struct GenericPublisher<const PAYLOAD: usize> {
    publisher: Publisher<ipc::Service, [u8; PAYLOAD], ()>,
}

impl<const PAYLOAD: usize> GenericPublisher<PAYLOAD> {
    pub fn new(channel_name: &str) -> anyhow::Result<Self> {
        // Use signal namespace for services
        let full_service = format!("signal_pubs/{}", channel_name);
        let service_name = ServiceName::new(&full_service)?;

        info!("Creating IceOryx publisher for channel: {}", channel_name);

        // Use channel name directly as node name, per request
        let node = NodeBuilder::new()
            .name(&NodeName::new(channel_name)?)
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

        info!(
            "Successfully created publisher for channel: {}",
            channel_name
        );

        Ok(Self { publisher })
    }

    pub fn publish(&self, data: &[u8]) -> anyhow::Result<()> {
        if data.len() > PAYLOAD {
            anyhow::bail!("Data size exceeds {} bytes", PAYLOAD);
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
