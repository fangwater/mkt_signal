use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::info;

pub struct SignalPublisher {
    publisher: Publisher<ipc::Service, [u8; 1024], ()>,
}

impl SignalPublisher {
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
            .publish_subscribe::<[u8; 1024]>()
            .open_or_create()?;

        let publisher = service.publisher_builder().create()?;

        info!(
            "Successfully created publisher for channel: {}",
            channel_name
        );

        Ok(Self { publisher })
    }

    pub fn publish(&self, data: &[u8]) -> anyhow::Result<()> {
        if data.len() > 1024 {
            anyhow::bail!("Data size exceeds 1024 bytes");
        }

        // Prepare a fixed-size buffer and copy the data
        let mut buffer = [0u8; 1024];
        buffer[..data.len()].copy_from_slice(data);

        // Send via loan + write_payload pattern (same as forwarder)
        let sample = self.publisher.loan_uninit()?;
        let sample = sample.write_payload(buffer);
        sample.send()?;

        // 生产环境去除发布详情 DEBUG 日志，避免刷屏

        Ok(())
    }
}
