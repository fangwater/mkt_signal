use crate::{
    runtime::Sink,
    wire::{Event, Header, EVENT_BYTES, HEADER_BYTES, MAX_CHUNK_BYTES},
};
use anyhow::{ensure, Result};
use iceoryx2::{port::publisher::Publisher, prelude::*, service::ipc};

/// Exact-length shared-memory slices, initialized once directly in the loan.
/// No fixed-size 64KiB zero-fill and no intermediate encoded Vec.
pub struct IpcSink {
    publisher: Publisher<ipc::Service, [u8], ()>,
    _node: Node<ipc::Service>,
}
impl IpcSink {
    pub fn new(service: &str) -> Result<Self> {
        Self::new_with_config(service, iceoryx2::config::Config::global_config())
    }
    pub fn new_with_config(service: &str, config: &iceoryx2::config::Config) -> Result<Self> {
        let node = NodeBuilder::new().config(config).create::<ipc::Service>()?;
        let factory = node
            .service_builder(&ServiceName::new(service)?)
            .publish_subscribe::<[u8]>()
            .max_publishers(1)
            .max_subscribers(32)
            .max_nodes(64)
            .history_size(0)
            .subscriber_max_buffer_size(1024)
            .enable_safe_overflow(true)
            .open_or_create()?;
        let publisher = factory
            .publisher_builder()
            .initial_max_slice_len(MAX_CHUNK_BYTES)
            .max_loaned_samples(2)
            .create()?;
        Ok(Self {
            publisher,
            _node: node,
        })
    }
}
impl Sink for IpcSink {
    fn publish(&mut self, header: &Header, events: &[Event]) -> Result<()> {
        let len = HEADER_BYTES + events.len() * EVENT_BYTES;
        ensure!(len <= MAX_CHUNK_BYTES, "L4 IPC payload bound exceeded");
        let mut sample = self.publisher.loan_slice_uninit(len)?;
        header.encode_uninit(events, sample.payload_mut())?;
        // encode_uninit initialized every element of this exact-length loan.
        let sample = unsafe { sample.assume_init() };
        sample.send()?;
        Ok(())
    }
}
