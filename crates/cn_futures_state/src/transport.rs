use crate::{service_name, Snapshot, BUFFER, PAYLOAD};
use anyhow::{ensure, Context, Result};
use bincode::Options;
use iceoryx2::{
    port::{publisher::Publisher, subscriber::Subscriber},
    prelude::*,
    service::ipc,
};

pub struct StatePublisher {
    _node: Node<ipc::Service>,
    publisher: Publisher<ipc::Service, [u8; PAYLOAD], ()>,
}
impl StatePublisher {
    pub fn new(namespace: &str) -> Result<Self> {
        let node = NodeBuilder::new().create::<ipc::Service>()?;
        let name = service_name(namespace)?;
        let service = node
            .service_builder(&ServiceName::new(&name)?)
            .publish_subscribe::<[u8; PAYLOAD]>()
            .max_publishers(1)
            .max_subscribers(16)
            .max_nodes(32)
            .history_size(1)
            .subscriber_max_buffer_size(BUFFER)
            .enable_safe_overflow(true)
            .open_or_create()?;
        let publisher = service.publisher_builder().create()?;
        Ok(Self {
            _node: node,
            publisher,
        })
    }
    pub fn publish(&self, snapshot: &Snapshot) -> Result<()> {
        snapshot.validate()?;
        let encoded = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit((PAYLOAD - 4) as u64)
            .serialize(snapshot)
            .context("encode native positions")?;
        ensure!(
            encoded.len() + 4 <= PAYLOAD,
            "native position payload exceeds IPC capacity"
        );
        let mut sample = self.publisher.loan_uninit()?;
        // Initialize the whole fixed payload: no uninitialized tail crosses IPC.
        let slot = sample.payload_mut();
        unsafe {
            std::ptr::write_bytes(slot.as_mut_ptr().cast::<u8>(), 0, PAYLOAD);
        }
        let bytes = unsafe { &mut *slot.as_mut_ptr() };
        bytes[..4].copy_from_slice(&(encoded.len() as u32).to_le_bytes());
        bytes[4..4 + encoded.len()].copy_from_slice(&encoded);
        unsafe { sample.assume_init() }.send()?;
        Ok(())
    }
}
pub struct StateSubscriber {
    _node: Node<ipc::Service>,
    subscriber: Subscriber<ipc::Service, [u8; PAYLOAD], ()>,
}
impl StateSubscriber {
    pub fn open(namespace: &str) -> Result<Self> {
        let node = NodeBuilder::new().create::<ipc::Service>()?;
        let name = service_name(namespace)?;
        let service = node
            .service_builder(&ServiceName::new(&name)?)
            .publish_subscribe::<[u8; PAYLOAD]>()
            .open()?;
        let subscriber = service.subscriber_builder().buffer_size(BUFFER).create()?;
        Ok(Self {
            _node: node,
            subscriber,
        })
    }
    pub fn latest(&self) -> Result<Option<Snapshot>> {
        let mut latest = None;
        for _ in 0..BUFFER {
            let Some(sample) = self.subscriber.receive()? else {
                break;
            };
            latest = Some(decode(sample.payload())?);
        }
        Ok(latest)
    }
}
pub fn decode(bytes: &[u8]) -> Result<Snapshot> {
    ensure!(bytes.len() >= 4, "short native state frame");
    let len = u32::from_le_bytes(bytes[..4].try_into()?) as usize;
    ensure!(
        len > 0 && len <= PAYLOAD - 4 && len <= bytes.len() - 4,
        "invalid native state frame length"
    );
    let snapshot: Snapshot = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit((PAYLOAD - 4) as u64)
        .reject_trailing_bytes()
        .deserialize(&bytes[4..4 + len])
        .context("decode native positions")?;
    snapshot.validate()?;
    Ok(snapshot)
}
