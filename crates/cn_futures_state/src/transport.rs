//! Exact-length bincode snapshots over an iceoryx2 `[u8]` service. The
//! publisher copies only the encoded bytes; no fixed payload is zero-filled.
use crate::{service_name, Snapshot, BUFFER, MAX_SNAPSHOT_BYTES};
use anyhow::{ensure, Context, Result};
use bincode::Options;
use iceoryx2::{
    port::{publisher::Publisher, subscriber::Subscriber},
    prelude::*,
    service::ipc,
};

/// A handful of contracts fits; growth happens only for unusually large books.
const INITIAL_SNAPSHOT_BYTES: usize = 16 * 1024;

fn codec() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_SNAPSHOT_BYTES as u64)
}

fn open(
    node: &Node<ipc::Service>,
    name: &str,
) -> Result<iceoryx2::service::port_factory::publish_subscribe::PortFactory<ipc::Service, [u8], ()>>
{
    Ok(node
        .service_builder(&ServiceName::new(name)?)
        .publish_subscribe::<[u8]>()
        .max_publishers(1)
        .max_subscribers(16)
        .max_nodes(32)
        .history_size(1)
        .subscriber_max_buffer_size(BUFFER)
        .enable_safe_overflow(true)
        .open_or_create()?)
}

pub struct StatePublisher {
    _node: Node<ipc::Service>,
    publisher: Publisher<ipc::Service, [u8], ()>,
    /// Reused encode buffer: steady-state publishing does not allocate.
    encoded: Vec<u8>,
}
impl StatePublisher {
    pub fn new(namespace: &str) -> Result<Self> {
        let node = NodeBuilder::new().create::<ipc::Service>()?;
        let publisher = open(&node, &service_name(namespace)?)?
            .publisher_builder()
            .initial_max_slice_len(INITIAL_SNAPSHOT_BYTES)
            .allocation_strategy(AllocationStrategy::PowerOfTwo)
            .max_loaned_samples(1)
            .create()?;
        Ok(Self {
            _node: node,
            publisher,
            encoded: Vec::with_capacity(INITIAL_SNAPSHOT_BYTES),
        })
    }
    /// Validation (which allocates for duplicate checks) runs in `decode` on the
    /// subscriber; the business loop only encodes and copies.
    pub fn publish(&mut self, snapshot: &Snapshot) -> Result<()> {
        self.encoded.clear();
        codec()
            .serialize_into(&mut self.encoded, snapshot)
            .context("encode native positions")?;
        ensure!(!self.encoded.is_empty(), "empty native position frame");
        let sample = self.publisher.loan_slice_uninit(self.encoded.len())?;
        sample.write_from_slice(&self.encoded).send()?;
        Ok(())
    }
}
pub struct StateSubscriber {
    _node: Node<ipc::Service>,
    subscriber: Subscriber<ipc::Service, [u8], ()>,
}
impl StateSubscriber {
    pub fn open(namespace: &str) -> Result<Self> {
        let node = NodeBuilder::new().create::<ipc::Service>()?;
        let subscriber = open(&node, &service_name(namespace)?)?
            .subscriber_builder()
            .buffer_size(BUFFER)
            .create()?;
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
    let snapshot: Snapshot = codec()
        .reject_trailing_bytes()
        .deserialize(bytes)
        .context("decode native positions")?;
    snapshot.validate()?;
    Ok(snapshot)
}
