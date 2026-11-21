//! Exec → persist_manager service `<IPC_NAMESPACE>/persist_pubs/cn_native`.
//! One record per sample, published from the business loop like the crypto
//! `PersistChannel`: loan, copy, send. No acknowledgement path exists.
use anyhow::{ensure, Context, Result};
use iceoryx2::{
    port::{publisher::Publisher as Port, subscriber::Subscriber as Reader},
    prelude::*,
    service::ipc,
};

pub const CHANNEL: &str = "persist_pubs/cn_native";
/// Trading records fit without growth; only recovery snapshots may grow the
/// segment, by powers of two, while trading is still blocked.
const INITIAL_RECORD_BYTES: usize = 256 * 1024;
pub const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;
/// Retained for a persist_manager that (re)attaches after Exec has published.
const HISTORY: usize = 4096;
const SUBSCRIBER_BUFFER: usize = 4096;

pub fn service_name() -> Result<String> {
    let namespace = std::env::var("IPC_NAMESPACE")
        .context("IPC_NAMESPACE must be set to isolate iceoryx services")?;
    ensure!(!namespace.is_empty(), "IPC_NAMESPACE must not be empty");
    Ok(format!("{namespace}/{CHANNEL}"))
}

fn open(
    node: &Node<ipc::Service>,
    name: &str,
) -> Result<iceoryx2::service::port_factory::publish_subscribe::PortFactory<ipc::Service, [u8], ()>>
{
    node.service_builder(&ServiceName::new(name)?)
        .publish_subscribe::<[u8]>()
        .max_publishers(1)
        .max_subscribers(2)
        .max_nodes(8)
        .history_size(HISTORY)
        .subscriber_max_buffer_size(SUBSCRIBER_BUFFER)
        .enable_safe_overflow(true)
        .open_or_create()
        .with_context(|| format!("open native persistence service {name}"))
}

pub struct Publisher {
    port: Port<ipc::Service, [u8], ()>,
    _node: Node<ipc::Service>,
}

impl Publisher {
    pub fn new() -> Result<Self> {
        let name = service_name()?;
        let node = NodeBuilder::new().create::<ipc::Service>()?;
        let port = open(&node, &name)?
            .publisher_builder()
            .initial_max_slice_len(INITIAL_RECORD_BYTES)
            .allocation_strategy(AllocationStrategy::PowerOfTwo)
            .max_loaned_samples(1)
            .create()
            .with_context(|| format!("create native persistence publisher {name}"))?;
        Ok(Self { port, _node: node })
    }

    pub fn publish(&self, record: &[u8]) -> Result<()> {
        ensure!(
            !record.is_empty() && record.len() <= MAX_RECORD_BYTES,
            "native persistence record exceeds {MAX_RECORD_BYTES} bytes"
        );
        let sample = self.port.loan_slice_uninit(record.len())?;
        sample.write_from_slice(record).send()?;
        Ok(())
    }
}

pub struct Subscriber {
    reader: Reader<ipc::Service, [u8], ()>,
    _node: Node<ipc::Service>,
}

impl Subscriber {
    pub fn new() -> Result<Self> {
        let name = service_name()?;
        let node = NodeBuilder::new().create::<ipc::Service>()?;
        let reader = open(&node, &name)?
            .subscriber_builder()
            .buffer_size(SUBSCRIBER_BUFFER)
            .create()
            .with_context(|| format!("create native persistence subscriber {name}"))?;
        Ok(Self {
            reader,
            _node: node,
        })
    }

    /// Hands the next queued record to `read`; `Ok(false)` when none is queued.
    pub fn receive(&self, read: impl FnOnce(&[u8])) -> Result<bool> {
        let Some(sample) = self.reader.receive()? else {
            return Ok(false);
        };
        read(sample.payload());
        Ok(true)
    }
}
