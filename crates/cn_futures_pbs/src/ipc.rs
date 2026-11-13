use crate::msg::{MarketDataMsg, SOURCE_RESET, VENUES};
use anyhow::{ensure, Context, Result};
use iceoryx2::{port::publisher::Publisher, prelude::*, service::ipc};
use std::time::Duration;

pub fn validate_root(root: &str) -> Result<()> {
    ensure!(
        !root.is_empty()
            && root.len() <= 100
            && root.split('/').all(|p| !p.is_empty()
                && p.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))),
        "invalid native IPC service_root"
    );
    Ok(())
}
pub fn service_name(root: &str, venue: &str) -> Result<String> {
    validate_root(root)?;
    ensure!(VENUES.contains(&venue), "invalid native venue");
    Ok(format!("{root}/{venue}/l2"))
}
pub struct Publishers {
    node: Node<ipc::Service>,
    publishers: Vec<Publisher<ipc::Service, MarketDataMsg, ()>>,
    epoch: u64,
    sequence: [u64; 6],
}
impl Publishers {
    pub fn new(root: &str) -> Result<Self> {
        validate_root(root)?;
        let node = NodeBuilder::new().create::<ipc::Service>()?;
        let mut publishers = Vec::with_capacity(6);
        for venue in VENUES {
            let name = service_name(root, venue)?;
            let service = node
                .service_builder(&ServiceName::new(&name)?)
                .publish_subscribe::<MarketDataMsg>()
                .max_publishers(1)
                .max_subscribers(16)
                .max_nodes(64)
                .history_size(1)
                .subscriber_max_buffer_size(2048)
                .open_or_create()
                .with_context(|| format!("open native L2 IPC {name}"))?;
            publishers.push(service.publisher_builder().create()?);
        }
        Ok(Self {
            node,
            publishers,
            epoch: crate::now_us(),
            sequence: [0; 6],
        })
    }
    pub fn running(&self) -> bool {
        self.node.wait(Duration::ZERO).is_ok()
    }
    pub fn publish(&mut self, mut msg: MarketDataMsg) -> Result<()> {
        msg.publisher_epoch = self.epoch;
        if msg.flags & SOURCE_RESET != 0 {
            for (index, venue) in VENUES.into_iter().enumerate() {
                msg.set_venue(venue)?;
                self.sequence[index] = self.sequence[index]
                    .checked_add(1)
                    .context("IPC sequence exhausted")?;
                msg.sequence = self.sequence[index];
                self.publishers[index]
                    .loan_uninit()?
                    .write_payload(msg)
                    .send()?;
            }
        } else {
            let index = VENUES
                .iter()
                .position(|v| v.as_bytes() == msg.venue())
                .context("unknown IPC message venue")?;
            self.sequence[index] = self.sequence[index]
                .checked_add(1)
                .context("IPC sequence exhausted")?;
            msg.sequence = self.sequence[index];
            self.publishers[index]
                .loan_uninit()?
                .write_payload(msg)
                .send()?;
        }
        Ok(())
    }
}
