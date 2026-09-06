use anyhow::{ensure, Context, Result};
use iceoryx2::port::{publisher::Publisher, subscriber::Subscriber};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use persist_common::rapidx_execution::ExecutionRecord;
use persist_common::rapidx_fact::{RapidXFact, ACK_BYTES, MAX_BYTES};
use persist_common::rapidx_statement::StatementRecord;
use runtime_common::ipc_service_name::build_service_name;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::time::{Duration, Instant};

const IN_FLIGHT_LIMIT: usize = 128;
const RETRY_DELAY: Duration = Duration::from_secs(1);

struct Pending {
    payload: [u8; MAX_BYTES],
    ack: [u8; ACK_BYTES],
    next_send: Instant,
}

/// The journal is the durable outbox. This bounded window only owns in-flight
/// copies; restart replays the journal and the receiver deduplicates by identity.
pub struct FactPublisher<R> {
    publisher: Publisher<ipc::Service, [u8; MAX_BYTES], ()>,
    subscriber: Subscriber<ipc::Service, [u8; ACK_BYTES], ()>,
    pending: VecDeque<Pending>,
    record_type: PhantomData<R>,
}

pub type ExecutionPublisher = FactPublisher<ExecutionRecord>;
pub type StatementPublisher = FactPublisher<StatementRecord>;

impl<R: RapidXFact> FactPublisher<R> {
    pub fn new() -> Result<Self> {
        let node = NodeBuilder::new()
            .name(&NodeName::new(&format!("{}_delivery", R::RECORD_CHANNEL))?)
            .create::<ipc::Service>()?;
        let records = node
            .service_builder(&ServiceName::new(&build_service_name(&format!(
                "persist_pubs/{}",
                R::RECORD_CHANNEL
            )))?)
            .publish_subscribe::<[u8; MAX_BYTES]>()
            .max_publishers(32)
            .max_subscribers(32)
            .history_size(128)
            .subscriber_max_buffer_size(256)
            .open_or_create()
            .context("open RapidX fact record service")?;
        let acks = node
            .service_builder(&ServiceName::new(&build_service_name(&format!(
                "persist_acks/{}",
                R::ACK_CHANNEL
            )))?)
            .publish_subscribe::<[u8; ACK_BYTES]>()
            .max_publishers(1)
            .max_subscribers(32)
            .history_size(128)
            .subscriber_max_buffer_size(256)
            .open_or_create()
            .context("open RapidX fact ACK service")?;
        Ok(Self {
            publisher: records.publisher_builder().create()?,
            subscriber: acks.subscriber_builder().create()?,
            pending: VecDeque::new(),
            record_type: PhantomData,
        })
    }

    pub fn has_capacity(&self) -> bool {
        self.pending.len() < IN_FLIGHT_LIMIT
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub fn enqueue(&mut self, record: &R) -> Result<()> {
        ensure!(self.has_capacity(), "RapidX fact in-flight window full");
        let ack = record.ack()?;
        if self.pending.iter().any(|item| item.ack == ack) {
            return Ok(());
        }
        self.pending.push_back(Pending {
            payload: record.to_ipc_payload()?,
            ack,
            next_send: Instant::now(),
        });
        Ok(())
    }

    pub fn poll(&mut self) -> Result<()> {
        for _ in 0..256 {
            let Some(sample) = self
                .subscriber
                .receive()
                .context("receive RapidX fact ACK")?
            else {
                break;
            };
            acknowledge(&mut self.pending, sample.payload());
        }
        send_due(&mut self.pending, Instant::now(), |payload| {
            let sample = self
                .publisher
                .loan_uninit()
                .context("loan RapidX fact sample")?;
            sample
                .write_payload(*payload)
                .send()
                .context("send RapidX fact")?;
            Ok(())
        })?;
        Ok(())
    }
}

fn send_due(
    pending: &mut VecDeque<Pending>,
    now: Instant,
    mut send: impl FnMut(&[u8; MAX_BYTES]) -> Result<()>,
) -> Result<usize> {
    let mut count = 0;
    // Limit work per tick so persistence catch-up cannot monopolize WS handling.
    for item in pending
        .iter_mut()
        .filter(|item| item.next_send <= now)
        .take(16)
    {
        send(&item.payload)?;
        item.next_send = now + RETRY_DELAY;
        count += 1;
    }
    Ok(count)
}

fn acknowledge(pending: &mut VecDeque<Pending>, ack: &[u8; ACK_BYTES]) {
    if let Some(index) = pending.iter().position(|item| &item.ack == ack) {
        pending.remove(index);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lost_ack_and_send_failure_keep_exact_record_for_retry() {
        let now = Instant::now();
        let mut pending = VecDeque::from([Pending {
            payload: [9; MAX_BYTES],
            ack: [7; ACK_BYTES],
            next_send: now,
        }]);
        assert!(send_due(&mut pending, now, |_| anyhow::bail!("send failed")).is_err());
        assert_eq!(pending[0].next_send, now);
        assert_eq!(
            send_due(&mut pending, now, |payload| {
                assert_eq!(payload, &[9; MAX_BYTES]);
                Ok(())
            })
            .unwrap(),
            1
        );
        assert_eq!(pending.len(), 1);
        assert_eq!(send_due(&mut pending, now, |_| unreachable!()).unwrap(), 0);
        assert_eq!(
            send_due(&mut pending, now + RETRY_DELAY, |payload| {
                assert_eq!(payload, &[9; MAX_BYTES]);
                Ok(())
            })
            .unwrap(),
            1
        );
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn per_tick_budget_allows_later_records_to_progress() {
        let now = Instant::now();
        let mut pending: VecDeque<_> = (0..32)
            .map(|id| Pending {
                payload: [id; MAX_BYTES],
                ack: [id; ACK_BYTES],
                next_send: now,
            })
            .collect();
        assert_eq!(send_due(&mut pending, now, |_| Ok(())).unwrap(), 16);
        let mut ids = Vec::new();
        assert_eq!(
            send_due(&mut pending, now, |payload| {
                ids.push(payload[0]);
                Ok(())
            })
            .unwrap(),
            16
        );
        assert_eq!(ids, (16..32).collect::<Vec<u8>>());
        assert_eq!(pending.len(), 32);
    }

    #[test]
    fn only_exact_content_ack_releases_in_flight_record() {
        let mut pending = VecDeque::from([Pending {
            payload: [0; MAX_BYTES],
            ack: [7; ACK_BYTES],
            next_send: Instant::now(),
        }]);
        let mut wrong_value = [7; ACK_BYTES];
        wrong_value[63] = 8;
        acknowledge(&mut pending, &wrong_value);
        assert_eq!(pending.len(), 1);
        let mut wrong_identity = [7; ACK_BYTES];
        wrong_identity[0] = 8;
        acknowledge(&mut pending, &wrong_identity);
        assert_eq!(pending.len(), 1);
        acknowledge(&mut pending, &[7; ACK_BYTES]);
        assert!(pending.is_empty());
        acknowledge(&mut pending, &[7; ACK_BYTES]);
        assert!(pending.is_empty());
    }
}
