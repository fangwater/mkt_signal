use std::sync::Arc;

use anyhow::{Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, warn};
use order_common::{
    BinanceUmNewAckTraceMsg, BINANCE_UM_NEW_ACK_TRACE_PAYLOAD_LEN, BINANCE_UM_NEW_ACK_TRACE_SERVICE,
};

use crate::polling::{PollStats, MAX_DRAIN_PER_CHANNEL};
use crate::runtime_common::build_service_name;
use crate::storage::RocksDbStore;

const NODE_NAME: &str = "persist_binance_um_new_ack_trace";

pub(crate) const CF_BINANCE_UM_NEW_ACK_TRACE: &str = "binance_um_new_ack_trace";

pub fn required_column_families() -> &'static [&'static str] {
    &[CF_BINANCE_UM_NEW_ACK_TRACE]
}

pub struct BinanceUmNewAckTracePersistor {
    subscriber: Subscriber<ipc::Service, [u8; BINANCE_UM_NEW_ACK_TRACE_PAYLOAD_LEN], ()>,
    store: Arc<RocksDbStore>,
}

impl BinanceUmNewAckTracePersistor {
    pub fn new(store: Arc<RocksDbStore>) -> Result<Self> {
        let node = NodeBuilder::new()
            .name(&NodeName::new(NODE_NAME)?)
            .create::<ipc::Service>()
            .with_context(|| format!("failed to create iceoryx node {}", NODE_NAME))?;
        let service_name = build_service_name(BINANCE_UM_NEW_ACK_TRACE_SERVICE);
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; BINANCE_UM_NEW_ACK_TRACE_PAYLOAD_LEN]>()
            .max_publishers(1)
            .max_subscribers(32)
            .history_size(128)
            .subscriber_max_buffer_size(8192)
            .open_or_create()
            .with_context(|| format!("failed to open service {}", service_name))?;
        let subscriber = service
            .subscriber_builder()
            .create()
            .with_context(|| format!("failed to create subscriber {}", service_name))?;
        Ok(Self { subscriber, store })
    }

    pub(crate) fn poll_available(&self) -> PollStats {
        let mut stats = PollStats::default();
        for _ in 0..MAX_DRAIN_PER_CHANNEL {
            match self.subscriber.receive() {
                Ok(Some(sample)) => {
                    stats.record_received();
                    let payload = sample.payload();
                    let Some(msg) = BinanceUmNewAckTraceMsg::from_bytes(payload.as_slice()) else {
                        warn!(
                            "binance um new ack trace payload invalid len={}",
                            payload.len()
                        );
                        continue;
                    };
                    let key = binance_um_new_ack_trace_key(&msg);
                    debug!(
                        "persist binance um new ack trace ack_recv_time_us={} client_order_id={} endpoint_id={} route_group_id={} rtt_us={}",
                        msg.ack_recv_time_us,
                        msg.client_order_id,
                        msg.endpoint_id,
                        msg.route_group_id,
                        msg.rtt_us
                    );
                    if let Err(err) =
                        self.store
                            .put(CF_BINANCE_UM_NEW_ACK_TRACE, &key, payload.as_slice())
                    {
                        warn!(
                            "persist binance um new ack trace failed client_order_id={} endpoint_id={} err={err:#}",
                            msg.client_order_id, msg.endpoint_id
                        );
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    warn!("binance um new ack trace receive error: {err}");
                    stats.record_error();
                    break;
                }
            }
        }
        stats
    }
}

fn binance_um_new_ack_trace_key(msg: &BinanceUmNewAckTraceMsg) -> [u8; 32] {
    let mut key = [0_u8; 32];
    key[0..8].copy_from_slice(&msg.ack_recv_time_us.to_be_bytes());
    key[8..16].copy_from_slice(&msg.client_order_id.to_be_bytes());
    key[16..20].copy_from_slice(&msg.endpoint_id.to_be_bytes());
    key[20..24].copy_from_slice(&msg.route_group_id.to_be_bytes());
    key[24..32].copy_from_slice(&msg.transport_id.to_be_bytes());
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_orders_by_ack_time_first() {
        let mut early = sample_msg();
        early.ack_recv_time_us = 10;
        let mut late = sample_msg();
        late.ack_recv_time_us = 11;

        assert!(binance_um_new_ack_trace_key(&early) < binance_um_new_ack_trace_key(&late));
    }

    fn sample_msg() -> BinanceUmNewAckTraceMsg {
        BinanceUmNewAckTraceMsg::new(
            2,
            1,
            1001,
            2002,
            1,
            2,
            3,
            4,
            1,
            "127.0.0.1".parse().unwrap(),
            None,
        )
    }
}
