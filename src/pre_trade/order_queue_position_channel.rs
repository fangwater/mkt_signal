use std::collections::HashSet;

use anyhow::{anyhow, Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use order_common::TradingVenue;
use persist_common::OrderQueuePositionRecord;
use runtime_common::ipc_service_name::build_service_name;
use runtime_common::time_util::get_timestamp_us;

use crate::depth_pub::order_queue_msg::{OrderQueuePositionMsg, ORDER_QUEUE_POSITION_MAX_BYTES};
use crate::pre_trade::PersistChannel;

const ORDER_POSITION_MAX_PUBLISHERS: usize = 1;
const ORDER_POSITION_MAX_SUBSCRIBERS: usize = 10;
const ORDER_POSITION_HISTORY_SIZE: usize = 1024;

struct VenueSubscription {
    venue: TradingVenue,
    subscriber: Subscriber<ipc::Service, [u8; ORDER_QUEUE_POSITION_MAX_BYTES], ()>,
}

pub struct OrderQueuePositionChannel {
    account_id: String,
    subscriptions: Vec<VenueSubscription>,
    next_subscription: usize,
}

impl OrderQueuePositionChannel {
    pub fn new(venues: impl IntoIterator<Item = TradingVenue>) -> Result<Self> {
        let account_id = std::env::var("IPC_NAMESPACE")
            .context("IPC_NAMESPACE is required for order-position persistence")?;
        let node = NodeBuilder::new()
            .name(&NodeName::new("pre_trade_order_position")?)
            .create::<ipc::Service>()
            .context("create pre-trade order-position iceoryx node")?;

        let mut seen = HashSet::new();
        let mut subscriptions = Vec::new();
        for venue in venues {
            if !seen.insert(venue) {
                continue;
            }
            let service_name =
                build_service_name(&format!("order_pos_pub/{}", venue.data_pub_slug()));
            let service = node
                .service_builder(&ServiceName::new(&service_name)?)
                .publish_subscribe::<[u8; ORDER_QUEUE_POSITION_MAX_BYTES]>()
                .max_publishers(ORDER_POSITION_MAX_PUBLISHERS)
                .max_subscribers(ORDER_POSITION_MAX_SUBSCRIBERS)
                .history_size(ORDER_POSITION_HISTORY_SIZE)
                .open_or_create()
                .with_context(|| format!("open order-position service {service_name}"))?;
            let subscriber = service
                .subscriber_builder()
                .create()
                .with_context(|| format!("create order-position subscriber {service_name}"))?;
            info!(
                "pre_trade subscribed order-position lifecycle: account_id={} venue={} service={}",
                account_id,
                venue.data_pub_slug(),
                service_name
            );
            subscriptions.push(VenueSubscription { venue, subscriber });
        }

        if subscriptions.is_empty() {
            return Err(anyhow!(
                "order-position persistence requires at least one venue"
            ));
        }

        Ok(Self {
            account_id,
            subscriptions,
            next_subscription: 0,
        })
    }

    pub fn drain_pending_limit(&mut self, limit: usize) -> bool {
        if limit == 0 || self.subscriptions.is_empty() {
            return false;
        }

        let mut remaining = limit;
        let mut had_sample = false;
        while remaining > 0 {
            let mut received_in_round = false;
            for _ in 0..self.subscriptions.len() {
                let index = self.next_subscription;
                self.next_subscription = (self.next_subscription + 1) % self.subscriptions.len();
                let subscription = &self.subscriptions[index];
                match subscription.subscriber.receive() {
                    Ok(Some(sample)) => {
                        remaining -= 1;
                        had_sample = true;
                        received_in_round = true;
                        match OrderQueuePositionMsg::from_bytes(sample.payload()) {
                            Ok(msg) => {
                                let record = OrderQueuePositionRecord {
                                    recv_ts_us: get_timestamp_us(),
                                    account_id: self.account_id.clone(),
                                    venue: subscription.venue.to_u8(),
                                    action: msg.action,
                                    create_tp: msg.create_tp,
                                    update_tp: msg.update_tp,
                                    local_tp: msg.local_tp,
                                    client_order_id: msg.client_order_id,
                                    tlen: msg.tlen,
                                    backlen: msg.backlen,
                                    inpos: msg.inpos,
                                };
                                PersistChannel::with(|channel| {
                                    channel.publish_order_queue_position(&record)
                                });
                            }
                            Err(err) => warn!(
                                "pre_trade invalid order-position payload: venue={} err={err:#}",
                                subscription.venue.data_pub_slug()
                            ),
                        }
                        if remaining == 0 {
                            break;
                        }
                    }
                    Ok(None) => {}
                    Err(err) => warn!(
                        "pre_trade order-position receive failed: venue={} err={err}",
                        subscription.venue.data_pub_slug()
                    ),
                }
            }
            if !received_in_round {
                break;
            }
        }
        had_sample
    }
}
