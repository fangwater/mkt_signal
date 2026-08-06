use std::collections::HashSet;

use anyhow::{anyhow, Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use order_common::TradingVenue;
use persist_common::{OrderQueuePositionMsg, ORDER_QUEUE_POSITION_MAX_BYTES};
use runtime_common::fast_hash::{fast_hash_map, FastHashMap};
use runtime_common::ipc_service_name::build_service_name;

const ORDER_POSITION_MAX_PUBLISHERS: usize = 1;
const ORDER_POSITION_MAX_SUBSCRIBERS: usize = 10;
const ORDER_POSITION_HISTORY_SIZE: usize = 1024;

struct VenueSubscription {
    venue: TradingVenue,
    subscriber: Subscriber<ipc::Service, [u8; ORDER_QUEUE_POSITION_MAX_BYTES], ()>,
}

pub struct OrderQueuePositionChannel {
    subscriptions: Vec<VenueSubscription>,
    next_subscription: usize,
    active_positions: FastHashMap<(TradingVenue, i64), OrderQueuePositionMsg>,
}

impl OrderQueuePositionChannel {
    pub fn new(venues: impl IntoIterator<Item = TradingVenue>) -> Result<Self> {
        let account_id = std::env::var("IPC_NAMESPACE")
            .context("IPC_NAMESPACE is required for order-position tracking")?;
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
                "order-position tracking requires at least one venue"
            ));
        }

        Ok(Self {
            subscriptions,
            next_subscription: 0,
            active_positions: fast_hash_map(),
        })
    }

    pub fn active_position(
        &self,
        venue: TradingVenue,
        client_order_id: i64,
    ) -> Option<&OrderQueuePositionMsg> {
        self.active_positions.get(&(venue, client_order_id))
    }

    pub fn active_position_count(&self) -> usize {
        self.active_positions.len()
    }

    pub fn active_positions(&self) -> impl Iterator<Item = (TradingVenue, &OrderQueuePositionMsg)> {
        self.active_positions
            .iter()
            .map(|((venue, _), msg)| (*venue, msg))
    }

    fn apply_position_update(&mut self, venue: TradingVenue, msg: OrderQueuePositionMsg) {
        let key = (venue, msg.client_order_id);
        if msg.action.is_terminal() {
            self.active_positions.remove(&key);
        } else {
            self.active_positions.insert(key, msg);
        }
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
                let venue = self.subscriptions[index].venue;
                let received = {
                    let subscription = &self.subscriptions[index];
                    match subscription.subscriber.receive() {
                        Ok(Some(sample)) => {
                            Some(OrderQueuePositionMsg::from_bytes(sample.payload()))
                        }
                        Ok(None) => None,
                        Err(err) => {
                            warn!(
                                "pre_trade order-position receive failed: venue={} err={err}",
                                venue.data_pub_slug()
                            );
                            None
                        }
                    }
                };
                match received {
                    Some(Ok(msg)) => {
                        remaining -= 1;
                        had_sample = true;
                        received_in_round = true;
                        self.apply_position_update(venue, msg);
                        if remaining == 0 {
                            break;
                        }
                    }
                    Some(Err(err)) => {
                        remaining -= 1;
                        had_sample = true;
                        received_in_round = true;
                        warn!(
                            "pre_trade invalid order-position payload: venue={} err={err:#}",
                            venue.data_pub_slug()
                        );
                        if remaining == 0 {
                            break;
                        }
                    }
                    None => {}
                }
            }
            if !received_in_round {
                break;
            }
        }
        had_sample
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use persist_common::OrderQueuePositionAction;

    fn msg(
        action: OrderQueuePositionAction,
        client_order_id: i64,
        tlen: f64,
    ) -> OrderQueuePositionMsg {
        OrderQueuePositionMsg {
            action,
            create_tp: 100,
            update_tp: 200,
            local_tp: 300,
            client_order_id,
            tlen,
            backlen: 2.0,
            inpos: 1.0,
        }
    }

    #[test]
    fn active_position_updates_and_terminal_event_removes_it() {
        let mut channel = OrderQueuePositionChannel {
            subscriptions: Vec::new(),
            next_subscription: 0,
            active_positions: fast_hash_map(),
        };

        channel.apply_position_update(
            TradingVenue::BinanceFutures,
            msg(OrderQueuePositionAction::New, 42, 5.0),
        );
        channel.apply_position_update(
            TradingVenue::BinanceFutures,
            msg(OrderQueuePositionAction::PartiallyFilled, 42, 3.0),
        );

        assert_eq!(channel.active_position_count(), 1);
        assert_eq!(
            channel
                .active_position(TradingVenue::BinanceFutures, 42)
                .map(|position| position.tlen),
            Some(3.0)
        );

        channel.apply_position_update(
            TradingVenue::BinanceFutures,
            msg(OrderQueuePositionAction::Filled, 42, 0.0),
        );
        assert_eq!(channel.active_position_count(), 0);
    }

    #[test]
    fn active_positions_are_scoped_by_venue() {
        let mut channel = OrderQueuePositionChannel {
            subscriptions: Vec::new(),
            next_subscription: 0,
            active_positions: fast_hash_map(),
        };

        channel.apply_position_update(
            TradingVenue::BinanceFutures,
            msg(OrderQueuePositionAction::New, 42, 5.0),
        );
        channel.apply_position_update(
            TradingVenue::OkexFutures,
            msg(OrderQueuePositionAction::New, 42, 7.0),
        );

        assert_eq!(channel.active_position_count(), 2);
        assert_eq!(
            channel
                .active_position(TradingVenue::OkexFutures, 42)
                .map(|position| position.tlen),
            Some(7.0)
        );
    }
}
