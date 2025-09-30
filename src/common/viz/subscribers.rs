use anyhow::Result;
use bytes::Bytes;
use iceoryx2::port::{publisher::Publisher, subscriber::Subscriber};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use std::time::Duration;

use crate::common::msg_parser::{get_msg_type, parse_index_price, parse_mark_price, parse_funding_rate, MktMsgType};
use crate::common::time_util::get_timestamp_us;
use crate::common::account_msg::get_event_type as get_account_event_type;

use super::state::SharedState;

pub const ACCOUNT_PAYLOAD: usize = 16_384;
pub const DERIVATIVES_PAYLOAD: usize = 128;

pub fn spawn_account_listener(service_name: String, label: Option<String>, state: SharedState) -> Result<()> {
    let label = label.unwrap_or_else(|| service_name.clone());
    tokio::task::spawn_local(async move {
        let node_name = "viz_account".to_string();
        let result = async move {
            let node = NodeBuilder::new()
                .name(&NodeName::new(&node_name)?)
                .create::<ipc::Service>()?;

            let service = node
                .service_builder(&ServiceName::new(&service_name)?)
                .publish_subscribe::<[u8; ACCOUNT_PAYLOAD]>()
                .open_or_create()?;
            let subscriber: Subscriber<ipc::Service, [u8; ACCOUNT_PAYLOAD], ()> = service.subscriber_builder().create()?;

            info!("viz account subscribed: service={} label={}", service.name(), label);
            loop {
                match subscriber.receive() {
                    Ok(Some(sample)) => {
                        let payload = trim_payload(sample.payload());
                        if payload.len() >= 8 {
                            let tp = get_account_event_type(&payload);
                            state.handle_account_event(tp, &payload[8..]); // 账户帧格式: [type:4][len:4][data]
                        }
                    }
                    Ok(None) => tokio::task::yield_now().await,
                    Err(err) => {
                        warn!("viz account receive error: {err}");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        };
        if let Err(err) = result.await { warn!("viz account listener exited: {err:?}"); }
    });
    Ok(())
}

pub fn spawn_derivatives_listener(service: String, state: SharedState) -> Result<()> {
    tokio::task::spawn_local(async move {
        let node_name = "viz_derivatives".to_string();
        let result = async move {
            let node = NodeBuilder::new().name(&NodeName::new(&node_name)?).create::<ipc::Service>()?;
            let service = node
                .service_builder(&ServiceName::new(&service)?)
                .publish_subscribe::<[u8; DERIVATIVES_PAYLOAD]>()
                .open_or_create()?;
            let subscriber: Subscriber<ipc::Service, [u8; DERIVATIVES_PAYLOAD], ()> = service.subscriber_builder().create()?;
            info!("viz derivatives subscribed: service={}", service.name());
            loop {
                match subscriber.receive() {
                    Ok(Some(sample)) => {
                        let payload = trim_payload(sample.payload());
                        if payload.is_empty() { continue; }
                        let Some(msg_type) = get_msg_type(&payload) else { continue; };
                        match msg_type {
                            MktMsgType::MarkPrice => match parse_mark_price(&payload) {
                                Ok(msg) => state.update_price_mark(&msg.symbol, msg.mark_price, msg.timestamp),
                                Err(err) => warn!("viz parse mark price failed: {err:?}"),
                            },
                            MktMsgType::IndexPrice => match parse_index_price(&payload) {
                                Ok(msg) => state.update_price_index(&msg.symbol, msg.index_price, msg.timestamp),
                                Err(err) => warn!("viz parse index price failed: {err:?}"),
                            },
                            MktMsgType::FundingRate => match parse_funding_rate(&payload) {
                                Ok(msg) => state.set_stream_funding(&msg.symbol, msg.funding_rate, msg.next_funding_time, msg.timestamp),
                                Err(err) => warn!("viz parse funding rate failed: {err:?}"),
                            },
                            _ => {}
                        }
                    }
                    Ok(None) => tokio::task::yield_now().await,
                    Err(err) => {
                        warn!("viz derivatives receive error: {err}");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        };
        if let Err(err) = result.await { warn!("viz derivatives listener exited: {err:?}"); }
    });
    Ok(())
}

fn trim_payload(payload: &[u8]) -> Bytes {
    Bytes::copy_from_slice(payload)
}
