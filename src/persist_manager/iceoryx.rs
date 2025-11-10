use anyhow::{Context, Result};
use bytes::Bytes;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;

use crate::common::iceoryx_publisher::SIGNAL_PAYLOAD;

const NODE_PREFIX: &str = "persist_signal_";

pub fn create_signal_record_subscriber(
    channel: &str,
) -> Result<Subscriber<ipc::Service, [u8; SIGNAL_PAYLOAD], ()>> {
    let node_name = format!("{}{}", NODE_PREFIX, sanitize_suffix(channel));
    let service_name = format!("signal_pubs/{}", channel);

    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()
        .with_context(|| format!("failed to create iceoryx node {}", node_name))?;

    let service = node
        .service_builder(&ServiceName::new(&service_name)?)
        .publish_subscribe::<[u8; SIGNAL_PAYLOAD]>()
        .max_publishers(1)
        .max_subscribers(32)
        .history_size(128)
        .subscriber_max_buffer_size(256)
        .open_or_create()
        .with_context(|| format!("failed to open service {}", service_name))?;

    let subscriber = service
        .subscriber_builder()
        .create()
        .with_context(|| format!("failed to create subscriber {}", service_name))?;

    Ok(subscriber)
}

pub fn trim_payload(payload: &[u8]) -> Bytes {
    Bytes::copy_from_slice(payload)
}

fn sanitize_suffix(raw: &str) -> std::borrow::Cow<'_, str> {
    if raw.chars().all(is_valid_node_char) {
        return std::borrow::Cow::Borrowed(raw);
    }
    let sanitized: String = raw
        .chars()
        .map(|c| if is_valid_node_char(c) { c } else { '_' })
        .collect();
    std::borrow::Cow::Owned(sanitized)
}

fn is_valid_node_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
} 
