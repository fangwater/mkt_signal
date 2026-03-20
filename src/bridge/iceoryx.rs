use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use iceoryx2::port::{publisher::Publisher, subscriber::Subscriber};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};

use crate::bridge::cfg::RouteEndpoint;

/// Supported fixed payload sizes for Iceoryx2 services.
///
/// Iceoryx2 ports require compile-time sizes, so we provide a small set of
/// variants that cover the project usage. Extend if new sizes appear.
pub const SUPPORTED_SIZES: &[usize] = &[64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384];

pub enum SubscriberEnum {
    Size64(Subscriber<ipc::Service, [u8; 64], ()>),
    Size128(Subscriber<ipc::Service, [u8; 128], ()>),
    Size256(Subscriber<ipc::Service, [u8; 256], ()>),
    Size512(Subscriber<ipc::Service, [u8; 512], ()>),
    Size1024(Subscriber<ipc::Service, [u8; 1024], ()>),
    Size2048(Subscriber<ipc::Service, [u8; 2048], ()>),
    Size4096(Subscriber<ipc::Service, [u8; 4096], ()>),
    Size8192(Subscriber<ipc::Service, [u8; 8192], ()>),
    Size16384(Subscriber<ipc::Service, [u8; 16384], ()>),
}

impl SubscriberEnum {
    pub fn new(
        node: &Node<ipc::Service>,
        service_name: &str,
        size: usize,
        endpoint: &RouteEndpoint,
    ) -> Result<Self> {
        if size >= 32_768 {
            panic!(
                "iceoryx payload size {} is too large for ipc_bridge (>=32768 not supported)",
                size
            );
        }
        let sub = match size {
            64 => Self::Size64(create_subscriber::<64>(node, service_name, endpoint)?),
            128 => Self::Size128(create_subscriber::<128>(node, service_name, endpoint)?),
            256 => Self::Size256(create_subscriber::<256>(node, service_name, endpoint)?),
            512 => Self::Size512(create_subscriber::<512>(node, service_name, endpoint)?),
            1024 => Self::Size1024(create_subscriber::<1024>(node, service_name, endpoint)?),
            2048 => Self::Size2048(create_subscriber::<2048>(node, service_name, endpoint)?),
            4096 => Self::Size4096(create_subscriber::<4096>(node, service_name, endpoint)?),
            8192 => Self::Size8192(create_subscriber::<8192>(node, service_name, endpoint)?),
            16384 => Self::Size16384(create_subscriber::<16384>(node, service_name, endpoint)?),
            _ => {
                return Err(anyhow!(
                    "unsupported iceoryx payload size {} (supported: {:?})",
                    size,
                    SUPPORTED_SIZES
                ))
            }
        };

        info!(
            "Iceoryx subscriber created: service='{}' size={}",
            service_name, size
        );
        Ok(sub)
    }

    pub fn receive_msg(&self) -> Result<Option<Bytes>> {
        match self {
            SubscriberEnum::Size64(sub) => receive_from_subscriber(sub),
            SubscriberEnum::Size128(sub) => receive_from_subscriber(sub),
            SubscriberEnum::Size256(sub) => receive_from_subscriber(sub),
            SubscriberEnum::Size512(sub) => receive_from_subscriber(sub),
            SubscriberEnum::Size1024(sub) => receive_from_subscriber(sub),
            SubscriberEnum::Size2048(sub) => receive_from_subscriber(sub),
            SubscriberEnum::Size4096(sub) => receive_from_subscriber(sub),
            SubscriberEnum::Size8192(sub) => receive_from_subscriber(sub),
            SubscriberEnum::Size16384(sub) => receive_from_subscriber(sub),
        }
    }
}

pub enum PublisherEnum {
    Size64(Publisher<ipc::Service, [u8; 64], ()>),
    Size128(Publisher<ipc::Service, [u8; 128], ()>),
    Size256(Publisher<ipc::Service, [u8; 256], ()>),
    Size512(Publisher<ipc::Service, [u8; 512], ()>),
    Size1024(Publisher<ipc::Service, [u8; 1024], ()>),
    Size2048(Publisher<ipc::Service, [u8; 2048], ()>),
    Size4096(Publisher<ipc::Service, [u8; 4096], ()>),
    Size8192(Publisher<ipc::Service, [u8; 8192], ()>),
    Size16384(Publisher<ipc::Service, [u8; 16384], ()>),
}

impl PublisherEnum {
    pub fn new(
        node: &Node<ipc::Service>,
        service_name: &str,
        size: usize,
        endpoint: &RouteEndpoint,
    ) -> Result<Self> {
        if size >= 32_768 {
            panic!(
                "iceoryx payload size {} is too large for ipc_bridge (>=32768 not supported)",
                size
            );
        }
        let pub_ = match size {
            64 => Self::Size64(create_publisher::<64>(node, service_name, endpoint)?),
            128 => Self::Size128(create_publisher::<128>(node, service_name, endpoint)?),
            256 => Self::Size256(create_publisher::<256>(node, service_name, endpoint)?),
            512 => Self::Size512(create_publisher::<512>(node, service_name, endpoint)?),
            1024 => Self::Size1024(create_publisher::<1024>(node, service_name, endpoint)?),
            2048 => Self::Size2048(create_publisher::<2048>(node, service_name, endpoint)?),
            4096 => Self::Size4096(create_publisher::<4096>(node, service_name, endpoint)?),
            8192 => Self::Size8192(create_publisher::<8192>(node, service_name, endpoint)?),
            16384 => Self::Size16384(create_publisher::<16384>(node, service_name, endpoint)?),
            _ => {
                return Err(anyhow!(
                    "unsupported iceoryx payload size {} (supported: {:?})",
                    size,
                    SUPPORTED_SIZES
                ))
            }
        };

        info!(
            "Iceoryx publisher created: service='{}' size={}",
            service_name, size
        );
        Ok(pub_)
    }

    pub fn publish(&self, data: &[u8]) -> Result<()> {
        match self {
            PublisherEnum::Size64(pub_) => publish_with::<64>(pub_, data),
            PublisherEnum::Size128(pub_) => publish_with::<128>(pub_, data),
            PublisherEnum::Size256(pub_) => publish_with::<256>(pub_, data),
            PublisherEnum::Size512(pub_) => publish_with::<512>(pub_, data),
            PublisherEnum::Size1024(pub_) => publish_with::<1024>(pub_, data),
            PublisherEnum::Size2048(pub_) => publish_with::<2048>(pub_, data),
            PublisherEnum::Size4096(pub_) => publish_with::<4096>(pub_, data),
            PublisherEnum::Size8192(pub_) => publish_with::<8192>(pub_, data),
            PublisherEnum::Size16384(pub_) => publish_with::<16384>(pub_, data),
        }
    }
}

fn create_subscriber<const SIZE: usize>(
    node: &Node<ipc::Service>,
    service_name: &str,
    endpoint: &RouteEndpoint,
) -> Result<Subscriber<ipc::Service, [u8; SIZE], ()>> {
    let mut builder = node
        .service_builder(&ServiceName::new(service_name)?)
        .publish_subscribe::<[u8; SIZE]>();
    if let Some(value) = endpoint.max_publishers {
        builder = builder.max_publishers(value);
    }
    if let Some(value) = endpoint.max_subscribers {
        builder = builder.max_subscribers(value);
    }
    if let Some(value) = endpoint.history_size {
        builder = builder.history_size(value);
    }
    if let Some(value) = endpoint.subscriber_max_buffer_size {
        builder = builder.subscriber_max_buffer_size(value);
    }

    let service = builder
        .open_or_create()
        .with_context(|| format!("failed to open_or_create service {}", service_name))?;
    service
        .subscriber_builder()
        .create()
        .with_context(|| format!("failed to create subscriber {}", service_name))
}

fn create_publisher<const SIZE: usize>(
    node: &Node<ipc::Service>,
    service_name: &str,
    endpoint: &RouteEndpoint,
) -> Result<Publisher<ipc::Service, [u8; SIZE], ()>> {
    let mut builder = node
        .service_builder(&ServiceName::new(service_name)?)
        .publish_subscribe::<[u8; SIZE]>();
    if let Some(value) = endpoint.max_publishers {
        builder = builder.max_publishers(value);
    }
    if let Some(value) = endpoint.max_subscribers {
        builder = builder.max_subscribers(value);
    }
    if let Some(value) = endpoint.history_size {
        builder = builder.history_size(value);
    }
    if let Some(value) = endpoint.subscriber_max_buffer_size {
        builder = builder.subscriber_max_buffer_size(value);
    }

    let service = builder
        .open_or_create()
        .with_context(|| format!("failed to open_or_create service {}", service_name))?;
    service
        .publisher_builder()
        .create()
        .with_context(|| format!("failed to create publisher {}", service_name))
}

fn receive_from_subscriber<const SIZE: usize>(
    subscriber: &Subscriber<ipc::Service, [u8; SIZE], ()>,
) -> Result<Option<Bytes>> {
    match subscriber.receive()? {
        Some(sample) => {
            let payload = sample.payload();
            if payload.iter().all(|&b| b == 0) {
                return Ok(None);
            }
            Ok(Some(Bytes::copy_from_slice(payload)))
        }
        None => Ok(None),
    }
}

fn publish_with<const SIZE: usize>(
    publisher: &Publisher<ipc::Service, [u8; SIZE], ()>,
    data: &[u8],
) -> Result<()> {
    if data.is_empty() {
        return Ok(());
    }
    if data.len() > SIZE {
        warn!(
            "bridge publisher truncating payload: len={} capacity={}",
            data.len(),
            SIZE
        );
    }

    let mut buf = [0u8; SIZE];
    let copy_len = data.len().min(SIZE);
    buf[..copy_len].copy_from_slice(&data[..copy_len]);

    let sample = publisher.loan_uninit()?;
    let sample = sample.write_payload(buf);
    sample.send()?;
    debug!(
        "bridge published {} bytes to iceoryx (capacity={})",
        copy_len, SIZE
    );
    Ok(())
}
