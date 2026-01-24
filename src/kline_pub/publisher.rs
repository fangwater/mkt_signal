//! Kline Message 发布模块
//!
//! 管理 IceOryx 发布

use anyhow::Result;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::time::{Duration, Instant};

use crate::common::mkt_msg::KlineMsg;

const KLINE_MAX_BYTES: usize = 128;
const WARN_INTERVAL_SECS: u64 = 5;

/// Kline Message Publisher
pub struct KlineMsgPublisher {
    node: Node<ipc::Service>,
    venue_slug: String,
    channel_label: String,
    publisher: Publisher<ipc::Service, [u8; KLINE_MAX_BYTES], ()>,
    last_warn: Instant,
    // 统计
    publish_count: u64,
    dropped_count: u64,
}

impl KlineMsgPublisher {
    /// 创建新的发布器
    /// venue_slug: 例如 "binance-futures", "okex-margin"
    /// channel_label: 例如 "kline5s"
    pub fn new(venue_slug: &str, channel_label: &str) -> Result<Self> {
        let node_name = format!("kline_msg_{}", venue_slug.replace('-', "_"));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service_name = format!("kline_pubs/{}/{}", venue_slug, channel_label);
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; KLINE_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(10)
            .history_size(100)
            .subscriber_max_buffer_size(8192)
            .open_or_create()?;

        let publisher = service.publisher_builder().create()?;

        info!(
            "KlineMsgPublisher created for {}: channel={}",
            venue_slug, service_name
        );

        Ok(Self {
            node,
            venue_slug: venue_slug.to_string(),
            channel_label: channel_label.to_string(),
            publisher,
            last_warn: Instant::now() - Duration::from_secs(WARN_INTERVAL_SECS),
            publish_count: 0,
            dropped_count: 0,
        })
    }

    /// 发布 Kline 消息
    pub fn publish(&mut self, msg: &KlineMsg) -> bool {
        let bytes = msg.to_bytes();
        if self.send_with_publisher(&bytes, KLINE_MAX_BYTES) {
            self.publish_count += 1;
            return true;
        }
        self.dropped_count += 1;
        false
    }

    fn send_with_publisher(&mut self, msg: &[u8], max_size: usize) -> bool {
        if msg.len() > max_size {
            warn!(
                "Kline message size {} exceeds max size {}",
                msg.len(),
                max_size
            );
            return false;
        }

        let mut buffer = [0u8; KLINE_MAX_BYTES];
        buffer[..msg.len()].copy_from_slice(msg);

        match self.publisher.loan_uninit() {
            Ok(sample) => {
                let sample = sample.write_payload(buffer);
                match sample.send() {
                    Ok(_) => true,
                    Err(err) => {
                        self.warn_throttled("send", &err);
                        false
                    }
                }
            }
            Err(err) => {
                self.warn_throttled("loan_uninit", &err);
                false
            }
        }
    }

    /// 获取节点引用
    pub fn node(&self) -> &Node<ipc::Service> {
        &self.node
    }

    /// 日志统计
    pub fn log_stats(&mut self) {
        info!(
            "KlineMsgPublisher[{}] stats: channel={}, published={}, dropped={}",
            self.venue_slug, self.channel_label, self.publish_count, self.dropped_count
        );
        self.publish_count = 0;
        self.dropped_count = 0;
    }

    fn warn_throttled(&mut self, action: &str, err: &dyn std::fmt::Debug) {
        if self.last_warn.elapsed() >= Duration::from_secs(WARN_INTERVAL_SECS) {
            warn!("Kline publish {} failed: {:?}", action, err);
            self.last_warn = Instant::now();
        }
    }
}
