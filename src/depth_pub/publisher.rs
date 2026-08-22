//! Depth Message 发布模块
//!
//! 管理 IceOryx 订阅和发布

use anyhow::Result;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};

use super::depth_msg::{DepthMsg, DEPTH25_MAX_BYTES};

/// Depth Message Publisher
pub struct DepthMsgPublisher {
    node: Node<ipc::Service>,
    venue_slug: String,
    depth25_publisher: Publisher<ipc::Service, [u8; DEPTH25_MAX_BYTES], ()>,
    // 统计
    depth25_count: u64,
    dropped_count: u64,
}

impl DepthMsgPublisher {
    /// 创建新的发布器
    /// venue_slug: 例如 "binance-futures", "okex-margin"
    pub fn new(venue_slug: &str) -> Result<Self> {
        let node_name = format!("depth_msg_{}", venue_slug.replace('-', "_"));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        // 发布通道格式: depth_pubs/{venue}/depth25
        let service_name = format!("depth_pubs/{}/depth25", venue_slug);
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; DEPTH25_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(10)
            .history_size(100)
            .open_or_create()?;
        let depth25_publisher = service.publisher_builder().create()?;

        info!("DepthMsgPublisher created for {}: depth25=true", venue_slug);

        Ok(Self {
            node,
            venue_slug: venue_slug.to_string(),
            depth25_publisher,
            depth25_count: 0,
            dropped_count: 0,
        })
    }

    /// 获取 venue slug
    pub fn venue_slug(&self) -> &str {
        &self.venue_slug
    }

    /// 发布 Depth25 消息
    pub fn publish_depth25(&mut self, msg: &DepthMsg) -> bool {
        let bytes = msg.to_bytes();
        if self.send_with_publisher(&self.depth25_publisher, &bytes, DEPTH25_MAX_BYTES) {
            self.depth25_count += 1;
            return true;
        }
        self.dropped_count += 1;
        false
    }

    fn send_with_publisher<const SIZE: usize>(
        &self,
        publisher: &Publisher<ipc::Service, [u8; SIZE], ()>,
        msg: &[u8],
        max_size: usize,
    ) -> bool {
        if msg.len() > max_size {
            warn!("Message size {} exceeds max size {}", msg.len(), max_size);
            return false;
        }

        let mut buffer = [0u8; SIZE];
        buffer[..msg.len()].copy_from_slice(msg);

        match publisher.loan_uninit() {
            Ok(sample) => {
                let sample = sample.write_payload(buffer);
                sample.send().is_ok()
            }
            Err(_) => false,
        }
    }

    /// 获取节点引用
    pub fn node(&self) -> &Node<ipc::Service> {
        &self.node
    }

    /// 日志统计
    pub fn log_stats(&mut self) {
        info!(
            "DepthMsgPublisher[{}] stats: depth25={}, dropped={}",
            self.venue_slug, self.depth25_count, self.dropped_count
        );
        self.depth25_count = 0;
        self.dropped_count = 0;
    }
}
