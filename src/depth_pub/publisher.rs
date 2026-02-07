//! Depth Message 发布模块
//!
//! 管理 IceOryx 订阅和发布

use anyhow::Result;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};

use super::depth_msg::{DepthMsg, DEPTH20_MAX_BYTES, DEPTH50_MAX_BYTES, DEPTH5_MAX_BYTES};

/// Depth Message Publisher
pub struct DepthMsgPublisher {
    node: Node<ipc::Service>,
    venue_slug: String,
    depth5_publisher: Option<Publisher<ipc::Service, [u8; DEPTH5_MAX_BYTES], ()>>,
    depth20_publisher: Option<Publisher<ipc::Service, [u8; DEPTH20_MAX_BYTES], ()>>,
    depth50_publisher: Option<Publisher<ipc::Service, [u8; DEPTH50_MAX_BYTES], ()>>,
    // 统计
    depth5_count: u64,
    depth20_count: u64,
    depth50_count: u64,
    dropped_count: u64,
}

impl DepthMsgPublisher {
    /// 创建新的发布器
    /// venue_slug: 例如 "binance-futures", "okex-margin"
    pub fn new(
        venue_slug: &str,
        enable_depth5: bool,
        enable_depth20: bool,
        enable_depth50: bool,
    ) -> Result<Self> {
        let node_name = format!("depth_msg_{}", venue_slug.replace('-', "_"));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        // 发布通道格式: depth_pubs/{venue}/depth5
        let depth5_publisher = if enable_depth5 {
            let service_name = format!("depth_pubs/{}/depth5", venue_slug);
            let service = node
                .service_builder(&ServiceName::new(&service_name)?)
                .publish_subscribe::<[u8; DEPTH5_MAX_BYTES]>()
                .max_publishers(1)
                .max_subscribers(10)
                .history_size(100)
                .open_or_create()?;
            Some(service.publisher_builder().create()?)
        } else {
            None
        };

        let depth20_publisher = if enable_depth20 {
            let service_name = format!("depth_pubs/{}/depth20", venue_slug);
            let service = node
                .service_builder(&ServiceName::new(&service_name)?)
                .publish_subscribe::<[u8; DEPTH20_MAX_BYTES]>()
                .max_publishers(1)
                .max_subscribers(10)
                .history_size(100)
                .open_or_create()?;
            Some(service.publisher_builder().create()?)
        } else {
            None
        };

        let depth50_publisher = if enable_depth50 {
            let service_name = format!("depth_pubs/{}/depth50", venue_slug);
            let service = node
                .service_builder(&ServiceName::new(&service_name)?)
                .publish_subscribe::<[u8; DEPTH50_MAX_BYTES]>()
                .max_publishers(1)
                .max_subscribers(10)
                .history_size(100)
                .open_or_create()?;
            Some(service.publisher_builder().create()?)
        } else {
            None
        };

        info!(
            "DepthMsgPublisher created for {}: depth5={}, depth20={}, depth50={}",
            venue_slug, enable_depth5, enable_depth20, enable_depth50
        );

        Ok(Self {
            node,
            venue_slug: venue_slug.to_string(),
            depth5_publisher,
            depth20_publisher,
            depth50_publisher,
            depth5_count: 0,
            depth20_count: 0,
            depth50_count: 0,
            dropped_count: 0,
        })
    }

    /// 获取 venue slug
    pub fn venue_slug(&self) -> &str {
        &self.venue_slug
    }

    /// 发布 Depth5 消息
    pub fn publish_depth5(&mut self, msg: &DepthMsg) -> bool {
        if let Some(ref publisher) = self.depth5_publisher {
            let bytes = msg.to_bytes();
            if self.send_with_publisher(publisher, &bytes, DEPTH5_MAX_BYTES) {
                self.depth5_count += 1;
                return true;
            }
            self.dropped_count += 1;
        }
        false
    }

    /// 发布 Depth20 消息
    pub fn publish_depth20(&mut self, msg: &DepthMsg) -> bool {
        if let Some(ref publisher) = self.depth20_publisher {
            let bytes = msg.to_bytes();
            if self.send_with_publisher(publisher, &bytes, DEPTH20_MAX_BYTES) {
                self.depth20_count += 1;
                return true;
            }
            self.dropped_count += 1;
        }
        false
    }

    /// 发布 Depth50 消息
    pub fn publish_depth50(&mut self, msg: &DepthMsg) -> bool {
        if let Some(ref publisher) = self.depth50_publisher {
            let bytes = msg.to_bytes();
            if self.send_with_publisher(publisher, &bytes, DEPTH50_MAX_BYTES) {
                self.depth50_count += 1;
                return true;
            }
            self.dropped_count += 1;
        }
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
                matches!(sample.send(), Ok(_))
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
            "DepthMsgPublisher[{}] stats: depth5={}, depth20={}, depth50={}, dropped={}",
            self.venue_slug,
            self.depth5_count,
            self.depth20_count,
            self.depth50_count,
            self.dropped_count
        );
        self.depth5_count = 0;
        self.depth20_count = 0;
        self.depth50_count = 0;
        self.dropped_count = 0;
    }
}
