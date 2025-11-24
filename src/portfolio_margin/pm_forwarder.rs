//! 账户 PM 数据的 Iceoryx 转发器
//!
//! - 发布到服务：`account_pubs/<exchange>_pm`
//! - 消息为原始 JSON（二进制）直接转发，固定上限 `PM_MAX_BYTES`
//! - 支持配置历史缓存与订阅者上限（来自 TOML 配置）
//!
//! 用法：
//! ```ignore
//! let mut fwd = PmForwarder::new("binance", Some(50), Some(10))?;
//! fwd.send_raw(&bytes);
//! ```
use anyhow::Result;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};

use crate::common::ipc_service_name::build_service_name;

const PM_MAX_BYTES: usize = 16384;

/// PM 转发器，内部持有 Iceoryx publisher
pub struct PmForwarder {
    publisher: Publisher<ipc::Service, [u8; PM_MAX_BYTES], ()>,
    sent: u64,
    dropped: u64,
    max_seen: usize,
}

impl PmForwarder {
    /// 创建 PM 转发器
    /// - `exchange` 交易所名（用于拼接服务名）
    /// - `hist` 历史缓存大小
    /// - `subs` 最大订阅者数
    pub fn new(exchange: &str, hist: Option<usize>, subs: Option<usize>) -> Result<Self> {
        info!("开始创建 PM forwarder，exchange: {}", exchange);

        let node_name = format!("account_monitor_{}_pm", exchange.replace("-", "_"));
        info!("IceOryx node 名称: '{}'", node_name);

        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;
        info!("IceOryx node 创建成功");

        let service_name = build_service_name(&format!("account_pubs/{}_pm", exchange));
        info!(
            "IceOryx service 名称: '{}'",
            service_name
        );

        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; PM_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(subs.unwrap_or(10))
            .history_size(hist.unwrap_or(50))
            .subscriber_max_buffer_size(1024)
            .open_or_create()?;

        let publisher = service.publisher_builder().create()?;
        info!("PM forwarder publisher 创建成功");

        Ok(Self {
            publisher,
            sent: 0,
            dropped: 0,
            max_seen: 0,
        })
    }

    /// 发送原始消息（二进制），超过上限则丢弃并计数
    pub fn send_raw(&mut self, msg: &[u8]) -> bool {
        if msg.len() > PM_MAX_BYTES {
            warn!(
                "PM message too large: {} > {} (dropping)",
                msg.len(),
                PM_MAX_BYTES
            );
            self.dropped += 1;
            return false;
        }

        let mut buffer = [0u8; PM_MAX_BYTES];
        buffer[..msg.len()].copy_from_slice(msg);
        match self.publisher.loan_uninit() {
            Ok(sample) => {
                let sample = sample.write_payload(buffer);
                if sample.send().is_ok() {
                    self.sent += 1;
                    if msg.len() > self.max_seen {
                        self.max_seen = msg.len();
                    }
                    true
                } else {
                    self.dropped += 1;
                    false
                }
            }
            Err(_) => {
                self.dropped += 1;
                false
            }
        }
    }

    /// 打印统计信息并清零窗口计数
    pub fn log_stats(&mut self) {
        info!(
            "PM forwarder stats: sent={}, dropped={}, max_seen={} bytes",
            self.sent, self.dropped, self.max_seen
        );
        self.sent = 0;
        self.dropped = 0;
        self.max_seen = 0;
    }
}
