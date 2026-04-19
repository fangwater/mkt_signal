//! 账户 PM 数据的 Iceoryx 转发器
//!
//! - 发布到服务：`account_pubs/<exchange>_pm`
//! - 消息为原始 JSON（二进制）直接转发，固定上限 `PM_MAX_BYTES`
//! - 订阅者上限固定 4，历史缓存固定 2048 条
//!
//! 用法：
//! ```ignore
//! let mut fwd = PmForwarder::new("binance")?;
//! fwd.send_raw(&bytes);
//! ```
use anyhow::{anyhow, Result};
use iceoryx2::config::Config;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};

use crate::common::ipc_service_name::build_service_name;

pub const PM_MAX_BYTES: usize = 16384;
pub const PM_HISTORY_SIZE: usize = 4096;
pub const PM_MAX_SUBSCRIBERS: usize = 4;
pub const PM_SUBSCRIBER_MAX_BUFFER_SIZE: usize = 4096;

/// PM 转发器，内部持有 Iceoryx publisher
pub struct PmForwarder {
    publisher: Publisher<ipc::Service, [u8; PM_MAX_BYTES], ()>,
    sent: u64,
    dropped: u64,
    max_seen: usize,
}

fn is_corrupted_service_err(err_text: &str) -> bool {
    err_text.contains("ServiceInCorruptedState")
}

fn is_publisher_capacity_err(err_text: &str) -> bool {
    err_text.contains("ExceedsMaxSupportedPublishers")
}

impl PmForwarder {
    /// 创建 PM 转发器
    /// - `exchange` 交易所名（用于拼接服务名）
    pub fn new(exchange: &str) -> Result<Self> {
        info!("开始创建 PM forwarder，exchange: {}", exchange);
        info!(
            "PM forwarder 历史缓存固定为 {} 条，最大订阅者固定为 {}（不再读取配置）",
            PM_HISTORY_SIZE, PM_MAX_SUBSCRIBERS
        );

        // 构造 service 名称（会检查 IPC_NAMESPACE 环境变量，未设置会 panic）
        let service_name = build_service_name(&format!("account_pubs/{}_pm", exchange));
        info!("IceOryx service 名称: '{}'", service_name);

        // Node 名称使用简单标识符（NodeName 可能不支持斜杠）
        let node_name = format!("account_monitor_{}_pm", exchange.replace("-", "_"));
        info!("准备创建 IceOryx node，node 名称: '{}'", node_name);

        let node = match NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()
        {
            Ok(n) => {
                info!("IceOryx node 创建成功");
                n
            }
            Err(e) => {
                return Err(anyhow::anyhow!("创建 IceOryx node 失败: {:?}", e));
            }
        };

        let service_name_obj = ServiceName::new(&service_name)?;
        let service_builder = || {
            node.service_builder(&service_name_obj)
                .publish_subscribe::<[u8; PM_MAX_BYTES]>()
                .max_publishers(1)
                .max_subscribers(PM_MAX_SUBSCRIBERS)
                .history_size(PM_HISTORY_SIZE)
                .subscriber_max_buffer_size(PM_SUBSCRIBER_MAX_BUFFER_SIZE)
        };

        let service = match service_builder().open() {
            Ok(s) => {
                info!("复用已存在的 IceOryx service: '{}'", service_name);
                s
            }
            Err(open_err) => {
                warn!(
                    "打开已有 IceOryx service 失败，尝试 open_or_create: service='{}' err={:?}",
                    service_name, open_err
                );
                match service_builder().open_or_create() {
                    Ok(s) => s,
                    Err(create_err) => {
                        let open_text = format!("{:?}", open_err);
                        let create_text = format!("{:?}", create_err);
                        if is_corrupted_service_err(&open_text)
                            || is_corrupted_service_err(&create_text)
                        {
                            warn!(
                                "检测到 ServiceInCorruptedState，先尝试 dead-node cleanup 后重试: service='{}'",
                                service_name
                            );
                            let cleanup =
                                Node::<ipc::Service>::cleanup_dead_nodes(Config::global_config());
                            warn!(
                                "dead-node cleanup 完成: cleanups={}, failed_cleanups={}",
                                cleanup.cleanups, cleanup.failed_cleanups
                            );

                            match service_builder().open_or_create() {
                                Ok(s) => {
                                    info!(
                                        "dead-node cleanup 后恢复成功，已创建/打开 IceOryx service: '{}'",
                                        service_name
                                    );
                                    s
                                }
                                Err(retry_err) => {
                                    return Err(anyhow!(
                                        "创建/打开 IceOryx service 失败: service='{}', open_err={:?}, create_err={:?}, retry_err={:?}; \
检测到 ServiceInCorruptedState，已执行 dead-node cleanup 但仍失败。\
请先停止该 service 的相关进程（仅 account_monitor 与其消费者）后重试。\
若仍失败，再在维护窗口执行 scripts/cleanup_iceoryx.sh 并重启 iox-roudi。",
                                        service_name,
                                        open_err,
                                        create_err,
                                        retry_err
                                    ));
                                }
                            }
                        } else {
                            return Err(anyhow!(
                                "创建/打开 IceOryx service 失败: service='{}', open_err={:?}, create_err={:?}",
                                service_name,
                                open_err,
                                create_err
                            ));
                        }
                    }
                }
            }
        };

        let publisher = match service.publisher_builder().create() {
            Ok(publisher) => publisher,
            Err(err) => {
                let err_text = format!("{:?}", err);
                if is_publisher_capacity_err(&err_text) {
                    warn!(
                        "publisher create hit max-publishers, attempting dead-node cleanup: service='{}' err={:?}",
                        service_name, err
                    );
                    let cleanup = Node::<ipc::Service>::cleanup_dead_nodes(Config::global_config());
                    warn!(
                        "dead-node cleanup 完成: cleanups={}, failed_cleanups={}",
                        cleanup.cleanups, cleanup.failed_cleanups
                    );
                    service.publisher_builder().create().map_err(|retry_err| {
                        anyhow!(
                            "创建 PM publisher 失败: service='{}', err={:?}, retry_err={:?}",
                            service_name,
                            err,
                            retry_err
                        )
                    })?
                } else {
                    return Err(err.into());
                }
            }
        };
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
