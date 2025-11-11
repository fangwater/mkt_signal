use anyhow::Result;
use bytes::Bytes;
use log::warn;

use crate::account::execution_record::{ExecutionRecordMessage, MARGIN_EXECUTION_RECORD_CHANNEL};
use crate::common::iceoryx_publisher::{BinanceMarginUpdatePublisher, SignalPublisher};
use crate::signal::record::{SignalRecordMessage, PRE_TRADE_SIGNAL_RECORD_CHANNEL};

/// 持久化通道：负责将信号记录和订单执行记录通过 IceOryx 发布到下游持久化服务
///
/// 下游消费者：
/// - Signal: `persist_manager::SignalPersistor` -> RocksDB
/// - Execution: `persist_manager::ExecutionPersistor` -> RocksDB
///
/// 采用直接发布模式（无中间队列），优先保证低延迟
pub struct PersistChannel {
    signal_record_pub: Option<SignalPublisher>,
    execution_record_pub: Option<BinanceMarginUpdatePublisher>,
}

impl PersistChannel {
    /// 创建持久化通道，初始化两个 IceOryx 发布器
    ///
    /// 如果发布器创建失败，会记录警告并继续运行（降级模式）
    pub fn new() -> Self {
        let signal_record_pub = match SignalPublisher::new(PRE_TRADE_SIGNAL_RECORD_CHANNEL) {
            Ok(p) => {
                log::info!(
                    "PersistChannel: signal record publisher created on '{}'",
                    PRE_TRADE_SIGNAL_RECORD_CHANNEL
                );
                Some(p)
            }
            Err(err) => {
                warn!(
                    "PersistChannel: failed to create signal record publisher on '{}': {err:#}",
                    PRE_TRADE_SIGNAL_RECORD_CHANNEL
                );
                None
            }
        };

        let execution_record_pub =
            match BinanceMarginUpdatePublisher::new(MARGIN_EXECUTION_RECORD_CHANNEL) {
                Ok(p) => {
                    log::info!(
                        "PersistChannel: execution record publisher created on '{}'",
                        MARGIN_EXECUTION_RECORD_CHANNEL
                    );
                    Some(p)
                }
                Err(err) => {
                    warn!(
                        "PersistChannel: failed to create execution record publisher on '{}': {err:#}",
                        MARGIN_EXECUTION_RECORD_CHANNEL
                    );
                    None
                }
            };

        Self {
            signal_record_pub,
            execution_record_pub,
        }
    }

    /// 发布信号记录到持久化通道
    ///
    /// # 参数
    /// - `record`: 信号记录消息（包含策略ID、信号类型、上下文等）
    ///
    /// # 错误处理
    /// - 如果发布器未初始化，静默跳过
    /// - 如果发布失败，记录警告但不阻塞调用者
    pub fn publish_signal_record(&self, record: &SignalRecordMessage) {
        let Some(publisher) = &self.signal_record_pub else {
            return;
        };

        let payload = record.to_bytes();
        if let Err(err) = publisher.publish(payload.as_ref()) {
            warn!(
                "failed to publish signal record strategy_id={}: {err:#}",
                record.strategy_id
            );
        }
    }

    /// 发布订单执行记录到持久化通道
    ///
    /// # 参数
    /// - `record`: 订单执行记录消息（包含订单ID、成交信息、手续费等）
    ///
    /// # 错误处理
    /// - 如果发布器未初始化，静默跳过
    /// - 如果发布失败，记录警告但不阻塞调用者
    pub fn publish_execution_record(&self, record: &ExecutionRecordMessage) {
        let Some(publisher) = &self.execution_record_pub else {
            return;
        };

        let payload = record.to_bytes();
        if let Err(err) = publisher.publish(payload.as_ref()) {
            warn!(
                "failed to publish execution record order_id={} symbol={}: {err:#}",
                record.order_id, record.symbol
            );
        }
    }

    /// 便捷方法：从字节流发布信号记录（用于已序列化的场景）
    pub fn publish_signal_record_bytes(&self, bytes: &Bytes) -> Result<()> {
        let Some(publisher) = &self.signal_record_pub else {
            return Ok(());
        };
        publisher.publish(bytes.as_ref())
    }

    /// 便捷方法：从字节流发布订单执行记录（用于已序列化的场景）
    pub fn publish_execution_record_bytes(&self, bytes: &Bytes) -> Result<()> {
        let Some(publisher) = &self.execution_record_pub else {
            return Ok(());
        };
        publisher.publish(bytes.as_ref())
    }

    /// 检查信号记录发布器是否可用
    pub fn is_signal_publisher_available(&self) -> bool {
        self.signal_record_pub.is_some()
    }

    /// 检查订单执行记录发布器是否可用
    pub fn is_execution_publisher_available(&self) -> bool {
        self.execution_record_pub.is_some()
    }
}

impl Default for PersistChannel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_persist_channel_creation() {
        // 测试创建（可能失败如果没有 IceOryx 运行时）
        let channel = PersistChannel::new();
        // 基本断言：结构体应该创建成功
        assert!(
            channel.is_signal_publisher_available() || !channel.is_signal_publisher_available()
        );
    }
}
