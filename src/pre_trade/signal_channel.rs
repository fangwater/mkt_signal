use crate::common::iceoryx_publisher::{SignalPublisher, SIGNAL_PAYLOAD};
use crate::signal::trade_signal::TradeSignal;
use anyhow::Result;
use bytes::Bytes;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

/// 信号频道 - 负责信号进程和 pre-trade 之间的通讯
pub struct SignalChannel {
    /// 接收端：接收来自上游的交易信号
    rx: UnboundedReceiver<TradeSignal>,
    /// 发送端的克隆：可用于创建多个消费者
    tx: UnboundedSender<TradeSignal>,
    /// 反向发布器：用于向上游信号进程发送查询或反馈
    backward_pub: Option<SignalPublisher>,
}

impl SignalChannel {
    /// 创建信号频道并自动启动监听器
    ///
    /// # 参数
    /// * `channel_name` - 要订阅的信号频道名称
    /// * `backward_channel` - 反向通道名称，用于向上游发送信号（可选）
    pub fn new(channel_name: &str, backward_channel: Option<&str>) -> Self {
        // 创建消息队列
        let (tx, rx) = mpsc::unbounded_channel();

        // 创建反向发布器
        let backward_pub = if let Some(backward_ch) = backward_channel {
            match SignalPublisher::new(backward_ch) {
                Ok(p) => Some(p),
                Err(err) => {
                    warn!(
                        "failed to create backward publisher on {}: {err:#}",
                        backward_ch
                    );
                    None
                }
            }
        } else {
            None
        };

        // 启动监听任务
        let channel_name_owned = channel_name.to_string();
        let tx_clone = tx.clone();
        tokio::task::spawn_local(async move {
            if let Err(err) = Self::run_listener(&channel_name_owned, tx_clone).await {
                warn!(
                    "signal listener exited (channel={}): {err:?}",
                    channel_name_owned
                );
            }
        });

        Self {
            rx,
            tx,
            backward_pub,
        }
    }

    /// 获取接收端的可变引用，用于接收交易信号
    pub fn get_queue_rx(&mut self) -> &mut UnboundedReceiver<TradeSignal> {
        &mut self.rx
    }

    /// 获取发送端的克隆，可用于创建多个消费者
    pub fn get_queue_tx(&self) -> UnboundedSender<TradeSignal> {
        self.tx.clone()
    }

    /// 向上游发送反馈数据
    ///
    /// # 参数
    /// * `data` - 要发送的数据
    ///
    /// # 返回
    /// 如果没有配置反向发布器，返回 Ok(false)；成功发送返回 Ok(true)
    pub fn publish_backward(&self, data: &[u8]) -> Result<bool> {
        if let Some(publisher) = &self.backward_pub {
            publisher.publish(data)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// 监听器的核心逻辑
    async fn run_listener(
        channel_name: &str,
        tx: UnboundedSender<TradeSignal>,
    ) -> Result<()> {
        let node_name = Self::signal_node_name(channel_name);
        let service_path = format!("signal_pubs/{}", channel_name);

        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(&service_path)?)
            .publish_subscribe::<[u8; SIGNAL_PAYLOAD]>()
            .max_publishers(1)
            .max_subscribers(32)
            .history_size(128)
            .subscriber_max_buffer_size(256)
            .open_or_create()?;

        let subscriber: Subscriber<ipc::Service, [u8; SIGNAL_PAYLOAD], ()> =
            service.subscriber_builder().create()?;

        info!(
            "signal subscribed: node={} service={} channel={}",
            node_name,
            service.name(),
            channel_name
        );

        loop {
            match subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = Bytes::copy_from_slice(sample.payload());
                    if payload.is_empty() {
                        continue;
                    }
                    match TradeSignal::from_bytes(&payload) {
                        Ok(signal) => {
                            if tx.send(signal).is_err() {
                                break;
                            }
                        }
                        Err(err) => warn!(
                            "failed to decode trade signal from channel {}: {}",
                            channel_name, err
                        ),
                    }
                }
                Ok(None) => tokio::task::yield_now().await,
                Err(err) => {
                    warn!("signal receive error (channel={}): {err}", channel_name);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
        Ok(())
    }

    /// 生成信号节点名称
    fn signal_node_name(channel: &str) -> String {
        format!("pre_trade_signal_{}", channel)
    }
}
