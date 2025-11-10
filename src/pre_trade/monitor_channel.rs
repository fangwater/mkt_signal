use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::common::account_msg::get_event_type as get_account_event_type;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::event::AccountEvent;

const ACCOUNT_PAYLOAD: usize = 16_384;

/// MonitorChannel 负责订阅单个 account monitor service
/// 每个实例有独立的 dedup 和 channel
pub struct MonitorChannel {
    dedup: crate::pre_trade::dedup::DedupCache,
    tx: UnboundedSender<AccountEvent>,
    rx: Option<UnboundedReceiver<AccountEvent>>,
}

impl MonitorChannel {
    /// 创建新的 MonitorChannel 实例并启动监听
    /// service_name: 如 "account_pubs/binance_pm"
    pub fn new(service_name: String) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        // 从 service_name 自动生成 node_name
        // 例如: "account_pubs/binance_pm" -> "pre_trade_account_pubs_binance_pm"
        let node_name = format!("pre_trade_{}", service_name.replace('/', "_"));

        // 立即启动监听
        Self::spawn_listener(
            tx.clone(),
            service_name,
            node_name,
        );

        Self {
            dedup: crate::pre_trade::dedup::DedupCache::new(8192),
            tx,
            rx: Some(rx),
        }
    }

    /// 获取 sender，用于发送事件
    pub fn get_sender(&self) -> UnboundedSender<AccountEvent> {
        self.tx.clone()
    }

    /// 获取 receiver，只能调用一次
    pub fn take_receiver(&mut self) -> Option<UnboundedReceiver<AccountEvent>> {
        self.rx.take()
    }

    fn spawn_listener(
        tx: UnboundedSender<AccountEvent>,
        service_name: String,
        node_name: String,
    ) {
        tokio::task::spawn_local(async move {
            let service_name_for_error = service_name.clone();
            let result = async move {
                let node = NodeBuilder::new()
                    .name(&NodeName::new(&node_name)?)
                    .create::<ipc::Service>()?;

                let service = node
                    .service_builder(&ServiceName::new(&service_name)?)
                    .publish_subscribe::<[u8; ACCOUNT_PAYLOAD]>()
                    .open_or_create()?;
                let subscriber: Subscriber<ipc::Service, [u8; ACCOUNT_PAYLOAD], ()> =
                    service.subscriber_builder().create()?;

                info!(
                    "account stream subscribed: service={}",
                    service_name
                );

                loop {
                    match subscriber.receive() {
                        Ok(Some(sample)) => {
                            let payload = sample.payload();
                            let received_at = get_timestamp_us();

                            // Account frames format: [type:4][len:4][data:len]
                            let (frame_len, event_type_str) = if payload.len() >= 8 {
                                let msg_type = get_account_event_type(payload);
                                let body_len = u32::from_le_bytes([
                                    payload[4], payload[5], payload[6], payload[7],
                                ]) as usize;
                                let total = 8 + body_len;
                                let clamped = total.min(payload.len());
                                (clamped, format!("{:?}", msg_type))
                            } else {
                                (payload.len(), "<too_short>".to_string())
                            };

                            let mut buf = payload[..frame_len].to_vec();

                            let evt = AccountEvent {
                                service: service_name.clone(),
                                received_at,
                                payload_len: buf.len(),
                                payload: std::mem::take(&mut buf),
                                event_type: Some(event_type_str),
                                event_time_ms: None,
                            };
                            if tx.send(evt).is_err() {
                                break;
                            }
                        }
                        Ok(None) => tokio::task::yield_now().await,
                        Err(err) => {
                            warn!("account stream receive error: {err}");
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            };

            if let Err(err) = result.await {
                warn!("account listener {} exited: {err:?}", service_name_for_error);
            }
        });
    }
}
