use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::common::account_msg::{
    get_event_type as get_account_event_type, AccountEventType, AccountPositionMsg,
    AccountUpdateBalanceMsg, AccountUpdateFlushMsg, AccountUpdatePositionMsg, BalanceUpdateMsg,
    ExecutionReportMsg, OrderTradeUpdateMsg,
};
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::event::AccountEvent;

const ACCOUNT_PAYLOAD: usize = 16_384;

// ==================== Deduplication Cache ====================

/// 简单的去重缓存（固定容量，FIFO 淘汰）
pub struct DedupCache {
    set: HashSet<u64>,
    queue: VecDeque<u64>,
    capacity: usize,
}

impl DedupCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            set: HashSet::new(),
            queue: VecDeque::new(),
            capacity: capacity.max(1024),
        }
    }

    /// 插入并返回是否为新条目；false 表示重复，应丢弃
    pub fn insert_check(&mut self, key: u64) -> bool {
        if self.set.contains(&key) {
            return false;
        }
        if self.queue.len() >= self.capacity {
            if let Some(old) = self.queue.pop_front() {
                self.set.remove(&old);
            }
        }
        self.queue.push_back(key);
        self.set.insert(key);
        true
    }
}

/// 组合多个 u64 片段生成稳定的 64 位哈希
pub fn hash64(parts: &[u64]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for p in parts {
        p.hash(&mut hasher);
    }
    hasher.finish()
}

fn hash_str64(s: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

// ---- Key helpers for account event types ----

pub fn key_balance_update(msg: &BalanceUpdateMsg) -> u64 {
    hash64(&[
        AccountEventType::BalanceUpdate as u32 as u64,
        msg.update_id as u64,
        msg.event_time as u64,
    ])
}

pub fn key_account_update_balance(msg: &AccountUpdateBalanceMsg) -> u64 {
    hash64(&[
        AccountEventType::AccountUpdateBalance as u32 as u64,
        msg.event_time as u64,
        msg.transaction_time as u64,
        hash_str64(&msg.asset),
    ])
}

pub fn key_account_position(msg: &AccountPositionMsg) -> u64 {
    hash64(&[
        AccountEventType::AccountPosition as u32 as u64,
        msg.update_id as u64,
        msg.event_time as u64,
        hash_str64(&msg.asset),
    ])
}

pub fn key_account_update_position(msg: &AccountUpdatePositionMsg) -> u64 {
    hash64(&[
        AccountEventType::AccountUpdatePosition as u32 as u64,
        msg.event_time as u64,
        msg.transaction_time as u64,
        msg.symbol_length as u64,
        msg.position_side as u8 as u64,
    ])
}

pub fn key_account_update_flush(msg: &AccountUpdateFlushMsg) -> u64 {
    hash64(&[
        AccountEventType::AccountUpdateFlush as u32 as u64,
        msg.hash,
        hash_str64(&msg.scope),
    ])
}

pub fn key_execution_report(msg: &ExecutionReportMsg) -> u64 {
    hash64(&[
        AccountEventType::ExecutionReport as u32 as u64,
        msg.order_id as u64,
        msg.trade_id as u64,
        msg.update_id as u64,
        msg.event_time as u64,
    ])
}

pub fn key_order_trade_update(msg: &OrderTradeUpdateMsg) -> u64 {
    hash64(&[
        AccountEventType::OrderTradeUpdate as u32 as u64,
        msg.order_id as u64,
        msg.trade_id as u64,
        msg.event_time as u64,
    ])
}

// ==================== Monitor Channel ====================

/// MonitorChannel 负责订阅单个 account monitor service
/// 每个实例有独立的 dedup 和 channel
pub struct MonitorChannel {
    dedup: DedupCache,
    tx: UnboundedSender<AccountEvent>,
    rx: Option<UnboundedReceiver<AccountEvent>>,
}

impl MonitorChannel {
    /// 创建 Binance PM account monitor 实例并启动监听
    /// 订阅频道: account_pubs/binance_pm
    pub fn new_binance_pm_monitor() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        let service_name = "account_pubs/binance_pm".to_string();
        let node_name = "pre_trade_account_pubs_binance_pm".to_string();

        // 立即启动监听
        Self::spawn_listener(
            tx.clone(),
            service_name,
            node_name,
        );

        Self {
            dedup: DedupCache::new(8192),
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
