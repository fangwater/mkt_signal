use anyhow::{anyhow, Context, Result};
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
use crate::pre_trade::binance_pm_spot_manager::{BinancePmSpotAccountManager, BinanceSpotBalance};
use crate::pre_trade::binance_pm_um_manager::{BinancePmUmAccountManager, BinanceUmPosition};
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

/// MonitorChannel 负责订阅 Binance PM account monitor service
/// 集成账户管理器（UM 合约 + Spot 现货），自动初始化并启动监听
pub struct MonitorChannel {
    dedup: DedupCache,
    tx: UnboundedSender<AccountEvent>,
    rx: Option<UnboundedReceiver<AccountEvent>>,
    /// 币安 合约资产管理器，基于统一账户 update 更新
    pub um_manager: BinancePmUmAccountManager,
    /// 币安 现货资产管理器，基于统一账户 update 更新
    pub spot_manager: BinancePmSpotAccountManager,
}

impl MonitorChannel {
    /// 创建 Binance PM account monitor 实例并启动监听
    ///
    /// # 功能
    /// - 从环境变量读取 API 凭证（BINANCE_API_KEY, BINANCE_API_SECRET）
    /// - 初始化 UM 合约账户管理器
    /// - 初始化 Spot 现货账户管理器
    /// - 订阅 IceOryx 频道: account_pubs/binance_pm
    /// - 启动后台监听任务
    ///
    /// # 环境变量
    /// - `BINANCE_API_KEY`: Binance API Key（必需）
    /// - `BINANCE_API_SECRET`: Binance API Secret（必需）
    /// - `SPOT_ASSET_FILTER`: 可选，过滤特定现货资产（如 "USDT"）
    ///
    /// # 错误
    /// - API 凭证未设置
    /// - 初始账户快照获取失败
    pub async fn new_binance_pm_monitor() -> Result<Self> {
        // Read API credentials from environment variables
        let api_key = std::env::var("BINANCE_API_KEY")
            .map_err(|_| anyhow!("BINANCE_API_KEY environment variable not set"))?;
        let api_secret = std::env::var("BINANCE_API_SECRET")
            .map_err(|_| anyhow!("BINANCE_API_SECRET environment variable not set"))?;

        // Hardcoded REST endpoints
        let rest_base = "https://papi.binance.com";
        let recv_window_ms = 5000;

        // 初始化 UM 合约管理器
        let um_manager = BinancePmUmAccountManager::new(
            rest_base,
            api_key.clone(),
            api_secret.clone(),
            recv_window_ms,
        );
        let um_snapshot = um_manager
            .init()
            .await
            .context("failed to load initial Binance UM snapshot")?;
        log_um_positions(&um_snapshot.positions);

        // 初始化 Spot 现货管理器（使用相同的 PM 账户凭证）
        let asset_filter = std::env::var("SPOT_ASSET_FILTER")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());

        let spot_manager = BinancePmSpotAccountManager::new(
            rest_base,
            api_key,
            api_secret,
            recv_window_ms,
            asset_filter,
        );
        let spot_snapshot = spot_manager
            .init()
            .await
            .context("failed to load initial Binance spot snapshot")?;
        log_spot_balances(&spot_snapshot.balances);

        // 创建事件通道
        let (tx, rx) = mpsc::unbounded_channel();

        let service_name = "account_pubs/binance_pm".to_string();
        let node_name = "pre_trade_account_pubs_binance_pm".to_string();

        // 启动后台监听任务
        Self::spawn_listener(tx.clone(), service_name, node_name);

        Ok(Self {
            dedup: DedupCache::new(8192),
            tx,
            rx: Some(rx),
            um_manager,
            spot_manager,
        })
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

// ==================== Helper Functions ====================

fn log_um_positions(positions: &[BinanceUmPosition]) {
    if positions.is_empty() {
        info!("Binance UM account initialized: no open positions");
        return;
    }
    info!("Binance UM account initialized: {} positions", positions.len());
    for pos in positions {
        if pos.position_amt.abs() > 1e-8 {
            info!(
                "  UM position: {} amt={:.8} entry={:.4} upnl={:.4}",
                pos.symbol, pos.position_amt, pos.entry_price, pos.unrealized_profit
            );
        }
    }
}

fn log_spot_balances(balances: &[BinanceSpotBalance]) {
    if balances.is_empty() {
        info!("Binance Spot account initialized: no balances");
        return;
    }
    info!("Binance Spot account initialized: {} assets", balances.len());
    for bal in balances {
        let total_balance = bal.total_wallet_balance;
        let net_asset = bal.net_asset();
        if total_balance.abs() > 1e-8 || net_asset.abs() > 1e-8 {
            info!(
                "  Spot balance: {} total={:.8} net={:.8} (margin_free={:.8} margin_locked={:.8})",
                bal.asset, total_balance, net_asset, bal.cross_margin_free, bal.cross_margin_locked
            );
        }
    }
}
