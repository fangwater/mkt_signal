use crate::common::exchange::Exchange;
use crate::common::ipc_service_name::build_service_name;
use crate::trade_engine::config::{ApiKey, TradeEngineCfg, TradeTransport};
use crate::trade_engine::dispatcher::Dispatcher;
use crate::trade_engine::trade_request::TradeRequestMsg;
use crate::trade_engine::trade_request_handle::{
    spawn_request_executor, spawn_ws_request_executor,
};
use crate::trade_engine::trade_response_handle::spawn_response_handle;
use crate::trade_engine::ws::TradeWsDispatcher;
use anyhow::{anyhow, Result};
use iceoryx2::port::{publisher::Publisher, subscriber::Subscriber};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};

pub struct TradeEngine {
    cfg: TradeEngineCfg,
    accounts: Vec<ApiKey>,
    req_tx: Option<tokio::sync::mpsc::UnboundedSender<TradeRequestMsg>>,
}

impl TradeEngine {
    pub fn new(cfg: TradeEngineCfg, accounts: Vec<ApiKey>) -> Self {
        Self {
            cfg,
            accounts,
            req_tx: None,
        }
    }

    /// 返回内部请求通道的一个克隆（在 run() 初始化后可用）
    pub fn sender(&self) -> Option<tokio::sync::mpsc::UnboundedSender<TradeRequestMsg>> {
        self.req_tx.clone()
    }
    /// 便捷发送接口
    pub fn send(&self, req: TradeRequestMsg) -> anyhow::Result<()> {
        if let Some(tx) = &self.req_tx {
            tx.send(req)
                .map_err(|_| anyhow::anyhow!("trade engine not accepting requests"))
        } else {
            Err(anyhow::anyhow!("trade engine not started"))
        }
    }

    pub async fn run(mut self) -> Result<()> {
        // Single-threaded tokio reactor is enforced by binary attribute.
        let raw_exchange = self
            .cfg
            .exchange
            .clone()
            .unwrap_or_else(|| "binance".to_string());
        let exchange_name = raw_exchange.to_ascii_lowercase();
        let canonical_exchange = match exchange_name.as_str() {
            "binance" | "okex" | "bybit" | "bitget" | "gate" => exchange_name.clone(),
            other => {
                return Err(anyhow!(
                    "unsupported exchange '{}'. Allowed: binance, okex, bybit, bitget, gate",
                    other
                ))
            }
        };

        // 构建带命名空间的服务名
        let order_req_service = build_service_name(&format!("order_reqs/{}", canonical_exchange));
        let order_resp_service = build_service_name(&format!("order_resps/{}", canonical_exchange));

        info!(
            "trade_engine starting; req_service='{}', resp_service='{}', transport={:?}",
            order_req_service, order_resp_service, self.cfg.transport
        );
        if matches!(self.cfg.transport, TradeTransport::Rest) {
            info!("rest base_url={}", self.cfg.rest.base_url);
        }
        if matches!(self.cfg.transport, TradeTransport::Ws) {
            if let Some(ws_cfg) = &self.cfg.ws {
                info!("ws endpoints={:?}", ws_cfg.urls);
            }
        }

        // Iceoryx subscriber for order requests
        let node_name = format!("trade_engine_{}", canonical_exchange);
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(&order_req_service)?)
            .publish_subscribe::<[u8; 4096]>()
            .subscriber_max_buffer_size(256)
            .open_or_create()?;
        let subscriber: Subscriber<ipc::Service, [u8; 4096], ()> =
            service.subscriber_builder().create()?;
        debug!("subscriber created for service: {}", order_req_service);

        // Result publisher
        let resp_service = node
            .service_builder(&ServiceName::new(&order_resp_service)?)
            .publish_subscribe::<[u8; 16384]>()
            .subscriber_max_buffer_size(256)
            .open_or_create()?;
        let resp_publisher: Publisher<ipc::Service, [u8; 16384], ()> =
            resp_service.publisher_builder().create()?;
        debug!("publisher created for service: {}", order_resp_service);

        // 解析 exchange 枚举
        let exchange = match canonical_exchange.as_str() {
            "binance" => Exchange::Binance,
            "okex" => Exchange::Okex,
            "bybit" => Exchange::Bybit,
            "bitget" => Exchange::BitgetFutures,
            "gate" => Exchange::Gate,
            other => {
                return Err(anyhow!(
                    "unsupported exchange '{}'. Allowed: binance, okex, bybit, bitget, gate",
                    other
                ))
            }
        };

        // Internal mpsc pipeline between request executor and response publisher
        let (req_tx, req_rx) = tokio::sync::mpsc::unbounded_channel::<TradeRequestMsg>();
        // 暴露内部请求通道（可供外部直接推送请求）
        self.req_tx = Some(req_tx.clone());
        let (resp_tx, resp_rx) = tokio::sync::mpsc::unbounded_channel();
        if self.accounts.is_empty() {
            return Err(anyhow!("no API keys provided for trade engine"));
        }

        let _req_worker = match self.cfg.transport {
            TradeTransport::Rest => {
                let dispatcher = Dispatcher::new(&self.cfg, &self.accounts)?;
                spawn_request_executor(dispatcher, exchange, req_rx, resp_tx.clone())
            }
            TradeTransport::Ws => {
                let ws_cfg =
                    self.cfg.ws.clone().ok_or_else(|| {
                        anyhow::anyhow!("ws transport configured but [ws] missing")
                    })?;
                let dispatcher = TradeWsDispatcher::new(
                    ws_cfg,
                    self.cfg.network.local_ips.clone(),
                    exchange,
                    resp_tx.clone(),
                )?;
                spawn_ws_request_executor(dispatcher, exchange, req_rx, resp_tx.clone())
            }
        };
        let _resp_worker = spawn_response_handle(resp_publisher, resp_rx);

        loop {
            match subscriber.receive()? {
                Some(sample) => {
                    // 复制有效负载后尽快释放 sample，避免底层回收异常
                    let actual_len = {
                        let payload = sample.payload();
                        payload
                            .iter()
                            .rposition(|&x| x != 0)
                            .map(|pos| pos + 1)
                            .unwrap_or(0)
                    };
                    if actual_len == 0 {
                        drop(sample);
                        continue;
                    }
                    let owned = {
                        let payload = sample.payload();
                        bytes::Bytes::copy_from_slice(&payload[..actual_len])
                    };
                    drop(sample);

                    debug!("received payload bytes: {}", actual_len);

                    // Expect binary TradeRequest
                    match crate::trade_engine::trade_request::TradeRequestMsg::parse(&owned) {
                        Some(msg) => {
                            debug!(
                                "enqueue request: type={:?}, client_order_id={}, params_len={}",
                                msg.req_type,
                                msg.client_order_id,
                                msg.params.len()
                            );
                            let _ = req_tx.send(msg);
                        }
                        None => {
                            warn!("invalid trade request binary payload (len={})", actual_len);
                        }
                    }
                }
                None => {
                    // 低延迟优先但仍需让出执行权，避免饿死其他本地任务
                    tokio::task::yield_now().await;
                }
            }
        }
    }
}
