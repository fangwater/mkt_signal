use crate::trade_engine::config::TradeEngineCfg;
use crate::trade_engine::dispatcher::Dispatcher;
use crate::trade_engine::trade_request::TradeRequestMsg;
use crate::trade_engine::trade_request_handle::spawn_request_executor;
use crate::trade_engine::trade_response_handle::spawn_response_handle;
use crate::common::exchange::Exchange;
use anyhow::Result;
use iceoryx2::port::{publisher::Publisher, subscriber::Subscriber};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn, debug};

pub struct TradeEngine {
    cfg: TradeEngineCfg,
    req_tx: Option<tokio::sync::mpsc::UnboundedSender<TradeRequestMsg>>,
}

impl TradeEngine {
    pub fn new(cfg: TradeEngineCfg) -> Self { Self { cfg, req_tx: None } }

    /// 返回内部请求通道的一个克隆（在 run() 初始化后可用）
    pub fn sender(&self) -> Option<tokio::sync::mpsc::UnboundedSender<TradeRequestMsg>> {
        self.req_tx.clone()
    }
    /// 便捷发送接口
    pub fn send(&self, req: TradeRequestMsg) -> anyhow::Result<()> {
        if let Some(tx) = &self.req_tx {
            tx.send(req).map_err(|_| anyhow::anyhow!("trade engine not accepting requests"))
        } else {
            Err(anyhow::anyhow!("trade engine not started"))
        }
    }

    pub async fn run(mut self) -> Result<()> {
        // Single-threaded tokio reactor is enforced by binary attribute.
        info!(
            "trade_engine starting; service='{}', rest='{}'",
            self.cfg.order_req_service, self.cfg.rest.base_url
        );

        // Iceoryx subscriber for order requests
        let node_name = format!(
            "trade_engine_{}",
            self.cfg.exchange.clone().unwrap_or_else(|| "default".to_string())
        );
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(&self.cfg.order_req_service)?)
            .publish_subscribe::<[u8; 4096]>()
            .open_or_create()?;
        let subscriber: Subscriber<ipc::Service, [u8; 4096], ()> = service.subscriber_builder().create()?;
        debug!("subscriber created for service: {}", self.cfg.order_req_service);

        // Result publisher
        let resp_service = node
            .service_builder(&ServiceName::new(&self.cfg.order_resp_service)?)
            .publish_subscribe::<[u8; 8192]>()
            .history_size(64)
            .open_or_create()?;
        let resp_publisher: Publisher<ipc::Service, [u8; 8192], ()> = resp_service.publisher_builder().create()?;
        debug!("publisher created for service: {}", self.cfg.order_resp_service);

        // Dispatcher (HTTP + limits)
        let dispatcher = Dispatcher::new(&self.cfg)?;

        // 解析 exchange 枚举
        let exchange = match self.cfg.exchange.as_deref() {
            Some("binance") => Exchange::Binance,
            Some("binance-futures") => Exchange::BinanceFutures,
            Some("okex") => Exchange::Okex,
            Some("okex-swap") => Exchange::OkexSwap,
            Some("bybit") => Exchange::Bybit,
            Some("bybit-spot") => Exchange::BybitSpot,
            _ => Exchange::BinanceFutures,
        };

        // Internal mpsc pipeline between request executor and response publisher
        let (req_tx, req_rx) = tokio::sync::mpsc::unbounded_channel::<TradeRequestMsg>();
        // 暴露内部请求通道（可供外部直接推送请求）
        self.req_tx = Some(req_tx.clone());
        let (resp_tx, resp_rx) = tokio::sync::mpsc::unbounded_channel();
        let _req_worker = spawn_request_executor(dispatcher, exchange, req_rx, resp_tx);
        let _resp_worker = spawn_response_handle(resp_publisher, resp_rx);

        loop {
            match subscriber.receive()? {
                Some(sample) => {
                    let payload = sample.payload();
                    let actual_len = payload
                        .iter()
                        .rposition(|&x| x != 0)
                        .map(|pos| pos + 1)
                        .unwrap_or(0);
                    if actual_len == 0 { continue; }
                    let bytes = &payload[..actual_len];
                    debug!("received payload bytes: {}", actual_len);

                    // Expect binary TradeRequest
                    match crate::trade_engine::trade_request::TradeRequestMsg::parse(bytes) {
                        Some(msg) => {
                            debug!(
                                "enqueue request: type={:?}, client_order_id={}, params_len={}",
                                msg.req_type, msg.client_order_id, msg.params.len()
                            );
                            let _ = req_tx.send(msg);
                        }
                        None => { warn!("invalid trade request binary payload (len={})", actual_len); }
                    }
                }
                None => { /* 低延迟优先：不休眠，忙轮询 */ }
            }
        }
    }
}
