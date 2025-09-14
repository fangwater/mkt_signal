use crate::trade_engine::config::TradeEngineCfg;
use crate::trade_engine::dispatcher::Dispatcher;
use crate::trade_engine::order_event::{OrderRequestEvent, OrderResponseEvent};
use anyhow::Result;
use bytes::Bytes;
use iceoryx2::port::{publisher::Publisher, subscriber::Subscriber};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{error, info, warn};
use serde_json::Value;

pub struct TradeEngine {
    cfg: TradeEngineCfg,
}

impl TradeEngine {
    pub fn new(cfg: TradeEngineCfg) -> Self { Self { cfg } }

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

        // Result publisher
        let resp_service = node
            .service_builder(&ServiceName::new(&self.cfg.order_resp_service)?)
            .publish_subscribe::<[u8; 8192]>()
            .history_size(64)
            .open_or_create()?;
        let resp_publisher: Publisher<ipc::Service, [u8; 8192], ()> = resp_service.publisher_builder().create()?;

        // Dispatcher (HTTP + limits)
        let mut dispatcher = Dispatcher::new(&self.cfg)?;

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

                    // Expect JSON for an order request event
                    match serde_json::from_slice::<OrderRequestEvent>(bytes) {
                        Ok(evt) => {
                            let result = dispatcher.dispatch(evt.clone()).await;
                            match result {
                                Ok(outcome) => {
                                    let ok = outcome.status >= 200 && outcome.status < 300;
                                    let resp_msg = OrderResponseEvent {
                                        ok,
                                        status: outcome.status,
                                        ip: outcome.ip.to_string(),
                                        account: outcome.account,
                                        endpoint: evt.endpoint,
                                        method: evt.method,
                                        req_id: evt.req_id,
                                        used_weight_1m: outcome.ip_used_weight_1m,
                                        order_count_1m: outcome.order_count_1m,
                                        body: outcome.body,
                                    };
                                    self.publish_response(&resp_publisher, &resp_msg)?;
                                }
                                Err(e) => {
                                    let resp_msg = OrderResponseEvent {
                                        ok: false,
                                        status: 0,
                                        ip: String::new(),
                                        account: evt.account.unwrap_or_default(),
                                        endpoint: evt.endpoint,
                                        method: evt.method,
                                        req_id: evt.req_id,
                                        used_weight_1m: None,
                                        order_count_1m: None,
                                        body: e.to_string(),
                                    };
                                    self.publish_response(&resp_publisher, &resp_msg)?;
                                }
                            }
                        }
                        Err(e) => {
                            warn!("invalid order event JSON: {}", e);
                        }
                    }
                }
                None => {
                    // idle; small sleep to avoid tight loop
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            }
        }
    }
}

impl TradeEngine {
    fn publish_response(
        &self,
        publisher: &Publisher<ipc::Service, [u8; 8192], ()>,
        msg: &OrderResponseEvent,
    ) -> Result<()> {
        let json = serde_json::to_vec(msg)?;
        if json.len() > 8192 {
            warn!("order response too large: {} bytes, truncating", json.len());
        }
        let mut buf = [0u8; 8192];
        let n = json.len().min(8192);
        buf[..n].copy_from_slice(&json[..n]);
        let sample = publisher.loan_uninit()?;
        let sample = sample.write_payload(buf);
        sample.send()?;
        Ok(())
    }
}
