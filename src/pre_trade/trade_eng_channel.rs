use anyhow::Result;
use bytes::Bytes;
use iceoryx2::port::{publisher::Publisher, subscriber::Subscriber};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::event::TradeEngineResponse;
//use crate::trade_engine::trade_response_handle::TradeExecOutcome;

const TRADE_REQ_PAYLOAD: usize = 4_096;
const TRADE_RESP_PAYLOAD: usize = 16_384;

/// TradeEngChannel 负责与 trade engine 的双向通信
/// - 发布订单请求到 order_reqs/binance
/// - 订阅交易响应从 order_resps/binance
pub struct TradeEngChannel {
    // Publisher for sending order requests
    order_req_publisher: Publisher<ipc::Service, [u8; TRADE_REQ_PAYLOAD], ()>,

    // Receiver for trade responses
    trade_resp_tx: UnboundedSender<TradeEngineResponse>,
    trade_resp_rx: Option<UnboundedReceiver<TradeEngineResponse>>,
}

impl TradeEngChannel {
    /// 创建 Binance trade engine channel 实例
    /// 发布到: order_reqs/binance
    /// 订阅自: order_resps/binance
    pub fn new_binance_trade_channel() -> Result<Self> {
        // 创建 publisher 用于发送订单请求
        let req_node = NodeBuilder::new()
            .name(&NodeName::new("pre_trade_order_req_binance")?)
            .create::<ipc::Service>()?;

        let req_service = req_node
            .service_builder(&ServiceName::new("order_reqs/binance")?)
            .publish_subscribe::<[u8; TRADE_REQ_PAYLOAD]>()
            .open_or_create()?;

        let order_req_publisher = req_service.publisher_builder().create()?;
        info!("Order request publisher created: service=order_reqs/binance");

        // 创建 channel 用于接收交易响应
        let (trade_resp_tx, trade_resp_rx) = mpsc::unbounded_channel();

        // 启动 subscriber 监听交易响应
        Self::spawn_trade_resp_listener(
            trade_resp_tx.clone(),
            "order_resps/binance".to_string(),
            "pre_trade_order_resp_binance".to_string(),
        );

        Ok(Self {
            order_req_publisher,
            trade_resp_tx,
            trade_resp_rx: Some(trade_resp_rx),
        })
    }

    /// 发布订单请求
    pub fn publish_order_request(&self, bytes: &Bytes) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }

        if bytes.len() > TRADE_REQ_PAYLOAD {
            warn!(
                "Order request truncated: len={} capacity={}",
                bytes.len(),
                TRADE_REQ_PAYLOAD
            );
        }

        let mut buf = [0u8; TRADE_REQ_PAYLOAD];
        let copy_len = bytes.len().min(TRADE_REQ_PAYLOAD);
        buf[..copy_len].copy_from_slice(&bytes[..copy_len]);

        let sample = self.order_req_publisher.loan_uninit()?;
        let sample = sample.write_payload(buf);
        sample.send()?;

        Ok(())
    }

    /// 获取 trade response receiver，只能调用一次
    pub fn take_receiver(&mut self) -> Option<UnboundedReceiver<TradeEngineResponse>> {
        self.trade_resp_rx.take()
    }

    fn spawn_trade_resp_listener(
        tx: UnboundedSender<TradeEngineResponse>,
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
                    .publish_subscribe::<[u8; TRADE_RESP_PAYLOAD]>()
                    .open_or_create()?;

                let subscriber: Subscriber<ipc::Service, [u8; TRADE_RESP_PAYLOAD], ()> =
                    service.subscriber_builder().create()?;

                info!(
                    "Trade response subscribed: service={}",
                    service_name
                );

                loop {
                    match subscriber.receive() {
                        Ok(Some(sample)) => {
                            let payload = sample.payload();
                            let received_at = get_timestamp_us();

                            // Trade response frames format: [header][body]
                            if payload.len() < 32 {
                                warn!("Trade response too short: {} bytes", payload.len());
                                continue;
                            }

                            // Parse header (32 bytes)
                            let req_type = u32::from_le_bytes([
                                payload[0], payload[1], payload[2], payload[3],
                            ]);
                            let local_recv_time = i64::from_le_bytes([
                                payload[4], payload[5], payload[6], payload[7],
                                payload[8], payload[9], payload[10], payload[11],
                            ]);
                            let client_order_id = i64::from_le_bytes([
                                payload[12], payload[13], payload[14], payload[15],
                                payload[16], payload[17], payload[18], payload[19],
                            ]);
                            let exchange = u32::from_le_bytes([
                                payload[20], payload[21], payload[22], payload[23],
                            ]);
                            let status = u16::from_le_bytes([payload[24], payload[25]]);
                            let ip_weight = u32::from_le_bytes([
                                payload[26], payload[27], payload[28], payload[29],
                            ]);
                            let order_count = u32::from_le_bytes([
                                payload[30], payload[31], payload[32], payload[33],
                            ]);

                            // Body starts at offset 34
                            let body = if payload.len() > 34 {
                                payload[34..].to_vec()
                            } else {
                                Vec::new()
                            };

                            let body_truncated = payload.len() >= TRADE_RESP_PAYLOAD;

                            let resp = TradeEngineResponse {
                                service: service_name.clone(),
                                received_at,
                                payload_len: payload.len(),
                                req_type,
                                local_recv_time,
                                client_order_id,
                                exchange,
                                status,
                                ip_used_weight_1m: if ip_weight > 0 { Some(ip_weight) } else { None },
                                order_count_1m: if order_count > 0 { Some(order_count) } else { None },
                                body,
                                body_truncated,
                            };

                            if tx.send(resp).is_err() {
                                break;
                            }
                        }
                        Ok(None) => tokio::task::yield_now().await,
                        Err(err) => {
                            warn!("Trade response receive error: {err}");
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            };

            if let Err(err) = result.await {
                warn!("Trade response listener {} exited: {err:?}", service_name_for_error);
            }
        });
    }
}
