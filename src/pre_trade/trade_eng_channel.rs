use anyhow::Result;
use bytes::Bytes;
use iceoryx2::port::{publisher::Publisher, subscriber::Subscriber};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::cell::OnceCell;
use std::time::Duration;

thread_local! {
    static TRADE_ENG_CHANNEL: OnceCell<TradeEngChannel> = OnceCell::new();
}

const TRADE_REQ_PAYLOAD: usize = 4_096;
const TRADE_RESP_PAYLOAD: usize = 16_384;

/// 默认订单请求服务名称
pub const DEFAULT_ORDER_REQ_SERVICE: &str = "order_reqs/binance";

/// 默认订单响应服务名称
pub const DEFAULT_ORDER_RESP_SERVICE: &str = "order_resps/binance";

/// TradeEngChannel 负责与 trade engine 的双向通信
///
/// 采用线程本地单例模式，通过 `TradeEngChannel::with()` 访问
///
/// # 使用示例
/// ```ignore
/// use crate::pre_trade::TradeEngChannel;
///
/// // 发送订单请求
/// TradeEngChannel::with(|ch| ch.publish_order_request(&order_bytes))?;
/// ```
pub struct TradeEngChannel {
    /// Publisher for sending order requests
    order_req_publisher: Publisher<ipc::Service, [u8; TRADE_REQ_PAYLOAD], ()>,
}

impl TradeEngChannel {
    /// 在当前线程的 TradeEngChannel 单例上执行操作
    ///
    /// 第一次调用时会自动初始化默认配置，后续调用直接使用已初始化的实例
    ///
    /// # 使用示例
    /// ```ignore
    /// // 发送订单请求
    /// TradeEngChannel::with(|ch| ch.publish_order_request(&order_bytes))?;
    /// ```
    pub fn with<F, R>(f: F) -> R
    where
        F: FnOnce(&TradeEngChannel) -> R,
    {
        TRADE_ENG_CHANNEL.with(|cell| {
            let channel = cell.get_or_init(|| {
                info!("Initializing thread-local TradeEngChannel singleton with default config");
                TradeEngChannel::new(DEFAULT_ORDER_REQ_SERVICE, DEFAULT_ORDER_RESP_SERVICE)
                    .expect("Failed to initialize default TradeEngChannel")
            });
            f(channel)
        })
    }

    /// 显式初始化交易引擎频道（可选）
    ///
    /// 如果在首次调用 `with()` 之前调用此方法，可以自定义服务名称
    ///
    /// # 参数
    /// * `order_req_service` - 订单请求服务名称
    /// * `order_resp_service` - 订单响应服务名称
    ///
    /// # 错误
    /// - 如果已经初始化，返回错误
    /// - 如果 IceOryx 初始化失败，返回错误
    pub fn initialize(order_req_service: &str, order_resp_service: &str) -> Result<()> {
        TRADE_ENG_CHANNEL.with(|cell| {
            if cell.get().is_some() {
                return Err(anyhow::anyhow!("TradeEngChannel already initialized"));
            }
            cell.set(TradeEngChannel::new(order_req_service, order_resp_service)?)
                .map_err(|_| anyhow::anyhow!("Failed to set TradeEngChannel (race condition)"))
        })
    }

    /// 创建 TradeEngChannel 实例
    ///
    /// # 参数
    /// * `order_req_service` - 订单请求服务名称 (例如: "order_reqs/binance")
    /// * `order_resp_service` - 订单响应服务名称 (例如: "order_resps/binance")
    ///
    /// 注意：通常应使用 `TradeEngChannel::with()` 访问线程本地单例，
    /// 而不是直接调用 `new()` 创建多个实例
    fn new(order_req_service: &str, order_resp_service: &str) -> Result<Self> {
        // 创建 publisher 用于发送订单请求
        let req_node = NodeBuilder::new()
            .name(&NodeName::new("pre_trade_order_req")?)
            .create::<ipc::Service>()?;

        let req_service = req_node
            .service_builder(&ServiceName::new(order_req_service)?)
            .publish_subscribe::<[u8; TRADE_REQ_PAYLOAD]>()
            .open_or_create()?;

        let order_req_publisher = req_service.publisher_builder().create()?;
        info!(
            "TradeEngChannel: order request publisher created on '{}'",
            order_req_service
        );

        // 启动交易响应监听器
        let resp_service = order_resp_service.to_string();
        tokio::task::spawn_local(async move {
            if let Err(err) = Self::run_trade_resp_listener(&resp_service).await {
                warn!(
                    "Trade response listener exited (service={}): {err:?}",
                    resp_service
                );
            }
        });

        Ok(Self {
            order_req_publisher,
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

    /// 运行交易响应监听器，直接处理响应
    async fn run_trade_resp_listener(service_name: &str) -> Result<()> {
        let node = NodeBuilder::new()
            .name(&NodeName::new("pre_trade_order_resp")?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(service_name)?)
            .publish_subscribe::<[u8; TRADE_RESP_PAYLOAD]>()
            .open_or_create()?;

        let subscriber: Subscriber<ipc::Service, [u8; TRADE_RESP_PAYLOAD], ()> =
            service.subscriber_builder().create()?;

        info!(
            "TradeEngChannel: trade response subscribed on '{}'",
            service_name
        );

        loop {
            match subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = sample.payload();

                    // Trade response frames format: [header][body]
                    if payload.len() < 34 {
                        warn!("Trade response too short: {} bytes", payload.len());
                        continue;
                    }

                    // Parse header (34 bytes)
                    let req_type = u32::from_le_bytes([
                        payload[0], payload[1], payload[2], payload[3],
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
                    let client_order_id = i64::from_le_bytes([
                        payload[12], payload[13], payload[14], payload[15],
                        payload[16], payload[17], payload[18], payload[19],
                    ]);

                    // Body starts at offset 34
                    let body = if payload.len() > 34 {
                        String::from_utf8_lossy(&payload[34..]).to_string()
                    } else {
                        String::new()
                    };

                    // 处理响应
                    Self::handle_trade_engine_response(
                        status,
                        req_type,
                        exchange,
                        client_order_id,
                        &body,
                        ip_weight,
                        order_count,
                    );
                }
                Ok(None) => tokio::task::yield_now().await,
                Err(err) => {
                    warn!("Trade response receive error: {err}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    /// 处理交易引擎响应
    fn handle_trade_engine_response(
        status: u16,
        req_type: u32,
        exchange: u32,
        client_order_id: i64,
        body: &str,
        ip_weight: u32,
        order_count: u32,
    ) {
        match status {
            200 => {
                // 成功响应，不打印日志
            }
            403 => {
                warn!(
                    "WAF Limit violated: exchange={} req_type={} cli_ord_id={} body={}",
                    exchange, req_type, client_order_id, body
                );
            }
            418 => {
                warn!(
                    "IP auto-banned for continuing requests after 429: exchange={} req_type={} cli_ord_id={} body={}",
                    exchange, req_type, client_order_id, body
                );
            }
            429 => {
                warn!(
                    "Request rate limit exceeded: exchange={} req_type={} cli_ord_id={} ip_weight={} order_count={} body={}",
                    exchange, req_type, client_order_id, ip_weight, order_count, body
                );
            }
            503 => {
                warn!(
                    "Service unavailable (503): exchange={} req_type={} cli_ord_id={} body={}",
                    exchange, req_type, client_order_id, body
                );
            }
            400..=499 => {
                warn!(
                    "Client error (4xx): status={} exchange={} req_type={} cli_ord_id={} body={}",
                    status, exchange, req_type, client_order_id, body
                );
            }
            500..=599 => {
                warn!(
                    "Server error (5xx): status={} exchange={} req_type={} cli_ord_id={} body={}",
                    status, exchange, req_type, client_order_id, body
                );
            }
            _ => {
                warn!(
                    "Unexpected HTTP status: status={} exchange={} req_type={} cli_ord_id={} body={}",
                    status, exchange, req_type, client_order_id, body
                );
            }
        }
    }
}
