use anyhow::{anyhow, Result};
use bytes::Bytes;
use iceoryx2::port::{publisher::Publisher, subscriber::Subscriber};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use std::cell::{OnceCell, RefCell};
use std::collections::HashMap;
use std::time::Duration;

use crate::common::ipc_service_name::build_service_name;
use crate::strategy::query_engine_response::{QueryEngineResponse, QueryEngineResponseMessage};

thread_local! {
    static QUERY_ENG_HUB: OnceCell<QueryEngHub> = OnceCell::new();
}

const QUERY_REQ_PAYLOAD: usize = 4_096;
const QUERY_RESP_PAYLOAD: usize = 64;

pub struct QueryEngHub {
    channels: RefCell<HashMap<String, QueryEngChannel>>,
}

impl QueryEngHub {
    pub fn with<F, R>(f: F) -> R
    where
        F: FnOnce(&QueryEngHub) -> R,
    {
        QUERY_ENG_HUB.with(|cell| {
            let hub = cell.get_or_init(|| {
                info!("Initializing QueryEngHub singleton with default exchange (binance)");
                let hub = QueryEngHub::new();
                hub.ensure_exchange("binance")
                    .expect("Failed to initialize default QueryEngHub");
                hub
            });
            f(hub)
        })
    }

    pub fn initialize<S>(exchanges: S) -> Result<()>
    where
        S: IntoIterator,
        S::Item: AsRef<str>,
    {
        QUERY_ENG_HUB.with(|cell| {
            if cell.get().is_some() {
                return Err(anyhow!("QueryEngHub already initialized"));
            }
            let hub = QueryEngHub::new();
            for exchange in exchanges {
                hub.ensure_exchange(exchange.as_ref())?;
            }
            cell.set(hub)
                .map_err(|_| anyhow!("Failed to set QueryEngHub (race condition)"))
        })
    }

    pub fn ensure_registered(exchange: &str) -> Result<()> {
        Self::with(|hub| hub.ensure_exchange(exchange))
    }

    pub fn publish_query_request(exchange: &str, bytes: &Bytes) -> Result<()> {
        Self::with(|hub| hub.publish_to_exchange(exchange, bytes))
    }

    fn new() -> Self {
        Self {
            channels: RefCell::new(HashMap::new()),
        }
    }

    fn publish_to_exchange(&self, exchange: &str, bytes: &Bytes) -> Result<()> {
        self.ensure_exchange(exchange)?;
        let key = Self::normalize_exchange(exchange);
        let channels = self.channels.borrow();
        let Some(channel) = channels.get(&key) else {
            return Err(anyhow!("QueryEngHub: exchange '{}' not registered", key));
        };
        channel.publish_query_request(bytes)
    }

    fn ensure_exchange(&self, exchange: &str) -> Result<()> {
        let key = Self::normalize_exchange(exchange);
        if self.channels.borrow().contains_key(&key) {
            return Ok(());
        }

        info!(
            "QueryEngHub: registering query engine channel for exchange '{}'",
            key
        );
        let channel = QueryEngChannel::new(&key)?;
        self.channels.borrow_mut().insert(key, channel);
        Ok(())
    }

    fn normalize_exchange(exchange: &str) -> String {
        exchange.trim().to_ascii_lowercase()
    }
}

struct QueryEngChannel {
    query_req_publisher: Publisher<ipc::Service, [u8; QUERY_REQ_PAYLOAD], ()>,
}

impl QueryEngChannel {
    fn new(exchange: &str) -> Result<Self> {
        let query_req_service = build_service_name(&format!("query_reqs/{}", exchange));
        let query_resp_service = build_service_name(&format!("query_resps/{}", exchange));

        let req_node = NodeBuilder::new()
            .name(&NodeName::new(&format!("pre_trade_query_req_{}", exchange))?)
            .create::<ipc::Service>()?;

        let req_service = req_node
            .service_builder(&ServiceName::new(&query_req_service)?)
            .publish_subscribe::<[u8; QUERY_REQ_PAYLOAD]>()
            .open_or_create()?;

        let query_req_publisher = req_service.publisher_builder().create()?;
        info!(
            "QueryEngHub: query request publisher created on '{}' (exchange={})",
            query_req_service, exchange
        );

        let resp_service_name = query_resp_service.clone();
        let exchange_name = exchange.to_string();
        tokio::task::spawn_local(async move {
            if let Err(err) =
                Self::run_query_resp_listener(&exchange_name, &resp_service_name).await
            {
                warn!(
                    "Query response listener exited (exchange={} service={}): {err:?}",
                    exchange_name, resp_service_name
                );
            }
        });

        Ok(Self { query_req_publisher })
    }

    fn publish_query_request(&self, bytes: &Bytes) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }

        if bytes.len() > QUERY_REQ_PAYLOAD {
            warn!(
                "Query request truncated: len={} capacity={}",
                bytes.len(),
                QUERY_REQ_PAYLOAD
            );
        }

        let mut buf = [0u8; QUERY_REQ_PAYLOAD];
        let copy_len = bytes.len().min(QUERY_REQ_PAYLOAD);
        buf[..copy_len].copy_from_slice(&bytes[..copy_len]);

        let sample = self.query_req_publisher.loan_uninit()?;
        let sample = sample.write_payload(buf);
        sample.send()?;
        Ok(())
    }

    async fn run_query_resp_listener(exchange: &str, service_name: &str) -> Result<()> {
        let node = NodeBuilder::new()
            .name(&NodeName::new(&format!("pre_trade_query_resp_{}", exchange))?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(service_name)?)
            .publish_subscribe::<[u8; QUERY_RESP_PAYLOAD]>()
            .open_or_create()?;

        let subscriber: Subscriber<ipc::Service, [u8; QUERY_RESP_PAYLOAD], ()> =
            service.subscriber_builder().create()?;

        info!(
            "QueryEngHub: query response subscribed on '{}' (exchange={})",
            service_name, exchange
        );

        loop {
            match subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = sample.payload();
                    match QueryEngineResponseMessage::from_payload(payload) {
                        Ok(resp) => {
                            debug!(
                                "queryResponse: exchange={} type={} cli_qid={} body_len={}",
                                exchange,
                                resp.req_type(),
                                resp.client_query_id(),
                                resp.body_bytes().len()
                            );
                        }
                        Err(err) => {
                            warn!("failed to decode query response (exchange={}): {err:#}", exchange)
                        }
                    }
                }
                Ok(None) => tokio::task::yield_now().await,
                Err(err) => {
                    warn!("Query response receive error: {err}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
}
