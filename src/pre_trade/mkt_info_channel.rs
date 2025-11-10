use crate::common::msg_parser::{get_msg_type, parse_index_price, parse_mark_price, MktMsgType};
use crate::pre_trade::price_table::PriceTable;
use anyhow::Result;
use bytes::Bytes;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use log::{error, info, warn};

const NODE_PRE_TRADE_DERIVATIVES: &str = "pre_trade_derivatives";
const DERIVATIVES_SERVICE: &str = "data_pubs/binance-futures/derivatives";
const DERIVATIVES_PAYLOAD: usize = 128;

/// 市场信息频道 - 负责订阅和管理衍生品数据（标记价格、指数价格）
pub struct MktInfoChannel {
    price_table: Rc<RefCell<PriceTable>>,
}

impl MktInfoChannel {
    pub fn new(price_table: Rc<RefCell<PriceTable>>) -> Self {
        Self { price_table }
    }

    pub fn get_price_table(&self) -> Rc<RefCell<PriceTable>> {
        self.price_table.clone()
    }

    /// 启动衍生品数据订阅任务
    pub fn spawn_worker(&self) -> Result<()> {
        let price_table = self.price_table.clone();

        tokio::task::spawn_local(async move {
            if let Err(err) = Self::run_derivatives_subscriber(price_table).await {
                error!("derivatives worker exited: {err:?}");
            }
        });
        Ok(())
    }

    /// 订阅并处理衍生品数据（标记价格、指数价格）
    async fn run_derivatives_subscriber(price_table: Rc<RefCell<PriceTable>>) -> Result<()> {
        let node = NodeBuilder::new()
            .name(&NodeName::new(NODE_PRE_TRADE_DERIVATIVES)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(DERIVATIVES_SERVICE)?)
            .publish_subscribe::<[u8; DERIVATIVES_PAYLOAD]>()
            .open_or_create()?;

        let subscriber: Subscriber<ipc::Service, [u8; DERIVATIVES_PAYLOAD], ()> =
            service.subscriber_builder().create()?;

        info!("derivatives metrics subscribed: service={}", service.name());

        loop {
            match subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = Bytes::copy_from_slice(sample.payload());
                    if payload.is_empty() {
                        continue;
                    }

                    let Some(msg_type) = get_msg_type(&payload) else {
                        continue;
                    };

                    match msg_type {
                        MktMsgType::MarkPrice => match parse_mark_price(&payload) {
                            Ok(msg) => {
                                let mut table = price_table.borrow_mut();
                                table.update_mark_price(&msg.symbol, msg.mark_price, msg.timestamp);
                            }
                            Err(err) => warn!("parse mark price failed: {err:?}"),
                        },
                        MktMsgType::IndexPrice => match parse_index_price(&payload) {
                            Ok(msg) => {
                                let mut table = price_table.borrow_mut();
                                table.update_index_price(&msg.symbol, msg.index_price, msg.timestamp);
                            }
                            Err(err) => warn!("parse index price failed: {err:?}"),
                        },
                        _ => {}
                    }
                }
                Ok(None) => {
                    tokio::task::yield_now().await;
                }
                Err(err) => {
                    warn!("derivatives stream receive error: {err}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
}
