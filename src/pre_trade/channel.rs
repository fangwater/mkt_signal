//前端展示采样频道常量
pub const FR_RESAMPLE_MSG_CHANNEL: &str = "binance_fr_signal_resample_msg";
pub const PRE_TRADE_POSITIONS_CHANNEL: &str = "pre_trade_positions_resample";
pub const PRE_TRADE_EXPOSURE_CHANNEL: &str = "pre_trade_exposure_resample";
pub const PRE_TRADE_RISK_CHANNEL: &str = "pre_trade_risk_resample";
pub const PRE_TRADE_RESAMPLE_MSG_CHANNEL: &str = "pre_trade_resample_msg";

//交易相关常量
const TRADE_RESP_PAYLOAD: usize = 16_384;
const TRADE_REQ_PAYLOAD: usize = 4_096;
const NODE_PRE_TRADE_ORDER_REQ: &str = "pre_trade_order_req";

struct TradeEngChannel{
    //交易请求从pre-trade发送给trade engine，pub
    binance_trade_resquest_node: Node<ipc::Service>,
    binance_trade_resquest: Publisher<ipc::Service, [u8; TRADE_REQ_PAYLOAD], ()>,
    binance_trade_response: 
    binance_trade
}

impl TradeChannel {
    fn publish_trade_request(&self, bytes: &Bytes) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        if bytes.len() > TRADE_REQ_PAYLOAD {
            warn!("trade request truncated: len={} capacity={}", bytes.len(), TRADE_REQ_PAYLOAD);
        }
        let mut buf = [0u8; TRADE_REQ_PAYLOAD];
        let copy_len = bytes.len().min(TRADE_REQ_PAYLOAD);
        buf[..copy_len].copy_from_slice(&bytes[..copy_len]);
        let sample = self.publisher.loan_uninit()?;
        let sample = sample.write_payload(buf);
        sample.send()?;
        Ok(())
    }
    fn new(){
        let node = NodeBuilder::new()
            .name(&NodeName::new(NODE_PRE_TRADE_ORDER_REQ)?)
            .create::<ipc::Service>()?;
        let service = node
            .service_builder(&ServiceName::new(service)?)
            .publish_subscribe::<[u8; ORDER_REQ_PAYLOAD]>()
            .subscriber_max_buffer_size(256)
            .open_or_create()?;
        let publisher = service.publisher_builder().create()?;
    }
   fn spawn_trade_response_listener(cfg: &TradeEngineRespCfg) -> Result<UnboundedReceiver<TradeExecOutcome>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let service_name = cfg.service.clone();
        let label = cfg.label.clone().unwrap_or_else(|| cfg.service.clone());

        tokio::task::spawn_local(async move {
            let node_name = NODE_PRE_TRADE_TRADE_RESP.to_string();
            let result = async move {
                let node = NodeBuilder::new()
                    .name(&NodeName::new(&node_name)?)
                    .create::<ipc::Service>()?;

                let service = node
                    .service_builder(&ServiceName::new(&service_name)?)
                    .publish_subscribe::<[u8; TRADE_RESP_PAYLOAD]>()
                    .subscriber_max_buffer_size(256)
                    .open_or_create()?;
                let subscriber: Subscriber<ipc::Service, [u8; TRADE_RESP_PAYLOAD], ()> =
                    service.subscriber_builder().create()?;

                info!(
                    "trade response subscribed: service={} label={}",
                    service_name, label
                );

                loop {
                    match subscriber.receive() {
                        Ok(Some(sample)) => {
                            let raw = Bytes::copy_from_slice(sample.payload());
                            if raw.is_empty() {
                                continue;
                            }
                            match TradeExecOutcome::parse(raw.as_ref()) {
                                Some(event) => {
                                    if tx.send(event).is_err() {
                                        break;
                                    }
                                }
                                None => warn!("failed to parse trade response payload"),
                            }
                        }
                        Ok(None) => tokio::task::yield_now().await,
                        Err(err) => {
                            warn!("trade response receive error: {err}");
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            };

            if let Err(err) = result.await {
                warn!("trade response listener exited: {err:?}");
            }
        });

        Ok(rx)
    }
    fn handle_trade_engine_response(_ctx: &mut RuntimeContext, outcome: TradeExecOutcome) {
        //暂时不处理TradeExec 只观察http响应的正确性
        match outcome.status {
            200 => {
                // 成功响应，不打印日志
            }
            403 => {
                warn!(
                    "WAF Limit violated: exchange={:?} req_type={:?} cli_ord_id={} body={}",
                    outcome.exchange, outcome.req_type, outcome.client_order_id, outcome.body
                );
            }
            418 => {
                warn!(
                    "IP auto-banned for continuing requests after 429: exchange={:?} req_type={:?} cli_ord_id={} body={}",
                    outcome.exchange,
                    outcome.req_type,
                    outcome.client_order_id,
                    outcome.body
                );
            }
            429 => {
                warn!(
                    "Request rate limit exceeded: exchange={:?} req_type={:?} cli_ord_id={} ip_weight={:?} order_count={:?} body={}",
                    outcome.exchange,
                    outcome.req_type,
                    outcome.client_order_id,
                    outcome.ip_used_weight_1m,
                    outcome.order_count_1m,
                    outcome.body
                );
            }
            503 => {
                warn!(
                    "Service unavailable (503): exchange={:?} req_type={:?} cli_ord_id={} body={}",
                    outcome.exchange, outcome.req_type, outcome.client_order_id, outcome.body
                );
            }
            400..=499 => {
                warn!(
                    "Client error (4xx): status={} exchange={:?} req_type={:?} cli_ord_id={} body={}",
                    outcome.status,
                    outcome.exchange,
                    outcome.req_type,
                    outcome.client_order_id,
                    outcome.body
                );
            }
            500..=599 => {
                warn!(
                    "Server error (5xx): status={} exchange={:?} req_type={:?} cli_ord_id={} body={}",
                    outcome.status,
                    outcome.exchange,
                    outcome.req_type,
                    outcome.client_order_id,
                    outcome.body
                );
            }
            _ => {
                warn!(
                    "Unexpected HTTP status: status={} exchange={:?} req_type={:?} cli_ord_id={} body={}",
                    outcome.status,
                    outcome.exchange,
                    outcome.req_type,
                    outcome.client_order_id,
                    outcome.body
                );
            }
        }
    }
}

//持久化记录，包括信号记录和订单记录
struct PersistChannel{
    order_record_tx: UnboundedSender<Bytes>,
    signal_record_pub: Option<SignalPublisher>,
}

