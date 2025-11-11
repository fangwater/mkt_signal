//前端展示采样频道常量
pub const FR_RESAMPLE_MSG_CHANNEL: &str = "binance_fr_signal_resample_msg";
pub const PRE_TRADE_POSITIONS_CHANNEL: &str = "pre_trade_positions_resample";
pub const PRE_TRADE_EXPOSURE_CHANNEL: &str = "pre_trade_exposure_resample";
pub const PRE_TRADE_RISK_CHANNEL: &str = "pre_trade_risk_resample";
pub const PRE_TRADE_RESAMPLE_MSG_CHANNEL: &str = "pre_trade_resample_msg";

//交易相关常量
 
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

