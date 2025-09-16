use crate::common::exchange::Exchange;
use crate::trade_engine::dispatcher::Dispatcher;
use crate::trade_engine::order_event::OrderRequestEvent;
use crate::trade_engine::trade_request::TradeRequestMsg;
use crate::trade_engine::trade_type_mapping::TradeTypeMapping;
use tokio::sync::mpsc;
use crate::trade_engine::trade_response_handle::TradeExecOutcome;
use log::debug;

/// 启动请求执行器：
/// - 从 mpsc::UnboundedReceiver<TradeRequestMsg> 读取请求（二进制头+参数）
/// - 通过 TradeTypeMapping 映射出 endpoint/method/weight
/// - 交给 Dispatcher 执行 HTTP
/// - 将 HTTP body 原样作为二进制 Bytes 通过 resp_tx 发送
pub fn spawn_request_executor(
    mut dispatcher: Dispatcher,
    exchange: Exchange,
    mut req_rx: mpsc::UnboundedReceiver<TradeRequestMsg>,
    resp_tx: mpsc::UnboundedSender<TradeExecOutcome>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_local(async move {
        //当req-rx监听到消息，进行阻塞式处理
        while let Some(msg) = req_rx.recv().await {
            //mapping基本的请求方式和url，后续可以兼容其他交易所
            let endpoint = TradeTypeMapping::get_endpoint(msg.req_type).to_string();
            let method = TradeTypeMapping::get_method(msg.req_type).to_string();
            let weight = TradeTypeMapping::get_weight(msg.req_type);
            debug!(
                "dispatch mapping: type={:?} -> {} {} (weight={})",
                msg.req_type, method, endpoint, weight
            );

            // Try to parse params as query string into key/value pairs
            let params: std::collections::BTreeMap<String, String> = match std::str::from_utf8(&msg.params) {
                Ok(s) => url::form_urlencoded::parse(s.as_bytes())
                    .into_owned()
                    .collect(),
                Err(_) => {
                    // Not valid UTF-8, proceed with empty params
                    std::collections::BTreeMap::new()
                }
            };

            let evt = OrderRequestEvent {
                endpoint,
                method,
                params,
                weight: Some(weight),
                account: None,
                // Use client_order_id as req_id for correlation
                req_id: Some(msg.client_order_id.to_string()),
            };
            debug!(
                "order event built: endpoint={}, method={}, params_count={}, req_id={}",
                evt.endpoint, evt.method, evt.params.len(), evt.req_id.as_deref().unwrap_or("")
            );

            match dispatcher.dispatch(evt).await {
                Ok(outcome) => {
                    debug!(
                        "http outcome: status={}, ip={}, used_weight_1m={:?}, order_count_1m={:?}, body_len={}",
                        outcome.status,
                        outcome.ip,
                        outcome.ip_used_weight_1m,
                        outcome.order_count_1m,
                        outcome.body.len()
                    );
                    let _ = resp_tx.send(TradeExecOutcome{
                        req_type: msg.req_type,
                        client_order_id: msg.client_order_id,
                        status: outcome.status,
                        body: outcome.body,
                        exchange,
                        ip_used_weight_1m: outcome.ip_used_weight_1m,
                        order_count_1m: outcome.order_count_1m,
                    });
                }
                Err(e) => {
                    debug!("http error: {}", e);
                    let _ = resp_tx.send(TradeExecOutcome{
                        req_type: msg.req_type,
                        client_order_id: msg.client_order_id,
                        status: 0,
                        body: e.to_string(),
                        exchange,
                        ip_used_weight_1m: None,
                        order_count_1m: None,
                    });
                }
            }
        }
    })
}

// Note: JSON响应发布器已不再使用，响应发布统一在 trade_response_handle 中实现为二进制转发
