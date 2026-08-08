use iceoryx2::port::publisher::Publisher;
use iceoryx2::service::ipc;
use log::{debug, warn};
use order_common::trade_error_code::{bitget, bybit, gate};
use order_common::TradeRequestType;
use runtime_common::exchange::Exchange;
use serde_json::Value;
use tokio::sync::mpsc;

const MAX_TRADE_RESP_ERROR_DETAIL_CHARS: usize = 512;
const BINANCE_NEW_ORDER_REJECTED: i32 = -2010;
const BINANCE_BALANCE_INSUFFICIENT: i32 = -2018;
const BINANCE_POST_ONLY_REJECTED: i32 = -5022;

// REST 请求执行后的输出（内部使用）
#[derive(Debug, Clone)]
pub struct TradeExecOutcome {
    pub req_type: TradeRequestType,
    pub client_order_id: i64,
    pub status: u16,
    pub body: String,
    pub exchange: Exchange,
    pub order_id: i64,
    pub order_status_u8: u8,
    pub order_update_time: i64,
    pub executed_qty: f64,
    pub response_price: f64,
}

fn extract_code(v: &Value) -> Option<i32> {
    let code = v.get("code")?;
    if let Some(n) = code.as_i64() {
        return i32::try_from(n).ok();
    }
    if let Some(s) = code.as_str() {
        return s.parse::<i32>().ok();
    }
    None
}

fn extract_s_code(v: &Value) -> Option<i32> {
    let data = v.get("data")?;
    let maybe_item = if let Some(arr) = data.as_array() {
        arr.first()
    } else if data.is_object() {
        Some(data)
    } else {
        None
    }?;
    let s_code = maybe_item.get("sCode")?;
    if let Some(n) = s_code.as_i64() {
        return i32::try_from(n).ok();
    }
    if let Some(s) = s_code.as_str() {
        return s.parse::<i32>().ok();
    }
    None
}

fn extract_msg(v: &Value) -> Option<String> {
    if let Some(s) = v
        .get("data")
        .and_then(|d| d.as_array())
        .and_then(|arr| arr.first())
        .and_then(|first| first.get("sMsg"))
        .and_then(|m| m.as_str())
    {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    if let Some(s) = v
        .get("data")
        .and_then(|d| d.get("errs"))
        .and_then(|e| e.get("label"))
        .and_then(|m| m.as_str())
    {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    if let Some(s) = v
        .get("data")
        .and_then(|d| d.get("errs"))
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
    {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    if let Some(s) = v.get("msg").and_then(|m| m.as_str()) {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    if let Some(s) = v.get("message").and_then(|m| m.as_str()) {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    if let Some(s) = v.get("label").and_then(|m| m.as_str()) {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    None
}

fn parse_error_code_and_msg(body: &str) -> (i32, Option<String>) {
    let body = body.trim();
    if body.is_empty() {
        return (0, None);
    }

    // Some WS wrappers include nested raw/payload text.
    if let Ok(v) = serde_json::from_str::<Value>(body) {
        if let Some(raw) = v.get("raw").and_then(|r| r.as_str()) {
            return parse_error_code_and_msg(raw);
        } else if let Some(payload) = v.get("payload").and_then(|p| p.as_str()) {
            return parse_error_code_and_msg(payload);
        }
        let mut code = extract_code(&v).unwrap_or(0);
        if let Some(s_code) = extract_s_code(&v) {
            if s_code != 0 {
                code = s_code;
            }
        }
        let msg = extract_msg(&v);
        return (code, msg);
    }

    (0, None)
}

fn compact_for_log(s: &str) -> String {
    s.trim().split_whitespace().collect::<Vec<_>>().join(" ")
}

fn truncate_for_log(s: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (idx, ch) in s.chars().enumerate() {
        if idx >= max_chars {
            out.push_str("...");
            return out;
        }
        out.push(ch);
    }
    out
}

fn trade_error_detail_for_log(msg: Option<&str>, body: &str) -> Option<String> {
    if let Some(msg) = msg {
        let msg = compact_for_log(msg);
        if !msg.is_empty() {
            return Some(format!(
                "msg={}",
                truncate_for_log(&msg, MAX_TRADE_RESP_ERROR_DETAIL_CHARS)
            ));
        }
    }

    let body = compact_for_log(body);
    if body.is_empty() {
        None
    } else {
        Some(format!(
            "body={}",
            truncate_for_log(&body, MAX_TRADE_RESP_ERROR_DETAIL_CHARS)
        ))
    }
}

fn is_binance_post_only_reject_msg(msg: &str) -> bool {
    let msg = msg.to_ascii_lowercase();
    msg.contains("would immediately match and take") || msg.contains("post only")
}

fn is_binance_insufficient_balance_msg(msg: &str) -> bool {
    let msg = msg.to_ascii_lowercase();
    msg.contains("insufficient balance")
}

fn is_bitget_margin_risk_msg(msg: &str) -> bool {
    let msg = msg.to_ascii_lowercase();
    [
        "insufficient margin",
        "insufficient balance",
        "account at risk",
        "forced liquidation",
    ]
    .iter()
    .any(|needle| msg.contains(needle))
}

fn normalize_trade_error(
    exchange: Exchange,
    code: i32,
    msg: Option<String>,
) -> (i32, Option<String>) {
    if exchange == Exchange::Binance && code == BINANCE_NEW_ORDER_REJECTED {
        if msg.as_deref().is_some_and(is_binance_post_only_reject_msg) {
            return (BINANCE_POST_ONLY_REJECTED, msg);
        }
        if msg
            .as_deref()
            .is_some_and(is_binance_insufficient_balance_msg)
        {
            return (BINANCE_BALANCE_INSUFFICIENT, msg);
        }
    }

    if exchange == Exchange::Gate && code == 0 {
        if let Some(m) = msg.as_deref() {
            if let Some(mapped_code) = gate::parse_error_label(m) {
                return (mapped_code, msg);
            }
        }
    }

    if exchange == Exchange::Bitget
        && code != bitget::UTA_INSUFFICIENT_MARGIN
        && msg.as_deref().is_some_and(is_bitget_margin_risk_msg)
    {
        return (bitget::UTA_INSUFFICIENT_MARGIN, msg);
    }

    (code, msg)
}

fn is_cancel_request(req_type: TradeRequestType) -> bool {
    matches!(
        req_type,
        TradeRequestType::BinanceCancelUMOrder
            | TradeRequestType::BinanceCancelMarginOrder
            | TradeRequestType::BinanceCancelUMConditionalOrder
            | TradeRequestType::BinanceWsCancelUMOrder
            | TradeRequestType::BinanceWsCancelMarginOrder
            | TradeRequestType::OkexCancelMarginOrder
            | TradeRequestType::OkexCancelUMOrder
            | TradeRequestType::GateUnifiedCancelOrder
            | TradeRequestType::GateFuturesCancelOrder
            | TradeRequestType::BybitCancelMarginOrder
            | TradeRequestType::BybitCancelUMOrder
            | TradeRequestType::BitgetCancelMarginOrder
            | TradeRequestType::BitgetCancelUMOrder
            | TradeRequestType::BitgetCancelSpotOrder
    )
}

fn is_cancel_not_cancellable(exchange: Exchange, error_code: i32) -> bool {
    match exchange {
        Exchange::Binance => error_code == -2011,
        Exchange::Okex => matches!(error_code, 51400 | 51410 | 51416),
        Exchange::Gate => error_code == gate::ORDER_NOT_FOUND,
        Exchange::Bybit => matches!(
            error_code,
            110001
                | 110008
                | 110010
                | 170139
                | 170142
                | 170143
                | 170145
                | 170190
                | 170191
                | bybit::ORDER_NOT_FOUND
        ),
        Exchange::Bitget => matches!(
            error_code,
            22001 | 25204 | 43001 | 43004 | 45031 | 45055 | 45057
        ),
        _ => false,
    }
}

fn should_downgrade_trade_resp_error(out: &TradeExecOutcome, error_code: i32) -> bool {
    is_cancel_request(out.req_type) && is_cancel_not_cancellable(out.exchange, error_code)
}

fn is_post_only_rejected(exchange: Exchange, error_code: i32) -> bool {
    match exchange {
        Exchange::Binance => error_code == BINANCE_POST_ONLY_REJECTED,
        Exchange::Okex => error_code == 51511,
        Exchange::Gate => error_code == gate::ORDER_POC,
        Exchange::Bybit => matches!(error_code, 170217 | 170218),
        Exchange::Bitget => false,
        _ => false,
    }
}

fn should_suppress_trade_resp_error_log(out: &TradeExecOutcome, error_code: i32) -> bool {
    out.req_type.is_new_order() && is_post_only_rejected(out.exchange, error_code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefers_okx_s_code_when_present() {
        let body = r#"{"code":"1","data":[{"sCode":"51006","sMsg":"Order price is not within the price limit"}],"msg":"All operations failed"}"#;
        let (code, msg) = parse_error_code_and_msg(body);
        assert_eq!(code, 51006);
        assert_eq!(
            msg.as_deref(),
            Some("Order price is not within the price limit")
        );
    }

    #[test]
    fn falls_back_to_top_level_code() {
        let body = r#"{"code":-5022,"msg":"Post Only order would be filled"}"#;
        let (code, msg) = parse_error_code_and_msg(body);
        assert_eq!(code, -5022);
        assert_eq!(msg.as_deref(), Some("Post Only order would be filled"));
    }

    #[test]
    fn logs_non_json_transport_error_body() {
        let detail = trade_error_detail_for_log(None, "request error: operation timed out\n");
        assert_eq!(
            detail.as_deref(),
            Some("body=request error: operation timed out")
        );
    }

    #[test]
    fn normalizes_gate_order_not_found_from_message() {
        let body = r#"{"header":{"status":400},"data":{"errs":{"label":"ORDER_NOT_FOUND","message":"ORDER_NOT_FOUND"}}}"#;
        let (code, msg) = parse_error_code_and_msg(body);
        let (code, msg) = normalize_trade_error(Exchange::Gate, code, msg);
        assert_eq!(code, gate::ORDER_NOT_FOUND);
        assert_eq!(msg.as_deref(), Some("ORDER_NOT_FOUND"));
    }

    #[test]
    fn normalizes_gate_risk_check_market_forbidden_from_message() {
        let body = r#"{"header":{"status":403},"data":{"errs":{"label":"RISK_CHECK_MARKET_FORBIDDEN","message":"Risk management requirements prohibit operations."}}}"#;
        let (code, msg) = parse_error_code_and_msg(body);
        let (code, msg) = normalize_trade_error(Exchange::Gate, code, msg);
        assert_eq!(code, gate::RISK_CHECK_MARKET_FORBIDDEN);
        assert_eq!(msg.as_deref(), Some("RISK_CHECK_MARKET_FORBIDDEN"));
    }

    #[test]
    fn normalizes_binance_limit_maker_cross_reject_from_message() {
        let body = r#"{"code":-2010,"msg":"Order would immediately match and take."}"#;
        let (code, msg) = parse_error_code_and_msg(body);
        let (code, msg) = normalize_trade_error(Exchange::Binance, code, msg);
        assert_eq!(code, BINANCE_POST_ONLY_REJECTED);
        assert_eq!(
            msg.as_deref(),
            Some("Order would immediately match and take.")
        );
    }

    #[test]
    fn normalizes_binance_spot_insufficient_balance_from_message() {
        let body =
            r#"{"code":-2010,"msg":"Account has insufficient balance for requested action."}"#;
        let (code, msg) = parse_error_code_and_msg(body);
        let (code, msg) = normalize_trade_error(Exchange::Binance, code, msg);
        assert_eq!(code, BINANCE_BALANCE_INSUFFICIENT);
        assert_eq!(
            msg.as_deref(),
            Some("Account has insufficient balance for requested action.")
        );
    }

    #[test]
    fn keeps_other_binance_new_order_rejected_messages() {
        let body = r#"{"code":-2010,"msg":"New order rejected"}"#;
        let (code, msg) = parse_error_code_and_msg(body);
        let (code, msg) = normalize_trade_error(Exchange::Binance, code, msg);
        assert_eq!(code, BINANCE_NEW_ORDER_REJECTED);
        assert_eq!(msg.as_deref(), Some("New order rejected"));
    }

    #[test]
    fn normalizes_bitget_unknown_margin_risk_messages() {
        for message in [
            "Insufficient margin",
            "Insufficient balance for this order",
            "Account at risk, trading temporarily disabled",
            "Account is in forced liquidation status",
        ] {
            let (code, msg) =
                normalize_trade_error(Exchange::Bitget, 29999, Some(message.to_string()));
            assert_eq!(code, bitget::UTA_INSUFFICIENT_MARGIN);
            assert_eq!(msg.as_deref(), Some(message));
        }
    }

    #[test]
    fn keeps_unrelated_bitget_unknown_message() {
        let (code, _) = normalize_trade_error(
            Exchange::Bitget,
            29999,
            Some("Order price is invalid".to_string()),
        );
        assert_eq!(code, 29999);
    }
    #[test]
    fn normalizes_gate_order_poc_from_label() {
        let body = r#"{"header":{"status":400},"data":{"errs":{"label":"ORDER_POC","message":"poc order would be filled immediately"}}}"#;
        let (code, msg) = parse_error_code_and_msg(body);
        let (code, msg) = normalize_trade_error(Exchange::Gate, code, msg);
        assert_eq!(code, gate::ORDER_POC);
        assert_eq!(msg.as_deref(), Some("ORDER_POC"));
    }

    #[test]
    fn normalizes_gate_poc_fill_immediately_from_label() {
        let body = r#"{"header":{"status":400},"data":{"errs":{"label":"POC_FILL_IMMEDIATELY","message":"poc order would be filled immediately"}}}"#;
        let (code, msg) = parse_error_code_and_msg(body);
        let (code, msg) = normalize_trade_error(Exchange::Gate, code, msg);
        assert_eq!(code, gate::ORDER_POC);
        assert_eq!(msg.as_deref(), Some("POC_FILL_IMMEDIATELY"));
    }

    #[test]
    fn normalizes_gate_auto_borrow_too_much_from_label() {
        let body = r#"{"header":{"status":400},"data":{"errs":{"label":"AUTO_BORROW_TOO_MUCH","message":"auto borrow too much"}}}"#;
        let (code, msg) = parse_error_code_and_msg(body);
        let (code, msg) = normalize_trade_error(Exchange::Gate, code, msg);
        assert_eq!(code, gate::AUTO_BORROW_TOO_MUCH);
        assert_eq!(msg.as_deref(), Some("AUTO_BORROW_TOO_MUCH"));
    }

    fn sample_outcome(req_type: TradeRequestType, exchange: Exchange) -> TradeExecOutcome {
        TradeExecOutcome {
            req_type,
            client_order_id: 1,
            status: 206,
            body: String::new(),
            exchange,
            order_id: 0,
            order_status_u8: 0,
            order_update_time: 0,
            executed_qty: 0.0,
            response_price: 0.0,
        }
    }

    #[test]
    fn downgrades_okx_terminal_cancel_errors() {
        let out = sample_outcome(TradeRequestType::OkexCancelUMOrder, Exchange::Okex);
        assert!(should_downgrade_trade_resp_error(&out, 51400));
        assert!(should_downgrade_trade_resp_error(&out, 51410));
        assert!(should_downgrade_trade_resp_error(&out, 51416));
        assert!(!should_downgrade_trade_resp_error(&out, 51412));
    }

    #[test]
    fn downgrades_bybit_order_not_found_cancel_error() {
        let out = sample_outcome(TradeRequestType::BybitCancelMarginOrder, Exchange::Bybit);
        assert!(should_downgrade_trade_resp_error(
            &out,
            bybit::ORDER_NOT_FOUND
        ));
    }

    #[test]
    fn downgrades_bitget_order_not_exist_cancel_error() {
        let out = sample_outcome(TradeRequestType::BitgetCancelMarginOrder, Exchange::Bitget);
        assert!(should_downgrade_trade_resp_error(&out, 25204));
    }

    #[test]
    fn does_not_downgrade_non_cancel_requests() {
        let out = sample_outcome(TradeRequestType::OkexNewUMOrder, Exchange::Okex);
        assert!(!should_downgrade_trade_resp_error(&out, 51400));
    }

    #[test]
    fn classifies_binance_ws_margin_cross_as_post_only_reject() {
        let body = r#"{"code":-2010,"msg":"Order would immediately match and take."}"#;
        let (code, msg) = parse_error_code_and_msg(body);
        let (code, _) = normalize_trade_error(Exchange::Binance, code, msg);
        let out = sample_outcome(TradeRequestType::BinanceWsNewMarginOrder, Exchange::Binance);

        assert_eq!(code, BINANCE_POST_ONLY_REJECTED);
        assert!(should_suppress_trade_resp_error_log(&out, code));
        assert!(!should_downgrade_trade_resp_error(&out, code));
    }

    #[test]
    fn does_not_classify_cancel_as_post_only_reject() {
        let out = sample_outcome(
            TradeRequestType::BinanceWsCancelMarginOrder,
            Exchange::Binance,
        );
        assert!(!should_suppress_trade_resp_error_log(
            &out,
            BINANCE_POST_ONLY_REJECTED
        ));
    }
}

pub fn publish_trade_response(
    publisher: &Publisher<ipc::Service, [u8; 64], ()>,
    out: TradeExecOutcome,
) {
    let (error_code, msg) = parse_error_code_and_msg(&out.body);
    let (error_code, msg) = normalize_trade_error(out.exchange, error_code, msg);
    let is_2xx = (200..300).contains(&(out.status as u32));
    if !is_2xx || error_code != 0 {
        let downgrade = should_downgrade_trade_resp_error(&out, error_code);
        if !should_suppress_trade_resp_error_log(&out, error_code) {
            let detail = trade_error_detail_for_log(msg.as_deref(), &out.body);
            if let Some(detail) = detail.as_deref() {
                if downgrade {
                    debug!(
                        "trade resp benign cancel terminal: ex={:?} type={:?} cli_ord_id={} status={} code={} {}",
                        out.exchange,
                        out.req_type,
                        out.client_order_id,
                        out.status,
                        error_code,
                        detail
                    );
                } else {
                    warn!(
                        "trade resp error: ex={:?} type={:?} cli_ord_id={} status={} code={} {}",
                        out.exchange,
                        out.req_type,
                        out.client_order_id,
                        out.status,
                        error_code,
                        detail
                    );
                }
            } else if downgrade {
                debug!(
                    "trade resp benign cancel terminal: ex={:?} type={:?} cli_ord_id={} status={} code={}",
                    out.exchange,
                    out.req_type,
                    out.client_order_id,
                    out.status,
                    error_code
                );
            } else {
                warn!(
                    "trade resp error: ex={:?} type={:?} cli_ord_id={} status={} code={}",
                    out.exchange, out.req_type, out.client_order_id, out.status, error_code
                );
            }
        }
    }

    let req_type = out.req_type as u32;
    let exchange = out.exchange as u32;
    let mut buf = [0u8; 64];
    buf[0..4].copy_from_slice(&req_type.to_le_bytes());
    buf[4..12].copy_from_slice(&out.client_order_id.to_le_bytes());
    buf[12..16].copy_from_slice(&exchange.to_le_bytes());
    buf[16..18].copy_from_slice(&out.status.to_le_bytes());
    buf[18..22].copy_from_slice(&error_code.to_le_bytes());
    let h = 22;
    if buf.len() >= h + 33 {
        buf[h..h + 8].copy_from_slice(&out.order_id.to_le_bytes());
        buf[h + 8] = out.order_status_u8;
        buf[h + 9..h + 17].copy_from_slice(&out.order_update_time.to_le_bytes());
        buf[h + 17..h + 25].copy_from_slice(&out.executed_qty.to_le_bytes());
        buf[h + 25..h + 33].copy_from_slice(&out.response_price.to_le_bytes());
    }
    debug!(
        "publish trade resp header: type={}, status={}, code={}",
        req_type, out.status, error_code
    );
    if let Ok(sample) = publisher.loan_uninit() {
        let sample = sample.write_payload(buf);
        let _ = sample.send();
    }
}

pub fn spawn_response_handle(
    publisher: Publisher<ipc::Service, [u8; 64], ()>,
    mut resp_rx: mpsc::UnboundedReceiver<TradeExecOutcome>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_local(async move {
        while let Some(out) = resp_rx.recv().await {
            publish_trade_response(&publisher, out);
        }
    })
}
