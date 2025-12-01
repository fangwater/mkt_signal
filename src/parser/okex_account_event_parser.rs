//! OKX 账户事件解析器
//!
//! 解析 OKX 订单频道推送的订单状态变化消息，
//! 转换为与 Binance 兼容的内部消息格式。

use crate::common::account_msg::{AccountEventMsg, AccountEventType, OrderTradeUpdateMsg};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use log::{debug, warn};
use tokio::sync::mpsc;

/// OKX 账户事件解析器
#[derive(Clone)]
pub struct OkexAccountEventParser;

impl OkexAccountEventParser {
    pub fn new() -> Self {
        Self
    }

    /// 解析订单频道消息
    ///
    /// OKX 订单消息格式:
    /// ```json
    /// {
    ///     "arg": {"channel": "orders", "instType": "SWAP"},
    ///     "data": [{
    ///         "instId": "BTC-USDT-SWAP",
    ///         "ordId": "123456",
    ///         "clOrdId": "client_order_1",
    ///         "px": "50000",
    ///         "sz": "1",
    ///         "side": "buy",
    ///         "ordType": "limit",
    ///         "state": "filled",
    ///         "fillPx": "49999",
    ///         "fillSz": "1",
    ///         "avgPx": "49999",
    ///         "accFillSz": "1",
    ///         "fee": "-0.5",
    ///         "feeCcy": "USDT",
    ///         "tradeId": "789",
    ///         "pnl": "10",
    ///         "posSide": "long",
    ///         "reduceOnly": "false",
    ///         "cTime": "1704876947000",
    ///         "uTime": "1704876948000",
    ///         "fillTime": "1704876948000"
    ///     }]
    /// }
    /// ```
    fn parse_order_message(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let data_array = match json_value.get("data").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return 0,
        };

        let mut count = 0;
        for order in data_array {
            if let Some(msg) = self.parse_single_order(order) {
                let payload = msg.to_bytes();
                let event = AccountEventMsg::create(AccountEventType::OrderTradeUpdate, payload);
                if tx.send(event.to_bytes()).is_ok() {
                    count += 1;
                }
            }
        }
        count
    }

    fn parse_single_order(&self, order: &serde_json::Value) -> Option<OrderTradeUpdateMsg> {
        // 解析必需字段
        let inst_id = order.get("instId").and_then(|v| v.as_str())?;
        let ord_id = order.get("ordId").and_then(|v| v.as_str())?;
        let state = order.get("state").and_then(|v| v.as_str())?;

        // 时间戳 (毫秒)
        let _c_time = order
            .get("cTime")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        let u_time = order
            .get("uTime")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);

        // 订单 ID
        let order_id = ord_id.parse::<i64>().unwrap_or(0);

        // 客户订单 ID (尝试解析为数字，失败则用 hash)
        let cl_ord_id_str = order
            .get("clOrdId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let client_order_id = cl_ord_id_str.parse::<i64>().unwrap_or_else(|_| {
            // 如果无法解析为数字，使用字符串 hash
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            cl_ord_id_str.hash(&mut hasher);
            hasher.finish() as i64
        });

        // 成交 ID
        let trade_id = order
            .get("tradeId")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);

        // 价格和数量
        let px = order
            .get("px")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let sz = order
            .get("sz")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let avg_px = order
            .get("avgPx")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let fill_sz = order
            .get("fillSz")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let fill_px = order
            .get("fillPx")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let acc_fill_sz = order
            .get("accFillSz")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);

        // 手续费
        let fee = order
            .get("fee")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let fee_ccy = order
            .get("feeCcy")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // 收益
        let pnl = order
            .get("pnl")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);

        // 方向
        let side_str = order.get("side").and_then(|v| v.as_str()).unwrap_or("");
        let side = match side_str {
            "buy" => 'B',
            "sell" => 'S',
            _ => '?',
        };

        // 持仓方向
        let pos_side_str = order.get("posSide").and_then(|v| v.as_str()).unwrap_or("");
        let position_side = match pos_side_str {
            "long" => 'L',
            "short" => 'S',
            "net" => 'N',
            _ => 'N',
        };

        // 订单类型
        let ord_type = order
            .get("ordType")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // 是否只减仓
        let reduce_only = order
            .get("reduceOnly")
            .and_then(|v| v.as_str())
            .map(|s| s == "true")
            .unwrap_or(false);

        // 转换 OKX 状态到执行类型和订单状态
        let (execution_type, order_status) = Self::convert_state(state);

        // 是否为 maker (需要从 execType 判断，OKX 暂无直接字段)
        let is_maker = order
            .get("execType")
            .and_then(|v| v.as_str())
            .map(|s| s == "M")
            .unwrap_or(false);

        Some(OrderTradeUpdateMsg::create(
            u_time,              // event_time
            u_time,              // transaction_time
            order_id,            // order_id
            trade_id,            // trade_id
            0,                   // strategy_id (OKX 无此字段)
            inst_id.to_string(), // symbol
            client_order_id,     // client_order_id
            side,                // side
            position_side,       // position_side
            is_maker,            // is_maker
            reduce_only,         // reduce_only
            px,                  // price
            sz,                  // quantity
            avg_px,              // average_price
            0.0,                 // stop_price (订单频道无此字段)
            fill_sz,             // last_executed_quantity
            acc_fill_sz,         // cumulative_filled_quantity
            fill_px,             // last_executed_price
            fee.abs(),           // commission_amount (OKX 负数表示扣费)
            0.0,                 // buy_notional
            0.0,                 // sell_notional
            pnl,                 // realized_profit
            ord_type,            // order_type
            "GTC".to_string(),   // time_in_force (默认 GTC)
            execution_type,      // execution_type
            order_status,        // order_status
            fee_ccy,             // commission_asset
            String::new(),       // strategy_type
            String::new(),       // business_unit
            Some(cl_ord_id_str.to_string()), // client_order_id_str
        ))
    }

    /// 将 OKX 订单状态转换为 Binance 风格的执行类型和订单状态
    fn convert_state(state: &str) -> (String, String) {
        match state {
            "live" => ("NEW".to_string(), "NEW".to_string()),
            "partially_filled" => ("TRADE".to_string(), "PARTIALLY_FILLED".to_string()),
            "filled" => ("TRADE".to_string(), "FILLED".to_string()),
            "canceled" => ("CANCELED".to_string(), "CANCELED".to_string()),
            "mmp_canceled" => ("CANCELED".to_string(), "CANCELED".to_string()),
            _ => (state.to_uppercase(), state.to_uppercase()),
        }
    }
}

impl Parser for OkexAccountEventParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let json_str = match std::str::from_utf8(&msg) {
            Ok(s) => s,
            Err(_) => return 0,
        };

        let json_value: serde_json::Value = match serde_json::from_str(json_str) {
            Ok(v) => v,
            Err(_) => return 0,
        };

        // 检查频道类型
        let channel = json_value
            .get("arg")
            .and_then(|arg| arg.get("channel"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        match channel {
            "orders" => self.parse_order_message(&json_value, tx),
            "balance_and_position" => {
                // TODO: 实现余额和持仓解析
                debug!("OKX: balance_and_position message: {}", json_str);
                0
            }
            "positions" => {
                // TODO: 实现持仓解析
                debug!("OKX: positions message: {}", json_str);
                0
            }
            "account" => {
                // TODO: 实现账户解析
                debug!("OKX: account message: {}", json_str);
                0
            }
            _ => {
                // 可能是事件响应 (login, subscribe 等)
                if json_value.get("event").is_some() {
                    debug!("OKX: event message: {}", json_str);
                } else {
                    warn!("OKX: Unknown channel: {}", channel);
                }
                0
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_order_message() {
        let parser = OkexAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let msg = r#"{
            "arg": {"channel": "orders", "instType": "SWAP"},
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "ordId": "123456789",
                "clOrdId": "test_order_1",
                "px": "50000",
                "sz": "1",
                "side": "buy",
                "ordType": "limit",
                "state": "filled",
                "fillPx": "49999",
                "fillSz": "1",
                "avgPx": "49999",
                "accFillSz": "1",
                "fee": "-0.5",
                "feeCcy": "USDT",
                "tradeId": "987654321",
                "pnl": "10.5",
                "posSide": "long",
                "reduceOnly": "false",
                "cTime": "1704876947000",
                "uTime": "1704876948000"
            }]
        }"#;

        let bytes = Bytes::from(msg);
        let count = parser.parse(bytes, &tx);
        assert_eq!(count, 1);

        let received = rx.try_recv();
        assert!(received.is_ok());
    }

    #[test]
    fn test_convert_state() {
        assert_eq!(
            OkexAccountEventParser::convert_state("live"),
            ("NEW".to_string(), "NEW".to_string())
        );
        assert_eq!(
            OkexAccountEventParser::convert_state("filled"),
            ("TRADE".to_string(), "FILLED".to_string())
        );
        assert_eq!(
            OkexAccountEventParser::convert_state("canceled"),
            ("CANCELED".to_string(), "CANCELED".to_string())
        );
    }
}
