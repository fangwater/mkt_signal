//! OKX 账户事件解析器（余额 / 持仓 / 订单）

use crate::common::basic_account_msg::{
    BasicAccountEventMsg, BasicAccountEventType, BasicBalanceMsg, BasicPositionMsg, OkexOrderMsg,
};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use log::{debug, info, warn};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct OkexAccountEventParser;

impl OkexAccountEventParser {
    pub fn new() -> Self {
        Self
    }

    fn parse_balance_and_position(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let mut count = 0;

        // balance
        if let Some(arr) = json_value
            .get("data")
            .and_then(|d| d.get(0))
            .and_then(|d| d.get("balData"))
            .and_then(|v| v.as_array())
        {
            for bal in arr {
                let timestamp = bal
                    .get("uTime")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(0);
                let balance = bal
                    .get("cashBal")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);

                let symbol = bal
                    .get("ccy")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();

                let msg = BasicBalanceMsg::create(timestamp, symbol, balance);
                let payload = msg.to_bytes();
                let event = BasicAccountEventMsg::create(msg.msg_type, payload);
                if tx.send(event.to_bytes()).is_ok() {
                    count += 1;
                }
            }
        }

        // position
        if let Some(arr) = json_value
            .get("data")
            .and_then(|d| d.get(0))
            .and_then(|d| d.get("posData"))
            .and_then(|v| v.as_array())
        {
            for pos in arr {
                let inst_id = match pos.get("instId").and_then(|v| v.as_str()) {
                    Some(s) => s.to_string(),
                    None => continue,
                };
                let pos_side = pos.get("posSide").and_then(|v| v.as_str()).unwrap_or("net");
                let position_side = match pos_side {
                    "long" => 'L',
                    "short" => 'S',
                    _ => 'N',
                };
                let position_amount = pos
                    .get("pos")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f32>().ok())
                    .unwrap_or(0.0);
                let timestamp = pos
                    .get("uTime")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(0);

                info!(
                    "OKX position: instId={} posSide={} pos={} uTime={}",
                    inst_id, pos_side, position_amount, timestamp
                );

                let msg =
                    BasicPositionMsg::create(timestamp, inst_id, position_side, position_amount);
                let payload = msg.to_bytes();
                let event = BasicAccountEventMsg::create(msg.msg_type(), payload);
                if tx.send(event.to_bytes()).is_ok() {
                    count += 1;
                }
            }
        }

        count
    }

    fn parse_orders(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let mut count = 0;
        let Some(arr) = json_value.get("data").and_then(|d| d.as_array()) else {
            return 0;
        };

        for order in arr {
            let inst_id = order
                .get("instId")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            if inst_id.is_empty() {
                continue;
            }

            let inst_type = order
                .get("instType")
                .and_then(|v| v.as_str())
                .map(OkexOrderMsg::inst_type_to_u8)
                .unwrap_or(u8::MAX);
            let ord_id = parse_i64_field(order.get("ordId"));
            let cl_ord_id = parse_i64_field(order.get("clOrdId"));
            let trade_id = parse_i64_field(order.get("tradeId"));
            let state = order
                .get("state")
                .and_then(|v| v.as_str())
                .map(OkexOrderMsg::state_to_u8)
                .unwrap_or(0);
            let side = parse_side(order.get("side").and_then(|v| v.as_str()).unwrap_or(""));
            let ord_type =
                parse_ord_type(order.get("ordType").and_then(|v| v.as_str()).unwrap_or(""));
            let cancel_source = parse_u8_field(order.get("cancelSource"));
            let amend_source = parse_u8_field(order.get("amendSource"));
            let price = parse_okex_order_price_by_state(order, state);
            let quantity = parse_f64_field(order.get("sz"));
            let cumulative_filled_quantity = parse_f64_field(order.get("accFillSz"));
            let create_time = parse_i64_field(order.get("cTime"));
            let update_time = {
                let ts = parse_i64_field(order.get("uTime"));
                if ts != 0 {
                    ts
                } else {
                    create_time
                }
            };
            let fill_time = {
                let ts = parse_i64_field(order.get("fillTime"));
                if ts != 0 {
                    ts
                } else if update_time != 0 {
                    update_time
                } else {
                    create_time
                }
            };

            let msg = OkexOrderMsg {
                msg_type: BasicAccountEventType::OrderUpdate,
                inst_id,
                inst_type,
                ord_id,
                cl_ord_id,
                trade_id,
                state,
                side,
                ord_type,
                cancel_source,
                amend_source,
                price,
                quantity,
                cumulative_filled_quantity,
                create_time,
                update_time,
                fill_time,
            };

            let payload = msg.to_bytes();
            let event = BasicAccountEventMsg::create(msg.msg_type, payload);
            if tx.send(event.to_bytes()).is_ok() {
                count += 1;
            }
        }

        count
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

        let channel = json_value
            .get("arg")
            .and_then(|arg| arg.get("channel"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        match channel {
            "balance_and_position" => self.parse_balance_and_position(&json_value, tx),
            "orders" => self.parse_orders(&json_value, tx),
            "positions" | "account" => {
                debug!("OKX: ignored channel={} payload={}", channel, json_str);
                0
            }
            _ => {
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

fn parse_i64_field(v: Option<&serde_json::Value>) -> i64 {
    v.and_then(|val| {
        if let Some(n) = val.as_i64() {
            Some(n)
        } else if let Some(n) = val.as_u64() {
            Some(n as i64)
        } else if let Some(s) = val.as_str() {
            s.parse::<i64>().ok()
        } else {
            None
        }
    })
    .unwrap_or(0)
}

fn parse_f64_field(v: Option<&serde_json::Value>) -> f64 {
    v.and_then(|val| {
        if let Some(n) = val.as_f64() {
            Some(n)
        } else if let Some(s) = val.as_str() {
            s.parse::<f64>().ok()
        } else {
            None
        }
    })
    .unwrap_or(0.0)
}

fn parse_u8_field(v: Option<&serde_json::Value>) -> u8 {
    v.and_then(|val| {
        if let Some(n) = val.as_u64() {
            u8::try_from(n).ok()
        } else if let Some(n) = val.as_i64() {
            u8::try_from(n).ok()
        } else if let Some(s) = val.as_str() {
            s.parse::<u8>().ok()
        } else {
            None
        }
    })
    .unwrap_or(0)
}

fn parse_side(side: &str) -> u8 {
    match side {
        "buy" | "BUY" | "Buy" => 1,
        "sell" | "SELL" | "Sell" => 2,
        _ => 0,
    }
}

fn parse_ord_type(ord_type: &str) -> u8 {
    match ord_type {
        "market" => 0,
        "limit" => 1,
        "post_only" => 2,
        "fok" => 3,
        "ioc" => 4,
        "optimal_limit_ioc" => 5,
        "mmp" => 6,
        "mmp_and_post_only" => 7,
        "elp" => 8,
        _ => u8::MAX,
    }
}

fn parse_okex_order_price_by_state(order: &serde_json::Value, state_u8: u8) -> f64 {
    let px = parse_f64_field(order.get("px"));
    let fill_px = parse_f64_field(order.get("fillPx"));
    match state_u8 {
        3 | 4 => fill_px,
        _ => px,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn parse_orders_builds_compact_msg() {
        let parser = OkexAccountEventParser::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let payload = serde_json::json!({
            "arg": {"channel": "orders", "instType": "SPOT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "ordId": "452197707845865472",
                "clOrdId": "12345",
                "tradeId": "242589207",
                "state": "filled",
                "side": "sell",
                "ordType": "limit",
                "cancelSource": "1",
                "amendSource": "2",
                "px": "31527.1",
                "sz": "0.001",
                "accFillSz": "0.001",
                "cTime": "1654084334977",
                "uTime": "1654084353264",
                "fillTime": "1654084353263"
            }]
        });
        let msg = Bytes::from(payload.to_string().into_bytes());

        let parsed = parser.parse(msg, &tx);
        assert_eq!(parsed, 1);

        let out = rx.try_recv().expect("order msg");
        assert_eq!(
            crate::common::basic_account_msg::get_basic_event_type(&out),
            BasicAccountEventType::OrderUpdate
        );
        let payload_len = u32::from_le_bytes([out[4], out[5], out[6], out[7]]) as usize;
        let order_payload = out.slice(8..8 + payload_len);
        let order = OkexOrderMsg::from_bytes(&order_payload).unwrap();

        assert_eq!(order.inst_id, "BTC-USDT");
        assert_eq!(order.inst_type, OkexOrderMsg::inst_type_to_u8("SPOT"));
        assert_eq!(order.ord_id, 452197707845865472i64);
        assert_eq!(order.cl_ord_id, 12345);
        assert_eq!(order.trade_id, 242589207);
        assert_eq!(order.state, OkexOrderMsg::state_to_u8("filled"));
        assert_eq!(order.side, 2);
        assert_eq!(order.ord_type, 1);
        assert_eq!(order.cancel_source, 1);
        assert_eq!(order.amend_source, 2);
        assert!((order.price - 31527.1).abs() < 1e-6);
        assert!((order.quantity - 0.001).abs() < 1e-9);
        assert!((order.cumulative_filled_quantity - 0.001).abs() < 1e-9);
        assert_eq!(order.create_time, 1654084334977);
        assert_eq!(order.update_time, 1654084353264);
        assert_eq!(order.fill_time, 1654084353263);
    }
}
