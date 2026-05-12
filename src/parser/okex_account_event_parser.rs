//! OKX 账户事件解析器（余额 / 持仓 / 订单）

use crate::common::basic_account_msg::{
    BasicAccountEventMsg, BasicAccountEventType, BasicAccountScope, BasicBalanceMsg,
    BasicBorrowInterestMsg, BasicPositionMsg, OkexOrderMsg,
};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use log::{debug, info, warn};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct OkexAccountEventParser;

impl Default for OkexAccountEventParser {
    fn default() -> Self {
        Self::new()
    }
}

impl OkexAccountEventParser {
    pub fn new() -> Self {
        Self
    }

    fn emit_balance(
        tx: &mpsc::UnboundedSender<Bytes>,
        timestamp: i64,
        symbol: String,
        balance: f64,
    ) -> bool {
        let msg = BasicBalanceMsg::create(timestamp, symbol, balance);
        let payload = msg.to_bytes();
        let event =
            BasicAccountEventMsg::create(msg.msg_type, BasicAccountScope::OkexUnified, payload);
        tx.send(event.to_bytes()).is_ok()
    }

    fn parse_balance_and_position(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let mut count = 0;

        // balance
        // `balance_and_position.balData` 通常不带 liab。新口径下 BasicBalanceMsg 必须是
        // gross wallet，只有 eq 但没有 liab 时不能安全推导 wallet，因此这里不再消费 balData
        // 的余额，避免把 net 覆盖成 wallet。
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
                let Some(eq) = bal
                    .get("eq")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                else {
                    continue;
                };
                let Some(liab) = parse_abs_f64_field(bal.get("liab")) else {
                    continue;
                };

                let symbol = bal
                    .get("ccy")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();

                let balance = eq + liab;
                if Self::emit_balance(tx, timestamp, symbol, balance) {
                    count += 1;
                }
            }
        }

        // position
        // 当前仅从 balance_and_position.posData 提取仓位方向/数量/更新时间。
        // OKX 这个 WS 事件里的 UPL 字段不稳定/可能缺失；UPL 统一由周期性
        // positions REST snapshot 补齐，避免 WS 覆盖 REST 的正确账户净值口径。
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
                let event = BasicAccountEventMsg::create(
                    msg.msg_type(),
                    BasicAccountScope::OkexUnified,
                    payload,
                );
                if tx.send(event.to_bytes()).is_ok() {
                    count += 1;
                }
            }
        }

        count
    }

    fn parse_account(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let mut count = 0;
        let Some(account) = json_value.get("data").and_then(|d| d.get(0)) else {
            return 0;
        };
        let fallback_ts = account
            .get("uTime")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        let Some(arr) = account.get("details").and_then(|v| v.as_array()) else {
            return 0;
        };

        for bal in arr {
            let symbol = bal
                .get("ccy")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            if symbol.is_empty() {
                continue;
            }
            let Some(eq) = bal
                .get("eq")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
            else {
                continue;
            };
            let timestamp = bal
                .get("uTime")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(fallback_ts);

            let Some(liab) = parse_abs_f64_field(bal.get("liab")) else {
                continue;
            };
            let interest = parse_abs_f64_field(bal.get("interest")).unwrap_or(0.0);
            let balance = eq + liab;

            if Self::emit_balance(tx, timestamp, symbol.clone(), balance) {
                count += 1;
            }

            let msg = BasicBorrowInterestMsg::create(
                timestamp,
                symbol,
                (liab - interest).max(0.0),
                interest,
            );
            let payload = msg.to_bytes();
            let event = BasicAccountEventMsg::create(
                BasicAccountEventType::BorrowInterest,
                BasicAccountScope::OkexUnified,
                payload,
            );
            if tx.send(event.to_bytes()).is_ok() {
                count += 1;
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
            let cl_ord_id = match parse_i64_field_opt(order.get("clOrdId")) {
                Some(id) => id,
                None => {
                    warn!("OKX: orders clOrdId is not i64, dropping: {}", order);
                    continue;
                }
            };
            let trade_id = parse_i64_field(order.get("tradeId"));
            let order_status = order
                .get("state")
                .and_then(|v| v.as_str())
                .map(OkexOrderMsg::state_to_u8)
                .unwrap_or(0);
            let side = parse_side(order.get("side").and_then(|v| v.as_str()).unwrap_or(""));
            let ord_type =
                parse_ord_type(order.get("ordType").and_then(|v| v.as_str()).unwrap_or(""));
            let cancel_source = parse_u8_field(order.get("cancelSource"));
            let amend_source = parse_u8_field(order.get("amendSource"));
            let price = parse_okex_order_price_by_state(order, order_status);
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
                state: order_status,
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
            let event =
                BasicAccountEventMsg::create(msg.msg_type, BasicAccountScope::OkexUnified, payload);
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
            "account" => self.parse_account(&json_value, tx),
            "positions" => {
                // 当前设计不消费 OKX positions 频道：
                // - positions: 未单独订阅，避免与 balance_and_position 的仓位数量语义重叠
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

fn parse_i64_field_opt(v: Option<&serde_json::Value>) -> Option<i64> {
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
}

fn parse_i64_field(v: Option<&serde_json::Value>) -> i64 {
    parse_i64_field_opt(v).unwrap_or(0)
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

fn parse_abs_f64_field(v: Option<&serde_json::Value>) -> Option<f64> {
    v.and_then(|val| {
        if let Some(n) = val.as_f64() {
            Some(n.abs())
        } else if let Some(s) = val.as_str() {
            s.parse::<f64>().ok().map(f64::abs)
        } else {
            None
        }
    })
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
    use crate::common::basic_account_msg::{
        split_basic_account_event, BasicAccountEventType, BasicBalanceMsg, BasicBorrowInterestMsg,
    };

    #[test]
    fn balance_and_position_ignores_cashbal_and_ws_upl() {
        let parser = OkexAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let json = r#"{
            "arg": {"channel": "balance_and_position"},
            "data": [{
                "balData": [{
                    "uTime": "1778479696355",
                    "cashBal": "56243.85926211993",
                    "ccy": "USDT"
                }],
                "posData": [{
                    "instId": "BTC-USDT-SWAP",
                    "posSide": "net",
                    "pos": "2",
                    "uTime": "1778472000000",
                    "upl": "-12.25"
                }]
            }]
        }"#;

        assert_eq!(parser.parse(Bytes::from(json), &tx), 1);

        let mut event_types = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            let (event_type, scope, _payload) =
                split_basic_account_event(&msg).expect("wrapped event");
            assert_eq!(scope, BasicAccountScope::OkexUnified);
            event_types.push(event_type);
        }

        assert_eq!(event_types, vec![BasicAccountEventType::PositionUpdate]);
    }

    #[test]
    fn account_channel_emits_gross_wallet_and_borrow() {
        let parser = OkexAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let json = r#"{
            "arg": {"channel": "account"},
            "data": [{
                "uTime": "1778479700000",
                "details": [{
                    "uTime": "1778479696355",
                    "ccy": "USDT",
                    "cashBal": "56243.85926211993",
                    "eq": "53443.166831427916",
                    "liab": "-50.5",
                    "interest": "-0.5",
                    "upl": "-2800.6924306920096"
                }]
            }]
        }"#;

        assert_eq!(parser.parse(Bytes::from(json), &tx), 2);
        let msg = rx.try_recv().expect("one balance event");
        let (event_type, scope, payload) = split_basic_account_event(&msg).expect("wrapped event");
        assert_eq!(event_type, BasicAccountEventType::BalanceUpdate);
        assert_eq!(scope, BasicAccountScope::OkexUnified);
        let balance = BasicBalanceMsg::from_bytes(&payload).expect("balance msg");
        assert_eq!(balance.symbol, "USDT");
        assert!((balance.wallet - 53493.666831427916).abs() < 1e-10);

        let msg = rx.try_recv().expect("borrow event");
        let (event_type, scope, payload) = split_basic_account_event(&msg).expect("wrapped event");
        assert_eq!(event_type, BasicAccountEventType::BorrowInterest);
        assert_eq!(scope, BasicAccountScope::OkexUnified);
        let borrow = BasicBorrowInterestMsg::from_bytes(&payload).expect("borrow msg");
        assert_eq!(borrow.symbol, "USDT");
        assert!((borrow.borrowed - 50.0).abs() < 1e-10);
        assert!((borrow.interest - 0.5).abs() < 1e-10);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn account_channel_skips_balance_when_liab_missing() {
        let parser = OkexAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let json = r#"{
            "arg": {"channel": "account"},
            "data": [{
                "uTime": "1778479700000",
                "details": [{
                    "uTime": "1778479696355",
                    "ccy": "USDT",
                    "eq": "53443.166831427916"
                }]
            }]
        }"#;

        assert_eq!(parser.parse(Bytes::from(json), &tx), 0);
        assert!(rx.try_recv().is_err());
    }
}
