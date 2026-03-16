//! Binance PM 账户事件解析器（basic 模式）
//!
//! - 余额 / 持仓 / 订单 / 负债 统一封装为 `BasicAccountEventMsg`
//! - OrderUpdate 的 payload 使用 basic 层统一 schema：`BinanceBasicOrderMsg`

use crate::common::basic_account_msg::{
    BasicAccountEventMsg, BasicAccountEventType, BasicAccountScope, BasicBalanceMsg,
    BasicBorrowInterestMsg, BasicPositionMsg, BasicUmUnrealizedMsg, BinanceBasicOrderMsg,
};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use log::{debug, warn};
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::mpsc;

use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::common::{ExecutionType, OrderStatus, TimeInForce};

#[derive(Clone)]
pub struct BinanceBasicAccountEventParser {
    parse_account_update_balances: bool,
    account_scope: BasicAccountScope,
}

impl BinanceBasicAccountEventParser {
    pub fn new(parse_account_update_balances: bool, account_scope: BasicAccountScope) -> Self {
        Self {
            parse_account_update_balances,
            account_scope,
        }
    }

    fn encode_order_type(order_type: &str) -> u8 {
        OrderType::from_str(order_type)
            .unwrap_or(OrderType::Limit)
            .to_u8()
    }

    fn encode_time_in_force(tif: &str) -> u8 {
        TimeInForce::from_str(tif)
            .unwrap_or(TimeInForce::GTC)
            .to_u8()
    }

    fn parse_execution_report(&self, json: &Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let event_time = json.get("E").and_then(|v| v.as_i64()).unwrap_or(0);
        let transaction_time = json.get("T").and_then(|v| v.as_i64()).unwrap_or(0);
        let order_id = json.get("i").and_then(|v| v.as_i64()).unwrap_or(0);
        let trade_id = json.get("t").and_then(|v| v.as_i64()).unwrap_or(0).max(0);

        let symbol = json
            .get("s")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let client_order_id_raw = json.get("c").and_then(|v| v.as_str());
        let orig_client_order_id_raw = json.get("C").and_then(|v| v.as_str());
        let client_order_id = client_order_id_raw
            .and_then(|s| s.parse::<i64>().ok())
            .or_else(|| orig_client_order_id_raw.and_then(|s| s.parse::<i64>().ok()))
            .unwrap_or(0);

        if client_order_id == 0 {
            warn!(
                "parser: skip executionReport with non-i64 clientOrderId c={:?} C={:?} sym={}",
                client_order_id_raw, orig_client_order_id_raw, symbol
            );
            return 0;
        }

        let side = Side::from_str(json.get("S").and_then(|v| v.as_str()).unwrap_or(""))
            .unwrap_or(Side::Buy)
            .to_u8();
        let is_maker = json.get("m").and_then(|v| v.as_bool()).unwrap_or(false);

        let price = json
            .get("p")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let quantity = json
            .get("q")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let last_executed_quantity = json
            .get("l")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let cumulative_filled_quantity = json
            .get("z")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let last_executed_price = json
            .get("L")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let commission_amount = json
            .get("n")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let cumulative_quote = json
            .get("Z")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);

        let order_type_str = json.get("o").and_then(|v| v.as_str()).unwrap_or("");
        let tif_str = json.get("f").and_then(|v| v.as_str()).unwrap_or("");
        let exe_code =
            ExecutionType::from_str(json.get("x").and_then(|v| v.as_str()).unwrap_or(""))
                .unwrap_or(ExecutionType::New)
                .to_u8();
        let status_code =
            OrderStatus::from_str(json.get("X").and_then(|v| v.as_str()).unwrap_or(""))
                .unwrap_or(OrderStatus::New)
                .to_u8();
        let commission_asset = json
            .get("N")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let average_price = if cumulative_filled_quantity > 0.0 {
            cumulative_quote / cumulative_filled_quantity
        } else {
            0.0
        };

        let bytes = BinanceBasicOrderMsg::create(
            BinanceBasicOrderMsg::VENUE_MARGIN,
            event_time,
            transaction_time,
            symbol,
            order_id,
            client_order_id,
            trade_id,
            side,
            Self::encode_order_type(order_type_str),
            Self::encode_time_in_force(tif_str),
            exe_code,
            status_code,
            is_maker,
            price,
            quantity,
            last_executed_quantity,
            cumulative_filled_quantity,
            last_executed_price,
            average_price,
            commission_amount,
            0.0,
            commission_asset,
        )
        .to_bytes();

        debug!(
            "parser: executionReport parsed sym={} c_raw={:?} cli_id_i64={} x={} X={} qty={} last_qty={} last_px={}",
            json.get("s").and_then(|v| v.as_str()).unwrap_or(""),
            client_order_id_raw,
            client_order_id,
            json.get("x").and_then(|v| v.as_str()).unwrap_or(""),
            json.get("X").and_then(|v| v.as_str()).unwrap_or(""),
            json.get("q").and_then(|v| v.as_str()).unwrap_or(""),
            json.get("l").and_then(|v| v.as_str()).unwrap_or(""),
            json.get("L").and_then(|v| v.as_str()).unwrap_or("")
        );

        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            self.account_scope,
            bytes,
        );
        if tx.send(event.to_bytes()).is_err() {
            return 0;
        }
        1
    }

    fn parse_order_trade_update(&self, json: &Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let event_time = json.get("E").and_then(|v| v.as_i64()).unwrap_or(0);
        let transaction_time = json.get("T").and_then(|v| v.as_i64()).unwrap_or(0);

        let Some(o) = json.get("o") else {
            return 0;
        };

        let order_id = o.get("i").and_then(|v| v.as_i64()).unwrap_or(0);
        let trade_id = o.get("t").and_then(|v| v.as_i64()).unwrap_or(0).max(0);

        let symbol = o
            .get("s")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let client_order_id_raw = o.get("c").and_then(|v| v.as_str());
        let client_order_id = client_order_id_raw
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);

        if client_order_id == 0 {
            warn!(
                "parser: skip orderTradeUpdate with non-i64 clientOrderId c={:?} sym={}",
                client_order_id_raw, symbol
            );
            return 0;
        }

        let side = Side::from_str(o.get("S").and_then(|v| v.as_str()).unwrap_or(""))
            .unwrap_or(Side::Buy)
            .to_u8();
        let is_maker = o.get("m").and_then(|v| v.as_bool()).unwrap_or(false);

        let price = o
            .get("p")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let quantity = o
            .get("q")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let average_price = o
            .get("ap")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let last_executed_quantity = o
            .get("l")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let cumulative_filled_quantity = o
            .get("z")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let last_executed_price = o
            .get("L")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let commission_amount = o
            .get("n")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let realized_profit = o
            .get("rp")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);

        let order_type_str = o.get("o").and_then(|v| v.as_str()).unwrap_or("");
        let tif_str = o.get("f").and_then(|v| v.as_str()).unwrap_or("");
        let exe_code = ExecutionType::from_str(o.get("x").and_then(|v| v.as_str()).unwrap_or(""))
            .unwrap_or(ExecutionType::New)
            .to_u8();
        let status_code = OrderStatus::from_str(o.get("X").and_then(|v| v.as_str()).unwrap_or(""))
            .unwrap_or(OrderStatus::New)
            .to_u8();
        let commission_asset = o
            .get("N")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let bytes = BinanceBasicOrderMsg::create(
            BinanceBasicOrderMsg::VENUE_UM,
            event_time,
            transaction_time,
            symbol,
            order_id,
            client_order_id,
            trade_id,
            side,
            Self::encode_order_type(order_type_str),
            Self::encode_time_in_force(tif_str),
            exe_code,
            status_code,
            is_maker,
            price,
            quantity,
            last_executed_quantity,
            cumulative_filled_quantity,
            last_executed_price,
            average_price,
            commission_amount,
            realized_profit,
            commission_asset,
        )
        .to_bytes();

        debug!(
            "parser: orderTradeUpdate parsed sym={} c_raw={:?} cli_id_i64={} x={} X={} qty={} last_qty={} last_px={}",
            o.get("s").and_then(|v| v.as_str()).unwrap_or(""),
            client_order_id_raw,
            client_order_id,
            o.get("x").and_then(|v| v.as_str()).unwrap_or(""),
            o.get("X").and_then(|v| v.as_str()).unwrap_or(""),
            o.get("q").and_then(|v| v.as_str()).unwrap_or(""),
            o.get("l").and_then(|v| v.as_str()).unwrap_or(""),
            o.get("L").and_then(|v| v.as_str()).unwrap_or("")
        );

        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            self.account_scope,
            bytes,
        );
        if tx.send(event.to_bytes()).is_err() {
            return 0;
        }
        1
    }

    fn parse_liability_change(&self, json: &Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let event_time = json.get("E").and_then(|v| v.as_i64()).unwrap_or(0);
        let asset = json
            .get("a")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let principal_str = json.get("p").and_then(|v| v.as_str()).unwrap_or("0");
        let interest_str = json.get("i").and_then(|v| v.as_str()).unwrap_or("0");

        let principal = principal_str.parse::<f64>().unwrap_or(0.0);
        let interest = interest_str.parse::<f64>().unwrap_or(0.0);

        let msg = BasicBorrowInterestMsg::create(event_time, asset, principal, interest);
        let payload = msg.to_bytes();
        let event = BasicAccountEventMsg::create(msg.msg_type, self.account_scope, payload);
        if tx.send(event.to_bytes()).is_err() {
            return 0;
        }
        1
    }

    fn parse_outbound_account_position(
        &self,
        json: &Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let event_time = json.get("E").and_then(|v| v.as_i64()).unwrap_or(0);
        let Some(balances) = json.get("B").and_then(|v| v.as_array()) else {
            return 0;
        };

        let mut count = 0;
        for balance in balances {
            let asset = balance
                .get("a")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if asset.is_empty() {
                continue;
            }
            // Use free balance ("f") only; locked ("l") is ignored by design.
            let free_balance = balance
                .get("f")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);

            let msg = BasicBalanceMsg::create(event_time, asset, free_balance);
            let payload = msg.to_bytes();
            let event = BasicAccountEventMsg::create(msg.msg_type, self.account_scope, payload);
            if tx.send(event.to_bytes()).is_err() {
                return count;
            }
            count += 1;
        }

        count
    }

    fn parse_account_update(&self, json: &Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let event_time = json.get("E").and_then(|v| v.as_i64()).unwrap_or(0);

        let mut count = 0;

        let Some(a) = json.get("a") else {
            return 0;
        };

        // ACCOUNT_UPDATE balance ("cw"/"wb") parsing is optional for standard mode.
        if self.parse_account_update_balances {
            if let Some(balances) = a.get("B").and_then(|v| v.as_array()) {
                for balance in balances {
                    let asset = balance
                        .get("a")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    if asset.is_empty() {
                        continue;
                    }
                    let balance_value = balance
                        .get("cw")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| {
                            balance
                                .get("wb")
                                .and_then(|v| v.as_str())
                                .and_then(|s| s.parse::<f64>().ok())
                        })
                        .unwrap_or(0.0);
                    let msg = BasicBalanceMsg::create(event_time, asset, balance_value);
                    let payload = msg.to_bytes();
                    let event =
                        BasicAccountEventMsg::create(msg.msg_type, self.account_scope, payload);
                    if tx.send(event.to_bytes()).is_err() {
                        return count;
                    }
                    count += 1;
                }
            }
        }

        // positions (merge by (symbol, side))
        if let Some(positions) = a.get("P").and_then(|v| v.as_array()) {
            let mut position_map: HashMap<(String, char), (f32, Option<f64>)> = HashMap::new();
            for position in positions {
                let symbol = position
                    .get("s")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let position_side = match position.get("ps").and_then(|v| v.as_str()).unwrap_or("")
                {
                    "LONG" => 'L',
                    "SHORT" => 'S',
                    _ => 'N',
                };
                let position_amount = position
                    .get("pa")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f32>().ok())
                    .unwrap_or(0.0);
                let unrealized_pnl = position
                    .get("up")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok());
                position_map.insert((symbol, position_side), (position_amount, unrealized_pnl));
            }

            for ((symbol, position_side), (position_amount, unrealized_pnl)) in position_map {
                let msg =
                    BasicPositionMsg::create(event_time, symbol, position_side, position_amount);
                let payload = msg.to_bytes();
                let event = BasicAccountEventMsg::create(msg.msg_type, self.account_scope, payload);
                if tx.send(event.to_bytes()).is_err() {
                    return count;
                }
                count += 1;

                if let Some(pnl) = unrealized_pnl {
                    let pnl_msg = BasicUmUnrealizedMsg::create(
                        event_time,
                        msg.inst_id.clone(),
                        position_side,
                        pnl,
                    );
                    let pnl_payload = pnl_msg.to_bytes();
                    let pnl_event = BasicAccountEventMsg::create(
                        pnl_msg.msg_type,
                        self.account_scope,
                        pnl_payload,
                    );
                    if tx.send(pnl_event.to_bytes()).is_err() {
                        return count;
                    }
                    count += 1;
                }
            }
        }

        count
    }
}

impl Parser for BinanceBasicAccountEventParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let json_str = match std::str::from_utf8(&msg) {
            Ok(s) => s,
            Err(_) => return 0,
        };
        let json_value: Value = match serde_json::from_str(json_str) {
            Ok(v) => v,
            Err(_) => return 0,
        };
        let event_json = json_value
            .get("event")
            .filter(|v| v.is_object())
            .unwrap_or(&json_value);

        let Some(event_type) = event_json.get("e").and_then(|v| v.as_str()) else {
            return 0;
        };

        match event_type {
            "executionReport" => self.parse_execution_report(event_json, tx),
            "ORDER_TRADE_UPDATE" => self.parse_order_trade_update(event_json, tx),
            "ACCOUNT_UPDATE" => self.parse_account_update(event_json, tx),
            "liabilityChange" => self.parse_liability_change(event_json, tx),
            "outboundAccountPosition" => self.parse_outbound_account_position(event_json, tx),
            _ => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::basic_account_msg::{
        split_basic_account_event, BasicAccountScope, BasicPositionMsg, BasicUmUnrealizedMsg,
    };
    use tokio::sync::mpsc;

    #[test]
    fn account_update_emits_scope_and_unrealized_pnl() {
        let parser = BinanceBasicAccountEventParser::new(true, BasicAccountScope::BinanceStdUm);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let json = Bytes::from(
            r#"{
                "e":"ACCOUNT_UPDATE",
                "E":1700000000000,
                "a":{
                    "B":[{"a":"USDT","cw":"101.5","wb":"101.5"}],
                    "P":[{"s":"BTCUSDT","ps":"LONG","pa":"0.25","up":"12.34"}]
                }
            }"#,
        );

        let emitted = parser.parse(json, &tx);
        assert_eq!(emitted, 3);

        let wrapped_balance = rx.try_recv().expect("balance event");
        let (_, scope, _) = split_basic_account_event(&wrapped_balance).expect("wrapped balance");
        assert_eq!(scope, BasicAccountScope::BinanceStdUm);

        let wrapped_position = rx.try_recv().expect("position event");
        let (_, _, position_payload) =
            split_basic_account_event(&wrapped_position).expect("wrapped position");
        let position = BasicPositionMsg::from_bytes(position_payload).expect("position payload");
        assert_eq!(position.inst_id, "BTCUSDT");
        assert_eq!(position.position_side, 'L');

        let wrapped_pnl = rx.try_recv().expect("pnl event");
        let (_, pnl_scope, pnl_payload) =
            split_basic_account_event(&wrapped_pnl).expect("wrapped pnl");
        assert_eq!(pnl_scope, BasicAccountScope::BinanceStdUm);
        let pnl = BasicUmUnrealizedMsg::from_bytes(pnl_payload).expect("pnl payload");
        assert_eq!(pnl.inst_id, "BTCUSDT");
        assert!((pnl.unrealized_pnl - 12.34).abs() < 1e-9);
    }
}
