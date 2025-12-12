//! Binance PM 账户事件解析器（basic 模式）
//!
//! - 余额 / 持仓 / 订单 / 负债 统一封装为 `BasicAccountEventMsg`
//! - OrderUpdate 的 payload 仍使用旧的 `ExecutionReportMsg` / `OrderTradeUpdateMsg`
//!   （它们已实现 OrderUpdate + TradeUpdate trait，便于策略复用）

use crate::common::account_msg::{ExecutionReportMsg, OrderTradeUpdateMsg};
use crate::common::basic_account_msg::{
    BasicAccountEventMsg, BasicAccountEventType, BasicBalanceMsg, BasicBorrowInterestMsg,
    BasicPositionMsg,
};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use log::{debug, warn};
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct BinanceBasicAccountEventParser;

impl BinanceBasicAccountEventParser {
    pub fn new() -> Self {
        Self
    }

    fn parse_execution_report(&self, json: &Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let event_time = json.get("E").and_then(|v| v.as_i64()).unwrap_or(0);
        let transaction_time = json.get("T").and_then(|v| v.as_i64()).unwrap_or(0);
        let order_id = json.get("i").and_then(|v| v.as_i64()).unwrap_or(0);
        let trade_id = json.get("t").and_then(|v| v.as_i64()).unwrap_or(-1);
        let order_creation_time = json.get("O").and_then(|v| v.as_i64()).unwrap_or(0);
        let working_time = json.get("W").and_then(|v| v.as_i64()).unwrap_or(0);
        let update_id = json.get("I").and_then(|v| v.as_i64()).unwrap_or(0);

        let symbol = json
            .get("s")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let client_order_id_raw = json.get("c").and_then(|v| v.as_str());
        let orig_client_order_id_raw = json.get("C").and_then(|v| v.as_str());
        // 策略单使用 i64 自定义 clientOrderId；若主字段解析失败，尝试使用 origClientOrderId (C)
        let client_order_id = client_order_id_raw
            .and_then(|s| s.parse::<i64>().ok())
            .or_else(|| orig_client_order_id_raw.and_then(|s| s.parse::<i64>().ok()))
            .unwrap_or(0);

        let side = match json.get("S").and_then(|v| v.as_str()).unwrap_or("") {
            "BUY" => 'B',
            "SELL" => 'S',
            _ => 'U',
        };

        let is_maker = json.get("m").and_then(|v| v.as_bool()).unwrap_or(false);
        let is_working = json.get("w").and_then(|v| v.as_bool()).unwrap_or(false);

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
        let last_quote = json
            .get("Y")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let quote_order_quantity = json
            .get("Q")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);

        let order_type = json
            .get("o")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let time_in_force = json
            .get("f")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let execution_type = json
            .get("x")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let order_status = json
            .get("X")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let commission_asset = json
            .get("N")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if client_order_id == 0 {
            warn!(
                "parser: skip executionReport with non-i64 clientOrderId c={:?} C={:?} sym={}",
                client_order_id_raw, orig_client_order_id_raw, symbol
            );
            return 0;
        }

        let msg = ExecutionReportMsg::create(
            event_time,
            transaction_time,
            order_id,
            trade_id,
            order_creation_time,
            working_time,
            update_id,
            symbol,
            client_order_id,
            side,
            is_maker,
            is_working,
            price,
            quantity,
            last_executed_quantity,
            cumulative_filled_quantity,
            last_executed_price,
            commission_amount,
            cumulative_quote,
            last_quote,
            quote_order_quantity,
            order_type,
            time_in_force,
            execution_type,
            order_status,
            commission_asset,
            client_order_id_raw.map(|s| s.to_string()),
        );
        let bytes = msg.to_bytes();
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

        let event = BasicAccountEventMsg::create(BasicAccountEventType::OrderUpdate, bytes);
        if tx.send(event.to_bytes()).is_err() {
            return 0;
        }
        1
    }

    fn parse_order_trade_update(&self, json: &Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let event_time = json.get("E").and_then(|v| v.as_i64()).unwrap_or(0);
        let transaction_time = json.get("T").and_then(|v| v.as_i64()).unwrap_or(0);
        let business_unit = json
            .get("fs")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let Some(o) = json.get("o") else {
            return 0;
        };

        let order_id = o.get("i").and_then(|v| v.as_i64()).unwrap_or(0);
        let trade_id = o.get("t").and_then(|v| v.as_i64()).unwrap_or(0);
        let strategy_id = o.get("si").and_then(|v| v.as_i64()).unwrap_or(0);

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

        let side = match o.get("S").and_then(|v| v.as_str()).unwrap_or("") {
            "BUY" => 'B',
            "SELL" => 'S',
            _ => 'U',
        };

        let position_side = match o.get("ps").and_then(|v| v.as_str()).unwrap_or("") {
            "LONG" => 'L',
            "SHORT" => 'S',
            _ => 'N',
        };

        let is_maker = o.get("m").and_then(|v| v.as_bool()).unwrap_or(false);
        let reduce_only = o.get("R").and_then(|v| v.as_bool()).unwrap_or(false);

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
        let stop_price = o
            .get("sp")
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
        let buy_notional = o
            .get("b")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let sell_notional = o
            .get("a")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let realized_profit = o
            .get("rp")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);

        let order_type = o
            .get("o")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let time_in_force = o
            .get("f")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let execution_type = o
            .get("x")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let order_status = o
            .get("X")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let commission_asset = o
            .get("N")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let strategy_type = o
            .get("st")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let msg = OrderTradeUpdateMsg::create(
            event_time,
            transaction_time,
            order_id,
            trade_id,
            strategy_id,
            symbol,
            client_order_id,
            side,
            position_side,
            is_maker,
            reduce_only,
            price,
            quantity,
            average_price,
            stop_price,
            last_executed_quantity,
            cumulative_filled_quantity,
            last_executed_price,
            commission_amount,
            buy_notional,
            sell_notional,
            realized_profit,
            order_type,
            time_in_force,
            execution_type,
            order_status,
            commission_asset,
            strategy_type,
            business_unit,
            client_order_id_raw.map(|s| s.to_string()),
        );
        let bytes = msg.to_bytes();
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

        let event = BasicAccountEventMsg::create(BasicAccountEventType::OrderUpdate, bytes);
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
        let event = BasicAccountEventMsg::create(msg.msg_type, payload);
        if tx.send(event.to_bytes()).is_err() {
            return 0;
        }
        1
    }

    fn parse_account_update(&self, json: &Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let event_time = json.get("E").and_then(|v| v.as_i64()).unwrap_or(0);

        let mut count = 0;

        let Some(a) = json.get("a") else {
            return 0;
        };

        // balances
        if let Some(balances) = a.get("B").and_then(|v| v.as_array()) {
            for balance in balances {
                let asset = balance
                    .get("a")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let cross_wallet_balance = balance
                    .get("cw")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);

                let msg = BasicBalanceMsg::create(event_time, asset, cross_wallet_balance);
                let payload = msg.to_bytes();
                let event = BasicAccountEventMsg::create(msg.msg_type, payload);
                if tx.send(event.to_bytes()).is_err() {
                    return count;
                }
                count += 1;
            }
        }

        // positions (merge by (symbol, side))
        if let Some(positions) = a.get("P").and_then(|v| v.as_array()) {
            let mut position_map: HashMap<(String, char), f32> = HashMap::new();
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
                position_map.insert((symbol, position_side), position_amount);
            }

            for ((symbol, position_side), position_amount) in position_map {
                let msg =
                    BasicPositionMsg::create(event_time, symbol, position_side, position_amount);
                let payload = msg.to_bytes();
                let event = BasicAccountEventMsg::create(msg.msg_type, payload);
                if tx.send(event.to_bytes()).is_err() {
                    return count;
                }
                count += 1;
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
        let Some(event_type) = json_value.get("e").and_then(|v| v.as_str()) else {
            return 0;
        };

        match event_type {
            "executionReport" => self.parse_execution_report(&json_value, tx),
            "ORDER_TRADE_UPDATE" => self.parse_order_trade_update(&json_value, tx),
            "ACCOUNT_UPDATE" => self.parse_account_update(&json_value, tx),
            "liabilityChange" => self.parse_liability_change(&json_value, tx),
            _ => 0,
        }
    }
}
