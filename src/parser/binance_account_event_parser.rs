use crate::common::account_msg::{
    AccountConfigUpdateMsg, AccountEventMsg, AccountEventType, AccountPositionMsg,
    AccountUpdateBalanceMsg, AccountUpdatePositionMsg, BalanceUpdateMsg, ConditionalOrderMsg,
    ExecutionReportMsg, LiabilityChangeMsg, OpenOrderLossMsg, OrderTradeUpdateMsg,
};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use log::debug;
use serde_json::Value;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct BinanceAccountEventParser;

impl BinanceAccountEventParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BinanceAccountEventParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<Value>(json_str) {
                if let Some(event_type) = json_value.get("e").and_then(|v| v.as_str()) {
                    match event_type {
                        "CONDITIONAL_ORDER_TRADE_UPDATE" => {
                            return self.parse_conditional_order(&json_value, tx);
                        }
                        "openOrderLoss" => {
                            return self.parse_open_order_loss(&json_value, tx);
                        }
                        "outboundAccountPosition" => {
                            return self.parse_account_position(&json_value, tx);
                        }
                        "liabilityChange" => {
                            return self.parse_liability_change(&json_value, tx);
                        }
                        "executionReport" => {
                            return self.parse_execution_report(&json_value, tx);
                        }
                        "ORDER_TRADE_UPDATE" => {
                            return self.parse_order_trade_update(&json_value, tx);
                        }
                        "ACCOUNT_UPDATE" => {
                            return self.parse_account_update(&json_value, tx);
                        }
                        "ACCOUNT_CONFIG_UPDATE" => {
                            return self.parse_account_config_update(&json_value, tx);
                        }
                        "balanceUpdate" => {
                            return self.parse_balance_update(&json_value, tx);
                        }
                        _ => return 0,
                    }
                }
            }
        }
        0
    }
}

impl BinanceAccountEventParser {
    fn parse_conditional_order(&self, json: &Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Some(so) = json.get("so") {
            let symbol = so
                .get("s")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let strategy_id = so.get("si").and_then(|v| v.as_i64()).unwrap_or(0);
            let order_id = so.get("i").and_then(|v| v.as_i64()).unwrap_or(0);
            let trade_time = json.get("T").and_then(|v| v.as_i64()).unwrap_or(0);
            let event_time = json.get("E").and_then(|v| v.as_i64()).unwrap_or(0);
            let update_time = so.get("ut").and_then(|v| v.as_i64()).unwrap_or(0);

            let side = match so.get("S").and_then(|v| v.as_str()).unwrap_or("") {
                "BUY" => 'B',
                "SELL" => 'S',
                _ => 'U',
            };

            let position_side = match so.get("ps").and_then(|v| v.as_str()).unwrap_or("") {
                "LONG" => 'L',
                "SHORT" => 'S',
                "BOTH" => 'B',
                _ => 'B',
            };

            let reduce_only = so.get("R").and_then(|v| v.as_bool()).unwrap_or(false);
            let close_position = so.get("cp").and_then(|v| v.as_bool()).unwrap_or(false);

            let quantity = so
                .get("q")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let price = so
                .get("p")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let stop_price = so
                .get("sp")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let activation_price = so
                .get("AP")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let callback_rate = so
                .get("cr")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);

            let strategy_type = so
                .get("st")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let order_status = so
                .get("os")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let custom_id = so
                .get("c")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let time_in_force = so
                .get("f")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let trigger_type = so
                .get("wt")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let msg = ConditionalOrderMsg::create(
                symbol,
                strategy_id,
                order_id,
                trade_time,
                event_time,
                update_time,
                side,
                position_side,
                reduce_only,
                close_position,
                quantity,
                price,
                stop_price,
                activation_price,
                callback_rate,
                strategy_type,
                order_status,
                custom_id,
                time_in_force,
                trigger_type,
            );

            let bytes = msg.to_bytes();
            let event_msg =
                AccountEventMsg::create(AccountEventType::ConditionalOrderUpdate, bytes);

            if let Err(_) = tx.send(event_msg.to_bytes()) {
                return 0;
            }
            return 1;
        }
        0
    }

    fn parse_open_order_loss(&self, json: &Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let event_time = json.get("E").and_then(|v| v.as_i64()).unwrap_or(0);

        if let Some(losses_array) = json.get("O").and_then(|v| v.as_array()) {
            let mut count = 0;

            for loss_obj in losses_array {
                let asset = loss_obj
                    .get("a")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let amount_str = loss_obj.get("o").and_then(|v| v.as_str()).unwrap_or("0");
                let amount = amount_str.parse::<f64>().unwrap_or(0.0);

                let msg = OpenOrderLossMsg::create(event_time, asset, amount);
                let bytes = msg.to_bytes();
                let event_msg = AccountEventMsg::create(AccountEventType::OpenOrderLoss, bytes);

                if let Err(_) = tx.send(event_msg.to_bytes()) {
                    break;
                }
                count += 1;
            }

            return count;
        }
        0
    }

    fn parse_account_position(&self, json: &Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let event_time = json.get("E").and_then(|v| v.as_i64()).unwrap_or(0);
        let update_time = json.get("u").and_then(|v| v.as_i64()).unwrap_or(0);
        let update_id = json.get("U").and_then(|v| v.as_i64()).unwrap_or(0);

        if let Some(balances_array) = json.get("B").and_then(|v| v.as_array()) {
            let mut count = 0;

            for balance_obj in balances_array {
                let asset = balance_obj
                    .get("a")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let free_str = balance_obj.get("f").and_then(|v| v.as_str()).unwrap_or("0");
                let locked_str = balance_obj.get("l").and_then(|v| v.as_str()).unwrap_or("0");

                let free_balance = free_str.parse::<f64>().unwrap_or(0.0);
                let locked_balance = locked_str.parse::<f64>().unwrap_or(0.0);

                let msg = AccountPositionMsg::create(
                    event_time,
                    update_time,
                    update_id,
                    asset,
                    free_balance,
                    locked_balance,
                );
                let bytes = msg.to_bytes();
                let event_msg = AccountEventMsg::create(AccountEventType::AccountPosition, bytes);

                if let Err(_) = tx.send(event_msg.to_bytes()) {
                    break;
                }
                count += 1;
            }

            return count;
        }
        0
    }

    fn parse_liability_change(&self, json: &Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let event_time = json.get("E").and_then(|v| v.as_i64()).unwrap_or(0);
        let transaction_id = json.get("T").and_then(|v| v.as_i64()).unwrap_or(0);
        let asset = json
            .get("a")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let liability_type = json
            .get("t")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let principal_str = json.get("p").and_then(|v| v.as_str()).unwrap_or("0");
        let interest_str = json.get("i").and_then(|v| v.as_str()).unwrap_or("0");
        let total_liability_str = json.get("l").and_then(|v| v.as_str()).unwrap_or("0");

        let principal = principal_str.parse::<f64>().unwrap_or(0.0);
        let interest = interest_str.parse::<f64>().unwrap_or(0.0);
        let total_liability = total_liability_str.parse::<f64>().unwrap_or(0.0);

        let msg = LiabilityChangeMsg::create(
            event_time,
            transaction_id,
            asset,
            liability_type,
            principal,
            interest,
            total_liability,
        );
        let bytes = msg.to_bytes();
        let event_msg = AccountEventMsg::create(AccountEventType::LiabilityChange, bytes);

        if let Err(_) = tx.send(event_msg.to_bytes()) {
            return 0;
        }
        1
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
        let client_order_id = client_order_id_raw
            .and_then(|s| s.parse::<i64>().ok())
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
        let event_msg = AccountEventMsg::create(AccountEventType::ExecutionReport, bytes);

        if let Err(_) = tx.send(event_msg.to_bytes()) {
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

        if let Some(o) = json.get("o") {
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

            let side = match o.get("S").and_then(|v| v.as_str()).unwrap_or("") {
                "BUY" => 'B',
                "SELL" => 'S',
                _ => 'U',
            };

            let position_side = match o.get("ps").and_then(|v| v.as_str()).unwrap_or("") {
                "LONG" => 'L',
                "SHORT" => 'S',
                "BOTH" => 'B',
                _ => 'B',
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
            let event_msg = AccountEventMsg::create(AccountEventType::OrderTradeUpdate, bytes);

            if let Err(_) = tx.send(event_msg.to_bytes()) {
                return 0;
            }
            return 1;
        }
        0
    }

    fn parse_account_update(&self, json: &Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let event_time = json.get("E").and_then(|v| v.as_i64()).unwrap_or(0);
        let transaction_time = json.get("T").and_then(|v| v.as_i64()).unwrap_or(0);
        let business_unit = json
            .get("fs")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let mut count = 0;

        if let Some(a) = json.get("a") {
            let reason = a
                .get("m")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            // Parse balances
            if let Some(balances) = a.get("B").and_then(|v| v.as_array()) {
                for balance in balances {
                    let asset = balance
                        .get("a")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let wallet_balance = balance
                        .get("wb")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    let cross_wallet_balance = balance
                        .get("cw")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    let balance_change = balance
                        .get("bc")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);

                    let msg = AccountUpdateBalanceMsg::create(
                        event_time,
                        transaction_time,
                        asset,
                        reason.clone(),
                        business_unit.clone(),
                        wallet_balance,
                        cross_wallet_balance,
                        balance_change,
                    );
                    let bytes = msg.to_bytes();
                    let event_msg =
                        AccountEventMsg::create(AccountEventType::AccountUpdateBalance, bytes);

                    if let Err(_) = tx.send(event_msg.to_bytes()) {
                        return count;
                    }
                    count += 1;
                }
            }

            // Parse positions
            if let Some(positions) = a.get("P").and_then(|v| v.as_array()) {
                for position in positions {
                    let symbol = position
                        .get("s")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();

                    let position_side =
                        match position.get("ps").and_then(|v| v.as_str()).unwrap_or("") {
                            "LONG" => 'L',
                            "SHORT" => 'S',
                            "BOTH" => 'B',
                            _ => 'B',
                        };

                    let position_amount = position
                        .get("pa")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    let entry_price = position
                        .get("ep")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    let accumulated_realized = position
                        .get("cr")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    let unrealized_pnl = position
                        .get("up")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    let breakeven_price = position
                        .get("bep")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);

                    let msg = AccountUpdatePositionMsg::create(
                        event_time,
                        transaction_time,
                        symbol,
                        reason.clone(),
                        business_unit.clone(),
                        position_side,
                        position_amount,
                        entry_price,
                        accumulated_realized,
                        unrealized_pnl,
                        breakeven_price,
                    );
                    let bytes = msg.to_bytes();
                    let event_msg =
                        AccountEventMsg::create(AccountEventType::AccountUpdatePosition, bytes);

                    if let Err(_) = tx.send(event_msg.to_bytes()) {
                        return count;
                    }
                    count += 1;
                }
            }
        }

        count
    }

    fn parse_account_config_update(
        &self,
        json: &Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let event_time = json.get("E").and_then(|v| v.as_i64()).unwrap_or(0);
        let transaction_time = json.get("T").and_then(|v| v.as_i64()).unwrap_or(0);
        let business_unit = json
            .get("fs")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if let Some(ac) = json.get("ac") {
            let symbol = ac
                .get("s")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let leverage = ac.get("l").and_then(|v| v.as_i64()).unwrap_or(0) as i32;

            let msg = AccountConfigUpdateMsg::create(
                event_time,
                transaction_time,
                symbol,
                business_unit,
                leverage,
            );
            let bytes = msg.to_bytes();
            let event_msg = AccountEventMsg::create(AccountEventType::AccountConfigUpdate, bytes);

            if let Err(_) = tx.send(event_msg.to_bytes()) {
                return 0;
            }
            return 1;
        }
        0
    }

    fn parse_balance_update(&self, json: &Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let event_time = json.get("E").and_then(|v| v.as_i64()).unwrap_or(0);
        let transaction_time = json.get("T").and_then(|v| v.as_i64()).unwrap_or(0);
        let update_id = json.get("U").and_then(|v| v.as_i64()).unwrap_or(0);
        let asset = json
            .get("a")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let delta_str = json.get("d").and_then(|v| v.as_str()).unwrap_or("0");
        let delta = delta_str.parse::<f64>().unwrap_or(0.0);

        let msg = BalanceUpdateMsg::create(event_time, transaction_time, update_id, asset, delta);
        let bytes = msg.to_bytes();
        let event_msg = AccountEventMsg::create(AccountEventType::BalanceUpdate, bytes);

        if let Err(_) = tx.send(event_msg.to_bytes()) {
            return 0;
        }
        1
    }
}
