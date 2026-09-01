//! Binance PM 账户事件解析器（basic 模式）
//!
//! - 余额 / 持仓 / 订单 / 负债 统一封装为 `BasicAccountEventMsg`
//! - OrderUpdate 的 payload 使用 basic 层统一 schema：`BinanceBasicOrderMsg`

use super::{
    balance_dedup_key, binance_order_dedup_key, borrow_interest_dedup_key, lazy_json,
    position_dedup_key, trade_lite_dedup_key, unrealized_pnl_dedup_key, AccountEventSink, Parser,
};
use crate::msg::basic_account_msg::{
    BasicAccountEventMsg, BasicAccountEventType, BasicAccountScope, BasicBalanceMsg,
    BasicBorrowInterestMsg, BasicPositionMsg, BasicTradeLiteMsg, BasicUmUnrealizedMsg,
    BinanceBasicOrderMsg,
};
use bytes::Bytes;
use log::{debug, warn};
use sonic_rs::{JsonValueTrait, LazyValue};
use std::collections::HashMap;

use crate::msg::order_codes;
use symbol_utils::TradingVenue;

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

    fn futures_scope(&self, json: &LazyValue<'_>) -> BasicAccountScope {
        if self.account_scope == BasicAccountScope::BinanceUnified
            && lazy_string(json, "fs").eq_ignore_ascii_case("CM")
        {
            BasicAccountScope::BinanceUnifiedCm
        } else {
            self.account_scope
        }
    }

    fn futures_venue(scope: BasicAccountScope) -> u8 {
        if matches!(
            scope,
            BasicAccountScope::BinanceStdCm | BasicAccountScope::BinanceUnifiedCm
        ) {
            BinanceBasicOrderMsg::VENUE_CM
        } else {
            BinanceBasicOrderMsg::VENUE_UM
        }
    }

    fn futures_trading_venue(scope: BasicAccountScope) -> TradingVenue {
        if matches!(
            scope,
            BasicAccountScope::BinanceStdCm | BasicAccountScope::BinanceUnifiedCm
        ) {
            TradingVenue::BinanceCoinFutures
        } else {
            TradingVenue::BinanceFutures
        }
    }

    fn parse_execution_report<S: AccountEventSink>(&self, json: &LazyValue<'_>, tx: &S) -> usize {
        let event_time = lazy_i64(json, "E");
        let transaction_time = lazy_i64(json, "T");
        let order_id = lazy_i64(json, "i");
        let trade_id = lazy_i64(json, "t").max(0);

        let symbol = lazy_string(json, "s");
        let client_order_id_raw = lazy_string_opt(json, "c");
        let orig_client_order_id_raw = lazy_string_opt(json, "C");
        let client_order_id = client_order_id_raw
            .as_deref()
            .and_then(parse_i64_str)
            .or_else(|| orig_client_order_id_raw.as_deref().and_then(parse_i64_str))
            .unwrap_or(0);

        if client_order_id == 0 {
            warn!(
                "parser: skip executionReport with non-i64 clientOrderId c={:?} C={:?} sym={}",
                client_order_id_raw, orig_client_order_id_raw, symbol
            );
            return 0;
        }

        let side_str = lazy_string(json, "S");
        let side = order_codes::side_to_u8_default_buy(&side_str);
        let is_maker = lazy_bool(json, "m");

        let price = lazy_f64(json, "p");
        let quantity = lazy_f64(json, "q");
        let last_executed_quantity = lazy_f64(json, "l");
        let cumulative_filled_quantity = lazy_f64(json, "z");
        let last_executed_price = lazy_f64(json, "L");
        let commission_amount = lazy_f64(json, "n");
        let cumulative_quote = lazy_f64(json, "Z");

        let order_type_str = lazy_string(json, "o");
        let tif_str = lazy_string(json, "f");
        let execution_type_str = lazy_string(json, "x");
        let status_str = lazy_string(json, "X");
        let exe_code = order_codes::execution_type_to_u8(&execution_type_str);
        let status_code = order_codes::order_status_to_u8(&status_str);
        let commission_asset = lazy_string(json, "N");

        let average_price = if cumulative_filled_quantity > 0.0 {
            cumulative_quote / cumulative_filled_quantity
        } else {
            0.0
        };

        let msg = BinanceBasicOrderMsg::create(
            BinanceBasicOrderMsg::VENUE_MARGIN,
            event_time,
            transaction_time,
            symbol.clone(),
            order_id,
            client_order_id,
            trade_id,
            side,
            order_codes::order_type_to_u8_default_limit(&order_type_str),
            order_codes::time_in_force_to_u8(&tif_str),
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
        );

        debug!(
            "parser: executionReport parsed sym={} c_raw={:?} cli_id_i64={} x={} X={} qty={} last_qty={} last_px={}",
            symbol,
            client_order_id_raw,
            client_order_id,
            execution_type_str,
            status_str,
            lazy_string(json, "q"),
            lazy_string(json, "l"),
            lazy_string(json, "L")
        );

        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            self.account_scope,
            msg.to_bytes(),
        );
        if !tx.emit_with_dedup_key(
            event.to_bytes(),
            binance_order_dedup_key(self.account_scope, &msg),
        ) {
            return 0;
        }
        1
    }

    fn parse_order_trade_update<S: AccountEventSink>(&self, json: &LazyValue<'_>, tx: &S) -> usize {
        let account_scope = self.futures_scope(json);
        let event_time = lazy_i64(json, "E");
        let transaction_time = lazy_i64(json, "T");

        let Some(o) = json.get("o") else {
            return 0;
        };

        let order_id = lazy_i64(&o, "i");
        let trade_id = lazy_i64(&o, "t").max(0);

        let symbol = lazy_string(&o, "s");
        let client_order_id_raw = lazy_string_opt(&o, "c");
        let external_order_kind = client_order_id_raw
            .as_deref()
            .and_then(binance_external_order_kind)
            .unwrap_or(BinanceBasicOrderMsg::EXTERNAL_NONE);
        let client_order_id = client_order_id_raw
            .as_deref()
            .and_then(parse_i64_str)
            .or_else(|| {
                (external_order_kind != BinanceBasicOrderMsg::EXTERNAL_NONE)
                    .then(|| synthetic_external_client_order_id(order_id, trade_id, event_time))
            })
            .unwrap_or(0);

        if client_order_id == 0 {
            warn!(
                "parser: skip orderTradeUpdate with non-i64 clientOrderId c={:?} sym={}",
                client_order_id_raw, symbol
            );
            return 0;
        }

        let side_str = lazy_string(&o, "S");
        let side = order_codes::side_to_u8_default_buy(&side_str);
        let is_maker = lazy_bool(&o, "m");

        let price = lazy_f64(&o, "p");
        let quantity = lazy_f64(&o, "q");
        let average_price = lazy_f64(&o, "ap");
        let last_executed_quantity = lazy_f64(&o, "l");
        let cumulative_filled_quantity = lazy_f64(&o, "z");
        let last_executed_price = lazy_f64(&o, "L");
        let commission_amount = lazy_f64(&o, "n");
        let realized_profit = lazy_f64(&o, "rp");

        let order_type_str = lazy_string(&o, "o");
        let tif_str = lazy_string(&o, "f");
        let execution_type_str = lazy_string(&o, "x");
        let status_str = lazy_string(&o, "X");
        let exe_code = if external_order_kind != BinanceBasicOrderMsg::EXTERNAL_NONE
            && last_executed_quantity > 0.0
        {
            order_codes::execution_type_to_u8("TRADE")
        } else {
            order_codes::execution_type_to_u8(&execution_type_str)
        };
        let status_code = order_codes::order_status_to_u8(&status_str);
        let commission_asset = lazy_string(&o, "N");

        let mut msg = BinanceBasicOrderMsg::create(
            Self::futures_venue(account_scope),
            event_time,
            transaction_time,
            symbol.clone(),
            order_id,
            client_order_id,
            trade_id,
            side,
            order_codes::order_type_to_u8_default_limit(&order_type_str),
            order_codes::time_in_force_to_u8(&tif_str),
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
        );
        msg.external_order_kind = external_order_kind;

        debug!(
            "parser: orderTradeUpdate parsed sym={} c_raw={:?} cli_id_i64={} x={} X={} qty={} last_qty={} last_px={}",
            symbol,
            client_order_id_raw,
            client_order_id,
            execution_type_str,
            status_str,
            lazy_string(&o, "q"),
            lazy_string(&o, "l"),
            lazy_string(&o, "L")
        );

        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            account_scope,
            msg.to_bytes(),
        );
        if !tx.emit_with_dedup_key(
            event.to_bytes(),
            binance_order_dedup_key(account_scope, &msg),
        ) {
            return 0;
        }
        1
    }

    fn parse_trade_lite<S: AccountEventSink>(&self, json: &LazyValue<'_>, tx: &S) -> usize {
        let account_scope = self.futures_scope(json);
        let event_time = lazy_i64(json, "E");
        let trade_time = lazy_i64(json, "T");
        let trade_id_num = lazy_i64(json, "t").max(0);
        let trade_id = trade_id_num.to_string();

        let symbol = lazy_string(json, "s");
        let client_order_id_raw = lazy_string(json, "c");
        let client_order_id = parse_i64_str(&client_order_id_raw).unwrap_or(0);

        if symbol.is_empty() || client_order_id == 0 {
            warn!(
                "parser: skip tradeLite with missing fields sym={} c={}",
                symbol, client_order_id_raw
            );
            return 0;
        }

        let side_str = lazy_string(json, "S");
        let side = order_codes::side_to_u8_default_buy(&side_str);
        let is_maker = lazy_bool(json, "m");
        let last_executed_price = lazy_f64(json, "L");
        let last_executed_quantity = lazy_f64(json, "l");

        let msg = BasicTradeLiteMsg::create(
            Self::futures_trading_venue(account_scope) as u8,
            event_time,
            trade_time,
            symbol.clone(),
            client_order_id,
            &trade_id,
            side,
            is_maker,
            last_executed_price,
            last_executed_quantity,
        );

        debug!(
            "parser: tradeLite parsed sym={} c={} trade_id={} side={} last_qty={} last_px={}",
            symbol,
            client_order_id,
            trade_id,
            side_str,
            lazy_string(json, "l"),
            lazy_string(json, "L")
        );

        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::TradeUpdateLite,
            account_scope,
            msg.to_bytes(),
        );
        if !tx.emit_with_dedup_key(event.to_bytes(), trade_lite_dedup_key(account_scope, &msg)) {
            return 0;
        }
        1
    }

    fn parse_liability_change<S: AccountEventSink>(&self, json: &LazyValue<'_>, tx: &S) -> usize {
        let event_time = lazy_i64(json, "E");
        let asset = lazy_string(json, "a");
        let principal = lazy_f64(json, "p");
        let interest = lazy_f64(json, "i");

        let msg = BasicBorrowInterestMsg::create(event_time, asset, principal, interest);
        let payload = msg.to_bytes();
        let event = BasicAccountEventMsg::create(msg.msg_type, self.account_scope, payload);
        if !tx.emit_with_dedup_key(
            event.to_bytes(),
            borrow_interest_dedup_key(self.account_scope, &msg),
        ) {
            return 0;
        }
        1
    }

    fn parse_outbound_account_position<S: AccountEventSink>(
        &self,
        json: &LazyValue<'_>,
        tx: &S,
    ) -> usize {
        let event_time = lazy_i64(json, "E");
        let Some(balances) = json.get("B").and_then(|v| v.into_array_iter()) else {
            return 0;
        };

        let mut count = 0;
        for balance in balances
            .filter_map(Result::ok)
            .filter(|value| value.is_object())
        {
            let asset = lazy_string(&balance, "a");
            if asset.is_empty() {
                continue;
            }
            // outboundAccountPosition carries both free and locked balances.
            // Use total balance here so equity semantics stay aligned with snapshot parsing.
            let free_balance = lazy_f64(&balance, "f");
            let locked_balance = lazy_f64(&balance, "l");

            let msg = BasicBalanceMsg::create(event_time, asset, free_balance + locked_balance);
            let payload = msg.to_bytes();
            let event = BasicAccountEventMsg::create(msg.msg_type, self.account_scope, payload);
            if !tx.emit_with_dedup_key(
                event.to_bytes(),
                balance_dedup_key(self.account_scope, &msg),
            ) {
                return count;
            }
            count += 1;
        }

        count
    }

    fn parse_account_update<S: AccountEventSink>(&self, json: &LazyValue<'_>, tx: &S) -> usize {
        let account_scope = self.futures_scope(json);
        let event_time = lazy_i64(json, "E");

        let mut count = 0;

        let Some(a) = json.get("a") else {
            return 0;
        };

        // ACCOUNT_UPDATE balance ("cw"/"wb") parsing is optional for standard mode.
        if self.parse_account_update_balances {
            if let Some(balances) = a.get("B").and_then(|v| v.into_array_iter()) {
                for balance in balances
                    .filter_map(Result::ok)
                    .filter(|value| value.is_object())
                {
                    let asset = lazy_string(&balance, "a");
                    if asset.is_empty() {
                        continue;
                    }
                    let balance_value = lazy_json::get_f64(&balance, &["cw", "wb"]).unwrap_or(0.0);
                    let msg = BasicBalanceMsg::create(event_time, asset, balance_value);
                    let payload = msg.to_bytes();
                    let event = BasicAccountEventMsg::create(msg.msg_type, account_scope, payload);
                    if !tx.emit_with_dedup_key(
                        event.to_bytes(),
                        balance_dedup_key(account_scope, &msg),
                    ) {
                        return count;
                    }
                    count += 1;
                }
            }
        }

        // positions (merge by (symbol, side))
        if let Some(positions) = a.get("P").and_then(|v| v.into_array_iter()) {
            let mut position_map: HashMap<(String, char), (f32, Option<f64>)> = HashMap::new();
            for position in positions
                .filter_map(Result::ok)
                .filter(|value| value.is_object())
            {
                let symbol = lazy_string(&position, "s");
                let position_side = match lazy_string(&position, "ps").as_str() {
                    "LONG" => 'L',
                    "SHORT" => 'S',
                    _ => 'N',
                };
                let position_amount = lazy_f64(&position, "pa") as f32;
                let unrealized_pnl = position.get("up").and_then(|v| lazy_json::parse_f64(&v));
                position_map.insert((symbol, position_side), (position_amount, unrealized_pnl));
            }

            for ((symbol, position_side), (position_amount, unrealized_pnl)) in position_map {
                let msg =
                    BasicPositionMsg::create(event_time, symbol, position_side, position_amount);
                let payload = msg.to_bytes();
                let event = BasicAccountEventMsg::create(msg.msg_type, account_scope, payload);
                if !tx
                    .emit_with_dedup_key(event.to_bytes(), position_dedup_key(account_scope, &msg))
                {
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
                    let pnl_event =
                        BasicAccountEventMsg::create(pnl_msg.msg_type, account_scope, pnl_payload);
                    if !tx.emit_with_dedup_key(
                        pnl_event.to_bytes(),
                        unrealized_pnl_dedup_key(account_scope, &pnl_msg),
                    ) {
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
    fn parse<S: AccountEventSink>(&self, msg: Bytes, tx: &S) -> usize {
        let Some(root) = lazy_json::root_from_bytes(&msg) else {
            return 0;
        };
        let event_json = root
            .get("event")
            .filter(|value| value.is_object())
            .unwrap_or_else(|| root.clone());

        let event_type = lazy_string(&event_json, "e");
        if event_type.is_empty() {
            return 0;
        }

        match event_type.as_str() {
            "executionReport" => self.parse_execution_report(&event_json, tx),
            "ORDER_TRADE_UPDATE" => self.parse_order_trade_update(&event_json, tx),
            "TRADE_LITE" => self.parse_trade_lite(&event_json, tx),
            "ACCOUNT_UPDATE" => self.parse_account_update(&event_json, tx),
            "liabilityChange" => self.parse_liability_change(&event_json, tx),
            "outboundAccountPosition" => self.parse_outbound_account_position(&event_json, tx),
            _ => 0,
        }
    }
}

#[inline]
fn lazy_string(obj: &LazyValue<'_>, key: &str) -> String {
    lazy_string_opt(obj, key).unwrap_or_default()
}

#[inline]
fn lazy_string_opt(obj: &LazyValue<'_>, key: &str) -> Option<String> {
    obj.get(key)
        .and_then(|value| value.as_str().map(|s| s.to_string()))
}

#[inline]
fn lazy_i64(obj: &LazyValue<'_>, key: &str) -> i64 {
    obj.get(key)
        .and_then(|value| lazy_json::parse_i64(&value))
        .unwrap_or(0)
}

#[inline]
fn lazy_f64(obj: &LazyValue<'_>, key: &str) -> f64 {
    obj.get(key)
        .and_then(|value| lazy_json::parse_f64(&value))
        .unwrap_or(0.0)
}

#[inline]
fn lazy_bool(obj: &LazyValue<'_>, key: &str) -> bool {
    obj.get(key)
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

#[inline]
fn parse_i64_str(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<i64>().ok()
}

fn binance_external_order_kind(client_order_id: &str) -> Option<u8> {
    let value = client_order_id.to_ascii_lowercase();
    if value.starts_with("settlement_autoclose-") {
        Some(BinanceBasicOrderMsg::EXTERNAL_SETTLEMENT)
    } else if value.starts_with("delivery_autoclose-") {
        Some(BinanceBasicOrderMsg::EXTERNAL_DELIVERY)
    } else if value == "adl_autoclose" || value.starts_with("adl_autoclose-") {
        Some(BinanceBasicOrderMsg::EXTERNAL_ADL)
    } else if value.starts_with("autoclose-") {
        Some(BinanceBasicOrderMsg::EXTERNAL_LIQUIDATION)
    } else {
        None
    }
}

fn synthetic_external_client_order_id(order_id: i64, trade_id: i64, event_time: i64) -> i64 {
    let source = [order_id, trade_id, event_time]
        .into_iter()
        .find(|value| *value > 0)
        .unwrap_or(1);
    -source
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account_event::test_sink::TestAccountEventSink;
    use crate::msg::basic_account_msg::{
        split_basic_account_event, BasicAccountEventType, BasicAccountScope, BasicPositionMsg,
        BasicTradeLiteMsg, BasicUmUnrealizedMsg,
    };

    #[test]
    fn account_update_emits_scope_and_unrealized_pnl() {
        let parser = BinanceBasicAccountEventParser::new(true, BasicAccountScope::BinanceStdUm);
        let sink = TestAccountEventSink::new();
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

        let emitted = parser.parse(json, &sink);
        assert_eq!(emitted, 3);

        let wrapped_balance = sink.recv().expect("balance event");
        let (_, scope, _) = split_basic_account_event(&wrapped_balance).expect("wrapped balance");
        assert_eq!(scope, BasicAccountScope::BinanceStdUm);

        let wrapped_position = sink.recv().expect("position event");
        let (_, _, position_payload) =
            split_basic_account_event(&wrapped_position).expect("wrapped position");
        let position = BasicPositionMsg::from_bytes(position_payload).expect("position payload");
        assert_eq!(position.inst_id, "BTCUSDT");
        assert_eq!(position.position_side, 'L');

        let wrapped_pnl = sink.recv().expect("pnl event");
        let (_, pnl_scope, pnl_payload) =
            split_basic_account_event(&wrapped_pnl).expect("wrapped pnl");
        assert_eq!(pnl_scope, BasicAccountScope::BinanceStdUm);
        let pnl = BasicUmUnrealizedMsg::from_bytes(pnl_payload).expect("pnl payload");
        assert_eq!(pnl.inst_id, "BTCUSDT");
        assert!((pnl.unrealized_pnl - 12.34).abs() < 1e-9);
    }

    #[test]
    fn order_trade_update_emits_order_update_from_event_wrapper() {
        let parser = BinanceBasicAccountEventParser::new(true, BasicAccountScope::BinanceStdUm);
        let sink = TestAccountEventSink::new();
        let json = Bytes::from(
            r#"{
                "event":{
                    "e":"ORDER_TRADE_UPDATE",
                    "E":1700000000000,
                    "T":1700000000123,
                    "o":{
                        "s":"BTCUSDT",
                        "c":"123456",
                        "S":"BUY",
                        "o":"LIMIT",
                        "f":"GTC",
                        "x":"TRADE",
                        "X":"PARTIALLY_FILLED",
                        "i":998877,
                        "t":556677,
                        "m":false,
                        "p":"64000.0",
                        "q":"0.010",
                        "ap":"64000.5",
                        "l":"0.002",
                        "z":"0.004",
                        "L":"64000.5",
                        "n":"0.01",
                        "rp":"1.25",
                        "N":"USDT"
                    }
                }
            }"#,
        );

        let emitted = parser.parse(json, &sink);
        assert_eq!(emitted, 1);

        let wrapped = sink.recv().expect("order event");
        let (event_type, scope, payload) =
            split_basic_account_event(&wrapped).expect("wrapped order");
        assert_eq!(event_type, BasicAccountEventType::OrderUpdate);
        assert_eq!(scope, BasicAccountScope::BinanceStdUm);

        let msg = BinanceBasicOrderMsg::from_bytes(payload).expect("order payload");
        assert_eq!(msg.venue, BinanceBasicOrderMsg::VENUE_UM);
        assert_eq!(msg.event_time, 1700000000000);
        assert_eq!(msg.trade_time, 1700000000123);
        assert_eq!(msg.symbol, "BTCUSDT");
        assert_eq!(msg.client_order_id, 123456);
        assert_eq!(msg.order_id, 998877);
        assert_eq!(msg.trade_id, 556677);
        assert!((msg.quantity - 0.010).abs() < 1e-12);
        assert!((msg.cumulative_filled_quantity - 0.004).abs() < 1e-12);
        assert!((msg.last_executed_price - 64000.5).abs() < 1e-9);
        assert!((msg.average_price - 64000.5).abs() < 1e-9);
        assert!((msg.commission - 0.01).abs() < 1e-12);
        assert!((msg.realized_pnl - 1.25).abs() < 1e-12);
        assert_eq!(msg.commission_asset, "USDT");
        assert_eq!(msg.external_order_kind, BinanceBasicOrderMsg::EXTERNAL_NONE);
    }

    #[test]
    fn settlement_autoclose_is_emitted_as_external_trade() {
        let parser = BinanceBasicAccountEventParser::new(true, BasicAccountScope::BinanceStdUm);
        let sink = TestAccountEventSink::new();
        let json = Bytes::from(
            r#"{
                "e":"ORDER_TRADE_UPDATE",
                "E":1787734851000,
                "T":1787734850999,
                "o":{
                    "s":"STORJUSDT",
                    "c":"settlement_autoclose-1787734851782",
                    "S":"SELL",
                    "o":"MARKET",
                    "f":"IOC",
                    "x":"CALCULATED",
                    "X":"FILLED",
                    "i":998877,
                    "t":556677,
                    "m":false,
                    "p":"0",
                    "q":"43407.86712966",
                    "ap":"0.1832",
                    "l":"43407.86712966",
                    "z":"43407.86712966",
                    "L":"0.1832",
                    "n":"0",
                    "rp":"-123.45",
                    "N":"USDT"
                }
            }"#,
        );

        assert_eq!(parser.parse(json, &sink), 1);
        let wrapped = sink.recv().expect("settlement order event");
        let (_, scope, payload) =
            split_basic_account_event(&wrapped).expect("wrapped settlement order");
        assert_eq!(scope, BasicAccountScope::BinanceStdUm);
        let msg = BinanceBasicOrderMsg::from_bytes(payload).expect("settlement payload");
        assert_eq!(msg.external_order_label(), Some("settlement"));
        assert_eq!(msg.client_order_id, -998877);
        assert_eq!(
            msg.execution_type,
            order_codes::execution_type_to_u8("TRADE")
        );
        assert!((msg.last_executed_quantity - 43_407.86712966).abs() < 1e-9);
    }

    #[test]
    fn execution_report_uses_orig_client_order_id_fallback() {
        let parser = BinanceBasicAccountEventParser::new(true, BasicAccountScope::BinanceStdSpot);
        let sink = TestAccountEventSink::new();
        let json = Bytes::from(
            r#"{
                "e":"executionReport",
                "E":1700000000000,
                "T":1700000000123,
                "s":"ETHUSDT",
                "c":"autoclose-1",
                "C":"987654",
                "S":"SELL",
                "o":"MARKET",
                "f":"IOC",
                "x":"CANCELED",
                "X":"CANCELED",
                "i":112233,
                "t":0,
                "m":true,
                "p":"0",
                "q":"0.5",
                "l":"0",
                "z":"0.2",
                "L":"0",
                "Z":"660.0",
                "n":"0",
                "N":""
            }"#,
        );

        let emitted = parser.parse(json, &sink);
        assert_eq!(emitted, 1);

        let wrapped = sink.recv().expect("order event");
        let (event_type, scope, payload) =
            split_basic_account_event(&wrapped).expect("wrapped order");
        assert_eq!(event_type, BasicAccountEventType::OrderUpdate);
        assert_eq!(scope, BasicAccountScope::BinanceStdSpot);

        let msg = BinanceBasicOrderMsg::from_bytes(payload).expect("order payload");
        assert_eq!(msg.venue, BinanceBasicOrderMsg::VENUE_MARGIN);
        assert_eq!(msg.symbol, "ETHUSDT");
        assert_eq!(msg.client_order_id, 987654);
        assert_eq!(msg.order_id, 112233);
        assert_eq!(msg.trade_id, 0);
        assert!(msg.is_maker != 0);
        assert!((msg.cumulative_filled_quantity - 0.2).abs() < 1e-12);
        assert!((msg.average_price - 3300.0).abs() < 1e-9);
    }

    #[test]
    fn trade_lite_emits_trade_update_lite_event() {
        let parser = BinanceBasicAccountEventParser::new(true, BasicAccountScope::BinanceStdUm);
        let sink = TestAccountEventSink::new();
        let json = Bytes::from(
            r#"{
                "e":"TRADE_LITE",
                "E":1700000000000,
                "T":1700000000123,
                "s":"BTCUSDT",
                "q":"0.010",
                "p":"0",
                "m":false,
                "c":"123456",
                "S":"BUY",
                "L":"64000.5",
                "l":"0.002",
                "t":556677,
                "i":998877
            }"#,
        );

        let emitted = parser.parse(json, &sink);
        assert_eq!(emitted, 1);

        let wrapped = sink.recv().expect("trade lite event");
        let (event_type, scope, payload) =
            split_basic_account_event(&wrapped).expect("wrapped trade lite");
        assert_eq!(event_type, BasicAccountEventType::TradeUpdateLite);
        assert_eq!(scope, BasicAccountScope::BinanceStdUm);

        let msg = BasicTradeLiteMsg::from_bytes(payload).expect("trade lite payload");
        assert_eq!(msg.venue, TradingVenue::BinanceFutures as u8);
        assert_eq!(msg.symbol, "BTCUSDT");
        assert_eq!(msg.client_order_id, 123456);
        assert_eq!(msg.trade_id_str(), "556677");
        assert!((msg.last_executed_price - 64000.5).abs() < 1e-9);
        assert!((msg.last_executed_quantity - 0.002).abs() < 1e-9);
    }

    #[test]
    fn standard_coin_order_update_uses_cm_venue() {
        let parser = BinanceBasicAccountEventParser::new(true, BasicAccountScope::BinanceStdCm);
        let sink = TestAccountEventSink::new();
        let json = Bytes::from(
            r#"{
                "e":"ORDER_TRADE_UPDATE","E":1700000000000,"T":1700000000123,
                "o":{"s":"BTCUSD_PERP","c":"123456","S":"BUY","o":"LIMIT",
                "f":"GTC","x":"NEW","X":"NEW","i":998877,"t":0,"m":false,
                "p":"64000","q":"2","ap":"0","l":"0","z":"0","L":"0",
                "n":"0","rp":"0","N":"BTC"}
            }"#,
        );
        assert_eq!(parser.parse(json, &sink), 1);
        let wrapped = sink.recv().expect("cm order event");
        let (_, scope, payload) = split_basic_account_event(&wrapped).expect("wrapped cm order");
        assert_eq!(scope, BasicAccountScope::BinanceStdCm);
        let msg = BinanceBasicOrderMsg::from_bytes(payload).expect("cm order payload");
        assert_eq!(msg.venue, BinanceBasicOrderMsg::VENUE_CM);
        assert_eq!(msg.symbol, "BTCUSD_PERP");
    }

    #[test]
    fn portfolio_margin_fs_cm_uses_unified_cm_scope() {
        let parser = BinanceBasicAccountEventParser::new(false, BasicAccountScope::BinanceUnified);
        let sink = TestAccountEventSink::new();
        let json = Bytes::from(
            r#"{
                "e":"ACCOUNT_UPDATE","E":1700000000000,"fs":"CM",
                "a":{"B":[],"P":[{"s":"ETHUSD_PERP","ps":"BOTH","pa":"-3","up":"0.01"}]}
            }"#,
        );
        assert_eq!(parser.parse(json, &sink), 2);
        let wrapped_position = sink.recv().expect("cm position event");
        let (_, scope, payload) =
            split_basic_account_event(&wrapped_position).expect("wrapped cm position");
        assert_eq!(scope, BasicAccountScope::BinanceUnifiedCm);
        let position = BasicPositionMsg::from_bytes(payload).expect("cm position payload");
        assert_eq!(position.inst_id, "ETHUSD_PERP");
        assert!((position.position_amount + 3.0).abs() < 1e-6);
        let wrapped_pnl = sink.recv().expect("cm pnl event");
        let (_, pnl_scope, _) = split_basic_account_event(&wrapped_pnl).expect("wrapped cm pnl");
        assert_eq!(pnl_scope, BasicAccountScope::BinanceUnifiedCm);
    }
}
