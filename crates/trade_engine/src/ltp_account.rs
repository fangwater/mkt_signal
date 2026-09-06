use crate::ltp_ws::{parse_ltp_user_data, LtpUserData};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use mkt_parsers::msg::basic_account_msg::{
    BasicAccountEventMsg, BasicAccountEventType, BasicAccountRiskMsg, BasicAccountScope,
    BasicBalanceMsg, BasicPositionMsg, BasicUmUnrealizedMsg,
};
use mkt_parsers::msg::basic_account_msg::{BinanceBasicOrderMsg, OkexOrderMsg};
use order_common::{ExecutionType, OrderStatus, OrderType, Side, TimeInForce};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub struct PositionSnapshotState {
    known: HashMap<(String, char), (BasicAccountScope, i64)>,
}

impl PositionSnapshotState {
    pub fn reconcile(
        &mut self,
        events: &mut Vec<Bytes>,
        complete: bool,
        observed_ms: i64,
    ) -> Result<()> {
        let mut present = HashSet::new();
        let mut accepted = Vec::new();
        for event in events.iter() {
            let Some((kind, scope, data)) =
                mkt_parsers::msg::basic_account_msg::split_basic_account_event(event)
            else {
                continue;
            };
            match kind {
                BasicAccountEventType::PositionUpdate => {
                    let position = BasicPositionMsg::from_bytes(data)?;
                    let key = (position.inst_id, position.position_side);
                    present.insert(key.clone());
                    if self
                        .known
                        .get(&key)
                        .is_some_and(|(_, ts)| *ts > position.timestamp)
                    {
                        continue;
                    }
                    self.known.insert(key, (scope, position.timestamp));
                }
                BasicAccountEventType::UnrealizedPnlUpdate => {
                    let pnl = BasicUmUnrealizedMsg::from_bytes(data)?;
                    if self
                        .known
                        .get(&(pnl.inst_id, pnl.position_side))
                        .is_some_and(|(_, ts)| *ts > pnl.timestamp)
                    {
                        continue;
                    }
                }
                _ => {}
            }
            accepted.push(event.clone());
        }
        if complete {
            for ((symbol, side), (scope, timestamp)) in &mut self.known {
                if !present.contains(&(symbol.clone(), *side)) && *timestamp <= observed_ms {
                    *timestamp = observed_ms;
                    accepted.push(wrap(
                        BasicAccountEventType::PositionUpdate,
                        *scope,
                        BasicPositionMsg::create(observed_ms, symbol.clone(), *side, 0.0)
                            .to_bytes(),
                    ));
                    accepted.push(wrap(
                        BasicAccountEventType::UnrealizedPnlUpdate,
                        *scope,
                        BasicUmUnrealizedMsg::create(observed_ms, symbol.clone(), *side, 0.0)
                            .to_bytes(),
                    ));
                }
            }
        }
        *events = accepted;
        Ok(())
    }
}

pub fn parse_order_push(
    payload: &str,
    portfolio_id: &str,
    exchange: &str,
) -> Result<Option<Bytes>> {
    let Some(LtpUserData::Order(order)) = parse_ltp_user_data(payload)? else {
        return Ok(None);
    };
    if order.portfolio_id != portfolio_id || !order.exchange_type.eq_ignore_ascii_case(exchange) {
        return Err(anyhow!("LTP order scope mismatch"));
    }
    if order
        .client_order_id
        .parse::<i64>()
        .ok()
        .filter(|id| *id > 0)
        .is_none()
        || order
            .order_id
            .parse::<i64>()
            .ok()
            .filter(|id| *id > 0)
            .is_none()
    {
        // The journal and execution history retain external string identities.
        // They cannot be assigned to a numeric strategy order by guessing an ID.
        return Ok(None);
    }
    if matches!(order.order_state.as_str(), "NEW" | "REJECT" | "FAIL") {
        return Ok(None);
    }
    if !matches!(
        order.order_state.as_str(),
        "OPEN" | "PARTIALLY_FILLED" | "FILLED" | "CANCELLED" | "REJECT" | "FAIL"
    ) {
        return Err(anyhow!("unsupported LTP order state {}", order.order_state));
    }
    if exchange == "BINANCE" {
        let order_id = order
            .order_id
            .parse()
            .map_err(|_| anyhow!("non-numeric LTP orderId cannot enter Binance lifecycle"))?;
        let client_id = order
            .client_order_id
            .parse()
            .map_err(|_| anyhow!("non-numeric LTP clientOrderId cannot enter Binance lifecycle"))?;
        let state = match order.order_state.as_str() {
            "FILLED" => OrderStatus::Filled,
            "CANCELLED" => OrderStatus::Canceled,
            "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
            _ => OrderStatus::New,
        };
        let exec = match state {
            OrderStatus::Canceled => ExecutionType::Canceled,
            OrderStatus::Filled | OrderStatus::PartiallyFilled => ExecutionType::Trade,
            _ => ExecutionType::New,
        }
        .to_u8();
        let side = if order.side == "BUY" {
            Side::Buy
        } else if order.side == "SELL" {
            Side::Sell
        } else {
            return Err(anyhow!("invalid LTP side"));
        };
        let typ = if order.exchange_order_type == "LIMIT" {
            OrderType::Limit
        } else if order.exchange_order_type == "MARKET" {
            OrderType::Market
        } else {
            return Err(anyhow!("invalid LTP order type"));
        };
        let msg = BinanceBasicOrderMsg::create(
            if order.business_type == "PERP" {
                BinanceBasicOrderMsg::VENUE_UM
            } else {
                BinanceBasicOrderMsg::VENUE_MARGIN
            },
            order.update_at_ms,
            order.update_at_ms,
            internal_symbol(&order.sym)?,
            order_id,
            client_id,
            0,
            side.to_u8(),
            typ.to_u8(),
            TimeInForce::from_str(&order.time_in_force)
                .unwrap_or(TimeInForce::GTC)
                .to_u8(),
            exec,
            state.to_u8(),
            false,
            decimal_text(&order.limit_price)?,
            decimal_text(&order.order_qty)?,
            decimal_text(&order.last_executed_qty)?,
            decimal_text(&order.executed_qty)?,
            decimal_text(&order.last_executed_price)?,
            decimal_text(&order.executed_avg_price)?,
            f64::NAN,
            f64::NAN,
            String::new(),
        );
        return Ok(Some(wrap(
            BasicAccountEventType::OrderUpdate,
            BasicAccountScope::BinanceUnified,
            msg.to_bytes(),
        )));
    }
    if exchange == "OKX" {
        let msg = OkexOrderMsg {
            msg_type: BasicAccountEventType::OrderUpdate,
            inst_id: internal_symbol(&order.sym)?,
            inst_type: if order.business_type == "PERP" { 2 } else { 1 },
            ord_id: order
                .order_id
                .parse()
                .map_err(|_| anyhow!("non-numeric RapidX orderId"))?,
            cl_ord_id: order
                .client_order_id
                .parse()
                .map_err(|_| anyhow!("non-numeric RapidX clientOrderId"))?,
            trade_id: 0,
            state: match order.order_state.as_str() {
                "OPEN" => 2,
                "PARTIALLY_FILLED" => 3,
                "FILLED" => 4,
                "CANCELLED" => 1,
                _ => return Err(anyhow!("unsupported RapidX order state")),
            },
            side: match order.side.as_str() {
                "BUY" => 1,
                "SELL" => 2,
                _ => return Err(anyhow!("invalid RapidX order side")),
            },
            ord_type: match (
                order.exchange_order_type.as_str(),
                order.time_in_force.as_str(),
            ) {
                ("MARKET", _) => 0,
                ("LIMIT", "GTC") => 1,
                ("LIMIT", "GTX") => 2,
                ("LIMIT", "FOK") => 3,
                ("LIMIT", "IOC") => 4,
                _ => return Err(anyhow!("unsupported RapidX order type")),
            },
            cancel_source: 0,
            amend_source: 0,
            price: decimal_text(&order.limit_price)?,
            quantity: decimal_text(&order.order_qty)?,
            cumulative_filled_quantity: decimal_text(&order.executed_qty)?,
            create_time: order.create_at_ms,
            update_time: order.update_at_ms,
            fill_time: order.update_at_ms,
        };
        return Ok(Some(wrap(
            BasicAccountEventType::OrderUpdate,
            BasicAccountScope::OkexUnified,
            msg.to_bytes(),
        )));
    }
    Err(anyhow!("unsupported RapidX exchange"))
}

fn decimal_text(value: &str) -> Result<f64> {
    if value.is_empty() {
        Ok(0.0)
    } else {
        let parsed = value
            .parse::<f64>()
            .map_err(|_| anyhow!("invalid LTP decimal"))?;
        if !parsed.is_finite() {
            return Err(anyhow!("non-finite RapidX decimal"));
        }
        Ok(parsed)
    }
}

pub fn parse_account_push(payload: &str, portfolio_id: &str, exchange: &str) -> Result<Vec<Bytes>> {
    if portfolio_id.trim().is_empty() || exchange.trim().is_empty() {
        return Err(anyhow!(
            "LTP account parser requires portfolio and exchange identity"
        ));
    }
    let value: Value = serde_json::from_str(payload)?;
    let Some(channel) = value.get("channel").and_then(Value::as_str) else {
        return Ok(vec![]);
    };
    if !matches!(channel, "Assets" | "Positions" | "Accounts" | "MarginCall") {
        return Ok(vec![]);
    }
    let scope = match exchange {
        "BINANCE" => BasicAccountScope::BinanceUnified,
        "OKX" => BasicAccountScope::OkexUnified,
        _ => return Err(anyhow!("unsupported LTP exchange {exchange}")),
    };
    let rows: Vec<&Map<String, Value>> = match value.get("data") {
        Some(Value::Array(rows)) => rows
            .iter()
            .map(|v| {
                v.as_object()
                    .ok_or_else(|| anyhow!("LTP {channel} row must be object"))
            })
            .collect::<Result<_>>()?,
        Some(Value::Object(row)) => vec![row],
        _ => return Err(anyhow!("LTP {channel} push missing data")),
    };
    let mut out = Vec::new();
    for row in rows {
        let source = row.get("exchangeType").and_then(Value::as_str).or_else(|| {
            row.get("sym")
                .and_then(Value::as_str)
                .and_then(|s| s.split('_').next())
        });
        if source.is_some_and(|source| source != exchange) {
            continue;
        }
        scope_row(row, portfolio_id, exchange, channel)?;
        let ts = millis(row, "updateAt").or_else(|_| millis(row, "createAt"))?;
        match channel {
            "Assets" => {
                let coin = text(row, "coin")?.to_ascii_uppercase();
                let balance = decimal(row, "balance")?;
                out.push(wrap(
                    BasicAccountEventType::BalanceUpdate,
                    scope,
                    BasicBalanceMsg::create(ts, coin, balance).to_bytes(),
                ));
            }
            "Positions" => {
                let symbol = internal_symbol(text(row, "sym")?)?;
                let qty_f64 = decimal(row, "positionQty")?;
                if qty_f64.abs() > f32::MAX as f64 {
                    return Err(anyhow!("LTP positionQty exceeds f32 range"));
                }
                let qty = qty_f64 as f32;
                let side = match text(row, "positionSide")?.to_ascii_uppercase().as_str() {
                    "LONG" => 'L',
                    "SHORT" => 'S',
                    "NONE" => 'N',
                    other => return Err(anyhow!("unknown LTP position side {other}")),
                };
                out.push(wrap(
                    BasicAccountEventType::PositionUpdate,
                    scope,
                    BasicPositionMsg::create(
                        ts,
                        symbol.clone(),
                        side,
                        if side == 'N' { qty } else { qty.abs() },
                    )
                    .to_bytes(),
                ));
                if let Ok(pnl) = decimal(row, "unrealizedPNL") {
                    out.push(wrap(
                        BasicAccountEventType::UnrealizedPnlUpdate,
                        scope,
                        BasicUmUnrealizedMsg::create(ts, symbol, side, pnl).to_bytes(),
                    ));
                }
            }
            "Accounts" | "MarginCall" => {
                let actual = optional_decimal(row, "equity");
                let adj = optional_decimal(row, "validMargin")
                    .or_else(|| optional_decimal(row, "netMarginValue"));
                let maintenance = optional_decimal(row, "maintainMargin");
                let ratio = decimal(row, "uniMMR")?;
                let risk = BasicAccountRiskMsg::create(
                    ts,
                    adj.unwrap_or(f64::NAN),
                    actual.unwrap_or(f64::NAN),
                    maintenance.unwrap_or(f64::NAN),
                    f64::NAN,
                    if row.get("accountStatus").and_then(Value::as_str) == Some("LIQUIDATED") {
                        0.0
                    } else {
                        ratio
                    },
                    optional_decimal(row, "loanValue")
                        .or_else(|| optional_decimal(row, "totalLoanValue"))
                        .unwrap_or(f64::NAN),
                    optional_decimal(row, "positionValue").unwrap_or(f64::NAN),
                );
                out.push(wrap(
                    BasicAccountEventType::AccountRisk,
                    scope,
                    risk.to_bytes(),
                ));
            }
            _ => unreachable!("channel filtered above"),
        }
    }
    Ok(out)
}

fn wrap(kind: BasicAccountEventType, scope: BasicAccountScope, payload: Bytes) -> Bytes {
    BasicAccountEventMsg::create(kind, scope, payload).to_bytes()
}
fn text<'a>(row: &'a Map<String, Value>, key: &str) -> Result<&'a str> {
    row.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("LTP field {key} must be string"))
}
fn decimal(row: &Map<String, Value>, key: &str) -> Result<f64> {
    let v = text(row, key)?
        .parse::<f64>()
        .map_err(|_| anyhow!("LTP field {key} invalid decimal"))?;
    if v.is_finite() {
        Ok(v)
    } else {
        Err(anyhow!("LTP field {key} non-finite"))
    }
}
fn optional_decimal(row: &Map<String, Value>, key: &str) -> Option<f64> {
    decimal(row, key).ok()
}
fn millis(row: &Map<String, Value>, key: &str) -> Result<i64> {
    let v = text(row, key)?
        .parse::<i64>()
        .map_err(|_| anyhow!("LTP field {key} invalid timestamp"))?;
    if v > 0 {
        Ok(v)
    } else {
        Err(anyhow!("LTP field {key} nonpositive timestamp"))
    }
}
fn scope_row(
    row: &Map<String, Value>,
    portfolio: &str,
    exchange: &str,
    channel: &str,
) -> Result<()> {
    if text(row, "portfolioId")? != portfolio {
        return Err(anyhow!("LTP {channel} portfolio mismatch"));
    }
    let actual = row
        .get("exchangeType")
        .and_then(Value::as_str)
        .or_else(|| {
            row.get("sym")
                .and_then(Value::as_str)
                .and_then(|s| s.split('_').next())
        })
        .unwrap_or("");
    if !actual.eq_ignore_ascii_case(exchange) {
        return Err(anyhow!("LTP {channel} exchange mismatch"));
    }
    Ok(())
}
fn internal_symbol(sym: &str) -> Result<String> {
    let parts: Vec<_> = sym.split('_').collect();
    if parts.len() == 4 && !parts[2].is_empty() && !parts[3].is_empty() {
        if parts[0].eq_ignore_ascii_case("OKX") && parts[1].eq_ignore_ascii_case("PERP") {
            Ok(format!("{}-{}-SWAP", parts[2], parts[3]))
        } else {
            Ok(if parts[0] == "OKX" {
                format!("{}-{}", parts[2], parts[3])
            } else {
                format!("{}{}", parts[2], parts[3])
            })
        }
    } else {
        Err(anyhow!("invalid LTP symbol {sym}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::basic_account_msg::{split_basic_account_event, BasicPositionMsg};

    fn order_fixture(exchange: &str, state: &str) -> String {
        serde_json::json!({"channel":"Orders", "data":{
            "portfolioId":"123", "exchangeType":exchange, "businessType":"PERP",
            "sym":format!("{exchange}_PERP_BTC_USDT"), "orderId":"10", "clientOrderId":"20",
            "limitPrice":"100", "orderQty":"2", "quoteOrderQty":"0", "side":"SELL",
            "exchangeOrderType":"LIMIT", "timeInForce":"IOC", "executedQty":"1",
            "executedAmount":"100", "executedAvgPrice":"100", "lastExecutedQty":"1",
            "lastExecutedPrice":"100", "lastExecutedAmount":"100", "fee":"0", "feeCoin":"",
            "rebate":"0.1", "rebateCoin":"USDT", "orderState":state,
            "updateAt":"1700000000001", "createAt":"1700000000000"
        }})
        .to_string()
    }

    #[test]
    fn ltp_order_conversion_uses_native_units_types_and_cumulative_fills() {
        let bytes = parse_order_push(
            &order_fixture("BINANCE", "PARTIALLY_FILLED"),
            "123",
            "BINANCE",
        )
        .unwrap()
        .unwrap();
        let (_, _, payload) = split_basic_account_event(&bytes).unwrap();
        let order = BinanceBasicOrderMsg::from_bytes(payload).unwrap();
        assert_eq!(order.venue, BinanceBasicOrderMsg::VENUE_UM);
        assert_eq!(order.cumulative_filled_quantity, 1.0);
        assert_eq!(order.execution_type, ExecutionType::Trade.to_u8());
        for (state, code) in [
            ("OPEN", 2),
            ("PARTIALLY_FILLED", 3),
            ("FILLED", 4),
            ("CANCELLED", 1),
        ] {
            let bytes = parse_order_push(&order_fixture("OKX", state), "123", "OKX")
                .unwrap()
                .unwrap();
            let (_, _, payload) = split_basic_account_event(&bytes).unwrap();
            let order = OkexOrderMsg::from_bytes(payload).unwrap();
            assert_eq!(order.inst_id, "BTC-USDT-SWAP");
            assert_eq!(order.quantity, 2.0);
            assert_eq!(order.state, code);
            assert_eq!(order.ord_type, 4);
            assert_eq!(order.side, 2);
        }
        assert!(
            parse_order_push(&order_fixture("BINANCE", "NEW"), "123", "BINANCE")
                .unwrap()
                .is_none()
        );
        assert!(parse_order_push(&order_fixture("BINANCE", "FILLED"), "999", "BINANCE").is_err());
    }

    #[test]
    fn external_string_order_identity_is_not_assigned_to_a_numeric_strategy() {
        for exchange in ["BINANCE", "OKX"] {
            let mut row: Value = serde_json::from_str(&order_fixture(exchange, "FILLED")).unwrap();
            row["data"]["clientOrderId"] = Value::String("manual_order".into());
            assert!(parse_order_push(&row.to_string(), "123", exchange)
                .unwrap()
                .is_none());
            row["data"]["clientOrderId"] = Value::String("20".into());
            row["data"]["orderId"] = Value::String("external_order".into());
            assert!(parse_order_push(&row.to_string(), "123", exchange)
                .unwrap()
                .is_none());
        }
    }

    #[test]
    fn position_snapshot_cannot_rewind_or_clear_a_newer_delta() {
        let mut state = PositionSnapshotState::default();
        let scope = BasicAccountScope::OkexUnified;
        let mut newer = vec![wrap(
            BasicAccountEventType::PositionUpdate,
            scope,
            BasicPositionMsg::create(30, "BTC-USDT-SWAP".into(), 'N', 2.0).to_bytes(),
        )];
        state.reconcile(&mut newer, false, 30).unwrap();
        let mut missing = vec![];
        state.reconcile(&mut missing, true, 20).unwrap();
        assert!(missing.is_empty());
        let mut stale = vec![
            wrap(
                BasicAccountEventType::PositionUpdate,
                scope,
                BasicPositionMsg::create(10, "BTC-USDT-SWAP".into(), 'N', 1.0).to_bytes(),
            ),
            wrap(
                BasicAccountEventType::UnrealizedPnlUpdate,
                scope,
                BasicUmUnrealizedMsg::create(10, "BTC-USDT-SWAP".into(), 'N', 1.0).to_bytes(),
            ),
        ];
        state.reconcile(&mut stale, true, 20).unwrap();
        assert!(stale.is_empty());
        state.reconcile(&mut missing, true, 40).unwrap();
        assert_eq!(missing.len(), 2);
    }

    #[test]
    fn ltp_complete_position_snapshot_clears_absent_position_but_delta_does_not() {
        let mut state = PositionSnapshotState::default();
        let mut events = vec![wrap(
            BasicAccountEventType::PositionUpdate,
            BasicAccountScope::BinanceUnified,
            BasicPositionMsg::create(1000, "BTCUSDT".into(), 'N', -2.0).to_bytes(),
        )];
        state.reconcile(&mut events, false, 1000).unwrap();
        let mut empty = vec![];
        state.reconcile(&mut empty, false, 2000).unwrap();
        assert!(empty.is_empty());
        state.reconcile(&mut empty, true, 3000).unwrap();
        assert_eq!(empty.len(), 2);
        let (_, _, payload) = split_basic_account_event(&empty[0]).unwrap();
        let position = BasicPositionMsg::from_bytes(payload).unwrap();
        assert_eq!(position.position_amount, 0.0);
        assert_eq!(position.timestamp, 3000);
    }

    #[test]
    fn ltp_risk_uses_reported_ratio_and_does_not_invent_initial_margin() {
        let payload = r#"{"channel":"Accounts","data":{"portfolioId":"123","exchangeType":"BINANCE","updateAt":"1700000000000","uniMMR":"999999","equity":"100","validMargin":"90","maintainMargin":"0","accountStatus":"NORMAL"}}"#;
        let events = parse_account_push(payload, "123", "BINANCE").unwrap();
        let (_, _, data) = split_basic_account_event(&events[0]).unwrap();
        let risk = BasicAccountRiskMsg::from_bytes(data).unwrap();
        assert_eq!(risk.margin_ratio, 999999.0);
        assert!(risk.initial_margin_usd.is_nan());
        assert!(risk.borrowed_usd.is_nan());
        assert!(parse_account_push(payload, "999", "BINANCE").is_err());
    }

    #[test]
    fn parses_scoped_negative_net_position_and_keeps_zero() {
        let body = r#"{"channel":"Positions","data":{"portfolioId":"p1","sym":"BINANCE_PERP_BTC_USDT","positionSide":"NONE","positionQty":"-2","unrealizedPNL":"0","updateAt":"1763977805203"}}"#;
        let out = parse_account_push(body, "p1", "BINANCE").unwrap();
        let (_, _, payload) = split_basic_account_event(&out[0]).unwrap();
        let pos = BasicPositionMsg::from_bytes(payload).unwrap();
        assert_eq!(pos.position_side, 'N');
        assert_eq!(pos.position_amount, -2.0);
    }

    #[test]
    fn unknown_channel_does_not_require_data_or_scope() {
        assert!(
            parse_account_push(r#"{"channel":"Orders"}"#, "p1", "BINANCE")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn okx_position_uses_swap_symbol() {
        let body = r#"{"channel":"Positions","data":{"portfolioId":"p1","sym":"OKX_PERP_BTC_USDT","positionSide":"LONG","positionQty":"1","updateAt":"1763977805203"}}"#;
        let out = parse_account_push(body, "p1", "OKX").unwrap();
        let (_, _, payload) = split_basic_account_event(&out[0]).unwrap();
        assert_eq!(
            BasicPositionMsg::from_bytes(payload).unwrap().inst_id,
            "BTC-USDT-SWAP"
        );
    }
}
