use anyhow::{anyhow, bail, Context, Result};
use order_common::{OrderExecutionStatus, TimeInForce};
use serde_json::Value;

use super::compact_order::CompactOrderQueryResp;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HyperliquidOrderQueryResult {
    Order(CompactOrderQueryResp),
    NotFound,
}

pub fn parse_hyperliquid_order_status_value(value: &Value) -> Result<HyperliquidOrderQueryResult> {
    let status = required_str(value.get("status"), "status")?;
    if status == "unknownOid" {
        return Ok(HyperliquidOrderQueryResult::NotFound);
    }
    if status != "order" {
        bail!("unexpected Hyperliquid orderStatus envelope status {status}");
    }

    let result = value
        .get("order")
        .and_then(Value::as_object)
        .context("Hyperliquid orderStatus missing order result")?;
    let order = result
        .get("order")
        .and_then(Value::as_object)
        .context("Hyperliquid orderStatus missing nested order")?;
    let exchange_status = required_str(result.get("status"), "order.status")?;
    let status_u8 = map_status(exchange_status)?.to_u8();
    let order_id = parse_u64(order.get("oid"), "order.oid")
        .and_then(|value| i64::try_from(value).context("Hyperliquid oid exceeds i64"))?;
    let original_qty = parse_nonnegative_decimal(order.get("origSz"), "order.origSz")?;
    let remaining_qty = parse_nonnegative_decimal(order.get("sz"), "order.sz")?;
    let tolerance = original_qty.abs().max(1.0) * 1e-12;
    if remaining_qty > original_qty + tolerance {
        bail!(
            "Hyperliquid orderStatus remaining size exceeds original: remaining={remaining_qty} original={original_qty}"
        );
    }
    let executed_qty = (original_qty - remaining_qty).max(0.0);
    // limitPx is the order's constraint, not a factual execution price.
    let _limit_price = parse_nonnegative_decimal(order.get("limitPx"), "order.limitPx")?;
    let update_time_ms = parse_i64(result.get("statusTimestamp"), "order.statusTimestamp")?;
    if update_time_ms <= 0 {
        bail!("Hyperliquid orderStatus timestamp must be positive");
    }
    let time_in_force_u8 = map_time_in_force(
        order.get("tif").and_then(Value::as_str),
        order.get("orderType").and_then(Value::as_str),
    )
    .to_u8();

    Ok(HyperliquidOrderQueryResult::Order(CompactOrderQueryResp {
        executed_qty,
        order_id,
        status_u8,
        update_time_ms,
        time_in_force_u8,
        response_price: 0.0,
    }))
}

fn map_status(status: &str) -> Result<OrderExecutionStatus> {
    match status {
        "open" => Ok(OrderExecutionStatus::Create),
        "filled" => Ok(OrderExecutionStatus::Filled),
        "canceled"
        | "triggered"
        | "marginCanceled"
        | "vaultWithdrawalCanceled"
        | "openInterestCapCanceled"
        | "selfTradeCanceled"
        | "reduceOnlyCanceled"
        | "siblingFilledCanceled"
        | "delistedCanceled"
        | "liquidatedCanceled"
        | "scheduledCancel" => Ok(OrderExecutionStatus::Cancelled),
        "rejected"
        | "tickRejected"
        | "minTradeNtlRejected"
        | "perpMarginRejected"
        | "reduceOnlyRejected"
        | "badAloPxRejected"
        | "iocCancelRejected"
        | "badTriggerPxRejected"
        | "marketOrderNoLiquidityRejected"
        | "positionIncreaseAtOpenInterestCapRejected"
        | "positionFlipAtOpenInterestCapRejected"
        | "tooAggressiveAtOpenInterestCapRejected"
        | "openInterestIncreaseRejected"
        | "insufficientSpotBalanceRejected"
        | "oracleRejected"
        | "perpMaxPositionRejected" => Ok(OrderExecutionStatus::Rejected),
        other => Err(anyhow!("unknown Hyperliquid order status {other}")),
    }
}

fn map_time_in_force(tif: Option<&str>, order_type: Option<&str>) -> TimeInForce {
    match tif.unwrap_or_default() {
        "Alo" => TimeInForce::GTX,
        "Ioc" | "FrontendMarket" => TimeInForce::IOC,
        "Gtc" => TimeInForce::GTC,
        _ if order_type == Some("Market") => TimeInForce::IOC,
        _ => TimeInForce::GTC,
    }
}

fn required_str<'a>(value: Option<&'a Value>, field: &str) -> Result<&'a str> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("Hyperliquid orderStatus missing {field}"))
}

fn parse_u64(value: Option<&Value>, field: &str) -> Result<u64> {
    let value = value.ok_or_else(|| anyhow!("Hyperliquid orderStatus missing {field}"))?;
    if let Some(value) = value.as_u64() {
        return Ok(value);
    }
    value
        .as_str()
        .ok_or_else(|| anyhow!("Hyperliquid orderStatus invalid {field}"))?
        .parse::<u64>()
        .with_context(|| format!("Hyperliquid orderStatus invalid {field}"))
}

fn parse_i64(value: Option<&Value>, field: &str) -> Result<i64> {
    let value = value.ok_or_else(|| anyhow!("Hyperliquid orderStatus missing {field}"))?;
    if let Some(value) = value.as_i64() {
        return Ok(value);
    }
    value
        .as_str()
        .ok_or_else(|| anyhow!("Hyperliquid orderStatus invalid {field}"))?
        .parse::<i64>()
        .with_context(|| format!("Hyperliquid orderStatus invalid {field}"))
}

fn parse_nonnegative_decimal(value: Option<&Value>, field: &str) -> Result<f64> {
    let text = value
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("Hyperliquid orderStatus missing {field}"))?;
    let parsed = text
        .parse::<f64>()
        .with_context(|| format!("Hyperliquid orderStatus invalid {field}"))?;
    if !parsed.is_finite() || parsed < 0.0 {
        bail!("Hyperliquid orderStatus invalid {field}: {text}");
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_partially_filled_open_order() {
        let value = serde_json::json!({
            "status": "order",
            "order": {
                "order": {
                    "coin": "ETH",
                    "limitPx": "2412.7",
                    "sz": "0.0026",
                    "origSz": "0.0076",
                    "oid": 123456,
                    "orderType": "Limit",
                    "tif": "Alo"
                },
                "status": "open",
                "statusTimestamp": 1724361546645_i64
            }
        });
        let HyperliquidOrderQueryResult::Order(parsed) =
            parse_hyperliquid_order_status_value(&value).unwrap()
        else {
            panic!("expected order");
        };
        assert!((parsed.executed_qty - 0.005).abs() < 1e-12);
        assert_eq!(parsed.order_id, 123456);
        assert_eq!(parsed.status_u8, OrderExecutionStatus::Create.to_u8());
        assert_eq!(parsed.time_in_force_u8, TimeInForce::GTX.to_u8());
        assert_eq!(parsed.response_price, 0.0);
    }

    #[test]
    fn parses_unknown_oid_marker() {
        assert_eq!(
            parse_hyperliquid_order_status_value(&serde_json::json!({
                "status": "unknownOid"
            }))
            .unwrap(),
            HyperliquidOrderQueryResult::NotFound
        );
    }

    #[test]
    fn maps_terminal_statuses_without_treating_reject_as_cancel() {
        for (status, expected) in [
            ("filled", OrderExecutionStatus::Filled),
            ("marginCanceled", OrderExecutionStatus::Cancelled),
            ("scheduledCancel", OrderExecutionStatus::Cancelled),
            ("badAloPxRejected", OrderExecutionStatus::Rejected),
            (
                "insufficientSpotBalanceRejected",
                OrderExecutionStatus::Rejected,
            ),
        ] {
            assert_eq!(map_status(status).unwrap(), expected);
        }
    }

    #[test]
    fn rejects_inconsistent_sizes_and_unknown_status() {
        let bad_size = serde_json::json!({
            "status": "order",
            "order": {
                "order": {
                    "limitPx": "1",
                    "sz": "2",
                    "origSz": "1",
                    "oid": 1,
                    "orderType": "Limit",
                    "tif": "Gtc"
                },
                "status": "open",
                "statusTimestamp": 1724361546645_i64
            }
        });
        assert!(parse_hyperliquid_order_status_value(&bad_size).is_err());
        assert!(map_status("futureUnknownStatus").is_err());
    }
}
