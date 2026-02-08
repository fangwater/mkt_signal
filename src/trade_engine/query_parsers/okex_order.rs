use crate::pre_trade::order_manager::OrderExecutionStatus;
use crate::signal::common::TimeInForce;
use serde::Deserialize;

use super::binance_um_order::BinanceUmOrderQueryResp;

#[derive(Debug, Deserialize)]
struct OkexOrderQueryOuter {
    #[serde(default)]
    code: String,
    #[serde(default)]
    data: Vec<OkexOrderQueryItem>,
}

#[derive(Debug, Deserialize)]
struct OkexOrderQueryItem {
    #[serde(default, rename = "accFillSz")]
    acc_fill_sz: String,
    #[serde(default, rename = "fillPx")]
    fill_px: String,
    #[serde(default, rename = "px")]
    px: String,
    #[serde(default, rename = "ordId")]
    ord_id: String,
    #[serde(default, rename = "ordType")]
    ord_type: String,
    #[serde(default, rename = "state")]
    state: String,
    #[serde(default, rename = "uTime")]
    u_time: String,
}

fn parse_f64_str(v: &str) -> f64 {
    v.parse::<f64>().unwrap_or(0.0)
}

fn parse_i64_str(v: &str) -> i64 {
    v.parse::<i64>().unwrap_or(0)
}

fn status_to_u8(state: &str) -> u8 {
    match state {
        "live" | "partially_filled" => OrderExecutionStatus::Create.to_u8(),
        "filled" => OrderExecutionStatus::Filled.to_u8(),
        "canceled" | "cancelled" | "mmp_canceled" => OrderExecutionStatus::Cancelled.to_u8(),
        "rejected" => OrderExecutionStatus::Rejected.to_u8(),
        _ => OrderExecutionStatus::Create.to_u8(),
    }
}

fn tif_to_u8(ord_type: &str) -> u8 {
    match ord_type.to_ascii_lowercase().as_str() {
        "fok" | "op_fok" => TimeInForce::FOK.to_u8(),
        "ioc" | "optimal_limit_ioc" => TimeInForce::IOC.to_u8(),
        "post_only" | "mmp_and_post_only" => TimeInForce::GTX.to_u8(),
        _ => TimeInForce::GTC.to_u8(),
    }
}

fn response_price_for_state(state: &str, fill_px: f64, px: f64) -> f64 {
    match state.trim().to_ascii_lowercase().as_str() {
        "partially_filled" | "filled" => fill_px,
        _ => px,
    }
}

pub fn parse_okex_order_query_json(json: &str) -> Option<BinanceUmOrderQueryResp> {
    let outer: OkexOrderQueryOuter = serde_json::from_str(json).ok()?;
    if outer.code != "0" {
        return None;
    }
    let first = outer.data.first()?;
    let fill_px = parse_f64_str(first.fill_px.as_str());
    let px = parse_f64_str(first.px.as_str());
    let response_price = response_price_for_state(first.state.as_str(), fill_px, px);
    // OKX query:
    // - spot/margin: accFillSz 为 base qty
    // - futures(swap): accFillSz 为 contracts
    // 这里保持交易所原始口径；策略层再通过 qty_to_base(...) 统一转换为 base qty。
    Some(BinanceUmOrderQueryResp {
        executed_qty: parse_f64_str(first.acc_fill_sz.as_str()),
        order_id: parse_i64_str(first.ord_id.as_str()),
        status_u8: status_to_u8(first.state.as_str()),
        update_time_ms: parse_i64_str(first.u_time.as_str()),
        time_in_force_u8: tif_to_u8(first.ord_type.as_str()),
        response_price,
    })
}
