use crate::common::time_util::get_timestamp_us;
use crate::trade_engine::query_parsers::compact_order::{
    CompactOrderQueryResp, COMPACT_ORDER_QUERY_RESP_LEN,
};

pub fn parse_compact_order_query_resp(body: &bytes::Bytes) -> Option<CompactOrderQueryResp> {
    if body.len() < COMPACT_ORDER_QUERY_RESP_LEN {
        return None;
    }
    let parsed = CompactOrderQueryResp::from_bytes_prefix(body.as_ref()).ok()?;

    if parsed.order_id < 0 || parsed.executed_qty < 0.0 {
        return None;
    }
    if !parsed.response_price.is_finite() || parsed.response_price < 0.0 {
        return None;
    }
    if parsed.status_u8 == 0 {
        return None;
    }
    if parsed.update_time_ms != 0 {
        let now_ms = get_timestamp_us().saturating_div(1_000);
        if parsed.update_time_ms < 1_300_000_000_000 {
            return None;
        }
        if parsed.update_time_ms > now_ms.saturating_add(86_400_000) {
            return None;
        }
    }
    Some(parsed)
}
