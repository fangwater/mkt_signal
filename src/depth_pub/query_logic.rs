use log::{debug, warn};

use super::orderbook::{key_to_price, price_to_key, OrderBook};
use super::query_msg::{
    price_to_tick_index, resp_status_name, tick_index_to_price, DepthQueryHeader,
    DepthQueryLoadTlenBatchReq, DepthQueryLoadTlenBatchResp, DepthQueryLoadTlenSingleReq,
    DepthQueryLoadTlenSingleResp, DepthQueryTop5PriceTlenReq, DepthQueryTop5PriceTlenResp,
    DepthQueryType, DEPTH_QUERY_PAYLOAD, RESP_STATUS_BAD_REQUEST, RESP_STATUS_BOOK_INVALID,
    RESP_STATUS_OK, RESP_STATUS_PAYLOAD_TOO_LARGE, RESP_STATUS_SYMBOL_MISSING,
    RESP_STATUS_UNSUPPORTED_TYPE, TLEN_QUERY_AMOUNT_INVALID,
};

pub trait DepthQuerySource {
    fn venue_slug(&self) -> &str;
    fn price_tick_for_symbol(&self, symbol: &str) -> Option<f64>;
    fn resolve_orderbook(&self, symbol: &str) -> Option<&OrderBook>;
}

pub fn build_query_response<S: DepthQuerySource>(
    source: &S,
    payload: &[u8],
    resp: &mut [u8; DEPTH_QUERY_PAYLOAD],
) -> usize {
    let header = match DepthQueryHeader::parse(payload) {
        Ok(h) => h,
        Err(err) => {
            warn!("Depth query parse failed: {err:#}");
            resp[0] = RESP_STATUS_BAD_REQUEST;
            return 1;
        }
    };

    let payload_offset = match DepthQueryHeader::write(resp, header.query_type, &header.symbol) {
        Ok(offset) => offset,
        Err(err) => {
            warn!("Depth query response header build failed: {err:#}");
            resp[0] = RESP_STATUS_BAD_REQUEST;
            return 1;
        }
    };

    let req_payload = &payload[payload_offset..];
    let status;
    let mut body_len = 0usize;

    match DepthQueryType::from_u8(header.query_type) {
        Some(DepthQueryType::LoadTlenSingle) => {
            let resp_payload = &mut resp[payload_offset..];
            let (st, written) =
                handle_load_tlen_single_query(source, &header.symbol, req_payload, resp_payload);
            status = st;
            body_len = written;
        }
        Some(DepthQueryType::LoadTlenBatch) => {
            let resp_payload = &mut resp[payload_offset..];
            let (st, written) =
                handle_load_tlen_batch_query(source, &header.symbol, req_payload, resp_payload);
            status = st;
            body_len = written;
        }
        Some(DepthQueryType::Top5PriceTlen) => {
            let resp_payload = &mut resp[payload_offset..];
            let (st, written) =
                handle_top5_price_tlen_query(source, &header.symbol, req_payload, resp_payload);
            status = st;
            body_len = written;
        }
        _ => {
            let resp_payload = &mut resp[payload_offset..];
            if !resp_payload.is_empty() {
                resp_payload[0] = RESP_STATUS_UNSUPPORTED_TYPE;
                body_len = 1;
            }
            status = RESP_STATUS_UNSUPPORTED_TYPE;
        }
    }

    if body_len == 0 {
        let resp_payload = &mut resp[payload_offset..];
        if !resp_payload.is_empty() {
            resp_payload[0] = status;
            body_len = 1;
        }
    }

    debug!(
        "Depth query handled: venue={} symbol={} type={} status={}({})",
        source.venue_slug(),
        header.symbol,
        header.query_type,
        status,
        resp_status_name(status)
    );

    payload_offset + body_len
}

fn handle_load_tlen_single_query<S: DepthQuerySource>(
    source: &S,
    symbol: &str,
    req_payload: &[u8],
    resp_payload: &mut [u8],
) -> (u8, usize) {
    if resp_payload.len() < 1 + DepthQueryLoadTlenSingleResp::RESP_LEN {
        return (RESP_STATUS_PAYLOAD_TOO_LARGE, 0);
    }

    let req = match DepthQueryLoadTlenSingleReq::from_payload(req_payload) {
        Ok(ctx) => ctx,
        Err(err) => {
            warn!("Depth query load_tlen single req parse failed: {err:#}");
            return (RESP_STATUS_BAD_REQUEST, 1);
        }
    };

    let amount = query_tlen_amount_by_tick_index(source, symbol, req.tick_index);
    let resp = DepthQueryLoadTlenSingleResp {
        timestamp_us: req.timestamp_us,
        amount,
    };
    match resp.write_to(&mut resp_payload[1..]) {
        Ok(written) => {
            resp_payload[0] = RESP_STATUS_OK;
            (RESP_STATUS_OK, 1 + written)
        }
        Err(err) => {
            warn!("Depth query load_tlen single resp write failed: {err:#}");
            resp_payload[0] = RESP_STATUS_PAYLOAD_TOO_LARGE;
            (RESP_STATUS_PAYLOAD_TOO_LARGE, 1)
        }
    }
}

fn handle_load_tlen_batch_query<S: DepthQuerySource>(
    source: &S,
    symbol: &str,
    req_payload: &[u8],
    resp_payload: &mut [u8],
) -> (u8, usize) {
    if resp_payload.len() < 2 {
        return (RESP_STATUS_PAYLOAD_TOO_LARGE, 0);
    }

    let req = match DepthQueryLoadTlenBatchReq::from_payload(req_payload) {
        Ok(v) => v,
        Err(err) => {
            warn!("Depth query load_tlen batch req parse failed: {err:#}");
            return (RESP_STATUS_BAD_REQUEST, 1);
        }
    };

    let amounts: Vec<f64> = req
        .tick_indices
        .into_iter()
        .map(|tick_index| query_tlen_amount_by_tick_index(source, symbol, tick_index))
        .collect();

    match DepthQueryLoadTlenBatchResp::write_to(&mut resp_payload[1..], req.timestamp_us, &amounts)
    {
        Ok(written) => {
            resp_payload[0] = RESP_STATUS_OK;
            (RESP_STATUS_OK, 1 + written)
        }
        Err(err) => {
            warn!("Depth query load_tlen batch resp write failed: {err:#}");
            resp_payload[0] = RESP_STATUS_PAYLOAD_TOO_LARGE;
            (RESP_STATUS_PAYLOAD_TOO_LARGE, 1)
        }
    }
}

fn handle_top5_price_tlen_query<S: DepthQuerySource>(
    source: &S,
    symbol: &str,
    req_payload: &[u8],
    resp_payload: &mut [u8],
) -> (u8, usize) {
    if resp_payload.len() < 2 {
        return (RESP_STATUS_PAYLOAD_TOO_LARGE, 0);
    }

    let req = match DepthQueryTop5PriceTlenReq::from_payload(req_payload) {
        Ok(v) => v,
        Err(err) => {
            warn!("Depth query top5 price+tlen req parse failed: {err:#}");
            return (RESP_STATUS_BAD_REQUEST, 1);
        }
    };

    let Some(orderbook) = source.resolve_orderbook(symbol) else {
        return (RESP_STATUS_SYMBOL_MISSING, 1);
    };
    if !orderbook.is_valid() {
        return (RESP_STATUS_BOOK_INVALID, 1);
    }
    let Some(tick) = source.price_tick_for_symbol(symbol) else {
        return (RESP_STATUS_BAD_REQUEST, 1);
    };

    let (bids, asks) = orderbook.get_depth_keys(5);
    let Some(top5_bids) = depth_levels_to_tick_indices(&bids, tick) else {
        return (RESP_STATUS_BAD_REQUEST, 1);
    };
    let Some(top5_asks) = depth_levels_to_tick_indices(&asks, tick) else {
        return (RESP_STATUS_BAD_REQUEST, 1);
    };

    match DepthQueryTop5PriceTlenResp::write_to(
        &mut resp_payload[1..],
        req.timestamp_us,
        &top5_bids,
        &top5_asks,
    ) {
        Ok(written) => {
            resp_payload[0] = RESP_STATUS_OK;
            (RESP_STATUS_OK, 1 + written)
        }
        Err(err) => {
            warn!("Depth query top5 price+tlen resp write failed: {err:#}");
            resp_payload[0] = RESP_STATUS_PAYLOAD_TOO_LARGE;
            (RESP_STATUS_PAYLOAD_TOO_LARGE, 1)
        }
    }
}

fn query_tlen_amount_by_tick_index<S: DepthQuerySource>(
    source: &S,
    symbol: &str,
    tick_index: i64,
) -> f64 {
    let Some(tick) = source.price_tick_for_symbol(symbol) else {
        return TLEN_QUERY_AMOUNT_INVALID;
    };
    let Some(price) = tick_index_to_price(tick_index, tick) else {
        return TLEN_QUERY_AMOUNT_INVALID;
    };
    let price_key = price_to_key(price);

    let Some(orderbook) = source.resolve_orderbook(symbol) else {
        return TLEN_QUERY_AMOUNT_INVALID;
    };
    if !orderbook.is_valid() {
        return TLEN_QUERY_AMOUNT_INVALID;
    }

    orderbook.amount_at_price_key(price_key).unwrap_or(0.0)
}

fn depth_levels_to_tick_indices(levels: &[(i64, f64)], tick: f64) -> Option<Vec<(i64, f64)>> {
    let mut out = Vec::with_capacity(levels.len());
    for (price_key, amount) in levels {
        let price = key_to_price(*price_key);
        let tick_index = price_to_tick_index(price, tick)?;
        out.push((tick_index, *amount));
    }
    Some(out)
}
