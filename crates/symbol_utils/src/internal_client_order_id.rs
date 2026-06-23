pub const BINANCE_UM_WS_CANCEL_PROBE_CLIENT_ORDER_ID_BASE: i64 = i64::MIN + 0x4257_5000;
pub const BINANCE_UM_WS_CANCEL_PROBE_CLIENT_ORDER_ID_SPAN: i64 = 1_000_000;

pub fn binance_um_ws_cancel_probe_client_order_id(endpoint_id: usize, seq: i64) -> i64 {
    let endpoint_part = ((endpoint_id as i64).rem_euclid(1_000)).saturating_mul(1_000);
    let seq_part = seq.rem_euclid(1_000);
    BINANCE_UM_WS_CANCEL_PROBE_CLIENT_ORDER_ID_BASE + endpoint_part + seq_part
}

pub fn is_binance_um_ws_cancel_probe_client_order_id(client_order_id: i64) -> bool {
    let offset = client_order_id.saturating_sub(BINANCE_UM_WS_CANCEL_PROBE_CLIENT_ORDER_ID_BASE);
    (0..BINANCE_UM_WS_CANCEL_PROBE_CLIENT_ORDER_ID_SPAN).contains(&offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binance_probe_ids_are_negative_and_in_range() {
        let id = binance_um_ws_cancel_probe_client_order_id(7, 42);

        assert!(id < 0);
        assert!(is_binance_um_ws_cancel_probe_client_order_id(id));
        assert!(!is_binance_um_ws_cancel_probe_client_order_id(42));
        assert!(!is_binance_um_ws_cancel_probe_client_order_id(0));
    }
}
