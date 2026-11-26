use super::normalize::{normalize, normalize_new, Lifecycle};
use super::*;
use crate::{
    rapidx_execution::{ExecutionEvidence, ExecutionRecord},
    rapidx_role::{ExecutionRole, ExecutionRoleRecord},
};

fn lifecycle(price: f64, quantity: f64, role: Option<FillLiquidity>) -> Lifecycle {
    let mut record = UnifiedOrderRecord {
        symbol_len: 0,
        symbol: b"FIL".to_vec(),
        create_ts: 1,
        update_ts: 100_000,
        signal_ts: 1,
        submit_ts: 2,
        local_ts: 3,
        mkt_ts: 4,
        client_order_id: 7,
        venue: 0,
        ttype: 3,
        side: 2,
        price,
        price_offset: 0.0,
        amount_init: 3.0,
        amount_update: quantity,
        status: 3,
        from_key_len: 0,
        from_key: b"batch_exec:alpha".to_vec(),
        signal_bbo: None,
        fill_liquidity: role.unwrap_or(FillLiquidity::Unknown),
        extension: None,
    };
    record.refresh_lengths();
    Lifecycle {
        key: format!("{:020}", 90_000),
        value: StoredUniformOrder {
            received_at_us: 90_000,
            record,
            bbo_spread: "audit-bbo".into(),
            provenance: None,
            role_present: role.is_some(),
        },
    }
}
fn execution(
    tx: &str,
    price: f64,
    quantity: f64,
    role: Option<&str>,
    rest: bool,
) -> ExecutionRecord {
    let mut value = serde_json::json!({"portfolioId":"123","exchangeType":"BINANCE","businessType":"MARGIN",
        "sym":"BINANCE_MARGIN_FIL_USDT","transactionId":tx,"orderId":"88","clientOrderId":"7",
        "side":"SELL","quantity":quantity.to_string(),"price":price.to_string(),"createAt":"100",
        "rpnl":"0","tradingFee":"0","tradingFeeCoin":"","fee":"0","feeCoin":"","rebate":"0","rebateCoin":""});
    if let Some(role) = role {
        value["execType"] = role.into();
    }
    ExecutionRecord {
        portfolio: "123".into(),
        exchange: "BINANCE".into(),
        execution: ExecutionEvidence::parse(&value, "123", "BINANCE", rest).unwrap(),
    }
}

#[test]
fn earlier_venue_order_event_validates_fills_when_local_clock_is_ahead() {
    let mut row = lifecycle(100.0, 1.0, Some(FillLiquidity::Unknown));
    row.value.record.create_ts = 122_054;
    row.value.record.update_ts = 99_000;
    let mut fill = execution("a", 100.0, 1.0, Some("MAKER"), false);

    let projected = normalize_new(&[row.clone()], &[fill.clone()], &[], "123", "BINANCE").unwrap();
    assert_eq!(projected[0].record.update_ts, 100_000);
    assert_eq!(projected[0].record.create_ts, 122_054);
    assert_eq!(projected[0].record.fill_liquidity, FillLiquidity::Maker);

    fill.execution.timestamp_ms = 98;
    assert!(normalize_new(&[row], &[fill], &[], "123", "BINANCE").is_err());
}

#[test]
fn uniform_normalization_recovers_once_and_preserves_provenance_and_raw_role() {
    let rows = [lifecycle(0.0, 3.0, Some(FillLiquidity::Unknown))];
    let a = execution("a", 100.0, 1.0, Some("MAKER"), false);
    let b = execution("b", 103.0, 2.0, Some("TAKER"), true);
    let duplicate = execution("a", 100.0, 1.0, Some("MAKER"), true);
    let output = normalize(&rows, &[a.clone(), b, duplicate], &[], "123", "BINANCE").unwrap();
    assert_eq!(output.len(), 2);
    assert_eq!(
        output.iter().map(|r| r.record.amount_update).sum::<f64>(),
        3.0
    );
    assert_eq!(
        output
            .iter()
            .map(|r| r.record.amount_update * r.record.price)
            .sum::<f64>(),
        306.0
    );
    assert_eq!(output[0].record.fill_liquidity, FillLiquidity::Maker);
    assert_eq!(output[1].record.fill_liquidity, FillLiquidity::Taker);
    assert_eq!(output[0].record.from_key, b"batch_exec:alpha");
    assert_eq!(rows[0].value.record.price, 0.0);
    let mut row = output[0].clone();
    row.received_at_us = 900_000;
    let encoded = encode(&row).unwrap();
    let decoded = decode(&encoded).unwrap();
    assert_eq!(
        encoded.as_ref(),
        include_bytes!("../testdata/rapidx_uniform.bin")
    );
    assert_eq!(decoded.record, row.record);
    assert_eq!(
        decoded.provenance.as_ref().unwrap().original_received_at_us,
        90_000
    );
    assert_eq!(
        decoded.provenance.as_ref().unwrap().logical_record_key,
        "00000000000000090000:execution:a"
    );
    assert_eq!(a.execution.liquidity, Some(ExecutionRole::Maker));
    assert_eq!(
        ExecutionRecord::from_ipc_payload(&a.to_ipc_payload().unwrap()).unwrap(),
        a
    );
    if let Some(path) = std::env::var_os("CTA_UNIFORM_FIXTURE") {
        std::fs::write(path, encoded).unwrap();
    }
}

#[test]
fn uniform_new_fills_wait_for_complete_execution_and_use_actual_prices() {
    let rows = [lifecycle(999.0, 1.0, Some(FillLiquidity::Unknown))];
    assert!(normalize_new(&rows, &[], &[], "123", "BINANCE").is_err());
    let facts = [execution("a", 100.0, 1.0, Some("MAKER"), false)];
    let new = normalize_new(&rows, &facts, &[], "123", "BINANCE").unwrap();
    assert_eq!(new[0].record.price, 100.0);
    assert_eq!(new[0].record.fill_liquidity, FillLiquidity::Maker);
    let historic = normalize(&rows, &facts, &[], "123", "BINANCE").unwrap();
    assert_eq!(historic[0].record.price, 999.0);
    assert_eq!(historic[0].record.update_ts, rows[0].value.record.update_ts);
}

#[test]
fn uniform_mixed_role_split_keeps_quantity_price_and_legacy_logical_ids() {
    let rows = [lifecycle(100.0, 3.0, Some(FillLiquidity::Unknown))];
    let facts = [
        execution("a", 100.0, 1.0, Some("MAKER"), false),
        execution("b", 100.0, 2.0, Some("TAKER"), false),
    ];
    let output = normalize(&rows, &facts, &[], "123", "BINANCE").unwrap();
    assert_eq!(output.len(), 2);
    assert_eq!(output[0].record.amount_update, 1.0);
    assert_eq!(output[1].record.amount_update, 2.0);
    for (i, row) in output.iter().enumerate() {
        assert_eq!(row.record.price, 100.0);
        assert_eq!(
            row.provenance.as_ref().unwrap().logical_record_key,
            format!("{}:liquidity:{i:08}", rows[0].key)
        );
    }
}

#[test]
fn uniform_rejects_conflicts_and_preserves_original_missing_role_semantics() {
    let rows = [lifecycle(0.0, 3.0, None)];
    assert!(normalize(
        &rows,
        &[execution("a", 100.0, 1.0, None, false)],
        &[],
        "123",
        "BINANCE"
    )
    .is_err());
    let ws = execution("a", 100.0, 3.0, Some("MAKER"), false);
    let rest = execution("a", 101.0, 3.0, Some("MAKER"), true);
    assert!(normalize(&rows, &[ws, rest], &[], "123", "BINANCE").is_err());
    let raw = [lifecycle(100.0, 1.0, None)];
    let output = normalize(&raw, &[], &[], "123", "BINANCE").unwrap();
    let decoded = decode(&encode(&output[0]).unwrap()).unwrap();
    assert!(!decoded.role_present);
    assert!(decoded.provenance.unwrap().legacy_role_absent);
}

#[test]
fn uniform_metadata_rejects_truncation_and_unknown_role_without_accepting_padding() {
    let row = lifecycle(100.0, 1.0, Some(FillLiquidity::Maker));
    let result = normalize(&[row], &[], &[], "123", "BINANCE").unwrap();
    let bytes = encode(&result[0]).unwrap();
    for n in [1, 2, 3, 4, 10] {
        assert!(decode(&bytes[..bytes.len() - n]).is_err());
    }
    let mut extra = bytes.to_vec();
    extra.push(0);
    assert!(decode(&extra).is_err());
}

#[test]
fn mixed_missing_execution_role_keeps_existing_explicit_role() {
    let rows = [lifecycle(100.0, 3.0, Some(FillLiquidity::Maker))];
    let facts = [
        execution("a", 100.0, 1.0, Some("MAKER"), false),
        execution("b", 100.0, 2.0, None, true),
    ];
    let output = normalize(&rows, &facts, &[], "123", "BINANCE").unwrap();
    assert_eq!(output.len(), 2);
    assert!(output
        .iter()
        .all(|r| r.record.fill_liquidity == FillLiquidity::Maker));
    assert_eq!(
        output.iter().map(|r| r.record.amount_update).sum::<f64>(),
        3.0
    );
}

#[test]
fn persisted_execution_role_is_joined_only_to_its_factual_trade() {
    let rows = [lifecycle(0.0, 3.0, Some(FillLiquidity::Unknown))];
    let first = execution("a", 100.0, 1.0, None, true);
    let second = execution("b", 103.0, 2.0, None, true);
    let role = ExecutionRoleRecord {
        record: first.clone(),
        role: ExecutionRole::Maker,
    };
    let output = normalize(
        &rows,
        &[first.clone(), second],
        std::slice::from_ref(&role),
        "123",
        "BINANCE",
    )
    .unwrap();
    assert_eq!(output[0].record.fill_liquidity, FillLiquidity::Maker);
    assert_eq!(output[1].record.fill_liquidity, FillLiquidity::Unknown);
    let mut conflict = role;
    conflict.record.execution.price = "101".into();
    assert!(normalize(&rows, &[first], &[conflict], "123", "BINANCE").is_err());
}

#[test]
fn stored_uniform_order_round_trips_the_cross_market_extension() {
    let mut value = lifecycle(101.5, 2.0, Some(FillLiquidity::Unknown)).value;
    value.record.venue = 65;
    value.record.extension = Some(crate::record_extension::RecordExtension {
        position_effect: Some(order_common::PositionEffect::CloseYesterday),
        trading_day: Some("20261120".into()),
        trade_id: Some("  88".into()),
        last_filled_quantity: Some(2.0),
        commission: Some(1.5),
        client_order_text: Some("ctp:1:2/17".into()),
    });
    let encoded = encode(&value).unwrap();
    let decoded = decode(&encoded).unwrap();
    assert_eq!(decoded.record, value.record);
    assert_eq!(decoded.bbo_spread, "audit-bbo");
    // Crypto records keep their exact bytes: no extension block is written.
    let mut crypto = value.clone();
    crypto.record.extension = None;
    let plain = encode(&crypto).unwrap();
    assert_eq!(encoded.len() - plain.len(), {
        let mut block = Vec::new();
        value
            .record
            .extension
            .as_ref()
            .unwrap()
            .as_ref()
            .write(&mut block);
        block.len()
    });
    assert_eq!(decode(&plain).unwrap().record.extension, None);
}
