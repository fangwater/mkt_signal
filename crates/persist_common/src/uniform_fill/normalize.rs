//! One-order normalization shared by the live owner and its explicit repair.
use super::{StoredUniformOrder, UniformFillProvenance};
use crate::{rapidx_execution::ExecutionRecord, rapidx_role::ExecutionRoleRecord, FillLiquidity};
use anyhow::{ensure, Context, Result};
use std::collections::BTreeMap;

pub const MAX_ORDER_ROWS: usize = 8192;
pub const MAX_ORDER_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct Lifecycle {
    pub key: String,
    pub value: StoredUniformOrder,
}

#[derive(Clone, Debug)]
struct Fill {
    transaction: String,
    order: String,
    price: f64,
    quantity: f64,
    timestamp_us: i64,
    role: Option<FillLiquidity>,
}

pub fn order_key(row: &StoredUniformOrder) -> (Vec<u8>, u8, i64) {
    (
        row.record.symbol.clone(),
        row.record.venue,
        row.record.client_order_id,
    )
}

pub fn execution_key(row: &ExecutionRecord) -> Result<(Vec<u8>, u8, i64)> {
    row.validate()?;
    let parts: Vec<_> = row.execution.symbol.split('_').collect();
    ensure!(
        parts.len() == 4 && parts[0] == row.exchange,
        "execution symbol identity mismatch"
    );
    let venue = match (row.exchange.as_str(), parts[1]) {
        ("BINANCE", "SPOT" | "MARGIN") => 0,
        ("BINANCE", "PERP") => 1,
        ("OKX", "SPOT" | "MARGIN") => 2,
        ("OKX", "PERP") => 3,
        _ => anyhow::bail!("unsupported execution market"),
    };
    ensure!(
        matches!(parts[3], "USDT" | "USDC"),
        "unsupported execution quote"
    );
    let symbol = if venue == 0 || venue == 2 {
        ensure!(parts[3] == "USDT", "cash execution requires USDT quote");
        parts[2].to_owned()
    } else {
        format!("{}{}", parts[2], parts[3])
    };
    let client = row
        .execution
        .client_order_id
        .parse::<i64>()
        .context("non-numeric execution client identity")?;
    ensure!(client > 0, "nonpositive execution client identity");
    Ok((symbol.into_bytes(), venue, client))
}

fn logical(row: &Lifecycle) -> (&str, i64) {
    match &row.value.provenance {
        Some(p) => (&p.logical_record_key, p.original_received_at_us),
        None => (
            &row.key,
            row.key.parse().unwrap_or(row.value.received_at_us),
        ),
    }
}

fn number(value: &str) -> Result<f64> {
    let value: f64 = value.parse()?;
    ensure!(
        value.is_finite() && value > 0.0,
        "invalid execution quantity or price"
    );
    Ok(value)
}

fn set_role(row: &mut StoredUniformOrder, role: Option<FillLiquidity>) -> Result<()> {
    if let Some(role) = role {
        ensure!(
            !row.role_present
                || row.record.fill_liquidity == FillLiquidity::Unknown
                || row.record.fill_liquidity == role,
            "lifecycle and execution role conflict"
        );
        row.record.fill_liquidity = role;
        row.role_present = true;
    }
    Ok(())
}

fn provenance(row: &Lifecycle, ids: Vec<String>, order: String) -> StoredUniformOrder {
    let mut value = row.value.clone();
    let (key, received) = logical(row);
    value.provenance = Some(UniformFillProvenance {
        logical_record_key: key.to_owned(),
        original_received_at_us: received,
        exchange_order_id: order,
        transaction_ids: ids,
        legacy_role_absent: !row.value.role_present,
        legacy_report_role: None,
    });
    value
}

/// Preserve already priced lifecycle fills exactly. Recover absent/unpriced
/// quantities from complete executions; split only for a factual mixed role.
pub fn normalize(
    rows: &[Lifecycle],
    executions: &[ExecutionRecord],
    roles: &[ExecutionRoleRecord],
    portfolio: &str,
    exchange: &str,
) -> Result<Vec<StoredUniformOrder>> {
    ensure!(
        !rows.is_empty()
            && rows.len() <= MAX_ORDER_ROWS
            && executions.len() <= MAX_ORDER_ROWS
            && roles.len() <= MAX_ORDER_ROWS,
        "uniform order group exceeds normalization bound"
    );
    let template = &rows[0];
    let key = order_key(&template.value);
    let mut expected = 0.0;
    let mut unpriced = false;
    for row in rows {
        ensure!(
            row.key.len() == 20 && row.key.bytes().all(|b| b.is_ascii_digit()),
            "invalid lifecycle receive identity"
        );
        let record = &row.value.record;
        ensure!(
            order_key(&row.value) == key
                && record.side == template.value.record.side
                && record.from_key == template.value.record.from_key
                && record.signal_ts == template.value.record.signal_ts,
            "conflicting lifecycle attribution"
        );
        ensure!(
            record.amount_update.is_finite() && record.amount_update >= 0.0,
            "invalid lifecycle quantity"
        );
        ensure!(
            record.amount_update == 0.0 || matches!(record.side, 1 | 2),
            "invalid fill side"
        );
        expected += record.amount_update;
        unpriced |=
            record.amount_update > 0.0 && (!record.price.is_finite() || record.price <= 0.0);
    }
    ensure!(expected.is_finite(), "lifecycle quantity overflow");
    let mut role_by_id = BTreeMap::new();
    for role in roles {
        use crate::rapidx_fact::RapidXFact;
        role.validate()?;
        ensure!(
            role.record.portfolio == portfolio
                && role.record.exchange == exchange
                && execution_key(&role.record)? == key,
            "role source identity mismatch"
        );
        let id = &role.record.execution.transaction_id;
        if let Some(old) = role_by_id.insert(id.clone(), role) {
            ensure!(
                old.role == role.role && same_execution(&old.record, &role.record)?,
                "conflicting execution roles"
            );
        }
    }
    let mut fills: BTreeMap<String, Fill> = BTreeMap::new();
    for record in executions {
        record.validate()?;
        ensure!(
            record.portfolio == portfolio
                && record.exchange == exchange
                && execution_key(record)? == key,
            "execution source identity mismatch"
        );
        let execution = &record.execution;
        let quantity = number(&execution.quantity)?;
        let price = number(&execution.price)?;
        let timestamp_us = execution
            .timestamp_ms
            .checked_mul(1000)
            .context("execution time overflow")?;
        ensure!(
            timestamp_us >= template.value.record.create_ts / 1000 * 1000,
            "execution predates order creation"
        );
        ensure!(
            execution.side
                == if template.value.record.side == 1 {
                    "BUY"
                } else {
                    "SELL"
                },
            "execution side mismatch"
        );
        let mut role = execution.liquidity.map(|r| r.uniform());
        if let Some(evidence) = role_by_id.get(&execution.transaction_id) {
            ensure!(
                same_execution(record, &evidence.record)?,
                "execution role identity mismatch"
            );
            let observed = evidence.role.uniform();
            ensure!(
                role.is_none_or(|role| role == observed),
                "execution role conflict"
            );
            role = Some(observed);
        }
        let fill = Fill {
            transaction: execution.transaction_id.clone(),
            order: execution.order_id.clone(),
            price,
            quantity,
            timestamp_us,
            role,
        };
        if let Some(old) = fills.get_mut(&fill.transaction) {
            ensure!(
                old.order == fill.order
                    && old.price == fill.price
                    && old.quantity == fill.quantity
                    && old.timestamp_us == fill.timestamp_us
                    && old.role.zip(fill.role).is_none_or(|(a, b)| a == b),
                "conflicting WS/REST execution"
            );
            old.role = old.role.or(fill.role);
        } else {
            fills.insert(fill.transaction.clone(), fill);
        }
    }
    let actual: f64 = fills.values().map(|f| f.quantity).sum();
    ensure!(actual.is_finite(), "execution quantity overflow");
    let tolerance = expected.abs().max(actual.abs()) * 1e-12;
    let internal = template.value.record.ttype == crate::UNIFORM_ORDER_TYPE_INTERNAL_CROSS
        || template.value.record.client_order_id <= 0;
    if fills.is_empty() || actual + tolerance < expected && !unpriced || internal {
        ensure!(!unpriced, "waiting for complete execution prices");
        return Ok(rows
            .iter()
            .filter(|r| r.value.record.amount_update > 0.0)
            .map(|r| provenance(r, Vec::new(), String::new()))
            .collect());
    }
    ensure!(
        actual + tolerance >= expected,
        "incomplete execution quantity"
    );
    let order = fills
        .values()
        .next()
        .context("missing execution")?
        .order
        .clone();
    ensure!(
        fills.values().all(|f| f.order == order),
        "conflicting exchange order identities"
    );
    if unpriced || (actual - expected).abs() > tolerance {
        return Ok(fills
            .values()
            .map(|f| {
                let mut row = provenance(template, vec![f.transaction.clone()], order.clone());
                row.record.update_ts = f.timestamp_us;
                row.record.price = f.price;
                row.record.amount_update = f.quantity;
                row.record.fill_liquidity = f.role.unwrap_or(FillLiquidity::Unknown);
                row.role_present = true;
                let p = row.provenance.as_mut().unwrap();
                p.logical_record_key = format!(
                    "{:020}:execution:{}",
                    p.original_received_at_us, f.transaction
                );
                row
            })
            .collect());
    }
    let first_role = fills.values().next().and_then(|f| f.role);
    if first_role.is_some() && fills.values().all(|f| f.role == first_role) {
        let mut result = Vec::new();
        for r in rows.iter().filter(|r| r.value.record.amount_update > 0.0) {
            // A retained lifecycle delta can aggregate executions; do not
            // invent a one-to-one transaction attribution for that old delta.
            let mut row = provenance(r, Vec::new(), order.clone());
            set_role(&mut row, first_role)?;
            result.push(row);
        }
        return Ok(result);
    }
    let mut remaining: BTreeMap<String, f64> = fills
        .values()
        .map(|f| (f.transaction.clone(), f.quantity))
        .collect();
    let mut result = Vec::new();
    for original in rows.iter().filter(|r| r.value.record.amount_update > 0.0) {
        let record = &original.value.record;
        let mut needed = record.amount_update;
        let tolerance = needed.abs() * 1e-12;
        let mut matched = Vec::new();
        // The live reference matches by fill millisecond and factual price,
        // then by transaction ID, independently of WS/REST arrival order.
        let mut candidates = fills
            .values()
            .filter(|f| {
                f.timestamp_us / 1000 == record.update_ts / 1000
                    && (f.price - record.price).abs() <= record.price.abs() * 1e-12
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|f| (f.timestamp_us / 1000, &f.transaction));
        for fill in candidates {
            if needed <= tolerance {
                break;
            }
            let quantity = needed.min(remaining[&fill.transaction]);
            if quantity > 0.0 {
                needed -= quantity;
                matched.push((fill, quantity));
            }
        }
        if needed > tolerance {
            result.push(provenance(original, Vec::new(), order.clone()));
            continue;
        }
        for (fill, quantity) in &matched {
            *remaining.get_mut(&fill.transaction).unwrap() -= quantity;
        }
        let role = matched[0].0.role;
        let same = matched.iter().all(|(f, _)| f.role == role);
        if same {
            let mut row = provenance(
                original,
                matched.iter().map(|(f, _)| f.transaction.clone()).collect(),
                order.clone(),
            );
            set_role(&mut row, role)?;
            result.push(row);
        } else {
            let mut used = 0.0;
            for (index, (fill, quantity)) in matched.iter().enumerate() {
                let mut row = provenance(original, vec![fill.transaction.clone()], order.clone());
                row.record.amount_update = if index + 1 == matched.len() {
                    record.amount_update - used
                } else {
                    *quantity
                };
                used += row.record.amount_update;
                set_role(&mut row, fill.role)?;
                row.provenance.as_mut().unwrap().logical_record_key =
                    format!("{}:liquidity:{index:08}", logical(original).0);
                result.push(row);
            }
        }
    }
    Ok(result)
}

/// New orders use execution facts exclusively. Lifecycle messages establish
/// attribution and a quantity-completeness floor, never a guessed fill price.
pub fn normalize_new(
    rows: &[Lifecycle],
    executions: &[ExecutionRecord],
    roles: &[ExecutionRoleRecord],
    portfolio: &str,
    exchange: &str,
) -> Result<Vec<StoredUniformOrder>> {
    ensure!(!rows.is_empty(), "missing lifecycle attribution");
    if rows[0].value.record.client_order_id <= 0
        || rows[0].value.record.ttype == crate::UNIFORM_ORDER_TYPE_INTERNAL_CROSS
    {
        return normalize(rows, executions, roles, portfolio, exchange);
    }
    // First validate all identities/conflicts using the same bounded matcher.
    normalize(rows, executions, roles, portfolio, exchange)?;
    let mut unique = BTreeMap::new();
    for execution in executions {
        unique
            .entry(&execution.execution.transaction_id)
            .or_insert(number(&execution.execution.quantity)?);
    }
    let actual: f64 = unique.values().copied().sum();
    let expected: f64 = rows.iter().map(|r| r.value.record.amount_update).sum();
    ensure!(
        actual.is_finite()
            && expected.is_finite()
            && actual + expected.abs().max(actual.abs()) * 1e-12 >= expected,
        "waiting for complete factual executions"
    );
    let mut templates = rows.to_vec();
    for row in &mut templates {
        row.value.record.amount_update = 0.0;
    }
    normalize(&templates, executions, roles, portfolio, exchange)
}

fn same_execution(a: &ExecutionRecord, b: &ExecutionRecord) -> Result<bool> {
    let (a, b) = (&a.execution, &b.execution);
    Ok(a.transaction_id == b.transaction_id
        && a.order_id == b.order_id
        && a.client_order_id == b.client_order_id
        && a.symbol == b.symbol
        && a.side == b.side
        && a.timestamp_ms == b.timestamp_ms
        && number(&a.quantity)? == number(&b.quantity)?
        && number(&a.price)? == number(&b.price)?)
}
