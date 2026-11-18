//! Provenance of a finalized uniform fill. Numeric trade facts stay in the
//! ordinary uniform fields; this tail preserves identity during explicit repair.
use crate::{native_codec, FillLiquidity, SignalBbo, UnifiedOrderRecord, SIGNAL_BBO_BINARY_LEN};
use anyhow::{ensure, Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

pub const MAX_PROVENANCE_BYTES: usize = 64 * 1024;
pub const NORMALIZED_UNIFORM_MARKER_KEY: &[u8] = b"rapidx_uniform";
/// Content revision in the default CF, written atomically with in-place changes.
/// An absent key is revision zero. Ordinary uniform appends leave it unchanged.
pub const UNIFORM_ORDERS_REVISION_KEY: &[u8] = b"uniform_orders_revision";

pub mod normalize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniformFillProvenance {
    pub logical_record_key: String,
    pub original_received_at_us: i64,
    pub exchange_order_id: String,
    pub transaction_ids: Vec<String>,
    #[serde(default)]
    pub legacy_role_absent: bool,
    #[serde(default)]
    pub legacy_report_role: Option<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedUniformMarker {
    pub db_identity: String,
    pub portfolio: String,
    pub exchange: String,
    pub complete: bool,
    pub rewrite_epoch: u64,
    #[serde(default)]
    pub pending_orders: u64,
}

#[derive(Clone, Debug)]
pub struct StoredUniformOrder {
    pub received_at_us: i64,
    pub record: UnifiedOrderRecord,
    pub bbo_spread: String,
    pub provenance: Option<UniformFillProvenance>,
    pub role_present: bool,
}

fn bytes<'a>(input: &mut &'a [u8], size: usize) -> Result<&'a [u8]> {
    ensure!(size <= input.len(), "truncated uniform order");
    let (value, rest) = input.split_at(size);
    *input = rest;
    Ok(value)
}
fn i64_value(input: &mut &[u8]) -> Result<i64> {
    Ok(i64::from_le_bytes(bytes(input, 8)?.try_into()?))
}
fn f64_value(input: &mut &[u8]) -> Result<f64> {
    Ok(f64::from_le_bytes(bytes(input, 8)?.try_into()?))
}
fn u8_value(input: &mut &[u8]) -> Result<u8> {
    Ok(bytes(input, 1)?[0])
}
fn u16_value(input: &mut &[u8]) -> Result<usize> {
    Ok(u16::from_le_bytes(bytes(input, 2)?.try_into()?) as usize)
}
fn u32_value(input: &mut &[u8]) -> Result<usize> {
    Ok(u32::from_le_bytes(bytes(input, 4)?.try_into()?) as usize)
}

pub fn decode(payload: &[u8]) -> Result<StoredUniformOrder> {
    let mut input = payload;
    let received_at_us = i64_value(&mut input)?;
    let symbol_len = u16_value(&mut input)?;
    let symbol = bytes(&mut input, symbol_len)?.to_vec();
    let create_ts = i64_value(&mut input)?;
    let update_ts = i64_value(&mut input)?;
    let signal_ts = i64_value(&mut input)?;
    let submit_ts = i64_value(&mut input)?;
    let local_ts = i64_value(&mut input)?;
    let mkt_ts = i64_value(&mut input)?;
    let client_order_id = i64_value(&mut input)?;
    let venue = u8_value(&mut input)?;
    let ttype = u8_value(&mut input)?;
    let side = u8_value(&mut input)?;
    let price = f64_value(&mut input)?;
    let price_offset = f64_value(&mut input)?;
    let amount_init = f64_value(&mut input)?;
    let amount_update = f64_value(&mut input)?;
    let status = u8_value(&mut input)?;
    let from_len = u32_value(&mut input)?;
    let from_key = bytes(&mut input, from_len)?.to_vec();
    let bbo_spread = if input.is_empty() {
        String::new()
    } else {
        let size = u16_value(&mut input)?;
        String::from_utf8(bytes(&mut input, size)?.to_vec()).context("invalid uniform BBO text")?
    };
    let signal_bbo = if input.is_empty() {
        None
    } else {
        SignalBbo::decode_optional(bytes(&mut input, SIGNAL_BBO_BINARY_LEN)?)
            .map_err(anyhow::Error::msg)?
    };
    let role_present = !input.is_empty();
    let fill_liquidity = if role_present {
        FillLiquidity::from_u8(u8_value(&mut input)?).context("invalid uniform fill role")?
    } else {
        FillLiquidity::Unknown
    };
    let provenance = if input.is_empty() {
        None
    } else {
        let size = u32_value(&mut input)?;
        ensure!(
            size > 0 && size <= MAX_PROVENANCE_BYTES,
            "invalid uniform provenance size"
        );
        let value: UniformFillProvenance = serde_json::from_slice(bytes(&mut input, size)?)?;
        ensure!(
            !value.logical_record_key.is_empty() && value.original_received_at_us > 0,
            "invalid uniform provenance identity"
        );
        ensure!(
            value.legacy_report_role.is_none_or(|role| role <= 2),
            "invalid legacy uniform role"
        );
        Some(value)
    };
    ensure!(input.is_empty(), "unexpected uniform order tail");
    let mut record = UnifiedOrderRecord {
        symbol_len: 0,
        symbol,
        create_ts,
        update_ts,
        signal_ts,
        submit_ts,
        local_ts,
        mkt_ts,
        client_order_id,
        venue,
        ttype,
        side,
        price,
        price_offset,
        amount_init,
        amount_update,
        status,
        from_key_len: 0,
        from_key,
        signal_bbo,
        fill_liquidity,
    };
    record.refresh_lengths();
    let role_present = role_present && !provenance.as_ref().is_some_and(|p| p.legacy_role_absent);
    Ok(StoredUniformOrder {
        received_at_us,
        record,
        bbo_spread,
        provenance,
        role_present,
    })
}

pub fn encode(value: &StoredUniformOrder) -> Result<Bytes> {
    let symbol = std::str::from_utf8(&value.record.symbol)?;
    let payload = native_codec::serialize_uniform_order(
        &value.record,
        value.received_at_us,
        symbol,
        value.record.amount_init,
        value.record.amount_update,
    );
    let payload = native_codec::append_bbo_spread(payload, &value.bbo_spread);
    let Some(provenance) = &value.provenance else {
        return Ok(payload);
    };
    let mut provenance = provenance.clone();
    provenance.legacy_role_absent = !value.role_present;
    if value.role_present {
        provenance.legacy_report_role = None;
    }
    let json = serde_json::to_vec(&provenance)?;
    ensure!(
        json.len() <= MAX_PROVENANCE_BYTES,
        "uniform provenance exceeds bound"
    );
    let mut output = BytesMut::from(payload.as_ref());
    output.put_u32_le(json.len() as u32);
    output.extend_from_slice(&json);
    Ok(output.freeze())
}

/// Historical native trade records carried an order-level fallback. Preserve
/// its meaning separately from an explicit exchange role during maintenance.
pub fn legacy_report(payload: &[u8]) -> Result<Option<(Vec<u8>, u8, i64, FillLiquidity)>> {
    let mut input = payload;
    for _ in 0..3 {
        i64_value(&mut input)?;
    }
    let size = u32_value(&mut input)?;
    let mut symbol = String::from_utf8(bytes(&mut input, size)?.to_vec())?;
    i64_value(&mut input)?;
    let client = i64_value(&mut input)?;
    u8_value(&mut input)?;
    f64_value(&mut input)?;
    let role = match u8_value(&mut input)? {
        0 => FillLiquidity::Taker,
        1 => FillLiquidity::Maker,
        _ => anyhow::bail!("invalid legacy trade role"),
    };
    let venue = u8_value(&mut input)?;
    let quantity = f64_value(&mut input)?;
    ensure!(quantity.is_finite(), "invalid legacy trade quantity");
    match u8_value(&mut input)? {
        0 => {}
        1 => {
            u8_value(&mut input)?;
        }
        _ => anyhow::bail!("invalid legacy trade status"),
    };
    ensure!(input.is_empty(), "invalid legacy trade tail");
    if quantity <= 0.0 || client == 0 {
        return Ok(None);
    }
    if matches!(venue, 0 | 2) && symbol.ends_with("USDT") {
        symbol.truncate(symbol.len() - 4);
    }
    Ok(Some((symbol.into_bytes(), venue, client, role)))
}

#[cfg(test)]
mod tests;
