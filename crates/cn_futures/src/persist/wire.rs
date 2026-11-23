//! One record per IPC sample. Exec writes fixed binary layouts on its loop:
//! the two stored record kinds are already in their crypto store format
//! (uniform order, trade update) and persist_manager writes them unchanged;
//! SDK callbacks travel as their `#[repr(C)]` bytes and decisions as bincode,
//! for the trader log only. Nothing here is JSON on the Exec side, and nothing
//! JSON is stored.
use crate::model::{Intent, OrderKey};
use anyhow::{bail, ensure, Context, Result};
use bincode::Options;
use persist_common::cn_futures::store_cf;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;

const MAGIC: u32 = u32::from_le_bytes(*b"CNP2");
/// Both processes are built from one source; refuse records of another layout.
pub const LAYOUT: u64 = (2u64 << 48)
    | ((std::mem::size_of::<crate::ctp::HotEvent>() as u64) << 32)
    | ((std::mem::size_of::<crate::ctp::RawEvent>() as u64) << 16)
    | std::mem::size_of::<crate::yida::RawEvent>() as u64;

pub(super) const DISPLAY: u8 = 1;
pub(super) const SHUTDOWN_TAIL: u8 = 2;
const HEADER: usize = 4 + 8 + 1 + 1 + 8 + 4;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub(super) enum Kind {
    /// Rare lifecycle text for the trader log; never stored.
    Json = 1,
    CtpHot = 2,
    CtpControl = 3,
    Yida = 4,
    QuoteDecision = 5,
    StrategyResult = 6,
    /// Stored as is in `cn_uniform_orders`.
    UniformOrder = 7,
    /// Stored as is in `cn_trade_updates`.
    TradeUpdate = 8,
}
impl Kind {
    fn parse(value: u8) -> Result<Self> {
        Ok(match value {
            1 => Self::Json,
            2 => Self::CtpHot,
            3 => Self::CtpControl,
            4 => Self::Yida,
            5 => Self::QuoteDecision,
            6 => Self::StrategyResult,
            7 => Self::UniformOrder,
            8 => Self::TradeUpdate,
            _ => bail!("unknown native persistence record kind {value}"),
        })
    }
}

/// Why the exec did or did not act on a contract's quote.
#[derive(Serialize, Deserialize)]
pub(crate) struct QuoteDecision<S> {
    pub source_id: S,
    pub batch_id: S,
    pub target_revision: i64,
    pub market: S,
    pub reason: S,
    pub quote_received_us: u64,
    pub quote_generation: u64,
}

/// One strategy action outcome. Exec encodes borrowed data; persist_manager
/// decodes owned values and renders the operator record.
#[derive(Serialize, Deserialize)]
pub(crate) enum StrategyResult<'a> {
    SubmitReceipt {
        tag: u64,
        receipt: Cow<'a, crate::core::SubmitReceipt>,
    },
    CancelSubmittedLocally {
        tag: u64,
        key: OrderKey,
    },
    ActionRejected {
        tag: u64,
        error: Cow<'a, str>,
    },
    CancelRejected {
        tag: u64,
        key: OrderKey,
        error: Cow<'a, str>,
    },
    DispatchBlocked {
        retryable: bool,
        checks: crate::ctp::strategy::CtpDispatchChecks,
        action: ActionRecord<'a>,
        quote_received_us: u64,
        quote_generation: u64,
    },
}
#[derive(Serialize, Deserialize)]
pub(crate) enum ActionRecord<'a> {
    Submit {
        tag: u64,
        intent: Intent,
    },
    SubmitExec {
        tag: u64,
        intent: Intent,
        source_id: Cow<'a, str>,
        batch_id: Cow<'a, str>,
        target_revision: i64,
    },
    Cancel {
        tag: u64,
        key: OrderKey,
    },
}

pub(super) fn bincode() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(super::ipc::MAX_RECORD_BYTES as u64)
}

/// Writes the header; returns the offset of the body length to patch.
pub(super) fn begin(buf: &mut Vec<u8>, kind: Kind, flags: u8, received_us: u64) -> usize {
    buf.clear();
    buf.extend_from_slice(&MAGIC.to_le_bytes());
    buf.extend_from_slice(&LAYOUT.to_le_bytes());
    buf.push(kind as u8);
    buf.push(flags);
    buf.extend_from_slice(&received_us.to_le_bytes());
    let at = buf.len();
    buf.extend_from_slice(&0u32.to_le_bytes());
    at
}

pub(super) fn finish(buf: &mut [u8], length_at: usize) -> Result<()> {
    let body = buf.len() - length_at - 4;
    buf[length_at..length_at + 4].copy_from_slice(&u32::try_from(body)?.to_le_bytes());
    Ok(())
}

/// One decoded record: what persist_manager stores and what it prints.
pub struct Materialized<'a> {
    pub received_us: u64,
    /// Column family and exact bytes to store, for the two stored kinds.
    pub store: Option<(&'static str, &'a [u8])>,
    /// Trader-log line, rendered only for operator-visible records.
    pub display: Option<Value>,
}

/// persist_manager side: split a record into its stored bytes and log line.
pub fn materialize(record: &[u8]) -> Result<Materialized<'_>> {
    ensure!(
        record.len() >= HEADER,
        "truncated native persistence record"
    );
    ensure!(
        record[..4] == MAGIC.to_le_bytes(),
        "not a native persistence record"
    );
    ensure!(
        u64::from_le_bytes(record[4..12].try_into()?) == LAYOUT,
        "native persistence layout differs; deploy Exec and persist_manager together"
    );
    let kind = Kind::parse(record[12])?;
    let flags = record[13];
    let received_us = u64::from_le_bytes(record[14..22].try_into()?);
    let body_len = u32::from_le_bytes(record[22..26].try_into()?) as usize;
    let body = &record[HEADER..];
    ensure!(body.len() == body_len, "native persistence record length");
    let display = flags & DISPLAY != 0;
    let store = match kind {
        Kind::UniformOrder => Some((store_cf::ORDERS, body)),
        Kind::TradeUpdate => Some((store_cf::TRADES, body)),
        _ => None,
    };
    let display = if display {
        let mut value = render(kind, body, received_us)?;
        if flags & SHUTDOWN_TAIL != 0 {
            value["shutdown_tail"] = Value::Bool(true);
        }
        Some(value)
    } else {
        None
    };
    Ok(Materialized {
        received_us,
        store,
        display,
    })
}

fn render(kind: Kind, body: &[u8], received_us: u64) -> Result<Value> {
    use serde_json::json;
    Ok(match kind {
        Kind::Json => serde_json::from_slice(body).context("decode native log record")?,
        Kind::CtpHot => {
            ensure!(
                body.len() == std::mem::size_of::<crate::ctp::HotEvent>(),
                "CTP hot event size"
            );
            let event: crate::ctp::HotEvent = bytemuck::pod_read_unaligned(body);
            json!({"type":"ctp_event","event":event.audit(received_us)})
        }
        Kind::CtpControl => {
            ensure!(
                body.len() == std::mem::size_of::<crate::ctp::RawEvent>(),
                "CTP control event size"
            );
            let event: crate::ctp::RawEvent = bytemuck::pod_read_unaligned(body);
            json!({"type":"ctp_event","event":event.audit(received_us)})
        }
        Kind::Yida => {
            ensure!(
                body.len() == std::mem::size_of::<crate::yida::RawEvent>(),
                "Yida event size"
            );
            let event: crate::yida::RawEvent = bytemuck::pod_read_unaligned(body);
            json!({"type":"yida_event","received_us":received_us,"event":event})
        }
        Kind::QuoteDecision => {
            let d: QuoteDecision<String> = bincode()
                .deserialize(body)
                .context("decode exec quote decision")?;
            json!({"type":"exec_quote_decision","source_id":d.source_id,"batch_id":d.batch_id,
                "target_revision":d.target_revision,"market":d.market,"reason":d.reason,
                "quote_received_us":d.quote_received_us,"quote_generation":d.quote_generation})
        }
        Kind::StrategyResult => {
            let result: StrategyResult<'static> = bincode()
                .deserialize(body)
                .context("decode strategy result")?;
            strategy_result(result)
        }
        Kind::UniformOrder => uniform_order(body)?,
        Kind::TradeUpdate => trade_update(body)?,
    })
}

/// Operator rendering of one stored record (export and diagnostics).
pub fn render_stored(cf: &str, body: &[u8]) -> Result<Value> {
    match cf {
        store_cf::ORDERS => uniform_order(body),
        store_cf::TRADES => trade_update(body),
        _ => bail!("unknown domestic column family {cf}"),
    }
}

fn extension(ext: Option<&persist_common::record_extension::RecordExtension>) -> Value {
    use serde_json::json;
    match ext {
        None => Value::Null,
        Some(e) => json!({"position_effect":e.position_effect.map(|p| p.as_str()),
            "trading_day":e.trading_day,"trade_id":e.trade_id,
            "last_filled_quantity":e.last_filled_quantity,"commission":e.commission,
            "client_order_text":e.client_order_text}),
    }
}

fn uniform_order(body: &[u8]) -> Result<Value> {
    let stored = persist_common::uniform_fill::decode(body)?;
    let r = &stored.record;
    Ok(
        serde_json::json!({"type":"uniform_order","received_us":stored.received_at_us,
        "symbol":String::from_utf8_lossy(&r.symbol),"venue":r.venue,"client_order_id":r.client_order_id,
        "ttype":r.ttype,"side":r.side,"price":r.price,"amount_init":r.amount_init,
        "amount_update":r.amount_update,"status":r.status,"from_key":String::from_utf8_lossy(&r.from_key),
        "create_ts":r.create_ts,"update_ts":r.update_ts,"extension":extension(r.extension.as_ref())}),
    )
}

fn trade_update(body: &[u8]) -> Result<Value> {
    let t = persist_common::native_codec::decode_trade_update(body)?;
    Ok(
        serde_json::json!({"type":"trade_update","received_us":t.recv_ts_us,"symbol":t.symbol,
        "venue":t.venue,"order_id":t.order_id,"client_order_id":t.client_order_id,"side":t.side,
        "price":t.price,"cumulative_filled_quantity":t.cumulative_filled_quantity,
        "order_status":t.order_status,"extension":extension(t.extension.as_ref())}),
    )
}

fn strategy_result(result: StrategyResult<'_>) -> Value {
    use serde_json::json;
    match result {
        StrategyResult::SubmitReceipt { tag, receipt } => {
            json!({"type":"strategy_submit_receipt","tag":tag,"receipt":receipt})
        }
        StrategyResult::CancelSubmittedLocally { tag, key } => {
            json!({"type":"strategy_cancel_submitted_locally","tag":tag,"key":key})
        }
        StrategyResult::ActionRejected { tag, error } => {
            json!({"type":"strategy_action_rejected","tag":tag,"error":error})
        }
        StrategyResult::CancelRejected { tag, key, error } => {
            json!({"type":"strategy_cancel_rejected","tag":tag,"key":key,"error":error})
        }
        StrategyResult::DispatchBlocked {
            retryable,
            checks,
            action,
            quote_received_us,
            quote_generation,
        } => {
            let action = match action {
                ActionRecord::Submit { tag, intent } => {
                    json!({"op":"submit","tag":tag,"intent":intent})
                }
                ActionRecord::SubmitExec {
                    tag,
                    intent,
                    source_id,
                    batch_id,
                    target_revision,
                } => json!({"op":"submit_exec","tag":tag,"intent":intent,"source_id":source_id,
                    "batch_id":batch_id,"target_revision":target_revision}),
                ActionRecord::Cancel { tag, key } => json!({"op":"cancel","tag":tag,"key":key}),
            };
            json!({"type":"ctp_strategy_dispatch_blocked","sent_to_sdk":false,"retryable":retryable,
                "checks":checks,"action":action,"quote_received_us":quote_received_us,
                "quote_generation":quote_generation})
        }
    }
}
