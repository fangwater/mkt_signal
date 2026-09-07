use crate::quote_codec::{MSG_QUOTE, MSG_QUOTE_STATE, QUOTE_STATE_VALUE_LEN, QUOTE_VALUE_LEN};
use crate::raw::RawMessage;
use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

pub const MSG_TRADE: u8 = 0x02;
pub const MSG_CANCEL: u8 = 0x10;
pub const MSG_PREVIOUS_DAY: u8 = 0x11;
pub const MSG_REFERENCE_CORRECTION: u8 = 0x12;
pub const MSG_CORPORATE_ACTION_CORRECTION: u8 = 0x13;
pub const MSG_ADJUSTED_CLOSE_CORRECTION: u8 = 0x14;
pub const MSG_MOVING_AVERAGE_CORRECTION: u8 = 0x15;
pub const MSG_CLOSING_QUOTE_CORRECTION: u8 = 0x16;
pub const MSG_TRADE_RESTATEMENT: u8 = 0x17;
pub const MSG_PRICE_LIMIT_STATE: u8 = 0x21;
pub const MSG_NEWS_STATE: u8 = 0x22;
pub const MSG_SESSION_STATE: u8 = 0x23;
pub const MSG_OFFICIAL_CLOSE_STATE: u8 = 0x24;
pub const MSG_TECHNICAL_STATE: u8 = 0x25;
pub const MSG_SHORT_SALE_STATE: u8 = 0x26;
pub const MSG_ANNUAL_RANGE_STATE: u8 = 0x27;
pub const MSG_FUNDAMENTAL_STATE: u8 = 0x28;
pub const MSG_VALUATION_STATE: u8 = 0x29;
pub const MSG_TRADE_STATISTICS_STATE: u8 = 0x2a;
pub const MSG_VERIFY_STATE: u8 = 0x2b;
pub const MSG_CLOSING_RUN_CLEAR: u8 = 0x2c;
pub const MSG_CLOSING_RUN_STATE: u8 = 0x2d;
pub const MSG_REFRESH: u8 = 0x30;
pub const MSG_ALT_CLOSE: u8 = 0x32;

pub const TRADE_VALUE_LEN: usize = 64;
pub const CORRECTION_VALUE_LEN: usize = 72;
pub const SLOT_VALUE_LEN: usize = 24;
pub const SLOT_ENUM_LEN: usize = 8;
pub const SLOT_LEN: usize = SLOT_VALUE_LEN + SLOT_ENUM_LEN;
pub const SLOT_HEADER_LEN: usize = 16;
pub const MISSING_U16: u16 = u16::MAX;
pub const MISSING_U32: u32 = u32::MAX;
pub const MISSING_U64: u64 = u64::MAX;
pub const MISSING_DATE: i32 = i32::MIN;

const AUDITS: [&str; 4] = [
    include_str!("../audit_part0_shard0.json"),
    include_str!("../audit_part1_shard0.json"),
    include_str!("../audit_part2_shard0.json"),
    include_str!("../audit_part3_shard0.json"),
];

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Signature {
    message_class: String,
    update_type: String,
    fields: Vec<(u32, String)>,
}

impl Signature {
    fn from_message(message: &RawMessage) -> Self {
        Self {
            message_class: message.message_class.clone(),
            update_type: message.update_type.clone(),
            fields: message
                .fields
                .iter()
                .map(|field| (field.fid, field.name.clone()))
                .collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct Audit {
    first_example_by_combination: BTreeMap<String, AuditExample>,
}

#[derive(Debug, Deserialize)]
struct AuditExample {
    message_class: String,
    update_type: String,
    fields: Vec<AuditField>,
}

#[derive(Debug, Deserialize)]
struct AuditField {
    fid: u32,
    name: String,
}

#[derive(Debug, Clone)]
pub struct WireLayout {
    pub msg_type: u8,
    pub name: String,
    pub value_len: usize,
    slot_fields: Vec<(u32, String)>,
    pub exact_slots: bool,
}

#[derive(Debug)]
struct Registry {
    by_signature: BTreeMap<Signature, WireLayout>,
    by_type: BTreeMap<u8, WireLayout>,
    known_fields: BTreeSet<(u32, String)>,
}

fn semantic_layout(signature: &Signature) -> (u8, &'static str, bool) {
    let names = signature
        .fields
        .iter()
        .map(|(_, name)| name.as_str())
        .collect::<BTreeSet<_>>();
    if signature.message_class == "REFRESH" {
        return (MSG_REFRESH, "RefreshMsg", true);
    }
    if signature.update_type == "VERIFY" {
        return (MSG_VERIFY_STATE, "VerifyStateMsg", true);
    }
    if signature.update_type == "CLOSING_RUN" {
        return if names.contains("BID") {
            (MSG_CLOSING_RUN_STATE, "ClosingRunStateMsg", true)
        } else {
            (MSG_CLOSING_RUN_CLEAR, "ClosingRunClearMsg", true)
        };
    }
    if signature.update_type == "TRADE" {
        return if names.len() == 2 && names.contains("ALT_CLOSE") && names.contains("ALT_CLS_DT") {
            (MSG_ALT_CLOSE, "AlternateCloseStateMsg", true)
        } else {
            (MSG_TRADE, "TradeMsg", false)
        };
    }
    if signature.update_type == "QUOTE" {
        return (MSG_QUOTE, "QuoteMsg", false);
    }
    if signature.update_type == "CORRECTION" {
        if names.contains("CAN_PRC") && names.iter().all(|name| CANCEL_FIELDS.contains(name)) {
            return (MSG_CANCEL, "TradeCancelMsg", false);
        }
        if names.contains("PDTRDPRC") && names.iter().all(|name| PREVIOUS_DAY_FIELDS.contains(name))
        {
            return (MSG_PREVIOUS_DAY, "PreviousDayTradeMsg", false);
        }
        return if names.contains("XMIC_CODE") {
            (MSG_REFERENCE_CORRECTION, "ReferenceCorrectionMsg", true)
        } else if names.contains("EXDIVDATE")
            || names.contains("DIVPAYDATE")
            || names.contains("CUM_EX_MKR")
        {
            (
                MSG_CORPORATE_ACTION_CORRECTION,
                "CorporateActionCorrectionMsg",
                true,
            )
        } else if names.len() == 1 && names.contains("ADJUST_CLS") {
            (
                MSG_ADJUSTED_CLOSE_CORRECTION,
                "AdjustedCloseCorrectionMsg",
                true,
            )
        } else if names.len() == 1 && names.contains("VMA_10D") {
            (
                MSG_MOVING_AVERAGE_CORRECTION,
                "MovingAverageCorrectionMsg",
                true,
            )
        } else if names.contains("OFF_CLOSE") {
            (MSG_OFFICIAL_CLOSE_STATE, "OfficialCloseStateMsg", true)
        } else if names.contains("CLOSE_BID") {
            (
                MSG_CLOSING_QUOTE_CORRECTION,
                "ClosingQuoteCorrectionMsg",
                true,
            )
        } else {
            (MSG_TRADE_RESTATEMENT, "TradeRestatementCorrectionMsg", true)
        };
    }
    if names.contains("UPLIMIT") {
        (MSG_PRICE_LIMIT_STATE, "PriceLimitStateMsg", true)
    } else if names.contains("NEWS") {
        (MSG_NEWS_STATE, "NewsStateMsg", true)
    } else if names.contains("INST_PHASE") {
        (MSG_SESSION_STATE, "SessionStateMsg", true)
    } else if names.contains("OFF_CLOSE") {
        (MSG_OFFICIAL_CLOSE_STATE, "OfficialCloseStateMsg", true)
    } else if names.contains("IMP_VOLT") {
        (MSG_TECHNICAL_STATE, "TechnicalStateMsg", true)
    } else if names.contains("SH_SAL_RES") {
        (MSG_SHORT_SALE_STATE, "ShortSaleStateMsg", true)
    } else if names.contains("YRHIGH") {
        (MSG_ANNUAL_RANGE_STATE, "AnnualRangeStateMsg", true)
    } else if names.contains("EARNINGS") {
        (MSG_FUNDAMENTAL_STATE, "FundamentalStateMsg", true)
    } else if names.contains("YIELD") || names.contains("PERATIO") || names.contains("DIVPAYDATE") {
        (MSG_VALUATION_STATE, "ValuationStateMsg", true)
    } else if signature.update_type == "UNSPECIFIED" {
        (MSG_TRADE_STATISTICS_STATE, "TradeStatisticsStateMsg", true)
    } else {
        (MSG_TRADE_RESTATEMENT, "TradeRestatementCorrectionMsg", true)
    }
}

const CANCEL_FIELDS: [&str; 13] = [
    "CAN_COND_N",
    "CAN_COND",
    "CAN_PRC",
    "CAN_VOL",
    "CTRDTIM",
    "CTRDTIM_MS",
    "CAN_TDTH_X",
    "CAN_SUBIND",
    "CAN_DATE",
    "INS_SEQNO",
    "INSTIM_MS",
    "CAN_EXID",
    "CAN_TRD_ID",
];

const PREVIOUS_DAY_FIELDS: [&str; 11] = [
    "PD_SALCOND",
    "PDTRDPRC",
    "PREDAYVOL",
    "PD_SEQNO",
    "PDTRDDATE",
    "PD_TDTH_X",
    "PDTRDTM_MS",
    "PD_SUBIND",
    "PDACVOL",
    "REPORT_VOL",
    "PD_TRDID",
];

fn build_registry() -> Registry {
    let mut signatures = BTreeSet::new();
    for text in AUDITS {
        let audit: Audit = serde_json::from_str(text).expect("checked-in RAW audit JSON is valid");
        for example in audit.first_example_by_combination.into_values() {
            signatures.insert(Signature {
                message_class: example.message_class,
                update_type: example.update_type,
                fields: example
                    .fields
                    .into_iter()
                    .map(|field| (field.fid, field.name))
                    .collect(),
            });
        }
    }

    let mut known_fields = BTreeSet::<(u32, String)>::new();
    let mut slot_groups = BTreeMap::<u8, (String, BTreeSet<(u32, String)>)>::new();
    for signature in &signatures {
        known_fields.extend(signature.fields.iter().cloned());
        let (tag, name, exact_slots) = semantic_layout(signature);
        if exact_slots {
            let group = slot_groups
                .entry(tag)
                .or_insert_with(|| (name.to_string(), BTreeSet::new()));
            assert_eq!(group.0, name);
            group.1.extend(signature.fields.iter().cloned());
        }
    }
    let restatement = slot_groups
        .get_mut(&MSG_TRADE_RESTATEMENT)
        .expect("audited trade restatement layout exists");
    for signature in &signatures {
        if signature.update_type == "TRADE" || signature.update_type == "CORRECTION" {
            restatement.1.extend(signature.fields.iter().cloned());
        }
    }
    slot_groups
        .get_mut(&MSG_CORPORATE_ACTION_CORRECTION)
        .expect("audited corporate-action correction layout exists")
        .1
        .insert((38, "DIVPAYDATE".to_string()));
    slot_groups
        .get_mut(&MSG_VALUATION_STATE)
        .expect("audited valuation state layout exists")
        .1
        .insert((38, "DIVPAYDATE".to_string()));

    let mut by_type = BTreeMap::new();
    for (tag, (name, fields)) in slot_groups {
        let slot_fields = fields.into_iter().collect::<Vec<_>>();
        by_type.insert(
            tag,
            WireLayout {
                msg_type: tag,
                name,
                value_len: SLOT_HEADER_LEN + slot_fields.len() * SLOT_LEN,
                slot_fields,
                exact_slots: true,
            },
        );
    }
    for (tag, name, len) in [
        (MSG_QUOTE, "QuoteMsg", QUOTE_VALUE_LEN),
        (MSG_TRADE, "TradeMsg", TRADE_VALUE_LEN),
        (MSG_CANCEL, "TradeCancelMsg", CORRECTION_VALUE_LEN),
        (
            MSG_PREVIOUS_DAY,
            "PreviousDayTradeMsg",
            CORRECTION_VALUE_LEN,
        ),
        (MSG_QUOTE_STATE, "QuoteStateMsg", QUOTE_STATE_VALUE_LEN),
    ] {
        by_type.insert(
            tag,
            WireLayout {
                msg_type: tag,
                name: name.to_string(),
                value_len: len,
                slot_fields: Vec::new(),
                exact_slots: false,
            },
        );
    }
    let by_signature = signatures
        .into_iter()
        .map(|signature| {
            let tag = semantic_layout(&signature).0;
            let layout = by_type.get(&tag).expect("semantic layout exists").clone();
            (signature, layout)
        })
        .collect();
    Registry {
        by_signature,
        by_type,
        known_fields,
    }
}

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(build_registry)
}

pub fn layout_for_message(message: &RawMessage) -> Result<&'static WireLayout> {
    let signature = Signature::from_message(message);
    if let Some(layout) = registry().by_signature.get(&signature) {
        return Ok(layout);
    }
    let unknown = signature
        .fields
        .iter()
        .filter(|field| !registry().known_fields.contains(*field))
        .cloned()
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        bail!(
            "unsupported RAW FID(s) in {}/{} at {} {}: {:?}",
            message.message_class,
            if message.update_type.is_empty() {
                "<EMPTY>"
            } else {
                &message.update_type
            },
            message.ric,
            message.date_time,
            unknown
        );
    }
    let tag = semantic_layout(&signature).0;
    layout_for_type(tag)
}

pub fn layout_for_type(msg_type: u8) -> Result<&'static WireLayout> {
    registry()
        .by_type
        .get(&msg_type)
        .ok_or_else(|| anyhow!("unknown RAW wire msg_type 0x{msg_type:02x}"))
}

pub fn wire_layouts() -> Vec<(u8, String, usize)> {
    registry()
        .by_type
        .values()
        .map(|layout| (layout.msg_type, layout.name.clone(), layout.value_len))
        .collect()
}

pub fn wire_layout_fields(msg_type: u8) -> Result<Vec<(u32, String)>> {
    Ok(layout_for_type(msg_type)?.slot_fields.clone())
}

fn put_ascii<const N: usize>(out: &mut [u8], value: &str, label: &str) -> Result<()> {
    if !value.is_ascii() || value.as_bytes().contains(&0) || value.len() > N {
        bail!("{label} must be ASCII, NUL-free, and at most {N} bytes: {value:?}");
    }
    out[..value.len()].copy_from_slice(value.as_bytes());
    Ok(())
}

fn validate_ascii_slot(bytes: &[u8]) -> Result<()> {
    if bytes.iter().all(|byte| *byte == 0xff) {
        return Ok(());
    }
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    if bytes[end..].iter().any(|byte| *byte != 0) {
        bail!("nonzero byte follows fixed ASCII slot terminator");
    }
    std::str::from_utf8(&bytes[..end]).context("fixed ASCII slot is not UTF-8")?;
    Ok(())
}

pub fn encode_exact_slots(
    message: &RawMessage,
    source_ts_utc_ns: u64,
    source_order: u64,
    layout: &WireLayout,
) -> Result<Vec<u8>> {
    if !layout.exact_slots {
        bail!("exact-slot encoder received a typed RAW layout");
    }
    for field in &message.fields {
        let in_layout = layout
            .slot_fields
            .iter()
            .any(|(fid, name)| *fid == field.fid && name == &field.name);
        let in_typed_correction = message.update_type == "CORRECTION"
            && (CANCEL_FIELDS.contains(&field.name.as_str())
                || PREVIOUS_DAY_FIELDS.contains(&field.name.as_str()));
        if !in_layout && !in_typed_correction {
            bail!(
                "{} has no slot for FID {} {}",
                layout.name,
                field.fid,
                field.name
            );
        }
    }
    let mut out = vec![0xff_u8; layout.value_len];
    out[0..8].copy_from_slice(&source_ts_utc_ns.to_le_bytes());
    out[8..16].copy_from_slice(&source_order.to_le_bytes());
    for (index, (_, name)) in layout.slot_fields.iter().enumerate() {
        let start = SLOT_HEADER_LEN + index * SLOT_LEN;
        let Some(field) = message.fields.iter().find(|field| &field.name == name) else {
            continue;
        };
        out[start..start + SLOT_LEN].fill(0);
        put_ascii::<SLOT_VALUE_LEN>(
            &mut out[start..start + SLOT_VALUE_LEN],
            &field.value,
            &field.name,
        )?;
        put_ascii::<SLOT_ENUM_LEN>(
            &mut out[start + SLOT_VALUE_LEN..start + SLOT_LEN],
            &field.enum_value,
            &field.name,
        )?;
    }
    Ok(out)
}

pub fn validate_exact_slots(bytes: &[u8], layout: &WireLayout) -> Result<()> {
    if !layout.exact_slots || bytes.len() != layout.value_len {
        bail!("{} must be {} bytes", layout.name, layout.value_len);
    }
    for slot in bytes[SLOT_HEADER_LEN..].as_chunks::<SLOT_LEN>().0 {
        validate_ascii_slot(&slot[..SLOT_VALUE_LEN])?;
        validate_ascii_slot(&slot[SLOT_VALUE_LEN..])?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeValue {
    pub source_ts_utc_ns: u64,
    pub source_order: u64,
    pub event_ms: u32,
    pub exchange_id: u32,
    pub price: i64,
    pub size: u64,
    pub trade_id: u64,
    pub sequence: u64,
    pub condition: [u8; 4],
    pub flags: u16,
    pub quality_code: u16,
}

pub fn encode_trade(row: &TradeValue) -> [u8; TRADE_VALUE_LEN] {
    let mut out = [0_u8; TRADE_VALUE_LEN];
    out[0..8].copy_from_slice(&row.source_ts_utc_ns.to_le_bytes());
    out[8..16].copy_from_slice(&row.source_order.to_le_bytes());
    out[16..20].copy_from_slice(&row.event_ms.to_le_bytes());
    out[20..24].copy_from_slice(&row.exchange_id.to_le_bytes());
    out[24..32].copy_from_slice(&row.price.to_le_bytes());
    out[32..40].copy_from_slice(&row.size.to_le_bytes());
    out[40..48].copy_from_slice(&row.trade_id.to_le_bytes());
    out[48..56].copy_from_slice(&row.sequence.to_le_bytes());
    out[56..60].copy_from_slice(&row.condition);
    out[60..62].copy_from_slice(&row.flags.to_le_bytes());
    out[62..64].copy_from_slice(&row.quality_code.to_le_bytes());
    out
}

pub fn decode_trade(bytes: &[u8]) -> Result<TradeValue> {
    if bytes.len() != TRADE_VALUE_LEN {
        bail!(
            "TradeMsg must be {TRADE_VALUE_LEN} bytes, got {}",
            bytes.len()
        );
    }
    Ok(TradeValue {
        source_ts_utc_ns: u64::from_le_bytes(bytes[0..8].try_into()?),
        source_order: u64::from_le_bytes(bytes[8..16].try_into()?),
        event_ms: u32::from_le_bytes(bytes[16..20].try_into()?),
        exchange_id: u32::from_le_bytes(bytes[20..24].try_into()?),
        price: i64::from_le_bytes(bytes[24..32].try_into()?),
        size: u64::from_le_bytes(bytes[32..40].try_into()?),
        trade_id: u64::from_le_bytes(bytes[40..48].try_into()?),
        sequence: u64::from_le_bytes(bytes[48..56].try_into()?),
        condition: bytes[56..60].try_into()?,
        flags: u16::from_le_bytes(bytes[60..62].try_into()?),
        quality_code: u16::from_le_bytes(bytes[62..64].try_into()?),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrectionValue {
    pub source_ts_utc_ns: u64,
    pub source_order: u64,
    pub event_ms: u32,
    pub exchange_id: u32,
    pub price: i64,
    pub size: u64,
    pub trade_id: u64,
    pub sequence: u64,
    pub condition: [u8; 4],
    pub condition_code: u16,
    pub flags: u16,
    pub trade_date_days: i32,
}

pub fn encode_correction(row: &CorrectionValue) -> [u8; CORRECTION_VALUE_LEN] {
    let mut out = [0_u8; CORRECTION_VALUE_LEN];
    out[0..8].copy_from_slice(&row.source_ts_utc_ns.to_le_bytes());
    out[8..16].copy_from_slice(&row.source_order.to_le_bytes());
    out[16..20].copy_from_slice(&row.event_ms.to_le_bytes());
    out[20..24].copy_from_slice(&row.exchange_id.to_le_bytes());
    out[24..32].copy_from_slice(&row.price.to_le_bytes());
    out[32..40].copy_from_slice(&row.size.to_le_bytes());
    out[40..48].copy_from_slice(&row.trade_id.to_le_bytes());
    out[48..56].copy_from_slice(&row.sequence.to_le_bytes());
    out[56..60].copy_from_slice(&row.condition);
    out[60..62].copy_from_slice(&row.condition_code.to_le_bytes());
    out[62..64].copy_from_slice(&row.flags.to_le_bytes());
    out[64..68].copy_from_slice(&row.trade_date_days.to_le_bytes());
    out
}

pub fn decode_correction(bytes: &[u8]) -> Result<CorrectionValue> {
    if bytes.len() != CORRECTION_VALUE_LEN {
        bail!(
            "CorrectionMsg must be {CORRECTION_VALUE_LEN} bytes, got {}",
            bytes.len()
        );
    }
    if bytes[68..].iter().any(|byte| *byte != 0) {
        bail!("CorrectionMsg reserved bytes are nonzero");
    }
    Ok(CorrectionValue {
        source_ts_utc_ns: u64::from_le_bytes(bytes[0..8].try_into()?),
        source_order: u64::from_le_bytes(bytes[8..16].try_into()?),
        event_ms: u32::from_le_bytes(bytes[16..20].try_into()?),
        exchange_id: u32::from_le_bytes(bytes[20..24].try_into()?),
        price: i64::from_le_bytes(bytes[24..32].try_into()?),
        size: u64::from_le_bytes(bytes[32..40].try_into()?),
        trade_id: u64::from_le_bytes(bytes[40..48].try_into()?),
        sequence: u64::from_le_bytes(bytes[48..56].try_into()?),
        condition: bytes[56..60].try_into()?,
        condition_code: u16::from_le_bytes(bytes[60..62].try_into()?),
        flags: u16::from_le_bytes(bytes[62..64].try_into()?),
        trade_date_days: i32::from_le_bytes(bytes[64..68].try_into()?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raw::read_messages;

    fn to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn parse_one(rows: &str) -> RawMessage {
        let source = format!(
            "#RIC,Domain,Date-Time,GMT Offset,Type,MsgClass/FID number,UpdateType/Action,FID Name,FID Value,FID Enum String,PE Code,Template Number,Key/Msg Sequence Number,Number of FIDs\n{rows}"
        );
        let mut message = None;
        read_messages(source.as_bytes(), |row| {
            message = Some(row);
            Ok(())
        })
        .unwrap();
        message.unwrap()
    }

    #[test]
    fn typed_event_codecs_are_fixed_and_round_trip() {
        let trade = TradeValue {
            source_ts_utc_ns: 1,
            source_order: 2,
            event_ms: 3,
            exchange_id: 4,
            price: 5,
            size: 6,
            trade_id: 7,
            sequence: 8,
            condition: *b"@F  ",
            flags: 9,
            quality_code: 10,
        };
        let trade_bytes = encode_trade(&trade);
        assert_eq!(decode_trade(&trade_bytes).unwrap(), trade);
        assert_eq!(
            to_hex(&trade_bytes),
            "01000000000000000200000000000000030000000400000005000000000000000600000000000000070000000000000008000000000000004046202009000a00"
        );
        let correction = CorrectionValue {
            source_ts_utc_ns: 1,
            source_order: 2,
            event_ms: 3,
            exchange_id: 4,
            price: 5,
            size: 6,
            trade_id: 7,
            sequence: 8,
            condition: *b"@  I",
            condition_code: 9,
            flags: 10,
            trade_date_days: 11,
        };
        let correction_bytes = encode_correction(&correction);
        assert_eq!(decode_correction(&correction_bytes).unwrap(), correction);
        assert_eq!(
            to_hex(&correction_bytes),
            "01000000000000000200000000000000030000000400000005000000000000000600000000000000070000000000000008000000000000004020204909000a000b00000000000000"
        );
    }

    #[test]
    fn audited_registry_has_all_82_combinations() {
        assert_eq!(registry().by_signature.len(), 82);
        assert_eq!(layout_for_type(MSG_REFRESH).unwrap().name, "RefreshMsg");
        assert!(layout_for_type(MSG_REFRESH).unwrap().value_len > 9_000);
    }

    #[test]
    fn known_fids_may_form_a_new_correction_combination() {
        let message = parse_one(concat!(
            "AAPL.O,Market Price,2021-07-01T00:00:00Z,-4,Raw,UPDATE,CORRECTION,,,,74,,1,3\n",
            ",,,,FID,6583,,CAN_PRC,10.5,\n",
            ",,,,FID,6584,,CAN_VOL,100,\n",
            ",,,,FID,3247,,BC_10_50K,2,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_TRADE_RESTATEMENT);
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
        let missing_index = layout
            .slot_fields
            .iter()
            .position(|(_, name)| name == "CAN_DATE")
            .unwrap();
        let start = SLOT_HEADER_LEN + missing_index * SLOT_LEN;
        assert!(encoded[start..start + SLOT_LEN]
            .iter()
            .all(|byte| *byte == 0xff));
    }

    #[test]
    fn standalone_dividend_payment_date_is_a_corporate_action_correction() {
        let message = parse_one(concat!(
            "AAPL.O,Market Price,2021-08-06T11:46:09.159157226Z,-4,Raw,UPDATE,CORRECTION,,,,74,,48864,1\n",
            ",,,,FID,38,,DIVPAYDATE,2021-08-12,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_CORPORATE_ACTION_CORRECTION);
        assert_eq!(layout.value_len, 144);
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
        let index = layout
            .slot_fields
            .iter()
            .position(|(_, name)| name == "DIVPAYDATE")
            .unwrap();
        let start = SLOT_HEADER_LEN + index * SLOT_LEN;
        assert_eq!(&encoded[start..start + 10], b"2021-08-12");
    }

    #[test]
    fn valuation_update_keeps_dividend_payment_date() {
        let message = parse_one(concat!(
            "AAPL.O,Market Price,2021-10-29T04:37:00.930897158Z,-4,Raw,UPDATE,UNSPECIFIED,,,,74,,60320,3\n",
            ",,,,FID,35,,YIELD,0.5768,\n",
            ",,,,FID,36,,PERATIO,27.1876,\n",
            ",,,,FID,38,,DIVPAYDATE,2021-11-11,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_VALUATION_STATE);
        assert_eq!(layout.value_len, 112);
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn official_close_correction_is_an_incremental_state_patch() {
        let message = parse_one(concat!(
            "AAPL.O,Market Price,2022-01-21T21:30:00.027466157Z,-5,Raw,UPDATE,CORRECTION,,,,74,,12768,9\n",
            ",,,,FID,3372,,OFF_CLOSE,162.41,\n",
            ",,,,FID,1392,,CLOSE_TIME,21:30:00.000000000,\n",
            ",,,,FID,347,,CLOSE_TONE, ,\n",
            ",,,,FID,32,,ACVOL_1,121664716,\n",
            ",,,,FID,6588,,REPORT_VOL,121665703,\n",
            ",,,,FID,32743,,ACVOL_UNS,121664716,\n",
            ",,,,FID,6762,,OFF_CLS_DT,2022-01-21,\n",
            ",,,,FID,6377,,OFF_CL_TIM,21:30:00.000000000,\n",
            ",,,,FID,14258,,OFF_CLS_MS,21:30:00.008000000,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_OFFICIAL_CLOSE_STATE);
        assert_eq!(layout.value_len, 304);
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn genuinely_new_fid_is_rejected() {
        let message = parse_one(concat!(
            "AAPL.O,Market Price,2021-07-01T00:00:00Z,-4,Raw,UPDATE,UNSPECIFIED,,,,74,,1,1\n",
            ",,,,FID,999999,,NEW_FIELD,1,\n",
        ));
        assert!(layout_for_message(&message)
            .unwrap_err()
            .to_string()
            .contains("unsupported RAW FID"));
    }
}
