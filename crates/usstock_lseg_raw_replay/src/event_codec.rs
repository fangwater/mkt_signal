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

pub const TRADE_VALUE_LEN: usize = 112;
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
        // Empty closing runs are discarded by the replay layer. Every encoded
        // closing run is therefore a full snapshot, even when its particular
        // update only carries metadata such as NEWS and has no BID field.
        return (MSG_CLOSING_RUN_STATE, "ClosingRunStateMsg", true);
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
        } else if names.contains("VMA_10D") {
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
        } else if names.contains("SH_SAL_RES") {
            (MSG_SHORT_SALE_STATE, "ShortSaleStateMsg", true)
        } else if names.contains("PROV_SYMB") {
            (MSG_REFERENCE_CORRECTION, "ReferenceCorrectionMsg", true)
        } else if names.contains("SECUR_ST")
            || names.contains("LMT_REFPR2")
            || names.contains("LIMIT_IND2")
            || names.iter().any(|name| {
                AUCTION_IMBALANCE_FIELDS
                    .iter()
                    .any(|(_, field)| field == name)
            })
        {
            (MSG_PRICE_LIMIT_STATE, "PriceLimitStateMsg", true)
        } else if names.iter().any(|name| ANNUAL_RANGE_FIELDS.contains(name)) {
            (MSG_ANNUAL_RANGE_STATE, "AnnualRangeStateMsg", true)
        } else {
            (MSG_TRADE_RESTATEMENT, "TradeRestatementCorrectionMsg", true)
        };
    }
    if names.contains("UPLIMIT")
        || names.contains("LMT_REFPR2")
        || names.contains("SECUR_ST")
        || names.contains("LIMIT_IND2")
        || names.iter().any(|name| {
            AUCTION_IMBALANCE_FIELDS
                .iter()
                .any(|(_, field)| field == name)
        })
    {
        (MSG_PRICE_LIMIT_STATE, "PriceLimitStateMsg", true)
    } else if names.contains("NEWS") {
        (MSG_NEWS_STATE, "NewsStateMsg", true)
    } else if names.contains("INST_PHASE") {
        (MSG_SESSION_STATE, "SessionStateMsg", true)
    } else if names.contains("OFF_CLOSE") {
        (MSG_OFFICIAL_CLOSE_STATE, "OfficialCloseStateMsg", true)
    } else if names.iter().any(|name| TECHNICAL_FIELDS.contains(name)) {
        (MSG_TECHNICAL_STATE, "TechnicalStateMsg", true)
    } else if names.contains("SH_SAL_RES") {
        (MSG_SHORT_SALE_STATE, "ShortSaleStateMsg", true)
    } else if names.contains("PROV_SYMB") {
        (MSG_REFERENCE_CORRECTION, "ReferenceCorrectionMsg", true)
    } else if names.iter().any(|name| ANNUAL_RANGE_FIELDS.contains(name)) {
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

const TECHNICAL_FIELDS: [&str; 19] = [
    "IMP_VOLT",
    "PMA_50D",
    "PMA_150D",
    "PMA_200D",
    "VMA_25D",
    "VMA_50D",
    "TRTN",
    "TRTN_3MT",
    "YR_TRTN",
    "YTD_TRTN",
    "TRTN_1W",
    "TRTN_1M",
    "TRTN_2Y",
    "TRTN_3Y",
    "TRTN_4Y",
    "TRTN_5Y",
    "MTD_TRTN",
    "QTD_TRTN",
    "ASY_VA_DT",
];

const ANNUAL_RANGE_FIELDS: [&str; 6] = [
    "YRHIGH",
    "YRLOW",
    "YCHIGH_IND",
    "YCLOW_IND",
    "YRHIGHDAT",
    "YRLOWDAT",
];

// Closing-auction imbalance updates are independent of the LULD and security
// status fields that normally identify PriceLimitStateMsg. They carry the
// indicative auction price, reference price, and buy/sell imbalance measures.
const AUCTION_IMBALANCE_FIELDS: [(u32, &str); 8] = [
    (7827, "AUC_ONL_PR"),
    (4344, "SEQNUM_IMB"),
    (4336, "IMB_PR_REF"),
    (777, "BUYMARGIN"),
    (779, "SELLMARGIN"),
    (3912, "IND_AUC"),
    (3870, "CLS_AUC"),
    (3873, "CLS_AUCVOL"),
];

const RETURN_FIELDS: [(u32, &str); 13] = [
    (4981, "TRTN"),
    (4982, "TRTN_3MT"),
    (5615, "YR_TRTN"),
    (5619, "YTD_TRTN"),
    (9959, "TRTN_1W"),
    (9960, "TRTN_1M"),
    (9961, "TRTN_2Y"),
    (9962, "TRTN_3Y"),
    (9963, "TRTN_4Y"),
    (9964, "TRTN_5Y"),
    (9965, "MTD_TRTN"),
    (9966, "QTD_TRTN"),
    (9257, "ASY_VA_DT"),
];

const CLOSING_RUN_EXTRA_FIELDS: [(u32, &str); 24] = [
    (8935, "RETAIL_INT"),
    (1501, "STOCK_TYPE"),
    (6513, "SETL_TYPE"),
    (6516, "BOOK_STATE"),
    (6694, "INSTRD_TIM"),
    (6810, "INSTRD_DT"),
    (4761, "CANCEL_IND"),
    (8950, "MK_STATUS"),
    (4333, "IMB_ACT_TP"),
    (4338, "IMB_SH"),
    (4340, "IMB_SIDE"),
    (4341, "IMB_TIM_MS"),
    (8414, "INS_SUBIND"),
    (3868, "OPN_AUC"),
    (3869, "INT_AUC"),
    (3871, "OPN_AUCVOL"),
    (3872, "INT_AUCVOL"),
    (6946, "CAN_SEQNO"),
    (7664, "PRIM_CLOSE"),
    (9250, "PRIMCLS_DT"),
    (32491, "MOC_ACVOL"),
    (14263, "ASK_TIM_NS"),
    (14264, "BID_TIM_NS"),
    (14265, "QUOTIM_NS"),
];

const REFRESH_EXTRA_FIELDS: [(u32, &str); 4] = [
    (104, "BOND_TYPE"),
    (1383, "DSO_ID"),
    (3915, "MKT_STATUS"),
    (4238, "RCS_AS_CLA"),
];

const FUNDAMENTAL_EXTRA_FIELDS: [(u32, &str); 5] = [
    (35, "YIELD"),
    (36, "PERATIO"),
    (37, "DIVIDENDTP"),
    (38, "DIVPAYDATE"),
    (71, "DIVIDEND"),
];

// Halt transitions are emitted as UNSPECIFIED market-state updates. The
// status, resume schedule, and price-quality fields form one atomic vendor
// state and must share a stable TradeStatisticsStateMsg layout.
const HALT_STATE_FIELDS: [(u32, &str); 14] = [
    (40, "CTS_QUAL"),
    (118, "PRC_QL_CD"),
    (131, "PRC_QL2"),
    (1021, "SEQNUM"),
    (3264, "PRC_QL3"),
    (3984, "TRD_TYPE"),
    (6517, "HALT_REASN"),
    (6614, "TRD_STATUS"),
    (6615, "HALT_RSN"),
    (6618, "HALT_DATE"),
    (6619, "HALT_TIME"),
    (9248, "HLT_RSM_DT"),
    (14250, "HLT_RSM_MS"),
    (14257, "HALT_TM_MS"),
];

// The LULD v2 reference-price panel is sometimes emitted without the legacy
// UPLIMIT/LOLIMIT identifiers. It is still one atomic price-limit state.
const LULD_V2_FIELDS: [(u32, &str); 6] = [
    (3815, "LOLIMIT_2"),
    (3816, "UPLIMIT_2"),
    (9072, "LULD_T2_MS"),
    (10011, "LMT_REFPR2"),
    (10339, "AUC_EX_NO"),
    (13584, "LMT_TYPE2"),
];

// Reference corrections carry the security identity panel as one atomic
// vendor snapshot. Keep its aliases and identifiers in a fixed layout.
const REFERENCE_CORRECTION_EXTRA_FIELDS: [(u32, &str); 28] = [
    (1, "PROD_PERM"),
    (2, "RDNDISPLAY"),
    (3, "DSPLY_NAME"),
    (4, "RDN_EXCHID"),
    (15, "CURRENCY"),
    (78, "OFFCL_CODE"),
    (104, "BOND_TYPE"),
    (105, "BCKGRNDPAG"),
    (198, "LOT_SIZE_A"),
    (869, "OFF_CD_IND"),
    (1080, "PREF_DISP"),
    (1055, "OFF_CD_IN2"),
    (1056, "OFFC_CODE2"),
    (1709, "RDN_EXCHD2"),
    (259, "RECORDTYPE"),
    (53, "TRD_UNITS"),
    (3183, "LIST_MKT"),
    (5357, "CONTEXT_ID"),
    (3422, "PROV_SYMB"),
    (3694, "MNEMONIC"),
    (39, "EXDIVDATE"),
    (2326, "YEAR_FCAST"),
    (6369, "POST_PANEL"),
    (3655, "ISIN_CODE"),
    (4738, "DOM_EQ_ID"),
    (4742, "CUSIP_CD"),
    (4758, "INSSALCOND"),
    (728, "BCAST_REF"),
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
    known_fields.insert((11685, "SECUR_ST".to_string()));
    // Preserve order-side evidence on trades without assuming it is aggressor side.
    known_fields.insert((1022, "PRNTYP".to_string()));
    known_fields.insert((13457, "HELD_T_IND".to_string()));
    known_fields.insert((2326, "YEAR_FCAST".to_string()));
    known_fields.insert((6369, "POST_PANEL".to_string()));
    known_fields.insert((3426, "ORDER_ID".to_string()));
    known_fields.insert((3428, "ORDER_SIDE".to_string()));
    known_fields.insert((4148, "TIMACT_MS".to_string()));
    known_fields.extend(
        RETURN_FIELDS
            .iter()
            .map(|(fid, name)| (*fid, (*name).to_string())),
    );
    known_fields.extend(
        CLOSING_RUN_EXTRA_FIELDS
            .iter()
            .map(|(fid, name)| (*fid, (*name).to_string())),
    );
    known_fields.extend(
        REFRESH_EXTRA_FIELDS
            .iter()
            .map(|(fid, name)| (*fid, (*name).to_string())),
    );
    known_fields.extend(
        FUNDAMENTAL_EXTRA_FIELDS
            .iter()
            .map(|(fid, name)| (*fid, (*name).to_string())),
    );
    known_fields.extend(
        HALT_STATE_FIELDS
            .iter()
            .map(|(fid, name)| (*fid, (*name).to_string())),
    );
    known_fields.extend(
        LULD_V2_FIELDS
            .iter()
            .map(|(fid, name)| (*fid, (*name).to_string())),
    );
    known_fields.extend(
        AUCTION_IMBALANCE_FIELDS
            .iter()
            .map(|(fid, name)| (*fid, (*name).to_string())),
    );
    known_fields.extend(
        REFERENCE_CORRECTION_EXTRA_FIELDS
            .iter()
            .map(|(fid, name)| (*fid, (*name).to_string())),
    );
    let restatement = slot_groups
        .get_mut(&MSG_TRADE_RESTATEMENT)
        .expect("audited trade restatement layout exists");
    for signature in &signatures {
        if signature.update_type == "TRADE" || signature.update_type == "CORRECTION" {
            restatement.1.extend(signature.fields.iter().cloned());
        }
    }
    // TradeRestatementCorrection is the fallback for otherwise unclassified
    // trade/correction updates. All audited fields need stable slots there so
    // a standalone vendor flag does not make the wire contract signature-led.
    restatement.1.extend(known_fields.iter().cloned());
    restatement.1.insert((3422, "PROV_SYMB".to_string()));
    slot_groups
        .get_mut(&MSG_CORPORATE_ACTION_CORRECTION)
        .expect("audited corporate-action correction layout exists")
        .1
        .insert((38, "DIVPAYDATE".to_string()));
    slot_groups
        .get_mut(&MSG_MOVING_AVERAGE_CORRECTION)
        .expect("audited moving-average correction layout exists")
        .1
        .insert((39, "EXDIVDATE".to_string()));
    slot_groups
        .get_mut(&MSG_CLOSING_QUOTE_CORRECTION)
        .expect("audited closing-quote correction layout exists")
        .1
        .extend([
            (39, "EXDIVDATE".to_string()),
            (40, "CTS_QUAL".to_string()),
            (118, "PRC_QL_CD".to_string()),
            (3264, "PRC_QL3".to_string()),
        ]);
    let valuation = slot_groups
        .get_mut(&MSG_VALUATION_STATE)
        .expect("audited valuation state layout exists");
    // Dividend valuation updates share the full fundamental valuation panel,
    // including yield, P/E, dividend type, payment date, and amount.
    for (fid, name) in FUNDAMENTAL_EXTRA_FIELDS {
        valuation.1.insert((fid, name.to_string()));
    }
    let price_limit = slot_groups
        .get_mut(&MSG_PRICE_LIMIT_STATE)
        .expect("audited price-limit state layout exists");
    // Price-limit updates are status snapshots and may co-carry the current
    // quote, quality, and auction fields. Preserve all audited fields on one
    // stable state layout rather than making the wire contract signature-led.
    price_limit.1.extend(known_fields.iter().cloned());
    for field in [
        (118, "PRC_QL_CD"),
        (3264, "PRC_QL3"),
        (8406, "QTE_ORIGIN"),
        (1041, "GV1_FLAG"),
        (11685, "SECUR_ST"),
        (3888, "FIN_STATUS"),
        (4333, "IMB_ACT_TP"),
        (4338, "IMB_SH"),
        (4340, "IMB_SIDE"),
        (4341, "IMB_TIM_MS"),
        (379, "SALTIM"),
        (3854, "SALTIM_MS"),
        (1044, "GV4_FLAG"),
        (6577, "SH_SAL_RES"),
    ]
    .into_iter()
    .chain(AUCTION_IMBALANCE_FIELDS)
    {
        price_limit.1.insert((field.0, field.1.to_string()));
    }
    let reference = slot_groups
        .get_mut(&MSG_REFERENCE_CORRECTION)
        .expect("audited reference correction layout exists");
    for field in REFERENCE_CORRECTION_EXTRA_FIELDS {
        reference.1.insert((field.0, field.1.to_string()));
    }
    slot_groups
        .get_mut(&MSG_OFFICIAL_CLOSE_STATE)
        .expect("audited official-close state layout exists")
        .1
        .extend([
            (19, "OPEN_PRC".to_string()),
            (1021, "SEQNUM".to_string()),
            (60, "CLOSE_BID".to_string()),
            (61, "CLOSE_ASK".to_string()),
            (3580, "BID_ASK_DT".to_string()),
        ]);
    let technical = slot_groups
        .get_mut(&MSG_TECHNICAL_STATE)
        .expect("audited technical state layout exists");
    for (fid, name) in RETURN_FIELDS {
        technical.1.insert((fid, name.to_string()));
    }
    let trade_statistics = slot_groups
        .get_mut(&MSG_TRADE_STATISTICS_STATE)
        .expect("audited trade-statistics state layout exists");
    // Standalone UNSPECIFIED market-state updates include both closing quote
    // metadata and dividend resets. Keep them in the one stable state layout.
    for field in [
        (60, "CLOSE_BID"),
        (61, "CLOSE_ASK"),
        (71, "DIVIDEND"),
        (3580, "BID_ASK_DT"),
    ] {
        trade_statistics.1.insert((field.0, field.1.to_string()));
    }
    for (fid, field) in HALT_STATE_FIELDS {
        trade_statistics.1.insert((fid, field.to_string()));
    }
    let session = slot_groups
        .get_mut(&MSG_SESSION_STATE)
        .expect("audited session state layout exists");
    // Session phase transitions can carry the current halt reason and quote
    // quality snapshot. These fields are state, not a separate quote event.
    for field in [
        (40, "CTS_QUAL"),
        (118, "PRC_QL_CD"),
        (131, "PRC_QL2"),
        (1021, "SEQNUM"),
        (3264, "PRC_QL3"),
        (3852, "PERIOD_CDE"),
        (6517, "HALT_REASN"),
        (6615, "HALT_RSN"),
        (8647, "PERIOD_CD2"),
    ] {
        session.1.insert((field.0, field.1.to_string()));
    }
    for (fid, field) in HALT_STATE_FIELDS {
        session.1.insert((fid, field.to_string()));
    }
    let short_sale = slot_groups
        .get_mut(&MSG_SHORT_SALE_STATE)
        .expect("audited short-sale state layout exists");
    for field in [
        (1044, "GV4_FLAG"),
        (3888, "FIN_STATUS"),
        (3422, "PROV_SYMB"),
        (6614, "TRD_STATUS"),
        (3984, "TRD_TYPE"),
        (6618, "HALT_DATE"),
        (6619, "HALT_TIME"),
    ] {
        short_sale.1.insert((field.0, field.1.to_string()));
    }
    let annual_range = slot_groups
        .get_mut(&MSG_ANNUAL_RANGE_STATE)
        .expect("audited annual-range state layout exists");
    for field in [(110, "YCHIGH_IND"), (111, "YCLOW_IND")] {
        annual_range.1.insert((field.0, field.1.to_string()));
    }
    let fundamental = slot_groups
        .get_mut(&MSG_FUNDAMENTAL_STATE)
        .expect("audited fundamental state layout exists");
    for (fid, name) in FUNDAMENTAL_EXTRA_FIELDS {
        fundamental.1.insert((fid, name.to_string()));
    }
    // Closing runs are full-market snapshots. Give every known field a stable
    // slot so different source signatures cannot create variable wire values.
    slot_groups
        .get_mut(&MSG_CLOSING_RUN_STATE)
        .expect("audited closing-run state layout exists")
        .1
        .extend(known_fields.iter().cloned());
    // Refreshes are full-market snapshots as well, including fields that are
    // otherwise only seen on specialized updates.
    let refresh = slot_groups
        .get_mut(&MSG_REFRESH)
        .expect("audited refresh layout exists");
    refresh.1.extend(known_fields.iter().cloned());

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
    pub order_id: [u8; 24],
    pub order_side: u16,
    pub aggressor_side: u8,
    pub unknown_reason: u8,
    pub venue_class: u8,
    pub side_method: u8,
    pub side_flags: u8,
    pub print_type: [u8; 8],
    pub held_trade_indicator: u16,
    pub activity_ms: u32,
}

/// No feed-specific ORDER_SIDE mapping is verified yet. Keep raw evidence
/// and classify the reason for N independently of the venue classification.
pub fn classify_trade_direction(venue: &str, order_side: u16) -> (u8, u8, u8) {
    let venue_class = match venue {
        "NAS" | "NYS" | "PSE" | "ASE" | "BAT" | "BTY" | "DEA" | "DEX" | "BOS" | "CIN" | "IEX"
        | "MID" | "MMX" | "MPE" | "XPH" => 1,
        "ADF" | "TRF" | "FINN" | "FINY" | "FINC" | "XADF" => 2,
        _ => 0,
    };
    let reason = if venue_class == 2 {
        1
    } else if order_side != 0 && order_side != MISSING_U16 {
        4
    } else if venue_class == 1 {
        2
    } else {
        3
    };
    (b'N', reason, venue_class)
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
    out[64..88].copy_from_slice(&row.order_id);
    out[88..90].copy_from_slice(&row.order_side.to_le_bytes());
    out[90] = row.aggressor_side;
    out[91] = row.unknown_reason;
    out[92] = row.venue_class;
    out[93] = row.side_method;
    out[94..102].copy_from_slice(&row.print_type);
    out[102..104].copy_from_slice(&row.held_trade_indicator.to_le_bytes());
    out[104..108].copy_from_slice(&row.activity_ms.to_le_bytes());
    out[108] = row.side_flags;
    out
}

pub fn decode_trade(bytes: &[u8]) -> Result<TradeValue> {
    if bytes.len() != TRADE_VALUE_LEN {
        bail!(
            "TradeMsg must be {TRADE_VALUE_LEN} bytes, got {}",
            bytes.len()
        );
    }
    validate_ascii_slot(&bytes[64..88])?;
    validate_ascii_slot(&bytes[94..102])?;
    if !matches!(bytes[90], b'B' | b'S' | b'N')
        || bytes[91] > 4
        || bytes[92] > 2
        || bytes[93] > 10
        || bytes[108] & !15 != 0
        || bytes[109..112] != [0; 3]
        || (bytes[90] == b'N') != (bytes[91] != 0)
    {
        bail!("invalid TradeMsg direction or reserved bytes");
    }
    let method = bytes[93];
    if (1..=8).contains(&method) {
        let forced = method == 1 || method >= 6;
        if bytes[90] == b'N' || bytes[92] == 2 || bytes[108] & 3 != 1 | (u8::from(forced) << 1) {
            bail!("inconsistent estimated TradeMsg direction provenance");
        }
        if method == 1 {
            let order_side = u16::from_le_bytes(bytes[88..90].try_into()?);
            if !matches!((order_side, bytes[90]), (1, b'S') | (2, b'B')) {
                bail!("ORDER_SIDE reversal does not match raw evidence");
            }
        }
    } else if method == 9
        && (bytes[90] != b'N' || bytes[91] != 1 || bytes[92] != 2 || bytes[108] & 3 != 0)
    {
        bail!("inconsistent off-exchange direction provenance");
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
        order_id: bytes[64..88].try_into()?,
        order_side: u16::from_le_bytes(bytes[88..90].try_into()?),
        aggressor_side: bytes[90],
        unknown_reason: bytes[91],
        venue_class: bytes[92],
        side_method: bytes[93],
        side_flags: bytes[108],
        print_type: bytes[94..102].try_into()?,
        held_trade_indicator: u16::from_le_bytes(bytes[102..104].try_into()?),
        activity_ms: u32::from_le_bytes(bytes[104..108].try_into()?),
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
            order_id: [0xff; 24],
            order_side: MISSING_U16,
            aggressor_side: b'N',
            unknown_reason: 3,
            venue_class: 0,
            side_method: 0,
            side_flags: 0,
            print_type: [0xff; 8],
            held_trade_indicator: MISSING_U16,
            activity_ms: MISSING_U32,
        };
        let trade_bytes = encode_trade(&trade);
        assert_eq!(decode_trade(&trade_bytes).unwrap(), trade);
        assert_eq!(
            to_hex(&trade_bytes),
            concat!("01000000000000000200000000000000030000000400000005000000000000000600000000000000070000000000000008000000000000004046202009000a00",
                "ffffffffffffffffffffffffffffffffffffffffffffffffffff4e030000ffffffffffffffffffffffffffff00000000")
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
    fn valuation_update_keeps_dividend_fields() {
        let message = parse_one(concat!(
            "AAPL.O,Market Price,2022-04-29T04:35:50.831316890Z,-4,Raw,UPDATE,UNSPECIFIED,,,,74,,14432,5\n",
            ",,,,FID,35,,YIELD,0.5622,\n",
            ",,,,FID,36,,PERATIO,27.1681,\n",
            ",,,,FID,37,,DIVIDENDTP,0,\"  \"\n",
            ",,,,FID,38,,DIVPAYDATE,2022-05-12,\n",
            ",,,,FID,71,,DIVIDEND,0.92,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_VALUATION_STATE);
        for (_, field) in FUNDAMENTAL_EXTRA_FIELDS {
            assert!(layout.slot_fields.iter().any(|(_, name)| name == field));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn moving_average_update_is_a_technical_state() {
        let message = parse_one(concat!(
            "AAPL.O,Market Price,2022-05-05T11:18:59.095047144Z,-4,Raw,UPDATE,UNSPECIFIED,,,,74,,59088,5\n",
            ",,,,FID,3250,,PMA_50D,165.496,\n",
            ",,,,FID,3251,,PMA_150D,163.3459,\n",
            ",,,,FID,3252,,PMA_200D,159.5387,\n",
            ",,,,FID,3254,,VMA_25D,88153356,\n",
            ",,,,FID,3255,,VMA_50D,92144430,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_TECHNICAL_STATE);
        for field in ["PMA_50D", "PMA_150D", "PMA_200D", "VMA_25D", "VMA_50D"] {
            assert!(layout.slot_fields.iter().any(|(_, name)| name == field));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn price_limit_update_keeps_quote_origin_and_quality_metadata() {
        let message = parse_one(concat!(
            "ABBV.N,Market Price,2021-07-22T14:48:00.016058848Z,-4,Raw,UPDATE,UNSPECIFIED,,,,6562,,51968,9\n",
            ",,,,FID,75,,UPLIMIT,122.47,\n",
            ",,,,FID,998,,GEN_VAL3,122.47,\n",
            ",,,,FID,76,,LOLIMIT,110.81,\n",
            ",,,,FID,999,,GEN_VAL4,110.81,\n",
            ",,,,FID,8406,,QTE_ORIGIN,S,\n",
            ",,,,FID,1041,,GV1_FLAG,S,\n",
            ",,,,FID,118,,PRC_QL_CD,0,\"   \"\n",
            ",,,,FID,3264,,PRC_QL3,332,RPB\n",
            ",,,,FID,9060,,LULD_TM_MS,53279996,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_PRICE_LIMIT_STATE);
        for field in ["QTE_ORIGIN", "GV1_FLAG", "PRC_QL_CD", "PRC_QL3"] {
            assert!(layout.slot_fields.iter().any(|(_, name)| name == field));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn price_limit_update_keeps_security_status() {
        let message = parse_one(concat!(
            "ABBV.N,Market Price,2021-07-22T14:48:00.016058848Z,-4,Raw,UPDATE,UNSPECIFIED,,,,6562,,51984,10\n",
            ",,,,FID,75,,UPLIMIT,122.47,\n",
            ",,,,FID,998,,GEN_VAL3,122.47,\n",
            ",,,,FID,76,,LOLIMIT,110.81,\n",
            ",,,,FID,999,,GEN_VAL4,110.81,\n",
            ",,,,FID,3132,,IRGVAL,4,\n",
            ",,,,FID,8936,,LIMIT_IND2,4,LMT\n",
            ",,,,FID,9060,,LULD_TM_MS,53279996,\n",
            ",,,,FID,13583,,LMT_TYPE,,\n",
            ",,,,FID,11685,,SECUR_ST,F,\n",
            ",,,,FID,3888,,FIN_STATUS,N,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_PRICE_LIMIT_STATE);
        for field in ["SECUR_ST", "FIN_STATUS"] {
            assert!(layout.slot_fields.iter().any(|(_, name)| name == field));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn luld_v2_panel_is_a_price_limit_state() {
        let message = parse_one(concat!(
            "HOOD.O,Market Price,2021-08-04T13:35:09.563944746Z,-4,Raw,UPDATE,UNSPECIFIED,,,,6560,,60608,6\n",
            ",,,,FID,10011,,LMT_REFPR2,65.6,\n",
            ",,,,FID,3816,,UPLIMIT_2,68.88,\n",
            ",,,,FID,3815,,LOLIMIT_2,53.67,\n",
            ",,,,FID,10339,,AUC_EX_NO,0,\n",
            ",,,,FID,9072,,LULD_T2_MS,48909547,\n",
            ",,,,FID,13584,,LMT_TYPE2,,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_PRICE_LIMIT_STATE);
        for (_, field) in LULD_V2_FIELDS {
            assert!(layout.slot_fields.iter().any(|(_, slot)| slot == field));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn closing_auction_imbalance_is_a_price_limit_state() {
        let message = parse_one(concat!(
            "ARKG.BAT,Market Price,2022-02-24T20:00:00.020187841Z,-5,Raw,UPDATE,UNSPECIFIED,,,,5054,,37008,10\n",
            ",,,,FID,7827,,AUC_ONL_PR,30.44,\n",
            ",,,,FID,4344,,SEQNUM_IMB,26539210,\n",
            ",,,,FID,4341,,IMB_TIM_MS,72000001,\n",
            ",,,,FID,4333,,IMB_ACT_TP,4,\"C \"\n",
            ",,,,FID,4336,,IMB_PR_REF,45.36,\n",
            ",,,,FID,777,,BUYMARGIN,606,\n",
            ",,,,FID,779,,SELLMARGIN,1731,\n",
            ",,,,FID,3912,,IND_AUC,45.31,\n",
            ",,,,FID,3870,,CLS_AUC,45.36,\n",
            ",,,,FID,3873,,CLS_AUCVOL,19203,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_PRICE_LIMIT_STATE);
        for (_, field) in AUCTION_IMBALANCE_FIELDS {
            assert!(layout.slot_fields.iter().any(|(_, slot)| slot == field));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn price_limit_state_accepts_co_carried_quote_fields() {
        let message = parse_one(concat!(
            "ALL.N,Market Price,2021-11-04T13:28:33.435537950Z,-4,Raw,UPDATE,UNSPECIFIED,,,,62,,11680,12\n",
            ",,,,FID,31,,ASKSIZE,,\n",
            ",,,,FID,22,,BID,119,\n",
            ",,,,FID,1021,,SEQNUM,24231,\n",
            ",,,,FID,40,,CTS_QUAL,12,IND\n",
            ",,,,FID,25,,ASK,122,\n",
            ",,,,FID,8936,,LIMIT_IND2,0,\"   \"\n",
            ",,,,FID,3132,,IRGVAL,0,\n",
            ",,,,FID,30,,BIDSIZE,,\n",
            ",,,,FID,118,,PRC_QL_CD,65,IND\n",
            ",,,,FID,3264,,PRC_QL3,65,IND\n",
            ",,,,FID,11685,,SECUR_ST,6,\n",
            ",,,,FID,3888,,FIN_STATUS,N,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_PRICE_LIMIT_STATE);
        for field in ["BID", "ASK", "BIDSIZE", "ASKSIZE", "SECUR_ST"] {
            assert!(layout.slot_fields.iter().any(|(_, slot)| slot == field));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn trade_accepts_publisher_order_and_held_trade_metadata() {
        let message = parse_one(concat!(
            "ARKG.BAT,Market Price,2022-02-24T17:20:10.743673286Z,-5,Raw,UPDATE,TRADE,,,,5054,,25582,7\n",
            ",,,,FID,1022,,PRNTYP,\" \",\n",
            ",,,,FID,372,,IRGPRC,43.99,\n",
            ",,,,FID,373,,IRGVOL,100,\n",
            ",,,,FID,13457,,HELD_T_IND,0,\"   \"\n",
            ",,,,FID,3426,,ORDER_ID,4299763959769040207,\n",
            ",,,,FID,3428,,ORDER_SIDE,1,BID\n",
            ",,,,FID,4148,,TIMACT_MS,62410725,\n",
        ));
        assert_eq!(layout_for_message(&message).unwrap().msg_type, MSG_TRADE);
    }

    #[test]
    fn return_update_is_a_technical_state() {
        let message = parse_one(concat!(
            "ABBV.N,Market Price,2021-07-22T14:48:20.887356753Z,-4,Raw,UPDATE,UNSPECIFIED,,,,6562,,55152,13\n",
            ",,,,FID,4981,,TRTN,0.36,\n",
            ",,,,FID,4982,,TRTN_3MT,6.69,\n",
            ",,,,FID,5615,,YR_TRTN,20.17,\n",
            ",,,,FID,5619,,YTD_TRTN,11.38,\n",
            ",,,,FID,9257,,ASY_VA_DT,2021-07-22,\n",
            ",,,,FID,9959,,TRTN_1W,0.2,\n",
            ",,,,FID,9960,,TRTN_1M,2.36,\n",
            ",,,,FID,9961,,TRTN_2Y,77.77,\n",
            ",,,,FID,9962,,TRTN_3Y,40.95,\n",
            ",,,,FID,9963,,TRTN_4Y,72.77,\n",
            ",,,,FID,9964,,TRTN_5Y,106.52,\n",
            ",,,,FID,9965,,MTD_TRTN,2.76,\n",
            ",,,,FID,9966,,QTD_TRTN,2.76,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_TECHNICAL_STATE);
        for (_, name) in RETURN_FIELDS {
            assert!(layout.slot_fields.iter().any(|(_, slot)| slot == name));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn reference_correction_keeps_full_identity_panel() {
        let message = parse_one(concat!(
            "IAK.P,Market Price,2021-07-01T03:53:08.731044891Z,-4,Raw,UPDATE,CORRECTION,,,,64,,50302,24\n",
            ",,,,FID,8493,,XMIC_CODE,XNYS,\n",
            ",,,,FID,105,,BCKGRNDPAG,,\n",
            ",,,,FID,4758,,INSSALCOND,,\n",
            ",,,,FID,15,,CURRENCY,840,USD\n",
            ",,,,FID,869,,OFF_CD_IND,4,CUS\n",
            ",,,,FID,1080,,PREF_DISP,5752,\n",
            ",,,,FID,1,,PROD_PERM,64,\n",
            ",,,,FID,2,,RDNDISPLAY,64,\n",
            ",,,,FID,1709,,RDN_EXCHD2,5,PSE\n",
            ",,,,FID,4,,RDN_EXCHID,5,PSE\n",
            ",,,,FID,259,,RECORDTYPE,112,\n",
            ",,,,FID,3183,,LIST_MKT,P,\n",
            ",,,,FID,5357,,CONTEXT_ID,1070,\n",
            ",,,,FID,3,,DSPLY_NAME,ISH US INSURANCE,\n",
            ",,,,FID,78,,OFFCL_CODE,000464288786,\n",
            ",,,,FID,1055,,OFF_CD_IN2,40,LOC\n",
            ",,,,FID,1056,,OFFC_CODE2,KPTzBMegXCAN,\n",
            ",,,,FID,4738,,DOM_EQ_ID,KPTzBMegXCAN,\n",
            ",,,,FID,3694,,MNEMONIC,IAK,\n",
            ",,,,FID,198,,LOT_SIZE_A,100,\n",
            ",,,,FID,3655,,ISIN_CODE,US4642887867,\n",
            ",,,,FID,4742,,CUSIP_CD,464288786,\n",
            ",,,,FID,104,,BOND_TYPE,252,ETF\n",
            ",,,,FID,728,,BCAST_REF,IAK.P,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_REFERENCE_CORRECTION);
        for (_, field) in REFERENCE_CORRECTION_EXTRA_FIELDS {
            assert!(layout.slot_fields.iter().any(|(_, slot)| slot == field));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn close_bid_ask_update_is_a_trade_statistics_state() {
        let message = parse_one(concat!(
            "ABBV.N,Market Price,2021-07-22T20:01:02.111106785Z,-4,Raw,UPDATE,UNSPECIFIED,,,,6562,,53856,3\n",
            ",,,,FID,61,,CLOSE_ASK,117.54,\n",
            ",,,,FID,60,,CLOSE_BID,117.53,\n",
            ",,,,FID,3580,,BID_ASK_DT,2021-07-22,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_TRADE_STATISTICS_STATE);
        for field in ["CLOSE_BID", "CLOSE_ASK", "BID_ASK_DT"] {
            assert!(layout.slot_fields.iter().any(|(_, slot)| slot == field));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn standalone_dividend_update_is_a_trade_statistics_state() {
        let message = parse_one(concat!(
            "CAKE.O,Market Price,2022-04-02T00:21:15.531666446Z,-4,Raw,UPDATE,UNSPECIFIED,,,,6560,,46272,1\n",
            ",,,,FID,71,,DIVIDEND,0,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_TRADE_STATISTICS_STATE);
        assert!(layout
            .slot_fields
            .iter()
            .any(|(_, slot)| slot == "DIVIDEND"));
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn halt_transition_is_a_trade_statistics_state() {
        let message = parse_one(concat!(
            "CRDO.O,Market Price,2022-01-26T09:00:00.106046083Z,-5,Raw,UPDATE,UNSPECIFIED,,,,6560,,288,14\n",
            ",,,,FID,1021,,SEQNUM,1697,\n",
            ",,,,FID,6615,,HALT_RSN,23,UN\n",
            ",,,,FID,6517,,HALT_REASN,\"    \",\n",
            ",,,,FID,3264,,PRC_QL3,59,\"TH \"\n",
            ",,,,FID,9248,,HLT_RSM_DT,,\n",
            ",,,,FID,6619,,HALT_TIME,09:00:00.000000000,\n",
            ",,,,FID,118,,PRC_QL_CD,59,\"TH \"\n",
            ",,,,FID,14257,,HALT_TM_MS,09:00:00.001000000,\n",
            ",,,,FID,40,,CTS_QUAL,11,\"TH \"\n",
            ",,,,FID,6618,,HALT_DATE,2022-01-26,\n",
            ",,,,FID,131,,PRC_QL2,59,\"TH \"\n",
            ",,,,FID,3984,,TRD_TYPE,H,\n",
            ",,,,FID,14250,,HLT_RSM_MS,,\n",
            ",,,,FID,6614,,TRD_STATUS,2,\"H \"\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_TRADE_STATISTICS_STATE);
        for (_, field) in HALT_STATE_FIELDS {
            assert!(layout.slot_fields.iter().any(|(_, slot)| slot == field));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn session_phase_transition_keeps_halt_and_quality_state() {
        let message = parse_one(concat!(
            "CRDO.O,Market Price,2022-01-27T15:10:00.018583118Z,-5,Raw,UPDATE,UNSPECIFIED,,,,6560,,1392,10\n",
            ",,,,FID,40,,CTS_QUAL,201,QOP\n",
            ",,,,FID,131,,PRC_QL2,239,QOP\n",
            ",,,,FID,8927,,INST_PHASE,16,\"QP \"\n",
            ",,,,FID,3852,,PERIOD_CDE,Q,\n",
            ",,,,FID,8647,,PERIOD_CD2,Q,\n",
            ",,,,FID,118,,PRC_QL_CD,0,\"   \"\n",
            ",,,,FID,3264,,PRC_QL3,0,\"   \"\n",
            ",,,,FID,6517,,HALT_REASN,IPOQ,\n",
            ",,,,FID,6615,,HALT_RSN,59,QO\n",
            ",,,,FID,1021,,SEQNUM,821797,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_SESSION_STATE);
        for field in [
            "CTS_QUAL",
            "PRC_QL2",
            "PERIOD_CDE",
            "PERIOD_CD2",
            "PRC_QL_CD",
            "PRC_QL3",
            "HALT_REASN",
            "HALT_RSN",
            "HLT_RSM_DT",
            "SEQNUM",
        ] {
            assert!(layout.slot_fields.iter().any(|(_, slot)| slot == field));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn closing_run_keeps_full_snapshot_metadata_in_stable_slots() {
        let message = parse_one(concat!(
            "ABBV.N,Market Price,2021-07-23T11:00:01.119245525Z,-4,Raw,UPDATE,CLOSING_RUN,,,,6562,,1,14\n",
            ",,,,FID,22,,BID,117.00,\n",
            ",,,,FID,8935,,RETAIL_INT,0,\n",
            ",,,,FID,1501,,STOCK_TYPE,C,\n",
            ",,,,FID,6513,,SETL_TYPE,R,\n",
            ",,,,FID,6516,,BOOK_STATE,O,\n",
            ",,,,FID,6694,,INSTRD_TIM,11:00:00.000000000,\n",
            ",,,,FID,6810,,INSTRD_DT,2021-07-23,\n",
            ",,,,FID,4761,,CANCEL_IND,0,\n",
            ",,,,FID,8950,,MK_STATUS,O,\n",
            ",,,,FID,4333,,IMB_ACT_TP,A,\n",
            ",,,,FID,4338,,IMB_SH,100,\n",
            ",,,,FID,4340,,IMB_SIDE,B,\n",
            ",,,,FID,4341,,IMB_TIM_MS,39600000,\n",
            ",,,,FID,8414,,INS_SUBIND,123,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_CLOSING_RUN_STATE);
        for (_, field) in CLOSING_RUN_EXTRA_FIELDS {
            assert!(layout.slot_fields.iter().any(|(_, slot)| slot == field));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn metadata_only_closing_run_uses_full_snapshot_slots() {
        let message = parse_one(concat!(
            "ALL.N,Market Price,2021-11-01T11:00:01.644061753Z,-4,Raw,UPDATE,CLOSING_RUN,,,,62,,30542,7\n",
            ",,,,FID,28,,NEWS,\"    \",\n",
            ",,,,FID,29,,NEWS_TIME,,\n",
            ",,,,FID,58,,DJTIME,,\n",
            ",,,,FID,110,,YCHIGH_IND,,\n",
            ",,,,FID,111,,YCLOW_IND,,\n",
            ",,,,FID,3449,,52W_HIND,,\n",
            ",,,,FID,3451,,52W_LIND,,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_CLOSING_RUN_STATE);
        assert!(layout.slot_fields.iter().any(|(_, slot)| slot == "NEWS"));
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn refresh_keeps_contract_reference_metadata_in_stable_slots() {
        let message = parse_one(concat!(
            "ABBV.N,Market Price,2021-07-24T01:42:33.575564202Z,-4,Raw,REFRESH,,,,,6562,79,34351,4\n",
            ",,,,FID,104,,BOND_TYPE,,\n",
            ",,,,FID,1383,,DSO_ID,,\n",
            ",,,,FID,3915,,MKT_STATUS,3,\n",
            ",,,,FID,4238,,RCS_AS_CLA,\" \",\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_REFRESH);
        for (_, field) in REFRESH_EXTRA_FIELDS {
            assert!(layout.slot_fields.iter().any(|(_, slot)| slot == field));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn annual_range_correction_keeps_high_marker() {
        let message = parse_one(concat!(
            "ABBV.N,Market Price,2021-07-26T01:36:29.159850704Z,-4,Raw,UPDATE,CORRECTION,,,,6562,,34576,1\n",
            ",,,,FID,110,,YCHIGH_IND,0,\" \"\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_ANNUAL_RANGE_STATE);
        assert!(layout
            .slot_fields
            .iter()
            .any(|(_, field)| field == "YCHIGH_IND"));
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn fundamental_update_keeps_dividend_metadata() {
        let message = parse_one(concat!(
            "ABBV.N,Market Price,2021-07-26T03:55:09.536237055Z,-4,Raw,UPDATE,UNSPECIFIED,,,,6562,,34672,6\n",
            ",,,,FID,34,,EARNINGS,2.8318,\n",
            ",,,,FID,35,,YIELD,4.3997,\n",
            ",,,,FID,36,,PERATIO,41.7366,\n",
            ",,,,FID,37,,DIVIDENDTP,0,\"  \"\n",
            ",,,,FID,38,,DIVPAYDATE,2021-08-16,\n",
            ",,,,FID,71,,DIVIDEND,5.2,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_FUNDAMENTAL_STATE);
        for (_, field) in FUNDAMENTAL_EXTRA_FIELDS {
            assert!(layout.slot_fields.iter().any(|(_, slot)| slot == field));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn price_limit_correction_keeps_auction_imbalance_metadata() {
        let message = parse_one(concat!(
            "ABBV.N,Market Price,2021-07-26T19:50:00.023585494Z,-4,Raw,UPDATE,CORRECTION,,,,6562,,18944,8\n",
            ",,,,FID,11685,,SECUR_ST,9,\n",
            ",,,,FID,4340,,IMB_SIDE,2,\"B \"\n",
            ",,,,FID,4333,,IMB_ACT_TP,4,\"C \"\n",
            ",,,,FID,4338,,IMB_SH,50446,\n",
            ",,,,FID,4341,,IMB_TIM_MS,71400007,\n",
            ",,,,FID,8936,,LIMIT_IND2,0,\"   \"\n",
            ",,,,FID,3132,,IRGVAL,0,\n",
            ",,,,FID,3888,,FIN_STATUS,N,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_PRICE_LIMIT_STATE);
        for field in ["IMB_ACT_TP", "IMB_SH", "IMB_SIDE", "IMB_TIM_MS"] {
            assert!(layout.slot_fields.iter().any(|(_, slot)| slot == field));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn price_limit_update_is_recognized_by_security_status() {
        let message = parse_one(concat!(
            "ABBV.N,Market Price,2021-08-17T19:50:00.031101635Z,-4,Raw,UPDATE,UNSPECIFIED,,,,6562,,50640,8\n",
            ",,,,FID,11685,,SECUR_ST,9,\n",
            ",,,,FID,8936,,LIMIT_IND2,0,\"   \"\n",
            ",,,,FID,4338,,IMB_SH,180502,\n",
            ",,,,FID,3132,,IRGVAL,0,\n",
            ",,,,FID,3888,,FIN_STATUS,N,\n",
            ",,,,FID,4333,,IMB_ACT_TP,4,\"C \"\n",
            ",,,,FID,4340,,IMB_SIDE,2,\"B \"\n",
            ",,,,FID,4341,,IMB_TIM_MS,71400007,\n",
        ));
        assert_eq!(
            layout_for_message(&message).unwrap().msg_type,
            MSG_PRICE_LIMIT_STATE
        );
    }

    #[test]
    fn price_limit_update_keeps_closing_trade_and_short_sale_status() {
        let message = parse_one(concat!(
            "ABBV.N,Market Price,2021-09-01T15:17:15.304153153Z,-4,Raw,UPDATE,UNSPECIFIED,,,,6562,,55136,8\n",
            ",,,,FID,11685,,SECUR_ST,E,\n",
            ",,,,FID,8936,,LIMIT_IND2,0,\"   \"\n",
            ",,,,FID,3854,,SALTIM_MS,55035288,\n",
            ",,,,FID,3132,,IRGVAL,0,\n",
            ",,,,FID,3888,,FIN_STATUS,N,\n",
            ",,,,FID,379,,SALTIM,15:17:15.000000000,\n",
            ",,,,FID,6577,,SH_SAL_RES,4,Y\n",
            ",,,,FID,1044,,GV4_FLAG,E,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_PRICE_LIMIT_STATE);
        for field in ["SALTIM", "SALTIM_MS", "GV4_FLAG", "SH_SAL_RES"] {
            assert!(layout.slot_fields.iter().any(|(_, slot)| slot == field));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn moving_average_correction_keeps_ex_dividend_date() {
        let message = parse_one(concat!(
            "ABBV.N,Market Price,2021-08-16T10:26:47.955767580Z,-4,Raw,UPDATE,CORRECTION,,,,6562,,24336,2\n",
            ",,,,FID,3253,,VMA_10D,1335798,\n",
            ",,,,FID,39,,EXDIVDATE,2021-07-14,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_MOVING_AVERAGE_CORRECTION);
        assert!(layout
            .slot_fields
            .iter()
            .any(|(_, field)| field == "EXDIVDATE"));
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn closing_quote_correction_keeps_ex_dividend_date() {
        let message = parse_one(concat!(
            "ABBV.N,Market Price,2021-09-08T01:20:11.755476007Z,-4,Raw,UPDATE,CORRECTION,,,,6562,,2304,4\n",
            ",,,,FID,39,,EXDIVDATE,2021-07-14,\n",
            ",,,,FID,61,,CLOSE_ASK,109.07,\n",
            ",,,,FID,60,,CLOSE_BID,109.03,\n",
            ",,,,FID,3580,,BID_ASK_DT,2021-09-07,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_CLOSING_QUOTE_CORRECTION);
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn closing_quote_correction_keeps_price_quality_fields() {
        let message = parse_one(concat!(
            "CRDO.O,Market Price,2022-01-26T21:02:00.046539820Z,-5,Raw,UPDATE,CORRECTION,,,,6560,,432,6\n",
            ",,,,FID,60,,CLOSE_BID,0,\n",
            ",,,,FID,3580,,BID_ASK_DT,2022-01-26,\n",
            ",,,,FID,61,,CLOSE_ASK,0,\n",
            ",,,,FID,118,,PRC_QL_CD,94,CLS\n",
            ",,,,FID,3264,,PRC_QL3,94,CLS\n",
            ",,,,FID,40,,CTS_QUAL,11,\"TH \"\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_CLOSING_QUOTE_CORRECTION);
        for field in ["CTS_QUAL", "PRC_QL_CD", "PRC_QL3"] {
            assert!(layout.slot_fields.iter().any(|(_, slot)| slot == field));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn official_close_correction_keeps_open_price() {
        let message = parse_one(concat!(
            "ABBV.N,Market Price,2021-07-22T20:15:00.107148645Z,-4,Raw,UPDATE,CORRECTION,,,,6562,,53936,13\n",
            ",,,,FID,3372,,OFF_CLOSE,117.54,\n",
            ",,,,FID,1392,,CLOSE_TIME,20:15:00.000000000,\n",
            ",,,,FID,6762,,OFF_CLS_DT,2021-07-22,\n",
            ",,,,FID,6377,,OFF_CL_TIM,20:15:00.000000000,\n",
            ",,,,FID,14258,,OFF_CLS_MS,20:15:00.090000000,\n",
            ",,,,FID,32,,ACVOL_1,700660,\n",
            ",,,,FID,6588,,REPORT_VOL,700660,\n",
            ",,,,FID,32743,,ACVOL_UNS,700660,\n",
            ",,,,FID,19,,OPEN_PRC,116.64,\n",
            ",,,,FID,1021,,SEQNUM,1879762,\n",
            ",,,,FID,60,,CLOSE_BID,117.53,\n",
            ",,,,FID,61,,CLOSE_ASK,117.54,\n",
            ",,,,FID,3580,,BID_ASK_DT,2021-07-22,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_OFFICIAL_CLOSE_STATE);
        assert!(layout
            .slot_fields
            .iter()
            .any(|(_, slot)| slot == "OPEN_PRC"));
        for field in ["CLOSE_BID", "CLOSE_ASK", "BID_ASK_DT"] {
            assert!(layout.slot_fields.iter().any(|(_, slot)| slot == field));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn short_sale_correction_is_a_short_sale_state() {
        let message = parse_one(concat!(
            "ABBV.N,Market Price,2021-07-23T07:45:00.107191596Z,-4,Raw,UPDATE,CORRECTION,,,,6562,,54272,8\n",
            ",,,,FID,1044,,GV4_FLAG,\" \",\n",
            ",,,,FID,6577,,SH_SAL_RES,1,N\n",
            ",,,,FID,3888,,FIN_STATUS,N,\n",
            ",,,,FID,3422,,PROV_SYMB,ABBV,\n",
            ",,,,FID,6614,,TRD_STATUS,0,\"  \"\n",
            ",,,,FID,3984,,TRD_TYPE,S,\n",
            ",,,,FID,6618,,HALT_DATE,2021-07-23,\n",
            ",,,,FID,6619,,HALT_TIME,07:45:00.000000000,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_SHORT_SALE_STATE);
        for field in ["TRD_STATUS", "TRD_TYPE", "HALT_DATE", "HALT_TIME"] {
            assert!(layout.slot_fields.iter().any(|(_, slot)| slot == field));
        }
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn provisional_symbol_update_is_a_reference_correction() {
        let message = parse_one(concat!(
            "ABBV.N,Market Price,2021-08-16T07:45:00.105532704Z,-4,Raw,UPDATE,UNSPECIFIED,,,,6562,,24272,1\n",
            ",,,,FID,3422,,PROV_SYMB,ABBV,\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_REFERENCE_CORRECTION);
        let encoded = encode_exact_slots(&message, 1, 2, layout).unwrap();
        validate_exact_slots(&encoded, layout).unwrap();
    }

    #[test]
    fn standalone_generic_flag_correction_uses_trade_restatement_slots() {
        let message = parse_one(concat!(
            "AAPL.O,Market Price,2022-06-29T20:30:36.055618982Z,-4,Raw,UPDATE,CORRECTION,,,,74,,26256,1\n",
            ",,,,FID,1044,,GV4_FLAG,\" \",\n",
        ));
        let layout = layout_for_message(&message).unwrap();
        assert_eq!(layout.msg_type, MSG_TRADE_RESTATEMENT);
        assert!(layout
            .slot_fields
            .iter()
            .any(|(_, field)| field == "GV4_FLAG"));
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
        assert_eq!(layout.value_len, 464);
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
