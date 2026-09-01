//! Slim CME TAS codec: classify a source row and pack printable trades.

pub mod backtest_1s;
pub mod baseline_1min;
pub mod ll2_1min;
pub mod ll2_shard;
pub mod ll2_source;
pub mod product;
pub mod shard;
pub mod ylabel_1min;

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Datelike, NaiveTime, Timelike};
use chrono_tz::America::Chicago;
use polars::prelude::{DataFrame, NamedFrom, ParquetWriter, Series};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;

pub const CME_TRADE_LEN: usize = 80;
pub const CME_QUOTE_LEN: usize = 64;
pub const SYMBOLOGY_CHANGE_LEN: usize = 64;
pub const CME_PRICE_LIMIT_LEN: usize = 48;
pub const CME_SETTLEMENT_LEN: usize = 40;
pub const CME_IMPLIED_VOL_LEN: usize = 64;
pub const CME_TOT_VOLUME_LEN: usize = 48;
pub const CME_PRICE_PRINT_LEN: usize = 48;
pub const CME_IMBALANCE_LEN: usize = 48;
pub const CME_AUCTION_LEN: usize = 48;
pub const CME_CORRECTION_LEN: usize = 224;
/// Current v1 status values use 184 bytes for Qualifiers. Existing v1 values
/// remain 192 bytes long and are decoded through their value length.
pub const CME_STATUS_LEN: usize = 224;
pub const CME_STATUS_LEGACY_LEN: usize = 192;
pub const REFERENCE_CHANGE_LEN: usize = 96;
pub const CORRECTION_QUALIFIER_LEN: usize = 152;
pub const STATUS_QUALIFIER_LEN: usize = 184;
pub const STATUS_QUALIFIER_LEGACY_LEN: usize = 152;
pub const REFERENCE_VALUE_LEN: usize = 32;
pub const RIC_LEN: usize = 16;
pub const KEY_TS_LEN: usize = 8;
pub const KEY_PART_LEN: usize = 2;
pub const KEY_SEQ_LEN: usize = 4;
pub const KEY_LEN: usize = RIC_LEN + KEY_TS_LEN + KEY_PART_LEN + KEY_SEQ_LEN;
pub const MAGIC: [u8; 2] = *b"CT";
pub const VERSION: u8 = 1;
pub const KIND_CME_TRADE: u8 = 1;
pub const KIND_CME_SPECIAL: u8 = 2;
pub const KIND_CME_QUOTE: u8 = 3;
pub const KIND_SYMBOLOGY_CHANGE: u8 = 4;
pub const KIND_CME_PRICE_LIMIT: u8 = 5;
pub const KIND_CME_SETTLEMENT: u8 = 6;
pub const KIND_CME_IMPLIED_VOL: u8 = 7;
pub const KIND_CME_TOT_VOLUME: u8 = 8;
pub const KIND_CME_PRICE_PRINT: u8 = 9;
pub const KIND_CME_IMBALANCE: u8 = 10;
pub const KIND_CME_AUCTION: u8 = 11;
pub const KIND_CME_CORRECTION: u8 = 12;
pub const KIND_CME_STATUS: u8 = 13;
pub const KIND_REFERENCE_CHANGE: u8 = 14;
pub const IV_SOURCE_LAST: u8 = 1;
pub const IV_SOURCE_QUOTE: u8 = 2;
pub const IV_SOURCE_SETTLE: u8 = 3;
pub const CHANGE_TYPE_RIC: u8 = 1;
pub const CHANGE_TYPE_DESCRIPTION: u8 = 2;
pub const CHANGE_TYPE_EXPIRY_DATE: u8 = 3;
pub const CHANGE_TYPE_CURRENCY: u8 = 4;
pub const CHANGE_TYPE_PERMISSION_CODE: u8 = 5;
pub const CHANGE_TYPE_OPTION_TYPE: u8 = 6;
pub const CHANGE_TYPE_TEMPLATE: u8 = 7;
pub const CHANGE_TYPE_EXCHANGE: u8 = 8;
pub const CHANGE_TYPE_BOND_TYPE: u8 = 9;
pub const CHANGE_TYPE_RECORD_TYPE: u8 = 10;
pub const CHANGE_TYPE_RATING: u8 = 11;
pub const CHANGE_TYPE_RATING_ID: u8 = 12;
/// `cme_correction` v2 assigns its former four-byte pad to `Acc. Volume`.
/// Other record kinds still use the common v1 layout.
pub const CME_CORRECTION_VERSION: u8 = 2;
pub const MISSING_SEQ: u64 = u64::MAX;
pub const EXPECTED_COLUMN_COUNT: usize = 294;
pub const PRICE_SCALE: i128 = 1_000_000_000;
pub const MISSING_EXCH_HMS_NS: u64 = u64::MAX;
pub const MISSING_PRICE: i64 = i64::MIN;
pub const MISSING_VOLUME: u32 = u32::MAX;
pub const SPECIAL_TRADES_USER: &str = "Special Trades[USER]";
pub const SETTLE_IV_USER: &str = "Settle IV[USER]";
pub const CF_CME_TRADE: &str = "cme_trade";
pub const CF_CME_SPECIAL: &str = "cme_special";
pub const CF_CME_QUOTE: &str = "cme_quote";
pub const CF_SYMBOLOGY_CHANGE: &str = "symbology_change";
pub const CF_CME_PRICE_LIMIT: &str = "cme_price_limit";
pub const CF_CME_SETTLEMENT: &str = "cme_settlement";
pub const CF_REPLAY_META: &str = "replay_meta";
pub const CF_SETTLEMENT_SCAN_META: &str = "settlement_scan_meta";
pub const PERIOD_META_PREFIX: &str = "period:";
pub const PERIOD_STATUS_WRITING: &str = "writing";
pub const PERIOD_STATUS_DONE: &str = "done";
pub const PRICE_LIMIT_COLUMNS: &[&str] = &["UpLim Price", "LoLim Price"];
pub const SETTLE_IV_COLUMNS: &[&str] = &["Imp. Vol."];
pub const IMPLIED_VOL_COLUMNS: &[&str] = &["Imp. Vol.", "Bid Imp. Vol", "Ask Imp. Vol"];
pub const TOT_VOLUME_COLUMNS: &[&str] = &["Total Volume"];
pub const IMPLIED_YIELD_COLUMNS: &[&str] = &["Implied Yield"];
pub const IMBALANCE_COLUMNS: &[&str] = &["Imbalance Quantity", "Imbalance Side"];
pub const IMBALANCE_SIDE_BID: u8 = 1;
pub const IMBALANCE_SIDE_ASK: u8 = 2;
pub const MISSING_VOLUME_U64: u64 = u64::MAX;
pub const AGGRESSOR_IMPLIED: u8 = 0;
pub const AGGRESSOR_BUY: u8 = 1;
pub const AGGRESSOR_SELL: u8 = 2;

/// Watermark for one TAS period directory inside the single live RocksDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeriodStatus {
    Writing,
    Done,
}

/// Config `period` is the TAS directory suffix, e.g. `2026-01-01_2026-06-01`.
pub fn validate_period(period: &str) -> Result<()> {
    if period.is_empty() {
        bail!("unhandled empty TAS period");
    }
    if !period.is_ascii() {
        bail!("TAS period {period:?} is not ASCII");
    }
    if period.len() > 64 {
        bail!("TAS period {period:?} longer than 64 bytes");
    }
    if !period
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        bail!("unhandled TAS period {period:?}");
    }
    Ok(())
}

pub fn period_meta_key(period: &str) -> Result<Vec<u8>> {
    validate_period(period)?;
    Ok(format!("{PERIOD_META_PREFIX}{period}").into_bytes())
}

pub fn encode_period_status(status: PeriodStatus) -> Vec<u8> {
    match status {
        PeriodStatus::Writing => PERIOD_STATUS_WRITING.as_bytes().to_vec(),
        PeriodStatus::Done => PERIOD_STATUS_DONE.as_bytes().to_vec(),
    }
}

pub fn decode_period_status(bytes: &[u8]) -> Result<PeriodStatus> {
    match bytes {
        b"writing" => Ok(PeriodStatus::Writing),
        b"done" => Ok(PeriodStatus::Done),
        other => bail!(
            "unhandled period status {:?}",
            String::from_utf8_lossy(other)
        ),
    }
}

/// Groups that must stay empty on non-index futures rows.
pub const FORBIDDEN_FUTURES_GROUPS: &[&str] = &[
    "equity_value",
    "yield",
    "theoretical",
    "iv",
    "energy_analytic",
    "macro",
    "cds",
    "auction",
    "book_accum",
    "dealer",
    "index_breadth",
];

/// CME Group month codes in `<root><month><1-or-2-digit-year>`.
pub const MONTH_CODES: &[u8] = b"FGHJKMNQUVXZ";

/// Live `#RIC` stem, dropping a Reuters historical suffix (`ADF26^2` → `ADF26`).
pub fn ric_live_stem(ric: &str) -> &str {
    ric.split_once('^').map(|(stem, _)| stem).unwrap_or(ric)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    CmeTrade,
    CmeSpecial,
    CmeQuote,
    CmeStatus,
    CmeCorrection,
    CmeAuction,
    CmeSettlement,
    ReferenceChange,
    SymbologyChange,
    IndexPrint,
    CmePriceLimit,
    CmeTotVolume,
    CmePricePrint,
    CmeImbalance,
    DropEmptyTrade,
    DropEmptyQuote,
    DropVolumeOnlyTrade,
    SpecialMissingVolume,
    DropSettleIv,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CmeTrade => "cme_trade",
            Self::CmeSpecial => "cme_special",
            Self::CmeQuote => "cme_quote",
            Self::CmeStatus => "cme_status",
            Self::CmeCorrection => "cme_correction",
            Self::CmeAuction => "cme_auction",
            Self::CmeSettlement => "cme_settlement",
            Self::ReferenceChange => "reference_change",
            Self::SymbologyChange => "symbology_change",
            Self::CmePriceLimit => "cme_price_limit",
            Self::IndexPrint => "index_print",
            Self::CmeTotVolume => "cme_tot_volume",
            Self::CmePricePrint => "cme_price_print",
            Self::CmeImbalance => "cme_imbalance",
            Self::DropEmptyTrade => "drop_empty_trade",
            Self::DropEmptyQuote => "drop_empty_quote",
            Self::DropVolumeOnlyTrade => "drop_volume_only_trade",
            Self::SpecialMissingVolume => "special_missing_volume",
            Self::DropSettleIv => "drop_settle_iv",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColumnRules {
    pub types: Vec<String>,
    pub required_identity: Vec<String>,
    pub columns: BTreeMap<String, String>,
}

impl ColumnRules {
    pub fn load(path: &Path) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context_path(path, "read TAS column rules")?;
        let rules: Self =
            serde_json::from_str(&text).with_context_path(path, "parse TAS column rules")?;
        if rules.columns.len() != EXPECTED_COLUMN_COUNT {
            bail!(
                "TAS column rules must catalogue {EXPECTED_COLUMN_COUNT} names, got {} from {}",
                rules.columns.len(),
                path.display()
            );
        }
        if rules.required_identity != ["#RIC", "Date-Time", "Type"] {
            bail!(
                "TAS required_identity must be [#RIC, Date-Time, Type], got {:?}",
                rules.required_identity
            );
        }
        let known: BTreeSet<&str> = [
            "Trade",
            "Quote",
            "Auction",
            "Correction",
            "Mkt. Condition",
            "Settlement Price",
            "Symbology Change",
            "Reference Change",
        ]
        .into_iter()
        .collect();
        let listed: BTreeSet<&str> = rules.types.iter().map(String::as_str).collect();
        if listed != known {
            bail!("TAS column rules types must be {known:?}, got {listed:?}");
        }
        Ok(rules)
    }

    pub fn group_of(&self, name: &str) -> Result<&str> {
        self.columns.get(name).map(String::as_str).ok_or_else(|| {
            anyhow!("unhandled TAS column {name:?}; add it to tas_column_rules.json")
        })
    }

    pub fn is_forbidden_futures_group(&self, group: &str) -> bool {
        FORBIDDEN_FUTURES_GROUPS.contains(&group)
    }

    pub fn is_allowed_price_limit_column(&self, name: &str) -> bool {
        PRICE_LIMIT_COLUMNS.contains(&name)
    }

    pub fn is_allowed_settle_iv_column(&self, name: &str) -> bool {
        SETTLE_IV_COLUMNS.contains(&name)
    }

    pub fn is_allowed_implied_vol_column(&self, name: &str) -> bool {
        IMPLIED_VOL_COLUMNS.contains(&name)
    }

    pub fn is_allowed_tot_volume_column(&self, name: &str) -> bool {
        TOT_VOLUME_COLUMNS.contains(&name)
    }

    pub fn is_allowed_implied_yield_column(&self, name: &str) -> bool {
        IMPLIED_YIELD_COLUMNS.contains(&name)
    }

    pub fn is_allowed_imbalance_column(&self, name: &str) -> bool {
        IMBALANCE_COLUMNS.contains(&name)
    }
}

trait WithPathContext<T> {
    fn with_context_path(self, path: &Path, what: &str) -> Result<T>;
}

impl<T, E> WithPathContext<T> for std::result::Result<T, E>
where
    E: Into<anyhow::Error>,
{
    fn with_context_path(self, path: &Path, what: &str) -> Result<T> {
        self.map_err(|err| anyhow!("{what} {}: {}", path.display(), err.into()))
    }
}

#[derive(Debug, Clone)]
pub struct SlimTrade {
    pub ric: String,
    pub ts_utc_ns: u64,
    pub exch_hms_ns: u64,
    pub price: i64,
    pub volume: u32,
    pub bid: i64,
    pub bid_size: u32,
    pub ask: i64,
    pub ask_size: u32,
    pub aggressor: u8,
}

#[derive(Debug, Clone)]
pub struct SlimQuote {
    pub ric: String,
    pub ts_utc_ns: u64,
    pub exch_hms_ns: u64,
    pub bid: i64,
    pub bid_size: u32,
    pub ask: i64,
    pub ask_size: u32,
}

#[derive(Debug, Clone)]
pub struct SlimSymbologyChange {
    pub ric: String,
    pub ts_utc_ns: u64,
    pub change_type: u8,
    pub old_value: String,
    pub new_value: String,
}

#[derive(Debug, Clone)]
pub struct SlimPriceLimit {
    pub ric: String,
    pub ts_utc_ns: u64,
    pub up_lim: i64,
    pub lo_lim: i64,
}

/// One source `Type=Settlement Price` message. The source timestamp is kept
/// because a RIC can publish more than one settlement update in a session.
#[derive(Debug, Clone)]
pub struct SlimSettlement {
    pub ric: String,
    pub ts_utc_ns: u64,
    pub price: i64,
    /// Source `Date` as YYYYMMDD, or zero when the source cell is empty.
    /// Price may be `MISSING_PRICE` when this date is present.
    pub source_date_yyyymmdd: u32,
}

/// Vendor implied volatility on a Trade or Quote. Not a trade, not BBO.
///
/// `source` is last (`Imp. Vol.`), quote (`Bid Imp. Vol` / `Ask Imp. Vol`),
/// or settle (`Settle IV[USER]`). Missing sides use `MISSING_PRICE`.
#[derive(Debug, Clone)]
pub struct SlimImpliedVol {
    pub ric: String,
    pub ts_utc_ns: u64,
    pub exch_hms_ns: u64,
    pub last_iv: i64,
    pub bid_iv: i64,
    pub ask_iv: i64,
    pub source: u8,
}

/// Product-level cumulative volume on a TOT RIC (`LCOTOT`). Not a dated contract.
#[derive(Debug, Clone)]
pub struct SlimTotVolume {
    pub ric: String,
    pub ts_utc_ns: u64,
    pub exch_hms_ns: u64,
    pub volume: u64,
}

/// Price indication / last print: `Type=Trade` with Price and no Volume.
/// Not a zero-lot `cme_trade`.
#[derive(Debug, Clone)]
pub struct SlimPricePrint {
    pub ric: String,
    pub ts_utc_ns: u64,
    pub exch_hms_ns: u64,
    pub price: i64,
}

/// Exchange-published indicative surplus. Not a BBO and not Type=Auction.
/// Seen on ETH Quote rows with no bid/ask: `Imbalance Quantity` + `Imbalance Side`.
#[derive(Debug, Clone)]
pub struct SlimImbalance {
    pub ric: String,
    pub ts_utc_ns: u64,
    pub exch_hms_ns: u64,
    pub quantity: u32,
    pub side: u8,
}

/// Official `Type=Auction` message. A missing volume is an auction price
/// indication, while a present volume is an auction match.
#[derive(Debug, Clone)]
pub struct SlimAuction {
    pub ric: String,
    pub ts_utc_ns: u64,
    pub exch_hms_ns: u64,
    pub price: i64,
    pub volume: u32,
}

/// Official `Type=Correction`. Cancel / correct of a prior print, not a new trade.
#[derive(Debug, Clone)]
pub struct SlimCorrection {
    pub ric: String,
    pub ts_utc_ns: u64,
    pub exch_hms_ns: u64,
    pub price: i64,
    pub volume: u32,
    /// Source `Acc. Volume` for a Correction event, not a newly traded volume.
    pub acc_volume: u32,
    pub original_price: i64,
    pub original_volume: u32,
    pub original_seq: u64,
    pub qualifiers: String,
}

/// Official `Type=Mkt. Condition`. Qualifiers carry the status FIDs.
#[derive(Debug, Clone)]
pub struct SlimStatus {
    pub ric: String,
    pub ts_utc_ns: u64,
    pub exch_hms_ns: u64,
    pub qualifiers: String,
}

/// Official `Type=Reference Change`. Contract description, not a RIC rename.
#[derive(Debug, Clone)]
pub struct SlimReferenceChange {
    pub ric: String,
    pub ts_utc_ns: u64,
    pub change_type: u8,
    pub old_value: String,
    pub new_value: String,
}

pub fn encode_ric(ric: &str) -> Result<[u8; RIC_LEN]> {
    if !ric.is_ascii() {
        bail!("RIC {ric:?} is not ASCII");
    }
    if ric.is_empty() {
        bail!("unhandled empty required TAS field \"#RIC\"");
    }
    if ric.len() > RIC_LEN {
        bail!("RIC {ric:?} longer than {RIC_LEN} bytes; refuse to truncate");
    }
    let mut out = [0u8; RIC_LEN];
    out[..ric.len()].copy_from_slice(ric.as_bytes());
    Ok(out)
}

pub fn decode_ric(bytes: &[u8]) -> Result<String> {
    if bytes.len() != RIC_LEN {
        bail!("ric slot must be {RIC_LEN} bytes, got {}", bytes.len());
    }
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(RIC_LEN);
    let raw = &bytes[..end];
    if raw.is_empty() {
        bail!("ric slot is empty");
    }
    if !raw.iter().all(|b| b.is_ascii() && *b != 0) {
        bail!("ric slot is not ASCII");
    }
    if bytes[end..].iter().any(|&b| b != 0) {
        bail!("ric slot has bytes after NUL");
    }
    Ok(std::str::from_utf8(raw)
        .map_err(|_| anyhow!("ric slot is not UTF-8"))?
        .to_string())
}

pub fn parse_date_time_ns(raw: &str) -> Result<u64> {
    if raw.is_empty() {
        bail!("unhandled empty required TAS field \"Date-Time\"");
    }
    let dt = DateTime::parse_from_rfc3339(raw)
        .map_err(|err| anyhow!("unhandled Date-Time {raw:?}: {err}"))?;
    dt.timestamp_nanos_opt()
        .map(|ns| ns as u64)
        .ok_or_else(|| anyhow!("Date-Time {raw:?} is out of nanosecond range"))
}

pub fn parse_exch_hms_ns(raw: &str) -> Result<u64> {
    if raw.is_empty() {
        return Ok(MISSING_EXCH_HMS_NS);
    }
    let (hms, frac) = match raw.split_once('.') {
        Some((hms, frac)) => (hms, frac),
        None => (raw, ""),
    };
    if frac.len() > 9 {
        bail!("Exch Time {raw:?} has more than 9 fractional digits");
    }
    if !frac.chars().all(|c| c.is_ascii_digit()) {
        bail!("Exch Time {raw:?} has a non-digit fraction");
    }
    let time = NaiveTime::parse_from_str(hms, "%H:%M:%S")
        .map_err(|err| anyhow!("unhandled Exch Time {raw:?}: {err}"))?;
    let mut frac_pad = frac.to_string();
    while frac_pad.len() < 9 {
        frac_pad.push('0');
    }
    let frac_ns: u64 = if frac_pad.is_empty() {
        0
    } else {
        frac_pad
            .parse()
            .map_err(|err| anyhow!("unhandled Exch Time {raw:?}: {err}"))?
    };
    let secs = u64::from(time.num_seconds_from_midnight());
    Ok(secs
        .checked_mul(1_000_000_000)
        .and_then(|v| v.checked_add(frac_ns))
        .ok_or_else(|| anyhow!("Exch Time {raw:?} overflowed"))?)
}

pub fn parse_price_e9(raw: &str) -> Result<i64> {
    if raw.is_empty() {
        return Ok(MISSING_PRICE);
    }
    let (neg, digits) = match raw.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, raw),
    };
    let (int, frac) = match digits.split_once('.') {
        Some((int, frac)) => (int, frac),
        None => (digits, ""),
    };
    if int.is_empty() || !int.chars().all(|c| c.is_ascii_digit()) {
        bail!("unhandled price {raw:?}");
    }
    if !frac.chars().all(|c| c.is_ascii_digit()) {
        bail!("unhandled price {raw:?}");
    }
    if frac.len() > 9 {
        bail!("price {raw:?} has more than 9 decimal digits");
    }
    let int_val: i128 = int
        .parse()
        .map_err(|err| anyhow!("unhandled price {raw:?}: {err}"))?;
    let mut frac_pad = frac.to_string();
    while frac_pad.len() < 9 {
        frac_pad.push('0');
    }
    let frac_val: i128 = if frac_pad.is_empty() {
        0
    } else {
        frac_pad
            .parse()
            .map_err(|err| anyhow!("unhandled price {raw:?}: {err}"))?
    };
    let mut scaled = int_val
        .checked_mul(PRICE_SCALE)
        .and_then(|v| v.checked_add(frac_val))
        .ok_or_else(|| anyhow!("price {raw:?} overflowed i64 after * 10^9"))?;
    if neg {
        scaled = -scaled;
    }
    i64::try_from(scaled).map_err(|_| anyhow!("price {raw:?} overflowed i64 after * 10^9"))
}

/// `UpLim Price` / `LoLim Price` only.
///
/// Most cages are ordinary decimals (`7379.25`, `0.25`) and use [`parse_price_e9`].
/// JY / MP sometimes emit a no-decimal integer that is already price × 10^6
/// (`11317500000` = 11317.5). Multiplying that by 10^9 again overflows i64.
/// Those cells are converted to the same e9 scale by multiplying the remaining 10^3.
pub fn parse_limit_price_e9(raw: &str) -> Result<i64> {
    match parse_price_e9(raw) {
        Ok(value) => Ok(value),
        Err(_) if is_pre_scaled_limit_integer(raw) => parse_pre_scaled_limit_e9(raw),
        Err(err) => Err(err),
    }
}

fn is_pre_scaled_limit_integer(raw: &str) -> bool {
    let digits = raw.strip_prefix('-').unwrap_or(raw);
    digits.len() >= 10 && digits.chars().all(|c| c.is_ascii_digit())
}

fn parse_pre_scaled_limit_e9(raw: &str) -> Result<i64> {
    let (neg, digits) = match raw.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, raw),
    };
    let int_val: i128 = digits
        .parse()
        .map_err(|err| anyhow!("unhandled price {raw:?}: {err}"))?;
    let mut scaled = int_val
        .checked_mul(1_000)
        .ok_or_else(|| anyhow!("price {raw:?} overflowed i64 after * 10^3"))?;
    if neg {
        scaled = -scaled;
    }
    i64::try_from(scaled).map_err(|_| anyhow!("price {raw:?} overflowed i64 after * 10^3"))
}

pub fn parse_volume(raw: &str) -> Result<u32> {
    if raw.is_empty() {
        return Ok(MISSING_VOLUME);
    }
    if !raw.chars().all(|c| c.is_ascii_digit()) {
        bail!("unhandled volume {raw:?}");
    }
    raw.parse::<u32>()
        .map_err(|err| anyhow!("unhandled volume {raw:?}: {err}"))
}

pub fn parse_volume_u64(raw: &str) -> Result<u64> {
    if raw.is_empty() {
        return Ok(MISSING_VOLUME_U64);
    }
    if !raw.chars().all(|c| c.is_ascii_digit()) {
        bail!("unhandled volume {raw:?}");
    }
    raw.parse::<u64>()
        .map_err(|err| anyhow!("unhandled volume {raw:?}: {err}"))
}

/// Product-level TOT RIC (`LCOTOT`), not a dated future.
pub fn is_tot_ric(ric: &str) -> bool {
    let stem = ric_live_stem(ric);
    let Some(root) = stem.strip_suffix("TOT") else {
        return false;
    };
    !root.is_empty() && root.bytes().all(|b| b.is_ascii_alphabetic())
}

pub fn parse_aggressor(qualifiers: &str) -> Result<u8> {
    if qualifiers.is_empty() || qualifiers == SPECIAL_TRADES_USER {
        return Ok(0);
    }
    let mut found: Option<String> = None;
    for part in qualifiers.split(';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let Some(open) = part.rfind('[') else {
            bail!("unhandled Qualifiers fragment {part:?} in {qualifiers:?}");
        };
        if !part.ends_with(']') {
            bail!("unhandled Qualifiers fragment {part:?} in {qualifiers:?}");
        }
        let fid = &part[open + 1..part.len() - 1];
        let mut value = part[..open].trim();
        if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
            value = &value[1..value.len() - 1];
        }
        let value = value.trim();
        if fid == "AGGRS_SID1" {
            found = Some(value.to_string());
        }
    }
    match found.as_deref() {
        None | Some("") => Ok(AGGRESSOR_IMPLIED),
        Some("BID") => Ok(AGGRESSOR_BUY),
        Some("ASK") => Ok(AGGRESSOR_SELL),
        Some(other) => bail!("unhandled AGGRS_SID1 {other:?}"),
    }
}

pub fn classify(
    ric: &str,
    event_type: &str,
    price: &str,
    volume: &str,
    qualifiers: &str,
) -> Result<EventKind> {
    match event_type {
        "" => bail!("unhandled TAS Type '' (empty)"),
        "Settlement Price" => Ok(EventKind::CmeSettlement),
        "Symbology Change" => Ok(EventKind::SymbologyChange),
        "Trade" => {
            if ric.starts_with('.') {
                if price.is_empty() && volume.is_empty() {
                    return Ok(EventKind::DropEmptyTrade);
                }
                return Ok(EventKind::IndexPrint);
            }
            if qualifiers == SPECIAL_TRADES_USER {
                if volume.is_empty() {
                    return Ok(EventKind::SpecialMissingVolume);
                }
                return Ok(EventKind::CmeSpecial);
            }
            if qualifiers == SETTLE_IV_USER {
                if !price.is_empty() || !volume.is_empty() {
                    bail!("settle IV {ric} unexpectedly has Price or Volume");
                }
                return Ok(EventKind::DropSettleIv);
            }
            match (!price.is_empty(), !volume.is_empty()) {
                (true, true) => Ok(EventKind::CmeTrade),
                (false, false) => Ok(EventKind::DropEmptyTrade),
                (true, false) => Ok(EventKind::CmePricePrint),
                (false, true) => Ok(EventKind::DropVolumeOnlyTrade),
            }
        }
        "Quote" => Ok(EventKind::CmeQuote),
        "Mkt. Condition" => Ok(EventKind::CmeStatus),
        "Correction" => Ok(EventKind::CmeCorrection),
        "Auction" => Ok(EventKind::CmeAuction),
        "Reference Change" => Ok(EventKind::ReferenceChange),
        other => bail!("unhandled TAS Type {other:?}"),
    }
}

/// Promote an empty Trade that carries FID 75/76 into a price-limit kind.
/// One published side is still a cage update; the missing side is the price sentinel.
pub fn overlay_price_limit(kind: EventKind, up: &str, lo: &str) -> Result<EventKind> {
    if kind != EventKind::DropEmptyTrade {
        return Ok(kind);
    }
    if up.is_empty() && lo.is_empty() {
        return Ok(EventKind::DropEmptyTrade);
    }
    Ok(EventKind::CmePriceLimit)
}

/// Promote a TOT RIC volume update (`LCOTOT`) off empty / volume-only Trade.
/// TOT is product-level cumulative volume, not a dated contract and not a print.
pub fn overlay_tot_volume(
    kind: EventKind,
    ric: &str,
    volume: &str,
    total_volume: &str,
) -> Result<EventKind> {
    if !is_tot_ric(ric) {
        return Ok(kind);
    }
    if volume.is_empty() && total_volume.is_empty() {
        return Ok(kind);
    }
    if kind == EventKind::CmeTrade || kind == EventKind::CmePricePrint {
        bail!("TOT RIC {ric} unexpectedly has Price");
    }
    if kind == EventKind::DropEmptyTrade || kind == EventKind::DropVolumeOnlyTrade {
        return Ok(EventKind::CmeTotVolume);
    }
    Ok(kind)
}

pub fn tot_volume_from_cells(volume: &str, total_volume: &str) -> Result<u64> {
    let from_volume = parse_volume_u64(volume)?;
    let from_total = parse_volume_u64(total_volume)?;
    match (
        from_volume != MISSING_VOLUME_U64,
        from_total != MISSING_VOLUME_U64,
    ) {
        (true, false) => Ok(from_volume),
        (false, true) => Ok(from_total),
        (true, true) if from_volume == from_total => Ok(from_volume),
        (true, true) => bail!("TOT Volume {volume:?} disagrees with Total Volume {total_volume:?}"),
        (false, false) => bail!("TOT row missing Volume and Total Volume"),
    }
}

/// Whether a Quote explicitly reports no auction imbalance.
///
/// `N` is not an ask-side code: the source pairs it with quantity zero and
/// `"N "[IMB_SIDE]`. It is a valid no-op rather than a directional imbalance.
pub fn is_no_imbalance(quantity: &str, side: &str) -> Result<bool> {
    if side.trim() != "N" {
        return Ok(false);
    }
    if parse_volume(quantity)? != 0 {
        bail!("Imbalance Side N requires Imbalance Quantity=0, got {quantity:?}");
    }
    Ok(true)
}

/// Promote an empty Quote that carries a directional Imbalance Quantity/Side.
/// Complete quotes keep their kind; the imbalance is a companion persist.
pub fn overlay_imbalance(kind: EventKind, quantity: &str, side: &str) -> Result<EventKind> {
    if quantity.is_empty() && side.is_empty() {
        return Ok(kind);
    }
    if quantity.is_empty() || side.is_empty() {
        bail!("imbalance missing Imbalance Quantity or Imbalance Side");
    }
    match kind {
        EventKind::CmeQuote | EventKind::CmeImbalance => Ok(kind),
        EventKind::DropEmptyQuote => Ok(EventKind::CmeImbalance),
        other => bail!("imbalance unexpectedly on {}", other.as_str()),
    }
}

pub fn parse_imbalance_side(raw: &str) -> Result<u8> {
    match raw.trim() {
        "B" | "BID" => Ok(IMBALANCE_SIDE_BID),
        "A" | "ASK" | "S" | "SELL" => Ok(IMBALANCE_SIDE_ASK),
        other => bail!("unhandled Imbalance Side {other:?}"),
    }
}

fn encode_fixed(trade: &SlimTrade, kind: u8) -> Result<[u8; CME_TRADE_LEN]> {
    if trade.aggressor > AGGRESSOR_SELL {
        bail!("aggressor {} is not 0/1/2", trade.aggressor);
    }
    let ric = encode_ric(&trade.ric)?;
    let mut buf = [0u8; CME_TRADE_LEN];
    buf[0..2].copy_from_slice(&MAGIC);
    buf[2] = VERSION;
    buf[3] = kind;
    buf[4..20].copy_from_slice(&ric);
    buf[20..28].copy_from_slice(&trade.ts_utc_ns.to_le_bytes());
    buf[28..36].copy_from_slice(&trade.exch_hms_ns.to_le_bytes());
    buf[36..44].copy_from_slice(&trade.price.to_le_bytes());
    buf[44..48].copy_from_slice(&trade.volume.to_le_bytes());
    buf[48..56].copy_from_slice(&trade.bid.to_le_bytes());
    buf[56..60].copy_from_slice(&trade.bid_size.to_le_bytes());
    buf[60..68].copy_from_slice(&trade.ask.to_le_bytes());
    buf[68..72].copy_from_slice(&trade.ask_size.to_le_bytes());
    buf[72] = trade.aggressor;
    Ok(buf)
}

pub fn encode_cme_trade(trade: &SlimTrade) -> Result<[u8; CME_TRADE_LEN]> {
    if trade.price == MISSING_PRICE {
        bail!("printable trade {} missing Price", trade.ric);
    }
    if trade.volume == MISSING_VOLUME {
        bail!("printable trade {} missing Volume", trade.ric);
    }
    encode_fixed(trade, KIND_CME_TRADE)
}

pub fn encode_cme_special(trade: &SlimTrade) -> Result<[u8; CME_TRADE_LEN]> {
    if trade.volume == MISSING_VOLUME {
        bail!("special trade {} missing Volume", trade.ric);
    }
    if trade.price != MISSING_PRICE {
        bail!("special trade {} unexpectedly has Price", trade.ric);
    }
    if trade.aggressor != AGGRESSOR_IMPLIED {
        bail!("special trade {} unexpectedly has aggressor", trade.ric);
    }
    encode_fixed(trade, KIND_CME_SPECIAL)
}

fn decode_fixed(buf: &[u8], expected_kind: u8, label: &str) -> Result<SlimTrade> {
    if buf.len() != CME_TRADE_LEN {
        bail!("{label} must be {CME_TRADE_LEN} bytes, got {}", buf.len());
    }
    if buf[0..2] != MAGIC {
        bail!("{label} magic is {:?}, expected CT", &buf[0..2]);
    }
    if buf[2] != VERSION {
        bail!("{label} version is {}, expected {VERSION}", buf[2]);
    }
    if buf[3] != expected_kind {
        bail!("{label} kind is {}, expected {expected_kind}", buf[3]);
    }
    if buf[73..80].iter().any(|&b| b != 0) {
        bail!("{label} pad is not all zeros");
    }
    let aggressor = buf[72];
    if aggressor > AGGRESSOR_SELL {
        bail!("{label} aggressor {aggressor} is not 0/1/2");
    }
    Ok(SlimTrade {
        ric: decode_ric(&buf[4..20])?,
        ts_utc_ns: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        exch_hms_ns: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
        price: i64::from_le_bytes(buf[36..44].try_into().unwrap()),
        volume: u32::from_le_bytes(buf[44..48].try_into().unwrap()),
        bid: i64::from_le_bytes(buf[48..56].try_into().unwrap()),
        bid_size: u32::from_le_bytes(buf[56..60].try_into().unwrap()),
        ask: i64::from_le_bytes(buf[60..68].try_into().unwrap()),
        ask_size: u32::from_le_bytes(buf[68..72].try_into().unwrap()),
        aggressor,
    })
}

pub fn decode_cme_trade(buf: &[u8]) -> Result<SlimTrade> {
    decode_fixed(buf, KIND_CME_TRADE, "cme_trade")
}

pub fn decode_cme_special(buf: &[u8]) -> Result<SlimTrade> {
    decode_fixed(buf, KIND_CME_SPECIAL, "cme_special")
}

fn side_complete(price: i64, size: u32) -> bool {
    price != MISSING_PRICE && size != MISSING_VOLUME
}

/// Incomplete sides (price without size, or size without price) are missing,
/// not a parse failure. They do not update that side of the standing BBO.
pub fn sanitize_quote_sides(quote: &mut SlimQuote) {
    if !side_complete(quote.bid, quote.bid_size) {
        quote.bid = MISSING_PRICE;
        quote.bid_size = MISSING_VOLUME;
    }
    if !side_complete(quote.ask, quote.ask_size) {
        quote.ask = MISSING_PRICE;
        quote.ask_size = MISSING_VOLUME;
    }
}

pub fn quote_has_complete_side(quote: &SlimQuote) -> bool {
    side_complete(quote.bid, quote.bid_size) || side_complete(quote.ask, quote.ask_size)
}

/// Apply `incoming` onto the standing book. Complete sides overwrite;
/// incomplete / empty sides keep the standing quote.
pub fn overlay_quote_bbo(standing: &SlimQuote, incoming: &SlimQuote) -> SlimQuote {
    let mut out = standing.clone();
    sanitize_quote_sides(&mut out);
    out.ts_utc_ns = incoming.ts_utc_ns;
    out.exch_hms_ns = incoming.exch_hms_ns;
    if side_complete(incoming.bid, incoming.bid_size) {
        out.bid = incoming.bid;
        out.bid_size = incoming.bid_size;
    }
    if side_complete(incoming.ask, incoming.ask_size) {
        out.ask = incoming.ask;
        out.ask_size = incoming.ask_size;
    }
    out
}

pub fn encode_cme_quote(quote: &SlimQuote) -> Result<[u8; CME_QUOTE_LEN]> {
    let mut quote = quote.clone();
    sanitize_quote_sides(&mut quote);
    if !quote_has_complete_side(&quote) {
        bail!("quote {} has neither bid nor ask", quote.ric);
    }
    let ric = encode_ric(&quote.ric)?;
    let mut buf = [0u8; CME_QUOTE_LEN];
    buf[0..2].copy_from_slice(&MAGIC);
    buf[2] = VERSION;
    buf[3] = KIND_CME_QUOTE;
    buf[4..20].copy_from_slice(&ric);
    buf[20..28].copy_from_slice(&quote.ts_utc_ns.to_le_bytes());
    buf[28..36].copy_from_slice(&quote.exch_hms_ns.to_le_bytes());
    buf[36..44].copy_from_slice(&quote.bid.to_le_bytes());
    buf[44..48].copy_from_slice(&quote.bid_size.to_le_bytes());
    buf[48..56].copy_from_slice(&quote.ask.to_le_bytes());
    buf[56..60].copy_from_slice(&quote.ask_size.to_le_bytes());
    Ok(buf)
}

pub fn decode_cme_quote(buf: &[u8]) -> Result<SlimQuote> {
    if buf.len() != CME_QUOTE_LEN {
        bail!("cme_quote must be {CME_QUOTE_LEN} bytes, got {}", buf.len());
    }
    if buf[0..2] != MAGIC {
        bail!("cme_quote magic is {:?}, expected CT", &buf[0..2]);
    }
    if buf[2] != VERSION {
        bail!("cme_quote version is {}, expected {VERSION}", buf[2]);
    }
    if buf[3] != KIND_CME_QUOTE {
        bail!("cme_quote kind is {}, expected {KIND_CME_QUOTE}", buf[3]);
    }
    if buf[60..64].iter().any(|&b| b != 0) {
        bail!("cme_quote pad is not all zeros");
    }
    let mut quote = SlimQuote {
        ric: decode_ric(&buf[4..20])?,
        ts_utc_ns: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        exch_hms_ns: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
        bid: i64::from_le_bytes(buf[36..44].try_into().unwrap()),
        bid_size: u32::from_le_bytes(buf[44..48].try_into().unwrap()),
        ask: i64::from_le_bytes(buf[48..56].try_into().unwrap()),
        ask_size: u32::from_le_bytes(buf[56..60].try_into().unwrap()),
    };
    sanitize_quote_sides(&mut quote);
    if !quote_has_complete_side(&quote) {
        bail!("decoded quote {} has neither bid nor ask", quote.ric);
    }
    Ok(quote)
}

pub fn parse_change_type(raw: &str) -> Result<u8> {
    match raw {
        "RIC" => Ok(CHANGE_TYPE_RIC),
        "" => bail!("unhandled empty Change Type"),
        other => bail!("unhandled Change Type {other:?}"),
    }
}

pub fn parse_reference_change_type(raw: &str) -> Result<u8> {
    match raw {
        "Description" => Ok(CHANGE_TYPE_DESCRIPTION),
        "Expiry Date" => Ok(CHANGE_TYPE_EXPIRY_DATE),
        "Currency" => Ok(CHANGE_TYPE_CURRENCY),
        "Permission Code" => Ok(CHANGE_TYPE_PERMISSION_CODE),
        "Option Type" => Ok(CHANGE_TYPE_OPTION_TYPE),
        "Template" => Ok(CHANGE_TYPE_TEMPLATE),
        "Exchange" => Ok(CHANGE_TYPE_EXCHANGE),
        "Bond Type" => Ok(CHANGE_TYPE_BOND_TYPE),
        "Record Type" => Ok(CHANGE_TYPE_RECORD_TYPE),
        "Rating" => Ok(CHANGE_TYPE_RATING),
        "Rating ID" => Ok(CHANGE_TYPE_RATING_ID),
        "" => bail!("unhandled empty Change Type"),
        other => bail!("unhandled Reference Change Type {other:?}"),
    }
}

fn reference_change_type_ok(change_type: u8) -> bool {
    matches!(
        change_type,
        CHANGE_TYPE_DESCRIPTION
            | CHANGE_TYPE_EXPIRY_DATE
            | CHANGE_TYPE_CURRENCY
            | CHANGE_TYPE_PERMISSION_CODE
            | CHANGE_TYPE_OPTION_TYPE
            | CHANGE_TYPE_TEMPLATE
            | CHANGE_TYPE_EXCHANGE
            | CHANGE_TYPE_BOND_TYPE
            | CHANGE_TYPE_RECORD_TYPE
            | CHANGE_TYPE_RATING
            | CHANGE_TYPE_RATING_ID
    )
}

pub fn parse_seq_u64(raw: &str) -> Result<u64> {
    if raw.is_empty() {
        return Ok(MISSING_SEQ);
    }
    if !raw.chars().all(|c| c.is_ascii_digit()) {
        bail!("unhandled sequence number {raw:?}");
    }
    raw.parse::<u64>()
        .map_err(|err| anyhow!("unhandled sequence number {raw:?}: {err}"))
}

fn encode_ascii_slot<const N: usize>(raw: &str, label: &str) -> Result<[u8; N]> {
    if !raw.is_ascii() {
        bail!("{label} {raw:?} is not ASCII");
    }
    if raw.len() > N {
        bail!("{label} {raw:?} longer than {N} bytes; refuse to truncate");
    }
    let mut out = [0u8; N];
    out[..raw.len()].copy_from_slice(raw.as_bytes());
    Ok(out)
}

fn decode_ascii_slot(bytes: &[u8], label: &str) -> Result<String> {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    let raw = &bytes[..end];
    if !raw.iter().all(|b| b.is_ascii() && *b != 0) {
        bail!("{label} slot is not ASCII");
    }
    if bytes[end..].iter().any(|&b| b != 0) {
        bail!("{label} slot has bytes after NUL");
    }
    Ok(std::str::from_utf8(raw)
        .map_err(|_| anyhow!("{label} slot is not UTF-8"))?
        .to_string())
}

pub fn encode_symbology_change(row: &SlimSymbologyChange) -> Result<[u8; SYMBOLOGY_CHANGE_LEN]> {
    if row.change_type != CHANGE_TYPE_RIC {
        bail!(
            "symbology change {} has unhandled change_type {}",
            row.ric,
            row.change_type
        );
    }
    if row.old_value.is_empty() {
        bail!("symbology change {} missing Old Value", row.ric);
    }
    if row.new_value.is_empty() {
        bail!("symbology change {} missing New Value", row.ric);
    }
    let ric = encode_ric(&row.ric)?;
    let old = encode_ric(&row.old_value)?;
    let new = encode_ric(&row.new_value)?;
    let mut buf = [0u8; SYMBOLOGY_CHANGE_LEN];
    buf[0..2].copy_from_slice(&MAGIC);
    buf[2] = VERSION;
    buf[3] = KIND_SYMBOLOGY_CHANGE;
    buf[4..20].copy_from_slice(&ric);
    buf[20..28].copy_from_slice(&row.ts_utc_ns.to_le_bytes());
    buf[28] = row.change_type;
    buf[29..45].copy_from_slice(&old);
    buf[45..61].copy_from_slice(&new);
    Ok(buf)
}

pub fn decode_symbology_change(buf: &[u8]) -> Result<SlimSymbologyChange> {
    if buf.len() != SYMBOLOGY_CHANGE_LEN {
        bail!(
            "symbology_change must be {SYMBOLOGY_CHANGE_LEN} bytes, got {}",
            buf.len()
        );
    }
    if buf[0..2] != MAGIC {
        bail!("symbology_change magic is {:?}, expected CT", &buf[0..2]);
    }
    if buf[2] != VERSION {
        bail!("symbology_change version is {}, expected {VERSION}", buf[2]);
    }
    if buf[3] != KIND_SYMBOLOGY_CHANGE {
        bail!(
            "symbology_change kind is {}, expected {KIND_SYMBOLOGY_CHANGE}",
            buf[3]
        );
    }
    if buf[61..64].iter().any(|&b| b != 0) {
        bail!("symbology_change pad is not all zeros");
    }
    let change_type = buf[28];
    if change_type != CHANGE_TYPE_RIC {
        bail!("symbology_change change_type {change_type} is not RIC");
    }
    Ok(SlimSymbologyChange {
        ric: decode_ric(&buf[4..20])?,
        ts_utc_ns: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        change_type,
        old_value: decode_ric(&buf[29..45])?,
        new_value: decode_ric(&buf[45..61])?,
    })
}

pub fn encode_cme_price_limit(row: &SlimPriceLimit) -> Result<[u8; CME_PRICE_LIMIT_LEN]> {
    if row.up_lim == MISSING_PRICE && row.lo_lim == MISSING_PRICE {
        bail!(
            "price limit {} missing both UpLim Price and LoLim Price",
            row.ric
        );
    }
    if row.up_lim != MISSING_PRICE && row.lo_lim != MISSING_PRICE && row.up_lim < row.lo_lim {
        bail!(
            "price limit {} has UpLim {} below LoLim {}",
            row.ric,
            row.up_lim,
            row.lo_lim
        );
    }
    let ric = encode_ric(&row.ric)?;
    let mut buf = [0u8; CME_PRICE_LIMIT_LEN];
    buf[0..2].copy_from_slice(&MAGIC);
    buf[2] = VERSION;
    buf[3] = KIND_CME_PRICE_LIMIT;
    buf[4..20].copy_from_slice(&ric);
    buf[20..28].copy_from_slice(&row.ts_utc_ns.to_le_bytes());
    buf[28..36].copy_from_slice(&row.up_lim.to_le_bytes());
    buf[36..44].copy_from_slice(&row.lo_lim.to_le_bytes());
    Ok(buf)
}

pub fn decode_cme_price_limit(buf: &[u8]) -> Result<SlimPriceLimit> {
    if buf.len() != CME_PRICE_LIMIT_LEN {
        bail!(
            "cme_price_limit must be {CME_PRICE_LIMIT_LEN} bytes, got {}",
            buf.len()
        );
    }
    if buf[0..2] != MAGIC {
        bail!("cme_price_limit magic is {:?}, expected CT", &buf[0..2]);
    }
    if buf[2] != VERSION {
        bail!("cme_price_limit version is {}, expected {VERSION}", buf[2]);
    }
    if buf[3] != KIND_CME_PRICE_LIMIT {
        bail!(
            "cme_price_limit kind is {}, expected {KIND_CME_PRICE_LIMIT}",
            buf[3]
        );
    }
    if buf[44..48].iter().any(|&b| b != 0) {
        bail!("cme_price_limit pad is not all zeros");
    }
    let row = SlimPriceLimit {
        ric: decode_ric(&buf[4..20])?,
        ts_utc_ns: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        up_lim: i64::from_le_bytes(buf[28..36].try_into().unwrap()),
        lo_lim: i64::from_le_bytes(buf[36..44].try_into().unwrap()),
    };
    if row.up_lim == MISSING_PRICE && row.lo_lim == MISSING_PRICE {
        bail!(
            "decoded price limit {} missing both UpLim Price and LoLim Price",
            row.ric
        );
    }
    if row.up_lim != MISSING_PRICE && row.lo_lim != MISSING_PRICE && row.up_lim < row.lo_lim {
        bail!(
            "decoded price limit {} has UpLim {} below LoLim {}",
            row.ric,
            row.up_lim,
            row.lo_lim
        );
    }
    Ok(row)
}

pub fn encode_cme_settlement(row: &SlimSettlement) -> Result<[u8; CME_SETTLEMENT_LEN]> {
    if row.price == MISSING_PRICE && row.source_date_yyyymmdd == 0 {
        bail!("settlement {} missing both Price and Date", row.ric);
    }
    let ric = encode_ric(&row.ric)?;
    let mut buf = [0u8; CME_SETTLEMENT_LEN];
    buf[0..2].copy_from_slice(&MAGIC);
    buf[2] = VERSION;
    buf[3] = KIND_CME_SETTLEMENT;
    buf[4..20].copy_from_slice(&ric);
    buf[20..28].copy_from_slice(&row.ts_utc_ns.to_le_bytes());
    buf[28..36].copy_from_slice(&row.price.to_le_bytes());
    buf[36..40].copy_from_slice(&row.source_date_yyyymmdd.to_le_bytes());
    Ok(buf)
}

pub fn decode_cme_settlement(buf: &[u8]) -> Result<SlimSettlement> {
    if buf.len() != CME_SETTLEMENT_LEN {
        bail!(
            "cme_settlement must be {CME_SETTLEMENT_LEN} bytes, got {}",
            buf.len()
        );
    }
    if buf[0..2] != MAGIC {
        bail!("cme_settlement magic is {:?}, expected CT", &buf[0..2]);
    }
    if buf[2] != VERSION {
        bail!("cme_settlement version is {}, expected {VERSION}", buf[2]);
    }
    if buf[3] != KIND_CME_SETTLEMENT {
        bail!(
            "cme_settlement kind is {}, expected {KIND_CME_SETTLEMENT}",
            buf[3]
        );
    }
    let row = SlimSettlement {
        ric: decode_ric(&buf[4..20])?,
        ts_utc_ns: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        price: i64::from_le_bytes(buf[28..36].try_into().unwrap()),
        source_date_yyyymmdd: u32::from_le_bytes(buf[36..40].try_into().unwrap()),
    };
    if row.price == MISSING_PRICE && row.source_date_yyyymmdd == 0 {
        bail!("decoded settlement {} missing both Price and Date", row.ric);
    }
    Ok(row)
}

fn iv_source_ok(source: u8) -> bool {
    source == IV_SOURCE_LAST || source == IV_SOURCE_QUOTE || source == IV_SOURCE_SETTLE
}

pub fn implied_vol_source(last: &str, bid: &str, ask: &str, settle: bool) -> Result<u8> {
    if settle {
        if !bid.is_empty() || !ask.is_empty() {
            bail!("settle IV unexpectedly has Bid Imp. Vol or Ask Imp. Vol");
        }
        if last.is_empty() {
            bail!("settle IV missing Imp. Vol.");
        }
        return Ok(IV_SOURCE_SETTLE);
    }
    if !bid.is_empty() || !ask.is_empty() {
        return Ok(IV_SOURCE_QUOTE);
    }
    if !last.is_empty() {
        return Ok(IV_SOURCE_LAST);
    }
    bail!("implied vol row has no Imp. Vol. / Bid Imp. Vol / Ask Imp. Vol")
}

pub fn encode_cme_implied_vol(row: &SlimImpliedVol) -> Result<[u8; CME_IMPLIED_VOL_LEN]> {
    if !iv_source_ok(row.source) {
        bail!(
            "implied vol {} has unhandled source {}",
            row.ric,
            row.source
        );
    }
    if row.last_iv == MISSING_PRICE && row.bid_iv == MISSING_PRICE && row.ask_iv == MISSING_PRICE {
        bail!("implied vol {} missing all IV sides", row.ric);
    }
    if row.source == IV_SOURCE_SETTLE && row.last_iv == MISSING_PRICE {
        bail!("settle IV {} missing Imp. Vol.", row.ric);
    }
    if row.source == IV_SOURCE_LAST && row.last_iv == MISSING_PRICE {
        bail!("last IV {} missing Imp. Vol.", row.ric);
    }
    if row.source == IV_SOURCE_QUOTE && row.bid_iv == MISSING_PRICE && row.ask_iv == MISSING_PRICE {
        bail!("quote IV {} missing Bid Imp. Vol and Ask Imp. Vol", row.ric);
    }
    let ric = encode_ric(&row.ric)?;
    let mut buf = [0u8; CME_IMPLIED_VOL_LEN];
    buf[0..2].copy_from_slice(&MAGIC);
    buf[2] = VERSION;
    buf[3] = KIND_CME_IMPLIED_VOL;
    buf[4..20].copy_from_slice(&ric);
    buf[20..28].copy_from_slice(&row.ts_utc_ns.to_le_bytes());
    buf[28..36].copy_from_slice(&row.exch_hms_ns.to_le_bytes());
    buf[36..44].copy_from_slice(&row.last_iv.to_le_bytes());
    buf[44..52].copy_from_slice(&row.bid_iv.to_le_bytes());
    buf[52..60].copy_from_slice(&row.ask_iv.to_le_bytes());
    buf[60] = row.source;
    Ok(buf)
}

pub fn decode_cme_implied_vol(buf: &[u8]) -> Result<SlimImpliedVol> {
    if buf.len() != CME_IMPLIED_VOL_LEN {
        bail!(
            "cme_implied_vol must be {CME_IMPLIED_VOL_LEN} bytes, got {}",
            buf.len()
        );
    }
    if buf[0..2] != MAGIC {
        bail!("cme_implied_vol magic is {:?}, expected CT", &buf[0..2]);
    }
    if buf[2] != VERSION {
        bail!("cme_implied_vol version is {}, expected {VERSION}", buf[2]);
    }
    if buf[3] != KIND_CME_IMPLIED_VOL {
        bail!(
            "cme_implied_vol kind is {}, expected {KIND_CME_IMPLIED_VOL}",
            buf[3]
        );
    }
    if buf[61..64].iter().any(|&b| b != 0) {
        bail!("cme_implied_vol pad is not all zeros");
    }
    let source = buf[60];
    if !iv_source_ok(source) {
        bail!("cme_implied_vol source {source} is not last/quote/settle");
    }
    let row = SlimImpliedVol {
        ric: decode_ric(&buf[4..20])?,
        ts_utc_ns: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        exch_hms_ns: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
        last_iv: i64::from_le_bytes(buf[36..44].try_into().unwrap()),
        bid_iv: i64::from_le_bytes(buf[44..52].try_into().unwrap()),
        ask_iv: i64::from_le_bytes(buf[52..60].try_into().unwrap()),
        source,
    };
    if row.last_iv == MISSING_PRICE && row.bid_iv == MISSING_PRICE && row.ask_iv == MISSING_PRICE {
        bail!("decoded implied vol {} missing all IV sides", row.ric);
    }
    Ok(row)
}

pub fn encode_cme_tot_volume(row: &SlimTotVolume) -> Result<[u8; CME_TOT_VOLUME_LEN]> {
    if row.volume == MISSING_VOLUME_U64 {
        bail!("TOT volume {} missing Volume", row.ric);
    }
    if !is_tot_ric(&row.ric) {
        bail!("TOT volume {} is not a TOT RIC", row.ric);
    }
    let ric = encode_ric(&row.ric)?;
    let mut buf = [0u8; CME_TOT_VOLUME_LEN];
    buf[0..2].copy_from_slice(&MAGIC);
    buf[2] = VERSION;
    buf[3] = KIND_CME_TOT_VOLUME;
    buf[4..20].copy_from_slice(&ric);
    buf[20..28].copy_from_slice(&row.ts_utc_ns.to_le_bytes());
    buf[28..36].copy_from_slice(&row.exch_hms_ns.to_le_bytes());
    buf[36..44].copy_from_slice(&row.volume.to_le_bytes());
    Ok(buf)
}

pub fn decode_cme_tot_volume(buf: &[u8]) -> Result<SlimTotVolume> {
    if buf.len() != CME_TOT_VOLUME_LEN {
        bail!(
            "cme_tot_volume must be {CME_TOT_VOLUME_LEN} bytes, got {}",
            buf.len()
        );
    }
    if buf[0..2] != MAGIC {
        bail!("cme_tot_volume magic is {:?}, expected CT", &buf[0..2]);
    }
    if buf[2] != VERSION {
        bail!("cme_tot_volume version is {}, expected {VERSION}", buf[2]);
    }
    if buf[3] != KIND_CME_TOT_VOLUME {
        bail!(
            "cme_tot_volume kind is {}, expected {KIND_CME_TOT_VOLUME}",
            buf[3]
        );
    }
    if buf[44..48].iter().any(|&b| b != 0) {
        bail!("cme_tot_volume pad is not all zeros");
    }
    let row = SlimTotVolume {
        ric: decode_ric(&buf[4..20])?,
        ts_utc_ns: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        exch_hms_ns: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
        volume: u64::from_le_bytes(buf[36..44].try_into().unwrap()),
    };
    if row.volume == MISSING_VOLUME_U64 {
        bail!("decoded TOT volume {} missing Volume", row.ric);
    }
    if !is_tot_ric(&row.ric) {
        bail!("decoded TOT volume {} is not a TOT RIC", row.ric);
    }
    Ok(row)
}

pub fn encode_cme_price_print(row: &SlimPricePrint) -> Result<[u8; CME_PRICE_PRINT_LEN]> {
    if row.price == MISSING_PRICE {
        bail!("price print {} missing Price", row.ric);
    }
    let ric = encode_ric(&row.ric)?;
    let mut buf = [0u8; CME_PRICE_PRINT_LEN];
    buf[0..2].copy_from_slice(&MAGIC);
    buf[2] = VERSION;
    buf[3] = KIND_CME_PRICE_PRINT;
    buf[4..20].copy_from_slice(&ric);
    buf[20..28].copy_from_slice(&row.ts_utc_ns.to_le_bytes());
    buf[28..36].copy_from_slice(&row.exch_hms_ns.to_le_bytes());
    buf[36..44].copy_from_slice(&row.price.to_le_bytes());
    Ok(buf)
}

pub fn decode_cme_price_print(buf: &[u8]) -> Result<SlimPricePrint> {
    if buf.len() != CME_PRICE_PRINT_LEN {
        bail!(
            "cme_price_print must be {CME_PRICE_PRINT_LEN} bytes, got {}",
            buf.len()
        );
    }
    if buf[0..2] != MAGIC {
        bail!("cme_price_print magic is {:?}, expected CT", &buf[0..2]);
    }
    if buf[2] != VERSION {
        bail!("cme_price_print version is {}, expected {VERSION}", buf[2]);
    }
    if buf[3] != KIND_CME_PRICE_PRINT {
        bail!(
            "cme_price_print kind is {}, expected {KIND_CME_PRICE_PRINT}",
            buf[3]
        );
    }
    if buf[44..48].iter().any(|&b| b != 0) {
        bail!("cme_price_print pad is not all zeros");
    }
    let row = SlimPricePrint {
        ric: decode_ric(&buf[4..20])?,
        ts_utc_ns: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        exch_hms_ns: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
        price: i64::from_le_bytes(buf[36..44].try_into().unwrap()),
    };
    if row.price == MISSING_PRICE {
        bail!("decoded price print {} missing Price", row.ric);
    }
    Ok(row)
}

pub fn encode_cme_imbalance(row: &SlimImbalance) -> Result<[u8; CME_IMBALANCE_LEN]> {
    if row.quantity == MISSING_VOLUME {
        bail!("imbalance {} missing Imbalance Quantity", row.ric);
    }
    if row.side != IMBALANCE_SIDE_BID && row.side != IMBALANCE_SIDE_ASK {
        bail!("imbalance {} has unhandled side {}", row.ric, row.side);
    }
    let ric = encode_ric(&row.ric)?;
    let mut buf = [0u8; CME_IMBALANCE_LEN];
    buf[0..2].copy_from_slice(&MAGIC);
    buf[2] = VERSION;
    buf[3] = KIND_CME_IMBALANCE;
    buf[4..20].copy_from_slice(&ric);
    buf[20..28].copy_from_slice(&row.ts_utc_ns.to_le_bytes());
    buf[28..36].copy_from_slice(&row.exch_hms_ns.to_le_bytes());
    buf[36..40].copy_from_slice(&row.quantity.to_le_bytes());
    buf[40] = row.side;
    Ok(buf)
}

pub fn decode_cme_imbalance(buf: &[u8]) -> Result<SlimImbalance> {
    if buf.len() != CME_IMBALANCE_LEN {
        bail!(
            "cme_imbalance must be {CME_IMBALANCE_LEN} bytes, got {}",
            buf.len()
        );
    }
    if buf[0..2] != MAGIC {
        bail!("cme_imbalance magic is {:?}, expected CT", &buf[0..2]);
    }
    if buf[2] != VERSION {
        bail!("cme_imbalance version is {}, expected {VERSION}", buf[2]);
    }
    if buf[3] != KIND_CME_IMBALANCE {
        bail!(
            "cme_imbalance kind is {}, expected {KIND_CME_IMBALANCE}",
            buf[3]
        );
    }
    if buf[41..48].iter().any(|&b| b != 0) {
        bail!("cme_imbalance pad is not all zeros");
    }
    let side = buf[40];
    if side != IMBALANCE_SIDE_BID && side != IMBALANCE_SIDE_ASK {
        bail!("cme_imbalance side {side} is not bid/ask");
    }
    let row = SlimImbalance {
        ric: decode_ric(&buf[4..20])?,
        ts_utc_ns: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        exch_hms_ns: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
        quantity: u32::from_le_bytes(buf[36..40].try_into().unwrap()),
        side,
    };
    if row.quantity == MISSING_VOLUME {
        bail!("decoded imbalance {} missing Imbalance Quantity", row.ric);
    }
    Ok(row)
}

pub fn encode_cme_auction(row: &SlimAuction) -> Result<[u8; CME_AUCTION_LEN]> {
    if row.price == MISSING_PRICE {
        bail!("auction {} missing Price", row.ric);
    }
    let ric = encode_ric(&row.ric)?;
    let mut buf = [0u8; CME_AUCTION_LEN];
    buf[0..2].copy_from_slice(&MAGIC);
    buf[2] = VERSION;
    buf[3] = KIND_CME_AUCTION;
    buf[4..20].copy_from_slice(&ric);
    buf[20..28].copy_from_slice(&row.ts_utc_ns.to_le_bytes());
    buf[28..36].copy_from_slice(&row.exch_hms_ns.to_le_bytes());
    buf[36..44].copy_from_slice(&row.price.to_le_bytes());
    buf[44..48].copy_from_slice(&row.volume.to_le_bytes());
    Ok(buf)
}

pub fn decode_cme_auction(buf: &[u8]) -> Result<SlimAuction> {
    if buf.len() != CME_AUCTION_LEN {
        bail!(
            "cme_auction must be {CME_AUCTION_LEN} bytes, got {}",
            buf.len()
        );
    }
    if buf[0..2] != MAGIC {
        bail!("cme_auction magic is {:?}, expected CT", &buf[0..2]);
    }
    if buf[2] != VERSION {
        bail!("cme_auction version is {}, expected {VERSION}", buf[2]);
    }
    if buf[3] != KIND_CME_AUCTION {
        bail!(
            "cme_auction kind is {}, expected {KIND_CME_AUCTION}",
            buf[3]
        );
    }
    let row = SlimAuction {
        ric: decode_ric(&buf[4..20])?,
        ts_utc_ns: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        exch_hms_ns: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
        price: i64::from_le_bytes(buf[36..44].try_into().unwrap()),
        volume: u32::from_le_bytes(buf[44..48].try_into().unwrap()),
    };
    if row.price == MISSING_PRICE {
        bail!("decoded auction {} missing Price", row.ric);
    }
    Ok(row)
}

pub fn encode_cme_correction(row: &SlimCorrection) -> Result<[u8; CME_CORRECTION_LEN]> {
    if row.price == MISSING_PRICE
        && row.volume == MISSING_VOLUME
        && row.acc_volume == MISSING_VOLUME
        && row.original_price == MISSING_PRICE
        && row.original_volume == MISSING_VOLUME
        && row.original_seq == MISSING_SEQ
        && row.qualifiers.is_empty()
    {
        bail!(
            "correction {} has no price, volume, original fields, or qualifiers",
            row.ric
        );
    }
    let ric = encode_ric(&row.ric)?;
    let qualifiers =
        encode_ascii_slot::<CORRECTION_QUALIFIER_LEN>(&row.qualifiers, "correction Qualifiers")?;
    let mut buf = [0u8; CME_CORRECTION_LEN];
    buf[0..2].copy_from_slice(&MAGIC);
    buf[2] = CME_CORRECTION_VERSION;
    buf[3] = KIND_CME_CORRECTION;
    buf[4..20].copy_from_slice(&ric);
    buf[20..28].copy_from_slice(&row.ts_utc_ns.to_le_bytes());
    buf[28..36].copy_from_slice(&row.exch_hms_ns.to_le_bytes());
    buf[36..44].copy_from_slice(&row.price.to_le_bytes());
    buf[44..48].copy_from_slice(&row.volume.to_le_bytes());
    buf[48..56].copy_from_slice(&row.original_price.to_le_bytes());
    buf[56..60].copy_from_slice(&row.original_volume.to_le_bytes());
    buf[60..68].copy_from_slice(&row.original_seq.to_le_bytes());
    buf[68..220].copy_from_slice(&qualifiers);
    buf[220..224].copy_from_slice(&row.acc_volume.to_le_bytes());
    Ok(buf)
}

pub fn decode_cme_correction(buf: &[u8]) -> Result<SlimCorrection> {
    if buf.len() != CME_CORRECTION_LEN {
        bail!(
            "cme_correction must be {CME_CORRECTION_LEN} bytes, got {}",
            buf.len()
        );
    }
    if buf[0..2] != MAGIC {
        bail!("cme_correction magic is {:?}, expected CT", &buf[0..2]);
    }
    let version = buf[2];
    if version != VERSION && version != CME_CORRECTION_VERSION {
        bail!("cme_correction version is {}, expected {VERSION}", buf[2]);
    }
    if buf[3] != KIND_CME_CORRECTION {
        bail!(
            "cme_correction kind is {}, expected {KIND_CME_CORRECTION}",
            buf[3]
        );
    }
    let acc_volume = match version {
        VERSION => {
            if buf[220..224].iter().any(|&b| b != 0) {
                bail!("cme_correction v1 pad is not all zeros");
            }
            MISSING_VOLUME
        }
        CME_CORRECTION_VERSION => u32::from_le_bytes(buf[220..224].try_into().unwrap()),
        _ => unreachable!("version was validated above"),
    };
    let row = SlimCorrection {
        ric: decode_ric(&buf[4..20])?,
        ts_utc_ns: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        exch_hms_ns: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
        price: i64::from_le_bytes(buf[36..44].try_into().unwrap()),
        volume: u32::from_le_bytes(buf[44..48].try_into().unwrap()),
        acc_volume,
        original_price: i64::from_le_bytes(buf[48..56].try_into().unwrap()),
        original_volume: u32::from_le_bytes(buf[56..60].try_into().unwrap()),
        original_seq: u64::from_le_bytes(buf[60..68].try_into().unwrap()),
        qualifiers: decode_ascii_slot(&buf[68..220], "correction Qualifiers")?,
    };
    if row.price == MISSING_PRICE
        && row.volume == MISSING_VOLUME
        && row.acc_volume == MISSING_VOLUME
        && row.original_price == MISSING_PRICE
        && row.original_volume == MISSING_VOLUME
        && row.original_seq == MISSING_SEQ
        && row.qualifiers.is_empty()
    {
        bail!(
            "decoded correction {} has no price, volume, original fields, or qualifiers",
            row.ric
        );
    }
    Ok(row)
}

pub fn encode_cme_status(row: &SlimStatus) -> Result<[u8; CME_STATUS_LEN]> {
    if row.qualifiers.is_empty() {
        bail!("status {} missing Qualifiers", row.ric);
    }
    let ric = encode_ric(&row.ric)?;
    let qualifiers =
        encode_ascii_slot::<STATUS_QUALIFIER_LEN>(&row.qualifiers, "status Qualifiers")?;
    let mut buf = [0u8; CME_STATUS_LEN];
    buf[0..2].copy_from_slice(&MAGIC);
    buf[2] = VERSION;
    buf[3] = KIND_CME_STATUS;
    buf[4..20].copy_from_slice(&ric);
    buf[20..28].copy_from_slice(&row.ts_utc_ns.to_le_bytes());
    buf[28..36].copy_from_slice(&row.exch_hms_ns.to_le_bytes());
    buf[36..220].copy_from_slice(&qualifiers);
    Ok(buf)
}

pub fn decode_cme_status(buf: &[u8]) -> Result<SlimStatus> {
    let qualifier_end = match buf.len() {
        CME_STATUS_LEGACY_LEN => 36 + STATUS_QUALIFIER_LEGACY_LEN,
        CME_STATUS_LEN => 36 + STATUS_QUALIFIER_LEN,
        other => {
            bail!(
                "cme_status must be {CME_STATUS_LEGACY_LEN} or {CME_STATUS_LEN} bytes, got {other}"
            )
        }
    };
    if buf[0..2] != MAGIC {
        bail!("cme_status magic is {:?}, expected CT", &buf[0..2]);
    }
    if buf[2] != VERSION {
        bail!("cme_status version is {}, expected {VERSION}", buf[2]);
    }
    if buf[3] != KIND_CME_STATUS {
        bail!("cme_status kind is {}, expected {KIND_CME_STATUS}", buf[3]);
    }
    if buf[qualifier_end..].iter().any(|&b| b != 0) {
        bail!("cme_status pad is not all zeros");
    }
    let row = SlimStatus {
        ric: decode_ric(&buf[4..20])?,
        ts_utc_ns: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        exch_hms_ns: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
        qualifiers: decode_ascii_slot(&buf[36..qualifier_end], "status Qualifiers")?,
    };
    if row.qualifiers.is_empty() {
        bail!("decoded status {} missing Qualifiers", row.ric);
    }
    Ok(row)
}

pub fn encode_reference_change(row: &SlimReferenceChange) -> Result<[u8; REFERENCE_CHANGE_LEN]> {
    if !reference_change_type_ok(row.change_type) {
        bail!(
            "reference change {} has unhandled change_type {}",
            row.ric,
            row.change_type
        );
    }
    if row.old_value.is_empty() && row.new_value.is_empty() {
        bail!("reference change {} has neither Old nor New Value", row.ric);
    }
    let ric = encode_ric(&row.ric)?;
    let old = encode_ascii_slot::<REFERENCE_VALUE_LEN>(&row.old_value, "reference Old Value")?;
    let new = encode_ascii_slot::<REFERENCE_VALUE_LEN>(&row.new_value, "reference New Value")?;
    let mut buf = [0u8; REFERENCE_CHANGE_LEN];
    buf[0..2].copy_from_slice(&MAGIC);
    buf[2] = VERSION;
    buf[3] = KIND_REFERENCE_CHANGE;
    buf[4..20].copy_from_slice(&ric);
    buf[20..28].copy_from_slice(&row.ts_utc_ns.to_le_bytes());
    buf[28] = row.change_type;
    buf[29..61].copy_from_slice(&old);
    buf[61..93].copy_from_slice(&new);
    Ok(buf)
}

pub fn decode_reference_change(buf: &[u8]) -> Result<SlimReferenceChange> {
    if buf.len() != REFERENCE_CHANGE_LEN {
        bail!(
            "reference_change must be {REFERENCE_CHANGE_LEN} bytes, got {}",
            buf.len()
        );
    }
    if buf[0..2] != MAGIC {
        bail!("reference_change magic is {:?}, expected CT", &buf[0..2]);
    }
    if buf[2] != VERSION {
        bail!("reference_change version is {}, expected {VERSION}", buf[2]);
    }
    if buf[3] != KIND_REFERENCE_CHANGE {
        bail!(
            "reference_change kind is {}, expected {KIND_REFERENCE_CHANGE}",
            buf[3]
        );
    }
    if buf[93..96].iter().any(|&b| b != 0) {
        bail!("reference_change pad is not all zeros");
    }
    let change_type = buf[28];
    if !reference_change_type_ok(change_type) {
        bail!("reference_change change_type {change_type} is unhandled");
    }
    Ok(SlimReferenceChange {
        ric: decode_ric(&buf[4..20])?,
        ts_utc_ns: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        change_type,
        old_value: decode_ascii_slot(&buf[29..61], "reference Old Value")?,
        new_value: decode_ascii_slot(&buf[61..93], "reference New Value")?,
    })
}

pub fn ric_prefix(ric: &str) -> Result<[u8; RIC_LEN]> {
    encode_ric(ric)
}

pub fn minute_left_edge_ns(ts_utc_ns: u64) -> u64 {
    const NS_PER_MIN: u64 = 60_000_000_000;
    (ts_utc_ns / NS_PER_MIN) * NS_PER_MIN
}

/// CME Globex session date: Chicago 17:00 roll. `2026-01-01T23:00Z` → 20260102.
pub fn tradeday_yyyymmdd(ts_utc_ns: u64) -> Result<u32> {
    let secs = i64::try_from(ts_utc_ns / 1_000_000_000)
        .map_err(|_| anyhow!("Date-Time ns {ts_utc_ns} out of range"))?;
    let nsec = (ts_utc_ns % 1_000_000_000) as u32;
    let utc = DateTime::from_timestamp(secs, nsec)
        .ok_or_else(|| anyhow!("Date-Time ns {ts_utc_ns} is not a UTC instant"))?;
    let local = utc.with_timezone(&Chicago);
    let rolled = if local.time() >= NaiveTime::from_hms_opt(17, 0, 0).unwrap() {
        local.date_naive().succ_opt()
    } else {
        Some(local.date_naive())
    };
    let date = rolled.ok_or_else(|| anyhow!("tradeday overflow at {ts_utc_ns}"))?;
    Ok(date.year() as u32 * 10_000 + date.month() * 100 + date.day())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SynthBar {
    pub ric: String,
    pub minute_utc_ns: u64,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub last: i64,
    pub volume: u64,
    pub no_trades: u32,
}

impl SynthBar {
    fn push(&mut self, trade: &SlimTrade) {
        if self.no_trades == 0 {
            self.open = trade.price;
            self.high = trade.price;
            self.low = trade.price;
        } else {
            self.high = self.high.max(trade.price);
            self.low = self.low.min(trade.price);
        }
        self.last = trade.price;
        self.volume += u64::from(trade.volume);
        self.no_trades += 1;
    }
}

pub fn synthesize_1min_bars(trades: &[SlimTrade]) -> Vec<SynthBar> {
    let mut bars: Vec<SynthBar> = Vec::new();
    for trade in trades {
        let minute = minute_left_edge_ns(trade.ts_utc_ns);
        match bars.last_mut() {
            Some(bar) if bar.ric == trade.ric && bar.minute_utc_ns == minute => {
                bar.push(trade);
            }
            _ => {
                let mut bar = SynthBar {
                    ric: trade.ric.clone(),
                    minute_utc_ns: minute,
                    open: 0,
                    high: 0,
                    low: 0,
                    last: 0,
                    volume: 0,
                    no_trades: 0,
                };
                bar.push(trade);
                bars.push(bar);
            }
        }
    }
    bars
}

/// One UTC-left-edge minute from `cme_trade` plus `cme_special`.
///
/// OHLC / `volume` / `no_trades` come only from printable trades.
/// Special volume is a separate column and may explain extra Summary Volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SynthMinute {
    pub ric: String,
    pub minute_utc_ns: u64,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub last: i64,
    pub volume: u64,
    pub no_trades: u32,
    pub special_volume: u64,
    pub special_count: u32,
}

impl SynthMinute {
    pub fn priced(&self) -> bool {
        self.open != MISSING_PRICE
    }

    pub fn volume_total(&self) -> u64 {
        self.volume + self.special_volume
    }

    pub fn as_trade_bar(&self) -> SynthBar {
        SynthBar {
            ric: self.ric.clone(),
            minute_utc_ns: self.minute_utc_ns,
            open: self.open,
            high: self.high,
            low: self.low,
            last: self.last,
            volume: self.volume,
            no_trades: self.no_trades,
        }
    }
}

pub fn price_e9_to_f64(price: i64) -> Option<f64> {
    if price == MISSING_PRICE {
        None
    } else {
        Some(price as f64 / PRICE_SCALE as f64)
    }
}

pub fn format_utc_ns_z(ns: u64) -> Result<String> {
    let secs =
        i64::try_from(ns / 1_000_000_000).map_err(|_| anyhow!("Date-Time ns {ns} out of range"))?;
    let nsec = (ns % 1_000_000_000) as u32;
    let utc = DateTime::from_timestamp(secs, nsec)
        .ok_or_else(|| anyhow!("Date-Time ns {ns} is not a UTC instant"))?;
    Ok(utc.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string())
}

/// Merge printable trades and Specials onto UTC minute-left-edge bars.
///
/// Trades own Open/High/Low/Last/Volume/No. Trades. Specials only add
/// `special_volume` / `special_count`. A Special-only minute has missing OHLC.
pub fn synthesize_1min_from_trade_and_special(
    trades: &[SlimTrade],
    specials: &[SlimTrade],
) -> Vec<SynthMinute> {
    let trade_bars = synthesize_1min_bars(trades);
    let mut special_by_min: BTreeMap<(String, u64), (u64, u32)> = BTreeMap::new();
    for rec in specials {
        let minute = minute_left_edge_ns(rec.ts_utc_ns);
        let entry = special_by_min
            .entry((rec.ric.clone(), minute))
            .or_insert((0, 0));
        entry.0 += u64::from(rec.volume);
        entry.1 += 1;
    }
    let mut out: BTreeMap<(String, u64), SynthMinute> = BTreeMap::new();
    for bar in trade_bars {
        let key = (bar.ric.clone(), bar.minute_utc_ns);
        let (special_volume, special_count) = special_by_min.remove(&key).unwrap_or((0, 0));
        out.insert(
            key,
            SynthMinute {
                ric: bar.ric,
                minute_utc_ns: bar.minute_utc_ns,
                open: bar.open,
                high: bar.high,
                low: bar.low,
                last: bar.last,
                volume: bar.volume,
                no_trades: bar.no_trades,
                special_volume,
                special_count,
            },
        );
    }
    for ((ric, minute_utc_ns), (special_volume, special_count)) in special_by_min {
        out.insert(
            (ric.clone(), minute_utc_ns),
            SynthMinute {
                ric,
                minute_utc_ns,
                open: MISSING_PRICE,
                high: MISSING_PRICE,
                low: MISSING_PRICE,
                last: MISSING_PRICE,
                volume: 0,
                no_trades: 0,
                special_volume,
                special_count,
            },
        );
    }
    out.into_values().collect()
}

/// Write synth minutes as parquet. Missing OHLC is null, not 0.
pub fn write_synth_minutes_parquet(path: &Path, minutes: &[SynthMinute]) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create parquet parent {}", parent.display()))?;
        }
    }
    let n = minutes.len();
    let mut ric = Vec::with_capacity(n);
    let mut ts = Vec::with_capacity(n);
    let mut ts_utc_ns = Vec::with_capacity(n);
    let mut date_time = Vec::with_capacity(n);
    let mut open = Vec::with_capacity(n);
    let mut high = Vec::with_capacity(n);
    let mut low = Vec::with_capacity(n);
    let mut close = Vec::with_capacity(n);
    let mut volume = Vec::with_capacity(n);
    let mut count = Vec::with_capacity(n);
    let mut special_volume = Vec::with_capacity(n);
    let mut special_count = Vec::with_capacity(n);
    let mut volume_total = Vec::with_capacity(n);
    for row in minutes {
        ric.push(row.ric.clone());
        ts.push((row.minute_utc_ns / 1_000_000_000) as i64);
        ts_utc_ns.push(
            i64::try_from(row.minute_utc_ns)
                .with_context(|| format!("minute {} overflowed i64", row.minute_utc_ns))?,
        );
        date_time.push(format_utc_ns_z(row.minute_utc_ns)?);
        open.push(price_e9_to_f64(row.open));
        high.push(price_e9_to_f64(row.high));
        low.push(price_e9_to_f64(row.low));
        close.push(price_e9_to_f64(row.last));
        volume.push(i64::try_from(row.volume).context("volume overflowed i64")?);
        count.push(i32::try_from(row.no_trades).context("no_trades overflowed i32")?);
        special_volume
            .push(i64::try_from(row.special_volume).context("special_volume overflowed i64")?);
        special_count
            .push(i32::try_from(row.special_count).context("special_count overflowed i32")?);
        volume_total
            .push(i64::try_from(row.volume_total()).context("volume_total overflowed i64")?);
    }
    let mut df = DataFrame::new(vec![
        Series::new("ric".into(), ric),
        Series::new("ts".into(), ts),
        Series::new("ts_utc_ns".into(), ts_utc_ns),
        Series::new("date_time".into(), date_time),
        Series::new("open".into(), open),
        Series::new("high".into(), high),
        Series::new("low".into(), low),
        Series::new("close".into(), close),
        Series::new("volume".into(), volume),
        Series::new("count".into(), count),
        Series::new("special_volume".into(), special_volume),
        Series::new("special_count".into(), special_count),
        Series::new("volume_total".into(), volume_total),
    ])
    .context("build synth 1min dataframe")?;
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    ParquetWriter::new(file)
        .finish(&mut df)
        .with_context(|| format!("write parquet {}", path.display()))?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareVerdict {
    Exact,
    /// Volume matches only after adding Special volume. OHLC still exact.
    Approximate,
    /// Leftover priced OHLC or Volume after Special accounting.
    Mismatch,
    MissingSide,
}

#[derive(Debug, Clone)]
pub struct FieldDelta {
    pub name: &'static str,
    pub synth: Option<i128>,
    pub summary: Option<i128>,
}

#[derive(Debug, Clone)]
pub struct MinuteCompare {
    pub ric: String,
    pub minute_utc_ns: u64,
    pub verdict: CompareVerdict,
    pub deltas: Vec<FieldDelta>,
}

#[derive(Debug, Clone)]
pub struct SummaryCompare {
    pub n_compared: usize,
    pub n_exact: usize,
    pub n_volume_only_summary: usize,
    pub n_missing_side: usize,
    pub verdict: CompareVerdict,
    pub rows: Vec<MinuteCompare>,
}

fn field_eq(name: &'static str, synth: i128, summary: Option<i128>) -> Option<FieldDelta> {
    match summary {
        Some(value) if value == synth => None,
        other => Some(FieldDelta {
            name,
            synth: Some(synth),
            summary: other,
        }),
    }
}

pub fn compare_synth_to_summary(synth: &SynthBar, summary: &SynthBar) -> MinuteCompare {
    let mut deltas = Vec::new();
    if let Some(delta) = field_eq("Open", synth.open as i128, Some(summary.open as i128)) {
        deltas.push(delta);
    }
    if let Some(delta) = field_eq("High", synth.high as i128, Some(summary.high as i128)) {
        deltas.push(delta);
    }
    if let Some(delta) = field_eq("Low", synth.low as i128, Some(summary.low as i128)) {
        deltas.push(delta);
    }
    if let Some(delta) = field_eq("Last", synth.last as i128, Some(summary.last as i128)) {
        deltas.push(delta);
    }
    if let Some(delta) = field_eq("Volume", synth.volume as i128, Some(summary.volume as i128)) {
        deltas.push(delta);
    }
    if let Some(delta) = field_eq(
        "No. Trades",
        i128::from(synth.no_trades),
        Some(i128::from(summary.no_trades)),
    ) {
        deltas.push(delta);
    }
    MinuteCompare {
        ric: synth.ric.clone(),
        minute_utc_ns: synth.minute_utc_ns,
        verdict: if deltas.is_empty() {
            CompareVerdict::Exact
        } else {
            CompareVerdict::Mismatch
        },
        deltas,
    }
}

/// Priced-minute check used by the independent Summary compare.
///
/// Printable trades own Open/High/Low/Last. Summary Volume may equal
/// printable Volume plus Special Volume. A leftover priced field after that
/// accounting is `Mismatch`. `No. Trades` is not a priced-kline field.
pub fn compare_priced_minute(
    synth: &SynthBar,
    summary: &SynthBar,
    special_volume: u64,
) -> MinuteCompare {
    let mut row = compare_synth_to_summary(synth, summary);
    row.deltas.retain(|delta| delta.name != "No. Trades");
    let special = i128::from(special_volume);
    let mut leftover = false;
    let mut volume_by_special = false;
    for delta in &row.deltas {
        if delta.name == "Volume"
            && delta.synth.unwrap_or(0) + special == delta.summary.unwrap_or(0)
        {
            volume_by_special = true;
        } else {
            leftover = true;
        }
    }
    row.verdict = if leftover {
        CompareVerdict::Mismatch
    } else if volume_by_special {
        CompareVerdict::Approximate
    } else {
        CompareVerdict::Exact
    };
    row
}

pub fn encode_key(ric: &str, ts_utc_ns: u64, part: u16, seq: u32) -> Result<[u8; KEY_LEN]> {
    let mut key = [0u8; KEY_LEN];
    key[..RIC_LEN].copy_from_slice(&encode_ric(ric)?);
    key[RIC_LEN..RIC_LEN + KEY_TS_LEN].copy_from_slice(&ts_utc_ns.to_be_bytes());
    key[RIC_LEN + KEY_TS_LEN..RIC_LEN + KEY_TS_LEN + KEY_PART_LEN]
        .copy_from_slice(&part.to_be_bytes());
    key[RIC_LEN + KEY_TS_LEN + KEY_PART_LEN..].copy_from_slice(&seq.to_be_bytes());
    Ok(key)
}

pub fn key_ts_utc_ns(key: &[u8]) -> Result<u64> {
    if key.len() != KEY_LEN {
        bail!("key length {} is not {KEY_LEN}", key.len());
    }
    Ok(u64::from_be_bytes(
        key[RIC_LEN..RIC_LEN + KEY_TS_LEN].try_into().unwrap(),
    ))
}

pub fn key_part(key: &[u8]) -> Result<u16> {
    if key.len() != KEY_LEN {
        bail!("key length {} is not {KEY_LEN}", key.len());
    }
    Ok(u16::from_be_bytes(
        key[RIC_LEN + KEY_TS_LEN..RIC_LEN + KEY_TS_LEN + KEY_PART_LEN]
            .try_into()
            .unwrap(),
    ))
}

pub fn ali_h26_fixture() -> SlimTrade {
    SlimTrade {
        ric: "ALIH26".to_string(),
        ts_utc_ns: parse_date_time_ns("2026-01-02T15:39:23.298829985Z").unwrap(),
        exch_hms_ns: parse_exch_hms_ns("15:39:23.290603633").unwrap(),
        price: 2_999_750_000_000,
        volume: 1,
        bid: 2_996_500_000_000,
        bid_size: 1,
        ask: 2_999_750_000_000,
        ask_size: 1,
        aggressor: 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packs_the_ali_trade() {
        let trade = ali_h26_fixture();
        let bytes = encode_cme_trade(&trade).unwrap();
        assert_eq!(bytes.len(), 80);
        assert_eq!(&bytes[0..2], b"CT");
        assert_eq!(bytes[2], 1);
        assert_eq!(bytes[3], 1);
        let back = decode_cme_trade(&bytes).unwrap();
        assert_eq!(back.ric, "ALIH26");
        assert_eq!(back.ts_utc_ns, trade.ts_utc_ns);
        assert_eq!(back.exch_hms_ns, 56_363_290_603_633);
        assert_eq!(back.price, 2_999_750_000_000);
        assert_eq!(back.bid, 2_996_500_000_000);
        assert_eq!(back.aggressor, 1);
        assert_eq!(&bytes[73..80], &[0; 7]);
    }

    #[test]
    fn packs_a_special_without_price() {
        let special = SlimTrade {
            ric: "ADF26".to_string(),
            ts_utc_ns: parse_date_time_ns("2026-01-02T10:22:03.633592953Z").unwrap(),
            exch_hms_ns: parse_exch_hms_ns("10:22:03.626553219").unwrap(),
            price: MISSING_PRICE,
            volume: 1,
            bid: MISSING_PRICE,
            bid_size: MISSING_VOLUME,
            ask: MISSING_PRICE,
            ask_size: MISSING_VOLUME,
            aggressor: 0,
        };
        let bytes = encode_cme_special(&special).unwrap();
        assert_eq!(bytes[3], KIND_CME_SPECIAL);
        let back = decode_cme_special(&bytes).unwrap();
        assert_eq!(back.ric, "ADF26");
        assert_eq!(back.volume, 1);
        assert_eq!(back.price, MISSING_PRICE);
        assert!(encode_cme_trade(&special).is_err());
        assert!(decode_cme_trade(&bytes).is_err());
    }

    #[test]
    fn packs_the_adf_open_quote() {
        let quote = SlimQuote {
            ric: "ADF26".to_string(),
            ts_utc_ns: parse_date_time_ns("2026-01-01T23:00:00.024033858Z").unwrap(),
            exch_hms_ns: parse_exch_hms_ns("23:00:00.000000000").unwrap(),
            bid: parse_price_e9("0.6671").unwrap(),
            bid_size: 1,
            ask: parse_price_e9("0.66785").unwrap(),
            ask_size: 6,
        };
        let bytes = encode_cme_quote(&quote).unwrap();
        assert_eq!(bytes.len(), 64);
        assert_eq!(bytes[3], KIND_CME_QUOTE);
        assert_eq!(quote.exch_hms_ns, 82_800_000_000_000);
        assert_eq!(quote.bid, 667_100_000);
        assert_eq!(quote.ask, 667_850_000);
        let back = decode_cme_quote(&bytes).unwrap();
        assert_eq!(back.ric, "ADF26");
        assert_eq!(back.bid_size, 1);
        assert_eq!(back.ask_size, 6);
        let one_sided = SlimQuote {
            ask: MISSING_PRICE,
            ask_size: MISSING_VOLUME,
            ..quote.clone()
        };
        assert!(encode_cme_quote(&one_sided).is_ok());
        let empty = SlimQuote {
            bid: MISSING_PRICE,
            bid_size: MISSING_VOLUME,
            ask: MISSING_PRICE,
            ask_size: MISSING_VOLUME,
            ..quote.clone()
        };
        assert!(!quote_has_complete_side(&empty));
        assert!(encode_cme_quote(&empty).is_err());
        let half = SlimQuote {
            bid: parse_price_e9("0.6671").unwrap(),
            bid_size: MISSING_VOLUME,
            ask: MISSING_PRICE,
            ask_size: MISSING_VOLUME,
            ..one_sided.clone()
        };
        assert!(!quote_has_complete_side(&half));
        assert!(encode_cme_quote(&half).is_err());
        let ask_zero_no_size = SlimQuote {
            ask: 0,
            ask_size: MISSING_VOLUME,
            ..one_sided.clone()
        };
        assert!(quote_has_complete_side(&ask_zero_no_size));
        let packed = encode_cme_quote(&ask_zero_no_size).unwrap();
        let back_half = decode_cme_quote(&packed).unwrap();
        assert_eq!(back_half.bid, one_sided.bid);
        assert_eq!(back_half.ask, MISSING_PRICE);
        assert_eq!(back_half.ask_size, MISSING_VOLUME);
        let overlaid = overlay_quote_bbo(
            &quote,
            &SlimQuote {
                ts_utc_ns: quote.ts_utc_ns + 1,
                exch_hms_ns: quote.exch_hms_ns,
                bid: parse_price_e9("0.67").unwrap(),
                bid_size: 2,
                ask: 0,
                ask_size: MISSING_VOLUME,
                ric: quote.ric.clone(),
            },
        );
        assert_eq!(overlaid.bid, parse_price_e9("0.67").unwrap());
        assert_eq!(overlaid.bid_size, 2);
        assert_eq!(overlaid.ask, quote.ask);
        assert_eq!(overlaid.ask_size, quote.ask_size);
        assert_eq!(overlaid.ts_utc_ns, quote.ts_utc_ns + 1);
    }

    #[test]
    fn packs_adf26_rename_to_historical_ric() {
        let row = SlimSymbologyChange {
            ric: "ADF26".to_string(),
            ts_utc_ns: parse_date_time_ns("2026-01-20T07:07:34.000000000Z").unwrap(),
            change_type: CHANGE_TYPE_RIC,
            old_value: "ADF26".to_string(),
            new_value: "ADF26^2".to_string(),
        };
        let bytes = encode_symbology_change(&row).unwrap();
        assert_eq!(bytes.len(), 64);
        assert_eq!(bytes[3], KIND_SYMBOLOGY_CHANGE);
        assert_eq!(bytes[28], CHANGE_TYPE_RIC);
        let back = decode_symbology_change(&bytes).unwrap();
        assert_eq!(back.old_value, "ADF26");
        assert_eq!(back.new_value, "ADF26^2");
        assert_eq!(parse_change_type("RIC").unwrap(), CHANGE_TYPE_RIC);
        assert!(parse_change_type("ISIN").is_err());
        assert!(parse_change_type("Description").is_err());
        assert!(encode_ric("ADF26^2").is_ok());
    }

    #[test]
    fn parses_aggressor_from_quoted_fid() {
        let q = r#"v[ACT_TP_1];2[LSTSALCOND];   [LIMIT_IND];"BID  "[AGGRS_SID1]"#;
        assert_eq!(parse_aggressor(q).unwrap(), AGGRESSOR_BUY);
        assert_eq!(parse_aggressor("").unwrap(), AGGRESSOR_IMPLIED);
        assert!(parse_aggressor(r#""XYZ "[AGGRS_SID1]"#).is_err());
    }

    #[test]
    fn classifies_slim_kinds_and_panics_on_unknown() {
        assert_eq!(
            classify("ALIH26", "Trade", "2999.75", "1", "BID[AGGRS_SID1]").unwrap(),
            EventKind::CmeTrade
        );
        assert_eq!(
            classify("ADF26", "Trade", "", "1", SPECIAL_TRADES_USER).unwrap(),
            EventKind::CmeSpecial
        );
        assert_eq!(
            classify("SQ24", "Trade", "", "", SPECIAL_TRADES_USER).unwrap(),
            EventKind::SpecialMissingVolume
        );
        assert_eq!(
            classify("ESH26", "Reference Change", "", "", "").unwrap(),
            EventKind::ReferenceChange
        );
        assert_eq!(
            classify("ADF26", "Quote", "", "", "").unwrap(),
            EventKind::CmeQuote
        );
        assert_eq!(
            classify(".FTXIN9", "Trade", "12024.72", "", "").unwrap(),
            EventKind::IndexPrint
        );
        assert_eq!(
            classify("ADF26", "Trade", "", "", "").unwrap(),
            EventKind::DropEmptyTrade
        );
        assert_eq!(
            classify("ADF26", "Settlement Price", "0.6673", "", "").unwrap(),
            EventKind::CmeSettlement
        );
        assert_eq!(
            classify("ADF26", "Symbology Change", "", "", "").unwrap(),
            EventKind::SymbologyChange
        );
        assert_eq!(
            classify("ESH26", "Trade", "", "20", "").unwrap(),
            EventKind::DropVolumeOnlyTrade
        );
        assert_eq!(
            classify(
                "ESH26",
                "Correction",
                "6985",
                "7",
                "2[CAN_COND_N];611[CAN_COND]"
            )
            .unwrap(),
            EventKind::CmeCorrection
        );
        assert_eq!(
            classify("JKMF27", "Trade", "", "", SETTLE_IV_USER).unwrap(),
            EventKind::DropSettleIv
        );
        assert_eq!(
            classify("KRWF6", "Auction", "1439.9", "1", "").unwrap(),
            EventKind::CmeAuction
        );
        assert!(classify("JKMF27", "Trade", "1", "", SETTLE_IV_USER).is_err());
        assert!(classify("X", "UnknownType", "", "", "").is_err());
        assert_eq!(
            classify("LCG1", "Trade", "108.325", "", "").unwrap(),
            EventKind::CmePricePrint
        );
        assert_eq!(
            overlay_tot_volume(
                classify("LCOTOT", "Trade", "", "978441", "").unwrap(),
                "LCOTOT",
                "978441",
                ""
            )
            .unwrap(),
            EventKind::CmeTotVolume
        );
        assert!(is_tot_ric("LCOTOT"));
        assert!(is_tot_ric("LCOTOT^1"));
        assert!(!is_tot_ric("LCOG24"));
        assert!(!is_tot_ric("TOT"));
    }

    #[test]
    fn packs_the_esh26_price_limit() {
        let row = SlimPriceLimit {
            ric: "ESH26".to_string(),
            ts_utc_ns: parse_date_time_ns("2026-01-02T21:01:01.323617360Z").unwrap(),
            up_lim: parse_price_e9("7379.25").unwrap(),
            lo_lim: parse_price_e9("6421.25").unwrap(),
        };
        let bytes = encode_cme_price_limit(&row).unwrap();
        assert_eq!(bytes.len(), 48);
        assert_eq!(bytes[3], KIND_CME_PRICE_LIMIT);
        let back = decode_cme_price_limit(&bytes).unwrap();
        assert_eq!(back.ric, "ESH26");
        assert_eq!(back.up_lim, 7_379_250_000_000);
        assert_eq!(back.lo_lim, 6_421_250_000_000);
        let inverted = SlimPriceLimit {
            up_lim: row.lo_lim,
            lo_lim: row.up_lim,
            ..row.clone()
        };
        assert!(encode_cme_price_limit(&inverted).is_err());
        let lo_only = SlimPriceLimit {
            ric: "KWH26".to_string(),
            ts_utc_ns: parse_date_time_ns("2026-02-25T22:39:41.455012123Z").unwrap(),
            up_lim: MISSING_PRICE,
            lo_lim: parse_price_e9("0.25").unwrap(),
        };
        let lo_bytes = encode_cme_price_limit(&lo_only).unwrap();
        let lo_back = decode_cme_price_limit(&lo_bytes).unwrap();
        assert_eq!(lo_back.ric, "KWH26");
        assert_eq!(lo_back.up_lim, MISSING_PRICE);
        assert_eq!(lo_back.lo_lim, 250_000_000);
        let up_only = SlimPriceLimit {
            up_lim: parse_price_e9("7379.25").unwrap(),
            lo_lim: MISSING_PRICE,
            ..row
        };
        let up_back = decode_cme_price_limit(&encode_cme_price_limit(&up_only).unwrap()).unwrap();
        assert_eq!(up_back.up_lim, 7_379_250_000_000);
        assert_eq!(up_back.lo_lim, MISSING_PRICE);
        let empty = SlimPriceLimit {
            up_lim: MISSING_PRICE,
            lo_lim: MISSING_PRICE,
            ..lo_only
        };
        assert!(encode_cme_price_limit(&empty).is_err());
    }

    #[test]
    fn packs_a_settlement_price() {
        let row = SlimSettlement {
            ric: "ADF26".to_string(),
            ts_utc_ns: parse_date_time_ns("2026-01-01T22:00:00.000000000Z").unwrap(),
            price: parse_price_e9("0.6673").unwrap(),
            source_date_yyyymmdd: 20260101,
        };
        let bytes = encode_cme_settlement(&row).unwrap();
        assert_eq!(bytes.len(), CME_SETTLEMENT_LEN);
        assert_eq!(&bytes[0..2], b"CT");
        assert_eq!(bytes[2], VERSION);
        assert_eq!(bytes[3], KIND_CME_SETTLEMENT);
        let back = decode_cme_settlement(&bytes).unwrap();
        assert_eq!(back.ric, "ADF26");
        assert_eq!(back.ts_utc_ns, row.ts_utc_ns);
        assert_eq!(back.price, 667_300_000);
        assert_eq!(back.source_date_yyyymmdd, 20260101);
        let date_only = SlimSettlement {
            ric: "HRCF1".to_string(),
            ts_utc_ns: parse_date_time_ns("2011-01-20T02:29:02.701847000Z").unwrap(),
            price: MISSING_PRICE,
            source_date_yyyymmdd: 20110119,
        };
        let date_only_back =
            decode_cme_settlement(&encode_cme_settlement(&date_only).unwrap()).unwrap();
        assert_eq!(date_only_back.price, MISSING_PRICE);
        assert_eq!(date_only_back.source_date_yyyymmdd, 20110119);
        let missing = SlimSettlement {
            price: MISSING_PRICE,
            source_date_yyyymmdd: 0,
            ..row
        };
        assert!(encode_cme_settlement(&missing).is_err());
    }

    #[test]
    fn packs_implied_vol_last_quote_and_settle() {
        let last = SlimImpliedVol {
            ric: "NQZ7".to_string(),
            ts_utc_ns: parse_date_time_ns("2017-12-15T21:00:00.123456789Z").unwrap(),
            exch_hms_ns: parse_exch_hms_ns("21:00:00.100000000").unwrap(),
            last_iv: parse_price_e9("0").unwrap(),
            bid_iv: MISSING_PRICE,
            ask_iv: MISSING_PRICE,
            source: IV_SOURCE_LAST,
        };
        let last_bytes = encode_cme_implied_vol(&last).unwrap();
        assert_eq!(last_bytes.len(), CME_IMPLIED_VOL_LEN);
        assert_eq!(last_bytes[3], KIND_CME_IMPLIED_VOL);
        assert_eq!(&last_bytes[61..64], &[0; 3]);
        let last_back = decode_cme_implied_vol(&last_bytes).unwrap();
        assert_eq!(last_back.ric, "NQZ7");
        assert_eq!(last_back.last_iv, 0);
        assert_eq!(last_back.bid_iv, MISSING_PRICE);
        assert_eq!(last_back.source, IV_SOURCE_LAST);

        let quote = SlimImpliedVol {
            ric: "FBTPM".to_string(),
            ts_utc_ns: parse_date_time_ns("2022-03-01T07:00:00.000000001Z").unwrap(),
            exch_hms_ns: parse_exch_hms_ns("07:00:00.000000000").unwrap(),
            last_iv: MISSING_PRICE,
            bid_iv: parse_price_e9("12.5").unwrap(),
            ask_iv: MISSING_PRICE,
            source: IV_SOURCE_QUOTE,
        };
        let quote_back = decode_cme_implied_vol(&encode_cme_implied_vol(&quote).unwrap()).unwrap();
        assert_eq!(quote_back.ric, "FBTPM");
        assert_eq!(quote_back.bid_iv, 12_500_000_000);
        assert_eq!(quote_back.ask_iv, MISSING_PRICE);
        assert_eq!(quote_back.source, IV_SOURCE_QUOTE);

        let settle = SlimImpliedVol {
            ric: "JKMF27".to_string(),
            ts_utc_ns: parse_date_time_ns("2026-01-01T00:19:01.619500177Z").unwrap(),
            exch_hms_ns: MISSING_EXCH_HMS_NS,
            last_iv: parse_price_e9("0").unwrap(),
            bid_iv: MISSING_PRICE,
            ask_iv: MISSING_PRICE,
            source: IV_SOURCE_SETTLE,
        };
        let settle_back =
            decode_cme_implied_vol(&encode_cme_implied_vol(&settle).unwrap()).unwrap();
        assert_eq!(settle_back.source, IV_SOURCE_SETTLE);
        assert_eq!(
            implied_vol_source("0", "", "", true).unwrap(),
            IV_SOURCE_SETTLE
        );
        assert_eq!(
            implied_vol_source("", "12.5", "", false).unwrap(),
            IV_SOURCE_QUOTE
        );
        assert_eq!(
            implied_vol_source("0", "", "", false).unwrap(),
            IV_SOURCE_LAST
        );
        assert!(implied_vol_source("", "", "", false).is_err());
        assert!(encode_cme_implied_vol(&SlimImpliedVol {
            last_iv: MISSING_PRICE,
            bid_iv: MISSING_PRICE,
            ask_iv: MISSING_PRICE,
            ..last
        })
        .is_err());
    }

    #[test]
    fn packs_lcotot_volume_and_lcg1_price_print() {
        let tot = SlimTotVolume {
            ric: "LCOTOT".to_string(),
            ts_utc_ns: parse_date_time_ns("2022-01-04T00:00:01.000000000Z").unwrap(),
            exch_hms_ns: parse_exch_hms_ns("00:00:01.000000000").unwrap(),
            volume: 978_441,
        };
        let tot_bytes = encode_cme_tot_volume(&tot).unwrap();
        assert_eq!(tot_bytes.len(), CME_TOT_VOLUME_LEN);
        assert_eq!(tot_bytes[3], KIND_CME_TOT_VOLUME);
        assert_eq!(&tot_bytes[44..48], &[0; 4]);
        let tot_back = decode_cme_tot_volume(&tot_bytes).unwrap();
        assert_eq!(tot_back.ric, "LCOTOT");
        assert_eq!(tot_back.volume, 978_441);
        assert_eq!(tot_volume_from_cells("978441", "").unwrap(), 978_441);
        assert_eq!(tot_volume_from_cells("", "978441").unwrap(), 978_441);
        assert_eq!(tot_volume_from_cells("978441", "978441").unwrap(), 978_441);
        assert!(tot_volume_from_cells("1", "2").is_err());
        assert!(encode_cme_tot_volume(&SlimTotVolume {
            ric: "LCOG24".to_string(),
            ..tot
        })
        .is_err());

        let print = SlimPricePrint {
            ric: "LCG1".to_string(),
            ts_utc_ns: parse_date_time_ns("2011-02-14T18:00:00.000000000Z").unwrap(),
            exch_hms_ns: parse_exch_hms_ns("18:00:00.000000000").unwrap(),
            price: parse_price_e9("108.325").unwrap(),
        };
        let print_bytes = encode_cme_price_print(&print).unwrap();
        assert_eq!(print_bytes.len(), CME_PRICE_PRINT_LEN);
        assert_eq!(print_bytes[3], KIND_CME_PRICE_PRINT);
        assert_eq!(&print_bytes[44..48], &[0; 4]);
        let print_back = decode_cme_price_print(&print_bytes).unwrap();
        assert_eq!(print_back.ric, "LCG1");
        assert_eq!(print_back.price, 108_325_000_000);
        assert!(encode_cme_price_print(&SlimPricePrint {
            price: MISSING_PRICE,
            ..print
        })
        .is_err());
    }

    #[test]
    fn packs_eth_indicative_surplus_imbalance() {
        let empty_quote = classify("ETHG26", "Quote", "", "", "").unwrap();
        assert_eq!(empty_quote, EventKind::CmeQuote);
        assert_eq!(
            overlay_imbalance(EventKind::DropEmptyQuote, "6", "B").unwrap(),
            EventKind::CmeImbalance
        );
        assert_eq!(
            overlay_imbalance(EventKind::CmeQuote, "6", "B").unwrap(),
            EventKind::CmeQuote
        );
        assert_eq!(
            overlay_imbalance(EventKind::DropEmptyQuote, "", "").unwrap(),
            EventKind::DropEmptyQuote
        );
        assert!(overlay_imbalance(EventKind::DropEmptyQuote, "6", "").is_err());
        assert!(overlay_imbalance(EventKind::CmeTrade, "6", "B").is_err());
        assert!(is_no_imbalance("0", "N").unwrap());
        assert!(!is_no_imbalance("6", "B").unwrap());
        assert!(is_no_imbalance("1", "N").is_err());
        assert_eq!(parse_imbalance_side("B").unwrap(), IMBALANCE_SIDE_BID);
        assert_eq!(parse_imbalance_side("S").unwrap(), IMBALANCE_SIDE_ASK);
        assert!(parse_imbalance_side("X").is_err());

        let rec = SlimImbalance {
            ric: "ETHG26".to_string(),
            ts_utc_ns: parse_date_time_ns("2026-01-21T14:57:35.224461965Z").unwrap(),
            exch_hms_ns: parse_exch_hms_ns("14:57:35.140000000").unwrap(),
            quantity: 6,
            side: IMBALANCE_SIDE_BID,
        };
        let bytes = encode_cme_imbalance(&rec).unwrap();
        assert_eq!(bytes.len(), CME_IMBALANCE_LEN);
        assert_eq!(bytes[3], KIND_CME_IMBALANCE);
        assert_eq!(&bytes[41..48], &[0; 7]);
        let back = decode_cme_imbalance(&bytes).unwrap();
        assert_eq!(back.ric, "ETHG26");
        assert_eq!(back.quantity, 6);
        assert_eq!(back.side, IMBALANCE_SIDE_BID);
        assert!(encode_cme_imbalance(&SlimImbalance {
            quantity: MISSING_VOLUME,
            ..rec
        })
        .is_err());
    }

    #[test]
    fn packs_auction_correction_status_and_reference_change() {
        let auction = SlimAuction {
            ric: "KRWF6".to_string(),
            ts_utc_ns: parse_date_time_ns("2026-01-02T00:30:00.173630791Z").unwrap(),
            exch_hms_ns: parse_exch_hms_ns("00:30:00.088149000").unwrap(),
            price: parse_price_e9("1439.9").unwrap(),
            volume: 1,
        };
        let auction_bytes = encode_cme_auction(&auction).unwrap();
        assert_eq!(auction_bytes.len(), CME_AUCTION_LEN);
        assert_eq!(auction_bytes[3], KIND_CME_AUCTION);
        let auction_back = decode_cme_auction(&auction_bytes).unwrap();
        assert_eq!(auction_back.ric, "KRWF6");
        assert_eq!(auction_back.price, 1_439_900_000_000);
        assert_eq!(auction_back.volume, 1);
        let auction_indication = SlimAuction {
            ric: "FBTPM4".to_string(),
            ts_utc_ns: parse_date_time_ns("2024-05-21T06:00:00.294856434Z").unwrap(),
            exch_hms_ns: MISSING_EXCH_HMS_NS,
            price: parse_price_e9("117.94").unwrap(),
            volume: MISSING_VOLUME,
        };
        let indication_back =
            decode_cme_auction(&encode_cme_auction(&auction_indication).unwrap()).unwrap();
        assert_eq!(indication_back.volume, MISSING_VOLUME);
        assert!(encode_cme_auction(&SlimAuction {
            price: MISSING_PRICE,
            ..auction
        })
        .is_err());

        let correction = SlimCorrection {
            ric: "ESH26".to_string(),
            ts_utc_ns: parse_date_time_ns("2026-01-07T09:22:58.013583911Z").unwrap(),
            exch_hms_ns: parse_exch_hms_ns("09:20:00.000000000").unwrap(),
            price: MISSING_PRICE,
            volume: MISSING_VOLUME,
            acc_volume: MISSING_VOLUME,
            original_price: parse_price_e9("6985").unwrap(),
            original_volume: 7,
            original_seq: 9960,
            qualifiers: "2[CAN_COND_N];611[CAN_COND]".to_string(),
        };
        let correction_bytes = encode_cme_correction(&correction).unwrap();
        assert_eq!(correction_bytes.len(), CME_CORRECTION_LEN);
        assert_eq!(correction_bytes[3], KIND_CME_CORRECTION);
        assert_eq!(correction_bytes[2], CME_CORRECTION_VERSION);
        assert_eq!(
            u32::from_le_bytes(correction_bytes[220..224].try_into().unwrap()),
            MISSING_VOLUME
        );
        let correction_back = decode_cme_correction(&correction_bytes).unwrap();
        assert_eq!(correction_back.ric, "ESH26");
        assert_eq!(correction_back.original_price, 6_985_000_000_000);
        assert_eq!(correction_back.original_volume, 7);
        assert_eq!(correction_back.original_seq, 9960);
        assert_eq!(correction_back.acc_volume, MISSING_VOLUME);
        assert_eq!(correction_back.qualifiers, "2[CAN_COND_N];611[CAN_COND]");
        let mut correction_v1 = correction_bytes;
        correction_v1[2] = VERSION;
        correction_v1[220..224].copy_from_slice(&[0; 4]);
        assert_eq!(
            decode_cme_correction(&correction_v1).unwrap().acc_volume,
            MISSING_VOLUME
        );
        let long_qual = SlimCorrection {
            ric: "HRCF2".to_string(),
            ts_utc_ns: parse_date_time_ns("2011-03-29T21:19:19.342213000Z").unwrap(),
            exch_hms_ns: parse_exch_hms_ns("21:19:19.000000000").unwrap(),
            price: MISSING_PRICE,
            volume: MISSING_VOLUME,
            acc_volume: MISSING_VOLUME,
            original_price: parse_price_e9("750").unwrap(),
            original_volume: MISSING_VOLUME,
            original_seq: MISSING_SEQ,
            qualifiers: "401[IRGCOND];  [OPNRNGTP];BBO[MKT_ST_IND]".to_string(),
        };
        assert_eq!(long_qual.qualifiers.len(), 41);
        let long_back = decode_cme_correction(&encode_cme_correction(&long_qual).unwrap()).unwrap();
        assert_eq!(
            long_back.qualifiers,
            "401[IRGCOND];  [OPNRNGTP];BBO[MKT_ST_IND]"
        );
        let index_correction = SlimCorrection {
            ric: ".FTXIN9".to_string(),
            ts_utc_ns: parse_date_time_ns("2010-01-04T01:30:00.098655000Z").unwrap(),
            exch_hms_ns: MISSING_EXCH_HMS_NS,
            price: parse_price_e9("12024.53").unwrap(),
            volume: MISSING_VOLUME,
            acc_volume: MISSING_VOLUME,
            original_price: MISSING_PRICE,
            original_volume: MISSING_VOLUME,
            original_seq: MISSING_SEQ,
            qualifiers: String::new(),
        };
        let index_back =
            decode_cme_correction(&encode_cme_correction(&index_correction).unwrap()).unwrap();
        assert_eq!(index_back.price, 12_024_530_000_000);
        assert!(index_back.qualifiers.is_empty());
        let acc_only = SlimCorrection {
            ric: "YAPH1".to_string(),
            ts_utc_ns: parse_date_time_ns("2010-07-05T05:34:35.895804000Z").unwrap(),
            exch_hms_ns: MISSING_EXCH_HMS_NS,
            price: MISSING_PRICE,
            volume: MISSING_VOLUME,
            acc_volume: 5,
            original_price: MISSING_PRICE,
            original_volume: MISSING_VOLUME,
            original_seq: MISSING_SEQ,
            qualifiers: String::new(),
        };
        let acc_only_back =
            decode_cme_correction(&encode_cme_correction(&acc_only).unwrap()).unwrap();
        assert_eq!(acc_only_back.acc_volume, 5);
        assert!(encode_cme_correction(&SlimCorrection {
            price: MISSING_PRICE,
            volume: MISSING_VOLUME,
            acc_volume: MISSING_VOLUME,
            original_price: MISSING_PRICE,
            original_volume: MISSING_VOLUME,
            original_seq: MISSING_SEQ,
            qualifiers: String::new(),
            ..correction
        })
        .is_err());

        let status = SlimStatus {
            ric: "BOH26".to_string(),
            ts_utc_ns: parse_date_time_ns("2026-01-02T14:30:00.000000000Z").unwrap(),
            exch_hms_ns: MISSING_EXCH_HMS_NS,
            qualifiers: "15[PERIOD_CDE];I[ORD_ENT_ST];15[PERIOD_CD2];15[TRD_TYPE];G  [STAT_IND];0[HALT_REASN];0[SECUR_ST];OQ [PRC_QL_CD];BBO[MKT_ST_IND];\"  \"[HALT_RSN]".to_string(),
        };
        assert_eq!(status.qualifiers.len(), 142);
        let status_bytes = encode_cme_status(&status).unwrap();
        assert_eq!(status_bytes.len(), CME_STATUS_LEN);
        assert_eq!(status_bytes[3], KIND_CME_STATUS);
        assert_eq!(&status_bytes[220..224], &[0; 4]);
        let status_back = decode_cme_status(&status_bytes).unwrap();
        assert_eq!(status_back.ric, "BOH26");
        assert_eq!(status_back.qualifiers, status.qualifiers);
        let mut legacy_status = [0u8; CME_STATUS_LEGACY_LEN];
        legacy_status.copy_from_slice(&status_bytes[..CME_STATUS_LEGACY_LEN]);
        assert_eq!(
            decode_cme_status(&legacy_status).unwrap().qualifiers,
            status.qualifiers
        );
        let long_status = SlimStatus {
            qualifiers: "X".repeat(175),
            ..status.clone()
        };
        assert_eq!(
            decode_cme_status(&encode_cme_status(&long_status).unwrap())
                .unwrap()
                .qualifiers,
            long_status.qualifiers
        );
        assert!(encode_cme_status(&SlimStatus {
            qualifiers: String::new(),
            ..status
        })
        .is_err());

        let rename = SlimReferenceChange {
            ric: "ESH26".to_string(),
            ts_utc_ns: parse_date_time_ns("2024-04-10T17:51:51.574327205Z").unwrap(),
            change_type: CHANGE_TYPE_DESCRIPTION,
            old_value: "EMINI S&P MAR6".to_string(),
            new_value: "EMINI S&P MAR26".to_string(),
        };
        assert_eq!(
            parse_reference_change_type("Description").unwrap(),
            CHANGE_TYPE_DESCRIPTION
        );
        assert_eq!(
            parse_reference_change_type("Expiry Date").unwrap(),
            CHANGE_TYPE_EXPIRY_DATE
        );
        assert_eq!(
            parse_reference_change_type("Currency").unwrap(),
            CHANGE_TYPE_CURRENCY
        );
        assert_eq!(
            parse_reference_change_type("Permission Code").unwrap(),
            CHANGE_TYPE_PERMISSION_CODE
        );
        assert_eq!(
            parse_reference_change_type("Option Type").unwrap(),
            CHANGE_TYPE_OPTION_TYPE
        );
        assert_eq!(
            parse_reference_change_type("Template").unwrap(),
            CHANGE_TYPE_TEMPLATE
        );
        assert_eq!(
            parse_reference_change_type("Exchange").unwrap(),
            CHANGE_TYPE_EXCHANGE
        );
        assert_eq!(
            parse_reference_change_type("Bond Type").unwrap(),
            CHANGE_TYPE_BOND_TYPE
        );
        assert_eq!(
            parse_reference_change_type("Record Type").unwrap(),
            CHANGE_TYPE_RECORD_TYPE
        );
        assert_eq!(
            parse_reference_change_type("Rating").unwrap(),
            CHANGE_TYPE_RATING
        );
        assert_eq!(
            parse_reference_change_type("Rating ID").unwrap(),
            CHANGE_TYPE_RATING_ID
        );
        assert!(parse_reference_change_type("RIC").is_err());
        let rename_bytes = encode_reference_change(&rename).unwrap();
        assert_eq!(rename_bytes.len(), REFERENCE_CHANGE_LEN);
        assert_eq!(rename_bytes[3], KIND_REFERENCE_CHANGE);
        assert_eq!(rename_bytes[28], CHANGE_TYPE_DESCRIPTION);
        assert_eq!(&rename_bytes[93..96], &[0; 3]);
        let rename_back = decode_reference_change(&rename_bytes).unwrap();
        assert_eq!(rename_back.old_value, "EMINI S&P MAR6");
        assert_eq!(rename_back.new_value, "EMINI S&P MAR26");
        let expiry = SlimReferenceChange {
            ric: "HRCJ1".to_string(),
            ts_utc_ns: parse_date_time_ns("2011-02-28T10:47:01.126986000Z").unwrap(),
            change_type: CHANGE_TYPE_EXPIRY_DATE,
            old_value: "2011-04-25T00:00:00.000000000Z".to_string(),
            new_value: "2011-04-26T00:00:00.000000000Z".to_string(),
        };
        assert_eq!(expiry.old_value.len(), 30);
        let expiry_back =
            decode_reference_change(&encode_reference_change(&expiry).unwrap()).unwrap();
        assert_eq!(expiry_back.change_type, CHANGE_TYPE_EXPIRY_DATE);
        assert_eq!(expiry_back.old_value, expiry.old_value);
        assert_eq!(expiry_back.new_value, expiry.new_value);
        let currency = SlimReferenceChange {
            ric: "LCJ4".to_string(),
            ts_utc_ns: parse_date_time_ns("2012-11-01T00:13:33.904707000Z").unwrap(),
            change_type: CHANGE_TYPE_CURRENCY,
            old_value: "841".to_string(),
            new_value: "2009".to_string(),
        };
        let currency_back =
            decode_reference_change(&encode_reference_change(&currency).unwrap()).unwrap();
        assert_eq!(currency_back.change_type, CHANGE_TYPE_CURRENCY);
        let permission = SlimReferenceChange {
            ric: "NQH1".to_string(),
            ts_utc_ns: parse_date_time_ns("2010-01-01T11:10:01.986036000Z").unwrap(),
            change_type: CHANGE_TYPE_PERMISSION_CODE,
            old_value: "5370".to_string(),
            new_value: "120".to_string(),
        };
        let permission_back =
            decode_reference_change(&encode_reference_change(&permission).unwrap()).unwrap();
        assert_eq!(permission_back.change_type, CHANGE_TYPE_PERMISSION_CODE);
        let option_type = SlimReferenceChange {
            ric: "CCU2".to_string(),
            ts_utc_ns: parse_date_time_ns("2010-09-30T23:46:22.465445000Z").unwrap(),
            change_type: CHANGE_TYPE_OPTION_TYPE,
            old_value: String::new(),
            new_value: "2".to_string(),
        };
        let option_type_back =
            decode_reference_change(&encode_reference_change(&option_type).unwrap()).unwrap();
        assert_eq!(option_type_back.change_type, CHANGE_TYPE_OPTION_TYPE);
        assert!(option_type_back.old_value.is_empty());
        assert_eq!(option_type_back.new_value, "2");
        let removal = SlimReferenceChange {
            ric: "Cc5".to_string(),
            ts_utc_ns: parse_date_time_ns("2010-03-13T23:11:24.454460000Z").unwrap(),
            change_type: CHANGE_TYPE_RECORD_TYPE,
            old_value: "194".to_string(),
            new_value: String::new(),
        };
        let removal_back =
            decode_reference_change(&encode_reference_change(&removal).unwrap()).unwrap();
        assert_eq!(removal_back.change_type, CHANGE_TYPE_RECORD_TYPE);
        assert_eq!(removal_back.old_value, "194");
        assert!(removal_back.new_value.is_empty());
        assert!(encode_reference_change(&SlimReferenceChange {
            old_value: String::new(),
            new_value: String::new(),
            ..removal
        })
        .is_err());
        assert!(encode_reference_change(&SlimReferenceChange {
            change_type: CHANGE_TYPE_RIC,
            ..rename
        })
        .is_err());
    }

    #[test]
    fn price_scale_matches_doc() {
        assert_eq!(parse_price_e9("2999.75").unwrap(), 2_999_750_000_000);
        assert_eq!(parse_price_e9("0.66825").unwrap(), 668_250_000);
        assert!(parse_price_e9("1.1234567891").is_err());
    }

    #[test]
    fn limit_price_keeps_decimal_cages_and_rescues_pre_scaled_integers() {
        assert_eq!(
            parse_limit_price_e9("7379.25").unwrap(),
            parse_price_e9("7379.25").unwrap()
        );
        assert_eq!(parse_limit_price_e9("0.25").unwrap(), 250_000_000);
        assert_eq!(parse_limit_price_e9("").unwrap(), MISSING_PRICE);
        assert_eq!(
            parse_limit_price_e9("11317500000").unwrap(),
            11_317_500_000_000
        );
        assert_eq!(
            parse_limit_price_e9("51930000000").unwrap(),
            51_930_000_000_000
        );
        assert_eq!(
            parse_limit_price_e9("-11317500000").unwrap(),
            -11_317_500_000_000
        );
        assert!(parse_price_e9("11317500000").is_err());
        assert!(parse_limit_price_e9("not-a-price").is_err());
    }

    #[test]
    fn key_orders_same_ric_by_time_then_part_then_seq() {
        let a = encode_key("ALIH26", 10, 0, 0).unwrap();
        let b = encode_key("ALIH26", 10, 0, 1).unwrap();
        let c = encode_key("ALIH26", 10, 1, 0).unwrap();
        let d = encode_key("ALIH26", 11, 0, 0).unwrap();
        assert!(a < b);
        assert!(b < c);
        assert!(c < d);
        assert_eq!(key_ts_utc_ns(&c).unwrap(), 10);
        assert_eq!(key_part(&c).unwrap(), 1);
        assert_eq!(KEY_LEN, 30);
    }

    #[test]
    fn chicago_1700_rolls_the_session() {
        assert_eq!(
            tradeday_yyyymmdd(parse_date_time_ns("2026-01-01T22:59:59Z").unwrap()).unwrap(),
            20260101
        );
        assert_eq!(
            tradeday_yyyymmdd(parse_date_time_ns("2026-01-01T23:00:00Z").unwrap()).unwrap(),
            20260102
        );
        assert_eq!(
            tradeday_yyyymmdd(parse_date_time_ns("2026-01-02T15:39:23.298829985Z").unwrap())
                .unwrap(),
            20260102
        );
    }

    fn trade(ts: &str, price: &str, volume: u32) -> SlimTrade {
        SlimTrade {
            ric: "ALIH26".to_string(),
            ts_utc_ns: parse_date_time_ns(ts).unwrap(),
            exch_hms_ns: MISSING_EXCH_HMS_NS,
            price: parse_price_e9(price).unwrap(),
            volume,
            bid: MISSING_PRICE,
            bid_size: MISSING_VOLUME,
            ask: MISSING_PRICE,
            ask_size: MISSING_VOLUME,
            aggressor: 0,
        }
    }

    #[test]
    fn ali_window_matches_summary_ohlc() {
        let trades = [
            trade("2026-01-02T15:39:23.298829985Z", "2999.75", 1),
            trade("2026-01-02T15:39:23.693860830Z", "2997.25", 2),
            trade("2026-01-02T15:39:23.693860830Z", "2996.75", 1),
            trade("2026-01-02T15:39:23.693860830Z", "2996.5", 9),
            trade("2026-01-02T15:39:23.693860830Z", "2996.75", 1),
            trade("2026-01-02T15:39:23.694057845Z", "2996.5", 1),
            trade("2026-01-02T15:39:23.699016188Z", "2996.5", 1),
            trade("2026-01-02T15:39:23.699016188Z", "2996", 1),
            trade("2026-01-02T15:39:23.699016188Z", "2995.75", 1),
            trade("2026-01-02T15:39:23.699016188Z", "2995.5", 1),
            trade("2026-01-02T15:39:23.699016188Z", "2995.25", 2),
            trade("2026-01-02T15:39:23.699016188Z", "2995.25", 1),
            trade("2026-01-02T15:39:23.938967443Z", "2995.5", 1),
            trade("2026-01-02T15:39:23.938967443Z", "2995.5", 1),
            trade("2026-01-02T15:44:27.769278153Z", "2998", 1),
        ];
        let bars = synthesize_1min_bars(&trades);
        assert_eq!(bars.len(), 2);
        assert_eq!(
            bars[0].minute_utc_ns,
            parse_date_time_ns("2026-01-02T15:39:00Z").unwrap()
        );
        assert_eq!(bars[0].open, parse_price_e9("2999.75").unwrap());
        assert_eq!(bars[0].high, parse_price_e9("2999.75").unwrap());
        assert_eq!(bars[0].low, parse_price_e9("2995.25").unwrap());
        assert_eq!(bars[0].last, parse_price_e9("2995.5").unwrap());
        assert_eq!(bars[0].volume, 24);
        assert_eq!(bars[0].no_trades, 14);
        assert_eq!(bars[1].volume, 1);
        assert_eq!(bars[1].no_trades, 1);
        let summary = SynthBar {
            ric: "ALIH26".into(),
            minute_utc_ns: bars[0].minute_utc_ns,
            open: bars[0].open,
            high: bars[0].high,
            low: bars[0].low,
            last: bars[0].last,
            volume: 24,
            no_trades: 14,
        };
        assert_eq!(
            compare_synth_to_summary(&bars[0], &summary).verdict,
            CompareVerdict::Exact
        );
        assert_eq!(
            compare_priced_minute(&bars[0], &summary, 0).verdict,
            CompareVerdict::Exact
        );
    }

    #[test]
    fn priced_minute_volume_explained_by_special_or_leftover_fails() {
        let trades = [
            trade("2026-01-02T15:39:23.298829985Z", "2999.75", 1),
            trade("2026-01-02T15:39:23.693860830Z", "2997.25", 2),
            trade("2026-01-02T15:39:23.693860830Z", "2996.75", 1),
            trade("2026-01-02T15:39:23.693860830Z", "2996.5", 9),
            trade("2026-01-02T15:39:23.693860830Z", "2996.75", 1),
            trade("2026-01-02T15:39:23.694057845Z", "2996.5", 1),
            trade("2026-01-02T15:39:23.699016188Z", "2996.5", 1),
            trade("2026-01-02T15:39:23.699016188Z", "2996", 1),
            trade("2026-01-02T15:39:23.699016188Z", "2995.75", 1),
            trade("2026-01-02T15:39:23.699016188Z", "2995.5", 1),
            trade("2026-01-02T15:39:23.699016188Z", "2995.25", 2),
            trade("2026-01-02T15:39:23.699016188Z", "2995.25", 1),
            trade("2026-01-02T15:39:23.938967443Z", "2995.5", 1),
            trade("2026-01-02T15:39:23.938967443Z", "2995.5", 1),
        ];
        let bars = synthesize_1min_bars(&trades);
        assert_eq!(bars[0].volume, 24);
        let mut summary = bars[0].clone();
        summary.volume = 38;
        summary.no_trades = 99;
        assert_eq!(
            compare_priced_minute(&bars[0], &summary, 14).verdict,
            CompareVerdict::Approximate
        );

        let leftover_volume = {
            let mut bar = summary.clone();
            bar.volume = 40;
            compare_priced_minute(&bars[0], &bar, 14)
        };
        assert_eq!(leftover_volume.verdict, CompareVerdict::Mismatch);
        assert!(leftover_volume.deltas.iter().any(|d| d.name == "Volume"));

        let leftover_ohlc = {
            let mut bar = bars[0].clone();
            bar.last = parse_price_e9("1").unwrap();
            compare_priced_minute(&bars[0], &bar, 0)
        };
        assert_eq!(leftover_ohlc.verdict, CompareVerdict::Mismatch);
        assert!(leftover_ohlc.deltas.iter().any(|d| d.name == "Last"));
    }

    fn special(ts: &str, volume: u32) -> SlimTrade {
        SlimTrade {
            ric: "ALIH26".to_string(),
            ts_utc_ns: parse_date_time_ns(ts).unwrap(),
            exch_hms_ns: MISSING_EXCH_HMS_NS,
            price: MISSING_PRICE,
            volume,
            bid: MISSING_PRICE,
            bid_size: MISSING_VOLUME,
            ask: MISSING_PRICE,
            ask_size: MISSING_VOLUME,
            aggressor: 0,
        }
    }

    #[test]
    fn trade_and_special_synth_keeps_ohlc_on_prints_and_volume_on_specials() {
        let trades = [
            trade("2026-01-02T15:39:23.298829985Z", "2999.75", 1),
            trade("2026-01-02T15:39:23.693860830Z", "2997.25", 2),
        ];
        let specials = [
            special("2026-01-02T15:39:40.000000000Z", 14),
            special("2026-01-02T10:22:03.633592953Z", 100),
        ];
        let minutes = synthesize_1min_from_trade_and_special(&trades, &specials);
        assert_eq!(minutes.len(), 2);
        assert_eq!(
            minutes[0].minute_utc_ns,
            parse_date_time_ns("2026-01-02T10:22:00Z").unwrap()
        );
        assert!(!minutes[0].priced());
        assert_eq!(minutes[0].volume, 0);
        assert_eq!(minutes[0].special_volume, 100);
        assert_eq!(minutes[0].special_count, 1);
        assert_eq!(minutes[0].volume_total(), 100);

        assert_eq!(
            minutes[1].minute_utc_ns,
            parse_date_time_ns("2026-01-02T15:39:00Z").unwrap()
        );
        assert_eq!(minutes[1].open, parse_price_e9("2999.75").unwrap());
        assert_eq!(minutes[1].last, parse_price_e9("2997.25").unwrap());
        assert_eq!(minutes[1].volume, 3);
        assert_eq!(minutes[1].no_trades, 2);
        assert_eq!(minutes[1].special_volume, 14);
        assert_eq!(minutes[1].volume_total(), 17);
        let summary = SynthBar {
            ric: "ALIH26".into(),
            minute_utc_ns: minutes[1].minute_utc_ns,
            open: minutes[1].open,
            high: minutes[1].high,
            low: minutes[1].low,
            last: minutes[1].last,
            volume: 17,
            no_trades: 99,
        };
        assert_eq!(
            compare_priced_minute(
                &minutes[1].as_trade_bar(),
                &summary,
                minutes[1].special_volume
            )
            .verdict,
            CompareVerdict::Approximate
        );
    }

    #[test]
    fn parquet_round_trip_keeps_null_ohlc_and_special_volume() {
        let trades = [trade("2026-01-02T15:39:23.298829985Z", "2999.75", 1)];
        let specials = [special("2026-01-02T10:22:03.633592953Z", 100)];
        let minutes = synthesize_1min_from_trade_and_special(&trades, &specials);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("synth_1min.parquet");
        write_synth_minutes_parquet(&path, &minutes).unwrap();
        use polars::prelude::{ParquetReader, SerReader};
        let file = std::fs::File::open(&path).unwrap();
        let df = ParquetReader::new(file).finish().unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(
            df.get_column_names()
                .into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
            vec![
                "ric".to_string(),
                "ts".to_string(),
                "ts_utc_ns".to_string(),
                "date_time".to_string(),
                "open".to_string(),
                "high".to_string(),
                "low".to_string(),
                "close".to_string(),
                "volume".to_string(),
                "count".to_string(),
                "special_volume".to_string(),
                "special_count".to_string(),
                "volume_total".to_string(),
            ]
        );
        let special_only = df
            .column("special_volume")
            .unwrap()
            .i64()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(special_only, 100);
        assert!(df.column("open").unwrap().f64().unwrap().get(0).is_none());
        let priced_close = df.column("close").unwrap().f64().unwrap().get(1).unwrap();
        assert!((priced_close - 2999.75).abs() < 1e-12);
        assert_eq!(
            df.column("volume").unwrap().i64().unwrap().get(1).unwrap(),
            1
        );
        assert_eq!(
            df.column("volume_total")
                .unwrap()
                .i64()
                .unwrap()
                .get(1)
                .unwrap(),
            1
        );
    }

    #[test]
    fn futures_session_change_fields_are_not_forbidden() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../preprocess/lseg/tas_column_rules.json");
        let rules = ColumnRules::load(&path).unwrap();
        for name in ["Percentage Change", "Net Change", "Percentage Daily Return"] {
            let group = rules.group_of(name).unwrap();
            assert_eq!(group, "session", "{name}");
            assert!(!rules.is_forbidden_futures_group(group), "{name}");
        }
        assert!(rules.is_forbidden_futures_group("index_breadth"));
        assert!(rules.is_forbidden_futures_group("theoretical"));
        assert!(rules.is_allowed_price_limit_column("UpLim Price"));
        assert!(rules.is_allowed_price_limit_column("LoLim Price"));
        assert!(!rules.is_allowed_price_limit_column("Theo. Price"));
        assert!(rules.is_allowed_settle_iv_column("Imp. Vol."));
        assert!(!rules.is_allowed_settle_iv_column("Bid Imp. Vol"));
        assert!(!rules.is_allowed_settle_iv_column("Delta"));
        assert!(rules.is_allowed_implied_yield_column("Implied Yield"));
        assert!(!rules.is_allowed_implied_yield_column("Bid Yld"));
        assert!(!rules.is_allowed_implied_yield_column("Yield"));
        assert!(!rules.is_allowed_implied_yield_column("Ask Yld"));
    }

    #[test]
    fn parked_crash_rows_are_classified_and_not_printable() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../preprocess/lseg/tas_column_rules.json");
        let rules = ColumnRules::load(&path).unwrap();

        let settle_iv = classify("JKMF27", "Trade", "", "", SETTLE_IV_USER).unwrap();
        assert_eq!(settle_iv, EventKind::DropSettleIv);
        assert_ne!(settle_iv, EventKind::CmeTrade);
        assert!(rules.is_allowed_settle_iv_column("Imp. Vol."));
        assert!(!rules.is_allowed_settle_iv_column("Bid Imp. Vol"));
        assert!(classify("JKMF27", "Trade", "1", "", SETTLE_IV_USER).is_err());

        let volume_only = classify("ESH26", "Trade", "", "20", "").unwrap();
        assert_eq!(volume_only, EventKind::DropVolumeOnlyTrade);
        assert_ne!(volume_only, EventKind::CmeTrade);

        let special_missing_volume =
            classify("SQ24", "Trade", "", "", SPECIAL_TRADES_USER).unwrap();
        assert_eq!(special_missing_volume, EventKind::SpecialMissingVolume);
        assert_ne!(special_missing_volume, EventKind::CmeSpecial);
        assert_ne!(special_missing_volume, EventKind::CmeTrade);

        let reference_change = classify("ESH26", "Reference Change", "", "", "").unwrap();
        assert_eq!(reference_change, EventKind::ReferenceChange);
        assert_ne!(reference_change, EventKind::SymbologyChange);

        let empty_trade = classify("ESH26", "Trade", "", "", "").unwrap();
        assert_eq!(empty_trade, EventKind::DropEmptyTrade);
        assert_ne!(empty_trade, EventKind::CmeTrade);

        let printable = classify("ALIH26", "Trade", "2999.75", "1", "BID[AGGRS_SID1]").unwrap();
        assert_eq!(printable, EventKind::CmeTrade);
        assert_ne!(printable, EventKind::DropSettleIv);
        assert_ne!(printable, EventKind::CmeAuction);
        assert!(rules.is_allowed_settle_iv_column("Imp. Vol."));
        assert!(!rules.is_allowed_price_limit_column("Imp. Vol."));

        let auction = classify("KRWF6", "Auction", "1439.9", "1", "").unwrap();
        assert_eq!(auction, EventKind::CmeAuction);
        assert_ne!(auction, EventKind::CmeTrade);

        let one_sided_base = classify("KWH26", "Trade", "", "", "").unwrap();
        assert_eq!(one_sided_base, EventKind::DropEmptyTrade);
        assert_eq!(
            overlay_price_limit(one_sided_base, "", "0.25").unwrap(),
            EventKind::CmePriceLimit
        );
        assert_eq!(
            overlay_price_limit(one_sided_base, "7379.25", "").unwrap(),
            EventKind::CmePriceLimit
        );
        assert_eq!(
            overlay_price_limit(one_sided_base, "7379.25", "6421.25").unwrap(),
            EventKind::CmePriceLimit
        );
        assert_eq!(
            overlay_price_limit(one_sided_base, "", "").unwrap(),
            EventKind::DropEmptyTrade
        );
        assert_ne!(
            overlay_price_limit(one_sided_base, "", "0.25").unwrap(),
            EventKind::CmeTrade
        );

        assert!(rules.is_allowed_implied_yield_column("Implied Yield"));
        assert!(!rules.is_allowed_implied_yield_column("Bid Yld"));
        assert!(!rules.is_allowed_implied_yield_column("Yield"));
        assert!(!rules.is_forbidden_futures_group("session"));
        assert!(rules.is_forbidden_futures_group("yield"));
        let yield_quote = classify("SRAU27", "Quote", "", "", "").unwrap();
        assert_eq!(yield_quote, EventKind::CmeQuote);
        assert_ne!(yield_quote, EventKind::CmeTrade);
        let cage_quote = classify("NGN25", "Quote", "", "", "").unwrap();
        assert_eq!(cage_quote, EventKind::CmeQuote);
        assert!(rules.is_allowed_price_limit_column("LoLim Price"));
        assert!(rules.is_allowed_price_limit_column("UpLim Price"));
        assert!(rules.is_allowed_imbalance_column("Imbalance Quantity"));
        assert!(rules.is_allowed_imbalance_column("Imbalance Side"));
        assert!(!rules.is_allowed_imbalance_column("Paired Quantity"));
        assert!(rules.is_forbidden_futures_group("auction"));
    }

    #[test]
    fn period_watermark_round_trips() {
        let key = period_meta_key("2026-01-01_2026-06-01").unwrap();
        assert_eq!(key, b"period:2026-01-01_2026-06-01");
        assert_eq!(
            decode_period_status(&encode_period_status(PeriodStatus::Writing)).unwrap(),
            PeriodStatus::Writing
        );
        assert_eq!(
            decode_period_status(&encode_period_status(PeriodStatus::Done)).unwrap(),
            PeriodStatus::Done
        );
        assert!(validate_period("").is_err());
        assert!(validate_period("2026/H1").is_err());
        assert!(decode_period_status(b"ok").is_err());
    }
}
