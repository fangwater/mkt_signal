//! Slim CME TAS codec: classify a source row and pack printable trades.

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Datelike, NaiveTime, Timelike};
use chrono_tz::America::Chicago;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub const CME_TRADE_LEN: usize = 80;
pub const CME_QUOTE_LEN: usize = 64;
pub const SYMBOLOGY_CHANGE_LEN: usize = 64;
pub const CME_PRICE_LIMIT_LEN: usize = 48;
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
pub const CHANGE_TYPE_RIC: u8 = 1;
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
pub const CF_REPLAY_META: &str = "replay_meta";
pub const PERIOD_META_PREFIX: &str = "period:";
pub const PERIOD_STATUS_WRITING: &str = "writing";
pub const PERIOD_STATUS_DONE: &str = "done";
pub const PRICE_LIMIT_COLUMNS: &[&str] = &["UpLim Price", "LoLim Price"];
pub const SETTLE_IV_COLUMNS: &[&str] = &["Imp. Vol."];
pub const IMPLIED_YIELD_COLUMNS: &[&str] = &["Implied Yield"];

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

/// Research routing: 51 product roots on CBOT / CME / COMEX / NYMEX.
/// Same set as the exchange pages and `ric_period_contract_map.csv`.
/// Longer roots first so `SM` / `US` / `CD` win over `S` / `U` / `C`.
pub const RESEARCH_PRODUCT_ROOTS: &[&str] = &[
    "NOKA", "WTCL", "ALI", "BTC", "ETH", "HRC", "JKM", "KRW", "MEM", "PLZ", "RTY", "S1R", "SEK",
    "SRA", "URO", "AD", "BO", "BP", "BR", "CD", "CL", "ES", "FC", "FF", "FV", "GC", "HG", "HO",
    "JY", "KW", "LC", "LH", "MP", "NE", "NG", "NQ", "PA", "PL", "RB", "SF", "SI", "SM", "TN", "TU",
    "TY", "US", "YM", "C", "S", "U", "W",
];

/// Live `#RIC` stem, dropping a Reuters historical suffix (`ADF26^2` → `ADF26`).
pub fn ric_live_stem(ric: &str) -> &str {
    ric.split_once('^').map(|(stem, _)| stem).unwrap_or(ric)
}

fn matches_research_root(stem: &str, root: &str) -> bool {
    let Some(rest) = stem.strip_prefix(root) else {
        return false;
    };
    let bytes = rest.as_bytes();
    if bytes.len() < 2 || bytes.len() > 3 {
        return false;
    }
    if !MONTH_CODES.contains(&bytes[0]) {
        return false;
    }
    rest[1..].bytes().all(|b| b.is_ascii_digit())
}

/// The unique research root for a fixed-expiry RIC, or `None` if unmapped.
/// Two roots matching the same RIC is a panic: the 51-root set must stay unambiguous.
pub fn research_root_of(ric: &str) -> Result<Option<&'static str>> {
    let stem = ric_live_stem(ric);
    if stem.is_empty() || !stem.is_ascii() {
        return Ok(None);
    }
    let mut found: Option<&'static str> = None;
    for root in RESEARCH_PRODUCT_ROOTS {
        if matches_research_root(stem, root) {
            if let Some(prev) = found {
                bail!("RIC {ric} matches both research roots {prev} and {root}");
            }
            found = Some(*root);
        }
    }
    Ok(found)
}

pub fn is_research_ric(ric: &str) -> Result<bool> {
    Ok(research_root_of(ric)?.is_some())
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
    DropEmptyTrade,
    DropEmptyQuote,
    DropVolumeOnlyTrade,
    DropSpecialNoVolume,
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
            Self::DropEmptyTrade => "drop_empty_trade",
            Self::DropEmptyQuote => "drop_empty_quote",
            Self::DropVolumeOnlyTrade => "drop_volume_only_trade",
            Self::DropSpecialNoVolume => "drop_special_no_volume",
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
        let known: BTreeSet<&str> = ["Trade", "Quote", "Mkt. Condition", "Correction"]
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

    pub fn is_allowed_implied_yield_column(&self, name: &str) -> bool {
        IMPLIED_YIELD_COLUMNS.contains(&name)
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
        None | Some("") => Ok(0),
        Some("BID") => Ok(1),
        Some("ASK") => Ok(2),
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
                    return Ok(EventKind::DropSpecialNoVolume);
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
                (true, false) => bail!("unhandled trade_price_only for {ric}"),
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

fn encode_fixed(trade: &SlimTrade, kind: u8) -> Result<[u8; CME_TRADE_LEN]> {
    if trade.aggressor > 2 {
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
    if trade.aggressor != 0 {
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
    if aggressor > 2 {
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

fn side_complete(price: i64, size: u32) -> Result<bool> {
    match (price == MISSING_PRICE, size == MISSING_VOLUME) {
        (false, false) => Ok(true),
        (true, true) => Ok(false),
        (false, true) => bail!("quote side has price but missing size"),
        (true, false) => bail!("quote side has size but missing price"),
    }
}

pub fn quote_has_complete_side(quote: &SlimQuote) -> Result<bool> {
    let bid_ok = side_complete(quote.bid, quote.bid_size)?;
    let ask_ok = side_complete(quote.ask, quote.ask_size)?;
    Ok(bid_ok || ask_ok)
}

pub fn encode_cme_quote(quote: &SlimQuote) -> Result<[u8; CME_QUOTE_LEN]> {
    if !quote_has_complete_side(quote)? {
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
    let quote = SlimQuote {
        ric: decode_ric(&buf[4..20])?,
        ts_utc_ns: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        exch_hms_ns: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
        bid: i64::from_le_bytes(buf[36..44].try_into().unwrap()),
        bid_size: u32::from_le_bytes(buf[44..48].try_into().unwrap()),
        ask: i64::from_le_bytes(buf[48..56].try_into().unwrap()),
        ask_size: u32::from_le_bytes(buf[56..60].try_into().unwrap()),
    };
    if !quote_has_complete_side(&quote)? {
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
        bail!("price limit {} missing both UpLim Price and LoLim Price", row.ric);
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
    use std::collections::BTreeSet;

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
            ..quote
        };
        assert!(!quote_has_complete_side(&empty).unwrap());
        assert!(encode_cme_quote(&empty).is_err());
        let half = SlimQuote {
            bid: parse_price_e9("0.6671").unwrap(),
            bid_size: MISSING_VOLUME,
            ask: MISSING_PRICE,
            ask_size: MISSING_VOLUME,
            ..one_sided
        };
        assert!(quote_has_complete_side(&half).is_err());
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
        assert!(encode_ric("ADF26^2").is_ok());
    }

    #[test]
    fn parses_aggressor_from_quoted_fid() {
        let q = r#"v[ACT_TP_1];2[LSTSALCOND];   [LIMIT_IND];"BID  "[AGGRS_SID1]"#;
        assert_eq!(parse_aggressor(q).unwrap(), 1);
        assert_eq!(parse_aggressor("").unwrap(), 0);
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
            EventKind::DropSpecialNoVolume
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
            classify("ESH26", "Correction", "6985", "7", "2[CAN_COND_N];611[CAN_COND]").unwrap(),
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
        assert!(classify("X", "Trade", "1", "", "").is_err());
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
    fn price_scale_matches_doc() {
        assert_eq!(parse_price_e9("2999.75").unwrap(), 2_999_750_000_000);
        assert_eq!(parse_price_e9("0.66825").unwrap(), 668_250_000);
        assert!(parse_price_e9("1.1234567891").is_err());
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

        let special_no_volume = classify("SQ24", "Trade", "", "", SPECIAL_TRADES_USER).unwrap();
        assert_eq!(special_no_volume, EventKind::DropSpecialNoVolume);
        assert_ne!(special_no_volume, EventKind::CmeSpecial);
        assert_ne!(special_no_volume, EventKind::CmeTrade);

        let reference_change = classify("ESH26", "Reference Change", "", "", "").unwrap();
        assert_eq!(reference_change, EventKind::ReferenceChange);
        assert_ne!(reference_change, EventKind::SymbologyChange);

        let empty_trade = classify("ESH26", "Trade", "", "", "").unwrap();
        assert_eq!(empty_trade, EventKind::DropEmptyTrade);
        assert_ne!(empty_trade, EventKind::CmeTrade);

        let printable = classify(
            "ALIH26",
            "Trade",
            "2999.75",
            "1",
            "BID[AGGRS_SID1]",
        )
        .unwrap();
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
    }

    #[test]
    fn research_roots_are_the_documented_51() {
        let mut unique = BTreeSet::new();
        for root in RESEARCH_PRODUCT_ROOTS {
            assert!(unique.insert(*root), "duplicate research root {root}");
        }
        assert_eq!(RESEARCH_PRODUCT_ROOTS.len(), 51);
        let expected: BTreeSet<&str> = [
            "AD", "ALI", "BO", "BP", "BR", "BTC", "C", "CD", "CL", "ES", "ETH", "FC", "FF", "FV",
            "GC", "HG", "HO", "HRC", "JKM", "JY", "KRW", "KW", "LC", "LH", "MEM", "MP", "NE", "NG",
            "NOKA", "NQ", "PA", "PL", "PLZ", "RB", "RTY", "S", "S1R", "SEK", "SF", "SI", "SM",
            "SRA", "TN", "TU", "TY", "U", "URO", "US", "W", "WTCL", "YM",
        ]
        .into_iter()
        .collect();
        assert_eq!(unique, expected);
    }

    #[test]
    fn research_root_matches_fixed_expiry_and_rejects_lookalikes() {
        assert_eq!(research_root_of("ADF26").unwrap(), Some("AD"));
        assert_eq!(research_root_of("ADF26^2").unwrap(), Some("AD"));
        assert_eq!(research_root_of("CH24").unwrap(), Some("C"));
        assert_eq!(research_root_of("CDH24").unwrap(), Some("CD"));
        assert_eq!(research_root_of("CLG24").unwrap(), Some("CL"));
        assert_eq!(research_root_of("SMH24").unwrap(), Some("SM"));
        assert_eq!(research_root_of("SF0").unwrap(), Some("S"));
        assert_eq!(research_root_of("SFH0").unwrap(), Some("SF"));
        assert_eq!(research_root_of("USH0").unwrap(), Some("US"));
        assert_eq!(research_root_of("UM4").unwrap(), Some("U"));
        assert_eq!(research_root_of("WTCLZ6").unwrap(), Some("WTCL"));
        assert_eq!(research_root_of("NOKAH0").unwrap(), Some("NOKA"));
        assert_eq!(research_root_of("S1RK8").unwrap(), Some("S1R"));
        assert_eq!(research_root_of("ALIH26").unwrap(), Some("ALI"));
        assert_eq!(research_root_of("YMH24").unwrap(), Some("YM"));
        assert_eq!(research_root_of("SILH24").unwrap(), None);
        assert_eq!(research_root_of("NGLNDG1324").unwrap(), None);
        assert_eq!(research_root_of("LCOG24").unwrap(), None);
        assert_eq!(research_root_of("VXH24").unwrap(), None);
        assert_eq!(research_root_of(".NSEI").unwrap(), None);
        assert!(is_research_ric("ESH24").unwrap());
        assert!(!is_research_ric("NGLNDG1324").unwrap());
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
