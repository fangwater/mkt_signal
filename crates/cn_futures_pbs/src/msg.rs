use chrono::NaiveDate;
use iceoryx2::prelude::ZeroCopySend;

pub const VENUES: [&str; 6] = ["CFFEX", "SHFE", "INE", "CZCE", "DCE", "GFEX"];
pub const UNKNOWN_QUANTITY: u64 = u64::MAX;
/// A source continuity boundary. This is not a quote and has an empty symbol.
pub const SOURCE_RESET: u64 = 1;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct Level {
    pub price: f64,
    pub quantity: u64,
}
unsafe impl ZeroCopySend for Level {}

/// Current shared IPC contract for all domestic-futures environments. Sizes
/// remain source lots; cash turnover is unchanged. NaN means an absent float.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct MarketDataMsg {
    pub publisher_epoch: u64,
    pub source_generation: u64,
    pub received_us: u64,
    pub exchange_ts_us: i64,
    pub source_sequence: u64,
    /// Monotonic per IPC venue stream, including reset markers.
    pub sequence: u64,
    pub last_price: f64,
    pub volume: u64,
    pub turnover: f64,
    pub open_interest: f64,
    pub bid: [Level; 5],
    pub ask: [Level; 5],
    pub symbol: [u8; 64],
    pub venue: [u8; 8],
    pub trading_day: u32,
    pub action_day: u32,
    pub flags: u64,
}
unsafe impl ZeroCopySend for MarketDataMsg {}

impl Default for MarketDataMsg {
    fn default() -> Self {
        Self {
            publisher_epoch: 0,
            source_generation: 0,
            received_us: 0,
            exchange_ts_us: 0,
            source_sequence: UNKNOWN_QUANTITY,
            sequence: 0,
            last_price: f64::NAN,
            volume: UNKNOWN_QUANTITY,
            turnover: f64::NAN,
            open_interest: f64::NAN,
            bid: [Level {
                price: f64::NAN,
                quantity: UNKNOWN_QUANTITY,
            }; 5],
            ask: [Level {
                price: f64::NAN,
                quantity: UNKNOWN_QUANTITY,
            }; 5],
            symbol: [0; 64],
            venue: [0; 8],
            trading_day: 0,
            action_day: 0,
            flags: 0,
        }
    }
}
impl MarketDataMsg {
    pub fn reset(generation: u64) -> Self {
        Self {
            source_generation: generation,
            received_us: crate::now_us(),
            flags: SOURCE_RESET,
            ..Self::default()
        }
    }
    pub fn symbol(&self) -> &[u8] {
        &self.symbol[..self
            .symbol
            .iter()
            .position(|b| *b == 0)
            .unwrap_or(self.symbol.len())]
    }
    pub fn venue(&self) -> &[u8] {
        &self.venue[..self
            .venue
            .iter()
            .position(|b| *b == 0)
            .unwrap_or(self.venue.len())]
    }
    pub fn set_symbol(&mut self, s: &[u8]) -> Result<(), ParseError> {
        if s.is_empty() || s.len() >= self.symbol.len() || !s.iter().all(u8::is_ascii_alphanumeric)
        {
            return Err(ParseError("invalid native contract"));
        }
        self.symbol.fill(0);
        self.symbol[..s.len()].copy_from_slice(s);
        Ok(())
    }
    pub fn set_venue(&mut self, venue: &str) -> Result<(), ParseError> {
        if !VENUES.contains(&venue) {
            return Err(ParseError("unsupported domestic venue"));
        }
        self.venue.fill(0);
        self.venue[..venue.len()].copy_from_slice(venue.as_bytes());
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseError(pub &'static str);
impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for ParseError {}

pub fn unsigned(s: &[u8]) -> Result<u64, ParseError> {
    if s.is_empty() {
        return Ok(UNKNOWN_QUANTITY);
    }
    let mut n = 0u64;
    for b in s {
        if !b.is_ascii_digit() {
            return Err(ParseError("invalid unsigned quantity"));
        }
        n = n
            .checked_mul(10)
            .and_then(|n| n.checked_add((b - b'0') as u64))
            .ok_or(ParseError("quantity overflow"))?;
    }
    Ok(n)
}
pub fn number(s: &[u8]) -> Result<f64, ParseError> {
    if s.is_empty() {
        return Ok(f64::NAN);
    }
    let x = std::str::from_utf8(s)
        .map_err(|_| ParseError("non-ASCII number"))?
        .parse::<f64>()
        .map_err(|_| ParseError("invalid decimal"))?;
    if !x.is_finite() {
        return Err(ParseError("nonfinite decimal"));
    }
    Ok(if x.abs() >= 1e100 { f64::NAN } else { x })
}
pub fn day(s: &[u8]) -> Result<u32, ParseError> {
    if s.is_empty() {
        return Ok(0);
    }
    let mut digits = [0u8; 8];
    match s.len() {
        8 => digits.copy_from_slice(s),
        10 if s[4] == b'-' && s[7] == b'-' => {
            digits[..4].copy_from_slice(&s[..4]);
            digits[4..6].copy_from_slice(&s[5..7]);
            digits[6..].copy_from_slice(&s[8..]);
        }
        _ => return Err(ParseError("invalid date format")),
    }
    let value = unsigned(&digits)? as u32;
    NaiveDate::from_ymd_opt((value / 10000) as i32, (value / 100) % 100, value % 100)
        .ok_or(ParseError("invalid date"))?;
    Ok(value)
}
/// ActionDay is the calendar date. TradDay must never supply the night-session date.
pub fn timestamp(action_day: u32, time: &[u8], extra_millis: u32) -> Result<i64, ParseError> {
    if action_day == 0 || time.is_empty() {
        return Ok(0);
    }
    if !matches!(time.len(), 8 | 12) || time[2] != b':' || time[5] != b':' || extra_millis >= 1000 {
        return Err(ParseError("invalid quote time"));
    }
    let h = unsigned(&time[..2])?;
    let m = unsigned(&time[3..5])?;
    let s = unsigned(&time[6..8])?;
    let millis = if time.len() == 12 {
        if time[8] != b'.' || extra_millis != 0 {
            return Err(ParseError("ambiguous quote milliseconds"));
        }
        unsigned(&time[9..])? as u32
    } else {
        extra_millis
    };
    let d = NaiveDate::from_ymd_opt(
        (action_day / 10000) as i32,
        (action_day / 100) % 100,
        action_day % 100,
    )
    .ok_or(ParseError("invalid action day"))?;
    let dt = d
        .and_hms_milli_opt(h as u32, m as u32, s as u32, millis)
        .ok_or(ParseError("invalid quote time"))?;
    Ok(dt.and_utc().timestamp_micros() - 8 * 3600 * 1_000_000)
}
