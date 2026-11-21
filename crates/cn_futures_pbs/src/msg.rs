use iceoryx2::prelude::ZeroCopySend;

pub const VENUES: [&str; 6] = ["CFFEX", "SHFE", "INE", "CZCE", "DCE", "GFEX"];
pub use cn_futures_core::mdl::UNKNOWN_QUANTITY;
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

// MDL field parsing lives in cn_futures_core (shared with market_replay).
pub use cn_futures_core::mdl::{day, number, timestamp, unsigned, ParseError};
