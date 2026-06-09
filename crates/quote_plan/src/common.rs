use order_common::TradingVenue;

const EPS: f64 = 1e-12;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Venue {
    BinanceMargin,
    BinanceFutures,
    OkexMargin,
    OkexFutures,
    BybitMargin,
    BybitFutures,
    BitgetMargin,
    BitgetFutures,
    GateMargin,
    GateFutures,
    AsterMargin,
    AsterFutures,
    HyperliquidMargin,
    HyperliquidFutures,
}


impl From<TradingVenue> for Venue {
    fn from(value: TradingVenue) -> Self {
        match value {
            TradingVenue::BinanceMargin => Venue::BinanceMargin,
            TradingVenue::BinanceFutures => Venue::BinanceFutures,
            TradingVenue::OkexMargin => Venue::OkexMargin,
            TradingVenue::OkexFutures => Venue::OkexFutures,
            TradingVenue::BybitMargin => Venue::BybitMargin,
            TradingVenue::BybitFutures => Venue::BybitFutures,
            TradingVenue::BitgetMargin => Venue::BitgetMargin,
            TradingVenue::BitgetFutures => Venue::BitgetFutures,
            TradingVenue::GateMargin => Venue::GateMargin,
            TradingVenue::GateFutures => Venue::GateFutures,
            TradingVenue::AsterMargin => Venue::AsterMargin,
            TradingVenue::AsterFutures => Venue::AsterFutures,
            TradingVenue::HyperliquidMargin => Venue::HyperliquidMargin,
            TradingVenue::HyperliquidFutures => Venue::HyperliquidFutures,
        }
    }
}

impl From<Venue> for TradingVenue {
    fn from(value: Venue) -> Self {
        match value {
            Venue::BinanceMargin => TradingVenue::BinanceMargin,
            Venue::BinanceFutures => TradingVenue::BinanceFutures,
            Venue::OkexMargin => TradingVenue::OkexMargin,
            Venue::OkexFutures => TradingVenue::OkexFutures,
            Venue::BybitMargin => TradingVenue::BybitMargin,
            Venue::BybitFutures => TradingVenue::BybitFutures,
            Venue::BitgetMargin => TradingVenue::BitgetMargin,
            Venue::BitgetFutures => TradingVenue::BitgetFutures,
            Venue::GateMargin => TradingVenue::GateMargin,
            Venue::GateFutures => TradingVenue::GateFutures,
            Venue::AsterMargin => TradingVenue::AsterMargin,
            Venue::AsterFutures => TradingVenue::AsterFutures,
            Venue::HyperliquidMargin => TradingVenue::HyperliquidMargin,
            Venue::HyperliquidFutures => TradingVenue::HyperliquidFutures,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Quote {
    pub bid: f64,
    pub ask: f64,
    pub ts: i64,
}

impl Quote {
    pub fn update(&mut self, bid: f64, ask: f64, ts: i64) {
        self.bid = bid;
        self.ask = ask;
        self.ts = ts;
    }

    pub fn is_valid(&self) -> bool {
        self.bid > 0.0 && self.ask > 0.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuantizedValue {
    tick_i64: i64,
    tick_exp: i32,
    count: i64,
}

impl QuantizedValue {
    pub fn zero() -> Self {
        Self {
            tick_i64: 0,
            tick_exp: 0,
            count: 0,
        }
    }

    pub fn from_parts(tick_i64: i64, tick_exp: i32, count: i64) -> Self {
        Self {
            tick_i64,
            tick_exp,
            count,
        }
    }

    pub fn from_decimal(value: f64) -> Option<Self> {
        let (value_i64, value_exp) = integerize_decimal(value)?;
        Some(Self {
            tick_i64: value_i64,
            tick_exp: value_exp,
            count: 1,
        })
    }

    pub fn encode_floor(value: f64, preferred_tick: f64) -> Option<Self> {
        encode_quantized_floor(value, preferred_tick)
    }

    pub fn get_val(&self) -> f64 {
        value_from_tick_count(self.tick_i64, self.tick_exp, self.count)
    }

    pub fn get_count(&self) -> i64 {
        self.count
    }

    pub fn get_tick_parts(&self) -> (i64, i32) {
        (self.tick_i64, self.tick_exp)
    }

    pub fn set_count_floor_from_val(&mut self, value: f64) {
        self.count = count_from_value_floor(value, self.tick_i64, self.tick_exp);
    }

    pub fn decimal_string(&self) -> String {
        if self.count == 0 || self.tick_i64 == 0 {
            return "0".to_string();
        }

        let sign = if (self.count < 0) ^ (self.tick_i64 < 0) {
            "-"
        } else {
            ""
        };
        let abs_count = (self.count as i128).abs();
        let abs_tick = (self.tick_i64 as i128).abs();
        let scaled = abs_count * abs_tick;

        if self.tick_exp >= 0 {
            let mut out = scaled.to_string();
            for _ in 0..self.tick_exp {
                out.push('0');
            }
            return format!("{sign}{out}");
        }

        let scale = (-self.tick_exp) as usize;
        let digits = scaled.to_string();
        if digits.len() <= scale {
            let zeros = "0".repeat(scale - digits.len());
            return format!("{sign}0.{zeros}{digits}");
        }

        let split = digits.len() - scale;
        format!("{sign}{}.{}", &digits[..split], &digits[split..])
    }
}

pub trait MinQtyLookup {
    fn min_qty(&self, symbol: &str) -> Option<f64>;
    fn step_size(&self, symbol: &str) -> Option<f64>;
    fn price_tick(&self, symbol: &str) -> Option<f64>;
    fn min_notional(&self, symbol: &str) -> Option<f64>;
    fn contract_multiplier_opt(&self, symbol: &str) -> Option<f64>;
}

pub fn is_futures_venue(venue: Venue) -> bool {
    matches!(
        venue,
        Venue::BinanceFutures
            | Venue::OkexFutures
            | Venue::BybitFutures
            | Venue::BitgetFutures
            | Venue::GateFutures
            | Venue::AsterFutures
            | Venue::HyperliquidFutures
    )
}

pub fn venue_qty_is_contracts(venue: Venue) -> bool {
    matches!(
        venue,
        Venue::BinanceFutures | Venue::OkexFutures | Venue::GateFutures
    )
}

pub fn normalize_symbol_for_internal(symbol: &str) -> String {
    let mut out = String::with_capacity(symbol.len());
    for ch in symbol.trim().chars() {
        if matches!(ch, '-' | '_' | '/') {
            continue;
        }
        for upper in ch.to_uppercase() {
            out.push(upper);
        }
    }
    if out.ends_with("SWAP") {
        out.truncate(out.len().saturating_sub("SWAP".len()));
    }
    out
}

pub fn normalize_symbol_for_venue(symbol: &str, venue: Venue) -> String {
    let symbol_upper = normalize_symbol_for_internal(symbol);
    match venue {
        Venue::OkexMargin => {
            let (base, quote) = extract_assets_from_internal_symbol(&symbol_upper);
            format!("{}-{}", base, quote)
        }
        Venue::OkexFutures => {
            let (base, quote) = extract_assets_from_internal_symbol(&symbol_upper);
            format!("{}-{}-SWAP", base, quote)
        }
        Venue::BinanceMargin | Venue::BinanceFutures => symbol_upper,
        _ => symbol_upper,
    }
}

pub fn min_qty_symbol_key(venue: Venue, symbol: &str) -> String {
    match venue {
        Venue::OkexMargin | Venue::OkexFutures => {
            symbol.to_uppercase().replace("-SWAP", "").replace('-', "")
        }
        Venue::GateMargin | Venue::GateFutures => symbol.to_uppercase().replace(['_', '-'], ""),
        _ => symbol.to_uppercase(),
    }
}

pub fn align_price_floor(price: f64, tick: f64) -> f64 {
    if tick <= 0.0 || !price.is_finite() || !tick.is_finite() {
        return price;
    }
    (price / tick).floor() * tick
}

pub fn align_price_ceil(price: f64, tick: f64) -> f64 {
    if tick <= 0.0 || !price.is_finite() || !tick.is_finite() {
        return price;
    }
    (price / tick).ceil() * tick
}

pub fn build_decision_from_key_base(
    now_us: i64,
    return_qtl: Option<f64>,
    return_threshold: Option<f64>,
    volatility: Option<f64>,
    env_score: Option<f64>,
    env_threshold: Option<f64>,
) -> String {
    let return_qtl_text = return_qtl
        .filter(|v| v.is_finite())
        .map(|v| format!("{v:.8}"))
        .unwrap_or_else(|| "0".to_string());
    let return_threshold_text = return_threshold
        .filter(|v| v.is_finite())
        .map(|v| format!("{v:.8}"))
        .unwrap_or_else(|| "0".to_string());
    let volatility_text = volatility
        .filter(|v| v.is_finite())
        .map(|v| format!("{v:.8}"))
        .unwrap_or_else(|| "0".to_string());
    let env_score_text = env_score
        .filter(|v| v.is_finite())
        .map(|v| format!("{v:.8}"))
        .unwrap_or_else(|| "0".to_string());
    let env_threshold_text = env_threshold
        .filter(|v| v.is_finite())
        .map(|v| format!("{v:.8}"))
        .unwrap_or_else(|| "0".to_string());
    format!(
        "{now_us}:ret_qtl={return_qtl_text}:ret_thr={return_threshold_text}:vol={volatility_text}:env_score={env_score_text}:env_thr={env_threshold_text}"
    )
}

fn extract_assets_from_internal_symbol(symbol_upper: &str) -> (&str, &str) {
    const QUOTE_ASSETS: [&str; 7] = ["USDT", "USDC", "BUSD", "FDUSD", "BIDR", "TRY", "USD"];
    for quote in QUOTE_ASSETS {
        if symbol_upper.ends_with(quote) && symbol_upper.len() > quote.len() {
            let base = &symbol_upper[..symbol_upper.len() - quote.len()];
            return (base, quote);
        }
    }
    (symbol_upper, "USDT")
}

fn value_from_int_exp(value_i64: i64, value_exp: i32) -> f64 {
    (value_i64 as f64) * 10f64.powi(value_exp)
}

fn value_from_tick_count(tick_i64: i64, tick_exp: i32, count: i64) -> f64 {
    value_from_int_exp(tick_i64, tick_exp) * (count as f64)
}

pub fn integerize_decimal(value: f64) -> Option<(i64, i32)> {
    if !value.is_finite() || value <= 0.0 {
        return None;
    }
    let mut exp: i32 = 0;
    let mut scaled = value;
    for _ in 0..12 {
        let rounded = scaled.round();
        if (scaled - rounded).abs() < 1e-9 {
            let mut int_value = rounded as i64;
            let mut int_exp = exp;
            while int_exp < 0 && int_value % 10 == 0 {
                int_value /= 10;
                int_exp += 1;
            }
            if int_value > 0 {
                return Some((int_value, int_exp));
            }
            return None;
        }
        scaled *= 10.0;
        exp -= 1;
    }
    None
}

pub fn count_from_value_floor(value: f64, tick_i64: i64, tick_exp: i32) -> i64 {
    if !value.is_finite() || value <= 0.0 || tick_i64 <= 0 {
        return 0;
    }
    let tick = value_from_int_exp(tick_i64, tick_exp);
    if !tick.is_finite() || tick <= 0.0 {
        return 0;
    }
    ((value / tick) + EPS).floor() as i64
}

fn encode_quantized_floor(value: f64, preferred_tick: f64) -> Option<QuantizedValue> {
    if !value.is_finite() || value <= 0.0 {
        return None;
    }
    let tick = if preferred_tick.is_finite() && preferred_tick > 0.0 {
        preferred_tick
    } else {
        value
    };
    let (tick_i64, tick_exp) = integerize_decimal(tick)?;
    let count = count_from_value_floor(value, tick_i64, tick_exp).max(1);
    Some(QuantizedValue {
        tick_i64,
        tick_exp,
        count,
    })
}
