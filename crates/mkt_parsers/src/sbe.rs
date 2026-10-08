//! Borrowed, fixed-point market data. Conversion is deferred until after deduplication.

#[derive(Debug, Clone, Copy)]
pub struct Bbo<'a> {
    pub symbol: &'a str,
    pub timestamp_us: i64,
    pub seq_id: i64,
    pub price_exponent: i8,
    pub size_exponent: i8,
    pub bid_price: i64,
    pub bid_amount: i64,
    pub ask_price: i64,
    pub ask_amount: i64,
}

impl Bbo<'_> {
    /// Bid price/size, ask price/size. Invalid quotes must not advance sequence state.
    pub fn prices(self) -> Option<[f64; 4]> {
        if self.bid_price <= 0
            || self.bid_amount <= 0
            || self.ask_price <= 0
            || self.ask_amount <= 0
        {
            return None;
        }
        let price_scale = 10_f64.powi(self.price_exponent as i32);
        let size_scale = 10_f64.powi(self.size_exponent as i32);
        let values = [
            self.bid_price as f64 * price_scale,
            self.bid_amount as f64 * size_scale,
            self.ask_price as f64 * price_scale,
            self.ask_amount as f64 * size_scale,
        ];
        values
            .iter()
            .all(|v| v.is_finite() && *v > 0.0)
            .then_some(values)
    }
}
