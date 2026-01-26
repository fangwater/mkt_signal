use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::common::TradingLeg;
use crate::signal::common::{bytes_helper, SignalBytes};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Generic arbitrage open signal context
#[derive(Debug, Clone, Copy)]
pub struct ArbOpenCtx {
    /// Opening leg (active leg)
    pub opening_leg: TradingLeg,

    /// Opening leg symbol - using fixed size array to avoid heap allocation
    pub opening_symbol: [u8; 32], // 32 bytes should be enough for symbol

    /// Hedging leg (passive leg)
    pub hedging_leg: TradingLeg,

    /// Hedging leg symbol
    pub hedging_symbol: [u8; 32],

    /// Trade amount
    pub amount: f32,

    /// Trade side (for opening leg) - stored as u8
    pub side: u8,

    /// Order type - stored as u8
    pub order_type: u8,

    /// Opening price
    pub price: f64,

    /// Price tick size / minimum price movement
    pub price_tick: f64,

    /// Order expiration time (microseconds)
    pub exp_time: i64,

    /// Creation timestamp (microseconds)
    pub create_ts: i64,

    /// Price offset from best bid/ask for limit order placement
    pub price_offset: f64,

    /// Hedge timeout (microseconds)
    pub hedge_timeout_us: i64,

    /// Funding rate moving average (futures specific, 0 means none)
    pub funding_ma: f64,

    /// Predicted funding rate (futures specific, 0 means none)
    pub predicted_funding_rate: f64,

    /// Loan rate (margin/leverage specific, 0 means none)
    pub loan_rate: f64,
}

impl ArbOpenCtx {
    /// Create new arbitrage open context
    pub fn new() -> Self {
        Self {
            opening_leg: TradingLeg {
                venue: 0,
                bid0: 0.0,
                ask0: 0.0,
                ts: 0,
            },
            opening_symbol: [0u8; 32],
            hedging_leg: TradingLeg {
                venue: 0,
                bid0: 0.0,
                ask0: 0.0,
                ts: 0,
            },
            hedging_symbol: [0u8; 32],
            amount: 0.0,
            side: 0,
            order_type: 0,
            price: 0.0,
            price_tick: 0.0,
            exp_time: 0,
            create_ts: 0,
            price_offset: 0.0,
            hedge_timeout_us: 0,
            funding_ma: 0.0,
            predicted_funding_rate: 0.0,
            loan_rate: 0.0,
        }
    }

    /// Set opening leg symbol
    pub fn set_opening_symbol(&mut self, symbol: &str) {
        let bytes = symbol.as_bytes();
        let len = bytes.len().min(32);
        self.opening_symbol[..len].copy_from_slice(&bytes[..len]);
    }

    /// Get opening leg symbol
    pub fn get_opening_symbol(&self) -> String {
        let end = self
            .opening_symbol
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(32);
        String::from_utf8_lossy(&self.opening_symbol[..end]).to_string()
    }

    /// Set hedging leg symbol
    pub fn set_hedging_symbol(&mut self, symbol: &str) {
        let bytes = symbol.as_bytes();
        let len = bytes.len().min(32);
        self.hedging_symbol[..len].copy_from_slice(&bytes[..len]);
    }

    /// Get hedging leg symbol
    pub fn get_hedging_symbol(&self) -> String {
        let end = self
            .hedging_symbol
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(32);
        String::from_utf8_lossy(&self.hedging_symbol[..end]).to_string()
    }

    /// Get Side enum
    pub fn get_side(&self) -> Option<Side> {
        Side::from_u8(self.side)
    }

    /// Set Side
    pub fn set_side(&mut self, side: Side) {
        self.side = side.to_u8();
    }

    /// Get OrderType enum
    pub fn get_order_type(&self) -> Option<OrderType> {
        OrderType::from_u8(self.order_type)
    }

    /// Set OrderType
    pub fn set_order_type(&mut self, order_type: OrderType) {
        self.order_type = order_type.to_u8();
    }
}

impl SignalBytes for ArbOpenCtx {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Opening leg
        buf.put_u8(self.opening_leg.venue);
        buf.put_f64_le(self.opening_leg.bid0);
        buf.put_f64_le(self.opening_leg.ask0);
        buf.put_i64_le(self.opening_leg.ts);
        bytes_helper::write_fixed_bytes(&mut buf, &self.opening_symbol);

        // Hedging leg
        buf.put_u8(self.hedging_leg.venue);
        buf.put_f64_le(self.hedging_leg.bid0);
        buf.put_f64_le(self.hedging_leg.ask0);
        buf.put_i64_le(self.hedging_leg.ts);
        bytes_helper::write_fixed_bytes(&mut buf, &self.hedging_symbol);

        // Trade parameters
        buf.put_f32_le(self.amount);
        buf.put_u8(self.side);
        buf.put_u8(self.order_type);
        buf.put_f64_le(self.price);
        buf.put_f64_le(self.price_tick);
        buf.put_i64_le(self.exp_time);
        buf.put_i64_le(self.create_ts);
        buf.put_f64_le(self.price_offset);
        buf.put_i64_le(self.hedge_timeout_us);

        // Optional fields (using 0.0 for None)
        buf.put_f64_le(self.funding_ma);
        buf.put_f64_le(self.predicted_funding_rate);
        buf.put_f64_le(self.loan_rate);

        buf.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, String> {
        const BASE_TAIL_LEN: usize = 4 + 1 + 1 + 8 + 8 + 8 + 8;
        const EXTRA_FIELDS_MAX: usize = 5;

        fn detect_format(bytes: &Bytes, base_offset: usize) -> Option<(bool, usize)> {
            let total = bytes.len();
            let mut with_ts_match = None;
            let mut without_ts_match = None;

            for with_ts in [true, false] {
                let (open_len_idx, hedge_len_base, total_base) = if with_ts {
                    (base_offset + 25, base_offset + 51, base_offset + 52)
                } else {
                    (base_offset + 17, base_offset + 35, base_offset + 36)
                };

                if total <= open_len_idx {
                    continue;
                }
                let open_len = bytes[open_len_idx] as usize;
                if open_len > 32 {
                    continue;
                }
                let hedge_len_idx = hedge_len_base + open_len;
                if total <= hedge_len_idx {
                    continue;
                }
                let hedge_len = bytes[hedge_len_idx] as usize;
                if hedge_len > 32 {
                    continue;
                }

                for extra_fields in 0..=EXTRA_FIELDS_MAX {
                    let tail_len = BASE_TAIL_LEN + extra_fields * 8;
                    let expected = total_base + open_len + hedge_len + tail_len;
                    if total == expected {
                        if with_ts {
                            with_ts_match = Some(extra_fields);
                        } else {
                            without_ts_match = Some(extra_fields);
                        }
                        break;
                    }
                }
            }

            match (with_ts_match, without_ts_match) {
                (Some(extra), None) => Some((true, extra)),
                (None, Some(extra)) => Some((false, extra)),
                (Some(extra), Some(_)) => Some((true, extra)),
                (None, None) => None,
            }
        }

        fn parse(
            mut bytes: Bytes,
            with_ts: bool,
            extra_fields: Option<usize>,
        ) -> Result<ArbOpenCtx, String> {
            // Opening leg
            if bytes.remaining() < 1 + 8 + 8 {
                return Err("Not enough bytes for opening leg".to_string());
            }
            let opening_venue = bytes.get_u8();
            let opening_bid0 = bytes.get_f64_le();
            let opening_ask0 = bytes.get_f64_le();
            let opening_ts = if with_ts {
                if bytes.remaining() < 8 {
                    return Err("Not enough bytes for opening leg ts".to_string());
                }
                bytes.get_i64_le()
            } else {
                0
            };
            let opening_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;

            // Hedging leg
            if bytes.remaining() < 1 + 8 + 8 {
                return Err("Not enough bytes for hedging leg".to_string());
            }
            let hedging_venue = bytes.get_u8();
            let hedging_bid0 = bytes.get_f64_le();
            let hedging_ask0 = bytes.get_f64_le();
            let hedging_ts = if with_ts {
                if bytes.remaining() < 8 {
                    return Err("Not enough bytes for hedging leg ts".to_string());
                }
                bytes.get_i64_le()
            } else {
                0
            };
            let hedging_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;

            // Trade parameters
            if bytes.remaining() < BASE_TAIL_LEN {
                return Err("Not enough bytes for trade parameters".to_string());
            }
            let amount = bytes.get_f32_le();
            let side = bytes.get_u8();
            let order_type = bytes.get_u8();
            let price = bytes.get_f64_le();
            let price_tick = bytes.get_f64_le();
            let exp_time = bytes.get_i64_le();
            let create_ts = bytes.get_i64_le();
            let remaining = bytes.remaining();
            let extra_fields = match extra_fields {
                Some(fields) => {
                    if remaining != fields * 8 {
                        return Err("Unexpected tail length for ArbOpenCtx".to_string());
                    }
                    fields
                }
                None => {
                    if remaining % 8 != 0 {
                        return Err("Invalid tail length for ArbOpenCtx".to_string());
                    }
                    let fields = remaining / 8;
                    if fields > EXTRA_FIELDS_MAX {
                        return Err("Too many tail fields for ArbOpenCtx".to_string());
                    }
                    fields
                }
            };

            let mut price_offset = 0.0;
            let mut hedge_timeout_us = 0;
            let mut funding_ma = 0.0;
            let mut predicted_funding_rate = 0.0;
            let mut loan_rate = 0.0;

            if extra_fields >= 1 {
                price_offset = bytes.get_f64_le();
            }
            if extra_fields >= 2 {
                hedge_timeout_us = bytes.get_i64_le();
            }
            if extra_fields >= 3 {
                funding_ma = bytes.get_f64_le();
            }
            if extra_fields >= 4 {
                predicted_funding_rate = bytes.get_f64_le();
            }
            if extra_fields >= 5 {
                loan_rate = bytes.get_f64_le();
            }
            if bytes.remaining() != 0 {
                return Err("Unexpected trailing bytes for ArbOpenCtx".to_string());
            }

            Ok(ArbOpenCtx {
                opening_leg: TradingLeg {
                    venue: opening_venue,
                    bid0: opening_bid0,
                    ask0: opening_ask0,
                    ts: opening_ts,
                },
                opening_symbol,
                hedging_leg: TradingLeg {
                    venue: hedging_venue,
                    bid0: hedging_bid0,
                    ask0: hedging_ask0,
                    ts: hedging_ts,
                },
                hedging_symbol,
                amount,
                side,
                order_type,
                price,
                price_tick,
                exp_time,
                create_ts,
                price_offset,
                hedge_timeout_us,
                funding_ma,
                predicted_funding_rate,
                loan_rate,
            })
        }

        let format = detect_format(&bytes, 0);
        match format {
            Some((with_ts, extra_fields)) => parse(bytes, with_ts, Some(extra_fields)),
            None => parse(bytes.clone(), true, None).or_else(|_| parse(bytes, false, None)),
        }
    }
}
