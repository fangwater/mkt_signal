const EPS: f64 = 1e-12;

use std::fmt;

const QUANTIZED_DECIMAL_CAPACITY: usize = 96;

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

    pub fn is_zero(&self) -> bool {
        self.count == 0 || self.tick_i64 == 0
    }

    pub fn get_tick_parts(&self) -> (i64, i32) {
        (self.tick_i64, self.tick_exp)
    }

    pub fn set_count_floor_from_val(&mut self, value: f64) {
        self.count = count_from_value_floor(value, self.tick_i64, self.tick_exp);
    }

    pub fn decimal_string(&self) -> String {
        let mut out = String::new();
        self.write_decimal_to(&mut out)
            .expect("write to String cannot fail");
        out
    }

    pub fn write_decimal_to<W: fmt::Write + ?Sized>(&self, out: &mut W) -> fmt::Result {
        if self.count == 0 || self.tick_i64 == 0 {
            out.write_char('0')?;
            return Ok(());
        }

        if (self.count < 0) ^ (self.tick_i64 < 0) {
            out.write_char('-')?;
        }
        let abs_count = (self.count as i128).abs();
        let abs_tick = (self.tick_i64 as i128).abs();
        let scaled = (abs_count * abs_tick) as u128;
        let mut digits = [0u8; 40];
        let digits = decimal_digits(scaled, &mut digits);

        if self.tick_exp >= 0 {
            write_ascii_digits(out, digits)?;
            for _ in 0..self.tick_exp {
                out.write_char('0')?;
            }
            return Ok(());
        }

        let scale = (-self.tick_exp) as usize;
        if digits.len() <= scale {
            out.write_str("0.")?;
            for _ in 0..scale - digits.len() {
                out.write_char('0')?;
            }
            write_ascii_digits(out, digits)?;
            return Ok(());
        }

        let split = digits.len() - scale;
        write_ascii_digits(out, &digits[..split])?;
        out.write_char('.')?;
        write_ascii_digits(out, &digits[split..])?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct QuantizedDecimal {
    len: usize,
    buf: [u8; QUANTIZED_DECIMAL_CAPACITY],
}

impl QuantizedDecimal {
    pub fn try_from_value(value: QuantizedValue) -> Option<Self> {
        let mut out = Self {
            len: 0,
            buf: [0; QUANTIZED_DECIMAL_CAPACITY],
        };
        value.write_decimal_to(&mut out).ok()?;
        Some(out)
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.buf[..self.len]).expect("quantized decimal is ascii")
    }
}

impl fmt::Write for QuantizedDecimal {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let next = self.len.checked_add(s.len()).ok_or(fmt::Error)?;
        if next > QUANTIZED_DECIMAL_CAPACITY {
            return Err(fmt::Error);
        }
        self.buf[self.len..next].copy_from_slice(s.as_bytes());
        self.len = next;
        Ok(())
    }
}

fn decimal_digits(mut value: u128, buf: &mut [u8; 40]) -> &[u8] {
    let mut idx = buf.len();
    if value == 0 {
        idx -= 1;
        buf[idx] = b'0';
        return &buf[idx..];
    }
    while value > 0 {
        idx -= 1;
        buf[idx] = b'0' + (value % 10) as u8;
        value /= 10;
    }
    &buf[idx..]
}

fn write_ascii_digits<W: fmt::Write + ?Sized>(out: &mut W, digits: &[u8]) -> fmt::Result {
    let digits = std::str::from_utf8(digits).expect("decimal digits are ascii");
    out.write_str(digits)
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

pub fn count_from_value_ceil(value: f64, tick_i64: i64, tick_exp: i32) -> i64 {
    if !value.is_finite() || value <= 0.0 || tick_i64 <= 0 {
        return 0;
    }
    let tick = value_from_int_exp(tick_i64, tick_exp);
    if !tick.is_finite() || tick <= 0.0 {
        return 0;
    }
    ((value / tick) - EPS).ceil() as i64
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

#[cfg(test)]
mod tests {
    use super::{QuantizedDecimal, QuantizedValue};

    #[test]
    fn quantized_decimal_inline_matches_decimal_string() {
        let cases = [
            (QuantizedValue::from_parts(0, 0, 10), "0"),
            (QuantizedValue::from_parts(1, -3, 10), "0.010"),
            (QuantizedValue::from_parts(12345, -2, 1), "123.45"),
            (QuantizedValue::from_parts(1, 2, 123), "12300"),
            (QuantizedValue::from_parts(-1, -4, 25), "-0.0025"),
            (QuantizedValue::from_parts(1, -4, -25), "-0.0025"),
        ];

        for (value, expected) in cases {
            let mut written = String::new();
            value
                .write_decimal_to(&mut written)
                .expect("write decimal to String");
            let inline = QuantizedDecimal::try_from_value(value).expect("inline decimal");

            assert_eq!(written, expected);
            assert_eq!(inline.as_str(), expected);
            assert_eq!(value.decimal_string(), expected);
        }
    }
}
