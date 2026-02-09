const EPS: f64 = 1e-12;

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
