//! Offline completion of the 31 formulas absent from the live Crypto dispatcher.
//! Formulas follow final_factor_pool_update20260123.py and the reviewed CN
//! trade-only implementations. No depth adaptation is used here.
use std::collections::VecDeque;

pub const NAMES: &[&str] = &[
    "baseline_023",
    "baseline_030",
    "baseline_033",
    "baseline_037",
    "baseline_041",
    "baseline_044",
    "baseline_048",
    "baseline_050",
    "baseline_056",
    "baseline_064",
    "baseline_075",
    "baseline_078",
    "baseline_084",
    "baseline_089",
    "baseline_094",
    "baseline_095",
    "baseline_097",
    "baseline_102",
    "baseline_106",
    "baseline_108",
    "baseline_130",
    "baseline_142",
    "baseline_144",
    "baseline_147",
    "baseline_150",
    "baseline_155",
    "baseline_165",
    "baseline_176",
    "baseline_183",
    "baseline_186",
    "baseline_197",
];
struct Sum {
    values: VecDeque<f64>,
    cap: usize,
    sum: f64,
    bad: usize,
}
impl Sum {
    fn new(cap: usize) -> Self {
        Self {
            values: VecDeque::new(),
            cap,
            sum: 0.,
            bad: 0,
        }
    }
    fn push(&mut self, v: f64) {
        if self.values.len() == self.cap {
            let old = self.values.pop_front().unwrap();
            if old.is_finite() {
                self.sum -= old;
            } else {
                self.bad -= 1;
            }
        }
        self.values.push_back(v);
        if v.is_finite() {
            self.sum += v;
        } else {
            self.bad += 1;
        }
    }
    fn value(&self) -> f64 {
        if self.bad == 0 && !self.values.is_empty() {
            self.sum
        } else {
            f64::NAN
        }
    }
}
pub struct ExtraState {
    raw: VecDeque<[f64; 32]>,
    sums: Vec<Sum>,
    ratios: Vec<VecDeque<f64>>,
}
impl Default for ExtraState {
    fn default() -> Self {
        Self {
            raw: VecDeque::new(),
            sums: [
                60, 60, 360, 360, 360, 1440, 1440, 1440, 180, 180, 2880, 2880,
            ]
            .into_iter()
            .map(Sum::new)
            .collect(),
            ratios: (0..7).map(|_| VecDeque::new()).collect(),
        }
    }
}
fn ratio(a: f64, b: f64) -> Option<f64> {
    (a.is_finite() && b.is_finite() && b.abs() > 1e-12)
        .then(|| a / b)
        .filter(|v| v.is_finite())
}
fn mean(q: &VecDeque<f64>, window: usize) -> Option<f64> {
    if q.len() < window {
        return None;
    }
    let vals = q.iter().rev().take(window);
    let mut sum = 0.;
    for v in vals {
        if !v.is_finite() {
            return None;
        }
        sum += v;
    }
    Some(sum / window as f64)
}
impl ExtraState {
    pub fn push(&mut self, v: [f64; 32]) {
        if self.raw.len() == 512 {
            self.raw.pop_front();
        }
        self.raw.push_back(v);
        for (s, i) in self
            .sums
            .iter_mut()
            .zip([10, 11, 14, 15, 16, 14, 15, 16, 31, 16, 10, 11])
        {
            s.push(v[i]);
        }
        let s: Vec<_> = self.sums.iter().map(Sum::value).collect();
        let specs = [
            (s[0], s[0] + s[1], 0.5),
            (s[2], s[2] + s[3] + s[4], 0.),
            (s[5], s[5] + s[6] + s[7], 0.),
            (s[4], s[2] + s[3] + s[4], 0.),
            (s[7], s[5] + s[6] + s[7], 0.),
            (s[8], s[9], 0.),
            (s[10], s[10] + s[11], 0.5),
        ];
        for (q, (a, b, zero)) in self.ratios.iter_mut().zip(specs) {
            if q.len() == 300 {
                q.pop_front();
            }
            q.push_back(if b.is_finite() && b.abs() <= 1e-12 {
                zero
            } else {
                ratio(a, b).unwrap_or(f64::NAN)
            });
        }
    }
    fn field(&self, i: usize) -> VecDeque<f64> {
        self.raw.iter().map(|v| v[i]).collect()
    }
    fn avg(&self, i: usize, n: usize) -> Option<f64> {
        mean(&self.field(i), n)
    }
    fn range(&self, n: usize, min: usize) -> Option<f64> {
        if self.raw.len() < min {
            return None;
        }
        let mut sum = 0.;
        let size = self.raw.len().min(n);
        for v in self.raw.iter().rev().take(size) {
            if !v[1].is_finite() || !v[2].is_finite() {
                return None;
            }
            sum += v[1] - v[2];
        }
        Some(sum / size as f64)
    }
    fn high_low(&self, n: usize) -> Option<f64> {
        if self.raw.len() < n {
            return None;
        }
        let mut high = f64::NEG_INFINITY;
        let mut low = f64::INFINITY;
        for v in self.raw.iter().rev().take(n) {
            if !v[1].is_finite() || !v[2].is_finite() {
                return None;
            }
            high = high.max(v[1]);
            low = low.min(v[2]);
        }
        Some(high - low)
    }
    fn momentum(&self, rsi: bool, zero: bool) -> Option<f64> {
        let n = self.raw.len();
        if n < 14 {
            return None;
        }
        let (mut gain, mut loss) = (0., 0.);
        for i in n - 14..n {
            let a = self.raw[i][3];
            let b = if i > 0 { self.raw[i - 1][3] } else { a };
            if !a.is_finite() || !b.is_finite() {
                return None;
            }
            let d = a - b;
            if d > 0. {
                gain += d
            } else {
                loss -= d;
            }
        }
        if gain + loss <= 1e-12 {
            return zero.then_some(0.);
        }
        Some(if rsi {
            100. * gain / (gain + loss)
        } else {
            100. * (gain - loss) / (gain + loss)
        })
    }
    pub fn values(&self) -> Vec<f64> {
        NAMES
            .iter()
            .map(|n| {
                self.compute(n)
                    .filter(|v| v.is_finite())
                    .unwrap_or(f64::NAN)
            })
            .collect()
    }
    fn compute(&self, name: &str) -> Option<f64> {
        let last = self.raw.back()?;
        let prev = self.raw.iter().rev().nth(1);
        match name {
            "baseline_023" => self.momentum(false, false),
            "baseline_030" => Some(self.avg(3, 9)? + self.avg(3, 150)?),
            "baseline_033" | "baseline_064" | "baseline_089" => {
                Some(self.avg(3, 12)? - self.avg(3, 26)?)
            }
            "baseline_037" | "baseline_142" => self.high_low(14),
            "baseline_041" | "baseline_056" => Some(self.avg(4, 12)? - self.avg(4, 26)?),
            "baseline_044" => {
                let q = self.field(3);
                if q.len() < 14 {
                    return None;
                }
                let y: Vec<_> = q.iter().rev().take(14).copied().rev().collect();
                if y.iter().any(|v| !v.is_finite()) {
                    return None;
                }
                let mean = y.iter().sum::<f64>() / 14.;
                Some(
                    y.iter()
                        .enumerate()
                        .map(|(i, v)| (i as f64 - 6.5) * (v - mean))
                        .sum::<f64>()
                        / 227.5,
                )
            }
            "baseline_048" => mean(&self.ratios[0], 30),
            "baseline_050" => self.range(10, 10),
            "baseline_075" | "baseline_078" | "baseline_094" | "baseline_095" => {
                let i = match name {
                    "baseline_075" => 1,
                    "baseline_078" => 2,
                    "baseline_094" => 3,
                    _ => 4,
                };
                ratio(*self.ratios[i].back()?, mean(&self.ratios[i], 300)?)
            }
            "baseline_084" => ratio(last[3] - last[0], last[25]),
            "baseline_097" => self.avg(3, 45),
            "baseline_102" => mean(&self.ratios[5], 120),
            "baseline_106" => Some(self.avg(3, 20)? + self.avg(1, 20)? - self.avg(2, 20)?),
            "baseline_108" | "baseline_165" => ratio(last[3] - prev?[3], prev?[3]),
            "baseline_130" => self.range(14, 1),
            "baseline_144" => Some(last[3] - last[0]),
            "baseline_147" => self.momentum(true, false),
            "baseline_150" => {
                if self.raw.len() < 15 {
                    return None;
                }
                let n = self.raw.len();
                let mut total = 0.;
                for i in n - 14..n {
                    let a = self.raw[i];
                    let old = self.raw[i - 1][3];
                    if !a[1].is_finite() || !a[2].is_finite() || !old.is_finite() {
                        return None;
                    }
                    total += (a[1] - a[2])
                        .max((a[1] - old).abs())
                        .max((a[2] - old).abs());
                }
                Some(total / 14.)
            }
            "baseline_155" => mean(&self.ratios[6], 150),
            "baseline_176" => ratio(last[3] - last[0], last[0]),
            "baseline_183" => {
                if let Some(prev) = prev {
                    ratio(last[3] - prev[3], last[3])
                } else {
                    Some(0.)
                }
            }
            "baseline_186" => self.range(14, 14),
            // The authoritative formula explicitly applies np.nan_to_num,
            // including its first thirteen observations.
            "baseline_197" => Some(self.momentum(true, true).unwrap_or(0.)),
            _ => None,
        }
    }
}
