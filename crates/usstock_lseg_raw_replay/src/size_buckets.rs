use anyhow::{bail, Result};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SizeThresholds {
    pub p50: f64,
    pub p90: f64,
}

impl SizeThresholds {
    pub fn new(p50: f64, p90: f64) -> Result<Self> {
        if !(p50.is_finite() && p90.is_finite() && p50 > 0.0 && p90 >= p50) {
            bail!("size thresholds must satisfy 0 < p50 <= p90, got {p50}, {p90}");
        }
        Ok(Self { p50, p90 })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct SizeBuckets {
    pub large_order: f64,
    pub medium_order: f64,
    pub small_order: f64,
    pub large_buy: f64,
    pub large_sell: f64,
    pub medium_buy: f64,
    pub medium_sell: f64,
    pub small_buy: f64,
    pub small_sell: f64,
}

impl SizeBuckets {
    /// Off-exchange `None` contributes to the total bucket but not B/S buckets.
    pub fn add(
        &mut self,
        amount: f64,
        side: Option<bool>,
        thresholds: SizeThresholds,
    ) -> Result<()> {
        if !(amount.is_finite() && amount > 0.0) {
            bail!("size-bucket amount must be finite and positive, got {amount}");
        }
        let (order, buy, sell) = if amount >= thresholds.p90 {
            (
                &mut self.large_order,
                &mut self.large_buy,
                &mut self.large_sell,
            )
        } else if amount >= thresholds.p50 {
            (
                &mut self.medium_order,
                &mut self.medium_buy,
                &mut self.medium_sell,
            )
        } else {
            (
                &mut self.small_order,
                &mut self.small_buy,
                &mut self.small_sell,
            )
        };
        *order += amount;
        match side {
            Some(true) => *buy += amount,
            Some(false) => *sell += amount,
            None => {}
        }
        Ok(())
    }

    pub fn total(self) -> f64 {
        self.large_order + self.medium_order + self.small_order
    }

    pub fn directional_total(self) -> f64 {
        self.large_buy
            + self.large_sell
            + self.medium_buy
            + self.medium_sell
            + self.small_buy
            + self.small_sell
    }

    pub fn nets(self) -> (f64, f64, f64) {
        (
            self.large_buy - self.large_sell,
            self.medium_buy - self.medium_sell,
            self.small_buy - self.small_sell,
        )
    }
}

/// Exact linear percentile, matching NumPy's default method and CME replay.
pub fn percentile_in_place(values: &mut [f64], percentile: f64) -> Result<f64> {
    if values.is_empty() || !(0.0..=1.0).contains(&percentile) {
        bail!(
            "invalid percentile sample size={} p={percentile}",
            values.len()
        );
    }
    if values
        .iter()
        .any(|value| !value.is_finite() || *value <= 0.0)
    {
        bail!("size percentile sample contains a nonpositive or nonfinite amount");
    }
    if values.len() == 1 {
        return Ok(values[0]);
    }
    let position = percentile * (values.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    let lower_value = {
        let (_, value, _) = values.select_nth_unstable_by(lower, f64::total_cmp);
        *value
    };
    if lower == upper {
        return Ok(lower_value);
    }
    let upper_value = {
        let (_, value, _) = values.select_nth_unstable_by(upper, f64::total_cmp);
        *value
    };
    let weight = position - lower as f64;
    Ok(lower_value * (1.0 - weight) + upper_value * weight)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Write;
    use std::process::{Command, Stdio};

    #[test]
    fn off_exchange_is_total_only() {
        let thresholds = SizeThresholds::new(100.0, 500.0).unwrap();
        let mut buckets = SizeBuckets::default();
        buckets.add(50.0, Some(true), thresholds).unwrap();
        buckets.add(200.0, Some(false), thresholds).unwrap();
        buckets.add(800.0, None, thresholds).unwrap();
        assert_eq!((buckets.small_order, buckets.small_buy), (50.0, 50.0));
        assert_eq!((buckets.medium_order, buckets.medium_sell), (200.0, 200.0));
        assert_eq!((buckets.large_order, buckets.large_buy), (800.0, 0.0));
        assert_eq!(buckets.total(), 1050.0);
        assert_eq!(buckets.directional_total(), 250.0);
        assert_eq!(buckets.nets(), (0.0, -200.0, 50.0));
    }

    #[test]
    fn exact_percentiles_match_python_reference() {
        let original = (1..=2003)
            .map(|index| ((index * 7919) % 104729 + 1) as f64 / 10.0)
            .collect::<Vec<_>>();
        let mut p50_values = original.clone();
        let mut p90_values = original.clone();
        let rust = json!({
            "p50": percentile_in_place(&mut p50_values, 0.5).unwrap(),
            "p90": percentile_in_place(&mut p90_values, 0.9).unwrap(),
        });
        let mut child = Command::new("python")
            .args(["size_reference.py"])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(&serde_json::to_vec(&original).unwrap())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success());
        let python: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(rust, python);
    }
}
