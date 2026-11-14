use anyhow::{bail, Context, Result};

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
    // RAW trades retain price in nanodollars and integer share size. Keep the
    // bucket totals on that exact scale until the parquet boundary.
    exact_nanos: Option<[i128; 9]>,
}

impl SizeBuckets {
    /// Off-exchange `None` contributes to the total bucket but not B/S buckets.
    pub fn add(
        &mut self,
        amount: f64,
        side: Option<bool>,
        thresholds: SizeThresholds,
    ) -> Result<()> {
        self.add_inner(amount, None, side, thresholds)
    }

    /// Adds an amount with its exact raw nanodollar notional. The floating
    /// amount still determines the percentile bucket so the threshold contract
    /// remains unchanged.
    pub fn add_exact(
        &mut self,
        amount: f64,
        amount_nanos: i128,
        side: Option<bool>,
        thresholds: SizeThresholds,
    ) -> Result<()> {
        if amount_nanos <= 0 {
            bail!("exact size-bucket amount must be positive, got {amount_nanos}");
        }
        self.add_inner(amount, Some(amount_nanos), side, thresholds)
    }

    fn add_inner(
        &mut self,
        amount: f64,
        amount_nanos: Option<i128>,
        side: Option<bool>,
        thresholds: SizeThresholds,
    ) -> Result<()> {
        if !(amount.is_finite() && amount > 0.0) {
            bail!("size-bucket amount must be finite and positive, got {amount}");
        }
        let bucket = if amount >= thresholds.p90 {
            0
        } else if amount >= thresholds.p50 {
            1
        } else {
            2
        };
        let (order, buy, sell) = if bucket == 0 {
            (
                &mut self.large_order,
                &mut self.large_buy,
                &mut self.large_sell,
            )
        } else if bucket == 1 {
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
        if let Some(amount_nanos) = amount_nanos {
            let exact = self.exact_nanos.get_or_insert([0; 9]);
            exact[bucket] = exact[bucket]
                .checked_add(amount_nanos)
                .context("exact size-bucket total overflow")?;
            let directional_index = match side {
                Some(true) => Some(3 + bucket * 2),
                Some(false) => Some(4 + bucket * 2),
                None => None,
            };
            if let Some(index) = directional_index {
                exact[index] = exact[index]
                    .checked_add(amount_nanos)
                    .context("exact directional size-bucket overflow")?;
            }
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

    /// Returns exact `(total, directional_total)` nanodollar sums when this
    /// bucket was built from RAW scaled notional values.
    pub fn exact_totals(self) -> Result<Option<(i128, i128)>> {
        let Some(values) = self.exact_nanos else {
            return Ok(None);
        };
        let total = values[0]
            .checked_add(values[1])
            .and_then(|value| value.checked_add(values[2]))
            .context("exact size-bucket total overflow")?;
        let directional = values[3..]
            .iter()
            .try_fold(0_i128, |total, value| total.checked_add(*value))
            .context("exact directional size-bucket overflow")?;
        Ok(Some((total, directional)))
    }

    /// Replaces the working floating totals with values converted once from
    /// exact raw nanodollar totals for parquet output.
    pub fn materialize_exact(&mut self) {
        let Some(values) = self.exact_nanos else {
            return;
        };
        self.large_order = values[0] as f64 / 1e9;
        self.medium_order = values[1] as f64 / 1e9;
        self.small_order = values[2] as f64 / 1e9;
        self.large_buy = values[3] as f64 / 1e9;
        self.large_sell = values[4] as f64 / 1e9;
        self.medium_buy = values[5] as f64 / 1e9;
        self.medium_sell = values[6] as f64 / 1e9;
        self.small_buy = values[7] as f64 / 1e9;
        self.small_sell = values[8] as f64 / 1e9;
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
    fn exact_raw_nanos_conserve_across_size_buckets() {
        let thresholds = SizeThresholds::new(0.15, 0.25).unwrap();
        let mut buckets = SizeBuckets::default();
        buckets
            .add_exact(0.1, 100_000_000, Some(true), thresholds)
            .unwrap();
        buckets
            .add_exact(0.2, 200_000_000, Some(false), thresholds)
            .unwrap();
        buckets
            .add_exact(0.3, 300_000_000, None, thresholds)
            .unwrap();
        assert_eq!(
            buckets.exact_totals().unwrap(),
            Some((600_000_000, 300_000_000))
        );
        buckets.materialize_exact();
        assert!((buckets.total() - 0.6).abs() <= f64::EPSILON);
        assert!((buckets.directional_total() - 0.3).abs() <= f64::EPSILON);
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
