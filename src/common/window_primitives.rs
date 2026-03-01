//! Rolling window primitives.
//!
//! All helpers here run on lightweight native loops and avoid DataFrame construction.

use crate::common::rolling_welford::{RollingWelford, WelfordCovariance};
use anyhow::Result;

pub trait F64SeriesView {
    fn len(&self) -> usize;
    fn value_at(&self, idx: usize) -> f64;
}

pub trait OptF64SeriesView {
    fn len(&self) -> usize;
    fn value_at(&self, idx: usize) -> Option<f64>;
}

impl F64SeriesView for [f64] {
    fn len(&self) -> usize {
        self.len()
    }

    fn value_at(&self, idx: usize) -> f64 {
        self[idx]
    }
}

impl F64SeriesView for Vec<f64> {
    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn value_at(&self, idx: usize) -> f64 {
        self[idx]
    }
}

impl OptF64SeriesView for [Option<f64>] {
    fn len(&self) -> usize {
        self.len()
    }

    fn value_at(&self, idx: usize) -> Option<f64> {
        self[idx]
    }
}

impl OptF64SeriesView for Vec<Option<f64>> {
    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn value_at(&self, idx: usize) -> Option<f64> {
        self[idx]
    }
}

fn finite_or_none(value: Option<f64>) -> Option<f64> {
    match value {
        Some(v) if v.is_finite() => Some(v),
        Some(_) => Some(0.0),
        None => None,
    }
}

fn tail_exact_bounds(n: usize, window: usize) -> Option<(usize, usize)> {
    if window == 0 || n < window {
        return None;
    }
    Some((n - window, n))
}

fn tail_with_min_periods_bounds(
    n: usize,
    window: usize,
    min_periods: usize,
) -> Option<(usize, usize)> {
    if window == 0 || min_periods == 0 || n < min_periods {
        return None;
    }
    let start = n.saturating_sub(window);
    let len = n - start;
    if len < min_periods {
        return None;
    }
    Some((start, n))
}

fn has_non_finite(values: &(impl F64SeriesView + ?Sized), start: usize, end: usize) -> bool {
    for i in start..end {
        if !values.value_at(i).is_finite() {
            return true;
        }
    }
    false
}

fn mean(values: &(impl F64SeriesView + ?Sized), start: usize, end: usize) -> f64 {
    let mut sum = 0.0;
    let mut n = 0usize;
    for i in start..end {
        sum += values.value_at(i);
        n += 1;
    }
    sum / n as f64
}

fn skew_from_range(
    values: &(impl F64SeriesView + ?Sized),
    start: usize,
    end: usize,
    bias: bool,
) -> Option<f64> {
    if start >= end {
        return None;
    }
    if has_non_finite(values, start, end) {
        return Some(0.0);
    }

    let n = (end - start) as f64;
    let m = mean(values, start, end);
    let mut m2 = 0.0;
    let mut m3 = 0.0;
    for i in start..end {
        let d = values.value_at(i) - m;
        let d2 = d * d;
        m2 += d2;
        m3 += d2 * d;
    }
    let m2 = m2 / n;
    if m2.abs() <= 1e-12 {
        return Some(0.0);
    }
    let m3 = m3 / n;
    let mut out = m3 / m2.powf(1.5);
    if !bias {
        if (end - start) < 3 {
            return None;
        }
        out *= (n * (n - 1.0)).sqrt() / (n - 2.0);
    }
    finite_or_none(Some(out)).or(Some(0.0))
}

fn kurtosis_from_range(
    values: &(impl F64SeriesView + ?Sized),
    start: usize,
    end: usize,
    fisher: bool,
    bias: bool,
) -> Option<f64> {
    if start >= end {
        return None;
    }
    if has_non_finite(values, start, end) {
        return Some(0.0);
    }

    let n = (end - start) as f64;
    let m = mean(values, start, end);
    let mut m2 = 0.0;
    let mut m4 = 0.0;
    for i in start..end {
        let d = values.value_at(i) - m;
        let d2 = d * d;
        m2 += d2;
        m4 += d2 * d2;
    }
    let m2 = m2 / n;
    if m2.abs() <= 1e-12 {
        return Some(0.0);
    }
    let m4 = m4 / n;
    let g2 = m4 / (m2 * m2) - 3.0;

    let mut out = if bias {
        g2
    } else {
        if (end - start) < 4 {
            return None;
        }
        ((n - 1.0) / ((n - 2.0) * (n - 3.0))) * ((n + 1.0) * g2 + 6.0)
    };
    if !fisher {
        out += 3.0;
    }
    finite_or_none(Some(out)).or(Some(0.0))
}

pub fn rolling_mean_last(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
) -> Result<Option<f64>> {
    let n = values.len();
    let Some((start, end)) = tail_exact_bounds(n, window) else {
        return Ok(None);
    };
    if has_non_finite(values, start, end) {
        return Ok(Some(0.0));
    }
    Ok(finite_or_none(Some(mean(values, start, end))).or(Some(0.0)))
}

pub fn rolling_mean_last_with_min_periods(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Result<Option<f64>> {
    let n = values.len();
    let Some((start, end)) = tail_with_min_periods_bounds(n, window, min_periods) else {
        return Ok(None);
    };
    if has_non_finite(values, start, end) {
        return Ok(Some(0.0));
    }
    Ok(finite_or_none(Some(mean(values, start, end))).or(Some(0.0)))
}

pub fn rolling_sum_last_with_min_periods(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Result<Option<f64>> {
    let n = values.len();
    let Some((start, end)) = tail_with_min_periods_bounds(n, window, min_periods) else {
        return Ok(None);
    };
    if has_non_finite(values, start, end) {
        return Ok(Some(0.0));
    }
    let mut sum = 0.0;
    for i in start..end {
        sum += values.value_at(i);
    }
    Ok(finite_or_none(Some(sum)).or(Some(0.0)))
}

/// Last value equivalent of `rolling_mean_series(...)[-1]`.
/// Preserves series semantics: non-finite values yield `Some(NaN)`.
pub fn rolling_mean_last_from_series(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Result<Option<f64>> {
    rolling_mean_at_from_series(values, values.len(), window, min_periods)
}

/// Last value equivalent of `rolling_sum_series(...)[-1]`.
/// Preserves series semantics: non-finite values yield `Some(NaN)`.
pub fn rolling_sum_last_from_series(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Result<Option<f64>> {
    rolling_sum_at_from_series(values, values.len(), window, min_periods)
}

/// Value at `end_exclusive-1` equivalent of `rolling_mean_series(...)[idx]`.
/// Preserves series semantics: non-finite values yield `Some(NaN)`.
pub fn rolling_mean_at_from_series(
    values: &(impl F64SeriesView + ?Sized),
    end_exclusive: usize,
    window: usize,
    min_periods: usize,
) -> Result<Option<f64>> {
    let end = end_exclusive.min(values.len());
    if window == 0 || min_periods == 0 || end < min_periods {
        return Ok(None);
    }
    let start = end.saturating_sub(window);
    let window_len = end - start;
    if window_len < min_periods {
        return Ok(None);
    }
    if has_non_finite(values, start, end) {
        return Ok(Some(f64::NAN));
    }
    Ok(Some(mean(values, start, end)))
}

/// Value at `end_exclusive-1` equivalent of `rolling_sum_series(...)[idx]`.
/// Preserves series semantics: non-finite values yield `Some(NaN)`.
pub fn rolling_sum_at_from_series(
    values: &(impl F64SeriesView + ?Sized),
    end_exclusive: usize,
    window: usize,
    min_periods: usize,
) -> Result<Option<f64>> {
    let end = end_exclusive.min(values.len());
    if window == 0 || min_periods == 0 || end < min_periods {
        return Ok(None);
    }
    let start = end.saturating_sub(window);
    let window_len = end - start;
    if window_len < min_periods {
        return Ok(None);
    }
    if has_non_finite(values, start, end) {
        return Ok(Some(f64::NAN));
    }
    let mut sum = 0.0;
    for i in start..end {
        sum += values.value_at(i);
    }
    Ok(Some(sum))
}

/// Last value equivalent of `rolling_mean_series_opt(...)[-1]`.
/// Preserves series semantics: non-finite values yield `Some(NaN)`.
pub fn rolling_mean_last_opt_from_series(
    values: &(impl OptF64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Result<Option<f64>> {
    rolling_mean_at_opt_from_series(values, values.len(), window, min_periods)
}

/// Last value equivalent of `rolling_sum_series_opt(...)[-1]`.
/// Preserves series semantics: non-finite values yield `Some(NaN)`.
pub fn rolling_sum_last_opt_from_series(
    values: &(impl OptF64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Result<Option<f64>> {
    rolling_sum_at_opt_from_series(values, values.len(), window, min_periods)
}

/// Value at `end_exclusive-1` equivalent of `rolling_mean_series_opt(...)[idx]`.
/// Preserves series semantics: non-finite values yield `Some(NaN)`.
pub fn rolling_mean_at_opt_from_series(
    values: &(impl OptF64SeriesView + ?Sized),
    end_exclusive: usize,
    window: usize,
    min_periods: usize,
) -> Result<Option<f64>> {
    let end = end_exclusive.min(values.len());
    if window == 0 || min_periods == 0 || end == 0 {
        return Ok(None);
    }
    let start = end.saturating_sub(window);
    let mut sum = 0.0;
    let mut valid = 0usize;
    let mut non_finite = 0usize;
    for i in start..end {
        if let Some(v) = values.value_at(i) {
            valid += 1;
            if v.is_finite() {
                sum += v;
            } else {
                non_finite += 1;
            }
        }
    }
    if valid < min_periods {
        return Ok(None);
    }
    if non_finite > 0 {
        return Ok(Some(f64::NAN));
    }
    Ok(Some(sum / valid as f64))
}

/// Value at `end_exclusive-1` equivalent of `rolling_sum_series_opt(...)[idx]`.
/// Preserves series semantics: non-finite values yield `Some(NaN)`.
pub fn rolling_sum_at_opt_from_series(
    values: &(impl OptF64SeriesView + ?Sized),
    end_exclusive: usize,
    window: usize,
    min_periods: usize,
) -> Result<Option<f64>> {
    let end = end_exclusive.min(values.len());
    if window == 0 || min_periods == 0 || end == 0 {
        return Ok(None);
    }
    let start = end.saturating_sub(window);
    let mut sum = 0.0;
    let mut valid = 0usize;
    let mut non_finite = 0usize;
    for i in start..end {
        if let Some(v) = values.value_at(i) {
            valid += 1;
            if v.is_finite() {
                sum += v;
            } else {
                non_finite += 1;
            }
        }
    }
    if valid < min_periods {
        return Ok(None);
    }
    if non_finite > 0 {
        return Ok(Some(f64::NAN));
    }
    Ok(Some(sum))
}

pub fn rolling_std_last(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
) -> Result<Option<f64>> {
    let n = values.len();
    let Some((start, end)) = tail_exact_bounds(n, window) else {
        return Ok(None);
    };
    let len = end - start;
    if len < 2 {
        return Ok(Some(0.0));
    }
    if has_non_finite(values, start, end) {
        return Ok(Some(0.0));
    }
    let mut stats = RollingWelford::new(window);
    for i in start..end {
        stats.push(values.value_at(i));
    }
    Ok(finite_or_none(Some(stats.std())).or(Some(0.0)))
}

pub fn rolling_mean_series_opt(
    values: &(impl OptF64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Result<Vec<Option<f64>>> {
    let n = values.len();
    if window == 0 || min_periods == 0 {
        return Ok(vec![None; n]);
    }

    let mut out = vec![None; n];
    let mut sum = 0.0;
    let mut valid = 0usize;
    let mut non_finite = 0usize;

    for i in 0..n {
        if let Some(v) = values.value_at(i) {
            valid += 1;
            if v.is_finite() {
                sum += v;
            } else {
                non_finite += 1;
            }
        }

        if i >= window {
            if let Some(v) = values.value_at(i - window) {
                valid = valid.saturating_sub(1);
                if v.is_finite() {
                    sum -= v;
                } else {
                    non_finite = non_finite.saturating_sub(1);
                }
            }
        }

        if valid >= min_periods {
            out[i] = if non_finite > 0 {
                Some(f64::NAN)
            } else {
                Some(sum / valid as f64)
            };
        }
    }
    Ok(out)
}

pub fn rolling_sum_series_opt(
    values: &(impl OptF64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Result<Vec<Option<f64>>> {
    let n = values.len();
    if window == 0 || min_periods == 0 {
        return Ok(vec![None; n]);
    }

    let mut out = vec![None; n];
    let mut sum = 0.0;
    let mut valid = 0usize;
    let mut non_finite = 0usize;

    for i in 0..n {
        if let Some(v) = values.value_at(i) {
            valid += 1;
            if v.is_finite() {
                sum += v;
            } else {
                non_finite += 1;
            }
        }

        if i >= window {
            if let Some(v) = values.value_at(i - window) {
                valid = valid.saturating_sub(1);
                if v.is_finite() {
                    sum -= v;
                } else {
                    non_finite = non_finite.saturating_sub(1);
                }
            }
        }

        if valid >= min_periods {
            out[i] = if non_finite > 0 {
                Some(f64::NAN)
            } else {
                Some(sum)
            };
        }
    }
    Ok(out)
}

pub fn rolling_mean_series(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Result<Vec<Option<f64>>> {
    let n = values.len();
    if window == 0 || min_periods == 0 {
        return Ok(vec![None; n]);
    }

    let mut out = vec![None; n];
    let mut sum = 0.0;
    let mut non_finite = 0usize;

    for i in 0..n {
        let v = values.value_at(i);
        if v.is_finite() {
            sum += v;
        } else {
            non_finite += 1;
        }

        if i >= window {
            let old = values.value_at(i - window);
            if old.is_finite() {
                sum -= old;
            } else {
                non_finite = non_finite.saturating_sub(1);
            }
        }

        let window_len = (i + 1).min(window);
        if window_len >= min_periods {
            out[i] = if non_finite > 0 {
                Some(f64::NAN)
            } else {
                Some(sum / window_len as f64)
            };
        }
    }
    Ok(out)
}

pub fn rolling_sum_series(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Result<Vec<Option<f64>>> {
    let n = values.len();
    if window == 0 || min_periods == 0 {
        return Ok(vec![None; n]);
    }

    let mut out = vec![None; n];
    let mut sum = 0.0;
    let mut non_finite = 0usize;

    for i in 0..n {
        let v = values.value_at(i);
        if v.is_finite() {
            sum += v;
        } else {
            non_finite += 1;
        }

        if i >= window {
            let old = values.value_at(i - window);
            if old.is_finite() {
                sum -= old;
            } else {
                non_finite = non_finite.saturating_sub(1);
            }
        }

        let window_len = (i + 1).min(window);
        if window_len >= min_periods {
            out[i] = if non_finite > 0 {
                Some(f64::NAN)
            } else {
                Some(sum)
            };
        }
    }
    Ok(out)
}

pub fn tail_skew_last_opt(
    values: &(impl OptF64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
    bias: bool,
) -> Result<Option<f64>> {
    if window == 0 || min_periods == 0 || values.len() == 0 {
        return Ok(None);
    }
    let end = values.len();
    let start = end.saturating_sub(window);
    let mut valid = Vec::with_capacity(window);
    for i in start..end {
        if let Some(v) = values.value_at(i) {
            if v.is_finite() {
                valid.push(v);
            } else {
                valid.push(0.0);
            }
        }
    }
    if valid.len() < min_periods {
        return Ok(None);
    }
    Ok(skew_from_range(&valid, 0, valid.len(), bias))
}

pub fn rolling_min_last(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
) -> Result<Option<f64>> {
    let n = values.len();
    let Some((start, end)) = tail_exact_bounds(n, window) else {
        return Ok(None);
    };
    if has_non_finite(values, start, end) {
        return Ok(Some(0.0));
    }
    let mut out = f64::INFINITY;
    for i in start..end {
        out = out.min(values.value_at(i));
    }
    Ok(finite_or_none(Some(out)).or(Some(0.0)))
}

pub fn rolling_max_last(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
) -> Result<Option<f64>> {
    let n = values.len();
    let Some((start, end)) = tail_exact_bounds(n, window) else {
        return Ok(None);
    };
    if has_non_finite(values, start, end) {
        return Ok(Some(0.0));
    }
    let mut out = f64::NEG_INFINITY;
    for i in start..end {
        out = out.max(values.value_at(i));
    }
    Ok(finite_or_none(Some(out)).or(Some(0.0)))
}

pub fn rolling_skew_last(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
    bias: bool,
) -> Result<Option<f64>> {
    let n = values.len();
    let Some((start, end)) = tail_exact_bounds(n, window) else {
        return Ok(None);
    };
    Ok(skew_from_range(values, start, end, bias).or(Some(0.0)))
}

pub fn rolling_kurt_last(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
    fisher: bool,
    bias: bool,
) -> Result<Option<f64>> {
    let n = values.len();
    let Some((start, end)) = tail_exact_bounds(n, window) else {
        return Ok(None);
    };
    Ok(kurtosis_from_range(values, start, end, fisher, bias).or(Some(0.0)))
}

pub fn rolling_rank_last(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
) -> Result<Option<f64>> {
    let n = values.len();
    let Some((start, end)) = tail_exact_bounds(n, window) else {
        return Ok(None);
    };
    if has_non_finite(values, start, end) {
        return Ok(Some(0.0));
    }
    let last = values.value_at(end - 1);
    if !last.is_finite() {
        return Ok(Some(0.0));
    }

    let mut lt = 0usize;
    let mut eq = 0usize;
    for i in start..end {
        let v = values.value_at(i);
        if v.total_cmp(&last).is_lt() {
            lt += 1;
        } else if v.total_cmp(&last).is_eq() {
            eq += 1;
        }
    }
    if eq == 0 {
        return Ok(None);
    }
    Ok(Some(lt as f64 + (eq as f64 + 1.0) / 2.0))
}

pub fn rolling_corr_last(
    xs: &(impl F64SeriesView + ?Sized),
    ys: &(impl F64SeriesView + ?Sized),
    window: usize,
    _ddof: u8,
) -> Result<Option<f64>> {
    if window == 0 {
        return Ok(None);
    }
    let n = xs.len().min(ys.len());
    if n < window {
        return Ok(None);
    }
    let start = n - window;
    let end = n;

    for i in start..end {
        if !xs.value_at(i).is_finite() || !ys.value_at(i).is_finite() {
            return Ok(Some(0.0));
        }
    }

    let mut stats = WelfordCovariance::new();
    for i in start..end {
        stats.push(xs.value_at(i), ys.value_at(i));
    }
    let out = stats.corr().unwrap_or(0.0);
    Ok(finite_or_none(Some(out)).or(Some(0.0)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_window_primitives_work() {
        let x = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let y = vec![2.0, 4.0, 6.0, 8.0, 10.0];

        assert_eq!(rolling_mean_last(&x, 3).expect("mean failed"), Some(4.0));
        assert_eq!(
            rolling_mean_last_from_series(&x, 3, 3).expect("mean series failed"),
            Some(4.0)
        );
        assert_eq!(
            rolling_mean_at_from_series(&x, 4, 3, 3).expect("mean at failed"),
            Some(3.0)
        );
        assert_eq!(
            rolling_sum_at_from_series(&x, 5, 3, 3).expect("sum at failed"),
            Some(12.0)
        );
        assert_eq!(rolling_min_last(&x, 3).expect("min failed"), Some(3.0));
        assert_eq!(rolling_max_last(&x, 3).expect("max failed"), Some(5.0));

        let std = rolling_std_last(&x, 3).expect("std failed");
        assert!(std.is_some());

        let opt = vec![Some(1.0), None, Some(3.0), Some(4.0)];
        assert_eq!(
            rolling_sum_at_opt_from_series(&opt, 4, 4, 3).expect("sum at opt failed"),
            Some(8.0)
        );
        let mean_opt = rolling_mean_at_opt_from_series(&opt, 4, 4, 3).expect("mean at opt failed");
        assert!((mean_opt.expect("mean at opt none") - (8.0 / 3.0)).abs() < 1e-12);

        let rank_data = [1.0, 3.0, 2.0];
        let rank = rolling_rank_last(&rank_data[..], 3).expect("rank failed");
        assert_eq!(rank, Some(2.0));

        let corr = rolling_corr_last(&x, &y, 5, 1).expect("corr failed");
        assert!(corr.is_some());
        assert!((corr.unwrap_or(0.0) - 1.0).abs() < 1e-12);

        let skew = rolling_skew_last(&x, 5, false).expect("skew failed");
        assert!(skew.is_some());

        let kurt = rolling_kurt_last(&x, 5, true, false).expect("kurt failed");
        assert!(kurt.is_some());
    }
}
