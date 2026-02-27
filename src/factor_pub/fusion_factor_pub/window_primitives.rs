//! Rolling window primitives.
//!
//! `*_series*` helpers still use Polars for convenience.

use anyhow::Result;
use polars::prelude::{NamedFrom, RollingOptionsFixedWindow, Series};
use polars_time::prelude::SeriesOpsTime;

fn finite_or_none(value: Option<f64>) -> Option<f64> {
    match value {
        Some(v) if v.is_finite() => Some(v),
        Some(_) => Some(0.0),
        None => None,
    }
}

fn series_from(values: &[f64]) -> Series {
    Series::new("x".into(), values.to_vec())
}

fn series_from_opt(values: &[Option<f64>]) -> Series {
    Series::new("x".into(), values.to_vec())
}

fn tail_exact(values: &[f64], window: usize) -> Option<&[f64]> {
    if window == 0 || values.len() < window {
        return None;
    }
    Some(&values[values.len() - window..])
}

fn tail_with_min_periods(values: &[f64], window: usize, min_periods: usize) -> Option<&[f64]> {
    if window == 0 || min_periods == 0 || values.len() < min_periods {
        return None;
    }
    let start = values.len().saturating_sub(window);
    let tail = &values[start..];
    if tail.len() < min_periods {
        return None;
    }
    Some(tail)
}

fn has_non_finite(values: &[f64]) -> bool {
    values.iter().any(|v| !v.is_finite())
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn skew_from_slice(values: &[f64], bias: bool) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    if has_non_finite(values) {
        return Some(0.0);
    }

    let n = values.len() as f64;
    let m = mean(values);
    let mut m2 = 0.0;
    let mut m3 = 0.0;
    for v in values {
        let d = *v - m;
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
        if values.len() < 3 {
            return None;
        }
        out *= (n * (n - 1.0)).sqrt() / (n - 2.0);
    }
    finite_or_none(Some(out)).or(Some(0.0))
}

fn kurtosis_from_slice(values: &[f64], fisher: bool, bias: bool) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    if has_non_finite(values) {
        return Some(0.0);
    }

    let n = values.len() as f64;
    let m = mean(values);
    let mut m2 = 0.0;
    let mut m4 = 0.0;
    for v in values {
        let d = *v - m;
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
        if values.len() < 4 {
            return None;
        }
        ((n - 1.0) / ((n - 2.0) * (n - 3.0))) * ((n + 1.0) * g2 + 6.0)
    };
    if !fisher {
        out += 3.0;
    }
    finite_or_none(Some(out)).or(Some(0.0))
}

pub fn rolling_mean_last(values: &[f64], window: usize) -> Result<Option<f64>> {
    let Some(tail) = tail_exact(values, window) else {
        return Ok(None);
    };
    if has_non_finite(tail) {
        return Ok(Some(0.0));
    }
    Ok(finite_or_none(Some(mean(tail))).or(Some(0.0)))
}

pub fn rolling_mean_last_with_min_periods(
    values: &[f64],
    window: usize,
    min_periods: usize,
) -> Result<Option<f64>> {
    let Some(tail) = tail_with_min_periods(values, window, min_periods) else {
        return Ok(None);
    };
    if has_non_finite(tail) {
        return Ok(Some(0.0));
    }
    Ok(finite_or_none(Some(mean(tail))).or(Some(0.0)))
}

pub fn rolling_sum_last_with_min_periods(
    values: &[f64],
    window: usize,
    min_periods: usize,
) -> Result<Option<f64>> {
    let Some(tail) = tail_with_min_periods(values, window, min_periods) else {
        return Ok(None);
    };
    if has_non_finite(tail) {
        return Ok(Some(0.0));
    }
    Ok(finite_or_none(Some(tail.iter().sum::<f64>())).or(Some(0.0)))
}

pub fn rolling_std_last(values: &[f64], window: usize) -> Result<Option<f64>> {
    let Some(tail) = tail_exact(values, window) else {
        return Ok(None);
    };
    if has_non_finite(tail) {
        return Ok(Some(0.0));
    }
    if tail.len() < 2 {
        return Ok(Some(0.0));
    }

    let m = mean(tail);
    let var = tail
        .iter()
        .map(|v| {
            let d = *v - m;
            d * d
        })
        .sum::<f64>()
        / (tail.len() as f64 - 1.0);
    Ok(finite_or_none(Some(var.sqrt())).or(Some(0.0)))
}

pub fn rolling_mean_series_opt(
    values: &[Option<f64>],
    window: usize,
    min_periods: usize,
) -> Result<Vec<Option<f64>>> {
    if window == 0 || min_periods == 0 {
        return Ok(vec![None; values.len()]);
    }
    let rolling = series_from_opt(values).rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods,
        ..Default::default()
    })?;
    let out = rolling.f64()?.into_iter().collect();
    Ok(out)
}

pub fn rolling_sum_series_opt(
    values: &[Option<f64>],
    window: usize,
    min_periods: usize,
) -> Result<Vec<Option<f64>>> {
    if window == 0 || min_periods == 0 {
        return Ok(vec![None; values.len()]);
    }
    let rolling = series_from_opt(values).rolling_sum(RollingOptionsFixedWindow {
        window_size: window,
        min_periods,
        ..Default::default()
    })?;
    let out = rolling.f64()?.into_iter().collect();
    Ok(out)
}

pub fn rolling_mean_series(
    values: &[f64],
    window: usize,
    min_periods: usize,
) -> Result<Vec<Option<f64>>> {
    if window == 0 || min_periods == 0 {
        return Ok(vec![None; values.len()]);
    }
    let rolling = series_from(values).rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods,
        ..Default::default()
    })?;
    let out = rolling.f64()?.into_iter().collect();
    Ok(out)
}

pub fn rolling_sum_series(
    values: &[f64],
    window: usize,
    min_periods: usize,
) -> Result<Vec<Option<f64>>> {
    if window == 0 || min_periods == 0 {
        return Ok(vec![None; values.len()]);
    }
    let rolling = series_from(values).rolling_sum(RollingOptionsFixedWindow {
        window_size: window,
        min_periods,
        ..Default::default()
    })?;
    let out = rolling.f64()?.into_iter().collect();
    Ok(out)
}

pub fn tail_skew_last_opt(
    values: &[Option<f64>],
    window: usize,
    min_periods: usize,
    bias: bool,
) -> Result<Option<f64>> {
    if window == 0 || min_periods == 0 || values.is_empty() {
        return Ok(None);
    }
    let end = values.len();
    let start = end.saturating_sub(window);
    let tail = &values[start..end];
    let mut valid = Vec::with_capacity(tail.len());
    for v in tail {
        if let Some(vv) = v {
            if vv.is_finite() {
                valid.push(*vv);
            } else {
                valid.push(0.0);
            }
        }
    }
    if valid.len() < min_periods {
        return Ok(None);
    }
    Ok(skew_from_slice(&valid, bias))
}

pub fn rolling_min_last(values: &[f64], window: usize) -> Result<Option<f64>> {
    let Some(tail) = tail_exact(values, window) else {
        return Ok(None);
    };
    if has_non_finite(tail) {
        return Ok(Some(0.0));
    }
    let out = tail.iter().copied().reduce(f64::min);
    Ok(finite_or_none(out).or(Some(0.0)))
}

pub fn rolling_max_last(values: &[f64], window: usize) -> Result<Option<f64>> {
    let Some(tail) = tail_exact(values, window) else {
        return Ok(None);
    };
    if has_non_finite(tail) {
        return Ok(Some(0.0));
    }
    let out = tail.iter().copied().reduce(f64::max);
    Ok(finite_or_none(out).or(Some(0.0)))
}

pub fn rolling_skew_last(values: &[f64], window: usize, bias: bool) -> Result<Option<f64>> {
    let Some(tail) = tail_exact(values, window) else {
        return Ok(None);
    };
    Ok(skew_from_slice(tail, bias).or(Some(0.0)))
}

pub fn rolling_kurt_last(
    values: &[f64],
    window: usize,
    fisher: bool,
    bias: bool,
) -> Result<Option<f64>> {
    let Some(tail) = tail_exact(values, window) else {
        return Ok(None);
    };
    Ok(kurtosis_from_slice(tail, fisher, bias).or(Some(0.0)))
}

pub fn rolling_rank_last(values: &[f64], window: usize) -> Result<Option<f64>> {
    let Some(tail) = tail_exact(values, window) else {
        return Ok(None);
    };
    if has_non_finite(tail) {
        return Ok(Some(0.0));
    }
    let Some(last) = tail.last().copied() else {
        return Ok(None);
    };
    if !last.is_finite() {
        return Ok(Some(0.0));
    }

    let mut lt = 0usize;
    let mut eq = 0usize;
    for v in tail {
        if v.total_cmp(&last).is_lt() {
            lt = lt.saturating_add(1);
        } else if v.total_cmp(&last).is_eq() {
            eq = eq.saturating_add(1);
        }
    }
    if eq == 0 {
        return Ok(None);
    }
    Ok(Some(lt as f64 + (eq as f64 + 1.0) / 2.0))
}

pub fn rolling_corr_last(xs: &[f64], ys: &[f64], window: usize, _ddof: u8) -> Result<Option<f64>> {
    if window == 0 {
        return Ok(None);
    }
    let n = xs.len().min(ys.len());
    if n < window {
        return Ok(None);
    }

    let x_tail = &xs[n - window..];
    let y_tail = &ys[n - window..];
    if has_non_finite(x_tail) || has_non_finite(y_tail) {
        return Ok(Some(0.0));
    }

    let mean_x = mean(x_tail);
    let mean_y = mean(y_tail);
    let mut cov = 0.0;
    let mut var_x = 0.0;
    let mut var_y = 0.0;
    for i in 0..window {
        let dx = x_tail[i] - mean_x;
        let dy = y_tail[i] - mean_y;
        cov += dx * dy;
        var_x += dx * dx;
        var_y += dy * dy;
    }
    if var_x.abs() <= 1e-12 || var_y.abs() <= 1e-12 {
        return Ok(Some(0.0));
    }
    let out = cov / (var_x.sqrt() * var_y.sqrt());
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
        assert_eq!(rolling_min_last(&x, 3).expect("min failed"), Some(3.0));
        assert_eq!(rolling_max_last(&x, 3).expect("max failed"), Some(5.0));

        let std = rolling_std_last(&x, 3).expect("std failed");
        assert!(std.is_some());

        let rank = rolling_rank_last(&[1.0, 3.0, 2.0], 3).expect("rank failed");
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
