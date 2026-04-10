use anyhow::Result;

use crate::view::{F64SeriesView, OptF64SeriesView};

fn finite_or_none(value: Option<f64>) -> Option<f64> {
    match value {
        Some(v) if v.is_finite() => Some(v),
        Some(_) => Some(0.0),
        None => None,
    }
}

pub fn finite_opt(value: Option<f64>) -> Option<f64> {
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
        if end - start < 3 {
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
        return Some(if fisher { -3.0 } else { 0.0 });
    }
    let m4 = m4 / n;
    let g2 = m4 / (m2 * m2) - 3.0;

    let mut out = if bias {
        g2
    } else {
        if end - start < 4 {
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

pub fn rolling_mean_last_from_series(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Result<Option<f64>> {
    rolling_mean_at_from_series(values, values.len(), window, min_periods)
}

pub fn rolling_sum_last_from_series(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Result<Option<f64>> {
    rolling_sum_at_from_series(values, values.len(), window, min_periods)
}

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

pub fn rolling_mean_last_opt_from_series(
    values: &(impl OptF64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Result<Option<f64>> {
    rolling_mean_at_opt_from_series(values, values.len(), window, min_periods)
}

pub fn rolling_sum_last_opt_from_series(
    values: &(impl OptF64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Result<Option<f64>> {
    rolling_sum_at_opt_from_series(values, values.len(), window, min_periods)
}

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

    let mean_x = mean(xs, start, end);
    let mean_y = mean(ys, start, end);
    let mut cov = 0.0;
    let mut var_x = 0.0;
    let mut var_y = 0.0;
    for i in start..end {
        let dx = xs.value_at(i) - mean_x;
        let dy = ys.value_at(i) - mean_y;
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

pub fn tail_quantile_last(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
    q: f64,
) -> Option<f64> {
    if window == 0 || values.len() < window || !(0.0..=1.0).contains(&q) {
        return None;
    }
    let start = values.len() - window;
    let mut tail = Vec::with_capacity(window);
    for i in start..values.len() {
        let v = values.value_at(i);
        if v.is_finite() {
            tail.push(v);
        }
    }
    if tail.is_empty() {
        return None;
    }
    tail.sort_by(|a, b| a.total_cmp(b));
    let n = tail.len();
    let pos = (n - 1) as f64 * q;
    let lo = pos.floor() as usize;
    let hi = (lo + 1).min(n - 1);
    let frac = pos - lo as f64;
    let value = tail[lo] * (1.0 - frac) + tail[hi] * frac;
    if value.is_finite() {
        Some(value)
    } else {
        None
    }
}

pub fn sample_std_last(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Option<f64> {
    if window == 0 || min_periods == 0 || values.len() < min_periods {
        return None;
    }
    let start = values.len().saturating_sub(window);
    let mut tail = Vec::with_capacity(values.len() - start);
    for i in start..values.len() {
        let v = values.value_at(i);
        if v.is_finite() {
            tail.push(v);
        }
    }
    if tail.len() < min_periods || tail.len() < 2 {
        return None;
    }
    let mean = tail.iter().sum::<f64>() / tail.len() as f64;
    let var = tail
        .iter()
        .map(|v| {
            let d = *v - mean;
            d * d
        })
        .sum::<f64>()
        / (tail.len() as f64 - 1.0);
    let out = var.sqrt();
    if out.is_finite() {
        Some(out)
    } else {
        None
    }
}

pub fn corr_last_with_min_periods(
    xs: &(impl F64SeriesView + ?Sized),
    ys: &(impl F64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Option<f64> {
    if window == 0 || min_periods == 0 {
        return None;
    }
    let n = xs.len().min(ys.len());
    if n < min_periods {
        return None;
    }
    let start = n.saturating_sub(window);
    let mut x = Vec::new();
    let mut y = Vec::new();
    for i in start..n {
        let xv = xs.value_at(i);
        let yv = ys.value_at(i);
        if xv.is_finite() && yv.is_finite() {
            x.push(xv);
            y.push(yv);
        }
    }
    if x.len() < min_periods {
        return None;
    }
    let mean_x = x.iter().sum::<f64>() / x.len() as f64;
    let mean_y = y.iter().sum::<f64>() / y.len() as f64;
    let mut cov = 0.0;
    let mut var_x = 0.0;
    let mut var_y = 0.0;
    for i in 0..x.len() {
        let dx = x[i] - mean_x;
        let dy = y[i] - mean_y;
        cov += dx * dy;
        var_x += dx * dx;
        var_y += dy * dy;
    }
    if var_x.abs() <= 1e-12 || var_y.abs() <= 1e-12 {
        return Some(0.0);
    }
    let out = cov / (var_x.sqrt() * var_y.sqrt());
    finite_opt(Some(out))
}

pub fn pct_change_last(values: &(impl F64SeriesView + ?Sized), periods: usize) -> Option<f64> {
    if periods == 0 || values.len() <= periods {
        return None;
    }
    let curr = values.value_at(values.len() - 1);
    let prev = values.value_at(values.len() - 1 - periods);
    if !curr.is_finite() || !prev.is_finite() || prev.abs() <= 1e-12 {
        return Some(0.0);
    }
    let value = (curr - prev) / prev;
    if value.is_finite() {
        Some(value)
    } else {
        None
    }
}

pub fn linear_regression_predict_last(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let n = values.len() as f64;
    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_xx = 0.0;
    let mut sum_xy = 0.0;
    for (i, y) in values.iter().enumerate() {
        if !y.is_finite() {
            return Some(0.0);
        }
        let x = i as f64;
        sum_x += x;
        sum_y += *y;
        sum_xx += x * x;
        sum_xy += x * *y;
    }
    let denom = n * sum_xx - sum_x * sum_x;
    if denom.abs() <= 1e-12 {
        return Some(0.0);
    }
    let slope = (n * sum_xy - sum_x * sum_y) / denom;
    let intercept = (sum_y - slope * sum_x) / n;
    let pred = slope * (n - 1.0) + intercept;
    finite_opt(Some(pred))
}

pub fn linear_regression_intercept(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let n = values.len() as f64;
    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_xx = 0.0;
    let mut sum_xy = 0.0;
    for (i, y) in values.iter().enumerate() {
        if !y.is_finite() {
            return Some(0.0);
        }
        let x = i as f64;
        sum_x += x;
        sum_y += *y;
        sum_xx += x * x;
        sum_xy += x * *y;
    }
    let denom = n * sum_xx - sum_x * sum_x;
    if denom.abs() <= 1e-12 {
        return Some(0.0);
    }
    let slope = (n * sum_xy - sum_x * sum_y) / denom;
    let intercept = (sum_y - slope * sum_x) / n;
    if intercept.is_finite() {
        Some(intercept)
    } else {
        None
    }
}

pub fn linear_regression_slope(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let n = values.len() as f64;
    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_xx = 0.0;
    let mut sum_xy = 0.0;
    for (i, y) in values.iter().enumerate() {
        if !y.is_finite() {
            return Some(0.0);
        }
        let x = i as f64;
        sum_x += x;
        sum_y += *y;
        sum_xx += x * x;
        sum_xy += x * *y;
    }
    let denom = n * sum_xx - sum_x * sum_x;
    if denom.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((n * sum_xy - sum_x * sum_y) / denom)).or(Some(0.0))
}

pub fn rolling_weighted_mean_last(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
) -> Option<f64> {
    if window == 0 || values.len() < window {
        return None;
    }
    let start = values.len() - window;
    let mut num = 0.0;
    let mut den = 0.0;
    for i in 0..window {
        let value = values.value_at(start + i);
        if !value.is_finite() {
            return Some(0.0);
        }
        let weight = (i + 1) as f64;
        num += value * weight;
        den += weight;
    }
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(num / den)).or(Some(0.0))
}

pub fn rolling_min_last_simple(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
) -> Option<f64> {
    rolling_min_last(values, window).ok().flatten()
}

pub fn rolling_max_last_simple(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
) -> Option<f64> {
    rolling_max_last(values, window).ok().flatten()
}

pub fn rsi_last_from_series(values: &(impl F64SeriesView + ?Sized), period: usize) -> Option<f64> {
    if period == 0 || values.len() < period + 1 {
        return None;
    }
    let n = values.len();
    let mut gains = vec![0.0; n];
    let mut losses = vec![0.0; n];
    for i in 1..n {
        let diff = values.value_at(i) - values.value_at(i - 1);
        if !diff.is_finite() {
            return Some(0.0);
        }
        if diff > 0.0 {
            gains[i] = diff;
        } else if diff < 0.0 {
            losses[i] = -diff;
        }
    }
    let avg_gain = rolling_mean_last(&gains, period).ok().flatten()?;
    let avg_loss = rolling_mean_last(&losses, period).ok().flatten()?;
    if avg_loss.abs() <= 1e-12 {
        if avg_gain.abs() <= 1e-12 {
            return Some(0.0);
        }
        return Some(100.0);
    }
    let rs = avg_gain / avg_loss;
    finite_opt(Some(100.0 - (100.0 / (1.0 + rs)))).or(Some(0.0))
}

pub fn cmo_last_from_series(values: &(impl F64SeriesView + ?Sized), period: usize) -> Option<f64> {
    if period == 0 || values.len() < period + 1 {
        return None;
    }
    let n = values.len();
    let mut gains = vec![0.0; n];
    let mut losses = vec![0.0; n];
    for i in 1..n {
        let diff = values.value_at(i) - values.value_at(i - 1);
        if !diff.is_finite() {
            return Some(0.0);
        }
        if diff > 0.0 {
            gains[i] = diff;
        } else if diff < 0.0 {
            losses[i] = -diff;
        }
    }
    let avg_gain = rolling_mean_last(&gains, period).ok().flatten()?;
    let avg_loss = rolling_mean_last(&losses, period).ok().flatten()?;
    let den = avg_gain + avg_loss;
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(100.0 * (avg_gain - avg_loss) / den)).or(Some(0.0))
}

pub fn rolling_position_last(values: &(impl F64SeriesView + ?Sized), window: usize) -> Option<f64> {
    let low = rolling_min_last_simple(values, window)?;
    let high = rolling_max_last_simple(values, window)?;
    let curr = values.value_at(values.len().saturating_sub(1));
    let den = high - low;
    if !curr.is_finite() || den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((curr - low) / den)).or(Some(0.0))
}

pub fn std_pop(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sum = 0.0;
    let mut cnt = 0usize;
    for v in values {
        if v.is_finite() {
            sum += *v;
            cnt += 1;
        }
    }
    if cnt == 0 {
        return Some(0.0);
    }
    let mean = sum / cnt as f64;
    let mut var_sum = 0.0;
    for v in values {
        if v.is_finite() {
            let d = *v - mean;
            var_sum += d * d;
        }
    }
    let value = (var_sum / cnt as f64).sqrt();
    if value.is_finite() {
        Some(value)
    } else {
        Some(0.0)
    }
}

pub fn sample_cov(xs: &[f64], ys: &[f64]) -> Option<f64> {
    let n = xs.len().min(ys.len());
    if n < 2 {
        return None;
    }
    let mut pairs = Vec::with_capacity(n);
    for i in 0..n {
        let x = xs[i];
        let y = ys[i];
        if x.is_finite() && y.is_finite() {
            pairs.push((x, y));
        }
    }
    if pairs.len() < 2 {
        return Some(0.0);
    }
    let mean_x = pairs.iter().map(|(x, _)| *x).sum::<f64>() / pairs.len() as f64;
    let mean_y = pairs.iter().map(|(_, y)| *y).sum::<f64>() / pairs.len() as f64;
    let cov_sum = pairs
        .iter()
        .map(|(x, y)| (x - mean_x) * (y - mean_y))
        .sum::<f64>();
    finite_opt(Some(cov_sum / (pairs.len() - 1) as f64)).or(Some(0.0))
}

pub fn harmonic_mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut denom = 0.0;
    for value in values {
        if !value.is_finite() || *value <= 0.0 {
            return Some(0.0);
        }
        denom += 1.0 / *value;
    }
    if denom.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(values.len() as f64 / denom))
}

pub fn weighted_harmonic_with_index_weights(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut denom = 0.0;
    for (idx, value) in values.iter().enumerate() {
        if !value.is_finite() || *value <= 0.0 {
            return Some(0.0);
        }
        denom += (idx + 1) as f64 / *value;
    }
    if denom.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(values.len() as f64 / denom))
}

pub fn median_from_iter<I>(iter: I) -> Option<f64>
where
    I: IntoIterator<Item = f64>,
{
    let mut values: Vec<f64> = iter.into_iter().filter(|v| v.is_finite()).collect();
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.total_cmp(b));
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        finite_opt(Some((values[mid - 1] + values[mid]) / 2.0))
    } else {
        finite_opt(Some(values[mid]))
    }
}

pub fn last_opt(values: &(impl OptF64SeriesView + ?Sized)) -> Option<f64> {
    if values.len() == 0 {
        return None;
    }
    finite_opt(values.value_at(values.len() - 1)).or(Some(0.0))
}
