//! Rolling window primitives backed by Polars.

use anyhow::Result;
use polars::lazy::dsl::pearson_corr;
use polars::prelude::{
    col, DataFrame, IntoLazy, NamedFrom, RankMethod, RankOptions, RollingOptionsFixedWindow,
    RollingSeries, Series,
};
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

fn last_f64(series: &Series) -> Option<f64> {
    let idx = series.len().saturating_sub(1);
    finite_or_none(series.f64().ok()?.get(idx)).or(Some(0.0))
}

pub fn rolling_mean_last(values: &[f64], window: usize) -> Result<Option<f64>> {
    if window == 0 || values.len() < window {
        return Ok(None);
    }
    let rolling = series_from(values).rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        ..Default::default()
    })?;
    Ok(last_f64(&rolling))
}

pub fn rolling_mean_last_with_min_periods(
    values: &[f64],
    window: usize,
    min_periods: usize,
) -> Result<Option<f64>> {
    if window == 0 || min_periods == 0 || values.len() < min_periods {
        return Ok(None);
    }
    let rolling = series_from(values).rolling_mean(RollingOptionsFixedWindow {
        window_size: window,
        min_periods,
        ..Default::default()
    })?;
    Ok(last_f64(&rolling))
}

pub fn rolling_sum_last_with_min_periods(
    values: &[f64],
    window: usize,
    min_periods: usize,
) -> Result<Option<f64>> {
    if window == 0 || min_periods == 0 || values.len() < min_periods {
        return Ok(None);
    }
    let rolling = series_from(values).rolling_sum(RollingOptionsFixedWindow {
        window_size: window,
        min_periods,
        ..Default::default()
    })?;
    Ok(last_f64(&rolling))
}

pub fn rolling_std_last(values: &[f64], window: usize) -> Result<Option<f64>> {
    if window == 0 || values.len() < window {
        return Ok(None);
    }
    let rolling = series_from(values).rolling_std(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        ..Default::default()
    })?;
    Ok(last_f64(&rolling))
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
    let valid_count = tail.iter().filter(|v| v.is_some()).count();
    if valid_count < min_periods {
        return Ok(None);
    }
    let df = DataFrame::new(vec![series_from_opt(tail).into()])?;
    let out = df
        .lazy()
        .select([col("x").skew(bias).alias("skew")])
        .collect()?;
    let value = out.column("skew")?.f64()?.get(0);
    Ok(finite_or_none(value))
}

pub fn rolling_min_last(values: &[f64], window: usize) -> Result<Option<f64>> {
    if window == 0 || values.len() < window {
        return Ok(None);
    }
    let rolling = series_from(values).rolling_min(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        ..Default::default()
    })?;
    Ok(last_f64(&rolling))
}

pub fn rolling_max_last(values: &[f64], window: usize) -> Result<Option<f64>> {
    if window == 0 || values.len() < window {
        return Ok(None);
    }
    let rolling = series_from(values).rolling_max(RollingOptionsFixedWindow {
        window_size: window,
        min_periods: window,
        ..Default::default()
    })?;
    Ok(last_f64(&rolling))
}

pub fn rolling_skew_last(values: &[f64], window: usize, bias: bool) -> Result<Option<f64>> {
    if window == 0 || values.len() < window {
        return Ok(None);
    }
    let rolling = series_from(values).rolling_skew(window, bias)?;
    Ok(last_f64(&rolling).or(Some(0.0)))
}

pub fn rolling_kurt_last(
    values: &[f64],
    window: usize,
    fisher: bool,
    bias: bool,
) -> Result<Option<f64>> {
    if window == 0 || values.len() < window {
        return Ok(None);
    }
    let tail = &values[values.len() - window..];
    let df = DataFrame::new(vec![series_from(tail).into()])?;
    let out = df
        .lazy()
        .select([col("x").kurtosis(fisher, bias).alias("kurt")])
        .collect()?;
    let value = out.column("kurt")?.f64()?.get(0);
    Ok(finite_or_none(value).or(Some(0.0)))
}

pub fn rolling_rank_last(values: &[f64], window: usize) -> Result<Option<f64>> {
    if window == 0 || values.len() < window {
        return Ok(None);
    }
    let tail = &values[values.len() - window..];
    let df = DataFrame::new(vec![series_from(tail).into()])?;
    let out = df
        .lazy()
        .select([col("x")
            .rank(
                RankOptions {
                    method: RankMethod::Average,
                    descending: false,
                },
                None,
            )
            .last()
            .alias("rank_last")])
        .collect()?;
    let value = out.column("rank_last")?.f64()?.get(0);
    Ok(finite_or_none(value))
}

pub fn rolling_corr_last(xs: &[f64], ys: &[f64], window: usize, ddof: u8) -> Result<Option<f64>> {
    if window == 0 {
        return Ok(None);
    }
    let n = xs.len().min(ys.len());
    if n < window {
        return Ok(None);
    }
    let x_tail = &xs[n - window..n];
    let y_tail = &ys[n - window..n];
    let df = DataFrame::new(vec![
        Series::new("x".into(), x_tail.to_vec()).into(),
        Series::new("y".into(), y_tail.to_vec()).into(),
    ])?;
    let out = df
        .lazy()
        .select([pearson_corr(col("x"), col("y"), ddof).alias("corr")])
        .collect()?;
    let value = out.column("corr")?.f64()?.get(0);
    Ok(finite_or_none(value))
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
