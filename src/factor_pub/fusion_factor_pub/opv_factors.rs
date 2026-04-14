use std::f64::consts::PI;

use factor_engine::math::{
    finite_opt, last_opt, pct_change_last, rolling_corr_last, rolling_max_last, rolling_mean_last,
    rolling_mean_last_opt_from_series, rolling_mean_last_with_min_periods, rolling_mean_series,
    rolling_mean_series_opt, rolling_min_last, rolling_rank_last, rolling_sum_last_from_series,
    rolling_sum_last_opt_from_series, rolling_sum_last_with_min_periods, rolling_sum_series,
    rolling_sum_series_opt, sample_std_last,
};
use factor_engine::view::{F64SeriesView, SymbolSeries};

use super::app::FusionFactorPubApp;
use super::factor_enum::FusionFactorId;
use super::plan::FactorBinding;

macro_rules! dispatch_opv {
    ($binding:expr, $series:expr, { $($variant:ident => $func:ident,)* }) => {
        match $binding.factor_id {
            $(Some(FusionFactorId::$variant) => {
                Some(FusionFactorPubApp::wrap_factor_value($series.and_then($func)))
            })*
            _ => None,
        }
    };
}

pub(super) fn compute_supported_opv_factor(
    binding: &FactorBinding,
    series: Option<&SymbolSeries<'_>>,
) -> Option<(f64, bool, &'static str)> {
    dispatch_opv!(binding, series, {
        TdTi001 => compute_td_ti_001,
        TdTi002 => compute_td_ti_002,
        TdTi003 => compute_td_ti_003,
        TdTi004 => compute_td_ti_004,
        TdTi005 => compute_td_ti_005,
        TdTi006 => compute_td_ti_006,
        TdTi007 => compute_td_ti_007,
        TdTi008 => compute_td_ti_008,
        TdTi009 => compute_td_ti_009,
        TdTi010 => compute_td_ti_010,
        TdTi011 => compute_td_ti_011,
        TdTi012 => compute_td_ti_012,
        TdTi013 => compute_td_ti_013,
        TdTi014 => compute_td_ti_014,
        TdTi015 => compute_td_ti_015,
        TdTi016 => compute_td_ti_016,
        TdTi017 => compute_td_ti_017,
        TdTi018 => compute_td_ti_018,
        TdTi019 => compute_td_ti_019,
        TdTi020 => compute_td_ti_020,
        TdTi021 => compute_td_ti_021,
        TdTi022 => compute_td_ti_022,
        TdTi023 => compute_td_ti_023,
        TdTi024 => compute_td_ti_024,
        TdTi025 => compute_td_ti_025,
        TdTi026 => compute_td_ti_026,
        TdTi027 => compute_td_ti_027,
        TdTi028 => compute_td_ti_028,
        TdTi029 => compute_td_ti_029,
        TdTi030 => compute_td_ti_030,
        TdTi031 => compute_td_ti_031,
        TdTi032 => compute_td_ti_032,
        TdTi033 => compute_td_ti_033,
        TdTi034 => compute_td_ti_034,
        TdTi035 => compute_td_ti_035,
        TdTi036 => compute_td_ti_036,
        TdTi037 => compute_td_ti_037,
        TdTi038 => compute_td_ti_038,
        TdTi039 => compute_td_ti_039,
        TdTi040 => compute_td_ti_040,
        TdTi041 => compute_td_ti_041,
        TdTi042 => compute_td_ti_042,
        TdTi043 => compute_td_ti_043,
        TdTi044 => compute_td_ti_044,
        TdTi045 => compute_td_ti_045,
        TdMt001 => compute_td_mt_001,
        TdMt002 => compute_td_mt_002,
        TdMt003 => compute_td_mt_003,
        TdMt004 => compute_td_mt_004,
        TdMt005 => compute_td_mt_005,
        TdMt006 => compute_td_mt_006,
        TdMt007 => compute_td_mt_007,
        TdMt008 => compute_td_mt_008,
        TdMt009 => compute_td_mt_009,
        TdMt010 => compute_td_mt_010,
        TdMt011 => compute_td_mt_011,
        TdMt012 => compute_td_mt_012,
        TdMt013 => compute_td_mt_013,
        TdMt014 => compute_td_mt_014,
        TdMt015 => compute_td_mt_015,
        TdMt016 => compute_td_mt_016,
        TdMt017 => compute_td_mt_017,
        TdMt018 => compute_td_mt_018,
        TdMt019 => compute_td_mt_019,
        TdMt020 => compute_td_mt_020,
        TdMt021 => compute_td_mt_021,
        TdMt022 => compute_td_mt_022,
        TdMt023 => compute_td_mt_023,
        TdMt024 => compute_td_mt_024,
        TdMt025 => compute_td_mt_025,
        TdMt026 => compute_td_mt_026,
        TdMt027 => compute_td_mt_027,
        TdMt028 => compute_td_mt_028,
        TdMt029 => compute_td_mt_029,
        TdMt030 => compute_td_mt_030,
        TdMt031 => compute_td_mt_031,
        TdMt032 => compute_td_mt_032,
        TdMt033 => compute_td_mt_033,
        TdMt034 => compute_td_mt_034,
        TdMt035 => compute_td_mt_035,
        TdMt036 => compute_td_mt_036,
        TdMt037 => compute_td_mt_037,
        TdMt038 => compute_td_mt_038,
        TdMt039 => compute_td_mt_039,
        TdMt040 => compute_td_mt_040,
        TdMt042 => compute_td_mt_042,
        TdMt041 => compute_td_mt_041,
        TdMt043 => compute_td_mt_043,
        TdMt044 => compute_td_mt_044,
        TpVpi001 => compute_tp_vpi_001,
        TpVpi002 => compute_tp_vpi_002,
        TpVpi003 => compute_tp_vpi_003,
        TpVpi004 => compute_tp_vpi_004,
        TpVpi005 => compute_tp_vpi_005,
        TpVpi006 => compute_tp_vpi_006,
        TpVpi007 => compute_tp_vpi_007,
        TpVpi008 => compute_tp_vpi_008,
        TpVpi009 => compute_tp_vpi_009,
        TpVpi010 => compute_tp_vpi_010,
        TpVpi011 => compute_tp_vpi_011,
        TpVpi012 => compute_tp_vpi_012,
        TpVpi013 => compute_tp_vpi_013,
        TpVpi014 => compute_tp_vpi_014,
        TpVpi015 => compute_tp_vpi_015,
        TpVpi016 => compute_tp_vpi_016,
        TpVpi017 => compute_tp_vpi_017,
        TpVpi018 => compute_tp_vpi_018,
        TpVpi019 => compute_tp_vpi_019,
        TdVi001 => compute_td_vi_001,
        TdVi002 => compute_td_vi_002,
        TdVi003 => compute_td_vi_003,
        TdVi004 => compute_td_vi_004,
        TdVi005 => compute_td_vi_005,
        TdVi006 => compute_td_vi_006,
        TdVi007 => compute_td_vi_007,
        TdVi008 => compute_td_vi_008,
        TdVi009 => compute_td_vi_009,
        TdVi010 => compute_td_vi_010,
        TdVi011 => compute_td_vi_011,
        TdVi012 => compute_td_vi_012,
        TdVi013 => compute_td_vi_013,
        TdVi014 => compute_td_vi_014,
        TdVi015 => compute_td_vi_015,
        TdVi016 => compute_td_vi_016,
        TdVi017 => compute_td_vi_017,
        TdVi018 => compute_td_vi_018,
        TdVi020 => compute_td_vi_020,
        TdVi021 => compute_td_vi_021,
        TdVi022 => compute_td_vi_022,
        TdVi023 => compute_td_vi_023,
        TdVi024 => compute_td_vi_024,
        TdVi025 => compute_td_vi_025,
        TdVi026 => compute_td_vi_026,
        TdVi027 => compute_td_vi_027,
        TdVi028 => compute_td_vi_028,
        TdVi029 => compute_td_vi_029,
        TdPt001 => compute_td_pt_001,
        TdPt002 => compute_td_pt_002,
        TdPt003 => compute_td_pt_003,
        TdPt004 => compute_td_pt_004,
        TdPt005 => compute_td_pt_005,
        TdPt006 => compute_td_pt_006,
        TdPt007 => compute_td_pt_007,
        TdPt008 => compute_td_pt_008,
        TdPt009 => compute_td_pt_009,
        TdPt010 => compute_td_pt_010,
        TdPt011 => compute_td_pt_011,
        TdPt012 => compute_td_pt_012,
        TdPt013 => compute_td_pt_013,
        TdPt014 => compute_td_pt_014,
        TdPt015 => compute_td_pt_015,
        TdPt016 => compute_td_pt_016,
        TdPt017 => compute_td_pt_017,
        TdPt018 => compute_td_pt_018,
        TdPt019 => compute_td_pt_019,
        TdPt020 => compute_td_pt_020,
        TdPt021 => compute_td_pt_021,
        TdPt022 => compute_td_pt_022,
        TdPt023 => compute_td_pt_023,
        TdPt024 => compute_td_pt_024,
        TdPt025 => compute_td_pt_025,
        TdPt026 => compute_td_pt_026,
        TdPt027 => compute_td_pt_027,
        TdPt028 => compute_td_pt_028,
        TdPt029 => compute_td_pt_029,
        TdCi001 => compute_td_ci_001,
        TdCi002 => compute_td_ci_002,
        TdCi003 => compute_td_ci_003,
        TdCi004 => compute_td_ci_004,
        TdCi005 => compute_td_ci_005,
        TdCi006 => compute_td_ci_006,
        TdCi007 => compute_td_ci_007,
        TdCi008 => compute_td_ci_008,
        TdCi009 => compute_td_ci_009,
        TdCi010 => compute_td_ci_010,
        TdSi001 => compute_td_si_001,
        TdSi002 => compute_td_si_002,
        TdSi003 => compute_td_si_003,
        TdSi004 => compute_td_si_004,
        TdSi005 => compute_td_si_005,
        TdSi006 => compute_td_si_006,
        TdSi007 => compute_td_si_007,
        TdSi008 => compute_td_si_008,
        TdSi009 => compute_td_si_009,
        TdSi010 => compute_td_si_010,
        TdSi011 => compute_td_si_011,
        TdSi012 => compute_td_si_012,
        TdSi013 => compute_td_si_013,
        TdPr001 => compute_td_pr_001,
        TdPr002 => compute_td_pr_002,
        TdPr003 => compute_td_pr_003,
        TdPr004 => compute_td_pr_004,
        TdPr005 => compute_td_pr_005,
        TdPr006 => compute_td_pr_006,
        TdPr007 => compute_td_pr_007,
        TdPr008 => compute_td_pr_008,
        TdPr009 => compute_td_pr_009,
        TdPr010 => compute_td_pr_010,
        TdPr011 => compute_td_pr_011,
        TdPr012 => compute_td_pr_012,
        TdPr013 => compute_td_pr_013,
        TdPr014 => compute_td_pr_014,
        TdPr015 => compute_td_pr_015,
        TdPr016 => compute_td_pr_016,
        TdPr017 => compute_td_pr_017,
    })
}

fn collect_vec(values: &(impl F64SeriesView + ?Sized)) -> Vec<f64> {
    (0..values.len()).map(|i| values.value_at(i)).collect()
}

fn current(values: &(impl F64SeriesView + ?Sized)) -> Option<f64> {
    if values.len() == 0 {
        None
    } else {
        finite_opt(Some(values.value_at(values.len() - 1)))
    }
}

fn shift_last(values: &(impl F64SeriesView + ?Sized), periods: usize) -> Option<f64> {
    if values.len() <= periods {
        None
    } else {
        finite_opt(Some(values.value_at(values.len() - 1 - periods)))
    }
}

fn diff_last(values: &(impl F64SeriesView + ?Sized), periods: usize) -> Option<f64> {
    let curr = current(values)?;
    let prev = shift_last(values, periods)?;
    finite_opt(Some(curr - prev))
}

fn ratio(num: f64, den: f64) -> Option<f64> {
    if den.abs() <= 1e-12 {
        return None;
    }
    finite_opt(Some(num / den))
}

fn rolling_mean_exact(values: &(impl F64SeriesView + ?Sized), window: usize) -> Option<f64> {
    rolling_mean_last(values, window).ok().flatten()
}

fn rolling_mean_min(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Option<f64> {
    rolling_mean_last_with_min_periods(values, window, min_periods)
        .ok()
        .flatten()
}

fn rolling_sum_exact(values: &(impl F64SeriesView + ?Sized), window: usize) -> Option<f64> {
    rolling_sum_last_from_series(values, window, window)
        .ok()
        .flatten()
}

fn rolling_sum_min(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Option<f64> {
    rolling_sum_last_with_min_periods(values, window, min_periods)
        .ok()
        .flatten()
}

fn rolling_mean_opt_exact(values: &[Option<f64>], window: usize) -> Option<f64> {
    rolling_mean_last_opt_from_series(values, window, window)
        .ok()
        .flatten()
}

fn rolling_sum_opt_exact(values: &[Option<f64>], window: usize) -> Option<f64> {
    rolling_sum_last_opt_from_series(values, window, window)
        .ok()
        .flatten()
}

fn rolling_sum_opt_min(values: &[Option<f64>], window: usize, min_periods: usize) -> Option<f64> {
    rolling_sum_last_opt_from_series(values, window, min_periods)
        .ok()
        .flatten()
}

fn rolling_std_opt_last(values: &[Option<f64>], window: usize, min_periods: usize) -> Option<f64> {
    if window == 0 || min_periods == 0 || values.is_empty() {
        return None;
    }
    let start = values.len().saturating_sub(window);
    let mut tail = Vec::new();
    for value in values.iter().skip(start).flatten() {
        if value.is_finite() {
            tail.push(*value);
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
    finite_opt(Some(var.sqrt()))
}

fn rolling_std_series_opt(
    values: &[Option<f64>],
    window: usize,
    min_periods: usize,
) -> Vec<Option<f64>> {
    values
        .iter()
        .enumerate()
        .map(|(idx, _)| rolling_std_opt_last(&values[..=idx], window, min_periods))
        .collect()
}

fn rolling_min_min(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Option<f64> {
    if values.len() < min_periods || window == 0 {
        return None;
    }
    let start = values.len().saturating_sub(window);
    let mut out = None::<f64>;
    for i in start..values.len() {
        let value = values.value_at(i);
        if !value.is_finite() {
            continue;
        }
        out = Some(match out {
            Some(curr) => curr.min(value),
            None => value,
        });
    }
    out
}

fn rolling_max_min(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Option<f64> {
    if values.len() < min_periods || window == 0 {
        return None;
    }
    let start = values.len().saturating_sub(window);
    let mut out = None::<f64>;
    for i in start..values.len() {
        let value = values.value_at(i);
        if !value.is_finite() {
            continue;
        }
        out = Some(match out {
            Some(curr) => curr.max(value),
            None => value,
        });
    }
    out
}

fn diff_series(values: &(impl F64SeriesView + ?Sized), periods: usize) -> Vec<Option<f64>> {
    (0..values.len())
        .map(|i| {
            if i < periods {
                None
            } else {
                finite_opt(Some(values.value_at(i) - values.value_at(i - periods)))
            }
        })
        .collect()
}

fn pct_change_series(values: &(impl F64SeriesView + ?Sized), periods: usize) -> Vec<Option<f64>> {
    (0..values.len())
        .map(|i| {
            if i < periods {
                None
            } else {
                let prev = values.value_at(i - periods);
                let curr = values.value_at(i);
                ratio(curr - prev, prev)
            }
        })
        .collect()
}

fn pct_change_last_opt(values: &[Option<f64>], periods: usize) -> Option<f64> {
    if values.len() <= periods {
        return None;
    }
    let curr = values[values.len() - 1]?;
    let prev = values[values.len() - 1 - periods]?;
    ratio(curr - prev, prev)
}

fn zip_map2(
    a: &(impl F64SeriesView + ?Sized),
    b: &(impl F64SeriesView + ?Sized),
    f: impl Fn(f64, f64) -> f64,
) -> Vec<f64> {
    let n = a.len().min(b.len());
    (0..n).map(|i| f(a.value_at(i), b.value_at(i))).collect()
}

fn zip_map2_opt(
    a: &(impl F64SeriesView + ?Sized),
    b: &(impl F64SeriesView + ?Sized),
    f: impl Fn(f64, f64) -> Option<f64>,
) -> Vec<Option<f64>> {
    let n = a.len().min(b.len());
    (0..n).map(|i| f(a.value_at(i), b.value_at(i))).collect()
}

fn zip_map3_opt(
    a: &(impl F64SeriesView + ?Sized),
    b: &(impl F64SeriesView + ?Sized),
    c: &(impl F64SeriesView + ?Sized),
    f: impl Fn(f64, f64, f64) -> Option<f64>,
) -> Vec<Option<f64>> {
    let n = a.len().min(b.len()).min(c.len());
    (0..n)
        .map(|i| f(a.value_at(i), b.value_at(i), c.value_at(i)))
        .collect()
}

fn binary_opt_vec(
    a: &[Option<f64>],
    b: &[Option<f64>],
    f: impl Fn(f64, f64) -> Option<f64>,
) -> Vec<Option<f64>> {
    let n = a.len().min(b.len());
    (0..n)
        .map(|i| match (a[i], b[i]) {
            (Some(x), Some(y)) => f(x, y),
            _ => None,
        })
        .collect()
}

fn unary_opt_vec(values: &[Option<f64>], f: impl Fn(f64) -> Option<f64>) -> Vec<Option<f64>> {
    values.iter().map(|value| value.and_then(&f)).collect()
}

fn ewm_mean_series_from_com(values: &(impl F64SeriesView + ?Sized), com: f64) -> Vec<Option<f64>> {
    if values.len() == 0 {
        return Vec::new();
    }
    let alpha = 1.0 / (1.0 + com);
    let decay = 1.0 - alpha;
    let mut weighted_sum = values.value_at(0);
    let mut weight = 1.0;
    let mut out = vec![finite_opt(Some(weighted_sum))];
    for i in 1..values.len() {
        let value = values.value_at(i);
        weighted_sum = value + decay * weighted_sum;
        weight = 1.0 + decay * weight;
        out.push(finite_opt(Some(weighted_sum / weight)));
    }
    out
}

fn recursive_kama_like_series(values: &(impl F64SeriesView + ?Sized), period: usize) -> Vec<f64> {
    let n = values.len();
    if n == 0 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(n);
    let mut prev = values.value_at(0);
    out.push(prev);
    for i in 1..n {
        let curr = values.value_at(i);
        prev = prev + (curr - prev) / period as f64;
        out.push(prev);
    }
    out
}

fn weighted_mean_last(values: &(impl F64SeriesView + ?Sized), window: usize) -> Option<f64> {
    if window == 0 || values.len() < window {
        return None;
    }
    let start = values.len() - window;
    let mut num = 0.0;
    let mut den = 0.0;
    for offset in 0..window {
        let w = (offset + 1) as f64;
        let v = values.value_at(start + offset);
        num += v * w;
        den += w;
    }
    ratio(num, den)
}

fn rolling_argmax_pos_last(values: &(impl F64SeriesView + ?Sized), window: usize) -> Option<f64> {
    if window == 0 || values.len() < window {
        return None;
    }
    let start = values.len() - window;
    let mut best_idx = 0usize;
    let mut best = values.value_at(start);
    for offset in 1..window {
        let value = values.value_at(start + offset);
        if value > best {
            best = value;
            best_idx = offset;
        }
    }
    Some(best_idx as f64)
}

fn rolling_argmin_pos_last(values: &(impl F64SeriesView + ?Sized), window: usize) -> Option<f64> {
    if window == 0 || values.len() < window {
        return None;
    }
    let start = values.len() - window;
    let mut best_idx = 0usize;
    let mut best = values.value_at(start);
    for offset in 1..window {
        let value = values.value_at(start + offset);
        if value < best {
            best = value;
            best_idx = offset;
        }
    }
    Some(best_idx as f64)
}

fn sign_close_open(close: f64, open: f64) -> f64 {
    if close > open {
        1.0
    } else if close < open {
        -1.0
    } else {
        0.0
    }
}

fn clv_series(series: &SymbolSeries<'_>) -> Vec<Option<f64>> {
    zip_map3_opt(
        &series.close,
        &series.low,
        &series.high,
        |close, low, high| ratio((close - low) - (high - close), high - low),
    )
}

fn tr_series(series: &SymbolSeries<'_>) -> Vec<Option<f64>> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        if i == 0 {
            out.push(None);
            continue;
        }
        let high = series.high.value_at(i);
        let low = series.low.value_at(i);
        let prev_close = series.close.value_at(i - 1);
        let tr = (high - low)
            .max((high - prev_close).abs())
            .max((low - prev_close).abs());
        out.push(finite_opt(Some(tr)));
    }
    out
}

fn bp_series(series: &SymbolSeries<'_>) -> Vec<Option<f64>> {
    let n = series.close.len().min(series.low.len());
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        if i == 0 {
            out.push(None);
            continue;
        }
        let close = series.close.value_at(i);
        let low = series.low.value_at(i);
        let prev_close = series.close.value_at(i - 1);
        out.push(finite_opt(Some(close - low.min(prev_close))));
    }
    out
}

fn plus_dm_series(series: &SymbolSeries<'_>) -> Vec<f64> {
    let n = series.high.len().min(series.low.len());
    let mut out = Vec::with_capacity(n);
    out.push(0.0);
    for i in 1..n {
        let high_diff = series.high.value_at(i) - series.high.value_at(i - 1);
        let low_diff = series.low.value_at(i) - series.low.value_at(i - 1);
        out.push(if high_diff > low_diff { high_diff } else { 0.0 });
    }
    out
}

fn minus_dm_series(series: &SymbolSeries<'_>) -> Vec<f64> {
    let n = series.high.len().min(series.low.len());
    let mut out = Vec::with_capacity(n);
    out.push(0.0);
    for i in 1..n {
        let high_diff = series.high.value_at(i) - series.high.value_at(i - 1);
        let low_diff = series.low.value_at(i) - series.low.value_at(i - 1);
        out.push(if low_diff > high_diff { low_diff } else { 0.0 });
    }
    out
}

fn aroon_up_last(values: &(impl F64SeriesView + ?Sized), timeperiod: usize) -> Option<f64> {
    let pos = rolling_argmax_pos_last(values, timeperiod + 1)?;
    finite_opt(Some(100.0 * pos / timeperiod as f64))
}

fn aroon_down_last(values: &(impl F64SeriesView + ?Sized), timeperiod: usize) -> Option<f64> {
    let pos = rolling_argmin_pos_last(values, timeperiod + 1)?;
    finite_opt(Some(100.0 * pos / timeperiod as f64))
}

fn body_last(series: &SymbolSeries<'_>) -> Option<f64> {
    let close = current(&series.close)?;
    let open = current(&series.open)?;
    finite_opt(Some((close - open).abs()))
}

fn upper_shadow_last(series: &SymbolSeries<'_>) -> Option<f64> {
    let high = current(&series.high)?;
    let close = current(&series.close)?;
    let open = current(&series.open)?;
    finite_opt(Some(high - close.max(open)))
}

fn lower_shadow_last(series: &SymbolSeries<'_>) -> Option<f64> {
    let low = current(&series.low)?;
    let close = current(&series.close)?;
    let open = current(&series.open)?;
    finite_opt(Some(close.min(open) - low))
}

fn compute_td_ti_001(series: &SymbolSeries<'_>) -> Option<f64> {
    rolling_mean_exact(&series.close, 5)
}

fn compute_td_ti_002(series: &SymbolSeries<'_>) -> Option<f64> {
    let hl2 = zip_map2(&series.high, &series.low, |h, l| (h + l) / 2.0);
    rolling_mean_exact(&hl2, 500)
}

fn compute_td_ti_005(series: &SymbolSeries<'_>) -> Option<f64> {
    let short = rolling_mean_exact(&series.close, 9)?;
    let long = rolling_mean_exact(&series.close, 25)?;
    finite_opt(Some(short - long))
}

fn compute_td_ti_006(series: &SymbolSeries<'_>) -> Option<f64> {
    rolling_mean_exact(&series.close, 120)
}

fn compute_td_ti_008(series: &SymbolSeries<'_>) -> Option<f64> {
    let hl = zip_map2_opt(&series.high, &series.low, ratio);
    rolling_mean_opt_exact(&hl, 120)
}

fn compute_td_ti_009(series: &SymbolSeries<'_>) -> Option<f64> {
    let ema = rolling_mean_exact(&series.close, 20)?;
    let sma_high = rolling_mean_exact(&series.high, 20)?;
    let sma_low = rolling_mean_exact(&series.low, 20)?;
    finite_opt(Some(ema + (sma_high - sma_low)))
}

fn compute_td_ti_010(series: &SymbolSeries<'_>) -> Option<f64> {
    let ema1 = rolling_mean_series(&series.close, 30, 30).ok()?;
    let ema2 = rolling_mean_series_opt(&ema1, 30, 30).ok()?;
    let ema3 = rolling_mean_series_opt(&ema2, 30, 30).ok()?;
    last_opt(&ema3)
}

fn compute_td_ti_011(series: &SymbolSeries<'_>) -> Option<f64> {
    let ema = rolling_mean_series(&series.close, 14, 14).ok()?;
    let ema2 = rolling_mean_series_opt(&ema, 14, 14).ok()?;
    match (last_opt(&ema), last_opt(&ema2)) {
        (Some(v1), Some(v2)) => finite_opt(Some(2.0 * v1 - v2)),
        _ => None,
    }
}

fn compute_td_ti_013(series: &SymbolSeries<'_>) -> Option<f64> {
    let pv = zip_map2(&series.close, &series.volume, |c, v| c * v);
    rolling_mean_exact(&pv, 30)
}

fn compute_td_ti_014(series: &SymbolSeries<'_>) -> Option<f64> {
    let middle = rolling_mean_exact(&series.close, 20)?;
    let std = sample_std_last(&series.close, 20, 20)?;
    finite_opt(Some(middle + 2.0 * std))
}

fn compute_td_ti_015(series: &SymbolSeries<'_>) -> Option<f64> {
    let middle_band = zip_map2_opt(&series.close, &series.open, ratio);
    let std1 = rolling_std_series_opt(&middle_band, 20, 20);
    let hl_ratio = zip_map2_opt(&series.high, &series.low, ratio);
    let std2 = rolling_std_series_opt(&hl_ratio, 20, 20);
    let diff = binary_opt_vec(&std1, &std2, |a, b| finite_opt(Some(a - b)));
    rolling_sum_opt_exact(&diff, 20)
}

fn compute_td_ti_016(series: &SymbolSeries<'_>) -> Option<f64> {
    let middle = rolling_mean_exact(&series.close, 20)?;
    let std = sample_std_last(&series.close, 20, 20)?;
    finite_opt(Some(middle - 2.0 * std))
}

fn compute_td_ti_017(series: &SymbolSeries<'_>) -> Option<f64> {
    let diff = diff_series(&series.close, 1);
    let ema = rolling_mean_opt_exact(&diff, 20)?;
    let stddev = rolling_std_opt_last(&diff, 20, 20)?;
    ratio(ema, stddev)
}

fn compute_td_ti_018(series: &SymbolSeries<'_>) -> Option<f64> {
    let pct = pct_change_series(&series.open, 5);
    let ema = rolling_mean_opt_exact(&pct, 20)?;
    let stddev = rolling_std_opt_last(&pct, 20, 20)?;
    ratio(ema, stddev)
}

fn compute_td_ti_019(series: &SymbolSeries<'_>) -> Option<f64> {
    let std = sample_std_last(&series.close, 20, 20)?;
    finite_opt(Some(4.0 * std))
}

fn compute_td_ti_020(series: &SymbolSeries<'_>) -> Option<f64> {
    let typical = zip_map3_opt(&series.high, &series.low, &series.close, |h, l, c| {
        finite_opt(Some((h + l + c) / 3.0))
    });
    let std = rolling_std_opt_last(&typical, 5, 5)?;
    finite_opt(Some(4.0 * std))
}

fn compute_td_ti_021(series: &SymbolSeries<'_>) -> Option<f64> {
    let kama = recursive_kama_like_series(&series.close, 30);
    rolling_mean_exact(&kama, 30)
}

fn compute_td_ti_022(series: &SymbolSeries<'_>) -> Option<f64> {
    let kama = recursive_kama_like_series(&series.amount, 14);
    rolling_mean_exact(&kama, 14)
}

fn compute_td_ti_023(series: &SymbolSeries<'_>) -> Option<f64> {
    let sma_high = rolling_mean_min(&series.high, 14, 1)?;
    let sma_low = rolling_mean_min(&series.low, 14, 1)?;
    finite_opt(Some(sma_high - sma_low))
}

fn compute_td_ti_024(series: &SymbolSeries<'_>) -> Option<f64> {
    let kama = ewm_mean_series_from_com(&series.close, 14.5);
    let curr = current(&series.close)?;
    let trend = last_opt(&kama)?;
    finite_opt(Some(curr - trend))
}

fn compute_td_ti_025(series: &SymbolSeries<'_>) -> Option<f64> {
    let high = rolling_max_last(&series.high, 14).ok().flatten()?;
    let low = rolling_min_last(&series.low, 14).ok().flatten()?;
    finite_opt(Some((high + low) / 2.0))
}

fn compute_td_ti_026(series: &SymbolSeries<'_>) -> Option<f64> {
    let close = current(&series.close)?;
    let sar = if series.close.len() > 1 {
        let prev_close = shift_last(&series.close, 1)?;
        if prev_close < close {
            current(&series.high)?
        } else {
            current(&series.low)?
        }
    } else {
        current(&series.low)?
    };
    finite_opt(Some(close - sar))
}

fn compute_td_ti_027(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series
        .close
        .len()
        .min(series.high.len())
        .min(series.low.len());
    let mut sar = Vec::with_capacity(n);
    for i in 0..n {
        let choose_high = i > 0 && series.close.value_at(i - 1) < series.close.value_at(i);
        sar.push(if choose_high {
            series.high.value_at(i)
        } else {
            series.low.value_at(i)
        });
    }
    let sar_roll = rolling_max_last(&sar, 14).ok().flatten()?;
    let close = current(&series.close)?;
    finite_opt(Some(close - sar_roll))
}

fn compute_td_ti_028(series: &SymbolSeries<'_>) -> Option<f64> {
    let sma1 = rolling_mean_series(&series.close, 30, 1).ok()?;
    let sma2 = rolling_mean_series_opt(&sma1, 30, 1).ok()?;
    let sma3 = rolling_mean_series_opt(&sma2, 30, 1).ok()?;
    match (last_opt(&sma1), last_opt(&sma2), last_opt(&sma3)) {
        (Some(v1), Some(v2), Some(v3)) => finite_opt(Some(3.0 * v1 - 3.0 * v2 + v3)),
        _ => None,
    }
}

fn compute_td_ti_030(series: &SymbolSeries<'_>) -> Option<f64> {
    weighted_mean_last(&series.close, 300)
}

fn compute_td_ti_031(series: &SymbolSeries<'_>) -> Option<f64> {
    rolling_corr_last(&series.large_order, &series.small_order, 60, 10)
        .ok()
        .flatten()
}

fn compute_td_ti_032(series: &SymbolSeries<'_>) -> Option<f64> {
    let range = zip_map2(&series.high, &series.low, |h, l| h - l);
    rolling_rank_last(&range, 60).ok().flatten()
}

fn compute_td_ti_033(series: &SymbolSeries<'_>) -> Option<f64> {
    rolling_corr_last(&series.open, &series.volume, 300, 100)
        .ok()
        .flatten()
}

fn compute_td_ti_034(series: &SymbolSeries<'_>) -> Option<f64> {
    let high_diff = diff_last(&series.net_buy_large, 1)?;
    let low_diff = diff_last(&series.net_buy_small, 1)?;
    ratio(high_diff, low_diff)
}

fn compute_td_ti_035(series: &SymbolSeries<'_>) -> Option<f64> {
    let curr = current(&series.net_buy_amount)?;
    let mean = rolling_mean_exact(&series.net_buy_amount, 30)?;
    ratio(curr, mean)
}

fn compute_td_ti_036(series: &SymbolSeries<'_>) -> Option<f64> {
    aroon_up_last(&series.high, 14)
}

fn compute_td_ti_037(series: &SymbolSeries<'_>) -> Option<f64> {
    aroon_down_last(&series.low, 14)
}

fn compute_td_ti_038(series: &SymbolSeries<'_>) -> Option<f64> {
    let up = aroon_up_last(&series.high, 14)?;
    let down = aroon_down_last(&series.low, 14)?;
    finite_opt(Some(up - down))
}

fn compute_td_ti_039(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    let mut osc = Vec::with_capacity(n);
    for i in 0..n {
        if i + 1 < 15 {
            osc.push(None);
            continue;
        }
        let mut max_idx = 0usize;
        let mut max_val = series.high.value_at(i + 1 - 15);
        let mut min_idx = 0usize;
        let mut min_val = series.low.value_at(i + 1 - 15);
        for offset in 1..15 {
            let high = series.high.value_at(i + 1 - 15 + offset);
            let low = series.low.value_at(i + 1 - 15 + offset);
            if high > max_val {
                max_val = high;
                max_idx = offset;
            }
            if low < min_val {
                min_val = low;
                min_idx = offset;
            }
        }
        osc.push(finite_opt(Some(
            100.0 * max_idx as f64 / 14.0 - 100.0 * min_idx as f64 / 14.0,
        )));
    }
    pct_change_last_opt(&osc, 10)
}

fn compute_td_ti_040(series: &SymbolSeries<'_>) -> Option<f64> {
    rolling_min_last(&series.low, 14).ok().flatten()
}

fn compute_td_ti_041(series: &SymbolSeries<'_>) -> Option<f64> {
    rolling_max_last(&series.high, 14).ok().flatten()
}

fn compute_td_ti_043(series: &SymbolSeries<'_>) -> Option<f64> {
    let curr = current(&series.close)?;
    let mean = rolling_mean_exact(&series.close, 30)?;
    ratio(curr, mean)
}

fn compute_td_ti_044(series: &SymbolSeries<'_>) -> Option<f64> {
    let sma5 = rolling_mean_series(&series.close, 5, 5).ok()?;
    let ht = binary_opt_vec(
        &unary_opt_vec(&sma5, |v| finite_opt(Some(-v / 3.0))),
        &{
            let close = collect_vec(&series.close);
            close
                .iter()
                .map(|v| finite_opt(Some(2.0 * *v / 3.0)))
                .collect::<Vec<Option<f64>>>()
        },
        |a, b| finite_opt(Some(a + b)),
    );
    rolling_mean_opt_exact(&ht, 5)
}

fn compute_td_ti_045(series: &SymbolSeries<'_>) -> Option<f64> {
    let diff5 = diff_series(&series.close, 5);
    let close = collect_vec(&series.close)
        .into_iter()
        .map(Some)
        .collect::<Vec<Option<f64>>>();
    let ht = binary_opt_vec(&close, &diff5, |c, d| finite_opt(Some((c - d) / 3.0)));
    rolling_mean_opt_exact(&ht, 14)
}

fn compute_td_mt_001(series: &SymbolSeries<'_>) -> Option<f64> {
    diff_last(&series.close, 10)
}

fn compute_td_mt_002(series: &SymbolSeries<'_>) -> Option<f64> {
    diff_last(&series.open, 30)
}

fn compute_td_mt_003(series: &SymbolSeries<'_>) -> Option<f64> {
    let mom = diff_series(&series.close, 60);
    rolling_mean_opt_exact(&mom, 30)
}

fn compute_td_mt_005(series: &SymbolSeries<'_>) -> Option<f64> {
    let fast = rolling_mean_min(&series.close, 12, 1)?;
    let slow = rolling_mean_min(&series.close, 26, 1)?;
    finite_opt(Some(fast - slow))
}

fn compute_td_mt_007(series: &SymbolSeries<'_>) -> Option<f64> {
    let close = collect_vec(&series.close);
    if close.len() < 14 {
        return None;
    }
    let mut gains = Vec::with_capacity(close.len());
    let mut losses = Vec::with_capacity(close.len());
    gains.push(0.0);
    losses.push(0.0);
    for i in 1..close.len() {
        let diff = close[i] - close[i - 1];
        gains.push(if diff > 0.0 { diff } else { 0.0 });
        losses.push(if diff < 0.0 { -diff } else { 0.0 });
    }
    let gain = rolling_mean_exact(&gains, 14)?;
    let loss = rolling_mean_exact(&losses, 14)?;
    let den = gain + loss;
    if den.abs() <= 1e-12 {
        return None;
    }
    finite_opt(Some(100.0 * (gain - loss) / den))
}

fn compute_td_mt_008(series: &SymbolSeries<'_>) -> Option<f64> {
    let plus_dm = plus_dm_series(series);
    let minus_dm = minus_dm_series(series);
    let tr = tr_series(series);
    let sma_tr = rolling_mean_opt_exact(&tr, 14)?;
    let plus_di = ratio(100.0 * rolling_mean_exact(&plus_dm, 14)?, sma_tr)?;
    let minus_di = ratio(100.0 * rolling_mean_exact(&minus_dm, 14)?, sma_tr)?;
    let den = plus_di + minus_di;
    if den.abs() <= 1e-12 {
        return None;
    }
    finite_opt(Some(((plus_di - minus_di).abs() / den) * 100.0))
}

fn compute_td_mt_010(series: &SymbolSeries<'_>) -> Option<f64> {
    let fast = rolling_mean_series(&series.close, 12, 1).ok()?;
    let slow = rolling_mean_series(&series.close, 26, 1).ok()?;
    let macd = binary_opt_vec(&fast, &slow, |a, b| finite_opt(Some(a - b)));
    rolling_mean_last_opt_from_series(&macd, 9, 1)
        .ok()
        .flatten()
}

fn compute_td_mt_011(series: &SymbolSeries<'_>) -> Option<f64> {
    let fast = rolling_mean_series(&series.close, 12, 1).ok()?;
    let slow = rolling_mean_series(&series.close, 26, 1).ok()?;
    let macd = binary_opt_vec(&fast, &slow, |a, b| finite_opt(Some(a - b)));
    let signal = rolling_mean_series_opt(&macd, 9, 1).ok()?;
    match (last_opt(&macd), last_opt(&signal)) {
        (Some(m), Some(s)) => finite_opt(Some(m - s)),
        _ => None,
    }
}

fn compute_td_mt_013(series: &SymbolSeries<'_>) -> Option<f64> {
    compute_td_mt_010(series)
}

fn compute_td_mt_014(series: &SymbolSeries<'_>) -> Option<f64> {
    compute_td_mt_011(series)
}

fn compute_td_mt_015(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 14 {
        return None;
    }
    let minus_dm = minus_dm_series(series);
    let tr = tr_series(series);
    let minus = rolling_mean_exact(&minus_dm, 14)?;
    let Some(tr_mean) = rolling_mean_opt_exact(&tr, 14) else {
        return Some(0.0);
    };
    if tr_mean.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(100.0 * minus / tr_mean)).or(Some(0.0))
}

fn compute_td_mt_016(series: &SymbolSeries<'_>) -> Option<f64> {
    let tp = zip_map3_opt(&series.high, &series.low, &series.close, |h, l, c| {
        finite_opt(Some((h + l + c) / 3.0))
    });
    let tp_shift = pct_change_last_opt(&tp, 1); // placeholder to align lengths
    let _ = tp_shift;
    let n = tp.len().min(series.volume.len());
    let mut pos = vec![None; n];
    let mut neg = vec![None; n];
    for i in 1..n {
        if let (Some(curr), Some(prev)) = (tp[i], tp[i - 1]) {
            let vol = series.volume.value_at(i);
            let d = curr - prev;
            pos[i] = finite_opt(Some(if d > 0.0 { d * vol } else { 0.0 }));
            neg[i] = finite_opt(Some(if d < 0.0 { -d * vol } else { 0.0 }));
        }
    }
    let pos_sum = rolling_sum_opt_exact(&pos, 14)?;
    let neg_sum = rolling_sum_opt_exact(&neg, 14)?;
    let rs = ratio(pos_sum, neg_sum)?;
    finite_opt(Some(100.0 - (100.0 / (1.0 + rs))))
}

fn compute_td_mt_017(series: &SymbolSeries<'_>) -> Option<f64> {
    let minus_dm = minus_dm_series(series);
    rolling_mean_exact(&minus_dm, 14)
}

fn compute_td_mt_018(series: &SymbolSeries<'_>) -> Option<f64> {
    let tp = zip_map3_opt(&series.high, &series.low, &series.close, |h, l, c| {
        finite_opt(Some((h + l + c) / 3.0))
    });
    let tp_curr = last_opt(&tp)?;
    let tp_sma = rolling_mean_opt_exact(&tp, 10)?;
    ratio(tp_curr - tp_sma, tp_sma)
}

fn compute_td_mt_019(series: &SymbolSeries<'_>) -> Option<f64> {
    let curr = current(&series.close)?;
    let sma = rolling_mean_exact(&series.close, 30)?;
    let std = sample_std_last(&series.close, 30, 30)?;
    ratio(curr - sma, std)
}

fn compute_td_mt_021(series: &SymbolSeries<'_>) -> Option<f64> {
    let clo_open = zip_map2(&series.close, &series.open, |c, o| c - o);
    let high_low = zip_map2(&series.high, &series.low, |h, l| h - l);
    let num = rolling_sum_exact(&clo_open, 20)?;
    let den = rolling_sum_exact(&high_low, 20)?;
    ratio(100.0 * num, den)
}

fn compute_td_mt_022(series: &SymbolSeries<'_>) -> Option<f64> {
    pct_change_last(&series.close, 1)
}

fn compute_td_mt_025(series: &SymbolSeries<'_>) -> Option<f64> {
    let range = zip_map2(&series.high, &series.low, |h, l| h - l);
    let diff = diff_series(&range, 1);
    let mut gains = Vec::with_capacity(diff.len());
    let mut losses = Vec::with_capacity(diff.len());
    for value in diff {
        match value {
            Some(v) if v > 0.0 => {
                gains.push(Some(v));
                losses.push(Some(0.0));
            }
            Some(v) if v < 0.0 => {
                gains.push(Some(0.0));
                losses.push(Some(-v));
            }
            Some(_) => {
                gains.push(Some(0.0));
                losses.push(Some(0.0));
            }
            None => {
                gains.push(Some(0.0));
                losses.push(Some(0.0));
            }
        }
    }
    let avg_gain = rolling_mean_opt_exact(&gains, 14)?;
    let avg_loss = rolling_mean_opt_exact(&losses, 14)?;
    let rs = ratio(avg_gain, avg_loss)?;
    finite_opt(Some(100.0 - (100.0 / (1.0 + rs))))
}

fn compute_td_mt_027(series: &SymbolSeries<'_>) -> Option<f64> {
    let max_close = rolling_max_last(&series.close, 10).ok().flatten()?;
    let min_close = rolling_min_last(&series.close, 10).ok().flatten()?;
    let close = current(&series.close)?;
    ratio(close - min_close, max_close - min_close)
}

fn compute_td_mt_028(series: &SymbolSeries<'_>) -> Option<f64> {
    let max_close = rolling_max_last(&series.close, 14).ok().flatten()?;
    let min_close = rolling_min_last(&series.close, 14).ok().flatten()?;
    let close = current(&series.close)?;
    ratio(close - min_close, max_close - min_close)
}

fn compute_td_mt_029(series: &SymbolSeries<'_>) -> Option<f64> {
    let max_close = rolling_max_last(&series.close, 30).ok().flatten()?;
    let min_close = rolling_min_last(&series.close, 30).ok().flatten()?;
    let close = current(&series.close)?;
    ratio(close - min_close, max_close - min_close)
}

fn compute_td_mt_030(series: &SymbolSeries<'_>) -> Option<f64> {
    pct_change_last(&series.close, 1)
}

fn compute_td_mt_032(series: &SymbolSeries<'_>) -> Option<f64> {
    let high = diff_series(&series.high, 1);
    let low = diff_series(&series.low, 1);
    let close = diff_series(&series.close, 1);
    let n = high.len().min(low.len()).min(close.len());
    let mut mean_change = Vec::with_capacity(n);
    for i in 0..n {
        match (high[i], low[i], close[i]) {
            (Some(h), Some(l), Some(c)) => {
                mean_change.push(finite_opt(Some((h.abs() + l.abs() + c.abs()) / 3.0)))
            }
            _ => mean_change.push(None),
        }
    }
    rolling_mean_opt_exact(&mean_change, 14)
}

fn compute_td_mt_034(series: &SymbolSeries<'_>) -> Option<f64> {
    let pct = pct_change_series(&series.close, 5);
    rolling_mean_opt_exact(&pct, 30)
}

fn compute_td_mt_035(series: &SymbolSeries<'_>) -> Option<f64> {
    let pct = pct_change_series(&series.close, 5);
    last_opt(&pct)
}

fn compute_td_mt_036(series: &SymbolSeries<'_>) -> Option<f64> {
    let pct = pct_change_series(&series.close, 5);
    rolling_mean_opt_exact(&pct, 5)
}

fn compute_td_mt_037(series: &SymbolSeries<'_>) -> Option<f64> {
    let pct = pct_change_series(&series.close, 5);
    rolling_mean_opt_exact(&pct, 10)
}

fn compute_td_mt_038(series: &SymbolSeries<'_>) -> Option<f64> {
    let pct = pct_change_series(&series.close, 5);
    rolling_mean_opt_exact(&pct, 14)
}

fn compute_td_mt_039(series: &SymbolSeries<'_>) -> Option<f64> {
    let diff = zip_map2(&series.close, &series.low, |c, l| c - l);
    rolling_sum_exact(&diff, 10)
}

fn compute_td_mt_040(series: &SymbolSeries<'_>) -> Option<f64> {
    let diff = zip_map2(&series.high, &series.close, |h, c| h - c);
    rolling_sum_exact(&diff, 10)
}

fn compute_td_mt_042(series: &SymbolSeries<'_>) -> Option<f64> {
    let close = current(&series.close)?;
    let min_close = rolling_min_last(&series.close, 26).ok().flatten()?;
    let max_close = rolling_max_last(&series.close, 26).ok().flatten()?;
    let den = max_close - min_close;
    if den.abs() <= 1e-12 {
        return None;
    }
    finite_opt(Some(2.0 * ((close - min_close) / den) - 1.0))
}

fn compute_tp_vpi_001(series: &SymbolSeries<'_>) -> Option<f64> {
    let clv = clv_series(series);
    let n = clv.len().min(series.volume.len());
    let mut flow = Vec::with_capacity(n);
    for i in 0..n {
        flow.push(clv[i].and_then(|v| finite_opt(Some(v * series.volume.value_at(i)))));
    }
    rolling_sum_opt_exact(&flow, 360)
}

fn compute_tp_vpi_002(series: &SymbolSeries<'_>) -> Option<f64> {
    let clv = clv_series(series);
    let n = clv.len().min(series.volume.len());
    let mut flow = Vec::with_capacity(n);
    for i in 0..n {
        flow.push(clv[i].and_then(|v| finite_opt(Some(v * series.volume.value_at(i)))));
    }
    let ad = rolling_sum_series_opt(&flow, 360, 360).ok()?;
    rolling_mean_opt_exact(&ad, 14)
}

fn compute_tp_vpi_003(series: &SymbolSeries<'_>) -> Option<f64> {
    let diff = diff_series(&series.close, 1);
    let mut up = Vec::with_capacity(diff.len());
    let mut down = Vec::with_capacity(diff.len());
    for value in diff {
        match value {
            Some(v) if v > 0.0 => {
                up.push(Some(v));
                down.push(Some(0.0));
            }
            Some(v) if v < 0.0 => {
                up.push(Some(0.0));
                down.push(Some(-v));
            }
            Some(_) | None => {
                up.push(Some(0.0));
                down.push(Some(0.0));
            }
        }
    }
    let au = rolling_sum_opt_exact(&up, 20)?;
    let ad = rolling_sum_opt_exact(&down, 20)?;
    let total = au + ad;
    if total <= 0.0 {
        return None;
    }
    ratio(au, total)
}

fn compute_tp_vpi_004(series: &SymbolSeries<'_>) -> Option<f64> {
    let mut clv = clv_series(series);
    for value in &mut clv {
        if value.is_none() {
            *value = Some(0.0);
        }
    }
    let n = clv.len().min(series.volume.len());
    let mut flow = Vec::with_capacity(n);
    for i in 0..n {
        flow.push(clv[i].and_then(|v| finite_opt(Some(v * series.volume.value_at(i)))));
    }
    let ad = rolling_sum_series_opt(&flow, 360, 360).ok()?;
    let fast = rolling_mean_series_opt(&ad, 12, 1).ok()?;
    let slow = rolling_mean_series_opt(&ad, 26, 1).ok()?;
    match (last_opt(&fast), last_opt(&slow)) {
        (Some(f), Some(s)) => finite_opt(Some(f - s)),
        _ => None,
    }
}

fn compute_tp_vpi_005(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series.close.len().min(series.volume.len());
    let mut obv = Vec::with_capacity(n);
    for i in 0..n {
        let delta = if i == 0 {
            0.0
        } else {
            let curr = series.close.value_at(i);
            let prev = series.close.value_at(i - 1);
            if curr > prev {
                series.volume.value_at(i)
            } else if curr < prev {
                -series.volume.value_at(i)
            } else {
                0.0
            }
        };
        obv.push(delta);
    }
    rolling_sum_exact(&obv, 360)
}

fn compute_tp_vpi_006(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series.close.len().min(series.volume.len());
    let mut obv = Vec::with_capacity(n);
    for i in 0..n {
        let delta = if i == 0 {
            0.0
        } else {
            let curr = series.close.value_at(i);
            let prev = series.close.value_at(i - 1);
            if curr > prev {
                series.volume.value_at(i)
            } else if curr < prev {
                -series.volume.value_at(i)
            } else {
                0.0
            }
        };
        obv.push(delta);
    }
    let obv_roll = rolling_sum_series(&obv, 360, 360).ok()?;
    rolling_mean_opt_exact(&obv_roll, 14)
}

fn compute_tp_vpi_008(series: &SymbolSeries<'_>) -> Option<f64> {
    let typical = zip_map3_opt(&series.high, &series.low, &series.close, |h, l, c| {
        finite_opt(Some((h + l + c) / 3.0))
    });
    let n = typical.len().min(series.volume.len());
    let mut total = Vec::with_capacity(n);
    for i in 0..n {
        total.push(typical[i].and_then(|v| finite_opt(Some(v * series.volume.value_at(i)))));
    }
    let num = rolling_sum_opt_min(&total, 30, 10)?;
    let den = rolling_sum_min(&series.volume, 30, 10)?;
    ratio(num, den)
}

fn compute_tp_vpi_009(series: &SymbolSeries<'_>) -> Option<f64> {
    let typical = zip_map3_opt(&series.high, &series.low, &series.close, |h, l, c| {
        finite_opt(Some((h + l + c) / 3.0))
    });
    let tp = last_opt(&typical)?;
    let n = typical.len().min(series.volume.len());
    let mut total = Vec::with_capacity(n);
    for i in 0..n {
        total.push(typical[i].and_then(|v| finite_opt(Some(v * series.volume.value_at(i)))));
    }
    let num = rolling_sum_opt_min(&total, 14, 10)?;
    let den = rolling_sum_min(&series.volume, 14, 10)?;
    let vwa = ratio(num, den)?;
    finite_opt(Some(tp - vwa))
}

fn compute_tp_vpi_011(series: &SymbolSeries<'_>) -> Option<f64> {
    let short = rolling_mean_exact(&series.volume, 12)?;
    let long = rolling_mean_exact(&series.volume, 26)?;
    finite_opt(Some(short - long))
}

fn compute_tp_vpi_012(series: &SymbolSeries<'_>) -> Option<f64> {
    let curr = current(&series.volume)?;
    let mean = rolling_mean_exact(&series.volume, 14)?;
    ratio(curr, mean)
}

fn compute_tp_vpi_013(series: &SymbolSeries<'_>) -> Option<f64> {
    let curr = current(&series.volume)?;
    let mean = rolling_mean_exact(&series.volume, 20)?;
    ratio(curr, mean)
}

fn compute_tp_vpi_014(series: &SymbolSeries<'_>) -> Option<f64> {
    let mean20 = rolling_mean_series(&series.volume, 20, 20).ok()?;
    let ratio_series = binary_opt_vec(
        &collect_vec(&series.volume)
            .into_iter()
            .map(|v| Some(v))
            .collect::<Vec<Option<f64>>>(),
        &mean20,
        ratio,
    );
    rolling_mean_opt_exact(&ratio_series, 20)
}

fn compute_tp_vpi_015(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series
        .close
        .len()
        .min(series.open.len())
        .min(series.volume.len());
    if n == 0 {
        return None;
    }
    let close = series.close.value_at(n - 1);
    let open = series.open.value_at(n - 1);
    let volume = series.volume.value_at(n - 1);
    if volume.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((close - open) / volume)).or(Some(0.0))
}

fn compute_tp_vpi_016(series: &SymbolSeries<'_>) -> Option<f64> {
    let numerator = zip_map2(&series.close, &series.volume, |c, v| c * v);
    let num = rolling_mean_exact(&numerator, 14);
    let den = rolling_mean_exact(&series.volume, 14);
    match (num, den) {
        (Some(n), Some(d)) => ratio(n, d).or(Some(0.0)),
        _ => Some(0.0),
    }
}

fn compute_tp_vpi_017(series: &SymbolSeries<'_>) -> Option<f64> {
    let typical = zip_map3_opt(&series.high, &series.low, &series.close, |h, l, c| {
        finite_opt(Some((h + l + c) / 3.0))
    });
    let n = typical
        .len()
        .min(series.close.len())
        .min(series.volume.len());
    let mut pos = vec![Some(0.0); n];
    let mut neg = vec![Some(0.0); n];
    for i in 1..n {
        if let (Some(curr), Some(_prev)) = (typical[i], typical[i - 1]) {
            let mf = curr * series.volume.value_at(i);
            if series.close.value_at(i) > series.close.value_at(i - 1) {
                pos[i] = finite_opt(Some(mf));
            } else if series.close.value_at(i) < series.close.value_at(i - 1) {
                neg[i] = finite_opt(Some(mf));
            }
        }
    }
    let pos_sum = rolling_sum_opt_exact(&pos, 14)?;
    let neg_sum = rolling_sum_opt_exact(&neg, 14)?;
    finite_opt(Some(pos_sum - neg_sum))
}

fn compute_tp_vpi_018(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    let mut tr = Vec::with_capacity(n);
    for i in 0..n {
        if i == 0 {
            tr.push(None);
            continue;
        }
        let high = series.high.value_at(i);
        let low = series.low.value_at(i);
        let prev_close = series.close.value_at(i - 1);
        let buggy_max = (high - low).max((high - prev_close).abs());
        tr.push(finite_opt(Some(buggy_max)));
    }
    rolling_sum_opt_exact(&tr, 20)
}

fn compute_tp_vpi_019(series: &SymbolSeries<'_>) -> Option<f64> {
    rolling_sum_exact(&series.volume, 10)
}

fn compute_td_vi_001(series: &SymbolSeries<'_>) -> Option<f64> {
    let max_price = rolling_max_min(&series.high, 14, 1)?;
    let min_price = rolling_min_min(&series.low, 14, 1)?;
    finite_opt(Some(max_price - min_price))
}

fn compute_td_vi_002(series: &SymbolSeries<'_>) -> Option<f64> {
    sample_std_last(&series.close, 5, 1)
}

fn compute_td_vi_005(series: &SymbolSeries<'_>) -> Option<f64> {
    let tr = tr_series(series);
    let atr = rolling_mean_opt_min(&tr, 14, 1)?;
    let sma_atr = rolling_mean_opt_min(&tr, 7, 1)?;
    finite_opt(Some(atr - sma_atr))
}

fn rolling_mean_opt_min(values: &[Option<f64>], window: usize, min_periods: usize) -> Option<f64> {
    rolling_mean_last_opt_from_series(values, window, min_periods)
        .ok()
        .flatten()
}

fn compute_td_vi_009(series: &SymbolSeries<'_>) -> Option<f64> {
    let tr = tr_series(series);
    rolling_std_opt_last(&tr, 14, 1)
}

fn compute_td_vi_010(series: &SymbolSeries<'_>) -> Option<f64> {
    plus_dm_series(series)
        .last()
        .copied()
        .and_then(|v| finite_opt(Some(v)))
}

fn compute_td_vi_011(series: &SymbolSeries<'_>) -> Option<f64> {
    let plus_dm = plus_dm_series(series);
    rolling_sum_min(&plus_dm, 14, 1)
}

fn compute_td_vi_012(series: &SymbolSeries<'_>) -> Option<f64> {
    let fast = rolling_mean_min(&series.close, 3, 1)?;
    let slow = rolling_mean_min(&series.close, 10, 1)?;
    ratio((fast - slow) * 100.0, slow)
}

fn compute_td_vi_013(series: &SymbolSeries<'_>) -> Option<f64> {
    pct_change_last(&series.close, 10).and_then(|v| finite_opt(Some(v * 100.0)))
}

fn compute_td_vi_020(series: &SymbolSeries<'_>) -> Option<f64> {
    let low = rolling_min_min(&series.low, 5, 1)?;
    let high = rolling_max_min(&series.high, 5, 1)?;
    let close = current(&series.close)?;
    let fastk = ratio((close - low) * 100.0, high - low)?;
    let low_min = rolling_min_min(&series.low, 5, 1)?;
    let high_max = rolling_max_min(&series.high, 5, 1)?;
    let _ = (low_min, high_max);
    let n = series.close.len();
    let mut fastk_series = Vec::with_capacity(n);
    for i in 0..n {
        let start = i + 1 - (i + 1).min(5);
        let mut min_low = series.low.value_at(start);
        let mut max_high = series.high.value_at(start);
        for j in (start + 1)..=i {
            min_low = min_low.min(series.low.value_at(j));
            max_high = max_high.max(series.high.value_at(j));
        }
        fastk_series.push(ratio(
            (series.close.value_at(i) - min_low) * 100.0,
            max_high - min_low,
        ));
    }
    rolling_mean_opt_min(&fastk_series, 3, 1).or(Some(fastk))
}

fn compute_td_vi_021(series: &SymbolSeries<'_>) -> Option<f64> {
    compute_td_vi_020(series)
}

fn compute_td_vi_022(series: &SymbolSeries<'_>) -> Option<f64> {
    let close = current(&series.close)?;
    let min_close = rolling_min_min(&series.close, 14, 1)?;
    let max_close = rolling_max_min(&series.close, 14, 1)?;
    ratio(close - min_close, max_close - min_close)
}

fn compute_td_vi_023(series: &SymbolSeries<'_>) -> Option<f64> {
    let sma1 = rolling_mean_series(&series.close, 14, 1).ok()?;
    let sma2 = rolling_mean_series_opt(&sma1, 14, 1).ok()?;
    let sma3 = rolling_mean_series_opt(&sma2, 14, 1).ok()?;
    pct_change_last_opt(&sma3, 1)
}

fn compute_td_vi_024(series: &SymbolSeries<'_>) -> Option<f64> {
    let close = current(&series.close)?;
    let open = current(&series.open)?;
    let high = current(&series.high)?;
    let low = current(&series.low)?;
    ratio(close - open, high - low + 0.0001)
}

fn compute_td_vi_025(series: &SymbolSeries<'_>) -> Option<f64> {
    let bp = bp_series(series);
    let tr = tr_series(series);
    let avg7 = ratio(
        rolling_sum_opt_min(&bp, 7, 1)?,
        rolling_sum_opt_min(&tr, 7, 1)?,
    )?;
    let avg14 = ratio(
        rolling_sum_opt_min(&bp, 14, 1)?,
        rolling_sum_opt_min(&tr, 14, 1)?,
    )?;
    let avg28 = ratio(
        rolling_sum_opt_min(&bp, 28, 1)?,
        rolling_sum_opt_min(&tr, 28, 1)?,
    )?;
    finite_opt(Some((4.0 * avg7 + 2.0 * avg14 + avg28) / 7.0))
}

fn compute_td_vi_026(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len())
        .min(series.open.len());
    let mut comp = Vec::with_capacity(n);
    for i in 0..n {
        let upper = series.high.value_at(i) - series.close.value_at(i).max(series.open.value_at(i));
        let lower = series.close.value_at(i).min(series.open.value_at(i)) - series.low.value_at(i);
        let body = (series.close.value_at(i) - series.open.value_at(i)).abs();
        comp.push(finite_opt(Some(upper - lower - body)));
    }
    rolling_sum_opt_min(&comp, 14, 1)
}

fn compute_td_vi_027(series: &SymbolSeries<'_>) -> Option<f64> {
    let range = zip_map2(&series.high, &series.low, |h, l| h - l);
    rolling_max_min(&range, 14, 1)
}

fn compute_td_vi_028(series: &SymbolSeries<'_>) -> Option<f64> {
    let upper = zip_map2(&series.high, &series.close, |h, c| h - c);
    let n = upper.len().min(series.open.len());
    let mut upper_shadow = Vec::with_capacity(n);
    for (i, high_part) in upper.iter().enumerate().take(n) {
        upper_shadow.push(finite_opt(Some(
            series.high.value_at(i) - series.close.value_at(i).max(series.open.value_at(i)),
        )));
        let _ = high_part;
    }
    rolling_sum_opt_min(&upper_shadow, 14, 1)
}

fn compute_td_vi_029(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series
        .low
        .len()
        .min(series.close.len())
        .min(series.open.len());
    let mut lower_shadow = Vec::with_capacity(n);
    for i in 0..n {
        lower_shadow.push(finite_opt(Some(
            series.close.value_at(i).min(series.open.value_at(i)) - series.low.value_at(i),
        )));
    }
    rolling_sum_opt_min(&lower_shadow, 14, 1)
}

fn compute_td_pt_001(series: &SymbolSeries<'_>) -> Option<f64> {
    current(&series.close).and_then(|v| finite_opt(Some(v.sin())))
}

fn compute_td_pt_002(series: &SymbolSeries<'_>) -> Option<f64> {
    current(&series.close).and_then(|v| finite_opt(Some(v.cos())))
}

fn compute_td_pt_003(series: &SymbolSeries<'_>) -> Option<f64> {
    let vwap = zip_map2_opt(&series.amount, &series.volume, ratio);
    let numerator = rolling_mean_series_opt(&vwap, 6, 6).ok()?;
    let amount_sum = rolling_sum_series(&series.amount, 6, 6).ok()?;
    let volume_sum = rolling_sum_series(&series.volume, 6, 6).ok()?;
    let denominator = binary_opt_vec(&amount_sum, &volume_sum, ratio);
    let apb = binary_opt_vec(&numerator, &denominator, |a, b| {
        finite_opt(Some((a / b).ln()))
    });
    let apb_ma = rolling_mean_series_opt(&apb, 18, 18).ok()?;
    last_opt(&apb_ma)
}

fn compute_td_pt_004(series: &SymbolSeries<'_>) -> Option<f64> {
    let vol_ma = rolling_mean_series(&series.volume, 120, 120).ok()?;
    let n = series
        .close
        .len()
        .min(series.open.len())
        .min(series.high.len())
        .min(series.low.len())
        .min(vol_ma.len());
    let mut var1 = Vec::with_capacity(n);
    for i in 0..n {
        let high_low = series.high.value_at(i) - series.low.value_at(i);
        let vol_mean = vol_ma[i];
        let value = match vol_mean {
            Some(vm) if vm.abs() > 1e-12 && high_low.abs() > 1e-12 => finite_opt(Some(
                ((series.close.value_at(i) - series.open.value_at(i)) / high_low)
                    * (series.volume.value_at(i) / vm),
            )),
            _ => None,
        };
        var1.push(value);
    }
    let accum = rolling_sum_series_opt(&var1, 60, 60).ok()?;
    let fast = rolling_mean_series_opt(&accum, 9, 1).ok()?;
    let slow = rolling_mean_series_opt(&accum, 25, 1).ok()?;
    match (last_opt(&fast), last_opt(&slow)) {
        (Some(f), Some(s)) => finite_opt(Some(f - s)).or(Some(0.0)),
        _ => Some(0.0),
    }
}

fn compute_td_pt_009(series: &SymbolSeries<'_>) -> Option<f64> {
    let close_shift = shift_last(&series.close, 10)?;
    let high_min = rolling_min_last(&series.high, 10).ok().flatten()?;
    let low_min = rolling_min_last(&series.low, 10).ok().flatten()?;
    let mid = (close_shift + high_min + low_min) / 3.0;
    let yl2 = mid + high_min - low_min;
    let close = current(&series.close)?;
    finite_opt(Some(close - yl2))
}

fn compute_td_pt_010(series: &SymbolSeries<'_>) -> Option<f64> {
    let mid = zip_map3_opt(&series.close, &series.high, &series.low, |c, h, l| {
        finite_opt(Some((c + h + l) / 3.0))
    });
    let mid_last = last_opt(&mid)?;
    let high_min = rolling_min_last(&series.high, 15).ok().flatten()?;
    let low_min = rolling_min_last(&series.low, 15).ok().flatten()?;
    let zc2 = mid_last - high_min + low_min;
    let close = current(&series.close)?;
    finite_opt(Some(close - zc2))
}

fn compute_td_pt_024(series: &SymbolSeries<'_>) -> Option<f64> {
    let high = current(&series.high)?;
    let low = current(&series.low)?;
    let close = current(&series.close)?;
    ratio(high - close, high - low)
}

fn compute_td_pt_025(series: &SymbolSeries<'_>) -> Option<f64> {
    let high = current(&series.high)?;
    let low = current(&series.low)?;
    let close = current(&series.close)?;
    ratio(close - low, high - low)
}

fn compute_td_pt_026(series: &SymbolSeries<'_>) -> Option<f64> {
    let high = current(&series.high)?;
    let low = current(&series.low)?;
    let close = current(&series.close)?;
    let open = current(&series.open)?;
    ratio(close - open, high - low)
}

fn compute_td_pt_027(series: &SymbolSeries<'_>) -> Option<f64> {
    rolling_argmin_pos_last(&series.close, 14)
}

fn compute_td_ci_003(series: &SymbolSeries<'_>) -> Option<f64> {
    let diff = diff_series(&series.close, 1);
    let diff_sma = rolling_mean_series_opt(&diff, 30, 1).ok()?;
    let close_sma = rolling_mean_series(&series.close, 30, 1).ok()?;
    let phase = binary_opt_vec(&diff_sma, &close_sma, |d, c| {
        finite_opt(Some((d / c).atan()))
    });
    let period = unary_opt_vec(&phase, |p| finite_opt(Some((2.0 * PI) / p)));
    let out = rolling_mean_series_opt(&period, 30, 1).ok()?;
    last_opt(&out)
}

fn compute_td_ci_007(series: &SymbolSeries<'_>) -> Option<f64> {
    let mean = rolling_mean_min(&series.close, 30, 1)?;
    finite_opt(Some(mean.sin()))
}

fn compute_td_ci_008(series: &SymbolSeries<'_>) -> Option<f64> {
    let mean = rolling_mean_min(&series.close, 30, 1)?;
    finite_opt(Some(mean.cos()))
}

fn compute_td_ci_010(_series: &SymbolSeries<'_>) -> Option<f64> {
    Some(0.0)
}

fn compute_td_pr_001(series: &SymbolSeries<'_>) -> Option<f64> {
    let body = body_last(series)?;
    let upper = upper_shadow_last(series)?;
    let lower = lower_shadow_last(series)?;
    Some(if upper > 2.0 * body && lower < 0.1 * body {
        1.0
    } else {
        0.0
    })
}

fn compute_td_pr_002(series: &SymbolSeries<'_>) -> Option<f64> {
    let body = body_last(series)?;
    let upper = upper_shadow_last(series)?;
    let lower = lower_shadow_last(series)?;
    Some(if lower > 2.0 * body && upper < 0.1 * body {
        1.0
    } else {
        0.0
    })
}

fn compute_td_pr_005(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series
        .close
        .len()
        .min(series.open.len())
        .min(series.high.len())
        .min(series.low.len());
    if n < 3 {
        return None;
    }
    let close2 = series.close.value_at(n - 3);
    let open2 = series.open.value_at(n - 3);
    let close1 = series.close.value_at(n - 2);
    let open1 = series.open.value_at(n - 2);
    let high1 = series.high.value_at(n - 2);
    let low1 = series.low.value_at(n - 2);
    let close0 = series.close.value_at(n - 1);
    let open0 = series.open.value_at(n - 1);
    let small_body = (close1 - open1).abs() < (high1 - low1) * 0.1;
    Some(if close2 < open2 && small_body && close0 > open0 {
        1.0
    } else {
        0.0
    })
}

fn compute_td_pr_006(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series.close.len().min(series.open.len());
    if n < 3 {
        return None;
    }
    Some(
        if series.close.value_at(n - 3) < series.open.value_at(n - 3)
            && series.close.value_at(n - 2) < series.open.value_at(n - 2)
            && series.close.value_at(n - 1) < series.open.value_at(n - 1)
        {
            1.0
        } else {
            0.0
        },
    )
}

fn compute_td_pr_007(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series.close.len().min(series.open.len());
    if n < 2 {
        return None;
    }
    let prev_close = series.close.value_at(n - 2);
    let prev_open = series.open.value_at(n - 2);
    let close = series.close.value_at(n - 1);
    let open = series.open.value_at(n - 1);
    let mid = prev_open + (prev_close - prev_open) / 2.0;
    Some(
        if prev_close > prev_open && open > prev_close && close < mid && close < open {
            1.0
        } else {
            0.0
        },
    )
}

fn compute_td_pr_008(series: &SymbolSeries<'_>) -> Option<f64> {
    let open = current(&series.open)?;
    let close = current(&series.close)?;
    let high = current(&series.high)?;
    let low = current(&series.low)?;
    Some(if open == close && (high - low) > 3.0 * (open - close) {
        1.0
    } else {
        0.0
    })
}

fn compute_td_pr_010(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series.close.len().min(series.open.len());
    if n < 3 {
        return None;
    }
    Some(
        if series.close.value_at(n - 3) > series.open.value_at(n - 3)
            && series.open.value_at(n - 2) > series.close.value_at(n - 3)
            && series.close.value_at(n - 2) < series.open.value_at(n - 2)
            && series.open.value_at(n - 1) > series.close.value_at(n - 2)
            && series.close.value_at(n - 1) < series.close.value_at(n - 2)
        {
            1.0
        } else {
            0.0
        },
    )
}

fn compute_td_pr_011(series: &SymbolSeries<'_>) -> Option<f64> {
    let body = body_last(series)?;
    let high = current(&series.high)?;
    let low = current(&series.low)?;
    Some(if body < 0.3 * (high - low) {
        100.0
    } else {
        0.0
    })
}

fn compute_td_pr_012(series: &SymbolSeries<'_>) -> Option<f64> {
    let open = current(&series.open)?;
    let close = current(&series.close)?;
    let high = current(&series.high)?;
    let low = current(&series.low)?;
    Some(if open == close && low < open && high == close {
        100.0
    } else {
        0.0
    })
}

fn compute_td_pr_013(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series.close.len().min(series.open.len());
    let mut sign = Vec::with_capacity(n);
    for i in 0..n {
        sign.push(sign_close_open(
            series.close.value_at(i),
            series.open.value_at(i),
        ));
    }
    rolling_sum_exact(&sign, 20)
}

fn compute_td_pr_014(series: &SymbolSeries<'_>) -> Option<f64> {
    let body = body_last(series)?;
    let high = current(&series.high)?;
    let low = current(&series.low)?;
    Some(if body > 0.6 * (high - low) {
        100.0
    } else {
        0.0
    })
}

fn compute_td_pr_015(series: &SymbolSeries<'_>) -> Option<f64> {
    let open = current(&series.open)?;
    let low = current(&series.low)?;
    let high = current(&series.high)?;
    Some(if open == low || open == high {
        100.0
    } else {
        0.0
    })
}

fn compute_td_pr_016(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series
        .close
        .len()
        .min(series.open.len())
        .min(series.volume.len());
    if n < 90 {
        return None;
    }
    let close = series.close.value_at(n - 1);
    let open = series.open.value_at(n - 1);
    let var1 = sign_close_open(close, open);
    let oc = zip_map2(&series.open, &series.close, |o, c| o - c);
    let vol = collect_vec(&series.volume);
    let var2 = ratio(open - close, rolling_mean_exact(&oc, 90)?)?;
    let var3 = ratio(series.volume.value_at(n - 1), rolling_mean_exact(&vol, 90)?)?;
    finite_opt(Some(var1 * var2 * var3))
}

fn compute_td_pr_017(series: &SymbolSeries<'_>) -> Option<f64> {
    let open = current(&series.open)?;
    let close = current(&series.close)?;
    let high = current(&series.high)?;
    let low = current(&series.low)?;
    Some(
        if open == close && low < open && (close - low) > 0.5 * (high - low) {
            100.0
        } else {
            0.0
        },
    )
}

fn linear_reg_slope_intercept(values: &[f64]) -> Option<(f64, f64)> {
    if values.len() < 2 || values.iter().any(|v| !v.is_finite()) {
        return None;
    }
    let n = values.len() as f64;
    let mean_x = (n - 1.0) / 2.0;
    let mean_y = values.iter().sum::<f64>() / n;
    let mut cov = 0.0;
    let mut var_x = 0.0;
    for (i, y) in values.iter().enumerate() {
        let x = i as f64;
        cov += (x - mean_x) * (*y - mean_y);
        var_x += (x - mean_x) * (x - mean_x);
    }
    if var_x.abs() <= 1e-12 {
        return None;
    }
    let slope = cov / var_x;
    let intercept = mean_y - slope * mean_x;
    Some((slope, intercept))
}

fn td_rsi_last(values: &(impl F64SeriesView + ?Sized), period: usize) -> Option<f64> {
    let diff = diff_series(values, 1);
    let gains = unary_opt_vec(&diff, |d| finite_opt(Some(if d > 0.0 { d } else { 0.0 })));
    let losses = unary_opt_vec(&diff, |d| finite_opt(Some(if d < 0.0 { -d } else { 0.0 })));
    let avg_gain = rolling_mean_opt_exact(&gains, period)?;
    let avg_loss = rolling_mean_opt_exact(&losses, period)?;
    if avg_loss.abs() <= 1e-12 {
        return Some(100.0);
    }
    let rs = avg_gain / avg_loss;
    finite_opt(Some(100.0 - (100.0 / (1.0 + rs))))
}

fn current_tr(series: &SymbolSeries<'_>) -> Option<f64> {
    last_opt(&tr_series(series))
}

fn compute_td_ci_001(series: &SymbolSeries<'_>) -> Option<f64> {
    let high = current(&series.high)?;
    let low = current(&series.low)?;
    finite_opt(Some(high - low))
}

fn compute_td_ci_002(series: &SymbolSeries<'_>) -> Option<f64> {
    let close = current(&series.close)?;
    let open = current(&series.open)?;
    finite_opt(Some(close - open))
}

fn compute_td_ci_004(series: &SymbolSeries<'_>) -> Option<f64> {
    let diff = diff_series(&series.close, 1);
    let diff_sma = rolling_mean_series_opt(&diff, 30, 1).ok()?;
    let close_sma = rolling_mean_series(&series.close, 30, 1).ok()?;
    let phase = binary_opt_vec(&diff_sma, &close_sma, |d, c| {
        finite_opt(Some((d / c).atan().to_degrees()))
    });
    last_opt(&phase)
}

fn compute_td_ci_005(series: &SymbolSeries<'_>) -> Option<f64> {
    rolling_mean_min(&series.close, 30, 1)
}

fn compute_td_ci_006(series: &SymbolSeries<'_>) -> Option<f64> {
    let diff = diff_series(&series.close, 1);
    let out = rolling_mean_series_opt(&diff, 30, 1).ok()?;
    last_opt(&out)
}

fn compute_td_ci_009(series: &SymbolSeries<'_>) -> Option<f64> {
    rolling_mean_min(&series.close, 30, 1)
}

fn compute_td_mt_004(series: &SymbolSeries<'_>) -> Option<f64> {
    diff_last(&series.close, 3)
}

fn compute_td_mt_006(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series
        .close
        .len()
        .min(series.high.len())
        .min(series.low.len());
    if n < 14 {
        return None;
    }
    let tp: Vec<f64> = (0..n)
        .map(|i| {
            (series.high.value_at(i) + series.low.value_at(i) + series.close.value_at(i)) / 3.0
        })
        .collect();
    let sma_tp = rolling_mean_exact(tp.as_slice(), 14)?;
    let tail = &tp[n - 14..];
    let mean_dev = tail.iter().map(|v| (v - sma_tp).abs()).sum::<f64>() / 14.0;
    if mean_dev.abs() <= 1e-12 {
        return None;
    }
    finite_opt(Some((tp[n - 1] - sma_tp) / (0.015 * mean_dev)))
}

fn compute_td_mt_009(series: &SymbolSeries<'_>) -> Option<f64> {
    let fast = rolling_mean_min(&series.close, 12, 1)?;
    let slow = rolling_mean_min(&series.close, 26, 1)?;
    finite_opt(Some(fast - slow))
}

fn compute_td_mt_012(series: &SymbolSeries<'_>) -> Option<f64> {
    compute_td_mt_009(series)
}

fn compute_td_mt_020(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series
        .close
        .len()
        .min(series.high.len())
        .min(series.low.len());
    if n < 14 {
        return None;
    }
    let tp: Vec<f64> = (0..n)
        .map(|i| {
            (series.high.value_at(i) + series.low.value_at(i) + series.close.value_at(i)) / 3.0
        })
        .collect();
    let mean = rolling_mean_exact(tp.as_slice(), 14)?;
    let std = sample_std_last(tp.as_slice(), 14, 14)?;
    if std.abs() <= 1e-12 {
        return None;
    }
    finite_opt(Some((tp[n - 1] - mean) / std))
}

fn compute_td_mt_023(series: &SymbolSeries<'_>) -> Option<f64> {
    td_rsi_last(&series.close, 14)
}

fn compute_td_mt_024(series: &SymbolSeries<'_>) -> Option<f64> {
    td_rsi_last(&series.volume, 14)
}

fn compute_td_mt_026(series: &SymbolSeries<'_>) -> Option<f64> {
    td_rsi_last(&series.close, 14)
}

fn compute_td_mt_031(series: &SymbolSeries<'_>) -> Option<f64> {
    let close = current(&series.close)?;
    let prev = shift_last(&series.close, 10)?;
    if close <= 0.0 || prev <= 0.0 {
        return None;
    }
    finite_opt(Some((close / prev).ln()))
}

fn compute_td_mt_033(series: &SymbolSeries<'_>) -> Option<f64> {
    diff_last(&series.close, 1)
}

fn compute_td_mt_041(series: &SymbolSeries<'_>) -> Option<f64> {
    let high = rolling_max_min(&series.high, 14, 14)?;
    let low = rolling_min_min(&series.low, 14, 14)?;
    let close = current(&series.close)?;
    ratio(-100.0 * (high - close), high - low)
}

fn compute_td_mt_043(series: &SymbolSeries<'_>) -> Option<f64> {
    rolling_mean_exact(&series.volume, 360)
}

fn compute_td_mt_044(series: &SymbolSeries<'_>) -> Option<f64> {
    let range = zip_map2(&series.high, &series.low, |h, l| h - l);
    rolling_mean_exact(range.as_slice(), 360)
}

fn compute_td_pr_003(series: &SymbolSeries<'_>) -> Option<f64> {
    let open = current(&series.open)?;
    let close = current(&series.close)?;
    let high = current(&series.high)?;
    let low = current(&series.low)?;
    let body = (close - open).abs();
    let upper_shadow = high - close.max(open);
    let lower_shadow = close.min(open) - low;
    Some(if lower_shadow > 2.0 * body && upper_shadow < 0.1 * body {
        1.0
    } else {
        0.0
    })
}

fn compute_td_pr_004(series: &SymbolSeries<'_>) -> Option<f64> {
    let open = current(&series.open)?;
    let close = current(&series.close)?;
    let high = current(&series.high)?;
    let low = current(&series.low)?;
    let body = (close - open).abs();
    let upper_shadow = high - close.max(open);
    let lower_shadow = close.min(open) - low;
    Some(if upper_shadow > 2.0 * body && lower_shadow < 0.1 * body {
        1.0
    } else {
        0.0
    })
}

fn compute_td_pr_009(series: &SymbolSeries<'_>) -> Option<f64> {
    let open = current(&series.open)?;
    let close = current(&series.close)?;
    Some(if open == close { 1.0 } else { 0.0 })
}

fn compute_td_pt_005(series: &SymbolSeries<'_>) -> Option<f64> {
    let high = current(&series.high)?;
    let low = current(&series.low)?;
    finite_opt(Some((high + low) / 2.0))
}

fn compute_td_pt_006(series: &SymbolSeries<'_>) -> Option<f64> {
    let high = current(&series.high)?;
    let low = current(&series.low)?;
    let close = current(&series.close)?;
    finite_opt(Some((high + low + close) / 3.0))
}

fn compute_td_pt_007(series: &SymbolSeries<'_>) -> Option<f64> {
    let high = current(&series.high)?;
    let low = current(&series.low)?;
    let close = current(&series.close)?;
    finite_opt(Some((high + low + 2.0 * close) / 4.0))
}

fn compute_td_pt_008(series: &SymbolSeries<'_>) -> Option<f64> {
    diff_last(&series.close, 14)
}

fn compute_td_pt_011(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series
        .close
        .len()
        .min(series.high.len())
        .min(series.low.len());
    if n <= 10 {
        return None;
    }
    let curr =
        (series.high.value_at(n - 1) + series.low.value_at(n - 1) + series.close.value_at(n - 1))
            / 3.0;
    let prev = (series.high.value_at(n - 11)
        + series.low.value_at(n - 11)
        + series.close.value_at(n - 11))
        / 3.0;
    finite_opt(Some(curr - prev))
}

fn compute_td_pt_012(series: &SymbolSeries<'_>) -> Option<f64> {
    let spread = zip_map2(&series.high, &series.low, |h, l| h - l);
    diff_last(spread.as_slice(), 10)
}

fn compute_td_pt_013(series: &SymbolSeries<'_>) -> Option<f64> {
    ratio(current(&series.high)?, current(&series.low)?)
}

fn compute_td_pt_014(series: &SymbolSeries<'_>) -> Option<f64> {
    let high = current(&series.high)?;
    let low = current(&series.low)?;
    let close = current(&series.close)?;
    ratio(high - low, close)
}

fn compute_td_pt_015(series: &SymbolSeries<'_>) -> Option<f64> {
    let close = current(&series.close)?;
    let open = current(&series.open)?;
    ratio(close - open, open)
}

fn compute_td_pt_016(series: &SymbolSeries<'_>) -> Option<f64> {
    pct_change_last(&series.close, 14)
}

fn compute_td_pt_017(series: &SymbolSeries<'_>) -> Option<f64> {
    pct_change_last(&series.close, 14)
}

fn compute_td_pt_018(series: &SymbolSeries<'_>) -> Option<f64> {
    let oc = zip_map2(&series.close, &series.open, |c, o| c - o);
    rolling_mean_exact(oc.as_slice(), 14)
}

fn compute_td_pt_019(series: &SymbolSeries<'_>) -> Option<f64> {
    let range = zip_map2(&series.high, &series.low, |h, l| h - l);
    rolling_mean_exact(range.as_slice(), 14)
}

fn compute_td_pt_020(series: &SymbolSeries<'_>) -> Option<f64> {
    let range = zip_map2(&series.high, &series.low, |h, l| h - l);
    rolling_max_min(range.as_slice(), 14, 14)
}

fn compute_td_pt_021(series: &SymbolSeries<'_>) -> Option<f64> {
    diff_last(&series.close, 14).and_then(|v| finite_opt(Some(v.abs())))
}

fn compute_td_pt_022(series: &SymbolSeries<'_>) -> Option<f64> {
    let max_close = rolling_max_min(&series.close, 14, 14)?;
    let min_close = rolling_min_min(&series.close, 14, 14)?;
    finite_opt(Some(max_close - min_close))
}

fn compute_td_pt_023(series: &SymbolSeries<'_>) -> Option<f64> {
    let close = current(&series.close)?;
    let mean = rolling_mean_exact(&series.close, 14)?;
    finite_opt(Some((close - mean).abs()))
}

fn compute_td_pt_028(series: &SymbolSeries<'_>) -> Option<f64> {
    let close = current(&series.close)?;
    let min_close = rolling_min_min(&series.close, 30, 30)?;
    ratio(min_close - close, close)
}

fn compute_td_pt_029(series: &SymbolSeries<'_>) -> Option<f64> {
    let close = current(&series.close)?;
    let max_close = rolling_max_min(&series.close, 30, 30)?;
    ratio(max_close - close, close)
}

fn compute_td_si_001(series: &SymbolSeries<'_>) -> Option<f64> {
    Some(if current(&series.close)? > current(&series.open)? {
        1.0
    } else {
        0.0
    })
}

fn compute_td_si_002(series: &SymbolSeries<'_>) -> Option<f64> {
    let tail = collect_vec(&series.close);
    let (_, intercept) = linear_reg_slope_intercept(&tail[tail.len().saturating_sub(14)..])?;
    finite_opt(Some(intercept))
}

fn compute_td_si_003(series: &SymbolSeries<'_>) -> Option<f64> {
    let tail = collect_vec(&series.close);
    let (slope, _) = linear_reg_slope_intercept(&tail[tail.len().saturating_sub(14)..])?;
    finite_opt(Some(slope.atan().to_degrees()))
}

fn compute_td_si_004(series: &SymbolSeries<'_>) -> Option<f64> {
    let tail = collect_vec(&series.close);
    let (slope, _) = linear_reg_slope_intercept(&tail[tail.len().saturating_sub(14)..])?;
    finite_opt(Some(slope))
}

fn compute_td_si_005(series: &SymbolSeries<'_>) -> Option<f64> {
    compute_td_si_002(series)
}

fn compute_td_si_006(series: &SymbolSeries<'_>) -> Option<f64> {
    let tail = collect_vec(&series.close);
    let (slope, intercept) = linear_reg_slope_intercept(&tail[tail.len().saturating_sub(14)..])?;
    finite_opt(Some(slope * 13.0 + intercept))
}

fn compute_td_si_007(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    if n < 14 {
        return None;
    }
    let high = collect_vec(&series.high);
    let low = collect_vec(&series.low);
    let x = &high[n - 14..];
    let y = &low[n - 14..];
    let mean_x = x.iter().sum::<f64>() / x.len() as f64;
    let mean_y = y.iter().sum::<f64>() / y.len() as f64;
    let cov = x
        .iter()
        .zip(y.iter())
        .map(|(a, b)| (a - mean_x) * (b - mean_y))
        .sum::<f64>()
        / (x.len() as f64 - 1.0);
    let var = y.iter().map(|v| (v - mean_y) * (v - mean_y)).sum::<f64>() / y.len() as f64;
    if var.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(cov / var))
}

fn compute_td_si_008(series: &SymbolSeries<'_>) -> Option<f64> {
    rolling_corr_last(&series.close, &series.volume, 14, 14)
        .ok()
        .flatten()
}

fn compute_td_si_009(series: &SymbolSeries<'_>) -> Option<f64> {
    sample_std_last(&series.volume, 14, 14)
}

fn compute_td_si_010(series: &SymbolSeries<'_>) -> Option<f64> {
    let tail = collect_vec(&series.close);
    let (slope, intercept) = linear_reg_slope_intercept(&tail[tail.len().saturating_sub(14)..])?;
    finite_opt(Some(slope * 14.0 + intercept))
}

fn compute_td_si_011(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series
        .close
        .len()
        .min(series.high.len())
        .min(series.low.len());
    if n < 10 {
        return None;
    }
    let tp: Vec<f64> = (0..n)
        .map(|i| {
            (series.high.value_at(i) + series.low.value_at(i) + series.close.value_at(i)) / 3.0
        })
        .collect();
    let (_, intercept) = linear_reg_slope_intercept(&tp[n - 10..])?;
    finite_opt(Some(intercept))
}

fn compute_td_si_012(series: &SymbolSeries<'_>) -> Option<f64> {
    let std = sample_std_last(&series.close, 5, 5)?;
    finite_opt(Some(std * std))
}

fn compute_td_si_013(series: &SymbolSeries<'_>) -> Option<f64> {
    let mean = rolling_mean_series(&series.close, 14, 14).ok()?;
    let sq_dev = (0..series.close.len())
        .map(|i| match mean[i] {
            Some(m) => finite_opt(Some((series.close.value_at(i) - m).powi(2))),
            None => None,
        })
        .collect::<Vec<Option<f64>>>();
    rolling_sum_opt_exact(&sq_dev, 14)
}

fn compute_td_ti_003(series: &SymbolSeries<'_>) -> Option<f64> {
    let high_ma = rolling_mean_exact(&series.high, 14)?;
    let low_ma = rolling_mean_exact(&series.low, 14)?;
    finite_opt(Some(high_ma - low_ma))
}

fn compute_td_ti_004(series: &SymbolSeries<'_>) -> Option<f64> {
    let sma = rolling_mean_exact(&series.close, 10)?;
    let close = current(&series.close)?;
    finite_opt(Some(close - sma))
}

fn compute_td_ti_007(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series
        .close
        .len()
        .min(series.high.len())
        .min(series.low.len());
    if n < 10 {
        return None;
    }
    let tp: Vec<f64> = (0..n)
        .map(|i| {
            (series.high.value_at(i) + series.low.value_at(i) + series.close.value_at(i)) / 3.0
        })
        .collect();
    rolling_mean_exact(tp.as_slice(), 10)
}

fn compute_td_ti_012(series: &SymbolSeries<'_>) -> Option<f64> {
    let ema = rolling_mean_series(&series.close, 14, 14).ok()?;
    let dema = binary_opt_vec(
        &ema.iter()
            .map(|v| v.map(|x| 2.0 * x))
            .collect::<Vec<Option<f64>>>(),
        &rolling_mean_series_opt(&ema, 14, 14).ok()?,
        |a, b| finite_opt(Some(a - b)),
    );
    last_opt(&dema)
}

fn compute_td_ti_029(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    if n < 10 {
        return None;
    }
    let median: Vec<f64> = (0..n)
        .map(|i| (series.high.value_at(i) + series.low.value_at(i)) / 2.0)
        .collect();
    let tail = &median[n - 10..];
    let half = 10 / 2;
    finite_opt(Some(
        tail[tail.len() - half..].iter().sum::<f64>() / half as f64,
    ))
}

fn compute_td_ti_042(series: &SymbolSeries<'_>) -> Option<f64> {
    let high = rolling_max_min(&series.high, 14, 14)?;
    let low = rolling_min_min(&series.low, 14, 14)?;
    finite_opt(Some(high - low))
}

fn compute_td_vi_003(series: &SymbolSeries<'_>) -> Option<f64> {
    let range = zip_map2(&series.high, &series.low, |h, l| h - l);
    rolling_mean_min(range.as_slice(), 14, 1)
}

fn compute_td_vi_004(series: &SymbolSeries<'_>) -> Option<f64> {
    let tr = tr_series(series);
    let atr = rolling_mean_series_opt(&tr, 14, 1).ok()?;
    last_opt(&atr)
}

fn compute_td_vi_006(series: &SymbolSeries<'_>) -> Option<f64> {
    let atr = compute_td_vi_004(series)?;
    let close = current(&series.close)?;
    ratio(atr * 100.0, close)
}

fn compute_td_vi_007(series: &SymbolSeries<'_>) -> Option<f64> {
    current_tr(series)
}

fn compute_td_vi_008(series: &SymbolSeries<'_>) -> Option<f64> {
    compute_td_vi_004(series)
}

fn compute_td_vi_014(series: &SymbolSeries<'_>) -> Option<f64> {
    pct_change_last(&series.close, 10)
}

fn compute_td_vi_015(series: &SymbolSeries<'_>) -> Option<f64> {
    let close = current(&series.close)?;
    let prev = shift_last(&series.close, 10)?;
    ratio(close, prev)
}

fn compute_td_vi_016(series: &SymbolSeries<'_>) -> Option<f64> {
    compute_td_vi_015(series).and_then(|v| finite_opt(Some(v * 100.0)))
}

fn compute_td_vi_017(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series
        .close
        .len()
        .min(series.high.len())
        .min(series.low.len());
    if n == 0 {
        return None;
    }
    let mut slowk = Vec::with_capacity(n);
    for i in 0..n {
        let start = i + 1 - (i + 1).min(14);
        let mut low_min = series.low.value_at(start);
        let mut high_max = series.high.value_at(start);
        for j in start + 1..=i {
            low_min = low_min.min(series.low.value_at(j));
            high_max = high_max.max(series.high.value_at(j));
        }
        let close = series.close.value_at(i);
        slowk.push(ratio((close - low_min) * 100.0, high_max - low_min));
    }
    let out = rolling_mean_series_opt(&slowk, 3, 1).ok()?;
    last_opt(&out)
}

fn compute_td_vi_018(series: &SymbolSeries<'_>) -> Option<f64> {
    compute_td_vi_017(series)
}

fn compute_tp_vpi_007(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series.close.len().min(series.volume.len());
    if n < 30 {
        return None;
    }
    let pxv: Vec<f64> = (0..n)
        .map(|i| series.close.value_at(i) * series.volume.value_at(i))
        .collect();
    let total = rolling_sum_exact(pxv.as_slice(), 30)?;
    let volume = rolling_sum_exact(&series.volume, 30)?;
    ratio(total, volume)
}

fn compute_tp_vpi_010(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series.close.len().min(series.volume.len());
    if n == 0 {
        return None;
    }
    let mut vpt = 0.0;
    for i in 1..n {
        let prev_close = series.close.value_at(i - 1);
        let close = series.close.value_at(i);
        if prev_close.abs() <= 1e-12 {
            continue;
        }
        vpt += series.volume.value_at(i) * ((close - prev_close) / prev_close);
    }
    finite_opt(Some(vpt))
}
