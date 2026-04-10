use crate::math::{
    finite_opt, rolling_mean_last_opt_from_series, rolling_sum_at_from_series,
    rolling_sum_series_opt,
};
use crate::view::{F64SeriesView, SymbolSeries};

pub fn build_true_range_series(series: &SymbolSeries<'_>) -> Option<Vec<Option<f64>>> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n == 0 {
        return None;
    }
    let mut tr = Vec::with_capacity(n);
    tr.push(None);
    for i in 1..n {
        let high = series.high[i];
        let low = series.low[i];
        let prev_close = series.close[i - 1];
        if !high.is_finite() || !low.is_finite() || !prev_close.is_finite() {
            tr.push(Some(f64::NAN));
            continue;
        }
        let value = (high - low)
            .max((high - prev_close).abs())
            .max((low - prev_close).abs());
        tr.push(finite_opt(Some(value)));
    }
    Some(tr)
}

pub fn build_ad_line_rolling360(series: &SymbolSeries<'_>) -> Option<Vec<Option<f64>>> {
    let n = series
        .close
        .len()
        .min(series.high.len())
        .min(series.low.len())
        .min(series.volume.len());
    if n == 0 {
        return None;
    }
    let mut clv_x = Vec::with_capacity(n);
    for i in 0..n {
        let high = series.high[i];
        let low = series.low[i];
        let close = series.close[i];
        let volume = series.volume[i];
        let den = high - low;
        if !high.is_finite() || !low.is_finite() || !close.is_finite() || !volume.is_finite() {
            clv_x.push(Some(f64::NAN));
            continue;
        }
        if den.abs() > 1e-12 {
            let raw = ((close - low) - (high - close)) / den;
            clv_x.push(finite_opt(Some(raw * volume)));
        } else {
            clv_x.push(Some(f64::NAN));
        }
    }
    rolling_sum_series_opt(&clv_x, 360, 360).ok()
}

pub fn build_obv_cumulative(series: &SymbolSeries<'_>) -> Option<Vec<f64>> {
    let n = series.close.len().min(series.volume.len());
    if n == 0 {
        return None;
    }
    let mut obv = vec![0.0; n];
    for i in 1..n {
        let prev = obv[i - 1];
        let value = if series.close[i] > series.close[i - 1] {
            prev + series.volume[i]
        } else if series.close[i] < series.close[i - 1] {
            prev - series.volume[i]
        } else {
            prev
        };
        obv[i] = if value.is_finite() { value } else { 0.0 };
    }
    Some(obv)
}

pub fn build_ht_dc_level(values: &(impl F64SeriesView + ?Sized)) -> Vec<f64> {
    let mut out = vec![0.0; values.len()];
    for (i, item) in out.iter_mut().enumerate().skip(6) {
        *item = (2.0 * values.value_at(i) - values.value_at(i - 4)) / 3.0;
    }
    out
}

pub fn build_ht_dc_phase(values: &(impl F64SeriesView + ?Sized)) -> Vec<f64> {
    let mut out = vec![0.0; values.len()];
    for (i, item) in out.iter_mut().enumerate().skip(6) {
        *item = (values.value_at(i) - values.value_at(i - 4)) / 3.0;
    }
    out
}

pub fn compute_tp_vpi_006(series: &SymbolSeries<'_>) -> Option<f64> {
    let n = series.close.len().min(series.volume.len());
    if n < 373 {
        return None;
    }
    let mut obv_delta = Vec::with_capacity(n);
    obv_delta.push(0.0);
    for i in 1..n {
        let d = series.close[i] - series.close[i - 1];
        let v = if d > 0.0 {
            series.volume[i]
        } else if d < 0.0 {
            -series.volume[i]
        } else {
            0.0
        };
        obv_delta.push(v);
    }
    let start = n - 14;
    let mut obv_tail = Vec::with_capacity(14);
    for end in start + 1..=n {
        obv_tail.push(
            rolling_sum_at_from_series(&obv_delta, end, 360, 360)
                .ok()
                .flatten(),
        );
    }
    rolling_mean_last_opt_from_series(&obv_tail, 14, 14)
        .ok()
        .flatten()
        .and_then(|v| finite_opt(Some(v)))
}
