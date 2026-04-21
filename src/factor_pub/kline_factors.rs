use anyhow::Result;
use std::collections::VecDeque;

pub fn compute_hf_vol_std(closes: &VecDeque<f64>, window: usize) -> Result<Option<f64>> {
    let required = window + 1;
    if closes.len() < required {
        return Ok(None);
    }

    let start = closes.len() - required;
    let data: Vec<f64> = closes.iter().skip(start).copied().collect();
    let mut returns = Vec::with_capacity(window);
    for idx in 1..data.len() {
        let prev = data[idx - 1];
        if prev <= 0.0 {
            return Ok(None);
        }
        returns.push(data[idx] / prev - 1.0);
    }

    Ok(sample_std(&returns))
}

pub fn compute_hf_highlow_range(
    highs: &VecDeque<f64>,
    lows: &VecDeque<f64>,
    window: usize,
) -> Result<Option<f64>> {
    if highs.len() < window || lows.len() < window {
        return Ok(None);
    }

    let start_h = highs.len() - window;
    let start_l = lows.len() - window;
    let ranges: Vec<f64> = highs
        .iter()
        .skip(start_h)
        .zip(lows.iter().skip(start_l))
        .map(|(h, l)| h - l)
        .collect();
    Ok(mean(&ranges))
}

pub fn compute_hf_spread_return(
    closes: &VecDeque<f64>,
    return_period: usize,
    ma_window: usize,
) -> Result<Option<f64>> {
    let required = return_period + ma_window;
    if closes.len() < required {
        return Ok(None);
    }

    let start = closes.len() - required;
    let data: Vec<f64> = closes.iter().skip(start).copied().collect();
    let mut returns = Vec::with_capacity(ma_window);
    for idx in return_period..data.len() {
        let prev = data[idx - return_period];
        if prev <= 0.0 {
            return Ok(None);
        }
        returns.push(data[idx] / prev - 1.0);
    }

    Ok(mean(&returns))
}

pub fn compute_hf_count_mean(counts: &VecDeque<f64>, window: usize) -> Result<Option<f64>> {
    if counts.len() < window {
        return Ok(None);
    }

    let start = counts.len() - window;
    let data: Vec<f64> = counts.iter().skip(start).copied().collect();
    Ok(mean(&data))
}

pub fn compute_hf_vol_abs_pct_by_vol(
    closes: &VecDeque<f64>,
    volumes: &VecDeque<f64>,
    window: usize,
) -> Result<Option<f64>> {
    let required = window + 1;
    if closes.len() < required || volumes.len() < required {
        return Ok(None);
    }

    let start = closes.len() - required;
    let close_data: Vec<f64> = closes.iter().skip(start).copied().collect();
    let volume_data: Vec<f64> = volumes.iter().skip(start).copied().collect();

    let mut values = Vec::with_capacity(window);
    for idx in 1..close_data.len() {
        let prev = close_data[idx - 1];
        let vol = volume_data[idx];
        if prev <= 0.0 || vol <= 0.0 {
            return Ok(None);
        }
        let ret_abs = (close_data[idx] / prev - 1.0).abs();
        values.push(ret_abs / vol);
    }

    Ok(mean(&values))
}

pub fn compute_hf_volume_mean(volumes: &VecDeque<f64>, window: usize) -> Result<Option<f64>> {
    if volumes.len() < window {
        return Ok(None);
    }

    let start = volumes.len() - window;
    let data: Vec<f64> = volumes.iter().skip(start).copied().collect();
    Ok(mean(&data))
}

pub fn compute_hf_price_volume_corr(
    closes: &VecDeque<f64>,
    volumes: &VecDeque<f64>,
    window: usize,
) -> Result<Option<f64>> {
    if closes.len() < window || volumes.len() < window {
        return Ok(None);
    }

    let start_close = closes.len() - window;
    let start_volume = volumes.len() - window;
    let close_data: Vec<f64> = closes.iter().skip(start_close).copied().collect();
    let volume_data: Vec<f64> = volumes.iter().skip(start_volume).copied().collect();
    Ok(pearson_corr_last_window(&close_data, &volume_data))
}

pub fn compute_hf_vol_volume_combined(
    closes: &VecDeque<f64>,
    volumes: &VecDeque<f64>,
    vol_window: usize,
    volu_window: usize,
) -> Result<Option<f64>> {
    let vol = compute_hf_vol_std(closes, vol_window)?;
    let volu = compute_hf_volume_mean(volumes, volu_window)?;
    match (vol, volu) {
        (Some(v1), Some(v2)) => {
            let out = v1 * v2;
            if out.is_finite() {
                Ok(Some(out))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() || values.iter().any(|value| !value.is_finite()) {
        return None;
    }
    let out = values.iter().sum::<f64>() / values.len() as f64;
    if out.is_finite() {
        Some(out)
    } else {
        None
    }
}

fn sample_std(values: &[f64]) -> Option<f64> {
    if values.len() < 2 || values.iter().any(|value| !value.is_finite()) {
        return None;
    }

    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| {
            let delta = *value - mean;
            delta * delta
        })
        .sum::<f64>()
        / (values.len() as f64 - 1.0);
    let std = variance.sqrt();
    if std.is_finite() {
        Some(std)
    } else {
        None
    }
}

fn pearson_corr_last_window(xs: &[f64], ys: &[f64]) -> Option<f64> {
    if xs.len() != ys.len()
        || xs.is_empty()
        || xs.iter().any(|value| !value.is_finite())
        || ys.iter().any(|value| !value.is_finite())
    {
        return None;
    }

    let n = xs.len() as f64;
    let mean_x = xs.iter().sum::<f64>() / n;
    let mean_y = ys.iter().sum::<f64>() / n;

    let mut cov = 0.0;
    let mut var_x = 0.0;
    let mut var_y = 0.0;

    for (x, y) in xs.iter().zip(ys.iter()) {
        let dx = *x - mean_x;
        let dy = *y - mean_y;
        cov += dx * dy;
        var_x += dx * dx;
        var_y += dy * dy;
    }

    if var_x <= 0.0 || var_y <= 0.0 {
        return None;
    }

    let corr = cov / (var_x.sqrt() * var_y.sqrt());
    if corr.is_finite() {
        Some(corr)
    } else {
        None
    }
}

pub fn compute_rl_return_volatility(
    closes: &VecDeque<f64>,
    pct_change_period: usize,
    rolling_window: usize,
) -> Result<Option<f64>> {
    crate::factor_pub::rl_vol::compute_rl_return_volatility(
        closes,
        pct_change_period,
        rolling_window,
    )
}
