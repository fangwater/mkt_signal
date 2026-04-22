use anyhow::Result;
use std::collections::VecDeque;

pub fn compute_rl_return_volatility(
    closes: &VecDeque<f64>,
    pct_change_period: usize,
    rolling_window: usize,
) -> Result<Option<f64>> {
    let required = pct_change_period + rolling_window;
    if closes.len() < required {
        return Ok(None);
    }

    let start = closes.len() - required;
    let window: Vec<f64> = closes.iter().skip(start).copied().collect();
    let mut returns = Vec::with_capacity(rolling_window);
    for idx in pct_change_period..window.len() {
        let prev = window[idx - pct_change_period];
        if prev <= 0.0 {
            return Ok(None);
        }
        returns.push(window[idx] / prev - 1.0);
    }

    Ok(sample_std(&returns))
}

fn sample_std(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
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

#[cfg(test)]
mod tests {
    use super::compute_rl_return_volatility;
    use std::collections::VecDeque;

    #[test]
    fn returns_none_when_history_is_short() {
        let closes = VecDeque::from(vec![100.0, 101.0, 102.0]);
        let value = compute_rl_return_volatility(&closes, 2, 3).unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn computes_sample_std_over_recent_returns() {
        let closes = VecDeque::from(vec![100.0, 102.0, 104.0, 103.0, 106.0]);
        let value = compute_rl_return_volatility(&closes, 1, 4)
            .unwrap()
            .unwrap();

        let returns = [
            0.02,
            104.0 / 102.0 - 1.0,
            103.0 / 104.0 - 1.0,
            106.0 / 103.0 - 1.0,
        ];
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns
            .iter()
            .map(|ret| {
                let delta = *ret - mean;
                delta * delta
            })
            .sum::<f64>()
            / (returns.len() as f64 - 1.0);
        let expected = variance.sqrt();

        assert!((value - expected).abs() < 1e-12);
    }
}
