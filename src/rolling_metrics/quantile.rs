use std::cmp::Ordering;

/// 计算多个分位点，使用 `select_nth_unstable` + 线性插值，以匹配 pandas `method="linear"`。
/// 返回值长度与 `qs` 相同，非法分位点或样本不足时返回 `NaN`。
pub fn quantiles_linear_select_unstable(values: &mut [f32], qs: &[f32]) -> Vec<f32> {
    let mut result = vec![f32::NAN; qs.len()];
    if values.is_empty() {
        return result;
    }

    let n = values.len();
    let mut indexed: Vec<(usize, f32)> = qs.iter().copied().enumerate().collect();
    indexed.sort_by(|a, b| compare_f32(a.1, b.1));

    for (orig_idx, q_raw) in indexed {
        if !(0.0..=1.0).contains(&q_raw) {
            continue;
        }
        let q = q_raw.clamp(0.0, 1.0);
        if n == 1 {
            result[orig_idx] = values[0];
            continue;
        }

        let rank = q * ((n - 1) as f32);
        let lower_idx = rank.floor() as usize;
        let upper_idx = rank.ceil() as usize;
        let frac = rank - (lower_idx as f32);

        select_nth(values, lower_idx);
        let lower_val = values[lower_idx];

        if upper_idx == lower_idx {
            result[orig_idx] = lower_val;
            continue;
        }

        select_nth(values, upper_idx);
        let upper_val = values[upper_idx];
        let interp = lower_val + (upper_val - lower_val) * frac;
        result[orig_idx] = interp;
    }

    result
}

#[inline]
fn select_nth(values: &mut [f32], idx: usize) {
    values.select_nth_unstable_by(idx, |a, b| a.total_cmp(b));
}

#[inline]
fn compare_f32(a: f32, b: f32) -> Ordering {
    a.partial_cmp(&b).unwrap_or_else(|| a.total_cmp(&b))
}

#[cfg(test)]
mod tests {
    use super::quantiles_linear_select_unstable;

    #[test]
    fn basic_quantiles() {
        let mut data = vec![1.0, 2.0, 3.0, 4.0];
        let qs = [0.0, 0.25, 0.5, 0.75, 1.0];
        let res = quantiles_linear_select_unstable(&mut data, &qs);
        let expected = vec![1.0, 1.75, 2.5, 3.25, 4.0];
        for (got, exp) in res.iter().zip(expected.iter()) {
            assert!((got - exp).abs() < 1e-6);
        }
    }

    #[test]
    fn single_element() {
        let mut data = vec![42.0];
        let qs = [0.0, 0.5, 1.0];
        let res = quantiles_linear_select_unstable(&mut data, &qs);
        assert_eq!(res, vec![42.0, 42.0, 42.0]);
    }

    #[test]
    fn empty_returns_nan() {
        let mut data = Vec::new();
        let qs = [0.1, 0.9];
        let res = quantiles_linear_select_unstable(&mut data, &qs);
        assert!(res.iter().all(|v| v.is_nan()));
    }
}
