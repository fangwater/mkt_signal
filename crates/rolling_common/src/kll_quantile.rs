const SEGMENT_DIVISOR: usize = 20;
const KLL_LEVEL_CAPACITY: usize = 512;
const RNG_GAMMA: u64 = 0x9e3779b97f4a7c15;
const RNG_SEED: u64 = 0x243f6a8885a308d3;

#[derive(Clone)]
struct KllSketch {
    levels: Vec<Vec<f64>>,
    n: usize,
    level_capacity: usize,
    rng_state: u64,
}

impl KllSketch {
    fn new(level_capacity: usize, seed: u64) -> Self {
        Self {
            levels: vec![Vec::with_capacity(level_capacity.saturating_add(1))],
            n: 0,
            level_capacity,
            rng_state: seed,
        }
    }

    fn len(&self) -> usize {
        self.n
    }

    fn is_empty(&self) -> bool {
        self.n == 0
    }

    fn insert(&mut self, value: f64) {
        self.n = self.n.saturating_add(1);
        self.levels[0].push(value);
        self.compact_if_needed(0);
    }

    fn append_weighted_samples(&self, out: &mut Vec<WeightedSample>) {
        for (level, values) in self.levels.iter().enumerate() {
            let weight = level_weight(level);
            out.extend(
                values
                    .iter()
                    .copied()
                    .map(|value| WeightedSample { value, weight }),
            );
        }
    }

    fn compact_if_needed(&mut self, level: usize) {
        if self.levels[level].len() <= self.level_capacity {
            return;
        }

        self.levels[level].sort_by(|a, b| a.total_cmp(b));

        let len = self.levels[level].len();
        let compact_len = len & !1;
        let carry = (compact_len < len).then(|| self.levels[level][compact_len]);
        let offset = (next_random(&mut self.rng_state) & 1) as usize;
        let promoted = self.levels[level][offset..compact_len]
            .iter()
            .step_by(2)
            .copied()
            .collect::<Vec<_>>();

        self.levels[level].clear();
        if let Some(value) = carry {
            self.levels[level].push(value);
        }

        let next_level = level + 1;
        if self.levels.len() <= next_level {
            self.levels
                .push(Vec::with_capacity(self.level_capacity.saturating_add(1)));
        }
        self.levels[next_level].extend(promoted);
        self.compact_if_needed(next_level);
    }
}

#[derive(Clone, Copy)]
struct WeightedSample {
    value: f64,
    weight: usize,
}

pub fn segmented_quantiles_linear<I>(
    values: I,
    sample_size: usize,
    qs: &[f32],
) -> (usize, Vec<Option<f64>>)
where
    I: IntoIterator<Item = f64>,
{
    if sample_size == 0 {
        return (0, qs.iter().map(|_| None).collect());
    }

    let segment_size = (sample_size / SEGMENT_DIVISOR).max(1);
    let mut seed_state = RNG_SEED;
    let mut segments: Vec<KllSketch> = Vec::with_capacity(sample_size.div_ceil(segment_size));
    let mut current = new_sketch(&mut seed_state);
    let mut finite_count = 0usize;

    for value in values {
        if !value.is_finite() {
            continue;
        }

        finite_count = finite_count.saturating_add(1);
        current.insert(value);
        if current.len() >= segment_size {
            segments.push(current);
            current = new_sketch(&mut seed_state);
        }
    }
    if !current.is_empty() {
        segments.push(current);
    }

    let mut samples = Vec::new();
    for segment in &segments {
        segment.append_weighted_samples(&mut samples);
    }
    samples.sort_by(|a, b| a.value.total_cmp(&b.value));

    let quantiles = qs
        .iter()
        .map(|&q| quantile_from_weighted(&samples, q))
        .collect();
    (finite_count, quantiles)
}

fn new_sketch(seed_state: &mut u64) -> KllSketch {
    KllSketch::new(KLL_LEVEL_CAPACITY, next_random(seed_state))
}

fn quantile_from_weighted(samples: &[WeightedSample], q: f32) -> Option<f64> {
    if !q.is_finite() || !(0.0..=1.0).contains(&q) || samples.is_empty() {
        return None;
    }

    let total_weight = samples
        .iter()
        .fold(0usize, |acc, sample| acc.saturating_add(sample.weight));
    if total_weight == 0 {
        return None;
    }
    if total_weight == 1 {
        return samples.first().map(|sample| sample.value);
    }

    let rank = (q as f64) * ((total_weight - 1) as f64);
    let lower_idx = rank.floor() as usize;
    let upper_idx = rank.ceil() as usize;
    let frac = rank - lower_idx as f64;

    let lower = weighted_kth(samples, lower_idx)?;
    if lower_idx == upper_idx {
        return Some(lower);
    }

    let upper = weighted_kth(samples, upper_idx)?;
    Some(lower + (upper - lower) * frac)
}

fn weighted_kth(samples: &[WeightedSample], rank: usize) -> Option<f64> {
    let mut seen = 0usize;
    for sample in samples {
        seen = seen.saturating_add(sample.weight);
        if rank < seen {
            return Some(sample.value);
        }
    }
    samples.last().map(|sample| sample.value)
}

fn level_weight(level: usize) -> usize {
    1usize.checked_shl(level as u32).unwrap_or(usize::MAX)
}

fn next_random(state: &mut u64) -> u64 {
    *state = state.wrapping_add(RNG_GAMMA);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::segmented_quantiles_linear;

    #[test]
    fn small_windows_remain_exact() {
        let values = [2.0, 3.0, 4.0];
        let (count, quantiles) = segmented_quantiles_linear(values, values.len(), &[0.0, 0.5, 1.0]);
        assert_eq!(count, 3);
        assert_eq!(quantiles, vec![Some(2.0), Some(3.0), Some(4.0)]);
    }

    #[test]
    fn reconfigured_window_uses_passed_sample_size() {
        let values = [2.0, 3.0, 4.0, 5.0];
        let (count, quantiles) = segmented_quantiles_linear(values, values.len(), &[0.0, 0.5, 1.0]);
        assert_eq!(count, 4);
        assert_eq!(quantiles, vec![Some(2.0), Some(3.5), Some(5.0)]);
    }

    #[test]
    fn large_windows_return_quantiles() {
        let values = (10_000..20_000).map(|value| value as f64);
        let (count, quantiles) = segmented_quantiles_linear(values, 10_000, &[0.1, 0.5, 0.9]);
        assert_eq!(count, 10_000);
        for value in quantiles {
            assert!(value.is_some());
        }
    }

    #[test]
    fn non_finite_values_do_not_count() {
        let values = [1.0, f64::NAN, 3.0];
        let (count, quantiles) = segmented_quantiles_linear(values, values.len(), &[0.5]);
        assert_eq!(count, 2);
        assert_eq!(quantiles, vec![Some(2.0)]);
    }
}
