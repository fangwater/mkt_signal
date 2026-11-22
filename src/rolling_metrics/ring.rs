use parking_lot::{Mutex, RwLock};

use super::quantiles::QuantileWindow;

struct Samples {
    values: Vec<f32>,
    end: usize,
}

impl Samples {
    fn slices(&self, start: usize, end: usize) -> (&[f32], &[f32]) {
        let length = end - start;
        let start = start % self.values.len();
        let first_length = length.min(self.values.len() - start);
        (
            &self.values[start..start + first_length],
            &self.values[..length - first_length],
        )
    }
}

struct IndexedWindow {
    end: usize,
    width: usize,
    values: QuantileWindow,
}

/// A fixed-size history. The reader copies just the published delta under
/// the read lock and does all sorting and index maintenance after releasing it.
pub struct RingBuffer {
    samples: RwLock<Samples>,
    quantiles: Mutex<Option<IndexedWindow>>,
}

impl RingBuffer {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "ring capacity must be positive");
        Self {
            samples: RwLock::new(Samples {
                values: vec![0.0; capacity],
                end: 0,
            }),
            quantiles: Mutex::new(None),
        }
    }

    pub fn capacity(&self) -> usize {
        self.samples.read().values.len()
    }

    pub fn push(&self, value: f32) {
        let mut samples = self.samples.write();
        let index = samples.end % samples.values.len();
        samples.values[index] = value;
        samples.end += 1;
    }

    pub fn len(&self) -> usize {
        let samples = self.samples.read();
        samples.end.min(samples.values.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn last(&self) -> Option<f32> {
        let samples = self.samples.read();
        let position = samples.end.checked_sub(1)? % samples.values.len();
        Some(samples.values[position])
    }

    pub fn quantiles_linear(&self, active_window: usize, qs: &[f32]) -> (usize, Vec<Option<f64>>) {
        let mut cache = self.quantiles.lock();
        let samples = self.samples.read();
        let width = active_window.clamp(1, samples.values.len());
        let end = samples.end;
        let start = end.saturating_sub(width);
        if qs.is_empty() {
            *cache = None;
            let (first, second) = samples.slices(start, end);
            let count = first.iter().chain(second).filter(|v| v.is_finite()).count();
            return (count, Vec::new());
        }
        let indexed = cache.as_mut().filter(|indexed| {
            indexed.width == width && (indexed.end > start || indexed.end == end)
        });
        let delta_start = indexed.as_ref().map_or(start, |indexed| indexed.end);
        let (first, second) = samples.slices(delta_start, end);
        let mut delta = Vec::with_capacity(end - delta_start);
        delta.extend_from_slice(first);
        delta.extend_from_slice(second);
        drop(samples);

        if let Some(indexed) = indexed {
            for value in delta {
                indexed.values.push(value);
            }
            indexed.end = end;
            indexed.values.quantiles(qs)
        } else {
            let mut values = QuantileWindow::new(width, delta);
            let result = values.quantiles(qs);
            *cache = Some(IndexedWindow { end, width, values });
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RingBuffer;

    #[test]
    fn quantiles_follow_latest_active_window() {
        let ring = RingBuffer::new(5);
        for value in [1.0, 2.0, 3.0, 4.0] {
            ring.push(value);
        }
        assert_eq!(
            ring.quantiles_linear(3, &[0.0, 0.5, 1.0]),
            (3, vec![Some(2.0), Some(3.0), Some(4.0)])
        );
        ring.push(5.0);
        ring.push(6.0);
        assert_eq!(
            ring.quantiles_linear(3, &[0.0, 0.5, 1.0]),
            (3, vec![Some(4.0), Some(5.0), Some(6.0)])
        );
    }

    #[test]
    fn query_rebuilds_after_window_changes_and_cursor_lag() {
        let ring = RingBuffer::new(6);
        assert_eq!(ring.quantiles_linear(4, &[0.5]), (0, vec![None]));
        for value in [1.0, 2.0, 3.0, 4.0] {
            ring.push(value);
        }
        assert_eq!(ring.quantiles_linear(2, &[0.5]), (2, vec![Some(3.5)]));
        assert_eq!(ring.quantiles_linear(4, &[0.5]), (4, vec![Some(2.5)]));
        for value in [50.0, 71.0, f32::INFINITY, 80.0, 90.0, 99.0, 100.0] {
            ring.push(value);
        }
        assert_eq!(
            ring.quantiles_linear(4, &[0.0, 0.5, 1.0]),
            (4, vec![Some(80.0), Some(94.5), Some(100.0)])
        );
        assert_eq!(ring.quantiles_linear(6, &[]), (5, vec![]));
        assert_eq!(ring.quantiles_linear(6, &[0.5]), (5, vec![Some(90.0)]));
    }
}
