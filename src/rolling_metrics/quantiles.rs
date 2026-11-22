use std::collections::VecDeque;

// Packed sorted blocks keep duplicate values and avoid one allocation per
// distinct value. Insertions and expirations move at most one block of keys.
const BLOCK_CAPACITY: usize = 512;
const BLOCK_TARGET: usize = BLOCK_CAPACITY / 2;
const BLOCK_MINIMUM: usize = BLOCK_CAPACITY / 4;

#[derive(Default)]
struct SortedBlocks {
    blocks: Vec<Vec<u32>>,
    ends: Vec<usize>,
}

impl SortedBlocks {
    fn new(mut keys: Vec<u32>) -> Self {
        keys.sort_unstable();
        let blocks = keys.chunks(BLOCK_TARGET).map(Self::block).collect();
        Self {
            blocks,
            ends: Vec::new(),
        }
    }

    fn block(keys: &[u32]) -> Vec<u32> {
        let mut block = Vec::with_capacity(BLOCK_CAPACITY);
        block.extend_from_slice(keys);
        block
    }

    fn block_index(&self, key: u32) -> usize {
        self.blocks
            .partition_point(|block| *block.last().unwrap() < key)
            .min(self.blocks.len() - 1)
    }

    fn insert(&mut self, key: u32) {
        if self.blocks.is_empty() {
            self.blocks.push(Self::block(&[key]));
            return;
        }

        let index = self.block_index(key);
        let block = &mut self.blocks[index];
        if block.len() == BLOCK_CAPACITY {
            let right = Self::block(&block[BLOCK_TARGET..]);
            block.truncate(BLOCK_TARGET);
            self.blocks.insert(index + 1, right);
        }
        let index = self.block_index(key);
        let block = &mut self.blocks[index];
        block.insert(block.partition_point(|&existing| existing < key), key);
    }

    fn remove(&mut self, key: u32) {
        let index = self.block_index(key);
        let block = &mut self.blocks[index];
        let position = block.binary_search(&key).expect("expired key was indexed");
        block.remove(position);
        if block.is_empty() {
            self.blocks.remove(index);
            return;
        }
        if block.len() >= BLOCK_MINIMUM || self.blocks.len() == 1 {
            return;
        }

        let left = index.min(self.blocks.len() - 2);
        let right = left + 1;
        let combined = self.blocks[left].len() + self.blocks[right].len();
        if combined <= BLOCK_CAPACITY {
            let merged = self.blocks.remove(right);
            self.blocks[left].extend(merged);
            return;
        }

        // The combined block would be too large. Restore balanced occupancy
        // while keeping the boundary keys ordered.
        let (head, tail) = self.blocks.split_at_mut(right);
        let left = &mut head[left];
        let right = &mut tail[0];
        let target = combined / 2;
        if left.len() < target {
            left.extend(right.drain(..target - left.len()));
        } else {
            right.splice(..0, left.drain(target..));
        }
    }

    fn index_ranks(&mut self) -> usize {
        self.ends.clear();
        let mut end = 0;
        self.ends.extend(self.blocks.iter().map(|block| {
            end += block.len();
            end
        }));
        end
    }

    fn at_rank(&self, rank: usize) -> f64 {
        let index = self.ends.partition_point(|&end| end <= rank);
        let start = if index == 0 { 0 } else { self.ends[index - 1] };
        from_key(self.blocks[index][rank - start]) as f64
    }
}

// IEEE total order, including the distinction between -0 and +0.
fn to_key(value: f32) -> u32 {
    let bits = value.to_bits();
    if bits >> 31 == 0 {
        bits ^ 0x8000_0000
    } else {
        !bits
    }
}

fn from_key(key: u32) -> f32 {
    f32::from_bits(if key >> 31 == 0 {
        !key
    } else {
        key ^ 0x8000_0000
    })
}

pub(super) struct QuantileWindow {
    width: usize,
    slots: VecDeque<f32>,
    sorted: SortedBlocks,
}

impl QuantileWindow {
    pub(super) fn new(width: usize, samples: Vec<f32>) -> Self {
        let keys = samples
            .iter()
            .copied()
            .filter(|v| v.is_finite())
            .map(to_key)
            .collect();
        let mut slots: VecDeque<_> = samples.into();
        slots.reserve_exact(width - slots.len());
        Self {
            width,
            slots,
            sorted: SortedBlocks::new(keys),
        }
    }

    pub(super) fn push(&mut self, value: f32) {
        let expired = if self.slots.len() == self.width {
            self.slots.pop_front()
        } else {
            None
        };
        self.slots.push_back(value);
        if expired.is_some_and(|old| old.to_bits() == value.to_bits()) {
            return;
        }
        if let Some(old) = expired.filter(|old| old.is_finite()) {
            self.sorted.remove(to_key(old));
        }
        if value.is_finite() {
            self.sorted.insert(to_key(value));
        }
    }

    pub(super) fn quantiles(&mut self, qs: &[f32]) -> (usize, Vec<Option<f64>>) {
        let count = self.sorted.index_ranks();
        let values = qs
            .iter()
            .map(|&q| {
                if count == 0 || !(0.0..=1.0).contains(&q) {
                    return None;
                }
                let rank = q as f64 * (count - 1) as f64;
                let lower_rank = rank.floor() as usize;
                let upper_rank = rank.ceil() as usize;
                let lower = self.sorted.at_rank(lower_rank);
                if lower_rank == upper_rank {
                    return Some(lower);
                }
                let upper = self.sorted.at_rank(upper_rank);
                Some(lower + (upper - lower) * (rank - lower_rank as f64))
            })
            .collect();
        (count, values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_splits_and_rebalances_preserve_ranks() {
        let mut window = QuantileWindow::new(3_000, Vec::new());
        let mut reference = VecDeque::new();
        let mut state = 1234567u32;
        let qs = [0.0, 0.03, 0.5, 0.95, 1.0];
        for index in 0..30_000 {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            let value = match index / 5_000 {
                0 | 3 => index as f32,
                1 => -(index as f32),
                2 => [0.0, -0.0, 18.0, f32::NAN][state as usize % 4],
                _ => (state % 25_000) as f32,
            };
            window.push(value);
            if reference.len() == 3_000 {
                reference.pop_front();
            }
            reference.push_back(value);
            if index % 51 != 0 {
                continue;
            }
            let mut sorted: Vec<f64> = reference
                .iter()
                .copied()
                .filter(|v| v.is_finite())
                .map(f64::from)
                .collect();
            sorted.sort_unstable_by(f64::total_cmp);
            let (count, quantiles) = window.quantiles(&qs);
            assert_eq!(count, sorted.len());
            for (q, actual) in qs.into_iter().zip(quantiles) {
                let rank = q as f64 * (count - 1) as f64;
                let left = sorted[rank.floor() as usize];
                let right = sorted[rank.ceil() as usize];
                let expected = if rank.floor() == rank.ceil() {
                    left
                } else {
                    left + (right - left) * rank.fract()
                };
                assert_eq!(actual.unwrap().to_bits(), expected.to_bits());
            }
            assert!(window
                .sorted
                .blocks
                .iter()
                .all(|block| !block.is_empty() && block.len() <= BLOCK_CAPACITY));
        }
    }
}
