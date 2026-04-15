use std::collections::VecDeque;

const DEFAULT_SCALE: f64 = 1_000_000.0;
const RNG_GAMMA: u64 = 0x9e3779b97f4a7c15;
const RNG_SEED: u64 = 0x243f6a8885a308d3;

type Link = Option<Box<Node>>;

struct Node {
    key: i64,
    priority: u64,
    count: usize,
    subtree_count: usize,
    left: Link,
    right: Link,
}

impl Node {
    fn new(key: i64, priority: u64) -> Self {
        Self {
            key,
            priority,
            count: 1,
            subtree_count: 1,
            left: None,
            right: None,
        }
    }

    fn refresh(&mut self) {
        self.subtree_count = self.count + link_size(&self.left) + link_size(&self.right);
    }
}

pub struct SlidingQuantileWindow {
    history_capacity: usize,
    active_window: usize,
    scale: f64,
    history: VecDeque<i64>,
    active: VecDeque<i64>,
    root: Link,
    rng_state: u64,
}

impl SlidingQuantileWindow {
    pub fn new(history_capacity: usize, active_window: usize) -> Self {
        Self::new_with_scale(history_capacity, active_window, DEFAULT_SCALE)
    }

    pub fn new_with_scale(history_capacity: usize, active_window: usize, scale: f64) -> Self {
        let history_capacity = history_capacity.max(1);
        let active_window = active_window.min(history_capacity).max(1);
        Self {
            history_capacity,
            active_window,
            scale: normalize_scale(scale),
            history: VecDeque::with_capacity(history_capacity),
            active: VecDeque::with_capacity(active_window),
            root: None,
            rng_state: RNG_SEED,
        }
    }

    pub fn history_capacity(&self) -> usize {
        self.history_capacity
    }

    pub fn active_window(&self) -> usize {
        self.active_window
    }

    pub fn sample_size(&self) -> usize {
        self.active.len()
    }

    pub fn last(&self) -> Option<f64> {
        self.active.back().map(|&key| self.dequantize_key(key))
    }

    pub fn push_f64(&mut self, value: f64) -> bool {
        let Some(key) = self.quantize_value(value) else {
            return false;
        };

        self.history.push_back(key);
        while self.history.len() > self.history_capacity {
            let _ = self.history.pop_front();
        }

        self.active.push_back(key);
        insert_link(&mut self.root, key, next_priority(&mut self.rng_state));
        while self.active.len() > self.active_window {
            if let Some(expired) = self.active.pop_front() {
                let removed = remove_link(&mut self.root, expired);
                debug_assert!(removed, "active tree missing expired key");
            }
        }
        true
    }

    pub fn reconfigure(&mut self, history_capacity: usize, active_window: usize) {
        self.history_capacity = history_capacity.max(1);
        self.active_window = active_window.min(self.history_capacity).max(1);

        while self.history.len() > self.history_capacity {
            let _ = self.history.pop_front();
        }

        self.rebuild_active_tree();
    }

    pub fn quantile_linear(&self, q: f32) -> Option<f64> {
        if !q.is_finite() || !(0.0..=1.0).contains(&q) {
            return None;
        }

        let n = self.sample_size();
        if n == 0 {
            return None;
        }
        if n == 1 {
            return self
                .active
                .front()
                .copied()
                .map(|key| self.dequantize_key(key));
        }

        let rank = (q as f64) * ((n - 1) as f64);
        let lower_idx = rank.floor() as usize;
        let upper_idx = rank.ceil() as usize;
        let frac = rank - lower_idx as f64;

        let lower = kth_key(&self.root, lower_idx)?;
        if lower_idx == upper_idx {
            return Some(self.dequantize_key(lower));
        }

        let upper = kth_key(&self.root, upper_idx)?;
        let lower_f = self.dequantize_key(lower);
        let upper_f = self.dequantize_key(upper);
        Some(lower_f + (upper_f - lower_f) * frac)
    }

    pub fn quantiles_linear(&self, qs: &[f32]) -> Vec<Option<f64>> {
        qs.iter().map(|&q| self.quantile_linear(q)).collect()
    }

    pub fn percentile_rank_f64(&self, value: f64) -> Option<f64> {
        let key = self.quantize_value(value)?;
        self.percentile_rank_key(key)
    }

    pub fn percentile_rank_if_pushed_f64(&self, value: f64) -> Option<f64> {
        let key = self.quantize_value(value)?;
        let n = self.sample_size();
        let next_n = n.saturating_add(1);
        let less = count_lt(&self.root, key);
        let less_or_equal = count_le(&self.root, key);
        let equal = less_or_equal.saturating_sub(less).saturating_add(1);
        Some((less as f64 + (equal as f64 * 0.5)) / next_n as f64)
    }

    pub fn percentile_rank_last(&self) -> Option<f64> {
        let key = *self.active.back()?;
        self.percentile_rank_key(key)
    }

    fn rebuild_active_tree(&mut self) {
        self.active.clear();
        self.root = None;

        let keep = self.active_window.min(self.history.len());
        let skip = self.history.len().saturating_sub(keep);
        for &key in self.history.iter().skip(skip) {
            self.active.push_back(key);
            insert_link(&mut self.root, key, next_priority(&mut self.rng_state));
        }
    }

    fn percentile_rank_key(&self, key: i64) -> Option<f64> {
        let n = self.sample_size();
        if n == 0 {
            return None;
        }

        let less = count_lt(&self.root, key);
        let less_or_equal = count_le(&self.root, key);
        let equal = less_or_equal.saturating_sub(less);
        Some((less as f64 + (equal as f64 * 0.5)) / n as f64)
    }

    fn quantize_value(&self, value: f64) -> Option<i64> {
        quantize_value(value, self.scale)
    }

    fn dequantize_key(&self, key: i64) -> f64 {
        dequantize_key(key, self.scale)
    }
}

fn normalize_scale(scale: f64) -> f64 {
    if scale.is_finite() && scale > 0.0 {
        scale
    } else {
        DEFAULT_SCALE
    }
}

fn quantize_value(value: f64, scale: f64) -> Option<i64> {
    if !value.is_finite() {
        return None;
    }
    let scaled = (value * scale).round();
    if scaled < i64::MIN as f64 || scaled > i64::MAX as f64 {
        return None;
    }
    Some(scaled as i64)
}

fn dequantize_key(key: i64, scale: f64) -> f64 {
    key as f64 / scale
}

fn next_priority(state: &mut u64) -> u64 {
    *state = state.wrapping_add(RNG_GAMMA);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

fn link_size(link: &Link) -> usize {
    link.as_ref().map(|node| node.subtree_count).unwrap_or(0)
}

fn rotate_left(mut root: Box<Node>) -> Box<Node> {
    let mut new_root = root.right.take().expect("rotate_left missing right child");
    root.right = new_root.left.take();
    root.refresh();
    new_root.left = Some(root);
    new_root.refresh();
    new_root
}

fn rotate_right(mut root: Box<Node>) -> Box<Node> {
    let mut new_root = root.left.take().expect("rotate_right missing left child");
    root.left = new_root.right.take();
    root.refresh();
    new_root.right = Some(root);
    new_root.refresh();
    new_root
}

fn insert_link(link: &mut Link, key: i64, priority: u64) {
    match link.take() {
        None => {
            *link = Some(Box::new(Node::new(key, priority)));
        }
        Some(mut node) => {
            if key == node.key {
                node.count += 1;
            } else if key < node.key {
                insert_link(&mut node.left, key, priority);
                if node
                    .left
                    .as_ref()
                    .map(|child| child.priority > node.priority)
                    .unwrap_or(false)
                {
                    node = rotate_right(node);
                }
            } else {
                insert_link(&mut node.right, key, priority);
                if node
                    .right
                    .as_ref()
                    .map(|child| child.priority > node.priority)
                    .unwrap_or(false)
                {
                    node = rotate_left(node);
                }
            }
            node.refresh();
            *link = Some(node);
        }
    }
}

fn remove_link(link: &mut Link, key: i64) -> bool {
    match link.take() {
        None => {
            *link = None;
            false
        }
        Some(mut node) => {
            let removed = if key < node.key {
                let removed = remove_link(&mut node.left, key);
                node.refresh();
                *link = Some(node);
                removed
            } else if key > node.key {
                let removed = remove_link(&mut node.right, key);
                node.refresh();
                *link = Some(node);
                removed
            } else if node.count > 1 {
                node.count -= 1;
                node.refresh();
                *link = Some(node);
                true
            } else {
                match (node.left.take(), node.right.take()) {
                    (None, None) => {
                        *link = None;
                        true
                    }
                    (Some(left), None) => {
                        *link = Some(left);
                        true
                    }
                    (None, Some(right)) => {
                        *link = Some(right);
                        true
                    }
                    (Some(left), Some(right)) => {
                        node.left = Some(left);
                        node.right = Some(right);
                        let left_priority =
                            node.left.as_ref().map(|child| child.priority).unwrap_or(0);
                        let right_priority =
                            node.right.as_ref().map(|child| child.priority).unwrap_or(0);
                        if left_priority > right_priority {
                            let mut rotated = rotate_right(node);
                            let removed = remove_link(&mut rotated.right, key);
                            rotated.refresh();
                            *link = Some(rotated);
                            removed
                        } else {
                            let mut rotated = rotate_left(node);
                            let removed = remove_link(&mut rotated.left, key);
                            rotated.refresh();
                            *link = Some(rotated);
                            removed
                        }
                    }
                }
            };
            removed
        }
    }
}

fn kth_key(link: &Link, rank: usize) -> Option<i64> {
    let node = link.as_ref()?;
    let left_size = link_size(&node.left);
    if rank < left_size {
        kth_key(&node.left, rank)
    } else if rank < left_size + node.count {
        Some(node.key)
    } else {
        kth_key(&node.right, rank.saturating_sub(left_size + node.count))
    }
}

fn count_lt(link: &Link, key: i64) -> usize {
    let Some(node) = link.as_ref() else {
        return 0;
    };

    if key <= node.key {
        count_lt(&node.left, key)
    } else {
        link_size(&node.left) + node.count + count_lt(&node.right, key)
    }
}

fn count_le(link: &Link, key: i64) -> usize {
    let Some(node) = link.as_ref() else {
        return 0;
    };

    if key < node.key {
        count_le(&node.left, key)
    } else {
        link_size(&node.left) + node.count + count_le(&node.right, key)
    }
}

#[cfg(test)]
mod tests {
    use super::SlidingQuantileWindow;

    #[test]
    fn quantile_tracks_sliding_window() {
        let mut window = SlidingQuantileWindow::new(5, 3);
        for value in [1.0, 2.0, 3.0, 4.0] {
            assert!(window.push_f64(value));
        }

        assert_eq!(window.sample_size(), 3);
        assert_eq!(window.quantile_linear(0.0), Some(2.0));
        assert_eq!(window.quantile_linear(0.5), Some(3.0));
        assert_eq!(window.quantile_linear(1.0), Some(4.0));
    }

    #[test]
    fn reconfigure_rebuilds_from_history() {
        let mut window = SlidingQuantileWindow::new(6, 2);
        for value in [1.0, 2.0, 3.0, 4.0, 5.0] {
            assert!(window.push_f64(value));
        }

        window.reconfigure(6, 4);
        assert_eq!(window.sample_size(), 4);
        assert_eq!(window.quantile_linear(0.0), Some(2.0));
        assert_eq!(window.quantile_linear(1.0), Some(5.0));
    }

    #[test]
    fn duplicate_values_use_counts() {
        let mut window = SlidingQuantileWindow::new(5, 5);
        for value in [1.234567, 1.2345674, 1.23456749, 2.0] {
            assert!(window.push_f64(value));
        }

        assert_eq!(window.sample_size(), 4);
        assert_eq!(window.quantile_linear(0.5), Some(1.234567));
    }

    #[test]
    fn percentile_rank_uses_midpoint_rank() {
        let mut window = SlidingQuantileWindow::new(5, 5);
        for value in [1.0, 2.0, 2.0, 4.0] {
            assert!(window.push_f64(value));
        }

        assert_eq!(window.percentile_rank_f64(2.0), Some(0.5));
        assert_eq!(window.percentile_rank_last(), Some(0.875));
    }
}
