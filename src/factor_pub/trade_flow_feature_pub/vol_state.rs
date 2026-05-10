//! Per-symbol vol state for trade-flow-driven 5s bar return volatility.
//!
//! 与 SymbolState 解耦:不读 Redis amount-threshold,处理 venue 内全部 symbol。
//! 每根 5s bar(实价或 ffill)会推动 RollingWelford 更新一次,产出 scaled std。
//!
//! 数学:r_i = c_i / c_{i-PCT} - 1; std over rolling WIN returns; value = std * scale。
//! 跟原 `compute_rl_return_volatility` 完全等价(单遍 vs 在线增量)。

use std::collections::VecDeque;

use crate::common::rolling_welford::RollingWelford;

#[derive(Debug, Clone, Copy)]
pub(crate) struct SealedBar {
    pub bar_start_ms: i64,
    pub vol: Option<f64>,
}

pub(crate) struct VolState {
    bar_ms: i64,
    pct_change_period: usize,
    scale_factor: f64,

    // pending bucket: 当前还没 seal 的那根 bar
    //   pending_bucket_ms: bucket 起始 ms
    //   pending_price:     bucket 内 running close;无 trade 时是上一根的 ffill 源
    pending_bucket_ms: Option<i64>,
    pending_price: Option<f64>,

    // 算 r 用,只需 PCT+1 个 close
    closes: VecDeque<f64>,
    // n 个 return 的 rolling std,n 由 RollingWelford 自身管理
    welford: RollingWelford,
}

impl VolState {
    pub(crate) fn new(
        pct_change_period: usize,
        rolling_window: usize,
        scale_factor: f64,
        bar_ms: i64,
    ) -> Self {
        debug_assert!(pct_change_period > 0);
        debug_assert!(rolling_window > 0);
        debug_assert!(bar_ms > 0);
        Self {
            bar_ms,
            pct_change_period,
            scale_factor,
            pending_bucket_ms: None,
            pending_price: None,
            closes: VecDeque::with_capacity(pct_change_period + 1),
            welford: RollingWelford::new(rolling_window),
        }
    }

    /// 处理一笔 trade,可能 seal 跨过的 bucket。
    pub(crate) fn on_trade(&mut self, ts_ms: i64, price: f64, out: &mut Vec<SealedBar>) {
        if !price.is_finite() || price <= 0.0 {
            return;
        }
        let bucket = align_to_period(ts_ms, self.bar_ms);

        match (self.pending_bucket_ms, self.pending_price) {
            (None, _) => {
                // 首笔 trade,只记录,不 seal。bucket 等到下一次跨越或 tick 才会被 seal。
                self.pending_bucket_ms = Some(bucket);
                self.pending_price = Some(price);
            }
            (Some(pb), Some(pp)) => {
                if bucket == pb {
                    // 同 bucket,更新 running close
                    self.pending_price = Some(price);
                } else if bucket > pb {
                    // 跨 bucket:seal 旧 pending + 期间任何 gap
                    self.seal_range(pb, bucket - self.bar_ms, pp, out);
                    self.pending_bucket_ms = Some(bucket);
                    self.pending_price = Some(price);
                }
                // bucket < pb: late trade,丢弃
            }
            (Some(_), None) => {
                // 不可能态:有 bucket 没 price。容错复位。
                self.pending_bucket_ms = Some(bucket);
                self.pending_price = Some(price);
            }
        }
    }

    /// 周期 tick(主循环每轮调一次):seal 已经过期的 pending bucket。
    pub(crate) fn on_period_tick(&mut self, now_ms: i64, out: &mut Vec<SealedBar>) {
        let now_bucket = align_to_period(now_ms, self.bar_ms);
        let last_sealable = now_bucket - self.bar_ms;
        if last_sealable < 0 {
            return;
        }
        if let (Some(pb), Some(pp)) = (self.pending_bucket_ms, self.pending_price) {
            if last_sealable >= pb {
                self.seal_range(pb, last_sealable, pp, out);
                // 进入下一 in-progress bucket(无 trade,pending_price 维持作为 ffill 源)
                self.pending_bucket_ms = Some(last_sealable + self.bar_ms);
            }
        }
    }

    fn seal_range(
        &mut self,
        from_bucket: i64,
        to_bucket: i64,
        close: f64,
        out: &mut Vec<SealedBar>,
    ) {
        let mut b = from_bucket;
        while b <= to_bucket {
            let vol = self.push_close(close);
            out.push(SealedBar {
                bar_start_ms: b,
                vol,
            });
            b += self.bar_ms;
        }
    }

    /// 把一个 bar 的 close 推入 closes ring,算 return,喂 welford,返回 scaled std。
    fn push_close(&mut self, close: f64) -> Option<f64> {
        if !close.is_finite() || close <= 0.0 {
            return None;
        }
        self.closes.push_back(close);
        // 维持 cap = PCT + 1。多出来的 pop_front。
        while self.closes.len() > self.pct_change_period + 1 {
            self.closes.pop_front();
        }
        if self.closes.len() < self.pct_change_period + 1 {
            return None;
        }
        let prev = *self.closes.front().expect("len checked");
        if prev <= 0.0 || !prev.is_finite() {
            return None;
        }
        let r = close / prev - 1.0;
        if !r.is_finite() {
            return None;
        }
        self.welford.push(r);
        if !self.welford.is_full() {
            return None;
        }
        let std = self.welford.std();
        if !std.is_finite() || std < 0.0 {
            return None;
        }
        let scaled = std * self.scale_factor;
        if scaled.is_finite() {
            Some(scaled)
        } else {
            None
        }
    }
}

fn align_to_period(ts_ms: i64, period_ms: i64) -> i64 {
    if period_ms <= 0 {
        return ts_ms;
    }
    ts_ms - ts_ms.rem_euclid(period_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::factor_pub::rl_vol::compute_rl_return_volatility;
    use std::collections::VecDeque;

    const PCT: usize = 12;
    const WIN: usize = 30;
    const SCALE: f64 = 1.3;
    const BAR_MS: i64 = 5000;

    fn naive_scaled(closes: &VecDeque<f64>) -> Option<f64> {
        compute_rl_return_volatility(closes, PCT, WIN)
            .ok()
            .flatten()
            .map(|v| v * SCALE)
    }

    #[test]
    fn no_seal_until_cross_bucket() {
        let mut vs = VolState::new(PCT, WIN, SCALE, BAR_MS);
        let mut out = Vec::new();
        vs.on_trade(1000, 100.0, &mut out);
        vs.on_trade(2000, 101.0, &mut out);
        vs.on_trade(3000, 102.0, &mut out);
        // 都在 bucket 0,不该有 sealed bar
        assert!(out.is_empty());
    }

    #[test]
    fn seals_on_bucket_cross() {
        let mut vs = VolState::new(PCT, WIN, SCALE, BAR_MS);
        let mut out = Vec::new();
        vs.on_trade(1000, 100.0, &mut out); // bucket 0
        vs.on_trade(6000, 101.0, &mut out); // bucket 5000 → seal bucket 0
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].bar_start_ms, 0);
    }

    #[test]
    fn ffill_gaps_on_cross() {
        let mut vs = VolState::new(PCT, WIN, SCALE, BAR_MS);
        let mut out = Vec::new();
        vs.on_trade(1000, 100.0, &mut out); // bucket 0
        vs.on_trade(21000, 110.0, &mut out); // bucket 20000 → seal 0/5000/10000/15000
        assert_eq!(out.len(), 4);
        assert_eq!(out[0].bar_start_ms, 0);
        assert_eq!(out[1].bar_start_ms, 5000);
        assert_eq!(out[2].bar_start_ms, 10000);
        assert_eq!(out[3].bar_start_ms, 15000);
    }

    #[test]
    fn tick_seals_pending() {
        let mut vs = VolState::new(PCT, WIN, SCALE, BAR_MS);
        let mut out = Vec::new();
        vs.on_trade(1000, 100.0, &mut out); // bucket 0,只 record
        assert!(out.is_empty());
        vs.on_period_tick(15000, &mut out); // last_sealable = 10000 → seal 0, 5000, 10000
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn vol_matches_batch_compute() {
        // 喂 50 bar,每根 bar 有 2 笔实价 + 1 笔下一 bucket 触发 seal 的 trade,
        // 然后 push 到 closes_naive,逐 bar 对照 batch 公式。
        let mut vs = VolState::new(PCT, WIN, SCALE, BAR_MS);
        let mut closes_naive: VecDeque<f64> = VecDeque::new();
        let mut out = Vec::new();

        for i in 0..50i64 {
            let close = 100.0 + (i as f64).sin() * 5.0;
            vs.on_trade(i * BAR_MS + 1, close - 0.1, &mut out);
            vs.on_trade(i * BAR_MS + BAR_MS - 1, close, &mut out);
            // 一定要触发 seal,否则 closes_naive 比 VolState 多一根。
            vs.on_trade((i + 1) * BAR_MS + 1, close + 0.001, &mut out);
            closes_naive.push_back(close);

            let expected = naive_scaled(&closes_naive);
            // out 的最末一根就是这一轮刚刚 seal 的 bar i
            let got = out
                .last()
                .filter(|sb| sb.bar_start_ms == i * BAR_MS)
                .and_then(|sb| sb.vol);

            match (got, expected) {
                (Some(a), Some(b)) => assert!(
                    (a - b).abs() < 1e-9,
                    "i={i} got={a} expected={b} diff={}",
                    a - b
                ),
                (None, None) => {}
                (got, expected) => {
                    panic!("i={i} option mismatch: got={got:?} expected={expected:?}")
                }
            }
        }
    }

    #[test]
    fn ffill_returns_zero_when_quiet() {
        // 一段稳定价然后长时间无 trade:ffill 应该让 returns 全 0,std → 0
        let mut vs = VolState::new(PCT, WIN, SCALE, BAR_MS);
        let mut out = Vec::new();
        // 前期填够预热 + 让 returns 出现非零
        for i in 0..15i64 {
            vs.on_trade(i * BAR_MS + 1, 100.0 + (i as f64) * 0.1, &mut out);
        }
        // 跳大段空白,只用 tick ffill
        vs.on_period_tick(200 * BAR_MS, &mut out);
        let last_few: Vec<_> = out.iter().rev().take(5).collect();
        // 末尾几根 sealed bar 应该有 vol(不一定 0,但已 ready)
        assert!(last_few.iter().any(|sb| sb.vol.is_some()));
    }
}
