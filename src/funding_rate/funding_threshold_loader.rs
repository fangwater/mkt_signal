use std::collections::HashMap;

/// Per-symbol funding 阈值集合。每个因子有 (forward_open, backward_open) 两个分位数阈值。
/// 通过 `factor_chain_to_funding_mapping` 从因子链生成 dest_field 形如
/// `<factor>__<direction>` 的 mapping,resolve 后再聚合到这里。
#[derive(Debug, Clone, Default)]
pub struct FundingThresholdsResolved {
    /// factor name → (forward_open, backward_open) 阈值
    pub factors: HashMap<String, FactorDirectionalThresholds>,
}

#[derive(Debug, Clone, Copy)]
pub struct FactorDirectionalThresholds {
    pub forward_open: f64,
    pub backward_open: f64,
}

impl FundingThresholdsResolved {
    pub fn factor(&self, factor: &str) -> Option<FactorDirectionalThresholds> {
        self.factors.get(factor).copied()
    }

    pub fn is_empty(&self) -> bool {
        self.factors.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factor_lookup_returns_pair() {
        let mut resolved = FundingThresholdsResolved::default();
        resolved.factors.insert(
            "hedge_premium_rate".to_string(),
            FactorDirectionalThresholds {
                forward_open: 0.01,
                backward_open: -0.02,
            },
        );
        let got = resolved
            .factor("hedge_premium_rate")
            .expect("factor present");
        assert!((got.forward_open - 0.01).abs() < 1e-12);
        assert!((got.backward_open + 0.02).abs() < 1e-12);
        assert!(resolved.factor("missing").is_none());
    }
}
