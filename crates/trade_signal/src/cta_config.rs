//! cta 模式规则配置（Redis 热加载）。
//!
//! 语义对齐 research 引擎 `version005_long_short_rust_two_exchange` 的
//! `SignalRule`：每条 rule 对应一个独立的因子信号流（`model_output/<service>`），
//! 各规则之间互不聚合、互不共享状态；组合决策由上层另行处理。
//!
//! Redis key：`{env_dir}:cta_rules`
//! - `env_dir` 取当前工作目录 basename（如 `binance-cta-rx01`），与其它
//!   env 作用域配置一致；exchange 已编码在 env 名里，不再挂 key_suffix。
//!
//! value 为 JSON 数组，每个元素是一条 `CtaRule`。

use anyhow::{bail, Context, Result};
use serde::Deserialize;

use super::model_output_hub::ModelOutputHub;

/// `rule_id` 允许字符集（会进入 from_key / 日志 / 冷却 key）。
const RULE_ID_MAX_LEN: usize = 32;
/// 单条 rule 的 open 档数上限（防御性约束）。
const MAX_OPEN_LEVELS: usize = 8;
/// open_offsets 单项上限（价格分数）。
const MAX_OPEN_OFFSET: f64 = 0.01;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CtaApplication {
    EachBar,
    OnChange,
}

impl CtaApplication {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EachBar => "each_bar",
            Self::OnChange => "on_change",
        }
    }
}

/// cta 规则：一条独立因子信号流。
///
/// 分位比较与引擎一致（严格 `>` / `<`）：`score_quantile > long_quantile`
/// → long vote；`score_quantile < short_quantile` → short vote。
/// spread overlay（同样严格比较）：
/// - long 仅当 `spread_rate < spread_short_quantile` 时允许挂单；
/// - short 仅当 `spread_rate > spread_long_quantile` 时允许挂单；
/// - `spread_rate` 越过 `spread_cancel_quantile` 触发同向撤单。
#[derive(Debug, Clone)]
pub struct CtaRule {
    pub rule_id: String,
    /// 规范化后的 model_output service 名（`model_output/...`）。
    pub model_service: String,
    pub allow_long: bool,
    pub allow_short: bool,
    pub long_quantile: f64,
    pub short_quantile: f64,
    pub spread_long_quantile: f64,
    pub spread_short_quantile: f64,
    pub spread_cancel_quantile: f64,
    /// spread overlay 滚动窗口（样本数）；因子分位由发布侧维护。
    pub rolling_window: usize,
    pub rolling_min_periods: usize,
    /// 信号采样周期（秒），仅作校验/记录；live 由 model_output bar 驱动。
    pub frequency_seconds: i64,
    pub cooldown_seconds: i64,
    pub signal_delay_seconds: i64,
    pub application: CtaApplication,
    /// 单档挂单名义金额（USDT）。引擎 `order_notional_usdt`。
    pub order_notional_usdt: f64,
    /// 各档挂单价格偏移（相对 touch 价的价格分数），如
    /// [0.0, 0.0001, 0.0003, 0.0005]（JSON 小数字面量与科学计数法均可）。
    /// 引擎 `open_offsets`，即"网格参数"：档数 = vec 长度。
    pub open_offsets: Vec<f64>,
    /// 开仓挂单存活时间（秒）。引擎 `maker_ttl_seconds`。
    pub open_ttl_seconds: i64,
    /// 单向名义上限（USDT）。引擎 `max_position_notional_usdt`，
    /// 必须覆盖一整组网格：>= order_notional_usdt * open_offsets.len()。
    pub max_position_notional_usdt: f64,
    /// swap 腿 maker 止盈偏移（价格分数）。0 = 不挂止盈对冲单。
    /// 引擎 `take_profit`。
    pub take_profit: f64,
    /// 止盈/止损比：stop_loss = take_profit / reward_risk_ratio。
    /// 引擎 `reward_risk_ratio`。
    pub reward_risk_ratio: f64,
    /// trailing stop 开关。引擎 `trailing_stop_enabled`。
    pub trailing_stop_enabled: bool,
    /// trailing 触发步进（价格分数）。引擎 `trailing_stop_trigger_step`。
    pub trailing_stop_trigger_step: f64,
    /// trailing 移动步进（价格分数）。引擎 `trailing_stop_move_step`。
    pub trailing_stop_move_step: f64,
    /// 最长持仓秒数；0 = 不限制。引擎 `max_holding_seconds`。
    pub max_holding_seconds: i64,
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCtaRule {
    rule_id: String,
    model_service: String,
    /// long|buy / short|sell / both|long_short|long,short|short,long；缺省 both。
    trade_sides: Option<String>,
    #[serde(default = "default_long_quantile")]
    long_quantile: f64,
    #[serde(default = "default_short_quantile")]
    short_quantile: f64,
    #[serde(default = "default_spread_long_quantile")]
    spread_long_quantile: f64,
    #[serde(default = "default_spread_short_quantile")]
    spread_short_quantile: f64,
    #[serde(default = "default_spread_cancel_quantile")]
    spread_cancel_quantile: f64,
    #[serde(default = "default_rolling_window")]
    rolling_window: usize,
    #[serde(default = "default_rolling_min_periods")]
    rolling_min_periods: usize,
    #[serde(default = "default_frequency_seconds")]
    frequency_seconds: i64,
    #[serde(default)]
    cooldown_seconds: i64,
    #[serde(default = "default_signal_delay_seconds")]
    signal_delay_seconds: i64,
    #[serde(default = "default_application")]
    application: String,
    #[serde(default = "default_order_notional_usdt")]
    order_notional_usdt: f64,
    open_offsets: Option<Vec<f64>>,
    #[serde(default = "default_open_ttl_seconds")]
    open_ttl_seconds: i64,
    #[serde(default = "default_max_position_notional_usdt")]
    max_position_notional_usdt: f64,
    #[serde(default)]
    take_profit: f64,
    #[serde(default = "default_reward_risk_ratio")]
    reward_risk_ratio: f64,
    #[serde(default = "default_trailing_stop_enabled")]
    trailing_stop_enabled: bool,
    #[serde(default = "default_trailing_stop_trigger_step")]
    trailing_stop_trigger_step: f64,
    #[serde(default = "default_trailing_stop_move_step")]
    trailing_stop_move_step: f64,
    #[serde(default = "default_max_holding_seconds")]
    max_holding_seconds: i64,
    #[serde(default = "default_enabled")]
    enabled: bool,
}

fn default_long_quantile() -> f64 {
    0.9
}
fn default_short_quantile() -> f64 {
    0.1
}
fn default_spread_long_quantile() -> f64 {
    0.7
}
fn default_spread_short_quantile() -> f64 {
    0.3
}
fn default_spread_cancel_quantile() -> f64 {
    0.5
}
fn default_rolling_window() -> usize {
    2880
}
fn default_rolling_min_periods() -> usize {
    1440
}
fn default_frequency_seconds() -> i64 {
    60
}
fn default_signal_delay_seconds() -> i64 {
    1
}
fn default_application() -> String {
    "each_bar".to_string()
}
fn default_order_notional_usdt() -> f64 {
    100.0
}
fn default_open_ttl_seconds() -> i64 {
    120
}
fn default_max_position_notional_usdt() -> f64 {
    10_000.0
}
fn default_reward_risk_ratio() -> f64 {
    1.0
}
fn default_trailing_stop_enabled() -> bool {
    true
}
fn default_trailing_stop_trigger_step() -> f64 {
    0.001
}
fn default_trailing_stop_move_step() -> f64 {
    0.0005
}
fn default_max_holding_seconds() -> i64 {
    14_400
}
fn default_enabled() -> bool {
    true
}

fn default_open_offsets() -> Vec<f64> {
    vec![0.0, 0.0001, 0.0003, 0.0005]
}

/// 与引擎 `parse_trade_sides` 一致：返回 (allow_long, allow_short)。
fn parse_trade_sides(raw: Option<&str>) -> Result<(bool, bool)> {
    let Some(value) = raw else {
        return Ok((true, true));
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "long" | "buy" => Ok((true, false)),
        "short" | "sell" => Ok((false, true)),
        "both" | "long_short" | "long,short" | "short,long" => Ok((true, true)),
        other => bail!("cta trade_sides must be long, short, or both, got '{other}'"),
    }
}

impl CtaRule {
    fn from_raw(raw: RawCtaRule, index: usize) -> Result<Self> {
        let rule_id = raw.rule_id.trim().to_string();
        if rule_id.is_empty()
            || rule_id.len() > RULE_ID_MAX_LEN
            || !rule_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            bail!(
                "cta rule[{}] rule_id '{}' invalid: 仅允许 [a-zA-Z0-9_-]，长度 1..={}",
                index,
                raw.rule_id,
                RULE_ID_MAX_LEN
            );
        }
        let model_service = ModelOutputHub::normalize_service_name(&raw.model_service)
            .with_context(|| {
                format!(
                    "cta rule[{}] '{}' model_service '{}' invalid",
                    index, rule_id, raw.model_service
                )
            })?;
        let (allow_long, allow_short) = parse_trade_sides(raw.trade_sides.as_deref())
            .with_context(|| format!("cta rule '{rule_id}'"))?;
        if !allow_long && !allow_short {
            bail!("cta rule '{rule_id}' trade_sides must enable long, short, or both");
        }
        let application = match raw.application.trim().to_ascii_lowercase().as_str() {
            "each_bar" => CtaApplication::EachBar,
            "on_change" => CtaApplication::OnChange,
            other => bail!(
                "cta rule '{rule_id}' application must be each_bar or on_change, got '{other}'"
            ),
        };

        let rule = Self {
            rule_id,
            model_service,
            allow_long,
            allow_short,
            long_quantile: raw.long_quantile,
            short_quantile: raw.short_quantile,
            spread_long_quantile: raw.spread_long_quantile,
            spread_short_quantile: raw.spread_short_quantile,
            spread_cancel_quantile: raw.spread_cancel_quantile,
            rolling_window: raw.rolling_window,
            rolling_min_periods: raw.rolling_min_periods,
            frequency_seconds: raw.frequency_seconds,
            cooldown_seconds: raw.cooldown_seconds,
            signal_delay_seconds: raw.signal_delay_seconds,
            application,
            order_notional_usdt: raw.order_notional_usdt,
            open_offsets: raw.open_offsets.unwrap_or_else(default_open_offsets),
            open_ttl_seconds: raw.open_ttl_seconds,
            max_position_notional_usdt: raw.max_position_notional_usdt,
            take_profit: raw.take_profit,
            reward_risk_ratio: raw.reward_risk_ratio,
            trailing_stop_enabled: raw.trailing_stop_enabled,
            trailing_stop_trigger_step: raw.trailing_stop_trigger_step,
            trailing_stop_move_step: raw.trailing_stop_move_step,
            max_holding_seconds: raw.max_holding_seconds,
            enabled: raw.enabled,
        };
        rule.validate()?;
        Ok(rule)
    }

    fn validate(&self) -> Result<()> {
        if self.frequency_seconds <= 0 || self.rolling_window == 0 {
            bail!(
                "cta rule '{}' frequency_seconds/rolling_window must be positive",
                self.rule_id
            );
        }
        if self.rolling_min_periods == 0 || self.rolling_min_periods > self.rolling_window {
            bail!(
                "cta rule '{}' rolling_min_periods must be in [1, rolling_window]",
                self.rule_id
            );
        }
        for (name, q) in [
            ("long_quantile", self.long_quantile),
            ("short_quantile", self.short_quantile),
            ("spread_long_quantile", self.spread_long_quantile),
            ("spread_short_quantile", self.spread_short_quantile),
            ("spread_cancel_quantile", self.spread_cancel_quantile),
        ] {
            if !q.is_finite() || !(0.0..=1.0).contains(&q) {
                bail!(
                    "cta rule '{}' {name} must be finite in [0,1], got {q}",
                    self.rule_id
                );
            }
        }
        if self.short_quantile >= self.long_quantile {
            bail!(
                "cta rule '{}' short_quantile({}) must be < long_quantile({})",
                self.rule_id,
                self.short_quantile,
                self.long_quantile
            );
        }
        if self.cooldown_seconds < 0 || self.signal_delay_seconds < 0 {
            bail!(
                "cta rule '{}' cooldown/signal_delay cannot be negative",
                self.rule_id
            );
        }
        if !self.order_notional_usdt.is_finite() || self.order_notional_usdt <= 0.0 {
            bail!(
                "cta rule '{}' order_notional_usdt must be positive finite, got {}",
                self.rule_id,
                self.order_notional_usdt
            );
        }
        if self.open_offsets.is_empty() || self.open_offsets.len() > MAX_OPEN_LEVELS {
            bail!(
                "cta rule '{}' open_offsets len must be in [1, {}], got {}",
                self.rule_id,
                MAX_OPEN_LEVELS,
                self.open_offsets.len()
            );
        }
        for offset in &self.open_offsets {
            if !offset.is_finite() || *offset < 0.0 || *offset > MAX_OPEN_OFFSET {
                bail!(
                    "cta rule '{}' open_offsets item must be finite in [0, {}], got {}",
                    self.rule_id,
                    MAX_OPEN_OFFSET,
                    offset
                );
            }
        }
        if self.open_ttl_seconds <= 0 {
            bail!(
                "cta rule '{}' open_ttl_seconds must be positive, got {}",
                self.rule_id,
                self.open_ttl_seconds
            );
        }
        // 引擎 ExecutionConfig 校验：max_position 必须覆盖一整组网格。
        if !self.max_position_notional_usdt.is_finite() || self.max_position_notional_usdt <= 0.0 {
            bail!(
                "cta rule '{}' max_position_notional_usdt must be positive finite, got {}",
                self.rule_id,
                self.max_position_notional_usdt
            );
        }
        let grid_notional = self.order_notional_usdt * self.open_offsets.len() as f64;
        if self.max_position_notional_usdt < grid_notional {
            bail!(
                "cta rule '{}' max_position_notional_usdt({}) must cover one complete grid ({grid_notional})",
                self.rule_id,
                self.max_position_notional_usdt
            );
        }
        if !self.take_profit.is_finite() || self.take_profit < 0.0 {
            bail!(
                "cta rule '{}' take_profit must be finite and >= 0 (0 disables the maker tp hedge), got {}",
                self.rule_id,
                self.take_profit
            );
        }
        if !self.reward_risk_ratio.is_finite() || self.reward_risk_ratio <= 0.0 {
            bail!(
                "cta rule '{}' reward_risk_ratio must be positive finite, got {}",
                self.rule_id,
                self.reward_risk_ratio
            );
        }
        for (name, step) in [
            (
                "trailing_stop_trigger_step",
                self.trailing_stop_trigger_step,
            ),
            ("trailing_stop_move_step", self.trailing_stop_move_step),
        ] {
            if self.trailing_stop_enabled && (!step.is_finite() || step <= 0.0) {
                bail!(
                    "cta rule '{}' {name} must be positive finite when trailing_stop_enabled, got {step}",
                    self.rule_id
                );
            }
        }
        if self.max_holding_seconds < 0 {
            bail!(
                "cta rule '{}' max_holding_seconds cannot be negative (0 disables), got {}",
                self.rule_id,
                self.max_holding_seconds
            );
        }
        Ok(())
    }

    /// 分位信号（严格比较，对齐引擎 gt/lt）：+1 long / -1 short / 0 中性。
    /// 已应用 trade_sides 方向限制。
    pub fn vote(&self, score_quantile: f64) -> i8 {
        if !score_quantile.is_finite() {
            return 0;
        }
        if self.allow_long && score_quantile > self.long_quantile {
            1
        } else if self.allow_short && score_quantile < self.short_quantile {
            -1
        } else {
            0
        }
    }

    /// spread overlay 挂单 gate：`spread_rate` 为 (mid_open - mid_hedge)/mid_open。
    pub fn spread_allows(
        &self,
        direction: i8,
        spread_rate: f64,
        long_thr: f64,
        short_thr: f64,
    ) -> bool {
        if !spread_rate.is_finite() {
            return false;
        }
        match direction {
            1 => short_thr.is_finite() && spread_rate < short_thr,
            -1 => long_thr.is_finite() && spread_rate > long_thr,
            _ => false,
        }
    }

    /// spread overlay 撤单方向：+1 撤 long，-1 撤 short，0 不撤。
    pub fn spread_cancel_direction(&self, spread_rate: f64, cancel_thr: f64) -> i8 {
        if !spread_rate.is_finite() || !cancel_thr.is_finite() {
            0
        } else if spread_rate > cancel_thr {
            1
        } else if spread_rate < cancel_thr {
            -1
        } else {
            0
        }
    }
}

/// 已校验的 cta 规则集（一次 Redis 加载的原子结果）。
#[derive(Debug, Clone, Default)]
pub struct CtaRuleSet {
    rules: Vec<CtaRule>,
}

impl CtaRuleSet {
    pub fn parse(raw: &str) -> Result<Self> {
        let raws: Vec<RawCtaRule> =
            serde_json::from_str(raw).context("cta rules JSON must be an array of rule objects")?;
        let mut rules = Vec::with_capacity(raws.len());
        for (index, raw) in raws.into_iter().enumerate() {
            let rule = CtaRule::from_raw(raw, index)?;
            if rules
                .iter()
                .any(|prev: &CtaRule| prev.rule_id == rule.rule_id)
            {
                bail!("cta rule_id '{}' duplicated", rule.rule_id);
            }
            rules.push(rule);
        }
        Ok(Self { rules })
    }

    pub fn rules(&self) -> &[CtaRule] {
        &self.rules
    }

    /// 去重后的 model_output service 订阅列表（顺序保持稳定）。
    pub fn model_services(&self) -> Vec<String> {
        let mut services: Vec<String> = Vec::new();
        for rule in &self.rules {
            if !services.iter().any(|s| s == &rule.model_service) {
                services.push(rule.model_service.clone());
            }
        }
        services
    }
}

/// `{env_dir}:cta_rules` —— env 作用域的 cta 规则 STRING key（一个 env 只对应
/// 一个 venue 对，exchange 已编码在 env 名里，不再挂 key_suffix）。
pub fn cta_rules_redis_key(env_dir: &str) -> String {
    let env = env_dir.trim().trim_end_matches(':').to_ascii_lowercase();
    format!("{env}:cta_rules")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule_json(rule_id: &str, service: &str) -> String {
        format!(r#"[{{"rule_id":"{rule_id}","model_service":"{service}"}}]"#)
    }

    #[test]
    fn parses_minimal_rule_with_defaults() {
        let set = CtaRuleSet::parse(&rule_json(
            "baseline035",
            "intra-binance-futures-1m-baseline_035",
        ))
        .unwrap();
        let rule = &set.rules()[0];
        assert_eq!(rule.rule_id, "baseline035");
        assert_eq!(
            rule.model_service,
            "model_output/intra-binance-futures-1m-baseline_035"
        );
        assert!(rule.allow_long && rule.allow_short);
        assert_eq!(rule.long_quantile, 0.9);
        assert_eq!(rule.short_quantile, 0.1);
        assert_eq!(rule.spread_long_quantile, 0.7);
        assert_eq!(rule.spread_short_quantile, 0.3);
        assert_eq!(rule.spread_cancel_quantile, 0.5);
        assert_eq!(rule.rolling_window, 2880);
        assert_eq!(rule.rolling_min_periods, 1440);
        assert_eq!(rule.frequency_seconds, 60);
        assert_eq!(rule.signal_delay_seconds, 1);
        assert_eq!(rule.open_offsets, vec![0.0, 0.0001, 0.0003, 0.0005]);
        assert_eq!(rule.open_ttl_seconds, 120);
        assert_eq!(rule.max_position_notional_usdt, 10_000.0);
        assert_eq!(rule.take_profit, 0.0);
        assert_eq!(rule.reward_risk_ratio, 1.0);
        assert!(rule.trailing_stop_enabled);
        assert_eq!(rule.trailing_stop_trigger_step, 0.001);
        assert_eq!(rule.trailing_stop_move_step, 0.0005);
        assert_eq!(rule.max_holding_seconds, 14_400);
        assert!(rule.enabled);
    }

    #[test]
    fn parses_full_rule() {
        let raw = r#"[{
            "rule_id": "tp_vpi_006",
            "model_service": "model_output/intra-binance-futures-1m-TP_VPI_006",
            "trade_sides": "short",
            "long_quantile": 0.95,
            "short_quantile": 0.05,
            "spread_long_quantile": 0.8,
            "spread_short_quantile": 0.2,
            "spread_cancel_quantile": 0.55,
            "rolling_window": 1440,
            "rolling_min_periods": 720,
            "frequency_seconds": 60,
            "cooldown_seconds": 30,
            "signal_delay_seconds": 0,
            "application": "on_change",
            "order_notional_usdt": 250.0,
            "open_offsets": [0.0, 0.0002],
            "open_ttl_seconds": 60,
            "max_position_notional_usdt": 5000.0,
            "take_profit": 0.005,
            "reward_risk_ratio": 2.0,
            "trailing_stop_enabled": true,
            "trailing_stop_trigger_step": 0.002,
            "trailing_stop_move_step": 0.001,
            "max_holding_seconds": 7200,
            "enabled": false
        }]"#;
        let set = CtaRuleSet::parse(raw).unwrap();
        let rule = &set.rules()[0];
        assert!(!rule.allow_long && rule.allow_short);
        assert_eq!(rule.application, CtaApplication::OnChange);
        assert_eq!(rule.open_offsets, vec![0.0, 0.0002]);
        assert_eq!(rule.max_position_notional_usdt, 5000.0);
        assert_eq!(rule.take_profit, 0.005);
        assert_eq!(rule.reward_risk_ratio, 2.0);
        assert_eq!(rule.trailing_stop_trigger_step, 0.002);
        assert_eq!(rule.trailing_stop_move_step, 0.001);
        assert_eq!(rule.max_holding_seconds, 7200);
        assert!(!rule.enabled);
    }

    #[test]
    fn open_offsets_accepts_decimal_and_scientific_notation() {
        // 运维在 Redis 里写小数（0.0001）或科学计数法（1e-4）都必须解析一致。
        let raw = r#"[{"rule_id":"r","model_service":"svc","open_offsets":[0.0,0.0001,0.0003,0.0005],"max_position_notional_usdt":10000.0}]"#;
        let dec = CtaRuleSet::parse(raw).unwrap();
        let raw = r#"[{"rule_id":"r","model_service":"svc","open_offsets":[0.0,1e-4,3e-4,5e-4],"max_position_notional_usdt":10000.0}]"#;
        let sci = CtaRuleSet::parse(raw).unwrap();
        assert_eq!(dec.rules()[0].open_offsets, sci.rules()[0].open_offsets);
        assert_eq!(dec.rules()[0].open_offsets[1], 0.0001);
    }

    #[test]
    fn rejects_grid_notional_above_max_position() {
        // 4 档 × 100u = 400 > 300
        let raw = r#"[{"rule_id":"r","model_service":"svc","order_notional_usdt":100.0,"open_offsets":[0.0,0.0001,0.0003,0.0005],"max_position_notional_usdt":300.0}]"#;
        assert!(CtaRuleSet::parse(raw).is_err());
        // 缩档数后通过
        let raw = r#"[{"rule_id":"r","model_service":"svc","order_notional_usdt":100.0,"open_offsets":[0.0,0.0001],"max_position_notional_usdt":300.0}]"#;
        assert!(CtaRuleSet::parse(raw).is_ok());
    }

    #[test]
    fn rejects_invalid_exit_params() {
        let raw = r#"[{"rule_id":"r","model_service":"svc","take_profit":-0.01}]"#;
        assert!(CtaRuleSet::parse(raw).is_err());
        let raw = r#"[{"rule_id":"r","model_service":"svc","reward_risk_ratio":0.0}]"#;
        assert!(CtaRuleSet::parse(raw).is_err());
        let raw = r#"[{"rule_id":"r","model_service":"svc","trailing_stop_trigger_step":0.0}]"#;
        assert!(CtaRuleSet::parse(raw).is_err());
        let raw = r#"[{"rule_id":"r","model_service":"svc","max_holding_seconds":-1}]"#;
        assert!(CtaRuleSet::parse(raw).is_err());
        // trailing 关闭时允许非法步进值（不参与）
        let raw = r#"[{"rule_id":"r","model_service":"svc","trailing_stop_enabled":false,"trailing_stop_trigger_step":0.0}]"#;
        assert!(CtaRuleSet::parse(raw).is_ok());
    }

    #[test]
    fn rejects_duplicate_rule_id() {
        let raw = r#"[
            {"rule_id":"a","model_service":"svc1"},
            {"rule_id":"a","model_service":"svc2"}
        ]"#;
        assert!(CtaRuleSet::parse(raw).is_err());
    }

    #[test]
    fn rejects_invalid_rule_id_charset() {
        assert!(CtaRuleSet::parse(&rule_json("bad|id", "svc")).is_err());
        assert!(CtaRuleSet::parse(&rule_json("", "svc")).is_err());
        assert!(CtaRuleSet::parse(&rule_json("has space", "svc")).is_err());
    }

    #[test]
    fn rejects_invalid_quantiles_and_sides() {
        let raw =
            r#"[{"rule_id":"r","model_service":"svc","long_quantile":0.4,"short_quantile":0.6}]"#;
        assert!(CtaRuleSet::parse(raw).is_err());
        let raw = r#"[{"rule_id":"r","model_service":"svc","trade_sides":"none"}]"#;
        assert!(CtaRuleSet::parse(raw).is_err());
    }

    #[test]
    fn rejects_disabled_service_name() {
        assert!(CtaRuleSet::parse(&rule_json("r", "-")).is_err());
        assert!(CtaRuleSet::parse(&rule_json("r", "")).is_err());
    }

    #[test]
    fn rejects_unknown_field() {
        let raw = r#"[{"rule_id":"r","model_service":"svc","bogus":1}]"#;
        assert!(CtaRuleSet::parse(raw).is_err());
    }

    #[test]
    fn vote_respects_quantiles_and_sides() {
        let set = CtaRuleSet::parse(&rule_json("r", "svc")).unwrap();
        let rule = &set.rules()[0];
        assert_eq!(rule.vote(0.95), 1);
        assert_eq!(rule.vote(0.05), -1);
        assert_eq!(rule.vote(0.9), 0);
        assert_eq!(rule.vote(0.1), 0);
        assert_eq!(rule.vote(f64::NAN), 0);

        let short_only =
            CtaRuleSet::parse(r#"[{"rule_id":"r","model_service":"svc","trade_sides":"short"}]"#)
                .unwrap();
        let rule = &short_only.rules()[0];
        assert_eq!(rule.vote(0.95), 0);
        assert_eq!(rule.vote(0.05), -1);
    }

    #[test]
    fn spread_overlay_gates_match_engine_semantics() {
        let set = CtaRuleSet::parse(&rule_json("r", "svc")).unwrap();
        let rule = &set.rules()[0];
        // long 需 spread < short 分位；short 需 spread > long 分位
        assert!(rule.spread_allows(1, -0.002, 0.7, -0.001));
        assert!(!rule.spread_allows(1, 0.0, 0.7, -0.001));
        assert!(rule.spread_allows(-1, 0.003, 0.002, -0.001));
        assert!(!rule.spread_allows(-1, 0.0, 0.002, -0.001));
        assert!(!rule.spread_allows(1, f64::NAN, 0.7, -0.001));
        // cancel 方向（引擎 cancel_direction 为持续方向：value>thr → 撤 long，
        // value<thr → 撤 short，恰好相等才不撤）
        assert_eq!(rule.spread_cancel_direction(0.001, 0.0005), 1);
        assert_eq!(rule.spread_cancel_direction(-0.001, 0.0005), -1);
        assert_eq!(rule.spread_cancel_direction(0.0004, 0.0005), -1);
        assert_eq!(rule.spread_cancel_direction(0.0005, 0.0005), 0);
        assert_eq!(rule.spread_cancel_direction(f64::NAN, 0.0005), 0);
    }

    #[test]
    fn model_services_dedup_keeps_order() {
        let raw = r#"[
            {"rule_id":"a","model_service":"svc1"},
            {"rule_id":"b","model_service":"model_output/svc2"},
            {"rule_id":"c","model_service":"svc1"}
        ]"#;
        let set = CtaRuleSet::parse(raw).unwrap();
        assert_eq!(
            set.model_services(),
            vec![
                "model_output/svc1".to_string(),
                "model_output/svc2".to_string()
            ]
        );
    }

    #[test]
    fn redis_key_is_env_scoped() {
        assert_eq!(
            cta_rules_redis_key("binance-cta-rx01"),
            "binance-cta-rx01:cta_rules"
        );
        assert_eq!(
            cta_rules_redis_key("binance_cta_v005"),
            "binance_cta_v005:cta_rules"
        );
    }

    #[test]
    fn empty_array_parses_to_empty_set() {
        let set = CtaRuleSet::parse("[]").unwrap();
        assert!(set.rules().is_empty());
        assert!(set.model_services().is_empty());
    }
}
