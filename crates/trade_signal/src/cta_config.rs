//! cta 模式信号配置（Redis 热加载）。
//!
//! 语义对齐 research 引擎 `version005_long_short_rust_two_exchange` 的
//! `SignalRule`：一条独立因子信号流（`model_output/<service>`）。
//!
//! Redis key：`{env_dir}:cta_rules`
//! - `env_dir` 取当前工作目录 basename（如 `binance-cta-rx01`），与其它
//!   env 作用域配置一致；exchange 已编码在 env 名里，不再挂 key_suffix。
//! - value 为单个 JSON 对象（信号配置）；解析器接受单元素数组，但一个环境最多
//!   配置一条独立规则（`rule_id` 可省略，缺省 "default"）；空数组停用规则。
//!
//! 执行/网格参数（`open_offsets` 档位、单笔名义、TP、trailing、持仓上限等）
//! 不放在该对象里，而是 `{env}:cta_strategy_params:{open}:{hedge}` hash 的字段；
//! 加载时 `CtaExecOverrides` 从 hash 解析并覆盖到规则上（对象内同名字段仅作
//! 兼容回退）。

use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use order_common::TradingVenue;
use serde::Deserialize;
use serde_json::Value;

use super::model_output_hub::ModelOutputHub;

/// `rule_id` 允许字符集（会进入 from_key / 日志 / 冷却 key）。
const RULE_ID_MAX_LEN: usize = 32;
/// 单条 rule 的 open 档数上限（防御性约束）。
const MAX_OPEN_LEVELS: usize = 8;
/// open_offsets 单项上限（价格分数）。
const MAX_OPEN_OFFSET: f64 = 0.01;
const SELECTED_MODEL_PREFIX: &str = "model_output/intra-binance-futures-1m-";

#[derive(Debug, Clone, Copy)]
struct SelectedRuleContract {
    take_profit: f64,
    reward_risk_ratio: f64,
    trailing_trigger: f64,
    trailing_move: f64,
}

fn selected_rule_contract(model_service: &str) -> Result<Option<SelectedRuleContract>> {
    let Some(factor) = model_service.strip_prefix(SELECTED_MODEL_PREFIX) else {
        return Ok(None);
    };
    let values = match factor {
        "baseline_035" | "td_pr_011" => (0.005, 1.0, 0.001, 0.0005),
        "baseline_053" | "factor_116" => (0.01, 1.0, 0.002, 0.001),
        "tp_vpi_006" => (0.01, 2.0, 0.002, 0.001),
        "td_pr_005" => (0.01, 1.0, 0.001, 0.0005),
        "net_buy_medium" => (0.01, 1.0, 0.002, 0.0005),
        "factor_004" | "baseline_091" => (0.01, 1.0, 0.001, 0.0005),
        _ => bail!(
            "unsupported selected CTA factor service '{}'; deployment contract contains exactly nine factors",
            model_service
        ),
    };
    Ok(Some(SelectedRuleContract {
        take_profit: values.0,
        reward_risk_ratio: values.1,
        trailing_trigger: values.2,
        trailing_move: values.3,
    }))
}

fn same_param(actual: f64, expected: f64) -> bool {
    (actual - expected).abs() <= 1e-12
}

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
/// raw 因子与发布端随消息给出的线性分位阈值比较（严格 `>` / `<`）。窗口、
/// 最小样本数、bar 周期和分位值均由 publisher 负责，不在执行侧重复配置。
/// spread overlay 不在规则内配置：per-symbol 价差阈值由
/// `cta_spread_thresholds_config_{open}_{hedge}` mapping + rolling_metrics
/// 发布值解析（`reload_spread_thresholds_from_rolling` → `SpreadFactor`），
/// 与 intra/cross 同一机制。
#[derive(Debug, Clone)]
pub struct CtaRule {
    pub rule_id: String,
    /// 规范化后的 model_output service 名（`model_output/...`）。
    pub model_service: String,
    pub allow_long: bool,
    pub allow_short: bool,
    pub nq_change_enabled: bool,
    /// 同一 symbol 两次开仓的最小间隔（秒）；0 = 不限制。
    pub cooldown_seconds: i64,
    pub application: CtaApplication,
    /// 单档挂单名义金额（USDT）。引擎 `order_notional_usdt`。
    pub order_notional_usdt: f64,
    /// 各档挂单价格偏移（相对 touch 价的价格分数），如
    /// [0.0, 0.0001, 0.0003, 0.0005]（JSON 小数字面量与科学计数法均可）。
    /// 引擎 `open_offsets`，即"网格参数"：档数 = vec 长度。
    pub open_offsets: Vec<f64>,
    /// 开仓挂单存活时间（秒）。引擎 `maker_ttl_seconds`。
    pub open_ttl_seconds: i64,
    /// swap 腿 maker 止盈偏移（价格分数），必须为正。
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
    /// 单对象信号配置下可省略（缺省 "default"）；数组格式中建议显式给出。
    rule_id: Option<String>,
    model_service: String,
    /// long|buy / short|sell / both|long_short|long,short|short,long；缺省 both。
    trade_sides: Option<String>,
    #[serde(default = "default_enabled")]
    nq_change_enabled: bool,
    #[serde(default)]
    cooldown_seconds: i64,
    #[serde(default = "default_application")]
    application: String,
    #[serde(default = "default_order_notional_usdt")]
    order_notional_usdt: f64,
    open_offsets: Option<Vec<f64>>,
    #[serde(default = "default_open_ttl_seconds")]
    open_ttl_seconds: i64,
    #[serde(default = "default_take_profit")]
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

fn default_application() -> String {
    "each_bar".to_string()
}
fn default_order_notional_usdt() -> f64 {
    100.0
}
fn default_open_ttl_seconds() -> i64 {
    120
}
fn default_take_profit() -> f64 {
    0.005
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

/// `{env}:cta_strategy_params:{open}:{hedge}` hash 里的执行/网格字段名
/// （覆盖到 `CtaRule` 执行段；与 RawCtaRule 同名字段一一对应）。
const CTA_EXEC_FLOAT_FIELDS: &[&str] = &[
    "order_notional_usdt",
    "take_profit",
    "reward_risk_ratio",
    "trailing_stop_trigger_step",
    "trailing_stop_move_step",
];
const CTA_EXEC_INT_FIELDS: &[&str] = &["open_ttl_seconds", "max_holding_seconds"];
const CTA_EXEC_BOOL_FIELDS: &[&str] = &["trailing_stop_enabled"];
const CTA_EXEC_OFFSETS_FIELD: &str = "open_offsets";

/// 从 `{env}:cta_strategy_params:{open}:{hedge}` hash（String->String）解析出的执行参数覆盖。
/// 全部为 Option：hash 缺字段时规则回退到对象内字段 / serde 默认值。
#[derive(Debug, Clone, Default)]
pub struct CtaExecOverrides {
    order_notional_usdt: Option<f64>,
    open_offsets: Option<Vec<f64>>,
    open_ttl_seconds: Option<i64>,
    take_profit: Option<f64>,
    reward_risk_ratio: Option<f64>,
    trailing_stop_enabled: Option<bool>,
    trailing_stop_trigger_step: Option<f64>,
    trailing_stop_move_step: Option<f64>,
    max_holding_seconds: Option<i64>,
}

fn parse_exec_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// `open_offsets` 在 hash 里存 JSON 数组字符串（`"[0.0, 0.0001]"`），
/// 也兼容逗号/空白分隔的裸列表（`"0, 0.0001, 0.0003"`）。
fn parse_exec_offsets(raw: &str) -> Option<Vec<f64>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(v) = serde_json::from_str::<Vec<f64>>(trimmed) {
        return Some(v);
    }
    let parsed: Option<Vec<f64>> = trimmed
        .split(|c: char| c == ',' || c.is_whitespace())
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<f64>().ok())
        .collect();
    parsed.filter(|v| !v.is_empty())
}

impl CtaExecOverrides {
    /// 从 env-scoped CTA strategy params hash 解析执行参数覆盖。
    /// 单个字段解析失败只 warn 跳过（沿用对象内字段/serde 默认），不整轮失败。
    pub fn from_strategy_params(params: &HashMap<String, String>, key_ctx: &str) -> Self {
        let mut out = Self::default();
        for (field, raw) in params {
            let value = raw.trim();
            match field.as_str() {
                name if CTA_EXEC_FLOAT_FIELDS.contains(&name) => match value.parse::<f64>() {
                    Ok(v) => match name {
                        "order_notional_usdt" => out.order_notional_usdt = Some(v),
                        "take_profit" => out.take_profit = Some(v),
                        "reward_risk_ratio" => out.reward_risk_ratio = Some(v),
                        "trailing_stop_trigger_step" => out.trailing_stop_trigger_step = Some(v),
                        "trailing_stop_move_step" => out.trailing_stop_move_step = Some(v),
                        _ => {}
                    },
                    Err(_) => log::warn!(
                        "cta exec param '{}' 在 '{}' 中不是数字: '{}'",
                        name,
                        key_ctx,
                        value
                    ),
                },
                name if CTA_EXEC_INT_FIELDS.contains(&name) => match value.parse::<i64>() {
                    Ok(v) => match name {
                        "open_ttl_seconds" => out.open_ttl_seconds = Some(v),
                        "max_holding_seconds" => out.max_holding_seconds = Some(v),
                        _ => {}
                    },
                    Err(_) => log::warn!(
                        "cta exec param '{}' 在 '{}' 中不是整数: '{}'",
                        name,
                        key_ctx,
                        value
                    ),
                },
                name if CTA_EXEC_BOOL_FIELDS.contains(&name) => match parse_exec_bool(value) {
                    Some(v) => out.trailing_stop_enabled = Some(v),
                    None => log::warn!(
                        "cta exec param '{}' 在 '{}' 中不是布尔值: '{}'",
                        name,
                        key_ctx,
                        value
                    ),
                },
                name if name == CTA_EXEC_OFFSETS_FIELD => match parse_exec_offsets(value) {
                    Some(v) => out.open_offsets = Some(v),
                    None => log::warn!(
                        "cta exec param 'open_offsets' 在 '{}' 中不是合法数组: '{}'",
                        key_ctx,
                        value
                    ),
                },
                _ => {}
            }
        }
        out
    }
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
    fn from_raw(raw: RawCtaRule, index: usize, exec: Option<&CtaExecOverrides>) -> Result<Self> {
        let rule_id = raw
            .rule_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("default")
            .to_string();
        if rule_id.len() > RULE_ID_MAX_LEN
            || !rule_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            bail!(
                "cta rule[{}] rule_id '{}' invalid: 仅允许 [a-zA-Z0-9_-]，长度 1..={}",
                index,
                rule_id,
                RULE_ID_MAX_LEN
            );
        }
        let model_service = ModelOutputHub::normalize_service_name(&raw.model_service)
            .with_context(|| {
                format!(
                    "cta rule[{}] '{}' model_service '{}' invalid",
                    index, rule_id, raw.model_service
                )
            })?
            .to_ascii_lowercase();
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
            nq_change_enabled: raw.nq_change_enabled,
            cooldown_seconds: raw.cooldown_seconds,
            application,
            // 执行/网格参数：strategy hash 覆盖 > 对象内字段 > serde 默认。
            order_notional_usdt: exec
                .and_then(|e| e.order_notional_usdt)
                .unwrap_or(raw.order_notional_usdt),
            open_offsets: exec
                .and_then(|e| e.open_offsets.clone())
                .or(raw.open_offsets)
                .unwrap_or_else(default_open_offsets),
            open_ttl_seconds: exec
                .and_then(|e| e.open_ttl_seconds)
                .unwrap_or(raw.open_ttl_seconds),
            take_profit: exec.and_then(|e| e.take_profit).unwrap_or(raw.take_profit),
            reward_risk_ratio: exec
                .and_then(|e| e.reward_risk_ratio)
                .unwrap_or(raw.reward_risk_ratio),
            trailing_stop_enabled: exec
                .and_then(|e| e.trailing_stop_enabled)
                .unwrap_or(raw.trailing_stop_enabled),
            trailing_stop_trigger_step: exec
                .and_then(|e| e.trailing_stop_trigger_step)
                .unwrap_or(raw.trailing_stop_trigger_step),
            trailing_stop_move_step: exec
                .and_then(|e| e.trailing_stop_move_step)
                .unwrap_or(raw.trailing_stop_move_step),
            max_holding_seconds: exec
                .and_then(|e| e.max_holding_seconds)
                .unwrap_or(raw.max_holding_seconds),
            enabled: raw.enabled,
        };
        rule.validate()?;
        Ok(rule)
    }

    fn validate(&self) -> Result<()> {
        if self.cooldown_seconds < 0 {
            bail!(
                "cta rule '{}' cooldown_seconds cannot be negative",
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
        if !self.take_profit.is_finite()
            || self.take_profit <= 0.0
            || self.take_profit >= 1.0
            || self.take_profit / self.reward_risk_ratio >= 1.0
        {
            bail!(
                "cta rule '{}' take_profit must be in (0,1) and take_profit/reward_risk_ratio < 1, got tp={} rr={}",
                self.rule_id,
                self.take_profit,
                self.reward_risk_ratio
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
        if self.trailing_stop_enabled
            && self.trailing_stop_move_step >= self.trailing_stop_trigger_step
        {
            bail!(
                "cta rule '{}' trailing_stop_move_step({}) must be < trailing_stop_trigger_step({})",
                self.rule_id,
                self.trailing_stop_move_step,
                self.trailing_stop_trigger_step
            );
        }
        if self.max_holding_seconds < 0 {
            bail!(
                "cta rule '{}' max_holding_seconds cannot be negative (0 disables), got {}",
                self.rule_id,
                self.max_holding_seconds
            );
        }
        if let Some(contract) = selected_rule_contract(&self.model_service)? {
            let offsets_match = self.open_offsets.len() == 4
                && self
                    .open_offsets
                    .iter()
                    .zip([0.0, 0.0001, 0.0003, 0.0005])
                    .all(|(actual, expected)| same_param(*actual, expected));
            if !self.allow_long
                || !self.allow_short
                || self.application != CtaApplication::EachBar
                || self.cooldown_seconds != 0
                || !self.nq_change_enabled
                || !same_param(self.order_notional_usdt, 100.0)
                || !offsets_match
                || self.open_ttl_seconds != 120
                || !same_param(self.take_profit, contract.take_profit)
                || !same_param(self.reward_risk_ratio, contract.reward_risk_ratio)
                || !self.trailing_stop_enabled
                || !same_param(self.trailing_stop_trigger_step, contract.trailing_trigger)
                || !same_param(self.trailing_stop_move_step, contract.trailing_move)
                || self.max_holding_seconds != 14_400
            {
                bail!(
                    "cta rule '{}' parameters do not match selected backtest contract for '{}'",
                    self.rule_id,
                    self.model_service
                );
            }
        }
        Ok(())
    }

    /// raw 因子阈值信号（严格比较，对齐引擎 gt/lt）：+1 long / -1 short / 0 中性。
    /// NQ 过滤与 trade_sides 方向限制已在此应用。
    pub fn vote(
        &self,
        score: f64,
        long_threshold: f64,
        short_threshold: f64,
        filter_long_value: Option<f64>,
        filter_long_threshold: Option<f64>,
        filter_short_value: Option<f64>,
        filter_short_threshold: Option<f64>,
    ) -> i8 {
        if !score.is_finite() || !long_threshold.is_finite() || !short_threshold.is_finite() {
            return 0;
        }
        let long_filter = !self.nq_change_enabled
            || matches!((filter_long_value, filter_long_threshold), (Some(value), Some(threshold)) if value >= threshold);
        let short_filter = !self.nq_change_enabled
            || matches!((filter_short_value, filter_short_threshold), (Some(value), Some(threshold)) if value <= threshold);
        if self.allow_long && score > long_threshold && long_filter {
            1
        } else if self.allow_short && score < short_threshold && short_filter {
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
    /// 兼容两种存储格式：单个规则对象（当前 UI 写入的信号配置）或规则数组。
    /// 执行/网格参数见 `parse_with_exec`。
    pub fn parse(raw: &str) -> Result<Self> {
        Self::parse_with_exec(raw, None)
    }

    /// 解析 `cta_rules` 内容并把 env-scoped strategy params hash 的执行参数
    /// 覆盖到每条规则（hash 字段 > 对象内字段 > serde 默认）。
    pub fn parse_with_exec(raw: &str, exec: Option<&CtaExecOverrides>) -> Result<Self> {
        let doc: Value = serde_json::from_str(raw).context("cta rules JSON invalid")?;
        let raws: Vec<RawCtaRule> = match doc {
            Value::Object(_) => vec![serde_json::from_value::<RawCtaRule>(doc)
                .context("cta rules 单对象必须是合法 rule 字段")?],
            Value::Array(_) => serde_json::from_value::<Vec<RawCtaRule>>(doc)
                .context("cta rules JSON must be an array of rule objects")?,
            _ => bail!("cta rules JSON must be an object or an array of rule objects"),
        };
        if raws.len() > 1 {
            bail!(
                "one CTA environment may contain at most one independent rule, got {}",
                raws.len()
            );
        }
        let mut rules = Vec::with_capacity(raws.len());
        for (index, raw) in raws.into_iter().enumerate() {
            let rule = CtaRule::from_raw(raw, index, exec)?;
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

/// `{env_dir}:cta_rules` —— env 作用域的 cta 信号配置 STRING key（一个 env 只对应
/// 一个 venue 对，exchange 已编码在 env 名里，不再挂 key_suffix）。
pub fn cta_rules_redis_key(env_dir: &str) -> String {
    let env = env_dir.trim().trim_end_matches(':').to_ascii_lowercase();
    format!("{env}:cta_rules")
}

/// `{env}:cta_strategy_params:{open}:{hedge}` —— cta 执行/网格参数 hash key。
/// CTA 规则必须独立部署，因此执行参数也必须按环境隔离。
pub fn cta_strategy_params_redis_key(
    env_dir: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> String {
    let env = env_dir.trim().trim_end_matches(':').to_ascii_lowercase();
    format!(
        "{env}:cta_strategy_params:{}:{}",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    )
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
        assert!(rule.nq_change_enabled);
        assert_eq!(rule.cooldown_seconds, 0);
        assert_eq!(rule.open_offsets, vec![0.0, 0.0001, 0.0003, 0.0005]);
        assert_eq!(rule.open_ttl_seconds, 120);
        assert_eq!(rule.take_profit, 0.005);
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
            "model_service": "model_output/test-service",
            "trade_sides": "short",
            "cooldown_seconds": 30,
            "application": "on_change",
            "order_notional_usdt": 250.0,
            "open_offsets": [0.0, 0.0002],
            "open_ttl_seconds": 60,
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
        let raw =
            r#"[{"rule_id":"r","model_service":"svc","open_offsets":[0.0,0.0001,0.0003,0.0005]}]"#;
        let dec = CtaRuleSet::parse(raw).unwrap();
        let raw = r#"[{"rule_id":"r","model_service":"svc","open_offsets":[0.0,1e-4,3e-4,5e-4]}]"#;
        let sci = CtaRuleSet::parse(raw).unwrap();
        assert_eq!(dec.rules()[0].open_offsets, sci.rules()[0].open_offsets);
        assert_eq!(dec.rules()[0].open_offsets[1], 0.0001);
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
        let raw = r#"[{"rule_id":"r","model_service":"svc","trailing_stop_trigger_step":0.001,"trailing_stop_move_step":0.001}]"#;
        assert!(CtaRuleSet::parse(raw).is_err());
        // trailing 关闭时允许非法步进值（不参与）
        let raw = r#"[{"rule_id":"r","model_service":"svc","trailing_stop_enabled":false,"trailing_stop_trigger_step":0.0}]"#;
        assert!(CtaRuleSet::parse(raw).is_ok());
    }

    #[test]
    fn selected_factor_rejects_backtest_parameter_drift() {
        let service = "intra-binance-futures-1m-baseline_053";
        assert!(CtaRuleSet::parse(&rule_json("r", service)).is_err());
        let raw = format!(
            r#"{{"rule_id":"r","model_service":"{service}","take_profit":0.01,"trailing_stop_trigger_step":0.002,"trailing_stop_move_step":0.001}}"#
        );
        assert!(CtaRuleSet::parse(&raw).is_ok());
        let drifted = format!(
            r#"{{"rule_id":"r","model_service":"{service}","take_profit":0.01,"trailing_stop_trigger_step":0.002,"trailing_stop_move_step":0.0005}}"#
        );
        assert!(CtaRuleSet::parse(&drifted).is_err());
    }

    #[test]
    fn all_selected_factor_exit_contracts_are_accepted() {
        for (factor, tp, rr, trigger, move_step) in [
            ("baseline_035", 0.005, 1.0, 0.001, 0.0005),
            ("td_pr_011", 0.005, 1.0, 0.001, 0.0005),
            ("baseline_053", 0.01, 1.0, 0.002, 0.001),
            ("tp_vpi_006", 0.01, 2.0, 0.002, 0.001),
            ("td_pr_005", 0.01, 1.0, 0.001, 0.0005),
            ("factor_116", 0.01, 1.0, 0.002, 0.001),
            ("net_buy_medium", 0.01, 1.0, 0.002, 0.0005),
            ("factor_004", 0.01, 1.0, 0.001, 0.0005),
            ("baseline_091", 0.01, 1.0, 0.001, 0.0005),
        ] {
            let raw = format!(
                r#"{{"rule_id":"r","model_service":"intra-binance-futures-1m-{factor}","take_profit":{tp},"reward_risk_ratio":{rr},"trailing_stop_trigger_step":{trigger},"trailing_stop_move_step":{move_step}}}"#
            );
            assert!(CtaRuleSet::parse(&raw).is_ok(), "factor={factor}");
        }
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
        assert!(CtaRuleSet::parse(&rule_json("has space", "svc")).is_err());
    }

    #[test]
    fn parses_single_object_without_rule_id() {
        // 新存储格式：`{env}:cta_rules` 为单个信号配置对象，rule_id 缺省 "default"。
        let raw = r#"{"model_service":"svc","trade_sides":"long"}"#;
        let set = CtaRuleSet::parse(raw).unwrap();
        assert_eq!(set.rules().len(), 1);
        let rule = &set.rules()[0];
        assert_eq!(rule.rule_id, "default");
        assert_eq!(rule.model_service, "model_output/svc");
        assert!(rule.allow_long && !rule.allow_short);
    }

    #[test]
    fn exec_overrides_from_strategy_params_win() {
        // 执行/网格参数以 cta_strategy_params hash 为准，覆盖对象内字段。
        let raw = r#"{"model_service":"svc","order_notional_usdt":50.0,"open_offsets":[0.0],"open_ttl_seconds":30}"#;
        let mut params = HashMap::new();
        params.insert("order_notional_usdt".to_string(), "250".to_string());
        params.insert("open_offsets".to_string(), "[0.0, 0.0002]".to_string());
        params.insert("open_ttl_seconds".to_string(), "60".to_string());
        params.insert("take_profit".to_string(), "0.005".to_string());
        params.insert("trailing_stop_enabled".to_string(), "false".to_string());
        let exec = CtaExecOverrides::from_strategy_params(&params, "test");
        let set = CtaRuleSet::parse_with_exec(raw, Some(&exec)).unwrap();
        let rule = &set.rules()[0];
        assert_eq!(rule.order_notional_usdt, 250.0);
        assert_eq!(rule.open_offsets, vec![0.0, 0.0002]);
        assert_eq!(rule.open_ttl_seconds, 60);
        assert_eq!(rule.take_profit, 0.005);
        assert!(!rule.trailing_stop_enabled);
    }

    #[test]
    fn exec_overrides_tolerate_bad_and_csv_offsets() {
        // 坏值只 warn 跳过，对象内字段兜底；open_offsets 兼容裸 CSV。
        let raw = r#"{"model_service":"svc","order_notional_usdt":50.0,"open_ttl_seconds":30}"#;
        let mut params = HashMap::new();
        params.insert("order_notional_usdt".to_string(), "not_a_num".to_string());
        params.insert("open_offsets".to_string(), "0, 0.0001, 0.0003".to_string());
        let exec = CtaExecOverrides::from_strategy_params(&params, "test");
        let set = CtaRuleSet::parse_with_exec(raw, Some(&exec)).unwrap();
        let rule = &set.rules()[0];
        assert_eq!(rule.order_notional_usdt, 50.0); // 坏值被忽略，对象内字段兜底
        assert_eq!(rule.open_offsets, vec![0.0, 0.0001, 0.0003]);
        assert_eq!(rule.open_ttl_seconds, 30);
    }

    #[test]
    fn rejects_invalid_sides() {
        let raw = r#"[{"rule_id":"r","model_service":"svc","trade_sides":"none"}]"#;
        assert!(CtaRuleSet::parse(raw).is_err());
    }

    #[test]
    fn rejects_removed_fields() {
        for field in [
            r#""long_quantile":0.9"#,
            r#""short_quantile":0.1"#,
            r#""frequency_seconds":60"#,
            r#""rolling_window":2880"#,
            r#""rolling_min_samples":1440"#,
            r#""signal_delay_seconds":1"#,
            r#""max_signal_age_seconds":120"#,
            r#""max_position_notional_usdt":10000"#,
        ] {
            let raw = format!(r#"{{"model_service":"svc",{field}}}"#);
            assert!(CtaRuleSet::parse(&raw).is_err(), "field={field}");
        }
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
        assert_eq!(
            rule.vote(2.0, 1.5, -1.5, Some(0.2), Some(0.1), None, None),
            1
        );
        assert_eq!(
            rule.vote(-2.0, 1.5, -1.5, None, None, Some(-0.2), Some(-0.1)),
            -1
        );
        assert_eq!(
            rule.vote(1.5, 1.5, -1.5, Some(0.2), Some(0.1), None, None),
            0
        );
        assert_eq!(rule.vote(f64::NAN, 1.5, -1.5, None, None, None, None), 0);
        assert_eq!(
            rule.vote(2.0, 1.5, -1.5, Some(0.0), Some(0.1), None, None),
            0
        );

        let short_only =
            CtaRuleSet::parse(r#"[{"rule_id":"r","model_service":"svc","trade_sides":"short"}]"#)
                .unwrap();
        let rule = &short_only.rules()[0];
        assert_eq!(
            rule.vote(2.0, 1.5, -1.5, Some(0.2), Some(0.1), None, None),
            0
        );
        assert_eq!(
            rule.vote(-2.0, 1.5, -1.5, None, None, Some(-0.2), Some(-0.1)),
            -1
        );
    }

    #[test]
    fn spread_overlay_gates_match_engine_semantics() {
        let set = CtaRuleSet::parse(&rule_json("r", "svc")).unwrap();
        let rule = &set.rules()[0];
        // 阈值入参为 resolved per-symbol 值（rolling_metrics + mapping 解析后
        // 由 SpreadFactor 供给）：long 需 spread < short_thr；short 需 spread > long_thr
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
    fn model_services_returns_the_single_normalized_service() {
        let raw = r#"[{"rule_id":"a","model_service":"SVC1"}]"#;
        let set = CtaRuleSet::parse(raw).unwrap();
        assert_eq!(set.model_services(), vec!["model_output/svc1".to_string()]);
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
        assert_eq!(
            cta_strategy_params_redis_key(
                "binance-cta-rx01",
                TradingVenue::BinanceMargin,
                TradingVenue::BinanceFutures,
            ),
            "binance-cta-rx01:cta_strategy_params:binance-margin:binance-futures"
        );
    }

    #[test]
    fn empty_array_parses_to_empty_set() {
        let set = CtaRuleSet::parse("[]").unwrap();
        assert!(set.rules().is_empty());
        assert!(set.model_services().is_empty());
    }
}
