//! UniMMR 算法平仓状态机（trade_signal 端，仅 fr / intra / cross arb 使用）
//!
//! 与 pre_trade 共享同一组阈值 `unimmr_trigger_line / unimmr_recover_line`
//! （Redis `pre_trade_risk_params` hash），并独立订阅 account_pubs 的
//! [`BasicAccountEventType::AccountRisk`] 消息，按 [`BasicAccountScope`] 维度
//! 维护一个 **带迟滞** 的状态机：
//!
//! | 当前 | 收到 mr | 转移 |
//! |---|---|---|
//! | * | `mr > recover` | -> [`UnimmrCloseState::Normal`] |
//! | * | `mr < trigger` | -> [`UnimmrCloseState::CloseAllowed`] |
//! | * | `trigger <= mr <= recover` | 保持原状（迟滞区） |
//!
//! 阈值 hot reload 不直接改变 state，只在下一条 AccountRisk 到来时生效；与
//! pre_trade 端 `PreTradeParamsLoader::normalize_unimmr_control_lines` 同一
//! 验证口径（`1.5 < trigger < recover`，否则回退到 `(2.0, 2.2)`）。
//!
//! 本次只实现"听 + 维护状态"，**不**接 close 触发；下游通过
//! [`UnimmrCloseGate::is_close_allowed`] 查询。

use anyhow::{Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::cell::RefCell;
use std::collections::HashMap;
use std::time::Duration;

use crate::common::basic_account_msg::{
    split_basic_account_event, BasicAccountEventType, BasicAccountRiskMsg, BasicAccountScope,
};
use crate::common::exchange::Exchange;
use crate::common::ipc_service_name::build_service_name;
use crate::common::redis_client::{RedisClient, RedisSettings};
use crate::portfolio_margin::pm_forwarder::{
    PM_HISTORY_SIZE, PM_MAX_BYTES, PM_MAX_SUBSCRIBERS, PM_SUBSCRIBER_MAX_BUFFER_SIZE,
};

const REDIS_KEY_RISK_PARAMS: &str = "pre_trade_risk_params";
const REFRESH_INTERVAL_SECS: u64 = 60;

/// 默认 trigger / recover，与 `PreTradeParamsLoader` 默认值保持一致。
pub const DEFAULT_TRIGGER_LINE: f64 = 2.0;
pub const DEFAULT_RECOVER_LINE: f64 = 2.2;
/// 交易所 warning 模式上沿；任何配置必须严格大于该值才合法。
pub const EXCHANGE_WARNING_MODE_UPPER_UNIMMR: f64 = 1.5;

/// UniMMR 算法平仓状态。默认 `Normal`（启动时保守不允许触发）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UnimmrCloseState {
    /// 风险充足，不允许触发算法平仓。
    #[default]
    Normal,
    /// 已跌破 trigger 线，允许触发算法平仓，直到 mr 恢复到 recover 线以上。
    CloseAllowed,
}

impl UnimmrCloseState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::CloseAllowed => "close_allowed",
        }
    }
}

struct UnimmrCloseGateInner {
    trigger_line: f64,
    recover_line: f64,
    states: HashMap<BasicAccountScope, UnimmrCloseState>,
    snapshots: HashMap<BasicAccountScope, BasicAccountRiskMsg>,
}

impl Default for UnimmrCloseGateInner {
    fn default() -> Self {
        Self {
            trigger_line: DEFAULT_TRIGGER_LINE,
            recover_line: DEFAULT_RECOVER_LINE,
            states: HashMap::new(),
            snapshots: HashMap::new(),
        }
    }
}

thread_local! {
    static GATE: RefCell<UnimmrCloseGateInner> = RefCell::new(UnimmrCloseGateInner::default());
}

/// `UnimmrCloseGate` 单例访问器（零大小类型）
pub struct UnimmrCloseGate;

impl UnimmrCloseGate {
    pub fn instance() -> Self {
        UnimmrCloseGate
    }

    /// 写入合法的阈值（调用方负责确保 `1.5 < trigger < recover`，否则使用默认）。
    /// 见 [`normalize_unimmr_control_lines`]。
    pub fn set_thresholds(&self, trigger_line: f64, recover_line: f64) {
        let (trigger_line, recover_line) =
            normalize_unimmr_control_lines(trigger_line, recover_line)
                .unwrap_or((DEFAULT_TRIGGER_LINE, DEFAULT_RECOVER_LINE));
        GATE.with(|gate| {
            let mut gate = gate.borrow_mut();
            gate.trigger_line = trigger_line;
            gate.recover_line = recover_line;
        });
    }

    /// 当前 (trigger, recover)。
    pub fn thresholds(&self) -> (f64, f64) {
        GATE.with(|gate| {
            let gate = gate.borrow();
            (gate.trigger_line, gate.recover_line)
        })
    }

    /// 喂入一份新的账户风险快照：更新该 scope 的状态（带迟滞）并缓存原始 msg。
    pub fn apply_account_risk(&self, scope: BasicAccountScope, msg: BasicAccountRiskMsg) {
        GATE.with(|gate| {
            let mut gate = gate.borrow_mut();
            let prev = gate.states.get(&scope).copied().unwrap_or_default();
            let next = next_state(prev, msg.margin_ratio, gate.trigger_line, gate.recover_line);
            if next != prev {
                info!(
                    "UnimmrCloseGate 状态切换 scope={} {:?} -> {:?} margin_ratio={:.6} trigger={:.3} recover={:.3}",
                    scope.as_str(),
                    prev,
                    next,
                    msg.margin_ratio,
                    gate.trigger_line,
                    gate.recover_line
                );
            }
            gate.states.insert(scope, next);
            gate.snapshots.insert(scope, msg);
        });
    }

    /// 当前 scope 的状态（未观测到时返回默认 [`UnimmrCloseState::Normal`]）。
    pub fn state(&self, scope: BasicAccountScope) -> UnimmrCloseState {
        GATE.with(|gate| {
            gate.borrow()
                .states
                .get(&scope)
                .copied()
                .unwrap_or_default()
        })
    }

    /// 便捷查询：是否允许触发算法平仓。
    pub fn is_close_allowed(&self, scope: BasicAccountScope) -> bool {
        matches!(self.state(scope), UnimmrCloseState::CloseAllowed)
    }

    /// 最近一份原始 [`BasicAccountRiskMsg`]（None 表示还未观测）。
    pub fn snapshot(&self, scope: BasicAccountScope) -> Option<BasicAccountRiskMsg> {
        GATE.with(|gate| gate.borrow().snapshots.get(&scope).cloned())
    }
}

/// 状态转移：迟滞区间 `[trigger, recover]` 保持原状；外部则按区间硬切换。
/// `mr` 非有限或 `trigger >= recover` 时维持原状（保守）。
fn next_state(
    prev: UnimmrCloseState,
    margin_ratio: f64,
    trigger_line: f64,
    recover_line: f64,
) -> UnimmrCloseState {
    if !margin_ratio.is_finite() || !(trigger_line < recover_line) {
        return prev;
    }
    if margin_ratio > recover_line {
        UnimmrCloseState::Normal
    } else if margin_ratio < trigger_line {
        UnimmrCloseState::CloseAllowed
    } else {
        prev
    }
}

/// 与 pre_trade 端 `normalize_unimmr_control_lines` 同口径。
fn normalize_unimmr_control_lines(trigger_line: f64, recover_line: f64) -> Option<(f64, f64)> {
    if trigger_line.is_finite()
        && recover_line.is_finite()
        && trigger_line > EXCHANGE_WARNING_MODE_UPPER_UNIMMR
        && recover_line > trigger_line
    {
        Some((trigger_line, recover_line))
    } else {
        None
    }
}

fn risk_params_full_key(env_prefix: Option<&str>) -> String {
    match env_prefix
        .map(str::trim)
        .map(|p| p.trim_end_matches(':'))
        .filter(|p| !p.is_empty())
    {
        Some(prefix) => format!("{prefix}:{REDIS_KEY_RISK_PARAMS}"),
        None => REDIS_KEY_RISK_PARAMS.to_string(),
    }
}

/// 从 Redis 读取一次 trigger/recover 并写入 gate。失败或不合法 → 用默认值。
pub async fn load_thresholds_from_redis(
    redis: &RedisSettings,
    env_prefix: Option<&str>,
) -> Result<(f64, f64)> {
    let key = risk_params_full_key(env_prefix);
    let mut settings = redis.clone();
    settings.prefix = None;
    let mut client = RedisClient::connect(settings)
        .await
        .context("connect redis for unimmr control lines")?;
    let hash = client.hgetall_map(&key).await?;
    let parse_f64 = |k: &str| -> Option<f64> { hash.get(k).and_then(|v| v.parse::<f64>().ok()) };
    let raw_trigger = parse_f64("unimmr_trigger_line").unwrap_or(DEFAULT_TRIGGER_LINE);
    let raw_recover = parse_f64("unimmr_recover_line").unwrap_or(DEFAULT_RECOVER_LINE);
    let (trigger, recover) = normalize_unimmr_control_lines(raw_trigger, raw_recover)
        .unwrap_or_else(|| {
            warn!(
                "unimmr control lines invalid trigger={} recover={}（要求 {:.2} < trigger < recover），回退默认 trigger={:.2} recover={:.2}",
                raw_trigger,
                raw_recover,
                EXCHANGE_WARNING_MODE_UPPER_UNIMMR,
                DEFAULT_TRIGGER_LINE,
                DEFAULT_RECOVER_LINE
            );
            (DEFAULT_TRIGGER_LINE, DEFAULT_RECOVER_LINE)
        });
    UnimmrCloseGate::instance().set_thresholds(trigger, recover);
    info!(
        "UnimmrCloseGate 阈值已加载 key='{}' trigger={:.3} recover={:.3}",
        key, trigger, recover
    );
    Ok((trigger, recover))
}

/// 启动 60s 周期刷新阈值（与 `PreTradeParamsLoader::start_background_refresh` 同款节奏）。
/// 启动处的同步加载应该已经做过一次，这里跳过 interval 的首个立即触发。
pub fn start_threshold_refresh(redis: RedisSettings, env_prefix: Option<String>) {
    tokio::task::spawn_local(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(REFRESH_INTERVAL_SECS));
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(err) = load_thresholds_from_redis(&redis, env_prefix.as_deref()).await {
                warn!(
                    "UnimmrCloseGate 阈值后台刷新失败 prefix={:?}: {:#}",
                    env_prefix, err
                );
            }
        }
    });
    info!(
        "UnimmrCloseGate 阈值后台刷新任务已启动 (间隔: {}s)",
        REFRESH_INTERVAL_SECS
    );
}

/// 启动 `account_pubs/<exchange>_pm` 的 iceoryx2 订阅，只解析
/// [`BasicAccountEventType::AccountRisk`] 并喂给 gate。
///
/// 与 pre_trade `monitor_channel::spawn_basic_listener` 共用同款 service/payload
/// 配置（`PM_MAX_BYTES` / `PM_MAX_SUBSCRIBERS` / `PM_HISTORY_SIZE` / `PM_SUBSCRIBER_MAX_BUFFER_SIZE`），
/// 以满足 publisher 端的 max_subscribers 上限。
pub fn spawn_account_risk_listener(exchange: Exchange) {
    let service_name = build_service_name(&format!("account_pubs/{}_pm", exchange.as_str()));
    let node_name = format!("trade_signal_unimmr_gate_{}_pm", exchange.as_str());

    tokio::task::spawn_local(async move {
        let result: Result<()> = async move {
            let node = NodeBuilder::new()
                .name(&NodeName::new(&node_name)?)
                .create::<ipc::Service>()?;
            let service_name_obj = ServiceName::new(&service_name)?;
            let service_builder = || {
                node.service_builder(&service_name_obj)
                    .publish_subscribe::<[u8; PM_MAX_BYTES]>()
                    .max_publishers(1)
                    .max_subscribers(PM_MAX_SUBSCRIBERS)
                    .history_size(PM_HISTORY_SIZE)
                    .subscriber_max_buffer_size(PM_SUBSCRIBER_MAX_BUFFER_SIZE)
            };
            // trade_signal 是被动观察方，不强依赖 account_monitor 先起：用 open_or_create
            // 避免阻塞启动流。
            let service = match service_builder().open() {
                Ok(service) => service,
                Err(err) => {
                    warn!(
                        "UnimmrCloseGate account_monitor service 暂未就绪，open_or_create 兜底: service={} err={:?}",
                        service_name, err
                    );
                    service_builder().open_or_create()?
                }
            };
            let subscriber: Subscriber<ipc::Service, [u8; PM_MAX_BYTES], ()> =
                service.subscriber_builder().create()?;
            info!(
                "UnimmrCloseGate account_pubs 订阅成功 service={} exchange={:?}",
                service_name, exchange
            );

            loop {
                match subscriber.receive() {
                    Ok(Some(sample)) => {
                        let payload = sample.payload();
                        let Some((msg_type, scope, data)) = split_basic_account_event(payload)
                        else {
                            continue;
                        };
                        if !matches!(msg_type, BasicAccountEventType::AccountRisk) {
                            continue;
                        }
                        match BasicAccountRiskMsg::from_bytes(data) {
                            Ok(msg) => {
                                UnimmrCloseGate::instance().apply_account_risk(scope, msg);
                            }
                            Err(err) => warn!(
                                "UnimmrCloseGate AccountRisk decode failed scope={} err={:#}",
                                scope.as_str(),
                                err
                            ),
                        }
                    }
                    Ok(None) => tokio::task::yield_now().await,
                    Err(err) => {
                        warn!(
                            "UnimmrCloseGate account_pubs receive error service={}: {err}",
                            service_name
                        );
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
            #[allow(unreachable_code)]
            Ok(())
        }
        .await;

        if let Err(err) = result {
            warn!("UnimmrCloseGate 订阅任务退出 (exchange={:?}): {err:?}", exchange);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reset() {
        GATE.with(|g| {
            *g.borrow_mut() = UnimmrCloseGateInner::default();
        });
    }

    fn risk_msg(margin_ratio: f64) -> BasicAccountRiskMsg {
        BasicAccountRiskMsg {
            msg_type: BasicAccountEventType::AccountRisk,
            timestamp: 1_700_000_000_000,
            adj_equity_usd: 0.0,
            actual_equity_usd: 0.0,
            maintenance_margin_usd: 0.0,
            initial_margin_usd: 0.0,
            margin_ratio,
            borrowed_usd: 0.0,
            notional_usd: 0.0,
        }
    }

    #[test]
    fn normalize_rejects_under_warning_floor_and_inverted() {
        assert_eq!(normalize_unimmr_control_lines(2.0, 2.2), Some((2.0, 2.2)));
        assert_eq!(normalize_unimmr_control_lines(1.5, 2.2), None);
        assert_eq!(normalize_unimmr_control_lines(2.2, 2.0), None);
        assert_eq!(normalize_unimmr_control_lines(f64::NAN, 2.0), None);
    }

    #[test]
    fn next_state_falls_into_close_allowed_below_trigger() {
        let st = next_state(UnimmrCloseState::Normal, 1.99, 2.0, 2.2);
        assert_eq!(st, UnimmrCloseState::CloseAllowed);
    }

    #[test]
    fn next_state_recovers_to_normal_above_recover() {
        let st = next_state(UnimmrCloseState::CloseAllowed, 2.21, 2.0, 2.2);
        assert_eq!(st, UnimmrCloseState::Normal);
    }

    #[test]
    fn next_state_keeps_prev_in_hysteresis_band() {
        assert_eq!(
            next_state(UnimmrCloseState::CloseAllowed, 2.1, 2.0, 2.2),
            UnimmrCloseState::CloseAllowed
        );
        assert_eq!(
            next_state(UnimmrCloseState::Normal, 2.1, 2.0, 2.2),
            UnimmrCloseState::Normal
        );
        // 边界条件：等于 trigger 或 recover 都属于迟滞区间，保持原状。
        assert_eq!(
            next_state(UnimmrCloseState::Normal, 2.0, 2.0, 2.2),
            UnimmrCloseState::Normal
        );
        assert_eq!(
            next_state(UnimmrCloseState::CloseAllowed, 2.2, 2.0, 2.2),
            UnimmrCloseState::CloseAllowed
        );
    }

    #[test]
    fn next_state_keeps_prev_on_garbage_input() {
        assert_eq!(
            next_state(UnimmrCloseState::Normal, f64::NAN, 2.0, 2.2),
            UnimmrCloseState::Normal
        );
        // trigger >= recover：保守保持原状
        assert_eq!(
            next_state(UnimmrCloseState::CloseAllowed, 1.0, 2.2, 2.0),
            UnimmrCloseState::CloseAllowed
        );
    }

    #[test]
    fn apply_account_risk_updates_state_and_snapshot_per_scope() {
        reset();
        let gate = UnimmrCloseGate::instance();
        gate.set_thresholds(2.0, 2.2);

        gate.apply_account_risk(BasicAccountScope::BinanceUnified, risk_msg(1.8));
        assert_eq!(
            gate.state(BasicAccountScope::BinanceUnified),
            UnimmrCloseState::CloseAllowed
        );
        assert!(gate.is_close_allowed(BasicAccountScope::BinanceUnified));

        gate.apply_account_risk(BasicAccountScope::OkexUnified, risk_msg(3.0));
        assert_eq!(
            gate.state(BasicAccountScope::OkexUnified),
            UnimmrCloseState::Normal
        );
        // Binance 的 scope 不受 Okex 喂入影响。
        assert!(gate.is_close_allowed(BasicAccountScope::BinanceUnified));

        // 回到迟滞区间：保持 CloseAllowed
        gate.apply_account_risk(BasicAccountScope::BinanceUnified, risk_msg(2.1));
        assert!(gate.is_close_allowed(BasicAccountScope::BinanceUnified));

        // 跨过 recover 才回到 Normal
        gate.apply_account_risk(BasicAccountScope::BinanceUnified, risk_msg(2.25));
        assert_eq!(
            gate.state(BasicAccountScope::BinanceUnified),
            UnimmrCloseState::Normal
        );

        let snap = gate.snapshot(BasicAccountScope::BinanceUnified).unwrap();
        assert!((snap.margin_ratio - 2.25).abs() < 1e-12);
    }

    #[test]
    fn risk_params_full_key_with_and_without_prefix() {
        assert_eq!(risk_params_full_key(None), "pre_trade_risk_params");
        assert_eq!(
            risk_params_full_key(Some("binance_fr_arb01")),
            "binance_fr_arb01:pre_trade_risk_params"
        );
        assert_eq!(
            risk_params_full_key(Some("env:")),
            "env:pre_trade_risk_params"
        );
        // 空 prefix 等价 None
        assert_eq!(risk_params_full_key(Some("")), "pre_trade_risk_params");
    }

    #[test]
    fn unknown_scope_defaults_to_normal_and_not_close_allowed() {
        reset();
        let gate = UnimmrCloseGate::instance();
        assert_eq!(
            gate.state(BasicAccountScope::BinanceUnified),
            UnimmrCloseState::Normal
        );
        assert!(!gate.is_close_allowed(BasicAccountScope::BinanceUnified));
    }
}
