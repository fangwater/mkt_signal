use crate::binance_fix::{
    is_binance_spot_fix_trade_request, spot_fix_enabled_from_env, BinanceSpotFixConfig,
    BinanceSpotFixHandle, BINANCE_SPOT_FIX_ENABLED_ENV,
};
use crate::bitget_query_rate_limiter::BitgetQueryRateLimiter;
use crate::config::WsConstants;
use crate::dispatcher::Dispatcher;
use crate::exec_backend::ExecBackend;
use crate::internal_terminate::{
    InternalOpenTerminateMsg, INTERNAL_OPEN_TERMINATED_ERROR_CODE, INTERNAL_OPEN_TERMINATE_TTL_US,
    ORDER_TERMINATE_PAYLOAD_LEN,
};
use crate::okex::OkexNewOrderParams;
use crate::okex_query_rate_limiter::OkexQueryRateLimiter;
use crate::query_parsers::binance_margin_order::parse_binance_margin_order_query_json;
use crate::query_parsers::binance_pm_balance_snapshot::parse_binance_pm_balance_snapshot;
use crate::query_parsers::binance_spot_account_snapshot_std::parse_binance_spot_account_snapshot_std;
use crate::query_parsers::binance_um_account_snapshot::parse_binance_um_account_snapshot;
use crate::query_parsers::binance_um_balance_snapshot_std::parse_binance_um_balance_snapshot_std;
use crate::query_parsers::binance_um_order::parse_binance_um_order_query_json;
use crate::query_parsers::bitget_account_balance_snapshot::parse_bitget_account_balance_snapshot;
use crate::query_parsers::bitget_order::{
    parse_bitget_order_query_json, BitgetOrderQueryParseErrorKind, BitgetOrderQueryParseResult,
};
use crate::query_parsers::bitget_positions_snapshot::parse_bitget_positions_snapshot;
use crate::query_parsers::bybit_account_balance_snapshot::parse_bybit_account_balance_snapshot;
use crate::query_parsers::bybit_order::{
    parse_bybit_order_query_json, BybitOrderQueryParseErrorKind, BybitOrderQueryParseResult,
};
use crate::query_parsers::bybit_positions_snapshot::parse_bybit_positions_snapshot_pages;
use crate::query_parsers::compact_order::ORDER_QUERY_NOT_FOUND_MARKER;
use crate::query_parsers::gate_positions_snapshot::parse_gate_positions_snapshot_with_meta;
use crate::query_parsers::gate_unified_balance_snapshot::parse_gate_unified_balance_snapshot;
use crate::query_parsers::okex_account_balance_snapshot::parse_okex_account_balance_snapshot;
use crate::query_parsers::okex_order::{
    parse_okex_order_query_json, OkexOrderQueryParseErrorKind, OkexOrderQueryParseResult,
};
use crate::query_parsers::okex_positions_snapshot::parse_okex_positions_snapshot;
use crate::query_request::{QueryRequestMsg, QueryRequestType};
use crate::query_response_handle::QueryExecOutcome;
use crate::query_type_mapping::QueryTypeMapping;
use crate::response_sink::{QueryResponseSink, TradeResponseSink};
use crate::trade_request::{
    BinanceCancelOrderParams, BinanceNewOrderParams, BinanceNewOrderParamsRef,
    BitgetNewOrderParams, GateNewOrderParams, TradeRequestIpcPayload, TradeRequestMsg,
    TradeRequestType,
};
use crate::trade_response_handle::TradeExecOutcome;
use crate::trade_type_mapping::TradeTypeMapping;
use crate::ws_client::{
    BinanceUmWsEndpointRouteEvalStats, BinanceUmWsHealthConfig as WsBinanceUmWsHealthConfig,
    BinanceUmWsHealthRuntime, RespLatencyBuckets, TradeWsClient, WsCommand, WsCommandQueue,
    WsEndpointHandle, WsLatencyBuckets,
};
use account_common::ApiKey;
use account_common::{binance_account_mode, BinanceAccountMode};
use anyhow::{anyhow, Context, Result};
use iceoryx2::port::{publisher::Publisher, subscriber::Subscriber};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use ipc_common::iceoryx_publisher::{QUERY_REQ_PAYLOAD, QUERY_RESP_PAYLOAD};
use log::{debug, info, warn};
use order_common::{BINANCE_UM_NEW_ACK_TRACE_PAYLOAD_LEN, BINANCE_UM_NEW_ACK_TRACE_SERVICE};
use rolling_common::latency_kll::LatencyKll;
use rolling_common::latency_snapshot::LATENCY_SNAPSHOT_PAYLOAD_LEN;
use rtrb::{Consumer, PopError, Producer, PushError, RingBuffer};
use runtime_common::affinity::pin_to_core;
use runtime_common::exchange::Exchange;
use runtime_common::fast_hash::{fast_hash_map, FastHashMap};
use runtime_common::ipc_service_name::build_service_name;
use runtime_common::mkt_cfg::{
    binance_um_ip_whitelist_mode_enabled, BinanceUmWsHealthConfig, BinanceUmWsRouteConfig,
    BinanceUmWsRouteKind,
};
use runtime_common::redis_client::{RedisClient, RedisSettings};
use runtime_common::time_util::get_timestamp_us;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::net::IpAddr;
use std::rc::Rc;
use std::thread;
use std::time::{Duration, Instant};
use std::{cell::RefCell, rc::Rc as StdRc};
use tokio_util::sync::CancellationToken;

const TRADE_REQ_IPC_RECV_SLOW_WARN_US: i64 = 50_000;
const DEFAULT_TE_IPC_REQ_QUEUE_CAP: usize = 4096;
const SPSC_QUEUE_FULL_WARN_INTERVAL: u64 = 100_000;
const IPC_THREAD_DRAIN_BUDGET: usize = 64;
const DEFAULT_TE_ROUTER_IDLE_SPIN_ITERS: usize = 1024;
const INTERNAL_OPEN_TERMINATE_SUMMARY_INTERVAL_SECS: u64 = 60;
const INTERNAL_OPEN_TERMINATE_SUMMARY_MAX_GROUPS: usize = 32;
const BINANCE_UM_BASIC_WS_CONNECTIONS: usize = 4;
const BINANCE_UM_BASIC_WS_RECONNECT_PERIOD_MS: u64 = 300_000;

#[derive(Clone, Debug)]
struct WsEndpointGroup {
    handles: Vec<WsEndpointHandle>,
    next_handle_idx: usize,
    fallback: bool,
    group_id: usize,
    source: &'static str,
    url: String,
    remote_ip: Option<IpAddr>,
    local_ips: Vec<IpAddr>,
}

impl WsEndpointGroup {
    fn new(
        handles: Vec<WsEndpointHandle>,
        fallback: bool,
        group_id: usize,
        source: &'static str,
        url: String,
        remote_ip: Option<IpAddr>,
        local_ips: Vec<IpAddr>,
    ) -> Self {
        Self {
            handles,
            next_handle_idx: 0,
            fallback,
            group_id,
            source,
            url,
            remote_ip,
            local_ips,
        }
    }

    fn is_fallback(&self) -> bool {
        self.fallback
    }

    fn is_available(&self) -> bool {
        self.handles.iter().any(|handle| handle.is_available())
    }

    fn is_available_for_new_binance_um(
        &self,
        new_block_threshold_us: Option<i64>,
        cancel_block_threshold_us: Option<i64>,
        pause_ms: u64,
    ) -> bool {
        self.handles.iter().any(|handle| {
            handle.is_available_for_new_binance_um(
                new_block_threshold_us,
                cancel_block_threshold_us,
                pause_ms,
            )
        })
    }

    fn recent_binance_um_new_ack_rtt_sum_count(&self, n: usize) -> (i64, usize) {
        self.handles
            .iter()
            .map(|handle| handle.recent_binance_um_new_ack_rtt_sum_count(n))
            .fold((0, 0), |(sum_acc, count_acc), (sum, count)| {
                (sum_acc.saturating_add(sum), count_acc.saturating_add(count))
            })
    }

    fn binance_um_route_eval_stats(
        &self,
        now_us: i64,
        half_life_ms: u64,
        window_ms: u64,
    ) -> BinanceUmWsEndpointRouteEvalStats {
        let mut out = BinanceUmWsEndpointRouteEvalStats::default();
        let mut weighted_score_sum = 0.0;
        for stats in self.handles.iter().map(|handle| {
            handle.binance_um_new_ack_route_eval_stats(now_us, half_life_ms, window_ms)
        }) {
            out.sample_n = out.sample_n.saturating_add(stats.sample_n);
            out.effective_n += stats.effective_n;
            weighted_score_sum += (stats.score_us as f64) * stats.effective_n;
            if let Some(age_ms) = stats.last_sample_age_ms {
                let replace_latest = out
                    .last_sample_age_ms
                    .map(|current_age_ms| age_ms < current_age_ms)
                    .unwrap_or(true);
                if replace_latest {
                    out.latest_us = stats.latest_us;
                    out.last_sample_age_ms = Some(age_ms);
                }
            }
        }
        if out.effective_n > 0.0 {
            out.score_us = (weighted_score_sum / out.effective_n).round() as i64;
        }
        out
    }

    fn binance_um_health_stats(&self, select_recent: usize) -> (i64, usize, i64) {
        let mut sum_us = 0i64;
        let mut count = 0usize;
        let mut max_pause_ms = 0i64;
        for handle in &self.handles {
            let (mean_us, recent_count, pause_ms_left) =
                handle.binance_um_health_stats(select_recent);
            sum_us = sum_us.saturating_add(mean_us.saturating_mul(recent_count as i64));
            count = count.saturating_add(recent_count);
            max_pause_ms = max_pause_ms.max(pause_ms_left);
        }
        let mean_us = if count > 0 { sum_us / count as i64 } else { 0 };
        (mean_us, count, max_pause_ms)
    }

    fn enqueue_available(&mut self, cmd: WsCommand) -> Option<usize> {
        if self.handles.is_empty() {
            return None;
        }
        let start = self.next_handle_idx % self.handles.len();
        self.next_handle_idx = (self.next_handle_idx + 1) % self.handles.len();
        for offset in 0..self.handles.len() {
            let idx = (start + offset) % self.handles.len();
            if self.handles[idx].is_available() {
                self.handles[idx].enqueue_available(cmd);
                return Some(idx);
            }
        }
        None
    }
}

fn binance_um_basic_ws_local_ips(local_ips: &[IpAddr]) -> Result<Vec<IpAddr>> {
    match local_ips.len() {
        1 => Ok(vec![local_ips[0]; BINANCE_UM_BASIC_WS_CONNECTIONS]),
        2 => Ok(vec![local_ips[0], local_ips[1], local_ips[0], local_ips[1]]),
        4 => Ok(local_ips.to_vec()),
        n => Err(anyhow!(
            "Binance UM basic WS requires 1, 2, or 4 local IPs for {} connections; got {}",
            BINANCE_UM_BASIC_WS_CONNECTIONS,
            n
        )),
    }
}

#[cfg(test)]
mod route_selection_tests {
    use super::{
        binance_um_basic_ws_local_ips, binance_um_route_eval_redis_key_for_env,
        build_binance_um_route_eval_snapshot, select_binance_um_ws_route,
        select_binance_um_ws_route_with_fallback, select_binance_um_ws_rr_route,
        BinanceUmWsRouteCandidate, BinanceUmWsRouteMode, WsEndpointGroup,
    };
    use crate::ws_client::{
        BinanceUmWsHealthConfig, BinanceUmWsHealthRuntime, WsCommand, WsCommandQueue,
        WsEndpointHandle, WsEndpointState,
    };
    use runtime_common::mkt_cfg::{BinanceUmWsRouteConfig, BinanceUmWsRouteKind};
    use std::cell::RefCell;
    use std::rc::Rc;

    fn candidate(
        um_available: bool,
        new_ack_rtt_sum_us: i64,
        new_ack_rtt_count: usize,
    ) -> BinanceUmWsRouteCandidate {
        BinanceUmWsRouteCandidate {
            base_available: um_available,
            um_available,
            new_ack_rtt_sum_us,
            new_ack_rtt_count,
        }
    }

    fn connected_handle() -> WsEndpointHandle {
        let handle = WsEndpointHandle::new(
            WsCommandQueue::new(),
            Rc::new(RefCell::new(WsEndpointState::default())),
        );
        handle.mark_connected();
        handle
    }

    #[test]
    fn binance_um_basic_ws_local_ip_layout_is_fixed_width() {
        let ip0 = "172.31.0.10".parse().unwrap();
        let ip1 = "172.31.0.11".parse().unwrap();
        let ip2 = "172.31.0.12".parse().unwrap();
        let ip3 = "172.31.0.13".parse().unwrap();

        assert_eq!(
            binance_um_basic_ws_local_ips(&[ip0]).unwrap(),
            vec![ip0, ip0, ip0, ip0]
        );
        assert_eq!(
            binance_um_basic_ws_local_ips(&[ip0, ip1]).unwrap(),
            vec![ip0, ip1, ip0, ip1]
        );
        assert!(binance_um_basic_ws_local_ips(&[ip0, ip1, ip2]).is_err());
        assert_eq!(
            binance_um_basic_ws_local_ips(&[ip0, ip1, ip2, ip3]).unwrap(),
            vec![ip0, ip1, ip2, ip3]
        );
    }

    #[test]
    fn binance_um_endpoint_group_round_robins_handles() {
        let handles = vec![
            connected_handle(),
            connected_handle(),
            connected_handle(),
            connected_handle(),
        ];
        let mut group = WsEndpointGroup::new(
            handles,
            true,
            0,
            "dns",
            "wss://example.invalid".to_string(),
            None,
            vec!["0.0.0.0".parse().unwrap()],
        );

        assert_eq!(group.enqueue_available(WsCommand::Shutdown), Some(0));
        assert_eq!(group.enqueue_available(WsCommand::Shutdown), Some(1));
        assert_eq!(group.enqueue_available(WsCommand::Shutdown), Some(2));
        assert_eq!(group.enqueue_available(WsCommand::Shutdown), Some(3));
        assert_eq!(group.enqueue_available(WsCommand::Shutdown), Some(0));
    }

    #[test]
    fn binance_um_route_prefers_lowest_actual_new_ack_rtt_mean() {
        let candidates = [
            candidate(true, 9_000, 3),
            candidate(true, 4_500, 3),
            candidate(true, 7_000, 2),
        ];

        let selected = select_binance_um_ws_route(&candidates, 0, 3);

        assert_eq!(selected.idx, Some(1));
        assert_eq!(selected.mode, BinanceUmWsRouteMode::Health);
        assert!(!selected.has_blocked_endpoint);
    }

    #[test]
    fn binance_um_route_skips_unsampled_endpoint() {
        let candidates = [
            candidate(true, 9_000, 3),
            candidate(true, 0, 0),
            candidate(true, 4_500, 3),
        ];

        let selected = select_binance_um_ws_route(&candidates, 0, 3);

        assert_eq!(selected.idx, Some(2));
        assert_eq!(selected.mode, BinanceUmWsRouteMode::Health);
        assert!(!selected.has_blocked_endpoint);
    }

    #[test]
    fn binance_um_route_skips_unavailable_unsampled_endpoint() {
        let candidates = [
            candidate(true, 9_000, 3),
            BinanceUmWsRouteCandidate {
                base_available: true,
                um_available: false,
                new_ack_rtt_sum_us: 0,
                new_ack_rtt_count: 0,
            },
            candidate(true, 4_500, 3),
        ];

        let selected = select_binance_um_ws_route(&candidates, 0, 3);

        assert_eq!(selected.idx, Some(2));
        assert_eq!(selected.mode, BinanceUmWsRouteMode::Health);
        assert!(selected.has_blocked_endpoint);
    }

    #[test]
    fn binance_um_route_ignores_fallback_when_direct_available() {
        let candidates = [
            candidate(true, 1_000, 3),
            candidate(true, 9_000, 3),
            candidate(true, 4_500, 3),
        ];
        let fallback = [true, false, false];

        let selected = select_binance_um_ws_route_with_fallback(&candidates, &fallback, 0, 3);

        assert_eq!(selected.idx, Some(2));
        assert_eq!(selected.mode, BinanceUmWsRouteMode::Health);
    }

    #[test]
    fn binance_um_eval_route_uses_base_available_when_all_um_health_blocked() {
        let candidates = [BinanceUmWsRouteCandidate {
            base_available: true,
            um_available: false,
            new_ack_rtt_sum_us: 3_432,
            new_ack_rtt_count: 3,
        }];

        let selected = select_binance_um_ws_route(&candidates, 0, 3);

        assert_eq!(selected.idx, Some(0));
        assert_eq!(selected.mode, BinanceUmWsRouteMode::Base);
        assert!(selected.has_blocked_endpoint);
    }

    #[test]
    fn binance_um_rr_route_uses_base_available_when_um_health_blocked() {
        let candidates = [BinanceUmWsRouteCandidate {
            base_available: true,
            um_available: false,
            new_ack_rtt_sum_us: 3_432,
            new_ack_rtt_count: 3,
        }];

        let selected = select_binance_um_ws_rr_route(&candidates, 0);

        assert_eq!(selected.idx, Some(0));
        assert_eq!(selected.mode, BinanceUmWsRouteMode::Rr);
        assert!(selected.has_blocked_endpoint);
    }

    #[test]
    fn binance_um_route_uses_fallback_only_when_direct_unavailable() {
        let candidates = [
            candidate(true, 1_000, 3),
            BinanceUmWsRouteCandidate {
                base_available: false,
                um_available: false,
                new_ack_rtt_sum_us: 0,
                new_ack_rtt_count: 0,
            },
        ];
        let fallback = [true, false];

        let selected = select_binance_um_ws_route_with_fallback(&candidates, &fallback, 1, 3);

        assert_eq!(selected.idx, Some(0));
        assert_eq!(selected.mode, BinanceUmWsRouteMode::Fallback);
        assert!(selected.has_blocked_endpoint);
    }

    #[test]
    fn binance_um_route_bootstraps_direct_when_actual_new_samples_missing() {
        let candidates = [
            candidate(true, 1_000, 3),
            BinanceUmWsRouteCandidate {
                base_available: true,
                um_available: true,
                new_ack_rtt_sum_us: 0,
                new_ack_rtt_count: 0,
            },
        ];
        let fallback = [true, false];

        let selected = select_binance_um_ws_route_with_fallback(&candidates, &fallback, 1, 3);

        assert_eq!(selected.idx, Some(1));
        assert_eq!(selected.mode, BinanceUmWsRouteMode::Bootstrap);
        assert!(!selected.has_blocked_endpoint);
    }

    #[test]
    fn binance_um_route_falls_back_to_direct_base_when_all_direct_health_blocked() {
        let candidates = [
            candidate(true, 1_000, 3),
            BinanceUmWsRouteCandidate {
                base_available: true,
                um_available: false,
                new_ack_rtt_sum_us: 0,
                new_ack_rtt_count: 0,
            },
        ];
        let fallback = [true, false];

        let selected = select_binance_um_ws_route_with_fallback(&candidates, &fallback, 1, 3);

        assert_eq!(selected.idx, Some(1));
        assert_eq!(selected.mode, BinanceUmWsRouteMode::Base);
        assert!(selected.has_blocked_endpoint);
    }

    #[test]
    fn binance_um_route_ties_choose_lowest_index() {
        let candidates = [
            candidate(true, 3_000, 3),
            candidate(true, 3_000, 3),
            candidate(true, 4_500, 3),
        ];

        let selected = select_binance_um_ws_route(&candidates, 1, 3);

        assert_eq!(selected.idx, Some(0));
        assert_eq!(selected.mode, BinanceUmWsRouteMode::Health);
    }

    #[test]
    fn binance_um_route_eval_snapshot_marks_ready_groups() {
        let handle = WsEndpointHandle::new(
            WsCommandQueue::new(),
            Rc::new(RefCell::new(WsEndpointState::default())),
        );
        let health = BinanceUmWsHealthRuntime::new(BinanceUmWsHealthConfig {
            new_rolling_window: 10,
            new_min_period: 3,
            cancel_rolling_window: 10,
            cancel_min_period: 3,
            percentile: 85,
            pause_ms: 1,
            select_recent: 3,
        });
        for rtt_us in [3_000, 2_000, 1_000] {
            let _ = handle.mark_binance_um_new_ack_rtt(
                rtt_us,
                &health,
                0,
                "0.0.0.0".parse().unwrap(),
                None,
                "wss://example.invalid",
            );
        }
        let group = WsEndpointGroup::new(
            vec![handle],
            false,
            7,
            "direct",
            "wss://example.invalid".to_string(),
            Some("13.112.240.202".parse().unwrap()),
            vec!["172.31.33.133".parse().unwrap()],
        );
        let cfg = BinanceUmWsRouteConfig {
            route: BinanceUmWsRouteKind::Eval,
            redis_env: None,
            redis_key_suffix: "test".to_string(),
            write_interval_ms: 1_000,
            read_interval_ms: 1_000,
            score_half_life_ms: 800,
            score_window_ms: 5_000,
            route_family: "unit".to_string(),
            min_samples: 3,
        };

        let snapshot = build_binance_um_route_eval_snapshot(&[group], &cfg, 3);

        assert!(snapshot.updated_at_us > 0);
        assert_eq!(snapshot.groups.len(), 1);
        let group = &snapshot.groups[0];
        assert_eq!(group.group_id, 7);
        assert_eq!(group.source, "direct");
        assert_eq!(group.endpoint, "13.112.240.202");
        assert_eq!(group.local_ips, vec!["172.31.33.133".to_string()]);
        assert_eq!(group.sample_n, 3);
        assert_eq!(group.score_us, Some(2_000));
    }

    #[test]
    fn binance_um_route_eval_key_uses_full_env_prefix() {
        let cfg = BinanceUmWsRouteConfig {
            route: BinanceUmWsRouteKind::Eval,
            redis_env: None,
            redis_key_suffix: "binance_um_ws_route_eval".to_string(),
            write_interval_ms: 1_000,
            read_interval_ms: 1_000,
            score_half_life_ms: 800,
            score_window_ms: 5_000,
            route_family: "unit".to_string(),
            min_samples: 3,
        };

        assert_eq!(
            binance_um_route_eval_redis_key_for_env("binance_mm_alpha", &cfg),
            "binance_mm_alpha:binance_um_ws_route_eval"
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BinanceUmWsRouteMode {
    Health,
    Bootstrap,
    Base,
    Rr,
    Fallback,
}

impl BinanceUmWsRouteMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Health => "health",
            Self::Bootstrap => "bootstrap",
            Self::Base => "base",
            Self::Rr => "rr",
            Self::Fallback => "fallback",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct BinanceUmWsRouteCandidate {
    base_available: bool,
    um_available: bool,
    new_ack_rtt_sum_us: i64,
    new_ack_rtt_count: usize,
}

#[derive(Clone, Copy, Debug)]
struct BinanceUmWsRouteSelection {
    idx: Option<usize>,
    mode: BinanceUmWsRouteMode,
    has_blocked_endpoint: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BinanceUmWsRouteEvalSnapshot {
    updated_at_us: i64,
    groups: Vec<BinanceUmWsRouteEvalGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BinanceUmWsRouteEvalGroup {
    group_id: usize,
    source: String,
    endpoint: String,
    local_ips: Vec<String>,
    sample_n: usize,
    effective_n: f64,
    latest_us: Option<i64>,
    last_sample_age_ms: Option<i64>,
    score_us: Option<i64>,
}

#[derive(Clone, Debug, Default)]
struct BinanceUmWsRouteFollowState {
    snapshot: Option<BinanceUmWsRouteEvalSnapshot>,
    updated_at: Option<Instant>,
}

type BinanceUmWsRouteFollowShared = Rc<RefCell<BinanceUmWsRouteFollowState>>;

fn binance_um_route_candidate_mean_us(candidate: BinanceUmWsRouteCandidate) -> Option<i64> {
    if candidate.new_ack_rtt_count == 0 {
        None
    } else {
        Some(candidate.new_ack_rtt_sum_us / candidate.new_ack_rtt_count as i64)
    }
}

fn format_binance_um_route_candidates(
    candidates: &[BinanceUmWsRouteCandidate],
    fallback: Option<&[bool]>,
    min_new_samples: usize,
    selected_idx: Option<usize>,
) -> String {
    let mut out = String::from("[");
    for (idx, candidate) in candidates.iter().copied().enumerate() {
        if idx > 0 {
            out.push(';');
        }
        let source = fallback
            .and_then(|fallback| fallback.get(idx))
            .map(|is_fallback| if *is_fallback { "fallback" } else { "direct" })
            .unwrap_or("endpoint");
        let mean_us = binance_um_route_candidate_mean_us(candidate)
            .map(|mean| mean.to_string())
            .unwrap_or_else(|| "NA".to_string());
        let health_eligible =
            candidate.um_available && candidate.new_ack_rtt_count >= min_new_samples;
        out.push_str(&format!(
            "{}{{src={},base={},um={},actual_new_n={},actual_new_mean_us={},actual_new_eligible={},selected={}}}",
            idx,
            source,
            candidate.base_available as u8,
            candidate.um_available as u8,
            candidate.new_ack_rtt_count,
            mean_us,
            health_eligible as u8,
            (selected_idx == Some(idx)) as u8
        ));
    }
    out.push(']');
    out
}

fn writer_env_name() -> String {
    std::env::var("IPC_NAMESPACE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::current_dir().ok().and_then(|path| {
                path.file_name()
                    .map(|name| name.to_string_lossy().to_string())
            })
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn normalize_redis_key_part(raw: &str) -> String {
    raw.trim()
        .trim_matches(':')
        .to_ascii_lowercase()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn binance_um_route_eval_redis_parts_for_env(
    env_name: &str,
    cfg: &BinanceUmWsRouteConfig,
) -> (String, String) {
    let env = normalize_redis_key_part(env_name);
    let suffix = normalize_redis_key_part(&cfg.redis_key_suffix);
    (format!("{env}:"), suffix)
}

fn binance_um_route_eval_env_name(cfg: &BinanceUmWsRouteConfig) -> String {
    cfg.redis_env.clone().unwrap_or_else(writer_env_name)
}

fn binance_um_route_eval_redis_parts(cfg: &BinanceUmWsRouteConfig) -> (String, String) {
    binance_um_route_eval_redis_parts_for_env(&binance_um_route_eval_env_name(cfg), cfg)
}

#[cfg(test)]
fn binance_um_route_eval_redis_key_for_env(env_name: &str, cfg: &BinanceUmWsRouteConfig) -> String {
    let (prefix, suffix) = binance_um_route_eval_redis_parts_for_env(env_name, cfg);
    format!("{prefix}{suffix}")
}

fn binance_um_route_eval_redis_key(cfg: &BinanceUmWsRouteConfig) -> String {
    let (prefix, suffix) = binance_um_route_eval_redis_parts(cfg);
    format!("{prefix}{suffix}")
}

fn build_binance_um_route_eval_snapshot(
    groups: &[WsEndpointGroup],
    cfg: &BinanceUmWsRouteConfig,
    _select_recent: usize,
) -> BinanceUmWsRouteEvalSnapshot {
    let now_us = get_timestamp_us();
    let groups = groups
        .iter()
        .map(|group| {
            let stats = group.binance_um_route_eval_stats(
                now_us,
                cfg.score_half_life_ms,
                cfg.score_window_ms,
            );
            let endpoint = match group.remote_ip {
                Some(remote_ip) => remote_ip.to_string(),
                None => group.url.clone(),
            };
            BinanceUmWsRouteEvalGroup {
                group_id: group.group_id,
                source: group.source.to_string(),
                endpoint,
                local_ips: group.local_ips.iter().map(|ip| ip.to_string()).collect(),
                sample_n: stats.sample_n,
                effective_n: stats.effective_n,
                latest_us: stats.latest_us,
                last_sample_age_ms: stats.last_sample_age_ms,
                score_us: (stats.sample_n >= cfg.min_samples).then_some(stats.score_us),
            }
        })
        .collect();
    BinanceUmWsRouteEvalSnapshot {
        updated_at_us: now_us,
        groups,
    }
}

async fn publish_binance_um_route_eval_snapshot(
    redis_client: &mut Option<RedisClient>,
    cfg: &BinanceUmWsRouteConfig,
    groups: &[WsEndpointGroup],
    select_recent: usize,
) {
    if cfg.route != BinanceUmWsRouteKind::Eval {
        return;
    }
    let (redis_prefix, redis_key_suffix) = binance_um_route_eval_redis_parts(cfg);
    let redis_key = format!("{redis_prefix}{redis_key_suffix}");
    if redis_client.is_none() {
        let mut settings = RedisSettings::default();
        settings.prefix = Some(redis_prefix);
        match RedisClient::connect(settings).await {
            Ok(client) => {
                *redis_client = Some(client);
            }
            Err(err) => {
                warn!(
                    "BinanceUmWsRouteEval: redis connect failed key={} err={:#}",
                    redis_key, err
                );
                return;
            }
        }
    }
    let snapshot = build_binance_um_route_eval_snapshot(groups, cfg, select_recent);
    let ready = snapshot
        .groups
        .iter()
        .filter(|group| group.score_us.is_some())
        .count();
    let total = snapshot.groups.len();
    let Some(client) = redis_client.as_mut() else {
        return;
    };
    match client.set_json(&redis_key_suffix, &snapshot).await {
        Ok(()) => {
            info!(
                "BinanceUmWsRouteEval: redis write ok key={} ready_groups={}/{} score_half_life_ms={} score_window_ms={} min_samples={}",
                redis_key,
                ready,
                total,
                cfg.score_half_life_ms,
                cfg.score_window_ms,
                cfg.min_samples
            );
        }
        Err(err) => {
            warn!(
                "BinanceUmWsRouteEval: redis write failed key={} err={:#}",
                redis_key, err
            );
            *redis_client = None;
        }
    }
}

async fn refresh_binance_um_route_follow_snapshot(
    redis_client: &mut Option<RedisClient>,
    cfg: &BinanceUmWsRouteConfig,
    state: &BinanceUmWsRouteFollowShared,
) {
    if cfg.route != BinanceUmWsRouteKind::Follow {
        return;
    }
    let (redis_prefix, redis_key_suffix) = binance_um_route_eval_redis_parts(cfg);
    let redis_key = format!("{redis_prefix}{redis_key_suffix}");
    if redis_client.is_none() {
        let mut settings = RedisSettings::default();
        settings.prefix = Some(redis_prefix);
        match RedisClient::connect(settings).await {
            Ok(client) => {
                *redis_client = Some(client);
            }
            Err(err) => {
                warn!(
                    "BinanceUmWsRouteFollow: redis connect failed key={} err={:#}",
                    redis_key, err
                );
                return;
            }
        }
    }
    let Some(client) = redis_client.as_mut() else {
        return;
    };
    match client
        .get_json::<BinanceUmWsRouteEvalSnapshot>(&redis_key_suffix)
        .await
    {
        Ok(Some(snapshot)) => {
            let ready = snapshot
                .groups
                .iter()
                .filter(|group| group.score_us.is_some())
                .count();
            let total = snapshot.groups.len();
            {
                let mut state = state.borrow_mut();
                state.snapshot = Some(snapshot);
                state.updated_at = Some(Instant::now());
            }
            info!(
                "BinanceUmWsRouteFollow: redis read ok key={} ready_groups={}/{}",
                redis_key, ready, total
            );
        }
        Ok(None) => {
            warn!(
                "BinanceUmWsRouteFollow: redis key missing key={}",
                redis_key
            );
        }
        Err(err) => {
            warn!(
                "BinanceUmWsRouteFollow: redis read failed key={} err={:#}",
                redis_key, err
            );
            *redis_client = None;
        }
    }
}

fn binance_um_route_reason(route: BinanceUmWsRouteSelection) -> &'static str {
    match route.mode {
        BinanceUmWsRouteMode::Health if route.idx.is_some() => "best_actual_new_ack_rtt",
        BinanceUmWsRouteMode::Health => "no_eligible_actual_new_ack_rtt",
        BinanceUmWsRouteMode::Bootstrap if route.idx.is_some() => "bootstrap_actual_new_ack_rtt",
        BinanceUmWsRouteMode::Bootstrap => "no_available_bootstrap_endpoint",
        BinanceUmWsRouteMode::Base if route.idx.is_some() => "base_available_health_fallback",
        BinanceUmWsRouteMode::Base => "no_base_available_endpoint",
        BinanceUmWsRouteMode::Rr if route.idx.is_some() => "round_robin_base_available",
        BinanceUmWsRouteMode::Rr => "no_available_endpoint",
        BinanceUmWsRouteMode::Fallback if route.idx.is_some() => "direct_unavailable_fallback",
        BinanceUmWsRouteMode::Fallback => "no_available_endpoint",
    }
}

fn select_binance_um_ws_route_with_fallback(
    candidates: &[BinanceUmWsRouteCandidate],
    fallback: &[bool],
    start: usize,
    min_new_samples: usize,
) -> BinanceUmWsRouteSelection {
    if candidates.len() != fallback.len() {
        return select_binance_um_ws_route(candidates, start, min_new_samples);
    }

    let direct_candidates: Vec<BinanceUmWsRouteCandidate> = candidates
        .iter()
        .zip(fallback.iter())
        .filter_map(|(candidate, is_fallback)| (!*is_fallback).then_some(*candidate))
        .collect();
    let direct_indices: Vec<usize> = fallback
        .iter()
        .enumerate()
        .filter_map(|(idx, is_fallback)| (!*is_fallback).then_some(idx))
        .collect();
    let direct_base_available = candidates
        .iter()
        .zip(fallback.iter())
        .any(|(candidate, is_fallback)| !*is_fallback && candidate.base_available);

    if !direct_candidates.is_empty() {
        let direct_start = direct_indices
            .iter()
            .position(|idx| *idx == start)
            .unwrap_or(0);
        let selected =
            select_binance_um_ws_route(&direct_candidates, direct_start, min_new_samples);
        if let Some(local_idx) = selected.idx {
            return BinanceUmWsRouteSelection {
                idx: direct_indices.get(local_idx).copied(),
                mode: selected.mode,
                has_blocked_endpoint: selected.has_blocked_endpoint,
            };
        }
    }

    if direct_base_available {
        for offset in 0..candidates.len() {
            let idx = (start + offset) % candidates.len();
            if !fallback[idx] && candidates[idx].base_available {
                return BinanceUmWsRouteSelection {
                    idx: Some(idx),
                    mode: BinanceUmWsRouteMode::Base,
                    has_blocked_endpoint: true,
                };
            }
        }
    }

    for offset in 0..candidates.len() {
        let idx = (start + offset) % candidates.len();
        if fallback[idx] && candidates[idx].um_available {
            return BinanceUmWsRouteSelection {
                idx: Some(idx),
                mode: BinanceUmWsRouteMode::Fallback,
                has_blocked_endpoint: true,
            };
        }
    }
    for offset in 0..candidates.len() {
        let idx = (start + offset) % candidates.len();
        if fallback[idx] && candidates[idx].base_available {
            return BinanceUmWsRouteSelection {
                idx: Some(idx),
                mode: BinanceUmWsRouteMode::Base,
                has_blocked_endpoint: true,
            };
        }
    }

    BinanceUmWsRouteSelection {
        idx: None,
        mode: BinanceUmWsRouteMode::Fallback,
        has_blocked_endpoint: candidates
            .iter()
            .any(|candidate| candidate.base_available && !candidate.um_available),
    }
}

fn select_binance_um_ws_route(
    candidates: &[BinanceUmWsRouteCandidate],
    start: usize,
    min_new_samples: usize,
) -> BinanceUmWsRouteSelection {
    if candidates.is_empty() {
        return BinanceUmWsRouteSelection {
            idx: None,
            mode: BinanceUmWsRouteMode::Fallback,
            has_blocked_endpoint: false,
        };
    }

    let len = candidates.len();
    let mut has_blocked_endpoint = false;
    let mut best: Option<(usize, i64, usize)> = None;

    for idx in 0..len {
        let candidate = candidates[idx];
        if candidate.base_available && !candidate.um_available {
            has_blocked_endpoint = true;
        }
        if !candidate.um_available {
            continue;
        }
        if candidate.new_ack_rtt_count < min_new_samples {
            continue;
        }
        if best
            .map(|(_, best_sum, best_count)| {
                (candidate.new_ack_rtt_sum_us as i128) * (best_count as i128)
                    < (best_sum as i128) * (candidate.new_ack_rtt_count as i128)
            })
            .unwrap_or(true)
        {
            best = Some((
                idx,
                candidate.new_ack_rtt_sum_us,
                candidate.new_ack_rtt_count,
            ));
        }
    }

    if let Some((idx, _, _)) = best {
        return BinanceUmWsRouteSelection {
            idx: Some(idx),
            mode: BinanceUmWsRouteMode::Health,
            has_blocked_endpoint,
        };
    }
    for offset in 0..len {
        let idx = (start + offset) % len;
        if candidates[idx].um_available {
            return BinanceUmWsRouteSelection {
                idx: Some(idx),
                mode: BinanceUmWsRouteMode::Bootstrap,
                has_blocked_endpoint,
            };
        }
    }
    for offset in 0..len {
        let idx = (start + offset) % len;
        if candidates[idx].base_available {
            return BinanceUmWsRouteSelection {
                idx: Some(idx),
                mode: BinanceUmWsRouteMode::Base,
                has_blocked_endpoint,
            };
        }
    }
    BinanceUmWsRouteSelection {
        idx: None,
        mode: BinanceUmWsRouteMode::Fallback,
        has_blocked_endpoint,
    }
}

fn select_binance_um_ws_rr_route(
    candidates: &[BinanceUmWsRouteCandidate],
    start: usize,
) -> BinanceUmWsRouteSelection {
    if candidates.is_empty() {
        return BinanceUmWsRouteSelection {
            idx: None,
            mode: BinanceUmWsRouteMode::Rr,
            has_blocked_endpoint: false,
        };
    }
    let mut has_blocked_endpoint = false;
    for offset in 0..candidates.len() {
        let idx = (start + offset) % candidates.len();
        let candidate = candidates[idx];
        if candidate.base_available && !candidate.um_available {
            has_blocked_endpoint = true;
        }
        if candidate.base_available {
            return BinanceUmWsRouteSelection {
                idx: Some(idx),
                mode: BinanceUmWsRouteMode::Rr,
                has_blocked_endpoint,
            };
        }
    }
    BinanceUmWsRouteSelection {
        idx: None,
        mode: BinanceUmWsRouteMode::Rr,
        has_blocked_endpoint,
    }
}

fn select_binance_um_ws_follow_group_route(
    candidates: &[BinanceUmWsRouteCandidate],
    fallback: &[bool],
    start: usize,
    follow_snapshot: Option<&BinanceUmWsRouteEvalSnapshot>,
) -> BinanceUmWsRouteSelection {
    if candidates.len() != fallback.len() {
        return select_binance_um_ws_rr_route(candidates, start);
    }
    let mut has_blocked_endpoint =
        candidates
            .iter()
            .zip(fallback.iter())
            .any(|(candidate, is_fallback)| {
                !*is_fallback && candidate.base_available && !candidate.um_available
            });
    let mut best: Option<(usize, i64)> = None;
    if let Some(snapshot) = follow_snapshot {
        for group in &snapshot.groups {
            let idx = group.group_id;
            if idx >= candidates.len() || fallback[idx] || !candidates[idx].um_available {
                continue;
            }
            let Some(score_us) = group.score_us else {
                continue;
            };
            if group.sample_n == 0 {
                continue;
            }
            if best
                .map(|(_, best_score)| score_us < best_score)
                .unwrap_or(true)
            {
                best = Some((idx, score_us));
            }
        }
    }
    if let Some((idx, _)) = best {
        return BinanceUmWsRouteSelection {
            idx: Some(idx),
            mode: BinanceUmWsRouteMode::Health,
            has_blocked_endpoint,
        };
    }

    let direct_base_available = candidates
        .iter()
        .zip(fallback.iter())
        .any(|(candidate, is_fallback)| !*is_fallback && candidate.base_available);
    for offset in 0..candidates.len() {
        let idx = (start + offset) % candidates.len();
        if !fallback[idx] && candidates[idx].um_available {
            return BinanceUmWsRouteSelection {
                idx: Some(idx),
                mode: BinanceUmWsRouteMode::Bootstrap,
                has_blocked_endpoint,
            };
        }
    }
    if direct_base_available {
        has_blocked_endpoint = true;
        for offset in 0..candidates.len() {
            let idx = (start + offset) % candidates.len();
            if !fallback[idx] && candidates[idx].base_available {
                return BinanceUmWsRouteSelection {
                    idx: Some(idx),
                    mode: BinanceUmWsRouteMode::Base,
                    has_blocked_endpoint,
                };
            }
        }
    }
    for offset in 0..candidates.len() {
        let idx = (start + offset) % candidates.len();
        if fallback[idx] && candidates[idx].um_available {
            return BinanceUmWsRouteSelection {
                idx: Some(idx),
                mode: BinanceUmWsRouteMode::Fallback,
                has_blocked_endpoint: true,
            };
        }
    }
    for offset in 0..candidates.len() {
        let idx = (start + offset) % candidates.len();
        if fallback[idx] && candidates[idx].base_available {
            return BinanceUmWsRouteSelection {
                idx: Some(idx),
                mode: BinanceUmWsRouteMode::Base,
                has_blocked_endpoint: true,
            };
        }
    }
    BinanceUmWsRouteSelection {
        idx: None,
        mode: BinanceUmWsRouteMode::Fallback,
        has_blocked_endpoint,
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct InternalOpenTerminateState {
    pub trigger_ts: i64,
    pub registered_at_us: i64,
}

pub(crate) type InternalOpenTerminateMap =
    Rc<RefCell<FastHashMap<i64, InternalOpenTerminateState>>>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct InternalOpenTerminateSummaryKey {
    symbol: String,
    dir: &'static str,
    venue: &'static str,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct InternalOpenTerminateSummaryBucket {
    count: u64,
    qty: f64,
}

pub(crate) type InternalOpenTerminateSummary =
    Rc<RefCell<FastHashMap<InternalOpenTerminateSummaryKey, InternalOpenTerminateSummaryBucket>>>;

#[derive(Debug, Clone)]
struct InternalOpenTerminateOrderMeta {
    symbol: String,
    dir: &'static str,
    venue: &'static str,
    qty: f64,
}

struct IpcThreadQueues {
    order_req_producer: Producer<TradeRequestMsg>,
    query_req_producer: Producer<QueryRequestMsg>,
    order_control_producer: Option<Producer<InternalOpenTerminateMsg>>,
}

struct AsyncThreadQueues {
    order_req_consumer: Consumer<TradeRequestMsg>,
    query_req_consumer: Consumer<QueryRequestMsg>,
    order_control_consumer: Option<Consumer<InternalOpenTerminateMsg>>,
}

enum OrderReqIngress {
    Spsc(Consumer<TradeRequestMsg>),
    Ipc(Subscriber<ipc::Service, TradeRequestIpcPayload, ()>),
}

enum QueryReqIngress {
    Spsc(Consumer<QueryRequestMsg>),
    Ipc(Subscriber<ipc::Service, [u8; QUERY_REQ_PAYLOAD], ()>),
}

enum OrderControlIngress {
    Disabled,
    Spsc(Consumer<InternalOpenTerminateMsg>),
}

fn env_usize_or(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => match value.trim().parse::<usize>() {
            Ok(parsed) if parsed > 0 => parsed,
            Ok(_) => {
                warn!("{} must be > 0, using default {}", name, default);
                default
            }
            Err(err) => {
                warn!(
                    "invalid {}='{}', using default {}: {}",
                    name, value, default, err
                );
                default
            }
        },
        Err(_) => default,
    }
}

fn env_u64_or(name: &str, default: u64) -> u64 {
    match std::env::var(name) {
        Ok(value) => match value.trim().parse::<u64>() {
            Ok(parsed) if parsed > 0 => parsed,
            Ok(_) => {
                warn!("{} must be > 0, using default {}", name, default);
                default
            }
            Err(err) => {
                warn!(
                    "invalid {}='{}', using default {}: {}",
                    name, value, default, err
                );
                default
            }
        },
        Err(_) => default,
    }
}

fn parse_bool_env(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Some(true),
        "0" | "false" | "no" | "n" | "off" => Some(false),
        _ => None,
    }
}

fn enable_ipc_fast_poll() -> bool {
    for name in ["enable_ipc_fast_poll", "ENABLE_IPC_FAST_POLL"] {
        if let Ok(value) = std::env::var(name) {
            if let Some(enabled) = parse_bool_env(&value) {
                return enabled;
            }
            warn!(
                "invalid {}='{}', treating enable_ipc_fast_poll as disabled",
                name, value
            );
            return false;
        }
    }
    true
}

fn router_idle_spin_iters(fast_poll: bool) -> usize {
    let default_iters = if fast_poll {
        DEFAULT_TE_ROUTER_IDLE_SPIN_ITERS
    } else {
        64
    };
    env_usize_or("TE_ROUTER_IDLE_SPIN_ITERS", default_iters)
}

fn new_ipc_spsc_queues(
    internal_open_terminate_enabled: bool,
) -> (IpcThreadQueues, AsyncThreadQueues) {
    let req_cap = env_usize_or("TE_IPC_REQ_QUEUE_CAP", DEFAULT_TE_IPC_REQ_QUEUE_CAP);
    info!(
        "trade_engine ipc spsc queues: req_cap={} internal_open_terminate_enabled={}",
        req_cap, internal_open_terminate_enabled
    );

    let (order_req_producer, order_req_consumer) = RingBuffer::new(req_cap);
    let (query_req_producer, query_req_consumer) = RingBuffer::new(req_cap);
    let (order_control_producer, order_control_consumer) = if internal_open_terminate_enabled {
        let (producer, consumer) = RingBuffer::new(req_cap);
        (Some(producer), Some(consumer))
    } else {
        (None, None)
    };

    (
        IpcThreadQueues {
            order_req_producer,
            query_req_producer,
            order_control_producer,
        },
        AsyncThreadQueues {
            order_req_consumer,
            query_req_consumer,
            order_control_consumer,
        },
    )
}

fn parse_trade_request_payload(payload: &TradeRequestIpcPayload) -> Option<TradeRequestMsg> {
    let Some(raw) = payload.as_request_slice() else {
        warn!(
            "invalid trade request ipc payload (capacity={})",
            TradeRequestIpcPayload::CAPACITY
        );
        return None;
    };
    let mut msg = match crate::trade_request::TradeRequestMsg::parse(raw) {
        Some(msg) => msg,
        None => {
            warn!("invalid trade request binary payload (len={})", raw.len());
            return None;
        }
    };
    let ipc_recv = Instant::now();
    let ipc_recv_us = get_timestamp_us();
    let create_to_ipc_recv_us = ipc_recv_us.saturating_sub(msg.create_time);
    if msg.create_time > 0 && create_to_ipc_recv_us >= TRADE_REQ_IPC_RECV_SLOW_WARN_US {
        warn!(
            "IpcIngressLatency: trade ipc_recv_slow req_type={:?} client_order_id={} params_len={} create_time_us={} ipc_thread_recv_us={} create_to_ipc_thread_recv_us={}",
            msg.req_type,
            msg.client_order_id,
            msg.params.len(),
            msg.create_time,
            ipc_recv_us,
            create_to_ipc_recv_us
        );
    }
    msg.ipc_recv = Some(ipc_recv);
    Some(msg)
}

type RestParamPairs = Vec<(String, String)>;

fn sorted_rest_pairs(mut pairs: RestParamPairs) -> RestParamPairs {
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs
}

fn push_or_replace_pair(pairs: &mut RestParamPairs, key: String, value: String) {
    if key == "timestamp" || key == "recvWindow" || key == "signature" {
        return;
    }
    if let Some((_, existing_value)) = pairs
        .iter_mut()
        .find(|(existing_key, _)| existing_key == &key)
    {
        *existing_value = value;
    } else {
        pairs.push((key, value));
    }
}

fn parse_urlencoded_rest_pairs(raw: &[u8], context: &str) -> Result<RestParamPairs> {
    let raw = std::str::from_utf8(raw)
        .with_context(|| format!("{context} params must be utf-8 urlencoded pairs"))?;
    let mut pairs = Vec::with_capacity(8);
    for (key, value) in url::form_urlencoded::parse(raw.as_bytes()) {
        push_or_replace_pair(&mut pairs, key.into_owned(), value.into_owned());
    }
    Ok(sorted_rest_pairs(pairs))
}

fn binance_std_transfer_direction(req_type: TradeRequestType) -> Option<&'static str> {
    match req_type {
        TradeRequestType::BinanceStdMainToUmTransfer => Some("MAIN_UMFUTURE"),
        TradeRequestType::BinanceStdUmToMainTransfer => Some("UMFUTURE_MAIN"),
        _ => None,
    }
}

fn binance_std_usdt_transfer_rest_pairs(msg: &TradeRequestMsg) -> Result<RestParamPairs> {
    let direction = binance_std_transfer_direction(msg.req_type).ok_or_else(|| {
        anyhow!(
            "unsupported Binance standard transfer req_type={:?}",
            msg.req_type
        )
    })?;
    let pairs = parse_urlencoded_rest_pairs(&msg.params, "Binance standard USDT transfer request")?;
    let amount = pairs
        .iter()
        .find(|(key, _)| key == "amount")
        .map(|(_, value)| value.as_str())
        .ok_or_else(|| anyhow!("Binance standard USDT transfer requires amount"))?;
    let parsed_amount = amount.parse::<f64>().with_context(|| {
        format!("Binance standard USDT transfer amount must be numeric: {amount}")
    })?;
    if !parsed_amount.is_finite() || parsed_amount <= 0.0 {
        return Err(anyhow!(
            "Binance standard USDT transfer amount must be positive: {amount}"
        ));
    }

    Ok(sorted_rest_pairs(vec![
        ("amount".to_string(), amount.to_string()),
        ("asset".to_string(), "USDT".to_string()),
        ("type".to_string(), direction.to_string()),
    ]))
}

fn binance_new_order_rest_pairs(msg: &TradeRequestMsg) -> Result<RestParamPairs> {
    let params = BinanceNewOrderParamsRef::from_bytes(&msg.params).ok_or_else(|| {
        anyhow!(
            "Binance REST new order requires typed params, req_type={:?}",
            msg.req_type
        )
    })?;
    let is_margin = msg.req_type == TradeRequestType::BinanceNewMarginOrder;
    let mut pairs = Vec::with_capacity(10);

    pairs.push((
        "newClientOrderId".to_string(),
        msg.client_order_id.to_string(),
    ));
    if params.ws_response_full {
        pairs.push(("newOrderRespType".to_string(), "FULL".to_string()));
    }
    if params.order_type.is_limit() {
        pairs.push(("price".to_string(), params.price_qv.decimal_string()));
    }
    pairs.push(("quantity".to_string(), params.quantity_qv.decimal_string()));
    if !is_margin {
        pairs.push(("reduceOnly".to_string(), params.reduce_only.to_string()));
    }
    pairs.push(("side".to_string(), params.side.as_str().to_string()));
    if is_margin && params.margin_buy {
        pairs.push(("sideEffectType".to_string(), "MARGIN_BUY".to_string()));
    }
    pairs.push(("symbol".to_string(), params.symbol.to_string()));
    if params.order_type.is_limit() {
        pairs.push((
            "timeInForce".to_string(),
            if is_margin { "GTC" } else { "GTX" }.to_string(),
        ));
    }
    pairs.push(("type".to_string(), params.order_type.as_str().to_string()));
    Ok(pairs)
}

fn binance_cancel_order_rest_pairs(msg: &TradeRequestMsg) -> Result<RestParamPairs> {
    let params = BinanceCancelOrderParams::from_bytes(&msg.params).ok_or_else(|| {
        anyhow!(
            "Binance REST cancel order requires typed params, req_type={:?}",
            msg.req_type
        )
    })?;
    Ok(vec![
        (
            "origClientOrderId".to_string(),
            params.orig_client_order_id.to_string(),
        ),
        ("symbol".to_string(), params.symbol),
    ])
}

fn trade_request_rest_pairs(msg: &TradeRequestMsg) -> Result<RestParamPairs> {
    match msg.req_type {
        TradeRequestType::BinanceNewUMOrder | TradeRequestType::BinanceNewMarginOrder => {
            binance_new_order_rest_pairs(msg)
        }
        TradeRequestType::BinanceCancelUMOrder | TradeRequestType::BinanceCancelMarginOrder => {
            binance_cancel_order_rest_pairs(msg)
        }
        TradeRequestType::BinanceStdMainToUmTransfer
        | TradeRequestType::BinanceStdUmToMainTransfer => binance_std_usdt_transfer_rest_pairs(msg),
        _ => parse_urlencoded_rest_pairs(&msg.params, "Binance REST trade request"),
    }
}

fn query_request_rest_pairs(msg: &QueryRequestMsg) -> Result<RestParamPairs> {
    parse_urlencoded_rest_pairs(&msg.params, "Binance REST query request")
}

fn parse_query_request_payload(payload: &[u8]) -> Option<QueryRequestMsg> {
    let Some(actual_len) = request_payload_len(payload) else {
        warn!(
            "invalid query request binary payload (min_len=24, buf_len={})",
            payload.len()
        );
        return None;
    };
    let msg = match crate::query_request::QueryRequestMsg::parse(&payload[..actual_len]) {
        Some(msg) => msg,
        None => {
            warn!("invalid query request binary payload (len={})", actual_len);
            return None;
        }
    };
    Some(msg)
}

fn parse_internal_open_terminate_payload(payload: &[u8]) -> Option<InternalOpenTerminateMsg> {
    let msg = InternalOpenTerminateMsg::parse(payload)?;
    let ipc_recv_us = get_timestamp_us();
    let create_to_ipc_recv_us = ipc_recv_us.saturating_sub(msg.create_time);
    if msg.create_time > 0 && create_to_ipc_recv_us >= TRADE_REQ_IPC_RECV_SLOW_WARN_US {
        warn!(
            "IpcIngressLatency: internal_open_terminate ipc_recv_slow client_order_id={} create_time_us={} ipc_thread_recv_us={} create_to_ipc_thread_recv_us={}",
            msg.client_order_id,
            msg.create_time,
            ipc_recv_us,
            create_to_ipc_recv_us
        );
    }
    Some(msg)
}

fn pop_trade_req_for_async(consumer: &mut Consumer<TradeRequestMsg>) -> Option<TradeRequestMsg> {
    match consumer.pop() {
        Ok(mut msg) => {
            let async_recv_us = get_timestamp_us();
            let create_to_async_recv_us = async_recv_us.saturating_sub(msg.create_time);
            if msg.create_time > 0 && create_to_async_recv_us >= TRADE_REQ_IPC_RECV_SLOW_WARN_US {
                warn!(
                    "SpscIngressLatency: trade async_recv_slow req_type={:?} client_order_id={} params_len={} create_time_us={} async_thread_recv_us={} create_to_async_thread_recv_us={}",
                    msg.req_type,
                    msg.client_order_id,
                    msg.params.len(),
                    msg.create_time,
                    async_recv_us,
                    create_to_async_recv_us
                );
            }
            if msg.ipc_recv.is_none() {
                msg.ipc_recv = Some(Instant::now());
            }
            Some(msg)
        }
        Err(PopError::Empty) => None,
    }
}

fn pop_query_req_for_async(consumer: &mut Consumer<QueryRequestMsg>) -> Option<QueryRequestMsg> {
    match consumer.pop() {
        Ok(msg) => Some(msg),
        Err(PopError::Empty) => None,
    }
}

fn pop_order_control_for_async(
    consumer: &mut Consumer<InternalOpenTerminateMsg>,
) -> Option<InternalOpenTerminateMsg> {
    match consumer.pop() {
        Ok(msg) => Some(msg),
        Err(PopError::Empty) => None,
    }
}

fn recv_trade_req_from_ipc(
    subscriber: &Subscriber<ipc::Service, TradeRequestIpcPayload, ()>,
) -> Option<TradeRequestMsg> {
    match subscriber.receive() {
        Ok(Some(sample)) => {
            let msg = parse_trade_request_payload(sample.payload());
            drop(sample);
            msg
        }
        Ok(None) => None,
        Err(err) => {
            warn!("trade request receive error: {err}");
            None
        }
    }
}

fn recv_query_req_from_ipc(
    subscriber: &Subscriber<ipc::Service, [u8; QUERY_REQ_PAYLOAD], ()>,
) -> Option<QueryRequestMsg> {
    match subscriber.receive() {
        Ok(Some(sample)) => {
            let msg = parse_query_request_payload(sample.payload());
            drop(sample);
            msg
        }
        Ok(None) => None,
        Err(err) => {
            warn!("query request receive error: {err}");
            None
        }
    }
}

impl OrderReqIngress {
    fn try_recv(&mut self) -> Option<TradeRequestMsg> {
        match self {
            Self::Spsc(consumer) => pop_trade_req_for_async(consumer),
            Self::Ipc(subscriber) => recv_trade_req_from_ipc(subscriber),
        }
    }
}

impl QueryReqIngress {
    fn try_recv(&mut self) -> Option<QueryRequestMsg> {
        match self {
            Self::Spsc(consumer) => pop_query_req_for_async(consumer),
            Self::Ipc(subscriber) => recv_query_req_from_ipc(subscriber),
        }
    }
}

impl OrderControlIngress {
    fn try_recv(&mut self) -> Option<InternalOpenTerminateMsg> {
        match self {
            Self::Disabled => None,
            Self::Spsc(consumer) => pop_order_control_for_async(consumer),
        }
    }
}

fn push_trade_req_or_pending(
    producer: &mut Producer<TradeRequestMsg>,
    msg: TradeRequestMsg,
    pending: &mut Option<TradeRequestMsg>,
    full_count: &mut u64,
) -> bool {
    match producer.push(msg) {
        Ok(()) => {
            *pending = None;
            true
        }
        Err(PushError::Full(returned)) => {
            *full_count = full_count.saturating_add(1);
            if *full_count % SPSC_QUEUE_FULL_WARN_INTERVAL == 1 {
                warn!(
                    "TE IPC order_req SPSC full; keeping pending client_order_id={} full_count={}",
                    returned.client_order_id, *full_count
                );
            }
            *pending = Some(returned);
            false
        }
    }
}

fn push_query_req_or_pending(
    producer: &mut Producer<QueryRequestMsg>,
    msg: QueryRequestMsg,
    pending: &mut Option<QueryRequestMsg>,
    full_count: &mut u64,
) -> bool {
    match producer.push(msg) {
        Ok(()) => {
            *pending = None;
            true
        }
        Err(PushError::Full(returned)) => {
            *full_count = full_count.saturating_add(1);
            if *full_count % SPSC_QUEUE_FULL_WARN_INTERVAL == 1 {
                warn!(
                    "TE IPC query_req SPSC full; keeping pending client_query_id={} full_count={}",
                    returned.client_query_id, *full_count
                );
            }
            *pending = Some(returned);
            false
        }
    }
}

fn push_order_control_or_pending(
    producer: &mut Producer<InternalOpenTerminateMsg>,
    msg: InternalOpenTerminateMsg,
    pending: &mut Option<InternalOpenTerminateMsg>,
    full_count: &mut u64,
) -> bool {
    match producer.push(msg) {
        Ok(()) => {
            *pending = None;
            true
        }
        Err(PushError::Full(returned)) => {
            *full_count = full_count.saturating_add(1);
            if *full_count % SPSC_QUEUE_FULL_WARN_INTERVAL == 1 {
                warn!(
                    "TE IPC order_control SPSC full; keeping pending client_order_id={} full_count={}",
                    returned.client_order_id, *full_count
                );
            }
            *pending = Some(returned);
            false
        }
    }
}

fn spawn_te_ipc_thread(
    exchange_name: String,
    order_req_service: String,
    order_control_service: Option<String>,
    query_req_service: String,
    mut queues: IpcThreadQueues,
    shutdown: CancellationToken,
    ipc_core: Option<usize>,
    fast_poll: bool,
) -> Result<thread::JoinHandle<()>> {
    let handle = thread::Builder::new()
        .name("te-ipc".to_string())
        .spawn(move || {
            if let Some(c) = ipc_core {
                if let Err(err) = pin_to_core(c) {
                    warn!(
                        "te-ipc thread pin to core {} failed: {:#}; continuing without affinity",
                        c, err
                    );
                }
            }
            if let Err(err) = run_te_ipc_thread(
                &exchange_name,
                &order_req_service,
                order_control_service.as_deref(),
                &query_req_service,
                &mut queues,
                shutdown.clone(),
                fast_poll,
            ) {
                warn!("trade_engine IPC thread exited with error: {:#}", err);
                shutdown.cancel();
            }
        })
        .context("spawn trade_engine IPC thread failed")?;
    Ok(handle)
}

fn run_te_ipc_thread(
    exchange_name: &str,
    order_req_service: &str,
    order_control_service: Option<&str>,
    query_req_service: &str,
    queues: &mut IpcThreadQueues,
    shutdown: CancellationToken,
    fast_poll: bool,
) -> Result<()> {
    let node_name = format!("trade_engine_{}_ipc", exchange_name);
    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;

    let order_service = node
        .service_builder(&ServiceName::new(order_req_service)?)
        .publish_subscribe::<TradeRequestIpcPayload>()
        .subscriber_max_buffer_size(256)
        .open_or_create()?;
    let order_subscriber: Subscriber<ipc::Service, TradeRequestIpcPayload, ()> =
        order_service.subscriber_builder().create()?;

    let order_control_subscriber = if let Some(order_control_service) = order_control_service {
        let control_service = node
            .service_builder(&ServiceName::new(order_control_service)?)
            .publish_subscribe::<[u8; ORDER_TERMINATE_PAYLOAD_LEN]>()
            .subscriber_max_buffer_size(256)
            .open_or_create()?;
        let subscriber: Subscriber<ipc::Service, [u8; ORDER_TERMINATE_PAYLOAD_LEN], ()> =
            control_service.subscriber_builder().create()?;
        Some(subscriber)
    } else {
        None
    };

    let query_service = node
        .service_builder(&ServiceName::new(query_req_service)?)
        .publish_subscribe::<[u8; QUERY_REQ_PAYLOAD]>()
        .subscriber_max_buffer_size(256)
        .open_or_create()?;
    let query_subscriber: Subscriber<ipc::Service, [u8; QUERY_REQ_PAYLOAD], ()> =
        query_service.subscriber_builder().create()?;

    info!(
        "trade_engine IPC thread started; order_req='{}' order_control='{}' query_req='{}' fast_poll={} idle_policy=spin",
        order_req_service,
        order_control_service.unwrap_or("-"),
        query_req_service,
        fast_poll
    );

    let mut pending_order_req: Option<TradeRequestMsg> = None;
    let mut pending_order_control: Option<InternalOpenTerminateMsg> = None;
    let mut pending_query_req: Option<QueryRequestMsg> = None;
    let mut order_req_full_count = 0u64;
    let mut order_control_full_count = 0u64;
    let mut query_req_full_count = 0u64;

    while !shutdown.is_cancelled() {
        let mut did_work = false;

        if let Some(msg) = pending_order_req.take() {
            did_work = true;
            push_trade_req_or_pending(
                &mut queues.order_req_producer,
                msg,
                &mut pending_order_req,
                &mut order_req_full_count,
            );
        }
        if let (Some(producer), Some(msg)) = (
            queues.order_control_producer.as_mut(),
            pending_order_control.take(),
        ) {
            did_work = true;
            push_order_control_or_pending(
                producer,
                msg,
                &mut pending_order_control,
                &mut order_control_full_count,
            );
        }
        if let Some(msg) = pending_query_req.take() {
            did_work = true;
            push_query_req_or_pending(
                &mut queues.query_req_producer,
                msg,
                &mut pending_query_req,
                &mut query_req_full_count,
            );
        }

        if pending_order_req.is_none() {
            loop {
                match order_subscriber.receive() {
                    Ok(Some(sample)) => {
                        did_work = true;
                        let msg = parse_trade_request_payload(sample.payload());
                        drop(sample);
                        if let Some(msg) = msg {
                            if !push_trade_req_or_pending(
                                &mut queues.order_req_producer,
                                msg,
                                &mut pending_order_req,
                                &mut order_req_full_count,
                            ) {
                                break;
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        warn!("trade request receive error: {err}");
                        break;
                    }
                }
            }
        }

        if let (Some(producer), Some(subscriber)) = (
            queues.order_control_producer.as_mut(),
            order_control_subscriber.as_ref(),
        ) {
            if pending_order_control.is_none() {
                for _ in 0..IPC_THREAD_DRAIN_BUDGET {
                    match subscriber.receive() {
                        Ok(Some(sample)) => {
                            did_work = true;
                            let msg = parse_internal_open_terminate_payload(sample.payload());
                            drop(sample);
                            if let Some(msg) = msg {
                                if !push_order_control_or_pending(
                                    producer,
                                    msg,
                                    &mut pending_order_control,
                                    &mut order_control_full_count,
                                ) {
                                    break;
                                }
                            }
                        }
                        Ok(None) => break,
                        Err(err) => {
                            warn!("internal open terminate receive error: {err}");
                            break;
                        }
                    }
                }
            }
        }

        if pending_query_req.is_none() {
            for _ in 0..IPC_THREAD_DRAIN_BUDGET {
                match query_subscriber.receive() {
                    Ok(Some(sample)) => {
                        did_work = true;
                        let msg = parse_query_request_payload(sample.payload());
                        drop(sample);
                        if let Some(msg) = msg {
                            if !push_query_req_or_pending(
                                &mut queues.query_req_producer,
                                msg,
                                &mut pending_query_req,
                                &mut query_req_full_count,
                            ) {
                                break;
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        warn!("query request receive error: {err}");
                        break;
                    }
                }
            }
        }

        if !did_work || fast_poll {
            std::hint::spin_loop();
        }
    }

    info!("trade_engine IPC thread exiting");
    Ok(())
}

fn request_payload_len(payload: &[u8]) -> Option<usize> {
    // Layout: u32 msg_type, u32 params_length, i64 create_time, i64 client_id, params...
    if payload.len() < 24 {
        return None;
    }
    let params_len = u32::from_le_bytes(payload[4..8].try_into().ok()?) as usize;
    let total = 24usize.saturating_add(params_len);
    if total == 0 || total > payload.len() {
        return None;
    }
    Some(total)
}

fn summarize_bybit_response(body: &str) -> String {
    let Ok(v) = serde_json::from_str::<Value>(body) else {
        return format!("non_json body_len={}", body.len());
    };

    let ret_code = v
        .get("retCode")
        .and_then(|x| x.as_i64())
        .map(|x| x.to_string())
        .unwrap_or_else(|| "NA".to_string());
    let ret_msg = v
        .get("retMsg")
        .and_then(|x| x.as_str())
        .unwrap_or("<missing>");

    let list_len = v
        .get("result")
        .and_then(|r| r.get("list"))
        .and_then(|x| x.as_array())
        .map(|x| x.len().to_string())
        .unwrap_or_else(|| "NA".to_string());

    format!(
        "retCode={} retMsg={} result.list.len={} body_len={}",
        ret_code,
        ret_msg,
        list_len,
        body.len()
    )
}

fn register_internal_open_terminate(
    internal_terminates: &InternalOpenTerminateMap,
    msg: InternalOpenTerminateMsg,
) {
    let now_us = get_timestamp_us();
    internal_terminates.borrow_mut().insert(
        msg.client_order_id,
        InternalOpenTerminateState {
            trigger_ts: msg.trigger_ts,
            registered_at_us: now_us,
        },
    );
    debug!(
        "internal open terminate registered client_order_id={} trigger_ts={} registered_at_us={}",
        msg.client_order_id, msg.trigger_ts, now_us
    );
}

fn prune_internal_open_terminates(internal_terminates: &InternalOpenTerminateMap, now_us: i64) {
    internal_terminates
        .borrow_mut()
        .retain(|client_order_id, state| {
            let keep =
                now_us.saturating_sub(state.registered_at_us) <= INTERNAL_OPEN_TERMINATE_TTL_US;
            if !keep {
                debug!(
                    "internal open terminate expired client_order_id={} trigger_ts={} registered_at_us={} now_us={}",
                    client_order_id, state.trigger_ts, state.registered_at_us, now_us
                );
            }
            keep
        });
}

fn drain_order_control_ingress(
    ingress: &mut OrderControlIngress,
    internal_terminates: &InternalOpenTerminateMap,
) -> bool {
    let mut did_work = false;
    let now_us = get_timestamp_us();
    prune_internal_open_terminates(internal_terminates, now_us);
    for _ in 0..IPC_THREAD_DRAIN_BUDGET {
        let Some(msg) = ingress.try_recv() else {
            break;
        };
        did_work = true;
        register_internal_open_terminate(internal_terminates, msg);
    }
    did_work
}

fn decode_internal_open_terminate_order_meta(
    msg: &TradeRequestMsg,
) -> Option<InternalOpenTerminateOrderMeta> {
    match msg.req_type {
        TradeRequestType::BinanceNewUMOrder | TradeRequestType::BinanceWsNewUMOrder => {
            let params = BinanceNewOrderParams::from_bytes(&msg.params)?;
            Some(InternalOpenTerminateOrderMeta {
                symbol: params.symbol,
                dir: params.side.as_str(),
                venue: "binance_um",
                qty: params.quantity_qv.get_val(),
            })
        }
        TradeRequestType::BinanceNewMarginOrder | TradeRequestType::BinanceWsNewMarginOrder => {
            let params = BinanceNewOrderParams::from_bytes(&msg.params)?;
            Some(InternalOpenTerminateOrderMeta {
                symbol: params.symbol,
                dir: params.side.as_str(),
                venue: "binance_margin",
                qty: params.quantity_qv.get_val(),
            })
        }
        TradeRequestType::OkexNewUMOrder => {
            let params = OkexNewOrderParams::from_bytes(&msg.params)?;
            Some(InternalOpenTerminateOrderMeta {
                symbol: params.symbol,
                dir: params.side.as_str(),
                venue: "okex_um",
                qty: params.quantity_qv.get_val(),
            })
        }
        TradeRequestType::OkexNewMarginOrder => {
            let params = OkexNewOrderParams::from_bytes(&msg.params)?;
            Some(InternalOpenTerminateOrderMeta {
                symbol: params.symbol,
                dir: params.side.as_str(),
                venue: "okex_margin",
                qty: params.quantity_qv.get_val(),
            })
        }
        TradeRequestType::GateFuturesNewOrder => {
            let params = GateNewOrderParams::from_bytes(&msg.params)?;
            Some(InternalOpenTerminateOrderMeta {
                symbol: params.symbol,
                dir: params.side.as_str(),
                venue: "gate_futures",
                qty: params.quantity_qv.get_val(),
            })
        }
        TradeRequestType::GateUnifiedNewOrder => {
            let params = GateNewOrderParams::from_bytes(&msg.params)?;
            Some(InternalOpenTerminateOrderMeta {
                symbol: params.symbol,
                dir: params.side.as_str(),
                venue: "gate_unified",
                qty: params.quantity_qv.get_val(),
            })
        }
        TradeRequestType::BybitNewUMOrder => {
            let params = crate::bybit::BybitNewOrderParams::from_bytes(&msg.params)?;
            Some(InternalOpenTerminateOrderMeta {
                symbol: params.symbol,
                dir: params.side.as_str(),
                venue: "bybit_um",
                qty: params.quantity_qv.get_val(),
            })
        }
        TradeRequestType::BybitNewMarginOrder => {
            let params = crate::bybit::BybitNewOrderParams::from_bytes(&msg.params)?;
            Some(InternalOpenTerminateOrderMeta {
                symbol: params.symbol,
                dir: params.side.as_str(),
                venue: "bybit_margin",
                qty: params.quantity_qv.get_val(),
            })
        }
        TradeRequestType::BitgetNewUMOrder => {
            let params = BitgetNewOrderParams::from_bytes(&msg.params)?;
            Some(InternalOpenTerminateOrderMeta {
                symbol: params.symbol,
                dir: params.side.as_str(),
                venue: "bitget_um",
                qty: params.quantity_qv.get_val(),
            })
        }
        TradeRequestType::BitgetNewMarginOrder => {
            let params = BitgetNewOrderParams::from_bytes(&msg.params)?;
            Some(InternalOpenTerminateOrderMeta {
                symbol: params.symbol,
                dir: params.side.as_str(),
                venue: "bitget_margin",
                qty: params.quantity_qv.get_val(),
            })
        }
        _ => None,
    }
}

pub(crate) fn record_internal_open_terminate_summary(
    summary: &InternalOpenTerminateSummary,
    msg: &TradeRequestMsg,
) {
    let Some(meta) = decode_internal_open_terminate_order_meta(msg) else {
        return;
    };
    let key = InternalOpenTerminateSummaryKey {
        symbol: meta.symbol,
        dir: meta.dir,
        venue: meta.venue,
    };
    let mut summary = summary.borrow_mut();
    let bucket = summary.entry(key).or_default();
    bucket.count = bucket.count.saturating_add(1);
    bucket.qty += meta.qty;
}

fn flush_internal_open_terminate_summary(summary: &InternalOpenTerminateSummary) {
    let mut summary = summary.borrow_mut();
    if summary.is_empty() {
        return;
    }

    let mut items: Vec<_> = summary.drain().collect();
    items.sort_by(|(left_key, left_bucket), (right_key, right_bucket)| {
        right_bucket
            .count
            .cmp(&left_bucket.count)
            .then_with(|| left_key.venue.cmp(right_key.venue))
            .then_with(|| left_key.symbol.cmp(&right_key.symbol))
            .then_with(|| left_key.dir.cmp(right_key.dir))
    });

    let total_count: u64 = items.iter().map(|(_, bucket)| bucket.count).sum();
    let total_qty: f64 = items.iter().map(|(_, bucket)| bucket.qty).sum();
    let truncated = items
        .len()
        .saturating_sub(INTERNAL_OPEN_TERMINATE_SUMMARY_MAX_GROUPS);
    let details = items
        .iter()
        .take(INTERNAL_OPEN_TERMINATE_SUMMARY_MAX_GROUPS)
        .map(|(key, bucket)| {
            format!(
                "{{venue={} symbol={} dir={} count={} qty={:.8}}}",
                key.venue, key.symbol, key.dir, bucket.count, bucket.qty
            )
        })
        .collect::<Vec<_>>()
        .join(",");

    info!(
        "InternalOpenTerminateSummary: window_s={} total_count={} total_qty={:.8} groups={} truncated_groups={} details=[{}]",
        INTERNAL_OPEN_TERMINATE_SUMMARY_INTERVAL_SECS,
        total_count,
        total_qty,
        items.len(),
        truncated,
        details
    );
}

pub(crate) fn take_internal_open_terminate(
    internal_terminates: &InternalOpenTerminateMap,
    client_order_id: i64,
) -> Option<InternalOpenTerminateState> {
    internal_terminates.borrow_mut().remove(&client_order_id)
}

pub(crate) fn internal_open_terminated_outcome(
    req_type: TradeRequestType,
    client_order_id: i64,
    exchange: Exchange,
    state: InternalOpenTerminateState,
    stage: &'static str,
) -> TradeExecOutcome {
    let body = serde_json::json!({
        "transport": "internal",
        "state": "terminated",
        "code": INTERNAL_OPEN_TERMINATED_ERROR_CODE,
        "msg": "arb open internal terminate before exchange send",
        "reason": "arb open internal terminate before exchange send",
        "stage": stage,
        "clientOrderId": client_order_id,
        "triggerTs": state.trigger_ts,
        "registeredAtUs": state.registered_at_us,
        "ttlUs": INTERNAL_OPEN_TERMINATE_TTL_US,
    })
    .to_string();
    TradeExecOutcome {
        req_type,
        client_order_id,
        status: 499,
        body,
        exchange,
        order_id: 0,
        order_status_u8: 0,
        order_update_time: 0,
        executed_qty: 0.0,
        response_price: 0.0,
    }
}

fn truncate_for_log(text: &str, max_chars: usize) -> String {
    let mut truncated = String::new();
    for (idx, ch) in text.chars().enumerate() {
        if idx >= max_chars {
            truncated.push_str("...");
            break;
        }
        truncated.push(ch);
    }
    truncated
}

async fn join_or_abort(name: &str, mut handle: tokio::task::JoinHandle<()>) {
    match tokio::time::timeout(Duration::from_secs(2), &mut handle).await {
        Ok(Ok(())) => info!("trade_engine worker stopped: {}", name),
        Ok(Err(err)) => warn!("trade_engine worker join error ({}): {}", name, err),
        Err(_) => {
            warn!("trade_engine worker shutdown timeout, aborting: {}", name);
            handle.abort();
            let _ = handle.await;
        }
    }
}

pub struct TradeEngine {
    local_ips: Vec<IpAddr>,
    accounts: Vec<ApiKey>,
    ipc_core: Option<usize>,
    binance_um_whitelist_ip: Option<IpAddr>,
    binance_um_ws_direct_ips: Vec<IpAddr>,
    binance_um_ws_health: BinanceUmWsHealthConfig,
    binance_um_ws_route: BinanceUmWsRouteConfig,
}

impl TradeEngine {
    pub fn new(
        local_ips: Vec<IpAddr>,
        accounts: Vec<ApiKey>,
        ipc_core: Option<usize>,
        binance_um_whitelist_ip: Option<IpAddr>,
        binance_um_ws_direct_ips: Vec<IpAddr>,
        binance_um_ws_health: BinanceUmWsHealthConfig,
        binance_um_ws_route: BinanceUmWsRouteConfig,
    ) -> Self {
        Self {
            local_ips,
            accounts,
            ipc_core,
            binance_um_whitelist_ip,
            binance_um_ws_direct_ips,
            binance_um_ws_health,
            binance_um_ws_route,
        }
    }

    pub async fn run(self, exchange: Exchange) -> Result<()> {
        self.run_with_shutdown(exchange, CancellationToken::new())
            .await
    }

    pub async fn run_with_shutdown(
        self,
        exchange: Exchange,
        shutdown: CancellationToken,
    ) -> Result<()> {
        if !matches!(
            exchange,
            Exchange::Binance
                | Exchange::Okex
                | Exchange::Bybit
                | Exchange::Bitget
                | Exchange::Gate
        ) {
            return Err(anyhow!(
                "unsupported exchange '{}'. Allowed: binance, okex, bybit, bitget, gate",
                exchange
            ));
        }
        let exec_backend = ExecBackend::for_exchange(exchange);
        if !exec_backend.supports_exchange(exchange) {
            return Err(anyhow!(
                "execution backend '{}' does not support exchange '{}'",
                exec_backend.as_str(),
                exchange
            ));
        }
        let use_ltp_backend = exec_backend == ExecBackend::Ltp;

        let canonical_exchange = exchange.as_str();
        let fast_poll = enable_ipc_fast_poll();
        let router_idle_spin_iters = router_idle_spin_iters(fast_poll);
        let internal_open_terminate_enabled = fast_poll;

        // 构建带命名空间的服务名
        let order_req_service = build_service_name(&format!("order_reqs/{}", canonical_exchange));
        let order_control_service =
            build_service_name(&format!("order_controls/{}", canonical_exchange));
        let order_resp_service = build_service_name(&format!("order_resps/{}", canonical_exchange));
        let query_req_service = build_service_name(&format!("query_reqs/{}", canonical_exchange));
        let query_resp_service = build_service_name(&format!("query_resps/{}", canonical_exchange));

        info!(
            "trade_engine starting; exchange={}, order_req='{}', order_control='{}', order_resp='{}', query_req='{}', query_resp='{}', enable_ipc_fast_poll={} router_idle_spin_iters={} internal_open_terminate_enabled={}",
            canonical_exchange,
            order_req_service,
            order_control_service,
            order_resp_service,
            query_req_service,
            query_resp_service,
            fast_poll,
            router_idle_spin_iters,
            internal_open_terminate_enabled
        );

        // Async thread owns outbound publishers and other network-facing IPC publications.
        let node_name = format!("trade_engine_{}_async", canonical_exchange);
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let order_resp_service_obj = node
            .service_builder(&ServiceName::new(&order_resp_service)?)
            .publish_subscribe::<[u8; 64]>()
            .subscriber_max_buffer_size(256)
            .open_or_create()?;
        let order_resp_publisher: Publisher<ipc::Service, [u8; 64], ()> =
            order_resp_service_obj.publisher_builder().create()?;

        let query_resp_service_obj = node
            .service_builder(&ServiceName::new(&query_resp_service)?)
            .publish_subscribe::<[u8; QUERY_RESP_PAYLOAD]>()
            .subscriber_max_buffer_size(256)
            .open_or_create()?;
        let query_resp_publisher: Publisher<ipc::Service, [u8; QUERY_RESP_PAYLOAD], ()> =
            query_resp_service_obj.publisher_builder().create()?;

        // Latency snapshot publisher（每 30s venue 级 IPC 推送，512B 定长载荷）。
        // service name: `<IPC_NAMESPACE>/te_pubs/<venue>/latency`——
        //   - `IPC_NAMESPACE` 由 `build_service_name` 自动加（多 te 实例的隔离）
        //   - `te_pubs` 前缀表明发布方是 trade_engine，避免与 spread_pbs 等其他源混淆
        let latency_service =
            build_service_name(&format!("te_pubs/{}/latency", canonical_exchange));
        let latency_service_obj = node
            .service_builder(&ServiceName::new(&latency_service)?)
            .publish_subscribe::<[u8; LATENCY_SNAPSHOT_PAYLOAD_LEN]>()
            .subscriber_max_buffer_size(8)
            .open_or_create()?;
        let latency_publisher: Publisher<ipc::Service, [u8; LATENCY_SNAPSHOT_PAYLOAD_LEN], ()> =
            latency_service_obj.publisher_builder().create()?;
        debug!("publisher created for service: {}", latency_service);
        let binance_um_new_ack_trace_publisher: Option<
            Rc<Publisher<ipc::Service, [u8; BINANCE_UM_NEW_ACK_TRACE_PAYLOAD_LEN], ()>>,
        > = if exchange == Exchange::Binance && !use_ltp_backend {
            let service = build_service_name(BINANCE_UM_NEW_ACK_TRACE_SERVICE);
            let service_obj = node
                .service_builder(&ServiceName::new(&service)?)
                .publish_subscribe::<[u8; BINANCE_UM_NEW_ACK_TRACE_PAYLOAD_LEN]>()
                .max_publishers(1)
                .max_subscribers(32)
                .history_size(128)
                .subscriber_max_buffer_size(8192)
                .open_or_create()?;
            let publisher = service_obj.publisher_builder().create()?;
            info!("publisher created for service: {}", service);
            Some(Rc::new(publisher))
        } else {
            None
        };

        // 直接使用传入的 exchange 枚举

        // Async thread publishes responses directly to iceoryx; inbound requests still come from SPSC.

        if exchange == Exchange::Binance && !use_ltp_backend && self.accounts.is_empty() {
            return Err(anyhow!("Binance requires API keys in config"));
        }

        let (order_req_ingress, order_control_ingress, query_req_ingress, ipc_thread_handle) =
            if fast_poll {
                let (ipc_queues, async_queues) =
                    new_ipc_spsc_queues(internal_open_terminate_enabled);
                let ipc_thread_handle = spawn_te_ipc_thread(
                    canonical_exchange.to_string(),
                    order_req_service.clone(),
                    internal_open_terminate_enabled.then(|| order_control_service.clone()),
                    query_req_service.clone(),
                    ipc_queues,
                    shutdown.clone(),
                    self.ipc_core,
                    fast_poll,
                )?;
                let AsyncThreadQueues {
                    order_req_consumer,
                    query_req_consumer,
                    order_control_consumer,
                } = async_queues;
                (
                    OrderReqIngress::Spsc(order_req_consumer),
                    order_control_consumer
                        .map(OrderControlIngress::Spsc)
                        .unwrap_or(OrderControlIngress::Disabled),
                    QueryReqIngress::Spsc(query_req_consumer),
                    Some(ipc_thread_handle),
                )
            } else {
                let order_service = node
                    .service_builder(&ServiceName::new(&order_req_service)?)
                    .publish_subscribe::<TradeRequestIpcPayload>()
                    .subscriber_max_buffer_size(256)
                    .open_or_create()?;
                let order_subscriber: Subscriber<ipc::Service, TradeRequestIpcPayload, ()> =
                    order_service.subscriber_builder().create()?;

                let query_service = node
                    .service_builder(&ServiceName::new(&query_req_service)?)
                    .publish_subscribe::<[u8; QUERY_REQ_PAYLOAD]>()
                    .subscriber_max_buffer_size(256)
                    .open_or_create()?;
                let query_subscriber: Subscriber<ipc::Service, [u8; QUERY_REQ_PAYLOAD], ()> =
                    query_service.subscriber_builder().create()?;

                info!(
                    "trade_engine ingress running on async thread; order_req='{}' query_req='{}'",
                    order_req_service, query_req_service
                );

                (
                    OrderReqIngress::Ipc(order_subscriber),
                    OrderControlIngress::Disabled,
                    QueryReqIngress::Ipc(query_subscriber),
                    None,
                )
            };

        // 跨 endpoint 共享的延迟分桶。capacity 10000。
        // - new/cancel: T1−T0（IPC→WS 端到端），所有 venue 通用。
        // - resp: 服务端响应 4 区间分解（uplink/server/downlink/rtt × new/cancel = 8 桶），
        //   仅在 venue 暴露服务端时间戳时启用（Bitget/Gate/Binance/OKEx/Bybit 均支持）。
        // current_thread runtime + LocalSet 下所有 ws task 同线程，`Rc<RefCell<..>>` 即可。
        let mk_bucket = |label: String| {
            Rc::new(RefCell::new(LatencyKll::with_capacity(
                label,
                LatencyKll::DEFAULT_CAPACITY,
            )))
        };
        let venue = exchange.as_str();
        let resp_enabled = matches!(
            exchange,
            Exchange::Bitget
                | Exchange::Gate
                | Exchange::Binance
                | Exchange::Okex
                | Exchange::Bybit
        );
        let lat_buckets = WsLatencyBuckets {
            new: mk_bucket(format!("trade_engine:{}:ws:new", venue)),
            cancel: mk_bucket(format!("trade_engine:{}:ws:cancel", venue)),
            resp: resp_enabled.then(|| RespLatencyBuckets {
                uplink_new: mk_bucket(format!("trade_engine:{}:ws:uplink:new", venue)),
                uplink_cancel: mk_bucket(format!("trade_engine:{}:ws:uplink:cancel", venue)),
                server_new: mk_bucket(format!("trade_engine:{}:ws:server:new", venue)),
                server_cancel: mk_bucket(format!("trade_engine:{}:ws:server:cancel", venue)),
                downlink_new: mk_bucket(format!("trade_engine:{}:ws:downlink:new", venue)),
                downlink_cancel: mk_bucket(format!("trade_engine:{}:ws:downlink:cancel", venue)),
                rtt_new: mk_bucket(format!("trade_engine:{}:ws:rtt:new", venue)),
                rtt_cancel: mk_bucket(format!("trade_engine:{}:ws:rtt:cancel", venue)),
            }),
        };

        // 初始化 REST dispatcher（用于 Binance）
        let binance_um_ip_whitelist_mode = exchange == Exchange::Binance
            && !use_ltp_backend
            && binance_um_ip_whitelist_mode_enabled();
        let rest_dispatcher = if exchange == Exchange::Binance && !use_ltp_backend {
            Some(Rc::new(tokio::sync::Mutex::new(Dispatcher::new(
                &self.local_ips,
                &self.accounts,
                shutdown.clone(),
                binance_um_ip_whitelist_mode,
                self.binance_um_whitelist_ip,
            )?)))
        } else {
            None
        };

        // 初始化 WebSocket 客户端（用于 OKEx/Gate/Binance）
        let binance_ws_enabled = exchange == Exchange::Binance
            && !use_ltp_backend
            && binance_account_mode() == BinanceAccountMode::Standard;
        if exchange == Exchange::Binance && !use_ltp_backend && !binance_ws_enabled {
            info!("binance ws disabled (BINANCE_ACCOUNT_MODE!=STANDARD)");
        }
        let binance_spot_fix_enabled = exchange == Exchange::Binance
            && !use_ltp_backend
            && binance_account_mode() == BinanceAccountMode::Standard
            && spot_fix_enabled_from_env()?;
        if exchange == Exchange::Binance && !use_ltp_backend {
            info!(
                "Binance Spot FIX mode: {}={} (default off)",
                BINANCE_SPOT_FIX_ENABLED_ENV,
                if binance_spot_fix_enabled {
                    "on"
                } else {
                    "off"
                }
            );
        }
        let mut worker_handles: Vec<(&'static str, tokio::task::JoinHandle<()>)> = Vec::new();

        let trade_resp_sink = TradeResponseSink::new(order_resp_publisher);
        let query_resp_sink = QueryResponseSink::new(query_resp_publisher);
        let internal_open_terminates: InternalOpenTerminateMap =
            Rc::new(RefCell::new(fast_hash_map()));
        let internal_open_terminate_summary: InternalOpenTerminateSummary =
            Rc::new(RefCell::new(fast_hash_map()));

        // 周期 publisher：每 30s 把所有非空桶的 KLL 快照打包成 LatencySnapshotMsg
        // 推到 IPC（service: <IPC_NAMESPACE>/te_pubs/<venue>/latency）。
        // 空桶不入消息，没桶不发。
        {
            let lat_buckets_for_ticker = lat_buckets.clone();
            let venue_id = exchange.to_u8() as u32;
            let shutdown_for_ticker = shutdown.clone();
            let ticker = tokio::task::spawn_local(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(30));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                interval.tick().await; // 跳过启动后立即触发的第一次
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown_for_ticker.cancelled() => break,
                        _ = interval.tick() => {
                            if let Some(msg) = lat_buckets_for_ticker.take_snapshot(venue_id) {
                                if let Err(e) = latency_publisher.send_copy(msg.into_bytes()) {
                                    warn!(
                                        "trade_engine: latency snapshot publish failed: {}",
                                        e
                                    );
                                }
                            }
                        }
                    }
                }
                info!("trade_engine: latency snapshot ticker stopped");
            });
            worker_handles.push(("latency_snapshot_ticker", ticker));
        }

        if internal_open_terminate_enabled {
            let summary = internal_open_terminate_summary.clone();
            let shutdown_for_ticker = shutdown.clone();
            let ticker = tokio::task::spawn_local(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(
                    INTERNAL_OPEN_TERMINATE_SUMMARY_INTERVAL_SECS,
                ));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                interval.tick().await;
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown_for_ticker.cancelled() => {
                            flush_internal_open_terminate_summary(&summary);
                            break;
                        }
                        _ = interval.tick() => {
                            flush_internal_open_terminate_summary(&summary);
                        }
                    }
                }
                info!("trade_engine: internal open terminate summary ticker stopped");
            });
            worker_handles.push(("internal_open_terminate_summary_ticker", ticker));
        }

        if self.binance_um_ws_route.route.uses_eval_redis() {
            info!(
                "BinanceUmWsRoute: route={} key={} suffix={} route_family={} write_interval_ms={} read_interval_ms={} score_half_life_ms={} score_window_ms={} min_samples={}",
                self.binance_um_ws_route.route.as_str(),
                binance_um_route_eval_redis_key(&self.binance_um_ws_route),
                self.binance_um_ws_route.redis_key_suffix,
                self.binance_um_ws_route.route_family,
                self.binance_um_ws_route.write_interval_ms,
                self.binance_um_ws_route.read_interval_ms,
                self.binance_um_ws_route.score_half_life_ms,
                self.binance_um_ws_route.score_window_ms,
                self.binance_um_ws_route.min_samples
            );
        } else {
            info!(
                "BinanceUmWsRoute: route={} redis disabled",
                self.binance_um_ws_route.route.as_str()
            );
        }

        let mut binance_spot_fix_handle: Option<BinanceSpotFixHandle> = None;
        if binance_spot_fix_enabled {
            let creds = self
                .accounts
                .first()
                .ok_or_else(|| anyhow!("Binance Spot FIX requires Binance API key"))?;
            let cfg = BinanceSpotFixConfig::from_env(
                creds.key.trim().to_string(),
                self.local_ips.first().copied(),
            )?;
            info!(
                "spawning Binance Spot FIX client url={} sender_comp_id={} source_ip={}",
                cfg.url(),
                cfg.sender_comp_id(),
                cfg.source_ip()
                    .map(|ip| ip.to_string())
                    .unwrap_or_else(|| "system-default".to_string())
            );
            let (handle, task) = crate::binance_fix::spawn_binance_spot_fix_client(
                cfg,
                trade_resp_sink.clone(),
                shutdown.clone(),
            );
            worker_handles.push(("binance_spot_fix_client", task));
            binance_spot_fix_handle = Some(handle);
        }
        let mut gate_futures_ws_endpoints: Option<Vec<WsEndpointHandle>> = None;
        let mut binance_spot_ws_endpoints: Option<Vec<WsEndpointHandle>> = None;
        let mut binance_um_ws_endpoint_groups: Option<Vec<WsEndpointGroup>> = None;
        let mut binance_um_ws_health_runtime: Option<BinanceUmWsHealthRuntime> = None;
        let binance_um_ws_route_follow_state: BinanceUmWsRouteFollowShared =
            Rc::new(RefCell::new(Default::default()));

        let ws_endpoints = if use_ltp_backend {
            crate::ltp_ws::warn_if_unsupported_ltp_exchange(exchange)?;
            let mut local_ips = self.local_ips.clone();
            if local_ips.is_empty() {
                warn!("LTP ws local_ips empty; using default binding 0.0.0.0");
                local_ips.push("0.0.0.0".parse()?);
            }

            let connect_timeout_ms = WsConstants::CONNECT_TIMEOUT_MS;
            let ping_interval_ms = env_u64_or("LTP_WS_PING_INTERVAL_MS", 10_000);
            let max_inflight = WsConstants::MAX_INFLIGHT;
            let ltp_ws_url = std::env::var("LTP_WS_URL")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| crate::ltp_ws::DEFAULT_WS_URL.to_string());

            let mut endpoints = Vec::with_capacity(local_ips.len());
            for (idx, ip) in local_ips.into_iter().enumerate() {
                let cmd_queue = WsCommandQueue::new();
                let state = StdRc::new(RefCell::new(Default::default()));
                let client = TradeWsClient::new(
                    idx,
                    exchange,
                    exchange,
                    true,
                    ip,
                    ltp_ws_url.clone(),
                    connect_timeout_ms,
                    ping_interval_ms,
                    max_inflight,
                    None,
                    None,
                    None,
                    None,
                    cmd_queue.clone(),
                    trade_resp_sink.clone(),
                    internal_open_terminates.clone(),
                    internal_open_terminate_summary.clone(),
                    shutdown.clone(),
                    state.clone(),
                    false,
                    lat_buckets.clone(),
                );
                info!(
                    "spawning LTP ws client id={} logical_exchange={} ip={} url={} ping_interval_ms={} max_inflight={}",
                    idx,
                    exchange,
                    client.local_ip(),
                    ltp_ws_url,
                    ping_interval_ms,
                    max_inflight
                );
                let handle = tokio::task::spawn_local(async move {
                    client.run().await;
                });
                worker_handles.push(("ltp_ws_client", handle));
                endpoints.push(WsEndpointHandle::new(cmd_queue, state));
            }
            Some(endpoints)
        } else if exchange == Exchange::Bitget {
            // 前置校验：Bitget 必须是 UTA + one-way 持仓模式；margin 路径还要求 Advanced。
            // 这里直接 panic，避免配置错误时 trade_engine 继续运行并反复拒单。
            let bitget_precheck_creds = account_common::bitget_auth::BitgetCredentials::from_env()
                .context(
                    "bitget precheck: BITGET_API_KEY/BITGET_API_SECRET/BITGET_PASSPHRASE not set",
                )?;
            let bitget_precheck_http = reqwest::Client::new();
            if let Err(err) = crate::bitget_precheck::ensure_unified_account(
                &bitget_precheck_http,
                &bitget_precheck_creds,
            )
            .await
            {
                panic!("bitget precheck failed: {err:#}");
            }

            let mut local_ips = self.local_ips.clone();
            if local_ips.is_empty() {
                warn!("bitget ws local_ips empty; using default binding 0.0.0.0");
                local_ips.push("0.0.0.0".parse()?);
            }

            let connect_timeout_ms = WsConstants::CONNECT_TIMEOUT_MS;
            let ping_interval_ms = WsConstants::PING_INTERVAL_MS;
            let max_inflight = WsConstants::MAX_INFLIGHT;

            let mut endpoints = Vec::with_capacity(local_ips.len());
            for (idx, ip) in local_ips.into_iter().enumerate() {
                let cmd_queue = WsCommandQueue::new();
                let state = StdRc::new(RefCell::new(Default::default()));
                let client = TradeWsClient::new(
                    idx,
                    exchange,
                    exchange,
                    false,
                    ip,
                    WsConstants::BITGET_TRADE_WS_URL.to_string(),
                    connect_timeout_ms,
                    ping_interval_ms,
                    max_inflight,
                    None,
                    None,
                    None,
                    None,
                    cmd_queue.clone(),
                    trade_resp_sink.clone(),
                    internal_open_terminates.clone(),
                    internal_open_terminate_summary.clone(),
                    shutdown.clone(),
                    state.clone(),
                    false,
                    lat_buckets.clone(),
                );
                info!(
                    "spawning bitget ws client id={} ip={} max_inflight={}",
                    idx,
                    client.local_ip(),
                    max_inflight
                );
                let handle = tokio::task::spawn_local(async move {
                    client.run().await;
                });
                worker_handles.push(("bitget_ws_client", handle));
                endpoints.push(WsEndpointHandle::new(cmd_queue, state));
            }
            Some(endpoints)
        } else if exchange == Exchange::Bybit {
            // 前置校验：账号必须升级到 UTA 且开启 spot margin，否则 isLeverage=1 的现货单会被交易所直接拒
            let bybit_precheck_creds = account_common::bybit_auth::BybitCredentials::from_env()
                .context("bybit precheck: BYBIT_API_KEY/BYBIT_API_SECRET not set")?;
            let bybit_precheck_http = reqwest::Client::new();
            crate::bybit_precheck::ensure_uta_and_spot_margin(
                &bybit_precheck_http,
                &bybit_precheck_creds,
            )
            .await?;

            let mut local_ips = self.local_ips.clone();
            if local_ips.is_empty() {
                warn!("bybit ws local_ips empty; using default binding 0.0.0.0");
                local_ips.push("0.0.0.0".parse()?);
            }

            let connect_timeout_ms = WsConstants::CONNECT_TIMEOUT_MS;
            let ping_interval_ms = WsConstants::PING_INTERVAL_MS;
            let max_inflight = WsConstants::MAX_INFLIGHT;

            let mut endpoints = Vec::with_capacity(local_ips.len());
            for (idx, ip) in local_ips.into_iter().enumerate() {
                let cmd_queue = WsCommandQueue::new();
                let state = StdRc::new(RefCell::new(Default::default()));
                let client = TradeWsClient::new(
                    idx,
                    exchange,
                    exchange,
                    false,
                    ip,
                    WsConstants::BYBIT_TRADE_WS_URL.to_string(),
                    connect_timeout_ms,
                    ping_interval_ms,
                    max_inflight,
                    None,
                    None,
                    None,
                    None,
                    cmd_queue.clone(),
                    trade_resp_sink.clone(),
                    internal_open_terminates.clone(),
                    internal_open_terminate_summary.clone(),
                    shutdown.clone(),
                    state.clone(),
                    false,
                    lat_buckets.clone(),
                );
                info!(
                    "spawning bybit ws client id={} ip={} max_inflight={}",
                    idx,
                    client.local_ip(),
                    max_inflight
                );
                let handle = tokio::task::spawn_local(async move {
                    client.run().await;
                });
                worker_handles.push(("bybit_ws_client", handle));
                endpoints.push(WsEndpointHandle::new(cmd_queue, state));
            }
            Some(endpoints)
        } else if exchange == Exchange::Okex {
            // 前置校验：账户必须处于 Multi-currency margin (acctLv=3) 或 Portfolio margin (acctLv=4)，
            // 否则 tdMode=cross 的现货/合约单会被拒
            let okex_precheck_creds = account_common::okex_auth::OkexCredentials::from_env()
                .context("okex precheck: OKX_API_KEY/OKX_API_SECRET/OKX_PASSPHRASE not set")?;
            let okex_precheck_http = reqwest::Client::new();
            crate::okex_precheck::ensure_unified_margin_mode(
                &okex_precheck_http,
                &okex_precheck_creds,
            )
            .await?;

            let mut local_ips = self.local_ips.clone();
            if local_ips.is_empty() {
                warn!("okex ws local_ips empty; using default binding 0.0.0.0");
                local_ips.push("0.0.0.0".parse()?);
                local_ips.push("0.0.0.0".parse()?);
            } else if local_ips.len() == 1 {
                local_ips.push(local_ips[0]);
                warn!(
                    "okex ws local_ips only 1 provided; duplicating {} for dual connection",
                    local_ips[0]
                );
            } else if local_ips.len() > 2 {
                local_ips.truncate(2);
                warn!(
                    "okex ws local_ips >2; truncating to first two ({}, {})",
                    local_ips[0], local_ips[1]
                );
            }

            let urls = vec![
                WsConstants::OKEX_BUSINESS_WS_URL.to_string(),
                WsConstants::OKEX_BUSINESS_WS_URL.to_string(),
            ];

            let connect_timeout_ms = WsConstants::CONNECT_TIMEOUT_MS;
            let ping_interval_ms = WsConstants::PING_INTERVAL_MS;
            let max_inflight = WsConstants::MAX_INFLIGHT;

            let mut endpoints = Vec::with_capacity(urls.len());
            for (idx, (ip, url)) in local_ips.into_iter().zip(urls.into_iter()).enumerate() {
                let cmd_queue = WsCommandQueue::new();
                let state = StdRc::new(RefCell::new(Default::default()));
                let client = TradeWsClient::new(
                    idx,
                    exchange,
                    exchange,
                    false,
                    ip,
                    url,
                    connect_timeout_ms,
                    ping_interval_ms,
                    max_inflight,
                    None, // OKEx 认证会自动从环境变量读取
                    None,
                    None,
                    None,
                    cmd_queue.clone(),
                    trade_resp_sink.clone(),
                    internal_open_terminates.clone(),
                    internal_open_terminate_summary.clone(),
                    shutdown.clone(),
                    state.clone(),
                    false,
                    lat_buckets.clone(),
                );
                info!(
                    "spawning ws client id={} ip={} max_inflight={}",
                    idx,
                    client.local_ip(),
                    max_inflight
                );
                let handle = tokio::task::spawn_local(async move {
                    client.run().await;
                });
                worker_handles.push(("ws_client", handle));
                endpoints.push(WsEndpointHandle::new(cmd_queue, state));
            }
            Some(endpoints)
        } else if exchange == Exchange::Gate {
            // 前置校验：账户必须升级到统一账户（mode != classic），否则 account=unified+auto_borrow 会被拒
            let gate_precheck_creds = account_common::gate_auth::GateCredentials::from_env()
                .context("gate precheck: GATE_API_KEY/GATE_API_SECRET not set")?;
            let gate_precheck_http = reqwest::Client::new();
            crate::gate_precheck::ensure_unified_account(&gate_precheck_http, &gate_precheck_creds)
                .await?;

            let mut local_ips = self.local_ips.clone();
            if local_ips.is_empty() {
                warn!("gate ws local_ips empty; using default binding 0.0.0.0");
                local_ips.push("0.0.0.0".parse()?);
            }

            let connect_timeout_ms = WsConstants::CONNECT_TIMEOUT_MS;
            let ping_interval_ms = WsConstants::PING_INTERVAL_MS;
            let max_inflight = WsConstants::MAX_INFLIGHT;

            let mut spot_endpoints = Vec::with_capacity(local_ips.len());
            let mut futures_endpoints = Vec::with_capacity(local_ips.len());

            for (idx, ip) in local_ips.into_iter().enumerate() {
                let spot_cmd_queue = WsCommandQueue::new();
                let spot_state = StdRc::new(RefCell::new(Default::default()));
                let spot_client = TradeWsClient::new(
                    idx,
                    exchange,
                    exchange,
                    false,
                    ip,
                    WsConstants::GATE_SPOT_WS_URL.to_string(),
                    connect_timeout_ms,
                    ping_interval_ms,
                    max_inflight,
                    None,
                    None,
                    Some(crate::gate_ws::GateWsKind::SpotUnified),
                    Some(query_resp_sink.clone()),
                    spot_cmd_queue.clone(),
                    trade_resp_sink.clone(),
                    internal_open_terminates.clone(),
                    internal_open_terminate_summary.clone(),
                    shutdown.clone(),
                    spot_state.clone(),
                    false,
                    lat_buckets.clone(),
                );
                info!(
                    "spawning gate spot ws client id={} ip={} max_inflight={}",
                    idx,
                    spot_client.local_ip(),
                    max_inflight
                );
                let handle = tokio::task::spawn_local(async move {
                    spot_client.run().await;
                });
                worker_handles.push(("gate_spot_ws_client", handle));
                spot_endpoints.push(WsEndpointHandle::new(spot_cmd_queue, spot_state));

                let fut_cmd_queue = WsCommandQueue::new();
                let fut_state = StdRc::new(RefCell::new(Default::default()));
                let fut_client = TradeWsClient::new(
                    idx,
                    exchange,
                    exchange,
                    false,
                    ip,
                    WsConstants::GATE_FUTURES_WS_URL.to_string(),
                    connect_timeout_ms,
                    ping_interval_ms,
                    max_inflight,
                    None,
                    None,
                    Some(crate::gate_ws::GateWsKind::FuturesUsdt),
                    Some(query_resp_sink.clone()),
                    fut_cmd_queue.clone(),
                    trade_resp_sink.clone(),
                    internal_open_terminates.clone(),
                    internal_open_terminate_summary.clone(),
                    shutdown.clone(),
                    fut_state.clone(),
                    false,
                    lat_buckets.clone(),
                );
                info!(
                    "spawning gate futures ws client id={} ip={} max_inflight={}",
                    idx,
                    fut_client.local_ip(),
                    max_inflight
                );
                let handle = tokio::task::spawn_local(async move {
                    fut_client.run().await;
                });
                worker_handles.push(("gate_futures_ws_client", handle));
                futures_endpoints.push(WsEndpointHandle::new(fut_cmd_queue, fut_state));
            }

            gate_futures_ws_endpoints = Some(futures_endpoints);
            Some(spot_endpoints)
        } else if exchange == Exchange::Binance && binance_ws_enabled {
            let mut local_ips = self.local_ips.clone();
            if local_ips.is_empty() {
                warn!("binance ws local_ips empty; using default binding 0.0.0.0");
                local_ips.push("0.0.0.0".parse()?);
            }

            let connect_timeout_ms = WsConstants::CONNECT_TIMEOUT_MS;
            // ping 频率仅作用于 trade_engine 的 Binance WS，UM(合约)与 spot 分开可调，
            // 避免探测/保活过频被 Binance 拒绝。未设时回退默认。
            let um_ping_interval_ms = env_u64_or(
                "TRADE_ENGINE_BINANCE_UM_PING_INTERVAL_MS",
                WsConstants::PING_INTERVAL_MS,
            );
            let spot_ping_interval_ms = env_u64_or(
                "TRADE_ENGINE_BINANCE_SPOT_PING_INTERVAL_MS",
                WsConstants::PING_INTERVAL_MS,
            );
            info!(
                "binance trade ws ping intervals: um_ping_interval_ms={} spot_ping_interval_ms={}",
                um_ping_interval_ms, spot_ping_interval_ms
            );
            let max_inflight = WsConstants::MAX_INFLIGHT;
            let binance_creds = self.accounts.first().cloned();
            let binance_um_ws_url = if binance_um_ip_whitelist_mode {
                WsConstants::BINANCE_UM_MM_WS_URL
            } else {
                WsConstants::BINANCE_UM_WS_URL
            };
            if binance_um_ip_whitelist_mode {
                info!(
                    "binance UM IP whitelist mode enabled; Binance UM WS url={} local_ip={:?}",
                    binance_um_ws_url, self.binance_um_whitelist_ip
                );
            }

            let (um_local_ips, spot_local_ips) = if binance_um_ip_whitelist_mode {
                let whitelist_ip = self.binance_um_whitelist_ip.ok_or_else(|| {
                    anyhow!("BINANCE_UM_IP_WHITELIST_MODE=on requires Binance UM whitelist IP for WS dispatch")
                })?;
                if !local_ips.contains(&whitelist_ip) {
                    return Err(anyhow!(
                        "Binance UM whitelist IP {} is not present in trade_engine local IPs",
                        whitelist_ip
                    ));
                }
                info!(
                    "binance UM IP whitelist mode enabled; Binance UM WS pinned to local_ip={}, Binance spot WS keeps all local_ips={:?}",
                    whitelist_ip, local_ips
                );
                (vec![whitelist_ip], local_ips.clone())
            } else {
                (local_ips.clone(), local_ips.clone())
            };

            let mut um_logical_endpoint_specs: Vec<(Option<IpAddr>, &'static str)> = Vec::new();
            um_logical_endpoint_specs.push((None, "dns"));
            for &remote_ip in &self.binance_um_ws_direct_ips {
                um_logical_endpoint_specs.push((Some(remote_ip), "direct"));
            }
            let basic_um_local_ips = if self.binance_um_ws_direct_ips.is_empty() {
                Some(binance_um_basic_ws_local_ips(&um_local_ips)?)
            } else {
                None
            };
            let planned_um_connection_count = basic_um_local_ips
                .as_ref()
                .map(|ips| ips.len())
                .unwrap_or_else(|| um_logical_endpoint_specs.len() * um_local_ips.len());
            if let Some(basic_ips) = basic_um_local_ips.as_ref() {
                info!(
                    "binance UM WS basic DNS mode enabled; spawning 1 logical endpoint group, {} connection(s), reconnect_period_ms={}, local_ips={}",
                    basic_ips.len(),
                    BINANCE_UM_BASIC_WS_RECONNECT_PERIOD_MS,
                    basic_ips
                        .iter()
                        .map(|ip| ip.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                );
            } else {
                info!(
                    "binance UM WS direct IP mode enabled; spawning {} logical endpoint group(s), {} connection(s): 1 DNS fallback group + {} direct group(s), local_ips={} reconnect_period_ms={}",
                    um_logical_endpoint_specs.len(),
                    planned_um_connection_count,
                    self.binance_um_ws_direct_ips.len(),
                    um_local_ips.len(),
                    BINANCE_UM_BASIC_WS_RECONNECT_PERIOD_MS
                );
            }

            let um_shutdown_on_rate_limit = um_logical_endpoint_specs.len() <= 1;
            let spot_shutdown_on_rate_limit = spot_local_ips.len() <= 1;
            let mut um_endpoints = Vec::with_capacity(planned_um_connection_count);
            let mut um_endpoint_groups = Vec::with_capacity(um_logical_endpoint_specs.len());
            let mut spot_endpoints = Vec::with_capacity(spot_local_ips.len());
            let binance_um_ws_health = BinanceUmWsHealthRuntime::new(WsBinanceUmWsHealthConfig {
                new_rolling_window: self.binance_um_ws_health.new_rolling_window,
                new_min_period: self.binance_um_ws_health.new_min_period,
                cancel_rolling_window: self.binance_um_ws_health.cancel_rolling_window,
                cancel_min_period: self.binance_um_ws_health.cancel_min_period,
                percentile: self.binance_um_ws_health.percentile,
                pause_ms: self.binance_um_ws_health.pause_ms,
                select_recent: self.binance_um_ws_health.select_recent,
            });
            binance_um_ws_health_runtime = Some(binance_um_ws_health.clone());
            let mut um_client_id = 0usize;
            for (group_idx, (remote_ip_override, source)) in
                um_logical_endpoint_specs.into_iter().enumerate()
            {
                let is_direct = remote_ip_override.is_some();
                let group_local_ips = if !is_direct {
                    basic_um_local_ips
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| um_local_ips.clone())
                } else {
                    um_local_ips.clone()
                };
                let mut group_handles = Vec::with_capacity(group_local_ips.len());
                for &ip in &group_local_ips {
                    let um_cmd_queue = WsCommandQueue::new();
                    let endpoint_state = StdRc::new(RefCell::new(Default::default()));
                    let reconnect_offset_ms = if planned_um_connection_count > 0 {
                        BINANCE_UM_BASIC_WS_RECONNECT_PERIOD_MS.saturating_mul(um_client_id as u64)
                            / planned_um_connection_count as u64
                    } else {
                        0
                    };
                    let mut um_client = TradeWsClient::new(
                        um_client_id,
                        exchange,
                        exchange,
                        false,
                        ip,
                        binance_um_ws_url.to_string(),
                        connect_timeout_ms,
                        um_ping_interval_ms,
                        max_inflight,
                        None,
                        binance_creds.clone(),
                        None,
                        Some(query_resp_sink.clone()),
                        um_cmd_queue.clone(),
                        trade_resp_sink.clone(),
                        internal_open_terminates.clone(),
                        internal_open_terminate_summary.clone(),
                        shutdown.clone(),
                        endpoint_state.clone(),
                        um_shutdown_on_rate_limit,
                        lat_buckets.clone(),
                    );
                    um_client = um_client.with_binance_um_ws_health(binance_um_ws_health.clone());
                    um_client = um_client.with_binance_um_route_group_id(group_idx as u32);
                    um_client = um_client.with_planned_reconnect(
                        BINANCE_UM_BASIC_WS_RECONNECT_PERIOD_MS,
                        reconnect_offset_ms,
                    );
                    if let Some(publisher) = binance_um_new_ack_trace_publisher.as_ref() {
                        um_client =
                            um_client.with_binance_um_new_ack_trace_publisher(publisher.clone());
                    }
                    if let Some(remote_ip) = remote_ip_override {
                        um_client = um_client.with_remote_ip_override(remote_ip);
                    }
                    info!(
                        "spawning binance um ws client id={} group_id={} ip={} source={} remote_ip={} max_inflight={} planned_reconnect_period_ms={} planned_reconnect_offset_ms={}",
                        um_client_id,
                        group_idx,
                        um_client.local_ip(),
                        source,
                        remote_ip_override
                            .map(|ip| ip.to_string())
                            .unwrap_or_else(|| "dns".to_string()),
                        max_inflight,
                        BINANCE_UM_BASIC_WS_RECONNECT_PERIOD_MS,
                        reconnect_offset_ms
                    );
                    let handle = tokio::task::spawn_local(async move {
                        um_client.run().await;
                    });
                    worker_handles.push(("binance_um_ws_client", handle));
                    let endpoint = WsEndpointHandle::new(um_cmd_queue, endpoint_state);
                    group_handles.push(endpoint.clone());
                    um_endpoints.push(endpoint);
                    um_client_id = um_client_id.saturating_add(1);
                }
                um_endpoint_groups.push(WsEndpointGroup::new(
                    group_handles,
                    !is_direct,
                    group_idx,
                    source,
                    binance_um_ws_url.to_string(),
                    remote_ip_override,
                    group_local_ips,
                ));
            }
            binance_um_ws_endpoint_groups = Some(um_endpoint_groups);
            if self.binance_um_ws_route.route == BinanceUmWsRouteKind::Eval {
                if let Some(groups) = binance_um_ws_endpoint_groups.as_ref().cloned() {
                    let cfg = self.binance_um_ws_route.clone();
                    let health = binance_um_ws_health_runtime.clone();
                    let shutdown_for_eval = shutdown.clone();
                    let ticker = tokio::task::spawn_local(async move {
                        let mut redis_client: Option<RedisClient> = None;
                        let mut interval =
                            tokio::time::interval(Duration::from_millis(cfg.write_interval_ms));
                        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                        interval.tick().await;
                        loop {
                            tokio::select! {
                                biased;
                                _ = shutdown_for_eval.cancelled() => break,
                                _ = interval.tick() => {
                                    let select_recent = health
                                        .as_ref()
                                        .map(|h| h.select_recent())
                                        .unwrap_or(3);
                                    publish_binance_um_route_eval_snapshot(
                                        &mut redis_client,
                                        &cfg,
                                        &groups,
                                        select_recent,
                                    )
                                    .await;
                                }
                            }
                        }
                        info!("BinanceUmWsRouteEval: ticker stopped");
                    });
                    worker_handles.push(("binance_um_ws_route_eval_ticker", ticker));
                }
            }
            if self.binance_um_ws_route.route == BinanceUmWsRouteKind::Follow {
                let cfg = self.binance_um_ws_route.clone();
                let state = binance_um_ws_route_follow_state.clone();
                let shutdown_for_follow = shutdown.clone();
                let ticker = tokio::task::spawn_local(async move {
                    let mut redis_client: Option<RedisClient> = None;
                    let mut interval =
                        tokio::time::interval(Duration::from_millis(cfg.read_interval_ms));
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    refresh_binance_um_route_follow_snapshot(&mut redis_client, &cfg, &state).await;
                    interval.tick().await;
                    loop {
                        tokio::select! {
                            biased;
                            _ = shutdown_for_follow.cancelled() => break,
                            _ = interval.tick() => {
                                refresh_binance_um_route_follow_snapshot(
                                    &mut redis_client,
                                    &cfg,
                                    &state,
                                )
                                .await;
                            }
                        }
                    }
                    info!("BinanceUmWsRouteFollow: ticker stopped");
                });
                worker_handles.push(("binance_um_ws_route_follow_ticker", ticker));
            }

            for (idx, ip) in spot_local_ips.into_iter().enumerate() {
                let spot_cmd_queue = WsCommandQueue::new();
                let spot_state = StdRc::new(RefCell::new(Default::default()));
                let spot_client = TradeWsClient::new(
                    idx,
                    exchange,
                    exchange,
                    false,
                    ip,
                    WsConstants::BINANCE_SPOT_WS_URL.to_string(),
                    connect_timeout_ms,
                    spot_ping_interval_ms,
                    max_inflight,
                    None,
                    binance_creds.clone(),
                    None,
                    Some(query_resp_sink.clone()),
                    spot_cmd_queue.clone(),
                    trade_resp_sink.clone(),
                    internal_open_terminates.clone(),
                    internal_open_terminate_summary.clone(),
                    shutdown.clone(),
                    spot_state.clone(),
                    spot_shutdown_on_rate_limit,
                    lat_buckets.clone(),
                );
                info!(
                    "spawning binance spot ws client id={} ip={} max_inflight={}",
                    idx,
                    spot_client.local_ip(),
                    max_inflight
                );
                let handle = tokio::task::spawn_local(async move {
                    spot_client.run().await;
                });
                worker_handles.push(("binance_spot_ws_client", handle));
                spot_endpoints.push(WsEndpointHandle::new(spot_cmd_queue, spot_state));
            }
            binance_spot_ws_endpoints = Some(spot_endpoints);
            Some(um_endpoints)
        } else {
            None
        };

        // Spawn unified request router
        let ws_endpoints_for_req_worker = ws_endpoints.clone();
        let binance_spot_fix_handle_for_req_worker = binance_spot_fix_handle.clone();
        let gate_futures_ws_endpoints_for_req_worker = gate_futures_ws_endpoints.clone();
        let binance_spot_ws_endpoints_for_req_worker = binance_spot_ws_endpoints.clone();
        let binance_um_ws_endpoint_groups_for_req_worker = binance_um_ws_endpoint_groups.clone();
        let binance_um_ws_health_for_req_worker = binance_um_ws_health_runtime.clone();
        let binance_um_ws_route_for_req_worker = self.binance_um_ws_route.clone();
        let binance_um_ws_route_follow_state_for_req_worker =
            binance_um_ws_route_follow_state.clone();
        let rest_dispatcher_for_orders = rest_dispatcher.clone();
        let trade_resp_sink_for_req_worker = trade_resp_sink.clone();
        let exchange_for_req_worker = exchange;
        let use_ltp_backend_for_req_worker = use_ltp_backend;
        let shutdown_for_req_worker = shutdown.clone();
        let internal_open_terminates_for_req_worker = internal_open_terminates.clone();
        let internal_open_terminate_summary_for_req_worker =
            internal_open_terminate_summary.clone();
        let router_idle_spin_iters_for_req_worker = router_idle_spin_iters;
        let req_worker = tokio::task::spawn_local(async move {
            let mut ws_endpoints = ws_endpoints_for_req_worker;
            let binance_spot_fix_handle = binance_spot_fix_handle_for_req_worker;
            let mut gate_futures_ws_endpoints = gate_futures_ws_endpoints_for_req_worker;
            let mut binance_spot_ws_endpoints = binance_spot_ws_endpoints_for_req_worker;
            let mut binance_um_ws_endpoint_groups = binance_um_ws_endpoint_groups_for_req_worker;
            let binance_um_ws_route = binance_um_ws_route_for_req_worker;
            let binance_um_ws_route_follow_state = binance_um_ws_route_follow_state_for_req_worker;
            let mut ws_rr_cursor = 0usize; // 轮询计数器
            let rest_dispatcher = rest_dispatcher_for_orders;
            let mut order_req_ingress = order_req_ingress;
            let mut order_control_ingress = order_control_ingress;
            let internal_open_terminates = internal_open_terminates_for_req_worker;
            let internal_open_terminate_summary = internal_open_terminate_summary_for_req_worker;
            let mut idle_spin_count = 0usize;
            let mut last_binance_um_health_log = Instant::now();
            let mut binance_um_fallback_routes = 0usize;
            let mut binance_um_bootstrap_routes = 0usize;
            let mut binance_um_health_routes = 0usize;
            let mut binance_um_endpoint_routes: Vec<usize> = Vec::new();

            loop {
                if shutdown_for_req_worker.is_cancelled() {
                    break;
                }
                let _ = drain_order_control_ingress(
                    &mut order_control_ingress,
                    &internal_open_terminates,
                );
                let Some(msg) = order_req_ingress.try_recv() else {
                    if idle_spin_count < router_idle_spin_iters_for_req_worker {
                        idle_spin_count += 1;
                        std::hint::spin_loop();
                    } else {
                        idle_spin_count = 0;
                        tokio::task::yield_now().await;
                    }
                    continue;
                };
                idle_spin_count = 0;
                let _ = drain_order_control_ingress(
                    &mut order_control_ingress,
                    &internal_open_terminates,
                );
                debug!(
                    "routing request: type={:?}, client_order_id={}",
                    msg.req_type, msg.client_order_id
                );

                if msg.req_type.is_new_order() {
                    if let Some(state) =
                        take_internal_open_terminate(&internal_open_terminates, msg.client_order_id)
                    {
                        info!(
                            "trade_engine req_worker internal open terminate hit exchange={} req_type={:?} client_order_id={} trigger_ts={}",
                            exchange_for_req_worker,
                            msg.req_type,
                            msg.client_order_id,
                            state.trigger_ts
                        );
                        record_internal_open_terminate_summary(
                            &internal_open_terminate_summary,
                            &msg,
                        );
                        let _ =
                            trade_resp_sink_for_req_worker.send(internal_open_terminated_outcome(
                                msg.req_type,
                                msg.client_order_id,
                                exchange_for_req_worker,
                                state,
                                "req_worker",
                            ));
                        continue;
                    }
                }

                if exchange_for_req_worker == Exchange::Binance
                    && is_binance_spot_fix_trade_request(msg.req_type)
                    && binance_spot_fix_handle.is_some()
                {
                    let handle = binance_spot_fix_handle.as_ref().expect("checked is_some");
                    if handle.is_available() {
                        let client_order_id = msg.client_order_id;
                        match handle.enqueue(msg) {
                            Ok(()) => {
                                debug!(
                                    "routed Binance spot order to FIX client_order_id={}",
                                    client_order_id
                                );
                            }
                            Err(msg) => {
                                let body = serde_json::json!({
                                    "transport": "fix",
                                    "state": "error",
                                    "reason": "Binance Spot FIX command queue closed",
                                    "clientOrderId": msg.client_order_id,
                                })
                                .to_string();
                                let _ = trade_resp_sink_for_req_worker.send(TradeExecOutcome {
                                    req_type: msg.req_type,
                                    client_order_id: msg.client_order_id,
                                    status: 503,
                                    body,
                                    exchange: exchange_for_req_worker,
                                    order_id: 0,
                                    order_status_u8: 0,
                                    order_update_time: 0,
                                    executed_qty: 0.0,
                                    response_price: 0.0,
                                });
                            }
                        }
                    } else {
                        let last_error = handle
                            .last_error()
                            .unwrap_or_else(|| "not logged on".to_string());
                        warn!(
                            "Binance Spot FIX unavailable for client_order_id={} last_error={}",
                            msg.client_order_id, last_error
                        );
                        let body = serde_json::json!({
                            "transport": "fix",
                            "state": "error",
                            "reason": "Binance Spot FIX unavailable",
                            "lastError": last_error,
                            "clientOrderId": msg.client_order_id,
                        })
                        .to_string();
                        let _ = trade_resp_sink_for_req_worker.send(TradeExecOutcome {
                            req_type: msg.req_type,
                            client_order_id: msg.client_order_id,
                            status: 503,
                            body,
                            exchange: exchange_for_req_worker,
                            order_id: 0,
                            order_status_u8: 0,
                            order_update_time: 0,
                            executed_qty: 0.0,
                            response_price: 0.0,
                        });
                    }
                    continue;
                }

                // 根据 mapping 判断是否走 WebSocket；LTP 后端统一从 WS 执行。
                if use_ltp_backend_for_req_worker || TradeTypeMapping::is_websocket(msg.req_type) {
                    let is_binance_um_new = exchange_for_req_worker == Exchange::Binance
                        && msg.req_type == TradeRequestType::BinanceWsNewUMOrder;
                    if is_binance_um_new {
                        if let Some(groups) = binance_um_ws_endpoint_groups.as_mut() {
                            let len = groups.len();
                            if len == 0 {
                                warn!("no Binance UM websocket endpoint groups available");
                                continue;
                            }
                            let select_recent = binance_um_ws_health_for_req_worker
                                .as_ref()
                                .map(|h| h.select_recent())
                                .unwrap_or(3);
                            let inflight_block_threshold_us = binance_um_ws_health_for_req_worker
                                .as_ref()
                                .and_then(|h| h.inflight_create_block_threshold_us());
                            let cancel_inflight_block_threshold_us =
                                binance_um_ws_health_for_req_worker
                                    .as_ref()
                                    .and_then(|h| h.cancel_inflight_block_threshold_us());
                            let inflight_block_pause_ms = binance_um_ws_health_for_req_worker
                                .as_ref()
                                .map(|h| h.pause_ms())
                                .unwrap_or(0);
                            let start = ws_rr_cursor;
                            ws_rr_cursor = (ws_rr_cursor + 1) % len;
                            let candidates: Vec<BinanceUmWsRouteCandidate> = groups
                                .iter()
                                .map(|group| {
                                    let base_available = group.is_available();
                                    let um_available = if matches!(
                                        binance_um_ws_route.route,
                                        BinanceUmWsRouteKind::Rr | BinanceUmWsRouteKind::Follow
                                    ) {
                                        base_available
                                    } else {
                                        group.is_available_for_new_binance_um(
                                            inflight_block_threshold_us,
                                            cancel_inflight_block_threshold_us,
                                            inflight_block_pause_ms,
                                        )
                                    };
                                    let (new_ack_rtt_sum_us, new_ack_rtt_count) = group
                                        .recent_binance_um_new_ack_rtt_sum_count(select_recent);
                                    BinanceUmWsRouteCandidate {
                                        base_available,
                                        um_available,
                                        new_ack_rtt_sum_us,
                                        new_ack_rtt_count,
                                    }
                                })
                                .collect();
                            let fallback: Vec<bool> =
                                groups.iter().map(|group| group.is_fallback()).collect();
                            let follow_snapshot =
                                binance_um_ws_route_follow_state.borrow().snapshot.clone();
                            let route = match binance_um_ws_route.route {
                                BinanceUmWsRouteKind::Rr => {
                                    select_binance_um_ws_rr_route(&candidates, start)
                                }
                                BinanceUmWsRouteKind::Eval => {
                                    select_binance_um_ws_route_with_fallback(
                                        &candidates,
                                        &fallback,
                                        start,
                                        select_recent,
                                    )
                                }
                                BinanceUmWsRouteKind::Follow => {
                                    select_binance_um_ws_follow_group_route(
                                        &candidates,
                                        &fallback,
                                        start,
                                        follow_snapshot.as_ref(),
                                    )
                                }
                            };
                            if last_binance_um_health_log.elapsed() >= Duration::from_secs(60) {
                                if let Some(health) = binance_um_ws_health_for_req_worker.as_ref() {
                                    let snap = health.snapshot();
                                    let mut table = String::from(
                                        "\n+----+----------+-------------+---+----------+\n| ep | selected | new_mean_us | n | pause_ms |\n+----+----------+-------------+---+----------+",
                                    );
                                    for (idx, group) in groups.iter().enumerate() {
                                        let selected = binance_um_endpoint_routes
                                            .get(idx)
                                            .copied()
                                            .unwrap_or(0);
                                        let (mean_us, recent_count, pause_ms_left) =
                                            group.binance_um_health_stats(select_recent);
                                        table.push_str(&format!(
                                            "\n| {:>2} | {:>8} | {:>11} | {:>1} | {:>8} |",
                                            idx, selected, mean_us, recent_count, pause_ms_left
                                        ));
                                    }
                                    table.push_str(
                                        "\n+----+----------+-------------+---+----------+",
                                    );
                                    info!(
                                        "binance UM WS sched fallback={} bootstrap={} health={} p{} new_threshold_us={:?} new_latest_us={:?} new_n={} new_p50_us={} new_p90_us={} new_p99_us={} new_max_us={} cancel_threshold_us={:?} cancel_latest_us={:?} cancel_n={} cancel_p50_us={} cancel_p90_us={} cancel_p99_us={} cancel_max_us={} new_inflight_block_threshold_us={:?} cancel_inflight_block_threshold_us={:?}{}",
                                        binance_um_fallback_routes,
                                        binance_um_bootstrap_routes,
                                        binance_um_health_routes,
                                        snap.percentile,
                                        snap.new_threshold_us,
                                        snap.latest_new_us,
                                        snap.new_summary.n,
                                        snap.new_summary.p50_us,
                                        snap.new_summary.p90_us,
                                        snap.new_summary.p99_us,
                                        snap.new_summary.max_us,
                                        snap.cancel_threshold_us,
                                        snap.latest_cancel_us,
                                        snap.cancel_summary.n,
                                        snap.cancel_summary.p50_us,
                                        snap.cancel_summary.p90_us,
                                        snap.cancel_summary.p99_us,
                                        snap.cancel_summary.max_us,
                                        inflight_block_threshold_us,
                                        cancel_inflight_block_threshold_us,
                                        table
                                    );
                                }
                                last_binance_um_health_log = Instant::now();
                                binance_um_fallback_routes = 0;
                                binance_um_bootstrap_routes = 0;
                                binance_um_health_routes = 0;
                                binance_um_endpoint_routes.clear();
                            }

                            if let Some(idx) = route.idx {
                                let client_order_id = msg.client_order_id;
                                let req_type = msg.req_type;
                                let selected_handle =
                                    groups[idx].enqueue_available(WsCommand::Send(msg));
                                if binance_um_ws_route.route != BinanceUmWsRouteKind::Rr
                                    || selected_handle.is_none()
                                {
                                    let route_candidates = format_binance_um_route_candidates(
                                        &candidates,
                                        Some(&fallback),
                                        select_recent,
                                        route.idx,
                                    );
                                    let route_reason = binance_um_route_reason(route);
                                    info!(
                                        "BinanceUmWsRouteDecision: client_order_id={} route={} route_type=group selected_group={} selected_group_handle={} mode={} reason={} min_actual_new_samples={} new_inflight_block_threshold_us={:?} cancel_inflight_block_threshold_us={:?} pause_ms={} candidates={}",
                                        client_order_id,
                                        binance_um_ws_route.route.as_str(),
                                        idx,
                                        selected_handle
                                            .map(|handle_idx| handle_idx.to_string())
                                            .unwrap_or_else(|| "NA".to_string()),
                                        route.mode.as_str(),
                                        if selected_handle.is_some() {
                                            route_reason
                                        } else {
                                            "enqueue_unavailable"
                                        },
                                        select_recent,
                                        inflight_block_threshold_us,
                                        cancel_inflight_block_threshold_us,
                                        inflight_block_pause_ms,
                                        route_candidates
                                    );
                                }
                                if selected_handle.is_none() {
                                    warn!(
                                        "selected Binance UM websocket endpoint group unavailable at enqueue for client_order_id={}",
                                        client_order_id
                                    );
                                    let body = serde_json::json!({
                                        "transport": "ws",
                                        "state": "error",
                                        "reason": "selected Binance UM websocket endpoint group unavailable at enqueue",
                                        "clientOrderId": client_order_id,
                                    })
                                    .to_string();
                                    let _ = trade_resp_sink_for_req_worker.send(TradeExecOutcome {
                                        req_type,
                                        client_order_id,
                                        status: 503,
                                        body,
                                        exchange: exchange_for_req_worker,
                                        order_id: 0,
                                        order_status_u8: 0,
                                        order_update_time: 0,
                                        executed_qty: 0.0,
                                        response_price: 0.0,
                                    });
                                    continue;
                                }
                                if binance_um_endpoint_routes.len() < len {
                                    binance_um_endpoint_routes.resize(len, 0);
                                }
                                if let Some(count) = binance_um_endpoint_routes.get_mut(idx) {
                                    *count = count.saturating_add(1);
                                }
                                match route.mode {
                                    BinanceUmWsRouteMode::Health => {
                                        binance_um_health_routes =
                                            binance_um_health_routes.saturating_add(1);
                                    }
                                    BinanceUmWsRouteMode::Bootstrap => {
                                        binance_um_bootstrap_routes =
                                            binance_um_bootstrap_routes.saturating_add(1);
                                    }
                                    BinanceUmWsRouteMode::Base => {
                                        binance_um_fallback_routes =
                                            binance_um_fallback_routes.saturating_add(1);
                                    }
                                    BinanceUmWsRouteMode::Rr => {
                                        binance_um_fallback_routes =
                                            binance_um_fallback_routes.saturating_add(1);
                                    }
                                    BinanceUmWsRouteMode::Fallback => {
                                        binance_um_fallback_routes =
                                            binance_um_fallback_routes.saturating_add(1);
                                    }
                                }
                            } else {
                                let route_candidates = format_binance_um_route_candidates(
                                    &candidates,
                                    Some(&fallback),
                                    select_recent,
                                    route.idx,
                                );
                                let route_reason = binance_um_route_reason(route);
                                info!(
                                    "BinanceUmWsRouteDecision: client_order_id={} route={} route_type=group selected_group=NA selected_group_handle=NA mode={} reason={} min_actual_new_samples={} new_inflight_block_threshold_us={:?} cancel_inflight_block_threshold_us={:?} pause_ms={} candidates={}",
                                    msg.client_order_id,
                                    binance_um_ws_route.route.as_str(),
                                    route.mode.as_str(),
                                    route_reason,
                                    select_recent,
                                    inflight_block_threshold_us,
                                    cancel_inflight_block_threshold_us,
                                    inflight_block_pause_ms,
                                    route_candidates
                                );
                                warn!(
                                    "no Binance UM websocket endpoint group eligible by actual new ACK route for client_order_id={}",
                                    msg.client_order_id
                                );
                                let body = serde_json::json!({
                                    "transport": "ws",
                                    "state": "error",
                                    "reason": "no Binance UM websocket endpoint group eligible by actual new ACK route",
                                    "clientOrderId": msg.client_order_id,
                                })
                                .to_string();
                                let _ = trade_resp_sink_for_req_worker.send(TradeExecOutcome {
                                    req_type: msg.req_type,
                                    client_order_id: msg.client_order_id,
                                    status: 503,
                                    body,
                                    exchange: exchange_for_req_worker,
                                    order_id: 0,
                                    order_status_u8: 0,
                                    order_update_time: 0,
                                    executed_qty: 0.0,
                                    response_price: 0.0,
                                });
                            }
                            continue;
                        }
                    }

                    let mut target_endpoints = if use_ltp_backend_for_req_worker {
                        ws_endpoints.as_mut()
                    } else if exchange_for_req_worker == Exchange::Gate
                        && matches!(
                            msg.req_type,
                            TradeRequestType::GateFuturesNewOrder
                                | TradeRequestType::GateFuturesCancelOrder
                        )
                    {
                        gate_futures_ws_endpoints.as_mut()
                    } else if exchange_for_req_worker == Exchange::Binance
                        && matches!(
                            msg.req_type,
                            TradeRequestType::BinanceWsNewMarginOrder
                                | TradeRequestType::BinanceWsCancelMarginOrder
                        )
                    {
                        binance_spot_ws_endpoints.as_mut()
                    } else {
                        ws_endpoints.as_mut()
                    };

                    // 走 WebSocket - 直接轮询分配
                    if let Some(ref mut endpoints) = target_endpoints {
                        let len = endpoints.len();
                        if len == 0 {
                            warn!("no websocket endpoints available");
                            continue;
                        }

                        let start = ws_rr_cursor;
                        ws_rr_cursor = (ws_rr_cursor + 1) % len;

                        let mut target_idx = None;
                        let mut binance_um_route_mode = BinanceUmWsRouteMode::Fallback;
                        let mut inflight_block_threshold_us = None;
                        let mut cancel_inflight_block_threshold_us = None;
                        let mut inflight_block_pause_ms = 0;
                        if is_binance_um_new {
                            let select_recent = binance_um_ws_health_for_req_worker
                                .as_ref()
                                .map(|h| h.select_recent())
                                .unwrap_or(3);
                            inflight_block_threshold_us = binance_um_ws_health_for_req_worker
                                .as_ref()
                                .and_then(|h| h.inflight_create_block_threshold_us());
                            cancel_inflight_block_threshold_us =
                                binance_um_ws_health_for_req_worker
                                    .as_ref()
                                    .and_then(|h| h.cancel_inflight_block_threshold_us());
                            inflight_block_pause_ms = binance_um_ws_health_for_req_worker
                                .as_ref()
                                .map(|h| h.pause_ms())
                                .unwrap_or(0);
                            let candidates: Vec<BinanceUmWsRouteCandidate> = endpoints
                                .iter()
                                .map(|endpoint| {
                                    let base_available = endpoint.is_available();
                                    let um_available = if matches!(
                                        binance_um_ws_route.route,
                                        BinanceUmWsRouteKind::Rr | BinanceUmWsRouteKind::Follow
                                    ) {
                                        base_available
                                    } else {
                                        endpoint.is_available_for_new_binance_um(
                                            inflight_block_threshold_us,
                                            cancel_inflight_block_threshold_us,
                                            inflight_block_pause_ms,
                                        )
                                    };
                                    let (new_ack_rtt_sum_us, new_ack_rtt_count) = endpoint
                                        .recent_binance_um_new_ack_rtt_sum_count(select_recent);
                                    BinanceUmWsRouteCandidate {
                                        base_available,
                                        um_available,
                                        new_ack_rtt_sum_us,
                                        new_ack_rtt_count,
                                    }
                                })
                                .collect();
                            let route = match binance_um_ws_route.route {
                                BinanceUmWsRouteKind::Eval => {
                                    select_binance_um_ws_route(&candidates, start, select_recent)
                                }
                                BinanceUmWsRouteKind::Rr | BinanceUmWsRouteKind::Follow => {
                                    select_binance_um_ws_rr_route(&candidates, start)
                                }
                            };
                            target_idx = route.idx;
                            binance_um_route_mode = route.mode;
                            if binance_um_ws_route.route != BinanceUmWsRouteKind::Rr
                                || route.idx.is_none()
                            {
                                let route_reason = binance_um_route_reason(route);
                                let route_candidates = format_binance_um_route_candidates(
                                    &candidates,
                                    None,
                                    select_recent,
                                    route.idx,
                                );
                                info!(
                                    "BinanceUmWsRouteDecision: client_order_id={} route={} route_type=endpoint selected_endpoint={} mode={} reason={} min_actual_new_samples={} new_inflight_block_threshold_us={:?} cancel_inflight_block_threshold_us={:?} pause_ms={} candidates={}",
                                    msg.client_order_id,
                                    binance_um_ws_route.route.as_str(),
                                    route
                                        .idx
                                        .map(|idx| idx.to_string())
                                        .unwrap_or_else(|| "NA".to_string()),
                                    route.mode.as_str(),
                                    route_reason,
                                    select_recent,
                                    inflight_block_threshold_us,
                                    cancel_inflight_block_threshold_us,
                                    inflight_block_pause_ms,
                                    route_candidates
                                );
                            }
                            if last_binance_um_health_log.elapsed() >= Duration::from_secs(60) {
                                if let Some(health) = binance_um_ws_health_for_req_worker.as_ref() {
                                    let snap = health.snapshot();
                                    let mut table = String::from(
                                        "\n+----+----------+-------------+---+----------+\n| ep | selected | new_mean_us | n | pause_ms |\n+----+----------+-------------+---+----------+",
                                    );
                                    for (idx, endpoint) in endpoints.iter().enumerate() {
                                        let selected = binance_um_endpoint_routes
                                            .get(idx)
                                            .copied()
                                            .unwrap_or(0);
                                        let (mean_us, recent_count, pause_ms_left) =
                                            endpoint.binance_um_health_stats(select_recent);
                                        table.push_str(&format!(
                                            "\n| {:>2} | {:>8} | {:>11} | {:>1} | {:>8} |",
                                            idx, selected, mean_us, recent_count, pause_ms_left
                                        ));
                                    }
                                    table.push_str(
                                        "\n+----+----------+-------------+---+----------+",
                                    );
                                    info!(
                                        "binance UM WS sched fallback={} bootstrap={} health={} p{} new_threshold_us={:?} new_latest_us={:?} new_n={} new_p50_us={} new_p90_us={} new_p99_us={} new_max_us={} cancel_threshold_us={:?} cancel_latest_us={:?} cancel_n={} cancel_p50_us={} cancel_p90_us={} cancel_p99_us={} cancel_max_us={} new_inflight_block_threshold_us={:?} cancel_inflight_block_threshold_us={:?}{}",
                                        binance_um_fallback_routes,
                                        binance_um_bootstrap_routes,
                                        binance_um_health_routes,
                                        snap.percentile,
                                        snap.new_threshold_us,
                                        snap.latest_new_us,
                                        snap.new_summary.n,
                                        snap.new_summary.p50_us,
                                        snap.new_summary.p90_us,
                                        snap.new_summary.p99_us,
                                        snap.new_summary.max_us,
                                        snap.cancel_threshold_us,
                                        snap.latest_cancel_us,
                                        snap.cancel_summary.n,
                                        snap.cancel_summary.p50_us,
                                        snap.cancel_summary.p90_us,
                                        snap.cancel_summary.p99_us,
                                        snap.cancel_summary.max_us,
                                        inflight_block_threshold_us,
                                        cancel_inflight_block_threshold_us,
                                        table
                                    );
                                }
                                last_binance_um_health_log = Instant::now();
                                binance_um_fallback_routes = 0;
                                binance_um_bootstrap_routes = 0;
                                binance_um_health_routes = 0;
                                binance_um_endpoint_routes.clear();
                            }
                        }

                        if target_idx.is_none() && !is_binance_um_new {
                            for offset in 0..len {
                                let idx = (start + offset) % len;
                                debug!(
                                    "routing order client_order_id={} to ws endpoint {}",
                                    msg.client_order_id, idx
                                );
                                let available = if is_binance_um_new {
                                    endpoints[idx].is_available_for_new_binance_um(
                                        inflight_block_threshold_us,
                                        cancel_inflight_block_threshold_us,
                                        inflight_block_pause_ms,
                                    )
                                } else {
                                    endpoints[idx].is_available()
                                };
                                if available {
                                    target_idx = Some(idx);
                                    break;
                                } else if is_binance_um_new {
                                    debug!(
                                        "BinanceUmWsRouteSkip: client_order_id={} endpoint={} reason=blocked",
                                        msg.client_order_id,
                                        idx
                                    );
                                } else {
                                    warn!(
                                        "ws endpoint {} not accepting messages, trying next",
                                        idx
                                    );
                                }
                            }
                        }

                        if let Some(idx) = target_idx {
                            if is_binance_um_new {
                                if binance_um_endpoint_routes.len() < len {
                                    binance_um_endpoint_routes.resize(len, 0);
                                }
                                if let Some(count) = binance_um_endpoint_routes.get_mut(idx) {
                                    *count = count.saturating_add(1);
                                }
                                match binance_um_route_mode {
                                    BinanceUmWsRouteMode::Health => {
                                        binance_um_health_routes =
                                            binance_um_health_routes.saturating_add(1);
                                    }
                                    BinanceUmWsRouteMode::Bootstrap => {
                                        binance_um_bootstrap_routes =
                                            binance_um_bootstrap_routes.saturating_add(1);
                                    }
                                    BinanceUmWsRouteMode::Base => {
                                        binance_um_fallback_routes =
                                            binance_um_fallback_routes.saturating_add(1);
                                    }
                                    BinanceUmWsRouteMode::Rr => {
                                        binance_um_fallback_routes =
                                            binance_um_fallback_routes.saturating_add(1);
                                    }
                                    BinanceUmWsRouteMode::Fallback => {
                                        binance_um_fallback_routes =
                                            binance_um_fallback_routes.saturating_add(1);
                                    }
                                }
                            }
                            endpoints[idx].enqueue_available(WsCommand::Send(msg));
                        } else {
                            let reason = if is_binance_um_new {
                                "no Binance UM websocket endpoint eligible by actual new ACK route"
                            } else {
                                "all websocket endpoints unavailable"
                            };
                            warn!("{} for client_order_id={}", reason, msg.client_order_id);
                            let body = serde_json::json!({
                                "transport": "ws",
                                "state": "error",
                                "reason": reason,
                                "clientOrderId": msg.client_order_id,
                            })
                            .to_string();
                            let _ = trade_resp_sink_for_req_worker.send(TradeExecOutcome {
                                req_type: msg.req_type,
                                client_order_id: msg.client_order_id,
                                status: 503,
                                body,
                                exchange: exchange_for_req_worker,
                                order_id: 0,
                                order_status_u8: 0,
                                order_update_time: 0,
                                executed_qty: 0.0,
                                response_price: 0.0,
                            });
                        }
                    } else {
                        warn!(
                            "request type {:?} requires WebSocket but no WS endpoints available",
                            msg.req_type
                        );
                    }
                } else {
                    // 走 REST
                    if let Some(dispatcher) = &rest_dispatcher {
                        let endpoint = TradeTypeMapping::get_endpoint(msg.req_type).to_string();
                        let method = TradeTypeMapping::get_method(msg.req_type).to_string();
                        let weight = TradeTypeMapping::get_weight(msg.req_type);
                        debug!(
                            "dispatch mapping: type={:?} -> {} {} (weight={})",
                            msg.req_type, method, endpoint, weight
                        );

                        let params = match trade_request_rest_pairs(&msg) {
                            Ok(params) => params,
                            Err(err) => {
                                warn!(
                                    "invalid REST trade params: req_type={:?} client_order_id={} err={}",
                                    msg.req_type, msg.client_order_id, err
                                );
                                let _ = trade_resp_sink_for_req_worker.send(TradeExecOutcome {
                                    req_type: msg.req_type,
                                    client_order_id: msg.client_order_id,
                                    status: 400,
                                    body: err.to_string(),
                                    exchange: exchange_for_req_worker,
                                    order_id: 0,
                                    order_status_u8: 0,
                                    order_update_time: 0,
                                    executed_qty: 0.0,
                                    response_price: 0.0,
                                });
                                continue;
                            }
                        };

                        let evt = crate::order_event::OrderRequestEvent {
                            req_type: Some(format!("{:?}", msg.req_type)),
                            endpoint,
                            method,
                            params,
                            weight: Some(weight),
                            account: None,
                            req_id: Some(msg.client_order_id.to_string()),
                            counts_toward_order_limit: TradeTypeMapping::counts_toward_order_limit(
                                msg.req_type,
                            ),
                        };

                        let outcome = {
                            let mut dispatcher = dispatcher.lock().await;
                            dispatcher.dispatch(evt).await
                        };
                        match outcome {
                            Ok(outcome) => {
                                debug!(
                                    "http outcome: status={}, ip={}, body_len={}",
                                    outcome.status,
                                    outcome.ip,
                                    outcome.body.len()
                                );
                                let _ = trade_resp_sink_for_req_worker.send(TradeExecOutcome {
                                    req_type: msg.req_type,
                                    client_order_id: msg.client_order_id,
                                    status: outcome.status,
                                    body: outcome.body,
                                    exchange: exchange_for_req_worker,
                                    order_id: 0,
                                    order_status_u8: 0,
                                    order_update_time: 0,
                                    executed_qty: 0.0,
                                    response_price: 0.0,
                                });
                            }
                            Err(e) => {
                                debug!("http error: {}", e);
                                let _ = trade_resp_sink_for_req_worker.send(TradeExecOutcome {
                                    req_type: msg.req_type,
                                    client_order_id: msg.client_order_id,
                                    status: 0,
                                    body: e.to_string(),
                                    exchange: exchange_for_req_worker,
                                    order_id: 0,
                                    order_status_u8: 0,
                                    order_update_time: 0,
                                    executed_qty: 0.0,
                                    response_price: 0.0,
                                });
                            }
                        }
                    } else {
                        warn!(
                            "request type {:?} requires REST but no REST dispatcher available",
                            msg.req_type
                        );
                    }
                }
            }

            // Shutdown ws clients
            if let Some(ref endpoints) = ws_endpoints {
                for tx in endpoints {
                    let _ = tx.send(WsCommand::Shutdown);
                }
            }
            if let Some(ref endpoints) = binance_spot_ws_endpoints {
                for tx in endpoints {
                    let _ = tx.send(WsCommand::Shutdown);
                }
            }
        });
        worker_handles.push(("req_worker", req_worker));

        // Query request router
        {
            let rest_dispatcher = rest_dispatcher.clone();
            let exchange_copy = exchange;
            let query_resp_sink = query_resp_sink.clone();
            let binance_ws_endpoints = ws_endpoints.clone();
            let binance_spot_ws_endpoints = binance_spot_ws_endpoints.clone();
            let gate_spot_ws_endpoints = ws_endpoints.clone();
            let gate_futures_ws_endpoints = gate_futures_ws_endpoints.clone();
            let use_ltp_backend_for_query_router = use_ltp_backend;
            let shutdown_for_query_router = shutdown.clone();
            let router_idle_spin_iters_for_query_router = router_idle_spin_iters;
            let query_router = tokio::task::spawn_local(async move {
                let ltp_rest = if use_ltp_backend_for_query_router {
                    Some(
                        crate::ltp_rest::LtpRestClient::from_env()
                            .expect("LTP query requires LTP_API_KEY and LTP_API_SECRET"),
                    )
                } else {
                    None
                };
                let okex_http = reqwest::Client::new();
                let okex_creds = account_common::okex_auth::OkexCredentials::from_env().ok();
                let bybit_http = reqwest::Client::new();
                let bybit_creds = account_common::bybit_auth::BybitCredentials::from_env().ok();
                let bitget_http = reqwest::Client::new();
                let bitget_creds = account_common::bitget_auth::BitgetCredentials::from_env().ok();
                let gate_http = reqwest::Client::new();
                let gate_creds = account_common::gate_auth::GateCredentials::from_env().ok();
                let mut binance_query_rr = 0usize;
                let mut gate_query_rr = 0usize;
                let mut gate_futures_query_rr = 0usize;
                let mut okex_query_rate_limiter = OkexQueryRateLimiter::default();
                let mut bitget_query_rate_limiter = BitgetQueryRateLimiter::default();
                let mut query_req_ingress = query_req_ingress;
                let mut idle_spin_count = 0usize;

                'query_router: loop {
                    if shutdown_for_query_router.is_cancelled() {
                        break;
                    }
                    let Some(msg) = query_req_ingress.try_recv() else {
                        if idle_spin_count < router_idle_spin_iters_for_query_router {
                            idle_spin_count += 1;
                            std::hint::spin_loop();
                        } else {
                            idle_spin_count = 0;
                            tokio::task::yield_now().await;
                        }
                        continue;
                    };
                    idle_spin_count = 0;
                    debug!(
                        "routing query: type={:?} client_query_id={}",
                        msg.req_type, msg.client_query_id
                    );

                    if use_ltp_backend_for_query_router {
                        let Some(ltp_rest) = ltp_rest.as_ref() else {
                            let _ = query_resp_sink.send(QueryExecOutcome {
                                req_type: msg.req_type,
                                client_query_id: msg.client_query_id,
                                status: 503,
                                body: bytes::Bytes::from_static(b"LTP REST client unavailable"),
                                exchange: exchange_copy,
                                ip_used_weight_1m: None,
                                query_count_1m: None,
                            });
                            continue;
                        };
                        for outcome in ltp_rest.query_snapshot(&msg, exchange_copy).await {
                            let _ = query_resp_sink.send(outcome);
                        }
                        continue;
                    }

                    match exchange_copy {
                        Exchange::Binance => {
                            if msg.req_type == QueryRequestType::BinanceWsUMQuery
                                || msg.req_type == QueryRequestType::BinanceWsMarginQuery
                            {
                                let target_endpoints =
                                    if msg.req_type == QueryRequestType::BinanceWsMarginQuery {
                                        binance_spot_ws_endpoints.as_ref()
                                    } else {
                                        binance_ws_endpoints.as_ref()
                                    };

                                let Some(endpoints) = target_endpoints else {
                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 503,
                                        body: bytes::Bytes::from_static(
                                            b"no binance ws endpoints available",
                                        ),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                    continue;
                                };
                                if endpoints.is_empty() {
                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 503,
                                        body: bytes::Bytes::from_static(
                                            b"no binance ws endpoints available",
                                        ),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                    continue;
                                }

                                let len = endpoints.len();
                                let start = binance_query_rr;
                                binance_query_rr = (binance_query_rr + 1) % len;

                                let mut target_idx = None;
                                for offset in 0..len {
                                    let idx = (start + offset) % len;
                                    if endpoints[idx].is_available() {
                                        target_idx = Some(idx);
                                        break;
                                    }
                                }

                                if let Some(idx) = target_idx {
                                    endpoints[idx].enqueue_available(WsCommand::SendQuery(msg));
                                } else {
                                    warn!(
                                        "binance ws query endpoints unavailable req_type={:?} client_query_id={}",
                                        msg.req_type, msg.client_query_id
                                    );
                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 503,
                                        body: bytes::Bytes::from_static(
                                            b"binance ws endpoints unavailable",
                                        ),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                                continue;
                            }

                            if !QueryTypeMapping::is_binance_rest(msg.req_type) {
                                let _ = query_resp_sink.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 400,
                                    body: bytes::Bytes::from_static(
                                        b"unsupported query type for binance engine",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            }
                            let Some(dispatcher) = &rest_dispatcher else {
                                let _ = query_resp_sink.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 503,
                                    body: bytes::Bytes::from_static(
                                        b"no rest dispatcher available",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            };

                            let endpoint = QueryTypeMapping::get_endpoint(msg.req_type).to_string();
                            let method = QueryTypeMapping::get_method(msg.req_type).to_string();
                            let weight = QueryTypeMapping::get_weight(msg.req_type);
                            let params = match query_request_rest_pairs(&msg) {
                                Ok(params) => params,
                                Err(err) => {
                                    warn!(
                                        "invalid REST query params: req_type={:?} client_query_id={} err={}",
                                        msg.req_type, msg.client_query_id, err
                                    );
                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 400,
                                        body: bytes::Bytes::from(err.to_string()),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                    continue;
                                }
                            };

                            let evt = crate::order_event::OrderRequestEvent {
                                req_type: Some(format!("{:?}", msg.req_type)),
                                endpoint,
                                method,
                                params,
                                weight: Some(weight),
                                account: None,
                                req_id: Some(msg.client_query_id.to_string()),
                                counts_toward_order_limit: false,
                            };

                            let outcome = {
                                let mut dispatcher = dispatcher.lock().await;
                                dispatcher.dispatch(evt).await
                            };
                            match outcome {
                                Ok(outcome) => {
                                    match msg.req_type {
                                        crate::query_request::QueryRequestType::BinanceUMQuery
                                            if outcome.status == 200 =>
                                        {
                                            if let Some(v) = parse_binance_um_order_query_json(&outcome.body) {
                                                let _ = query_resp_sink.send(QueryExecOutcome {
                                                    req_type: msg.req_type,
                                                    client_query_id: msg.client_query_id,
                                                    status: outcome.status,
                                                    body: v.to_bytes(),
                                                    exchange: exchange_copy,
                                                    ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                    query_count_1m: outcome.order_count_1m,
                                                });
                                            } else {
                                                warn!(
                                                    "binance um order query parse failed: client_query_id={} body_len={}",
                                                    msg.client_query_id,
                                                    outcome.body.len()
                                                );
                                                let _ = query_resp_sink.send(QueryExecOutcome {
                                                    req_type: msg.req_type,
                                                    client_query_id: msg.client_query_id,
                                                    status: outcome.status,
                                                    body: bytes::Bytes::from_static(b"E"),
                                                    exchange: exchange_copy,
                                                    ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                    query_count_1m: outcome.order_count_1m,
                                                });
                                            }
                                        }
                                        crate::query_request::QueryRequestType::BinanceMarginQuery
                                            if outcome.status == 200 =>
                                        {
                                            if let Some(v) = parse_binance_margin_order_query_json(&outcome.body) {
                                                let _ = query_resp_sink.send(QueryExecOutcome {
                                                    req_type: msg.req_type,
                                                    client_query_id: msg.client_query_id,
                                                    status: outcome.status,
                                                    body: v.to_bytes(),
                                                    exchange: exchange_copy,
                                                    ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                    query_count_1m: outcome.order_count_1m,
                                                });
                                            } else {
                                                warn!(
                                                    "binance margin order query parse failed: client_query_id={} body_len={}",
                                                    msg.client_query_id,
                                                    outcome.body.len()
                                                );
                                                let _ = query_resp_sink.send(QueryExecOutcome {
                                                    req_type: msg.req_type,
                                                    client_query_id: msg.client_query_id,
                                                    status: outcome.status,
                                                    body: bytes::Bytes::from_static(b"E"),
                                                    exchange: exchange_copy,
                                                    ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                    query_count_1m: outcome.order_count_1m,
                                                });
                                            }
                                        }
                                        crate::query_request::QueryRequestType::BinancePmBalanceSnapshot
                                            if outcome.status == 200 =>
                                        {
                                            if let Some(msgs) = parse_binance_pm_balance_snapshot(&outcome.body) {
                                                for payload in msgs {
                                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                                        req_type: msg.req_type,
                                                        client_query_id: msg.client_query_id,
                                                        status: outcome.status,
                                                        body: payload,
                                                        exchange: exchange_copy,
                                                        ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                        query_count_1m: outcome.order_count_1m,
                                                    });
                                                }
                                            }
                                        }
                                        crate::query_request::QueryRequestType::BinanceUmBalanceSnapshotStd
                                            if outcome.status == 200 =>
                                        {
                                            if let Some(msgs) =
                                                parse_binance_um_balance_snapshot_std(&outcome.body)
                                            {
                                                for payload in msgs {
                                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                                        req_type: msg.req_type,
                                                        client_query_id: msg.client_query_id,
                                                        status: outcome.status,
                                                        body: payload,
                                                        exchange: exchange_copy,
                                                        ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                        query_count_1m: outcome.order_count_1m,
                                                    });
                                                }
                                            }
                                        }
                                        crate::query_request::QueryRequestType::BinanceUmAccountSnapshot
                                            if outcome.status == 200 =>
                                        {
                                            if let Some(msgs) = parse_binance_um_account_snapshot(&outcome.body) {
                                                if msgs.is_empty() {
                                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                                        req_type: msg.req_type,
                                                        client_query_id: msg.client_query_id,
                                                        status: outcome.status,
                                                        body: bytes::Bytes::new(),
                                                        exchange: exchange_copy,
                                                        ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                        query_count_1m: outcome.order_count_1m,
                                                    });
                                                } else {
                                                    for payload in msgs {
                                                        let _ = query_resp_sink.send(QueryExecOutcome {
                                                            req_type: msg.req_type,
                                                            client_query_id: msg.client_query_id,
                                                            status: outcome.status,
                                                            body: payload,
                                                            exchange: exchange_copy,
                                                            ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                            query_count_1m: outcome.order_count_1m,
                                                        });
                                                    }
                                                }
                                            }
                                        }
                                        crate::query_request::QueryRequestType::BinanceUmAccountSnapshotStd
                                            if outcome.status == 200 =>
                                        {
                                            if let Some(msgs) = parse_binance_um_account_snapshot(&outcome.body) {
                                                if msgs.is_empty() {
                                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                                        req_type: msg.req_type,
                                                        client_query_id: msg.client_query_id,
                                                        status: outcome.status,
                                                        body: bytes::Bytes::new(),
                                                        exchange: exchange_copy,
                                                        ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                        query_count_1m: outcome.order_count_1m,
                                                    });
                                                } else {
                                                    for payload in msgs {
                                                        let _ = query_resp_sink.send(QueryExecOutcome {
                                                            req_type: msg.req_type,
                                                            client_query_id: msg.client_query_id,
                                                            status: outcome.status,
                                                            body: payload,
                                                            exchange: exchange_copy,
                                                            ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                            query_count_1m: outcome.order_count_1m,
                                                        });
                                                    }
                                                }
                                            }
                                        }
                                        crate::query_request::QueryRequestType::BinanceSpotAccountSnapshotStd
                                            if outcome.status == 200 =>
                                        {
                                            if let Some(msgs) =
                                                parse_binance_spot_account_snapshot_std(&outcome.body)
                                            {
                                                for payload in msgs {
                                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                                        req_type: msg.req_type,
                                                        client_query_id: msg.client_query_id,
                                                        status: outcome.status,
                                                        body: payload,
                                                        exchange: exchange_copy,
                                                        ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                        query_count_1m: outcome.order_count_1m,
                                                    });
                                                }
                                            }
                                        }
                                        _ => {
                                            let _ = query_resp_sink.send(QueryExecOutcome {
                                                req_type: msg.req_type,
                                                client_query_id: msg.client_query_id,
                                                status: outcome.status,
                                                body: bytes::Bytes::from(outcome.body),
                                                exchange: exchange_copy,
                                                ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                query_count_1m: outcome.order_count_1m,
                                            });
                                        }
                                    }
                                }
                                        Err(_e) => {
                                            let _ = query_resp_sink.send(QueryExecOutcome {
                                                req_type: msg.req_type,
                                                client_query_id: msg.client_query_id,
                                                status: 0,
                                        body: bytes::Bytes::from_static(b"E"),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                            }
                        }
                        Exchange::Okex => {
                            if !QueryTypeMapping::is_okex_rest(msg.req_type) {
                                let _ = query_resp_sink.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 400,
                                    body: bytes::Bytes::from_static(
                                        b"unsupported query type for okex engine",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            }
                            let Some(creds) = &okex_creds else {
                                let _ = query_resp_sink.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 401,
                                    body: bytes::Bytes::from_static(
                                        b"missing OKX credentials in env",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            };

                            let endpoint = QueryTypeMapping::get_endpoint(msg.req_type);
                            let qs = std::str::from_utf8(&msg.params).unwrap_or("");
                            let path_with_query = if qs.is_empty() {
                                endpoint.to_string()
                            } else {
                                format!("{}?{}", endpoint, qs)
                            };

                            while let Some(block) = okex_query_rate_limiter
                                .should_block(msg.req_type, std::time::Instant::now())
                            {
                                warn!(
                                    "okex query rate limited: req_type={:?} client_query_id={} wait_ms={} queued_in_window={} limit={} window_ms={}",
                                    msg.req_type,
                                    msg.client_query_id,
                                    block.wait_for.as_millis(),
                                    block.queued_in_window,
                                    block.max_requests,
                                    block.window.as_millis()
                                );
                                tokio::select! {
                                    biased;
                                    _ = shutdown_for_query_router.cancelled() => break 'query_router,
                                    _ = tokio::time::sleep(block.wait_for) => {}
                                }
                            }
                            if let Some(snapshot) = okex_query_rate_limiter
                                .record(msg.req_type, std::time::Instant::now())
                            {
                                debug!(
                                    "okex query rate recorded: req_type={:?} client_query_id={} count_in_window={} limit={} window_ms={}",
                                    msg.req_type,
                                    msg.client_query_id,
                                    snapshot.queued_in_window,
                                    snapshot.max_requests,
                                    snapshot.window.as_millis()
                                );
                            }

                            match crate::okex_query::okex_rest_get(
                                &okex_http,
                                creds,
                                &path_with_query,
                            )
                            .await
                            {
                                Ok((status, body)) => {
                                    let body_bytes = match msg.req_type {
                                        crate::query_request::QueryRequestType::OkexMarginQuery
                                            | crate::query_request::QueryRequestType::OkexUMQuery
                                            if status == 200 =>
                                        {
                                            match parse_okex_order_query_json(&body) {
                                                OkexOrderQueryParseResult::Success(v) => {
                                                    v.to_bytes()
                                                }
                                                OkexOrderQueryParseResult::Error {
                                                    kind: OkexOrderQueryParseErrorKind::OrderNotFound,
                                                    ..
                                                } => bytes::Bytes::from_static(
                                                    ORDER_QUERY_NOT_FOUND_MARKER,
                                                ),
                                                OkexOrderQueryParseResult::Error {
                                                    kind: OkexOrderQueryParseErrorKind::Other,
                                                    code: okx_code,
                                                    msg: okx_msg,
                                                } => {
                                                    const QUERY_RESP_HEADER_LEN: usize = 4 + 8;
                                                    let max_body_len = QUERY_RESP_PAYLOAD
                                                        .saturating_sub(QUERY_RESP_HEADER_LEN);
                                                    warn!(
                                                        "okex order query parse failed: client_query_id={} http_status={} okx_code={} okx_msg={} body_len={} max_body_len={}",
                                                        msg.client_query_id,
                                                        status,
                                                        okx_code,
                                                        okx_msg,
                                                        body.len(),
                                                        max_body_len
                                                    );
                                                    bytes::Bytes::from_static(b"E")
                                                }
                                            }
                                        }
                                        crate::query_request::QueryRequestType::OkexAccountBalanceSnapshot
                                            if status == 200 =>
                                        {
                                            if let Some(msgs) =
                                                parse_okex_account_balance_snapshot(&body)
                                            {
                                                if !msgs.is_empty() {
                                                    for payload in msgs {
                                                        let _ = query_resp_sink.send(QueryExecOutcome {
                                                            req_type: msg.req_type,
                                                            client_query_id: msg.client_query_id,
                                                            status,
                                                            body: payload,
                                                            exchange: exchange_copy,
                                                            ip_used_weight_1m: None,
                                                            query_count_1m: None,
                                                        });
                                                    }
                                                    continue;
                                                }
                                            }
                                            warn!("okex account balance snapshot parse produced no basic msgs; skipping response body");
                                            bytes::Bytes::new()
                                        }
                                        crate::query_request::QueryRequestType::OkexPositionsSnapshot
                                            if status == 200 =>
                                        {
                                            if let Some(msgs) = parse_okex_positions_snapshot(&body) {
                                                if !msgs.is_empty() {
                                                    for payload in msgs {
                                                        let _ = query_resp_sink.send(QueryExecOutcome {
                                                            req_type: msg.req_type,
                                                            client_query_id: msg.client_query_id,
                                                            status,
                                                            body: payload,
                                                            exchange: exchange_copy,
                                                            ip_used_weight_1m: None,
                                                            query_count_1m: None,
                                                        });
                                                    }
                                                    continue;
                                                }
                                            }
                                            warn!("okex positions snapshot parse produced no basic msgs; skipping response body");
                                            bytes::Bytes::new()
                                        }
                                        _ => bytes::Bytes::from(body),
                                    };
                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status,
                                        body: body_bytes,
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                                Err(e) => {
                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 0,
                                        body: bytes::Bytes::from(e.to_string()),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                            }
                        }
                        Exchange::Gate => {
                            if matches!(
                                msg.req_type,
                                crate::query_request::QueryRequestType::GateUnifiedOrderQuery
                                    | crate::query_request::QueryRequestType::GateFuturesOrderQuery
                            ) {
                                let target_endpoints = if matches!(
                                    msg.req_type,
                                    crate::query_request::QueryRequestType::GateFuturesOrderQuery
                                ) {
                                    gate_futures_ws_endpoints.as_ref()
                                } else {
                                    gate_spot_ws_endpoints.as_ref()
                                };

                                let Some(endpoints) = target_endpoints else {
                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 503,
                                        body: bytes::Bytes::from_static(
                                            b"no gate ws endpoints available",
                                        ),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                    continue;
                                };
                                if endpoints.is_empty() {
                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 503,
                                        body: bytes::Bytes::from_static(
                                            b"no gate ws endpoints available",
                                        ),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                    continue;
                                }

                                let (cursor, len) = if matches!(
                                    msg.req_type,
                                    crate::query_request::QueryRequestType::GateFuturesOrderQuery
                                ) {
                                    let len = endpoints.len();
                                    let start = gate_futures_query_rr;
                                    gate_futures_query_rr = (gate_futures_query_rr + 1) % len;
                                    (start, len)
                                } else {
                                    let len = endpoints.len();
                                    let start = gate_query_rr;
                                    gate_query_rr = (gate_query_rr + 1) % len;
                                    (start, len)
                                };

                                let mut target_idx = None;
                                for offset in 0..len {
                                    let idx = (cursor + offset) % len;
                                    if endpoints[idx].is_available() {
                                        target_idx = Some(idx);
                                        break;
                                    }
                                }

                                if let Some(idx) = target_idx {
                                    endpoints[idx].enqueue_available(WsCommand::SendQuery(msg));
                                } else {
                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 503,
                                        body: bytes::Bytes::from_static(
                                            b"gate ws query dispatch failed",
                                        ),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                                continue;
                            }

                            if !QueryTypeMapping::is_gate_rest(msg.req_type) {
                                let _ = query_resp_sink.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 400,
                                    body: bytes::Bytes::from_static(
                                        b"unsupported query type for gate engine",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            }
                            let Some(creds) = &gate_creds else {
                                let _ = query_resp_sink.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 401,
                                    body: bytes::Bytes::from_static(
                                        b"missing Gate credentials in env",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            };

                            let endpoint = QueryTypeMapping::get_endpoint(msg.req_type);
                            let qs = std::str::from_utf8(&msg.params).unwrap_or("");

                            let extra_headers = if matches!(
                                msg.req_type,
                                crate::query_request::QueryRequestType::GateUnifiedPositionsSnapshot
                            ) {
                                &[("X-Gate-Size-Decimal", "1")][..]
                            } else {
                                &[][..]
                            };
                            match crate::gate_query::gate_rest_get_with_headers(
                                &gate_http,
                                creds,
                                endpoint,
                                qs,
                                extra_headers,
                            )
                            .await
                            {
                                Ok((status, body)) => {
                                    let body_bytes = match msg.req_type {
                                        crate::query_request::QueryRequestType::GateUnifiedBalanceSnapshot
                                            if status == 200 =>
                                        {
                                            if let Some(msgs) =
                                                parse_gate_unified_balance_snapshot(&body)
                                            {
                                                if !msgs.is_empty() {
                                                    for payload in msgs {
                                                        let _ = query_resp_sink.send(QueryExecOutcome {
                                                            req_type: msg.req_type,
                                                            client_query_id: msg.client_query_id,
                                                            status,
                                                            body: payload,
                                                            exchange: exchange_copy,
                                                            ip_used_weight_1m: None,
                                                            query_count_1m: None,
                                                        });
                                                    }
                                                    continue;
                                                }
                                            }
                                            warn!("gate unified balance snapshot parse produced no basic msgs; skipping response body");
                                            bytes::Bytes::new()
                                        }
                                        crate::query_request::QueryRequestType::GateUnifiedPositionsSnapshot
                                            if status == 200 =>
                                        {
                                            if let Some(parsed) =
                                                parse_gate_positions_snapshot_with_meta(&body)
                                            {
                                                if !parsed.msgs.is_empty() {
                                                    for payload in parsed.msgs {
                                                        let _ = query_resp_sink.send(QueryExecOutcome {
                                                            req_type: msg.req_type,
                                                            client_query_id: msg.client_query_id,
                                                            status,
                                                            body: payload,
                                                            exchange: exchange_copy,
                                                            ip_used_weight_1m: None,
                                                            query_count_1m: None,
                                                        });
                                                    }
                                                    continue;
                                                }
                                                let no_positions = parsed.rows_total == 0
                                                    || (parsed.rows_with_inst > 0
                                                        && parsed.rows_with_nonzero_size == 0
                                                        && parsed.rows_with_pnl == 0);
                                                if no_positions {
                                                    info!(
                                                        "gate positions snapshot empty; rows_total={}, rows_with_inst={}, rows_nonzero_size={}, rows_with_pnl={}",
                                                        parsed.rows_total,
                                                        parsed.rows_with_inst,
                                                        parsed.rows_with_nonzero_size,
                                                        parsed.rows_with_pnl
                                                    );
                                                    bytes::Bytes::new()
                                                } else {
                                                    warn!(
                                                        "gate positions snapshot parse produced no basic msgs; rows_total={}, rows_with_inst={}, rows_nonzero_size={}, rows_with_pnl={}",
                                                        parsed.rows_total,
                                                        parsed.rows_with_inst,
                                                        parsed.rows_with_nonzero_size,
                                                        parsed.rows_with_pnl
                                                    );
                                                    bytes::Bytes::from_static(b"E")
                                                }
                                            } else {
                                                warn!(
                                                    "gate positions snapshot parse failed; body_len={}",
                                                    body.len()
                                                );
                                                bytes::Bytes::from_static(b"E")
                                            }
                                        }
                                        _ => bytes::Bytes::from(body),
                                    };
                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status,
                                        body: body_bytes,
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                                Err(e) => {
                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 0,
                                        body: bytes::Bytes::from(e.to_string()),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                            }
                        }
                        Exchange::Bybit => {
                            if !QueryTypeMapping::is_bybit_rest(msg.req_type) {
                                let _ = query_resp_sink.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 400,
                                    body: bytes::Bytes::from_static(
                                        b"unsupported query type for bybit engine",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            }
                            let Some(creds) = &bybit_creds else {
                                let _ = query_resp_sink.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 401,
                                    body: bytes::Bytes::from_static(
                                        b"missing Bybit credentials in env",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            };

                            let endpoint = QueryTypeMapping::get_endpoint(msg.req_type);
                            let qs = std::str::from_utf8(&msg.params).unwrap_or("");
                            let is_bybit_snapshot = matches!(
                                msg.req_type,
                                crate::query_request::QueryRequestType::BybitAccountBalanceSnapshot
                                    | crate::query_request::QueryRequestType::BybitPositionsSnapshot
                            );
                            if !is_bybit_snapshot {
                                debug!(
                                    "trade_engine bybit query start req_type={:?} client_query_id={} endpoint={} qs={}",
                                    msg.req_type, msg.client_query_id, endpoint, qs
                                );
                            }

                            if matches!(
                                msg.req_type,
                                crate::query_request::QueryRequestType::BybitPositionsSnapshot
                            ) {
                                match crate::bybit_query::bybit_rest_get_position_list_pages(
                                    &bybit_http,
                                    creds,
                                    endpoint,
                                    qs,
                                )
                                .await
                                {
                                    Ok(pages) => {
                                        let page_refs: Vec<&str> =
                                            pages.iter().map(String::as_str).collect();
                                        if let Some(msgs) =
                                            parse_bybit_positions_snapshot_pages(page_refs)
                                        {
                                            if msgs.is_empty() {
                                                debug!(
                                                    "trade_engine bybit positions snapshot returned empty list req_type={:?} client_query_id={} pages={}",
                                                    msg.req_type,
                                                    msg.client_query_id,
                                                    pages.len()
                                                );
                                                let _ = query_resp_sink.send(QueryExecOutcome {
                                                    req_type: msg.req_type,
                                                    client_query_id: msg.client_query_id,
                                                    status: 200,
                                                    body: bytes::Bytes::new(),
                                                    exchange: exchange_copy,
                                                    ip_used_weight_1m: None,
                                                    query_count_1m: None,
                                                });
                                            } else {
                                                for payload in msgs {
                                                    let _ =
                                                        query_resp_sink.send(QueryExecOutcome {
                                                            req_type: msg.req_type,
                                                            client_query_id: msg.client_query_id,
                                                            status: 200,
                                                            body: payload,
                                                            exchange: exchange_copy,
                                                            ip_used_weight_1m: None,
                                                            query_count_1m: None,
                                                        });
                                                }
                                            }
                                        } else {
                                            warn!(
                                                "trade_engine bybit positions snapshot parse failed req_type={:?} client_query_id={} pages={}",
                                                msg.req_type,
                                                msg.client_query_id,
                                                pages.len()
                                            );
                                            let _ = query_resp_sink.send(QueryExecOutcome {
                                                req_type: msg.req_type,
                                                client_query_id: msg.client_query_id,
                                                status: 200,
                                                body: bytes::Bytes::from_static(b"E"),
                                                exchange: exchange_copy,
                                                ip_used_weight_1m: None,
                                                query_count_1m: None,
                                            });
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            "trade_engine bybit positions snapshot failed req_type={:?} client_query_id={} endpoint={} qs={} err={:#}",
                                            msg.req_type,
                                            msg.client_query_id,
                                            endpoint,
                                            qs,
                                            e
                                        );
                                        let _ = query_resp_sink.send(QueryExecOutcome {
                                            req_type: msg.req_type,
                                            client_query_id: msg.client_query_id,
                                            status: 0,
                                            body: bytes::Bytes::from(e.to_string()),
                                            exchange: exchange_copy,
                                            ip_used_weight_1m: None,
                                            query_count_1m: None,
                                        });
                                    }
                                }
                                continue;
                            }

                            match crate::bybit_query::bybit_rest_get(
                                &bybit_http,
                                creds,
                                endpoint,
                                qs,
                            )
                            .await
                            {
                                Ok((status, body)) => {
                                    let bybit_summary = summarize_bybit_response(&body);
                                    if !is_bybit_snapshot {
                                        debug!(
                                            "trade_engine bybit query response req_type={:?} client_query_id={} status={} {}",
                                            msg.req_type,
                                            msg.client_query_id,
                                            status,
                                            bybit_summary
                                        );
                                    }
                                    let body_bytes = match msg.req_type {
                                        crate::query_request::QueryRequestType::BybitMarginQuery
                                        | crate::query_request::QueryRequestType::BybitUMQuery
                                            if status == 200 =>
                                        {
                                            match parse_bybit_order_query_json(&body) {
                                                BybitOrderQueryParseResult::Success(v) => v.to_bytes(),
                                                BybitOrderQueryParseResult::Error {
                                                    kind: BybitOrderQueryParseErrorKind::OrderNotFound,
                                                    ..
                                                } => bytes::Bytes::from_static(ORDER_QUERY_NOT_FOUND_MARKER),
                                                BybitOrderQueryParseResult::Error { .. } => {
                                                    bytes::Bytes::from_static(b"E")
                                                }
                                            }
                                        }
                                        crate::query_request::QueryRequestType::BybitAccountBalanceSnapshot
                                            if status == 200 =>
                                        {
                                            if let Some(msgs) =
                                                parse_bybit_account_balance_snapshot(&body)
                                            {
                                                if !msgs.is_empty() {
                                                    for payload in msgs {
                                                        let _ = query_resp_sink.send(QueryExecOutcome {
                                                            req_type: msg.req_type,
                                                            client_query_id: msg.client_query_id,
                                                            status,
                                                            body: payload,
                                                            exchange: exchange_copy,
                                                            ip_used_weight_1m: None,
                                                            query_count_1m: None,
                                                        });
                                                    }
                                                    continue;
                                                }
                                            }
                                            warn!(
                                                "trade_engine bybit balance snapshot parse produced no basic msgs req_type={:?} client_query_id={} status={} {} body={}",
                                                msg.req_type,
                                                msg.client_query_id,
                                                status,
                                                bybit_summary,
                                                truncate_for_log(&body, 512)
                                            );
                                            bytes::Bytes::new()
                                        }
                                        _ => bytes::Bytes::from(body),
                                    };

                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status,
                                        body: body_bytes,
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                                Err(e) => {
                                    warn!(
                                        "trade_engine bybit query failed req_type={:?} client_query_id={} endpoint={} qs={} err={:#}",
                                        msg.req_type,
                                        msg.client_query_id,
                                        endpoint,
                                        qs,
                                        e
                                    );
                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 0,
                                        body: bytes::Bytes::from(e.to_string()),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                            }
                        }
                        Exchange::Bitget => {
                            if !QueryTypeMapping::is_bitget_rest(msg.req_type) {
                                let _ = query_resp_sink.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 400,
                                    body: bytes::Bytes::from_static(
                                        b"unsupported query type for bitget engine",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            }
                            let Some(creds) = &bitget_creds else {
                                let _ = query_resp_sink.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 401,
                                    body: bytes::Bytes::from_static(
                                        b"missing Bitget credentials in env",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            };

                            let now = std::time::Instant::now();
                            if let Some(block) =
                                bitget_query_rate_limiter.should_block(msg.req_type, now)
                            {
                                warn!(
                                    "bitget query rate-limited: req_type={:?} client_query_id={} queued_in_window={} max_requests={} window_ms={} wait_ms={}",
                                    msg.req_type,
                                    msg.client_query_id,
                                    block.queued_in_window,
                                    block.max_requests,
                                    block.window.as_millis(),
                                    block.wait_for.as_millis()
                                );
                                tokio::time::sleep(block.wait_for).await;
                            }
                            let _ = bitget_query_rate_limiter
                                .record(msg.req_type, std::time::Instant::now());

                            let endpoint = QueryTypeMapping::get_endpoint(msg.req_type);
                            let qs = std::str::from_utf8(&msg.params).unwrap_or("");
                            match crate::bitget_query::bitget_rest_get(
                                &bitget_http,
                                creds,
                                endpoint,
                                qs,
                            )
                            .await
                            {
                                Ok((status, body)) => {
                                    let body_bytes = match msg.req_type {
                                        crate::query_request::QueryRequestType::BitgetMarginQuery
                                        | crate::query_request::QueryRequestType::BitgetUMQuery
                                            if status == 200 =>
                                        {
                                            match parse_bitget_order_query_json(&body) {
                                                BitgetOrderQueryParseResult::Success(v) => v.to_bytes(),
                                                BitgetOrderQueryParseResult::Error {
                                                    kind: BitgetOrderQueryParseErrorKind::OrderNotFound,
                                                    ..
                                                } => bytes::Bytes::from_static(ORDER_QUERY_NOT_FOUND_MARKER),
                                                BitgetOrderQueryParseResult::Error { .. } => {
                                                    bytes::Bytes::from_static(b"E")
                                                }
                                            }
                                        }
                                        crate::query_request::QueryRequestType::BitgetAccountBalanceSnapshot
                                            if status == 200 =>
                                        {
                                            if let Some(msgs) =
                                                parse_bitget_account_balance_snapshot(&body)
                                            {
                                                if !msgs.is_empty() {
                                                    for payload in msgs {
                                                        let _ = query_resp_sink.send(QueryExecOutcome {
                                                            req_type: msg.req_type,
                                                            client_query_id: msg.client_query_id,
                                                            status,
                                                            body: payload,
                                                            exchange: exchange_copy,
                                                            ip_used_weight_1m: None,
                                                            query_count_1m: None,
                                                        });
                                                    }
                                                    continue;
                                                }
                                            }
                                            warn!(
                                                "bitget account balance snapshot parse produced no basic msgs; body={}",
                                                truncate_for_log(&body, 512)
                                            );
                                            bytes::Bytes::new()
                                        }
                                        crate::query_request::QueryRequestType::BitgetPositionsSnapshot
                                            if status == 200 =>
                                        {
                                            if let Some(msgs) = parse_bitget_positions_snapshot(&body)
                                            {
                                                if !msgs.is_empty() {
                                                    for payload in msgs {
                                                        let _ = query_resp_sink.send(QueryExecOutcome {
                                                            req_type: msg.req_type,
                                                            client_query_id: msg.client_query_id,
                                                            status,
                                                            body: payload,
                                                            exchange: exchange_copy,
                                                            ip_used_weight_1m: None,
                                                            query_count_1m: None,
                                                        });
                                                    }
                                                    continue;
                                                }
                                                bytes::Bytes::new()
                                            } else {
                                                warn!(
                                                    "bitget positions snapshot parse failed; body={}",
                                                    truncate_for_log(&body, 512)
                                                );
                                                bytes::Bytes::from_static(b"E")
                                            }
                                        }
                                        _ => bytes::Bytes::from(body),
                                    };

                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status,
                                        body: body_bytes,
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                                Err(e) => {
                                    let _ = query_resp_sink.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 0,
                                        body: bytes::Bytes::from(e.to_string()),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                            }
                        }
                        _ => {
                            let _ = query_resp_sink.send(QueryExecOutcome {
                                req_type: msg.req_type,
                                client_query_id: msg.client_query_id,
                                status: 400,
                                body: bytes::Bytes::from_static(b"E"),
                                exchange: exchange_copy,
                                ip_used_weight_1m: None,
                                query_count_1m: None,
                            });
                        }
                    }
                }
            });
            worker_handles.push(("query_router", query_router));
        }

        while !shutdown.is_cancelled() {
            tokio::task::yield_now().await;
        }

        info!("trade_engine shutdown requested; stopping workers");

        // Give ws clients a direct shutdown signal to shorten reconnect/backoff delays.
        if let Some(endpoints) = &ws_endpoints {
            for tx in endpoints {
                let _ = tx.send(WsCommand::Shutdown);
            }
        }
        if let Some(endpoints) = &binance_spot_ws_endpoints {
            for tx in endpoints {
                let _ = tx.send(WsCommand::Shutdown);
            }
        }
        drop(ws_endpoints);

        if let Some(ipc_thread_handle) = ipc_thread_handle {
            if let Err(err) = ipc_thread_handle.join() {
                warn!("trade_engine IPC thread join failed: {:?}", err);
            }
        }

        for (name, handle) in worker_handles {
            join_or_abort(name, handle).await;
        }

        info!("trade_engine shutdown complete");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        binance_std_usdt_transfer_rest_pairs, enable_ipc_fast_poll, parse_bool_env,
        router_idle_spin_iters, DEFAULT_TE_ROUTER_IDLE_SPIN_ITERS,
    };
    use std::sync::{Mutex, OnceLock};

    fn env_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn binance_std_transfer_forces_direction_and_usdt_asset() {
        let msg = crate::trade_request::TradeRequestMsg::create(
            crate::trade_request::TradeRequestType::BinanceStdUmToMainTransfer,
            1,
            -1,
            b"amount=123.45&type=MAIN_UMFUTURE&asset=BTC",
        )
        .expect("trade msg");

        let pairs = binance_std_usdt_transfer_rest_pairs(&msg).expect("pairs");
        assert_eq!(
            pairs,
            vec![
                ("amount".to_string(), "123.45".to_string()),
                ("asset".to_string(), "USDT".to_string()),
                ("type".to_string(), "UMFUTURE_MAIN".to_string()),
            ]
        );
    }

    #[test]
    fn parse_bool_env_accepts_common_values() {
        assert_eq!(parse_bool_env("1"), Some(true));
        assert_eq!(parse_bool_env("true"), Some(true));
        assert_eq!(parse_bool_env("on"), Some(true));
        assert_eq!(parse_bool_env("0"), Some(false));
        assert_eq!(parse_bool_env("false"), Some(false));
        assert_eq!(parse_bool_env("off"), Some(false));
        assert_eq!(parse_bool_env("maybe"), None);
    }

    #[test]
    fn enable_ipc_fast_poll_defaults_on() {
        let _guard = env_test_lock();
        std::env::remove_var("ENABLE_IPC_FAST_POLL");
        std::env::remove_var("enable_ipc_fast_poll");
        assert!(enable_ipc_fast_poll());
    }

    #[test]
    fn enable_ipc_fast_poll_honors_env() {
        let _guard = env_test_lock();
        std::env::set_var("ENABLE_IPC_FAST_POLL", "1");
        assert!(enable_ipc_fast_poll());
        std::env::set_var("ENABLE_IPC_FAST_POLL", "off");
        assert!(!enable_ipc_fast_poll());
        std::env::remove_var("ENABLE_IPC_FAST_POLL");
    }

    #[test]
    fn enable_ipc_fast_poll_accepts_lowercase_env_name() {
        let _guard = env_test_lock();
        std::env::set_var("enable_ipc_fast_poll", "yes");
        assert!(enable_ipc_fast_poll());
        std::env::remove_var("enable_ipc_fast_poll");
    }

    #[test]
    fn router_idle_spin_iters_keeps_spin_when_fast_poll_disabled() {
        let _guard = env_test_lock();
        std::env::remove_var("TE_ROUTER_IDLE_SPIN_ITERS");
        assert_eq!(
            router_idle_spin_iters(true),
            DEFAULT_TE_ROUTER_IDLE_SPIN_ITERS
        );
        assert_eq!(router_idle_spin_iters(false), 64);
    }

    #[test]
    fn router_idle_spin_iters_honors_env_override() {
        let _guard = env_test_lock();
        std::env::set_var("TE_ROUTER_IDLE_SPIN_ITERS", "2048");
        assert_eq!(router_idle_spin_iters(true), 2048);
        assert_eq!(router_idle_spin_iters(false), 2048);
        std::env::remove_var("TE_ROUTER_IDLE_SPIN_ITERS");
    }
}
