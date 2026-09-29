use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::symbol_mapper::create_symbol_mapper;
use crate::pre_trade::symbol_util::is_exposure_exempt_asset;
use crate::strategy::batch_exec_strategy::BatchExecSnapshot;
use crate::strategy::chase_exec::ChaseExecSnapshot;
use anyhow::Result;
use ipc_common::iceoryx_publisher::{
    ExecStateResamplePublisher, GenericPublisher, ResamplePublisher,
};
use log::{info, warn};
use runtime_common::time_util::get_timestamp_us;
use std::cell::OnceCell;
use std::time::Duration;
use trade_signal::MktChannel;
use viz_common::resample::{
    ExecAccountRiskResampleEntry, ExecStrategyStateResampleEntry, ExecStrategyStateRow,
};
use viz_common::{EXEC_RISK_CHANNEL, EXEC_STATE_CHANNEL};

thread_local! {
    static EXEC_RESAMPLE_CHANNEL: OnceCell<ExecResampleChannel> = const { OnceCell::new() };
}

pub struct ExecResampleChannel {
    state_pub: Option<ExecStateResamplePublisher>,
    risk_pub: Option<ResamplePublisher>,
}

fn is_idle_batch_exec(snapshot: &BatchExecSnapshot) -> bool {
    snapshot.target_qty == Some(0.0)
        && snapshot.position_qty == 0.0
        && snapshot.effective_position_qty == 0.0
        && snapshot.live_order_qty == 0.0
        && snapshot.pending_qty == 0.0
        && snapshot.active_batches == 0
        && snapshot.remaining_batches == 0
        && !snapshot.has_execution_in_flight
        && snapshot.execution_complete
        && snapshot.position_allocated
}

fn is_idle_chase_exec(snapshot: &ChaseExecSnapshot) -> bool {
    snapshot.target_qty == Some(0.0)
        && snapshot.position_qty == 0.0
        && snapshot.effective_position_qty == 0.0
        && snapshot.live_order_qty == 0.0
        && snapshot.pending_qty == 0.0
        && snapshot.live_children == 0
        && !snapshot.has_execution_in_flight
        && !snapshot.taker_obligation_pending
        && snapshot.execution_complete
        && snapshot.position_allocated
}

impl ExecResampleChannel {
    pub fn initialize() -> Result<()> {
        EXEC_RESAMPLE_CHANNEL.with(|cell| {
            if cell.get().is_some() {
                anyhow::bail!("ExecResampleChannel already initialized");
            }
            cell.set(Self::new())
                .map_err(|_| anyhow::anyhow!("failed to set ExecResampleChannel"))
        })
    }

    fn new() -> Self {
        Self {
            state_pub: ExecStateResamplePublisher::new_with_prefix("viz_pubs", EXEC_STATE_CHANNEL)
                .map_err(|err| {
                    warn!("ExecResampleChannel init failed channel={EXEC_STATE_CHANNEL}: {err:#}")
                })
                .ok(),
            risk_pub: ResamplePublisher::new_with_prefix("viz_pubs", EXEC_RISK_CHANNEL)
                .map_err(|err| {
                    warn!("ExecResampleChannel init failed channel={EXEC_RISK_CHANNEL}: {err:#}")
                })
                .ok(),
        }
    }

    fn with<R>(f: impl FnOnce(&Self) -> R) -> R {
        EXEC_RESAMPLE_CHANNEL.with(|cell| {
            let channel = cell.get_or_init(Self::new);
            f(channel)
        })
    }

    pub fn start(interval: Duration) {
        tokio::task::spawn_local(async move {
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                timer.tick().await;
                if let Err(err) = Self::with(Self::publish) {
                    warn!("exec resample publish failed: {err:#}");
                }
            }
        });
        info!(
            "exec resample started: interval_ms={}",
            interval.as_millis()
        );
    }

    fn publish(&self) -> Result<usize> {
        let mon = MonitorChannel::instance();
        mon.refresh_exec_risk_state();
        let ts_ms = get_timestamp_us() / 1_000;
        let mut published = 0usize;

        if let Some(publisher) = self.state_pub.as_ref() {
            let mgr = mon.strategy_mgr();
            let mgr = mgr.borrow();
            let snapshots = mgr.batch_exec_snapshots(ts_ms * 1_000);
            let chase_snapshots = mgr.chase_exec_snapshots(ts_ms * 1_000);
            let position_ready = mon.exec_position_snapshot_ready()
                && snapshots.iter().all(|snapshot| snapshot.position_allocated)
                && chase_snapshots
                    .iter()
                    .all(|snapshot| snapshot.position_allocated);
            let mut rows = Vec::with_capacity(snapshots.len() + chase_snapshots.len());
            for snapshot in snapshots {
                if is_idle_batch_exec(&snapshot) {
                    continue;
                }
                let price = MktChannel::instance()
                    .get_quote(&snapshot.symbol, snapshot.exec_venue)
                    .map(|quote| (quote.bid + quote.ask) * 0.5)
                    .unwrap_or(0.0);
                let target_qty = snapshot.target_qty.unwrap_or(0.0);
                let delta_qty = target_qty - snapshot.effective_position_qty;
                rows.push(ExecStrategyStateRow {
                    algorithm: snapshot.algorithm,
                    pov: snapshot.pov,
                    strategy_name: snapshot.strategy_name,
                    source_updated_at_ms: snapshot.source_updated_at_ms,
                    symbol: snapshot.symbol,
                    position_allocated: snapshot.position_allocated,
                    account_position_qty: snapshot.account_position_qty,
                    target_qty,
                    current_qty: snapshot.position_qty,
                    effective_position_qty: snapshot.effective_position_qty,
                    delta_qty,
                    live_order_qty: snapshot.live_order_qty,
                    pending_qty: snapshot.pending_qty,
                    account_position_usdt: snapshot.account_position_qty * price,
                    target_usdt: target_qty * price,
                    current_usdt: snapshot.position_qty * price,
                    delta_usdt: delta_qty * price,
                    live_order_usdt: snapshot.live_order_qty * price,
                    pending_usdt: snapshot.pending_qty * price,
                    active_batches: snapshot.active_batches.min(u32::MAX as usize) as u32,
                    remaining_batches: snapshot.remaining_batches,
                    estimated_completion_ts_ms: snapshot.estimated_completion_ts_ms,
                    execution_complete: snapshot.execution_complete,
                    completion_reason: snapshot.completion_reason,
                    mid_price: price,
                });
            }
            for snapshot in chase_snapshots {
                if is_idle_chase_exec(&snapshot) {
                    continue;
                }
                let price = MktChannel::instance()
                    .get_quote(&snapshot.symbol, snapshot.exec_venue)
                    .map(|quote| (quote.bid + quote.ask) * 0.5)
                    .unwrap_or(0.0);
                let target_qty = snapshot.target_qty.unwrap_or(0.0);
                let delta_qty = target_qty - snapshot.effective_position_qty;
                rows.push(ExecStrategyStateRow {
                    algorithm: snapshot.algorithm,
                    pov: snapshot.pov,
                    strategy_name: snapshot.strategy_name,
                    source_updated_at_ms: snapshot.source_updated_at_ms,
                    symbol: snapshot.symbol,
                    position_allocated: snapshot.position_allocated,
                    account_position_qty: snapshot.account_position_qty,
                    target_qty,
                    current_qty: snapshot.position_qty,
                    effective_position_qty: snapshot.effective_position_qty,
                    delta_qty,
                    live_order_qty: snapshot.live_order_qty,
                    pending_qty: snapshot.pending_qty,
                    account_position_usdt: snapshot.account_position_qty * price,
                    target_usdt: target_qty * price,
                    current_usdt: snapshot.position_qty * price,
                    delta_usdt: delta_qty * price,
                    live_order_usdt: snapshot.live_order_qty * price,
                    pending_usdt: snapshot.pending_qty * price,
                    active_batches: snapshot.live_children.min(u32::MAX as usize) as u32,
                    remaining_batches: 0,
                    estimated_completion_ts_ms: 0,
                    execution_complete: snapshot.execution_complete,
                    completion_reason: snapshot.completion_reason,
                    mid_price: price,
                });
            }
            let entry = ExecStrategyStateResampleEntry::from_rows(ts_ms, position_ready, rows)?;
            if Self::publish_encoded(entry.to_bytes()?, publisher, EXEC_STATE_CHANNEL)? {
                published += 1;
            }
        }

        if let Some(publisher) = self.risk_pub.as_ref() {
            let (exposures, equity_usdt, _, _, _) = mon.basic_state_snapshot();
            let price_snapshot = mon.price_table().borrow().snapshot();
            let price_mapper = create_symbol_mapper(mon.mark_price_exchange());
            let mut long_notional_usdt = 0.0;
            let mut short_notional_usdt = 0.0;
            for (asset, (open_qty, hedge_qty)) in exposures {
                let qty = open_qty + hedge_qty;
                if qty == 0.0 || is_exposure_exempt_asset(&asset) {
                    continue;
                }
                let symbol = price_mapper.asset_to_price_symbol(&asset);
                let price = price_snapshot
                    .get(&symbol)
                    .map(|entry| entry.mark_price)
                    .filter(|price| price.is_finite() && *price > 0.0)
                    .unwrap_or(0.0);
                let notional = qty * price;
                if notional > 0.0 {
                    long_notional_usdt += notional;
                } else {
                    short_notional_usdt += -notional;
                }
            }
            let gross_notional_usdt = long_notional_usdt + short_notional_usdt;
            let net_notional_usdt = long_notional_usdt - short_notional_usdt;
            let leverage = if equity_usdt.abs() <= f64::EPSILON {
                0.0
            } else {
                gross_notional_usdt / equity_usdt
            };
            let entry = ExecAccountRiskResampleEntry {
                ts_ms,
                venue: mon.open_venue().data_pub_slug().to_string(),
                equity_usdt,
                long_notional_usdt,
                short_notional_usdt,
                net_notional_usdt,
                gross_notional_usdt,
                leverage,
            };
            if Self::publish_encoded(entry.to_bytes()?, publisher, EXEC_RISK_CHANNEL)? {
                published += 1;
            }
        }

        Ok(published)
    }

    fn publish_encoded<const PAYLOAD: usize>(
        bytes: Vec<u8>,
        publisher: &GenericPublisher<PAYLOAD>,
        channel: &str,
    ) -> Result<bool> {
        let mut payload = Vec::with_capacity(bytes.len() + 4);
        payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        payload.extend_from_slice(&bytes);
        if payload.len() > PAYLOAD {
            warn!(
                "exec resample payload too large: channel={} bytes={} limit={}",
                channel,
                payload.len(),
                PAYLOAD
            );
            return Ok(false);
        }
        publisher.publish(&payload)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::{is_idle_batch_exec, is_idle_chase_exec};
    use crate::strategy::batch_exec_strategy::BatchExecSnapshot;
    use crate::strategy::chase_exec::ChaseExecSnapshot;
    use order_common::TradingVenue;

    fn idle_batch() -> BatchExecSnapshot {
        BatchExecSnapshot {
            algorithm: "batch".into(),
            pov: None,
            strategy_name: "test".into(),
            source_updated_at_ms: 0,
            symbol: "BTCUSDT".into(),
            exec_venue: TradingVenue::BinanceFutures,
            account_position_qty: 1.0,
            position_qty: 0.0,
            effective_position_qty: 0.0,
            position_allocated: true,
            target_qty: Some(0.0),
            pending_qty: 0.0,
            live_order_qty: 0.0,
            active_batches: 0,
            remaining_batches: 0,
            has_execution_in_flight: false,
            estimated_completion_ts_ms: 0,
            execution_complete: true,
            completion_reason: "target_reached".into(),
        }
    }

    fn idle_chase() -> ChaseExecSnapshot {
        ChaseExecSnapshot {
            algorithm: "chase_exec".into(),
            pov: None,
            strategy_name: "test".into(),
            source_updated_at_ms: 0,
            symbol: "BTCUSDT".into(),
            exec_venue: TradingVenue::BinanceFutures,
            account_position_qty: 1.0,
            position_qty: 0.0,
            effective_position_qty: 0.0,
            position_allocated: true,
            target_qty: Some(0.0),
            pending_qty: 0.0,
            live_order_qty: 0.0,
            live_children: 0,
            has_execution_in_flight: false,
            taker_obligation_pending: false,
            execution_complete: true,
            completion_reason: "target_reached".into(),
        }
    }

    #[test]
    fn idle_snapshots_are_filtered() {
        assert!(is_idle_batch_exec(&idle_batch()));
        assert!(is_idle_chase_exec(&idle_chase()));
    }

    #[test]
    fn each_batch_disqualifier_keeps_the_row() {
        let idle = idle_batch();
        macro_rules! keeps_row {
            ($field:ident = $value:expr) => {{
                let mut snapshot = idle.clone();
                snapshot.$field = $value;
                assert!(!is_idle_batch_exec(&snapshot), stringify!($field));
            }};
        }
        keeps_row!(target_qty = None);
        keeps_row!(target_qty = Some(1.0));
        keeps_row!(position_qty = 1.0);
        keeps_row!(effective_position_qty = 1.0);
        keeps_row!(live_order_qty = 1.0);
        keeps_row!(pending_qty = 1.0);
        keeps_row!(active_batches = 1);
        keeps_row!(remaining_batches = 1);
        keeps_row!(has_execution_in_flight = true);
        keeps_row!(execution_complete = false);
        keeps_row!(position_allocated = false);
        keeps_row!(position_qty = f64::NAN);
    }

    #[test]
    fn each_chase_disqualifier_keeps_the_row() {
        let idle = idle_chase();
        macro_rules! keeps_row {
            ($field:ident = $value:expr) => {{
                let mut snapshot = idle.clone();
                snapshot.$field = $value;
                assert!(!is_idle_chase_exec(&snapshot), stringify!($field));
            }};
        }
        keeps_row!(target_qty = None);
        keeps_row!(target_qty = Some(1.0));
        keeps_row!(position_qty = 1.0);
        keeps_row!(effective_position_qty = 1.0);
        keeps_row!(live_order_qty = 1.0);
        keeps_row!(pending_qty = 1.0);
        keeps_row!(live_children = 1);
        keeps_row!(has_execution_in_flight = true);
        keeps_row!(taker_obligation_pending = true);
        keeps_row!(execution_complete = false);
        keeps_row!(position_allocated = false);
        keeps_row!(pending_qty = f64::NAN);
    }

    #[test]
    fn row_reappears_when_a_target_becomes_non_idle() {
        let mut batch = idle_batch();
        let visible_count =
            |snapshot: &BatchExecSnapshot| usize::from(!is_idle_batch_exec(snapshot));
        assert_eq!(visible_count(&batch), 0);
        batch.target_qty = Some(1.0);
        assert_eq!(visible_count(&batch), 1);
        batch.target_qty = Some(0.0);
        assert_eq!(visible_count(&batch), 0);

        let mut chase = idle_chase();
        assert!(is_idle_chase_exec(&chase));
        chase.has_execution_in_flight = true;
        assert!(!is_idle_chase_exec(&chase));
    }
}
