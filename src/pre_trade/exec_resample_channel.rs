use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::symbol_mapper::create_symbol_mapper;
use anyhow::Result;
use ipc_common::iceoryx_publisher::{ResamplePublisher, RESAMPLE_PAYLOAD};
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
    state_pub: Option<ResamplePublisher>,
    risk_pub: Option<ResamplePublisher>,
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
        let make_pub = |channel: &str| {
            ResamplePublisher::new_with_prefix("viz_pubs", channel)
                .map_err(|err| warn!("ExecResampleChannel init failed channel={channel}: {err:#}"))
                .ok()
        };
        Self {
            state_pub: make_pub(EXEC_STATE_CHANNEL),
            risk_pub: make_pub(EXEC_RISK_CHANNEL),
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
            let snapshots = mon
                .strategy_mgr()
                .borrow()
                .batch_exec_snapshots(ts_ms * 1_000);
            let position_ready = mon.exec_position_snapshot_ready()
                && snapshots.iter().all(|snapshot| snapshot.position_allocated);
            let mut rows = Vec::with_capacity(snapshots.len());
            for snapshot in snapshots {
                let price = MktChannel::instance()
                    .get_quote(&snapshot.symbol, snapshot.exec_venue)
                    .map(|quote| (quote.bid + quote.ask) * 0.5)
                    .unwrap_or(0.0);
                let target_qty = snapshot.target_qty.unwrap_or(0.0);
                let delta_qty = target_qty - snapshot.effective_position_qty;
                rows.push(ExecStrategyStateRow {
                    strategy_name: snapshot.strategy_name,
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
                });
            }
            rows.sort_by(|lhs, rhs| {
                (&lhs.strategy_name, &lhs.symbol).cmp(&(&rhs.strategy_name, &rhs.symbol))
            });
            let entry = ExecStrategyStateResampleEntry {
                ts_ms,
                position_ready,
                rows,
            };
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
                if qty == 0.0 || asset.eq_ignore_ascii_case("USDT") {
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

    fn publish_encoded(
        bytes: Vec<u8>,
        publisher: &ResamplePublisher,
        channel: &str,
    ) -> Result<bool> {
        let mut payload = Vec::with_capacity(bytes.len() + 4);
        payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        payload.extend_from_slice(&bytes);
        if payload.len() > RESAMPLE_PAYLOAD {
            warn!(
                "exec resample payload too large: channel={} bytes={} limit={}",
                channel,
                payload.len(),
                RESAMPLE_PAYLOAD
            );
            return Ok(false);
        }
        publisher.publish(&payload)?;
        Ok(true)
    }
}
