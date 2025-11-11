use crate::common::iceoryx_publisher::{
    ResamplePublisher, SignalPublisher, RESAMPLE_PAYLOAD, SIGNAL_PAYLOAD,
};
use crate::common::min_qty_table::MinQtyTable;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::common::{SignalBytes, TradingVenue};
use crate::strategy::hedge_arb_strategy::HedgeArbStrategy;
use crate::signal::hedge_signal::ArbHedgeCtx;
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::record::{SignalRecordMessage, PRE_TRADE_SIGNAL_RECORD_CHANNEL};
use crate::signal::resample::{
    PreTradeExposureResampleEntry, PreTradeExposureRow, PreTradePositionResampleEntry,
    PreTradeRiskResampleEntry, PreTradeSpotBalanceRow, PreTradeUmPositionRow,

};
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::{Strategy, StrategyManager};
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use iceoryx2::port::{publisher::Publisher, subscriber::Subscriber};
use log::{debug, error, info, warn};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};


const NODE_PRE_TRADE_SIGNAL_PREFIX: &str = "signals";

pub struct PreTrade {}

impl PreTrade {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn run(self) -> Result<()> {
        info!("pre_trade starting");
        // 首次从 Redis 拉取 pre-trade 参数
        // 提升周期检查频率到 100ms，使策略状态响应更及时
        let mut ticker = tokio::time::interval(Duration::from_millis(100));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    break;
                }
                _ = ticker.tick() => {
                    runtime.tick().await;
                }
                else => break,
            }
        }

        info!("pre_trade exiting");
        Ok(())
    }
}

struct RuntimeContext {
    strategy_mgr: Rc<RefCell<StrategyManager>>,
    resample_interval: std::time::Duration,
    next_resample: std::time::Instant,
    next_params_refresh: std::time::Instant,
    params_refresh_secs: u64,
    last_params_snapshot: Option<PreTradeParamsSnap>,
}

impl RuntimeContext {
    fn new(
    ) -> Self {

        Self {
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            strategy_params,
            max_pending_limit_orders: Rc::new(Cell::new(3)),
            min_qty_table,
            resample_positions_pub·,
            resample_exposure_pub,
            resample_risk_pub,
            signal_record_pub,
            backward_pub,
            resample_interval: std::time::Duration::from_secs(3),
            next_resample: std::time::Instant::now() + std::time::Duration::from_secs(3),
            next_params_refresh: std::time::Instant::now(),
            params_refresh_secs: 30,
            last_params_snapshot: None,
        }
    }

    async fn tick(&mut self) {
        let now = get_timestamp_us();
        self.strategy_mgr.borrow_mut().handle_period_clock(now);
        let instant_now = std::time::Instant::now();
        if instant_now >= self.next_params_refresh {
            match self.reload_params().await {
                Ok(()) => {
                }
                Err(err) => {
                    warn!("pre_trade params refresh failed: {err:#}");
                } 
            } 
            self.next_params_refresh =
                instant_now + std::time::Duration::from_secs(self.params_refresh_secs.max(5));
        }

        if self.resample_positions_pub.is_some()
            || self.resample_exposure_pub.is_some()
            || self.resample_risk_pub.is_some()
        {
            while instant_now >= self.next_resample {
                if let Err(err) = self.publish_resample_entries() {
                    warn!("pre_trade resample publish failed: {err:#}");
                    self.next_resample = std::time::Instant::now() + self.resample_interval;
                    break;
                }
                self.next_resample += self.resample_interval;
            }
        }
    }
}

impl RuntimeContext {
    async fn reload_params(&mut self) -> Result<()> {
        let snapshot = PreTradeParamsSnap {
            max_pos_u: sp.max_pos_u,
            max_symbol_exposure_ratio: sp.max_symbol_exposure_ratio,
            max_total_exposure_ratio: sp.max_total_exposure_ratio,
            max_leverage: sp.max_leverage,
            refresh_secs: new_refresh,
        };
        let changed = self
            .last_params_snapshot
            .as_ref()
            .map(|old| old != &snapshot)
            .unwrap_or(true);

        self.strategy_params = sp;
        self.params_refresh_secs = new_refresh;
        self.last_params_snapshot = Some(snapshot);
        if changed {
            debug!(
                "pre_trade params updated: max_pos_u={:.2} sym_ratio={:.4} total_ratio={:.4} max_leverage={:.2} refresh={}s",
                self.strategy_params.max_pos_u,
                self.strategy_params.max_symbol_exposure_ratio,
                self.strategy_params.max_total_exposure_ratio,
                self.strategy_params.max_leverage,
                self.params_refresh_secs
            );
        }
        Ok(())
    }

    fn publish_resample_entries(&mut self) -> Result<usize> {
        if self.resample_positions_pub.is_none()
            && self.resample_exposure_pub.is_none()
            && self.resample_risk_pub.is_none()
        {
            return Ok(0);
        }

        let Some(spot_snapshot) = self.spot_manager.snapshot() else {
            return Ok(0);
        };
        let Some(um_snapshot) = self.um_manager.snapshot() else {
            return Ok(0);
        };

        let price_snapshot = self.price_table.borrow().snapshot();
        let ts_ms = (get_timestamp_us() / 1000) as i64;

        let mut exposures_mgr = self.exposure_manager.borrow_mut();
        exposures_mgr.revalue_with_prices(&price_snapshot);
        exposures_mgr.log_summary("resample估值");
        let exposures_vec = exposures_mgr.exposures().to_vec();
        let total_equity = exposures_mgr.total_equity();
        let total_abs_exposure = exposures_mgr.total_abs_exposure();
        let total_position = exposures_mgr.total_position();
        let spot_equity_usd = exposures_mgr.total_spot_value_usd();
        let borrowed_usd = exposures_mgr.total_borrowed_usd();
        let interest_usd = exposures_mgr.total_interest_usd();
        let um_unrealized_usd = exposures_mgr.total_um_unrealized();
        let max_leverage = self.strategy_params.max_leverage;
        drop(exposures_mgr);

        let mut published = 0usize;

        if let Some(publisher) = self.resample_positions_pub.as_ref() {
            let um_rows: Vec<PreTradeUmPositionRow> = um_snapshot
                .positions
                .iter()
                .map(|pos| PreTradeUmPositionRow {
                    symbol: pos.symbol.clone(),
                    side: pos.position_side.to_string(),
                    position_amount: pos.position_amt,
                    entry_price: pos.entry_price,
                    leverage: pos.leverage,
                    position_initial_margin: pos.position_initial_margin,
                    open_order_initial_margin: pos.open_order_initial_margin,
                    unrealized_profit: pos.unrealized_profit,
                })
                .collect();

            let spot_rows: Vec<PreTradeSpotBalanceRow> = spot_snapshot
                .balances
                .iter()
                .map(|bal| PreTradeSpotBalanceRow {
                    asset: bal.asset.clone(),
                    total_wallet: bal.total_wallet_balance,
                    cross_free: bal.cross_margin_free,
                    cross_locked: bal.cross_margin_locked,
                    cross_borrowed: bal.cross_margin_borrowed,
                    cross_interest: bal.cross_margin_interest,
                    um_wallet: bal.um_wallet_balance,
                    um_unrealized_pnl: bal.um_unrealized_pnl,
                })
                .collect();

            let entry = PreTradePositionResampleEntry {
                ts_ms,
                um_positions: um_rows,
                spot_balances: spot_rows,
            };
            if Self::publish_encoded(entry.to_bytes()?, publisher)? {
                published += 1;
            }
        }

        if let Some(publisher) = self.resample_exposure_pub.as_ref() {
            let mut rows: Vec<PreTradeExposureRow> = Vec::new();
            let mut exposure_sum_usdt = 0.0_f64;
            for entry in &exposures_vec {
                let asset_upper = entry.asset.to_uppercase();
                if asset_upper == "USDT" {
                    continue;
                }
                let symbol = format!("{}USDT", asset_upper);
                let mark = price_snapshot
                    .get(&symbol)
                    .map(|p| p.mark_price)
                    .unwrap_or(0.0);
                if mark == 0.0 && (entry.spot_total_wallet != 0.0 || entry.um_net_position != 0.0) {
                    debug!("missing mark price for {} when resampling exposure", symbol);
                }
                let spot_usdt = entry.spot_total_wallet * mark;
                let um_usdt = entry.um_net_position * mark;
                let exposure_usdt = spot_usdt + um_usdt;
                exposure_sum_usdt += exposure_usdt;
                rows.push(PreTradeExposureRow {
                    asset: entry.asset.clone(),
                    spot_qty: Some(entry.spot_total_wallet),
                    spot_usdt: Some(spot_usdt),
                    um_net_qty: Some(entry.um_net_position),
                    um_net_usdt: Some(um_usdt),
                    exposure_qty: Some(entry.exposure),
                    exposure_usdt: Some(exposure_usdt),
                    is_total: false,
                });
            }
            if !rows.is_empty() {
                rows.push(PreTradeExposureRow {
                    asset: "TOTAL".to_string(),
                    spot_qty: None,
                    spot_usdt: None,
                    um_net_qty: None,
                    um_net_usdt: None,
                    exposure_qty: None,
                    exposure_usdt: Some(exposure_sum_usdt),
                    is_total: true,
                });
            }

            let entry = PreTradeExposureResampleEntry { ts_ms, rows };
            if Self::publish_encoded(entry.to_bytes()?, publisher)? {
                published += 1;
            }
        }

        if let Some(publisher) = self.resample_risk_pub.as_ref() {
            let leverage = if total_equity.abs() <= f64::EPSILON {
                0.0
            } else {
                total_position / total_equity
            };
            let entry = PreTradeRiskResampleEntry {
                ts_ms,
                total_equity,
                total_exposure: total_abs_exposure,
                total_position,
                spot_equity_usd,
                borrowed_usd,
                interest_usd,
                um_unrealized_usd,
                leverage,
                max_leverage,
            };
            if Self::publish_encoded(entry.to_bytes()?, publisher)? {
                published += 1;
            }
        }

        Ok(published)
    }

    fn publish_encoded(bytes: Vec<u8>, publisher: &ResamplePublisher) -> Result<bool> {
        if bytes.is_empty() {
            return Ok(false);
        }
        let mut buf = Vec::with_capacity(bytes.len() + 4);
        let len = bytes.len() as u32;
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(&bytes);
        if buf.len() > RESAMPLE_PAYLOAD {
            warn!(
                "pre_trade重采样载荷过大 ({} 字节，阈值 {} 字节)，已跳过",
                buf.len(),
                RESAMPLE_PAYLOAD
            );
            return Ok(false);
        }
        publisher.publish(&buf)?;
        Ok(true)
    }
}

#[derive(Debug, Clone, PartialEq)]
struct PreTradeParamsSnap {
    max_pos_u: f64,
    max_symbol_exposure_ratio: f64,
    max_total_exposure_ratio: f64,
    max_leverage: f64,
    refresh_secs: u64,
}


// 删除了基于 JSON 的账户元数据提取逻辑，账户事件采用二进制帧头解析
fn signal_node_name(channel: &str) -> String {
    format!(
        "{}{}",
        NODE_PRE_TRADE_SIGNAL_PREFIX,
        sanitize_suffix(channel)
    )
}

fn sanitize_suffix(raw: &str) -> std::borrow::Cow<'_, str> {
    if raw.chars().all(is_valid_node_char) {
        return std::borrow::Cow::Borrowed(raw);
    }
    let sanitized: String = raw
        .chars()
        .map(|c| if is_valid_node_char(c) { c } else { '_' })
        .collect();
    std::borrow::Cow::Owned(sanitized)
}
