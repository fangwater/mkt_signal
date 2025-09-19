use crate::common::account_msg::{
    get_event_type as get_account_event_type, AccountEventType, AccountUpdateBalanceMsg,
    AccountUpdatePositionMsg, BalanceUpdateMsg,
};
use crate::common::msg_parser::{get_msg_type, parse_index_price, parse_mark_price, MktMsgType};
use crate::common::signal_event::{SignalEvent, SignalEventType};
use crate::pre_trade::binance_pm_spot_manager::{BinancePmSpotAccountManager, BinanceSpotBalance};
use crate::pre_trade::binance_pm_um_manager::{
    BinancePmUmAccountManager, BinanceUmPosition, PositionSide,
};
use crate::pre_trade::config::{
    AccountStreamCfg, PreTradeCfg, SignalSubscriptionsCfg, TradeEngineRespCfg,
};
use crate::pre_trade::event::{AccountEvent, PreTradeEvent, TradeEngineResponse, TradeSignalEvent};
use crate::pre_trade::exposure_manager::{ExposureEntry, ExposureManager};
use crate::pre_trade::price_table::{PriceEntry, PriceTable};
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, error, info, warn};
use std::{
    borrow::Cow,
    cell::RefCell,
    cmp::max,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
    time::Duration,
};
use tokio::sync::mpsc::UnboundedSender;

const ACCOUNT_PAYLOAD: usize = 16_384;
const TRADE_RESP_PAYLOAD: usize = 16_384;
const SIGNAL_PAYLOAD: usize = 1_024;
const NODE_PRE_TRADE_ACCOUNT: &str = "pre_trade_account";
const NODE_PRE_TRADE_TRADE_RESP: &str = "pre_trade_trade_resp";
const NODE_PRE_TRADE_SIGNAL_PREFIX: &str = "pre_trade_signal_";
const NODE_PRE_TRADE_DERIVATIVES: &str = "pre_trade_derivatives";
const DERIVATIVES_SERVICE: &str = "data_pubs/binance-futures/derivatives";
const DERIVATIVES_PAYLOAD: usize = 128;

pub struct PreTrade {
    cfg: PreTradeCfg,
}

impl PreTrade {
    pub fn new(cfg: PreTradeCfg) -> Self {
        Self { cfg }
    }

    pub async fn run(self) -> Result<()> {
        info!("pre_trade starting");

        let um_cfg = self
            .cfg
            .risk_checks
            .binance_pm_um
            .as_ref()
            .ok_or_else(|| anyhow!("risk_checks.binance_pm_um must be configured"))?;

        let um_api_key = std::env::var(&um_cfg.api_key_env)
            .map_err(|_| anyhow!("environment variable {} not set", um_cfg.api_key_env))?;
        let um_api_secret = std::env::var(&um_cfg.api_secret_env)
            .map_err(|_| anyhow!("environment variable {} not set", um_cfg.api_secret_env))?;

        let um_manager = BinancePmUmAccountManager::new(
            &um_cfg.rest_base,
            um_api_key.clone(),
            um_api_secret.clone(),
            um_cfg.recv_window_ms,
        );

        let snapshot = um_manager
            .init()
            .await
            .context("failed to load initial Binance UM snapshot")?;
        let active_positions = snapshot
            .positions
            .iter()
            .filter(|p| p.position_amt != 0.0)
            .count();
        info!(
            "Binance UM snapshot ready: total_positions={}, active_positions={}, fetched_at={}",
            snapshot.positions.len(),
            active_positions,
            snapshot.fetched_at,
        );

        let spot_cfg = self
            .cfg
            .risk_checks
            .binance_spot
            .as_ref()
            .ok_or_else(|| anyhow!("risk_checks.binance_spot must be configured"))?;

        let spot_api_key = if spot_cfg.api_key_env == um_cfg.api_key_env {
            um_api_key.clone()
        } else {
            std::env::var(&spot_cfg.api_key_env)
                .map_err(|_| anyhow!("environment variable {} not set", spot_cfg.api_key_env))?
        };
        let spot_api_secret = if spot_cfg.api_secret_env == um_cfg.api_secret_env {
            um_api_secret.clone()
        } else {
            std::env::var(&spot_cfg.api_secret_env)
                .map_err(|_| anyhow!("environment variable {} not set", spot_cfg.api_secret_env))?
        };

        let asset_filter = spot_cfg
            .asset
            .as_ref()
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .map(|v| v.to_string());

        let spot_manager = BinancePmSpotAccountManager::new(
            &spot_cfg.rest_base,
            spot_api_key,
            spot_api_secret,
            spot_cfg.recv_window_ms,
            asset_filter,
        );

        let spot_snapshot = spot_manager
            .init()
            .await
            .context("failed to load initial Binance spot snapshot")?;

        info!(
            "Binance PM 初始化完成: um_positions={}, spot_assets={}",
            snapshot.positions.len(),
            spot_snapshot.balances.len()
        );

        log_um_positions(&snapshot.positions);
        log_spot_balances(&spot_snapshot.balances);

        let exposure_manager = ExposureManager::new(&snapshot, &spot_snapshot);
        log_exposures(exposure_manager.exposures());
        if let Some(usdt) = exposure_manager.usdt_summary() {
            info!(
                "USDT余额: total_wallet={:.6}, cross_free={:.6}, cross_locked={:.6}, um_wallet={:.6}, cm_wallet={:.6}",
                usdt.total_wallet_balance,
                usdt.cross_margin_free,
                usdt.cross_margin_locked,
                usdt.um_wallet_balance,
                usdt.cm_wallet_balance
            );
        } else {
            warn!("USDT余额信息缺失");
        }

        let mut price_symbols: BTreeSet<String> = BTreeSet::new();
        for pos in &snapshot.positions {
            price_symbols.insert(pos.symbol.to_uppercase());
        }
        for bal in &spot_snapshot.balances {
            if bal.asset.eq_ignore_ascii_case("USDT") {
                continue;
            }
            price_symbols.insert(format!("{}USDT", bal.asset.to_uppercase()));
        }

        let price_table = Rc::new(RefCell::new(PriceTable::new()));
        {
            let mut table = price_table.borrow_mut();
            table
                .init(&price_symbols)
                .await
                .context("failed to load initial mark/index prices")?;
            let snapshot = table.snapshot();
            log_price_table(&snapshot);
        }

        spawn_derivatives_worker(price_table.clone())?;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PreTradeEvent>();

        spawn_account_stream_worker(&self.cfg.account_stream, tx.clone())?;
        spawn_trade_response_worker(&self.cfg.trade_engine, tx.clone())?;
        spawn_signal_workers(&self.cfg.signals, tx.clone())?;

        // Drop extra sender reference held locally to ensure shutdown after workers exit.
        drop(tx);

        let shutdown = tokio::signal::ctrl_c();
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    info!("shutdown signal received");
                    break;
                }
                Some(evt) = rx.recv() => {
                    match evt {
                        PreTradeEvent::Account(acc_evt) => {
                            if let Err(err) =
                                process_account_event(&acc_evt, &spot_manager, &um_manager)
                            {
                                warn!("process account event failed: {err:?}");
                            }
                            log_account_event(&acc_evt);
                        }
                        PreTradeEvent::TradeResponse(resp_evt) => {
                            log_trade_response(&resp_evt);
                        }
                        PreTradeEvent::Signal(sig_evt) => {
                            log_signal_event(&sig_evt);
                        }
                    }
                }
                else => { break; }
            }
        }

        info!("pre_trade exiting");
        Ok(())
    }
}

fn log_um_positions(positions: &[BinanceUmPosition]) {
    if positions.is_empty() {
        info!("UM 持仓为空");
        return;
    }

    let mut rows: Vec<Vec<String>> = positions
        .iter()
        .map(|pos| {
            vec![
                pos.symbol.clone(),
                pos.position_side.to_string(),
                fmt_decimal(pos.position_amt),
                fmt_decimal(signed_position_amount(pos)),
                fmt_decimal(pos.entry_price),
                fmt_decimal(pos.leverage),
                fmt_decimal(pos.position_initial_margin),
                fmt_decimal(pos.open_order_initial_margin),
                fmt_decimal(pos.unrealized_profit),
            ]
        })
        .collect();
    rows.sort_by(|a, b| a[0].cmp(&b[0]));

    let table = render_three_line_table(
        &[
            "Symbol", "Side", "PosAmt", "NetAmt", "EntryPx", "Lev", "PosIM", "OpenIM", "uPnL",
        ],
        &rows,
    );
    info!("UM 持仓概览\n{}", table);
}

fn log_spot_balances(balances: &[BinanceSpotBalance]) {
    if balances.is_empty() {
        warn!("现货资产列表为空");
        return;
    }

    let mut rows: Vec<Vec<String>> = balances
        .iter()
        .map(|bal| {
            vec![
                bal.asset.clone(),
                fmt_decimal(bal.total_wallet_balance),
                fmt_decimal(bal.cross_margin_asset),
                fmt_decimal(bal.cross_margin_free),
                fmt_decimal(bal.cross_margin_locked),
                fmt_decimal(bal.cross_margin_borrowed),
                fmt_decimal(bal.um_wallet_balance),
                fmt_decimal(bal.um_unrealized_pnl),
                fmt_decimal(bal.cm_wallet_balance),
                fmt_decimal(bal.cm_unrealized_pnl),
            ]
        })
        .collect();
    rows.sort_by(|a, b| a[0].cmp(&b[0]));

    let table = render_three_line_table(
        &[
            "Asset",
            "TotalWallet",
            "CrossAsset",
            "CrossFree",
            "CrossLocked",
            "CrossBorrowed",
            "UMWallet",
            "UMUPNL",
            "CMWallet",
            "CMUPNL",
        ],
        &rows,
    );
    info!("现货资产概览\n{}", table);
}

fn log_exposures(entries: &[ExposureEntry]) {
    if entries.is_empty() {
        info!("非 USDT 资产敞口为空");
        return;
    }

    let rows: Vec<Vec<String>> = entries
        .iter()
        .map(|entry| {
            vec![
                entry.asset.clone(),
                fmt_decimal(entry.spot_total_wallet),
                fmt_decimal(entry.spot_cross_free),
                fmt_decimal(entry.spot_cross_locked),
                fmt_decimal(entry.um_net_position),
                fmt_decimal(entry.um_position_initial_margin),
                fmt_decimal(entry.um_open_order_initial_margin),
                fmt_decimal(entry.exposure),
            ]
        })
        .collect();

    let table = render_three_line_table(
        &[
            "Asset",
            "SpotTotal",
            "SpotFree",
            "SpotLocked",
            "UMNet",
            "UMPosIM",
            "UMOpenIM",
            "Exposure",
        ],
        &rows,
    );
    info!("现货+UM 敞口汇总\n{}", table);
}

fn log_price_table(entries: &BTreeMap<String, PriceEntry>) {
    if entries.is_empty() {
        warn!("未获取到标记价格数据");
        return;
    }

    let rows: Vec<Vec<String>> = entries
        .values()
        .map(|entry| {
            vec![
                entry.symbol.clone(),
                fmt_decimal(entry.mark_price),
                fmt_decimal(entry.index_price),
                entry.update_time.to_string(),
            ]
        })
        .collect();

    let table =
        render_three_line_table(&["Symbol", "MarkPrice", "IndexPrice", "UpdateTime"], &rows);
    info!("标记价格表\n{}", table);
}

fn spawn_derivatives_worker(price_table: Rc<RefCell<PriceTable>>) -> Result<()> {
    let service = DERIVATIVES_SERVICE.to_string();
    let node_name = NODE_PRE_TRADE_DERIVATIVES.to_string();
    tokio::task::spawn_local(async move {
        if let Err(err) = derivatives_loop(node_name, service, price_table).await {
            error!("derivatives worker exited: {err:?}");
        }
    });
    Ok(())
}

async fn derivatives_loop(
    node_name: String,
    service: String,
    price_table: Rc<RefCell<PriceTable>>,
) -> Result<()> {
    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;

    let service = node
        .service_builder(&ServiceName::new(&service)?)
        .publish_subscribe::<[u8; DERIVATIVES_PAYLOAD]>()
        .open_or_create()?;
    let subscriber: Subscriber<ipc::Service, [u8; DERIVATIVES_PAYLOAD], ()> =
        service.subscriber_builder().create()?;
    info!("derivatives metrics subscribed: service={}", service.name());

    loop {
        match subscriber.receive() {
            Ok(Some(sample)) => {
                let payload = trim_payload(sample.payload());
                if payload.is_empty() {
                    continue;
                }
                let Some(msg_type) = get_msg_type(&payload) else {
                    continue;
                };
                match msg_type {
                    MktMsgType::MarkPrice => match parse_mark_price(&payload) {
                        Ok(msg) => {
                            let mut table = price_table.borrow_mut();
                            table.update_mark_price(&msg.symbol, msg.mark_price, msg.timestamp);
                        }
                        Err(err) => warn!("parse mark price failed: {err:?}"),
                    },
                    MktMsgType::IndexPrice => match parse_index_price(&payload) {
                        Ok(msg) => {
                            let mut table = price_table.borrow_mut();
                            table.update_index_price(&msg.symbol, msg.index_price, msg.timestamp);
                        }
                        Err(err) => warn!("parse index price failed: {err:?}"),
                    },
                    _ => {}
                }
            }
            Ok(None) => {
                tokio::task::yield_now().await;
            }
            Err(err) => {
                warn!("derivatives stream receive error: {err}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

fn fmt_decimal(value: f64) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    let mut s = format!("{:.6}", value);
    if s.contains('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    if s.is_empty() {
        "0".to_string()
    } else {
        s
    }
}

fn signed_position_amount(position: &BinanceUmPosition) -> f64 {
    match position.position_side {
        PositionSide::Both => position.position_amt,
        PositionSide::Long => position.position_amt.abs(),
        PositionSide::Short => -position.position_amt.abs(),
    }
}

fn render_three_line_table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let widths = compute_widths(headers, rows);
    let mut out = String::new();
    out.push_str(&build_separator(&widths, '-'));
    out.push('\n');
    out.push_str(&build_row(
        headers
            .iter()
            .map(|h| h.to_string())
            .collect::<Vec<String>>(),
        &widths,
    ));
    out.push('\n');
    out.push_str(&build_separator(&widths, '='));
    if rows.is_empty() {
        out.push('\n');
        out.push_str(&build_separator(&widths, '-'));
        return out;
    }
    for row in rows {
        out.push('\n');
        out.push_str(&build_row(row.clone(), &widths));
    }
    out.push('\n');
    out.push_str(&build_separator(&widths, '-'));
    out
}

fn compute_widths(headers: &[&str], rows: &[Vec<String>]) -> Vec<usize> {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (idx, cell) in row.iter().enumerate() {
            if idx >= widths.len() {
                continue;
            }
            widths[idx] = max(widths[idx], cell.len());
        }
    }
    widths
}

fn build_separator(widths: &[usize], fill: char) -> String {
    let mut line = String::new();
    line.push('+');
    for width in widths {
        line.push_str(&fill.to_string().repeat(width + 2));
        line.push('+');
    }
    line
}

fn build_row(cells: Vec<String>, widths: &[usize]) -> String {
    let mut row = String::new();
    row.push('|');
    for (cell, width) in cells.iter().zip(widths.iter()) {
        row.push(' ');
        row.push_str(&format!("{:<width$}", cell, width = *width));
        row.push(' ');
        row.push('|');
    }
    row
}

fn spawn_account_stream_worker(
    cfg: &AccountStreamCfg,
    tx: UnboundedSender<PreTradeEvent>,
) -> Result<()> {
    if cfg.max_payload_bytes != ACCOUNT_PAYLOAD {
        warn!(
            "account_stream.max_payload_bytes={} unsupported, using {}",
            cfg.max_payload_bytes, ACCOUNT_PAYLOAD
        );
    }

    let service = cfg.service.clone();
    let node_name = cfg
        .label
        .as_deref()
        .map(|label| format!("{}_{}", NODE_PRE_TRADE_ACCOUNT, sanitize_suffix(label)))
        .unwrap_or_else(|| NODE_PRE_TRADE_ACCOUNT.to_string());

    tokio::task::spawn_local(async move {
        if let Err(err) = account_stream_loop(node_name, service, tx).await {
            error!("account stream worker exited: {err:?}");
        }
    });
    Ok(())
}

fn spawn_trade_response_worker(
    cfg: &TradeEngineRespCfg,
    tx: UnboundedSender<PreTradeEvent>,
) -> Result<()> {
    if cfg.max_payload_bytes != TRADE_RESP_PAYLOAD {
        warn!(
            "trade_engine.max_payload_bytes={} unsupported, using {}",
            cfg.max_payload_bytes, TRADE_RESP_PAYLOAD
        );
    }

    let service = cfg.service.clone();
    let node_name = cfg
        .label
        .as_deref()
        .map(|label| format!("{}_{}", NODE_PRE_TRADE_TRADE_RESP, sanitize_suffix(label)))
        .unwrap_or_else(|| NODE_PRE_TRADE_TRADE_RESP.to_string());

    tokio::task::spawn_local(async move {
        if let Err(err) = trade_response_loop(node_name, service, tx).await {
            error!("trade response worker exited: {err:?}");
        }
    });
    Ok(())
}

fn spawn_signal_workers(
    cfg: &SignalSubscriptionsCfg,
    tx: UnboundedSender<PreTradeEvent>,
) -> Result<()> {
    if cfg.channels.is_empty() {
        info!("no signal channels configured");
        return Ok(());
    }
    if cfg.max_payload_bytes != SIGNAL_PAYLOAD {
        warn!(
            "signals.max_payload_bytes={} unsupported, using {}",
            cfg.max_payload_bytes, SIGNAL_PAYLOAD
        );
    }

    for channel in &cfg.channels {
        let channel_name = channel.clone();
        let node_name = signal_node_name(channel);
        let svc_name = format!("signal_pubs/{}", channel);
        let tx_clone = tx.clone();

        tokio::task::spawn_local(async move {
            if let Err(err) = signal_loop(node_name, svc_name, channel_name, tx_clone).await {
                error!("signal worker for channel exited: {err:?}");
            }
        });
    }

    Ok(())
}

async fn account_stream_loop(
    node_name: String,
    service: String,
    tx: UnboundedSender<PreTradeEvent>,
) -> Result<()> {
    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;

    let service = node
        .service_builder(&ServiceName::new(&service)?)
        .publish_subscribe::<[u8; ACCOUNT_PAYLOAD]>()
        .open_or_create()?;
    let subscriber: Subscriber<ipc::Service, [u8; ACCOUNT_PAYLOAD], ()> =
        service.subscriber_builder().create()?;
    let service_name = service.name().to_string();

    info!("account stream subscribed: service={}", service_name);

    loop {
        match subscriber.receive() {
            Ok(Some(sample)) => {
                let payload = trim_payload(sample.payload());
                let received_at = Utc::now();
                let payload_len = payload.len();
                let (event_type, event_time_ms) = extract_account_metadata(&payload);
                let event = AccountEvent {
                    service: service_name.clone(),
                    received_at,
                    payload,
                    payload_len,
                    event_type,
                    event_time_ms,
                };
                if tx.send(PreTradeEvent::Account(event)).is_err() {
                    break;
                }
            }
            Ok(None) => {
                tokio::task::yield_now().await;
            }
            Err(err) => {
                warn!("account stream receive error: {err}");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }

    Ok(())
}

async fn trade_response_loop(
    node_name: String,
    service: String,
    tx: UnboundedSender<PreTradeEvent>,
) -> Result<()> {
    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;

    let service = node
        .service_builder(&ServiceName::new(&service)?)
        .publish_subscribe::<[u8; TRADE_RESP_PAYLOAD]>()
        .open_or_create()?;
    let subscriber: Subscriber<ipc::Service, [u8; TRADE_RESP_PAYLOAD], ()> =
        service.subscriber_builder().create()?;
    let service_name = service.name().to_string();

    info!("trade response subscribed: service={}", service_name);

    loop {
        match subscriber.receive() {
            Ok(Some(sample)) => match parse_trade_response(sample.payload(), &service_name) {
                Ok(event) => {
                    if tx.send(PreTradeEvent::TradeResponse(event)).is_err() {
                        break;
                    }
                }
                Err(err) => {
                    warn!("failed to parse trade response: {err}");
                }
            },
            Ok(None) => {
                tokio::task::yield_now().await;
            }
            Err(err) => {
                warn!("trade response receive error: {err}");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }

    Ok(())
}

async fn signal_loop(
    node_name: String,
    service: String,
    channel_name: String,
    tx: UnboundedSender<PreTradeEvent>,
) -> Result<()> {
    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;

    let service = node
        .service_builder(&ServiceName::new(&service)?)
        .publish_subscribe::<[u8; SIGNAL_PAYLOAD]>()
        .open_or_create()?;
    let subscriber: Subscriber<ipc::Service, [u8; SIGNAL_PAYLOAD], ()> =
        service.subscriber_builder().create()?;
    let service_name = service.name().to_string();

    info!(
        "signal subscribed: service={}, channel={}",
        service_name, channel_name
    );

    loop {
        match subscriber.receive() {
            Ok(Some(sample)) => {
                let frame = trim_payload(sample.payload());
                let received_at = Utc::now();
                let frame_len = frame.len();
                match SignalEvent::parse(&frame) {
                    Ok(parsed) => {
                        let header = parsed.header;
                        if let Some(kind) = header.event_type() {
                            if kind == SignalEventType::TradeSignal {
                                match decode_trade_signal(&parsed) {
                                    Ok(data) => {
                                        drop(parsed);
                                        let event = TradeSignalEvent {
                                            channel: channel_name.clone(),
                                            received_at,
                                            frame_len,
                                            frame,
                                            header,
                                            data,
                                        };
                                        if tx.send(PreTradeEvent::Signal(event)).is_err() {
                                            break;
                                        }
                                    }
                                    Err(err) => {
                                        warn!(
                                            "failed to decode trade signal payload from channel {}: {}",
                                            channel_name, err
                                        );
                                    }
                                }
                            } else {
                                warn!(
                                    "unsupported signal event type {:?} on channel {}",
                                    kind, channel_name
                                );
                            }
                        } else {
                            warn!(
                                "unknown signal event type {} on channel {}",
                                header.event_type, channel_name
                            );
                        }
                    }
                    Err(err) => {
                        warn!(
                            "failed to parse signal frame from channel {}: {}",
                            channel_name, err
                        );
                    }
                }
            }
            Ok(None) => {
                tokio::task::yield_now().await;
            }
            Err(err) => {
                warn!("signal receive error (channel={}): {err}", channel_name);
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }

    Ok(())
}

fn parse_trade_response(
    payload: &[u8; TRADE_RESP_PAYLOAD],
    service_name: &str,
) -> Result<TradeEngineResponse> {
    let actual_len = payload
        .iter()
        .rposition(|&x| x != 0)
        .map(|pos| pos + 1)
        .unwrap_or(0);

    if actual_len < 40 {
        return Err(anyhow!(
            "trade response payload too short ({} bytes) from service {}",
            actual_len,
            service_name
        ));
    }

    let read_u32 = |offset: usize| -> u32 {
        u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap())
    };
    let read_i64 = |offset: usize| -> i64 {
        i64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap())
    };
    let read_u16 = |offset: usize| -> u16 {
        u16::from_le_bytes(payload[offset..offset + 2].try_into().unwrap())
    };

    let req_type = read_u32(0);
    let local_recv_time = read_i64(4);
    let client_order_id = read_i64(12);
    let exchange = read_u32(20);
    let status = read_u16(24);
    let ip_used_weight_1m = read_u32(28);
    let order_count_1m = read_u32(32);
    let body_len = read_u32(36) as usize;

    let available = actual_len.saturating_sub(40);
    let take = std::cmp::min(available, body_len);
    let body_truncated = take < body_len;
    let mut body = Vec::with_capacity(take);
    body.extend_from_slice(&payload[40..40 + take]);

    Ok(TradeEngineResponse {
        service: service_name.to_string(),
        received_at: Utc::now(),
        payload_len: actual_len,
        req_type,
        local_recv_time,
        client_order_id,
        exchange,
        status,
        ip_used_weight_1m: remap_u32(ip_used_weight_1m),
        order_count_1m: remap_u32(order_count_1m),
        body,
        body_truncated,
    })
}

fn remap_u32(value: u32) -> Option<u32> {
    if value == u32::MAX {
        None
    } else {
        Some(value)
    }
}

fn trim_payload<T: AsRef<[u8]>>(payload: T) -> Vec<u8> {
    let bytes = payload.as_ref();
    let actual_len = bytes
        .iter()
        .rposition(|&x| x != 0)
        .map(|pos| pos + 1)
        .unwrap_or(0);
    bytes[..actual_len].to_vec()
}

fn extract_account_metadata(payload: &[u8]) -> (Option<String>, Option<i64>) {
    match serde_json::from_slice::<serde_json::Value>(payload) {
        Ok(serde_json::Value::Object(map)) => {
            let event_type = map.get("e").and_then(|v| v.as_str()).map(|s| s.to_string());
            let event_time = map.get("E").and_then(|v| v.as_i64());
            (event_type, event_time)
        }
        _ => (None, None),
    }
}

fn signal_node_name(channel: &str) -> String {
    format!(
        "{}{}",
        NODE_PRE_TRADE_SIGNAL_PREFIX,
        sanitize_suffix(channel)
    )
}

fn sanitize_suffix(raw: &str) -> Cow<'_, str> {
    if raw.chars().all(is_valid_node_char) {
        return Cow::Borrowed(raw);
    }
    let sanitized: String = raw
        .chars()
        .map(|c| if is_valid_node_char(c) { c } else { '_' })
        .collect();
    Cow::Owned(sanitized)
}

fn is_valid_node_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

fn process_account_event(
    evt: &AccountEvent,
    spot_manager: &BinancePmSpotAccountManager,
    um_manager: &BinancePmUmAccountManager,
) -> Result<()> {
    if evt.payload.len() < 8 {
        anyhow::bail!("account payload too short: {} bytes", evt.payload.len());
    }

    let msg_type = get_account_event_type(&evt.payload);
    let payload_len = u32::from_le_bytes([
        evt.payload[4],
        evt.payload[5],
        evt.payload[6],
        evt.payload[7],
    ]) as usize;
    if evt.payload.len() < 8 + payload_len {
        anyhow::bail!(
            "account payload truncated: have {} expect {}",
            evt.payload.len(),
            8 + payload_len
        );
    }
    let data = &evt.payload[8..8 + payload_len];

    match msg_type {
        AccountEventType::BalanceUpdate => {
            let msg = BalanceUpdateMsg::from_bytes(data)?;
            spot_manager.apply_balance_delta(&msg.asset, msg.delta, msg.event_time);
            debug!(
                "balance_update: asset={} delta={} event_time={} tx_time={} update_id={}",
                msg.asset, msg.delta, msg.event_time, msg.transaction_time, msg.update_id
            );
        }
        AccountEventType::AccountUpdateBalance => {
            let msg = AccountUpdateBalanceMsg::from_bytes(data)?;
            spot_manager.apply_balance_snapshot(
                &msg.asset,
                msg.wallet_balance,
                msg.cross_wallet_balance,
                msg.balance_change,
                msg.event_time,
            );
            debug!(
                "account_update_balance: asset={} wallet={} cross={} change={} reason={}",
                msg.asset,
                msg.wallet_balance,
                msg.cross_wallet_balance,
                msg.balance_change,
                msg.reason
            );
        }
        AccountEventType::AccountUpdatePosition => {
            let msg = AccountUpdatePositionMsg::from_bytes(data)?;
            um_manager.apply_position_update(
                &msg.symbol,
                msg.position_side,
                msg.position_amount,
                msg.entry_price,
                msg.unrealized_pnl,
                msg.breakeven_price,
                msg.event_time,
            );
            debug!(
                "account_update_position: symbol={} side={} amount={} entry={} upnl={} reason={}",
                msg.symbol,
                msg.position_side,
                msg.position_amount,
                msg.entry_price,
                msg.unrealized_pnl,
                msg.reason
            );
        }
        _ => {}
    }

    Ok(())
}

fn log_account_event(evt: &AccountEvent) {
    debug!(
        "account msg: service={} received_at={} type={:?} account_ts={:?} bytes={}",
        evt.service, evt.received_at, evt.event_type, evt.event_time_ms, evt.payload_len
    );
}

fn log_trade_response(evt: &TradeEngineResponse) {
    debug!(
        "trade resp: service={} received_at={} req_type={} status={} client_order_id={} exchange={} bytes={} body_truncated={}",
        evt.service,
        evt.received_at,
        evt.req_type,
        evt.status,
        evt.client_order_id,
        evt.exchange,
        evt.payload_len,
        evt.body_truncated
    );
}

fn log_signal_event(evt: &TradeSignalEvent) {
    let evt_kind = evt
        .header
        .event_type()
        .map(|k| format!("{:?}", k))
        .unwrap_or_else(|| format!("unknown({})", evt.header.event_type));
    debug!(
        "signal: channel={} received_at={} kind={} frame_len={} payload_len={} event_ts={} publish_ts={} symbol={} exchange={}",
        evt.channel,
        evt.received_at,
        evt_kind,
        evt.frame_len,
        evt.header.payload_len,
        evt.header.event_ts_ms,
        evt.header.publish_ts_ms,
        evt.data.symbol,
        evt.data.exchange
    );
}
