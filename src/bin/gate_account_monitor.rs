//! Gate.io 账户监控程序
//!
//! 功能：
//! - 连接到 Gate.io 统一账户 WebSocket 频道
//! - 使用 API Key/Secret 进行鉴权
//! - 订阅 unified.asset_detail 频道
//! - 解析账户事件并通过 Iceoryx 转发
//! - 支持主备双路连接
//!
//! 使用方式：
//! ```bash
//! export GATE_API_KEY="your_api_key"
//! export GATE_API_SECRET="your_api_secret"
//! cargo run --bin gate_account_monitor
//! ```

use anyhow::Result;
use bytes::Bytes;
use log::{debug, error, info, warn};
use mkt_signal::common::basic_account_msg::{
    get_basic_event_type, split_basic_account_event, BasicAccountEventMsg, BasicAccountEventType,
    BasicAccountRiskMsg, BasicAccountScope, BasicBalanceMsg, BasicBorrowInterestMsg,
    BasicPositionMsg, BasicTradeLiteMsg, BasicUmUnrealizedMsg, GateBasicOrderMsg,
};
use mkt_signal::common::mkt_cfg::load_local_ips_preferring_trade_engine;
use mkt_signal::connection::connection::{MktConnection, MktConnectionHandler};
use mkt_signal::parser::gate_account_event_parser::GateAccountEventParser;
use mkt_signal::portfolio_margin::gate_auth::{GateCredentials, GatePrivateWsUrls};
use mkt_signal::portfolio_margin::gate_rest::fetch_borrow_interest;
use mkt_signal::portfolio_margin::gate_user_stream::{GateUserDataConnection, SubscribeChannel};
use mkt_signal::portfolio_margin::pm_forwarder::PmForwarder;
use mkt_signal::trade_engine::gate_query::gate_rest_get_with_headers;
use mkt_signal::trade_engine::query_parsers::gate_positions_snapshot::{
    parse_gate_positions_snapshot_with_meta, GatePositionsSnapshotParse,
};
use reqwest::Client;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tokio::signal;
use tokio::sync::{broadcast, watch};

fn credential_edges(value: &str) -> (String, String, usize) {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return (String::new(), String::new(), 0);
    }
    let chars: Vec<char> = trimmed.chars().collect();
    let len = chars.len();
    let prefix_len = len.min(4);
    let suffix_len = len.min(4);
    let first: String = chars.iter().take(prefix_len).collect();
    let last: String = chars.iter().skip(len.saturating_sub(suffix_len)).collect();
    (first, last, len)
}

fn log_credential_preview(label: &str, value: &str) {
    let (first4, last4, len) = credential_edges(value);
    if len == 0 {
        info!("{} not set or empty", label);
    } else {
        info!(
            "{} preview len={} first4='{}' last4='{}'",
            label, len, first4, last4
        );
    }
}

fn should_log_gate_ws_text(msg: &str) -> bool {
    !msg.contains(r#""channel":"unified.asset_detail""#)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    // 从环境变量加载 Gate.io 凭证
    let credentials = GateCredentials::from_env()?;
    log_credential_preview("GATE_API_KEY", &credentials.api_key);
    log_credential_preview("GATE_API_SECRET", &credentials.secret_key);

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    setup_signals(shutdown_tx.clone());

    // WebSocket URLs
    let unified_ws_url = GatePrivateWsUrls::UNIFIED.to_string();
    let spot_ws_url = GatePrivateWsUrls::SPOT.to_string();
    let futures_ws_url = GatePrivateWsUrls::FUTURES_USDT.to_string();

    // IP 设置（从 /home/<user>/mkt_pub/config/mkt_cfg.yaml 读取）
    let ((primary_ip, secondary_ip), ip_source) = load_local_ips_preferring_trade_engine().await?;
    let session_max = None; // Gate.io 没有明确的会话时长限制
    info!(
        "Primary IP='{}', Secondary IP='{}', session_max={:?} (local_ip_source: {})",
        primary_ip, secondary_ip, session_max, ip_source
    );

    // 统一账户频道：资产明细 + 账户级聚合风险。
    let unified_channels = vec![
        SubscribeChannel {
            channel: "unified.asset_detail".to_string(),
            payload: vec!["!all".to_string()],
        },
        SubscribeChannel {
            channel: "unified.assets".to_string(),
            payload: vec!["!all".to_string()],
        },
    ];

    // 现货频道 (spot.orders_v2)
    let spot_channels = vec![SubscribeChannel {
        channel: "spot.orders_v2".to_string(),
        payload: vec!["!all".to_string()],
    }];

    // 合约频道 (futures.orders)
    let futures_channels = vec![
        SubscribeChannel {
            channel: "futures.orders".to_string(),
            payload: vec!["!all".to_string()],
        },
        SubscribeChannel {
            channel: "futures.positions".to_string(),
            payload: vec!["!all".to_string()],
        },
    ];

    // 创建事件收集通道
    let (evt_tx, mut evt_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

    // 创建 PM 转发器 (account_pubs/gate/pm)
    let mut forwarder = PmForwarder::new("gate")?;
    let mut deduper = AccountEventDeduper::new(8192);
    let mut stats = tokio::time::interval(Duration::from_secs(30));
    let mut interest_poll =
        spawn_gate_borrow_interest_poll(credentials.clone(), evt_tx.clone(), shutdown_rx.clone());
    let mut positions_poll =
        spawn_gate_positions_poll(credentials.clone(), evt_tx.clone(), shutdown_rx.clone());

    // 启动统一账户主备双路连接 (unified.asset_detail)
    let mut unified_primary = spawn_gate_stream_path(
        "unified-primary",
        &unified_ws_url,
        primary_ip.clone(),
        credentials.clone(),
        unified_channels.clone(),
        shutdown_rx.clone(),
        evt_tx.clone(),
        session_max,
    );
    let mut unified_secondary = spawn_gate_stream_path(
        "unified-secondary",
        &unified_ws_url,
        secondary_ip.clone(),
        credentials.clone(),
        unified_channels.clone(),
        shutdown_rx.clone(),
        evt_tx.clone(),
        session_max,
    );

    // 启动现货主备双路连接 (spot.orders_v2)
    let mut spot_primary = spawn_gate_stream_path(
        "spot-primary",
        &spot_ws_url,
        primary_ip.clone(),
        credentials.clone(),
        spot_channels.clone(),
        shutdown_rx.clone(),
        evt_tx.clone(),
        session_max,
    );
    let mut spot_secondary = spawn_gate_stream_path(
        "spot-secondary",
        &spot_ws_url,
        secondary_ip.clone(),
        credentials.clone(),
        spot_channels.clone(),
        shutdown_rx.clone(),
        evt_tx.clone(),
        session_max,
    );

    // 启动合约主备双路连接 (futures.orders)
    let mut futures_primary = spawn_gate_stream_path(
        "futures-primary",
        &futures_ws_url,
        primary_ip.clone(),
        credentials.clone(),
        futures_channels.clone(),
        shutdown_rx.clone(),
        evt_tx.clone(),
        session_max,
    );
    let mut futures_secondary = spawn_gate_stream_path(
        "futures-secondary",
        &futures_ws_url,
        secondary_ip.clone(),
        credentials.clone(),
        futures_channels.clone(),
        shutdown_rx.clone(),
        evt_tx.clone(),
        session_max,
    );

    // 主循环：接收事件、去重、转发
    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => { break; }
            Some(msg) = evt_rx.recv() => {
                // 统一去重后再发送
                if deduper.should_forward(&msg) {
                    // 打印解析后的消息
                    log_parsed_event(&msg);
                    forwarder.send_raw(&msg);
                }
            }
            _ = stats.tick() => {
                forwarder.log_stats();
            }
            res = &mut interest_poll => {
                match res {
                    Ok(()) => warn!("gate interest poll task exited; restarting"),
                    Err(e) => warn!("gate interest poll task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    interest_poll = spawn_gate_borrow_interest_poll(
                        credentials.clone(),
                        evt_tx.clone(),
                        shutdown_rx.clone(),
                    );
                }
            }
            res = &mut positions_poll => {
                match res {
                    Ok(()) => warn!("gate positions poll task exited; restarting"),
                    Err(e) => warn!("gate positions poll task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    positions_poll = spawn_gate_positions_poll(
                        credentials.clone(),
                        evt_tx.clone(),
                        shutdown_rx.clone(),
                    );
                }
            }
            // 统一账户连接重启
            res = &mut unified_primary => {
                match res {
                    Ok(()) => warn!("unified-primary task exited; restarting"),
                    Err(e) => warn!("unified-primary task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    unified_primary = spawn_gate_stream_path(
                        "unified-primary",
                        &unified_ws_url,
                        primary_ip.clone(),
                        credentials.clone(),
                        unified_channels.clone(),
                        shutdown_rx.clone(),
                        evt_tx.clone(),
                        session_max,
                    );
                }
            }
            res = &mut unified_secondary => {
                match res {
                    Ok(()) => warn!("unified-secondary task exited; restarting"),
                    Err(e) => warn!("unified-secondary task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    unified_secondary = spawn_gate_stream_path(
                        "unified-secondary",
                        &unified_ws_url,
                        secondary_ip.clone(),
                        credentials.clone(),
                        unified_channels.clone(),
                        shutdown_rx.clone(),
                        evt_tx.clone(),
                        session_max,
                    );
                }
            }
            // 现货连接重启
            res = &mut spot_primary => {
                match res {
                    Ok(()) => warn!("spot-primary task exited; restarting"),
                    Err(e) => warn!("spot-primary task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    spot_primary = spawn_gate_stream_path(
                        "spot-primary",
                        &spot_ws_url,
                        primary_ip.clone(),
                        credentials.clone(),
                        spot_channels.clone(),
                        shutdown_rx.clone(),
                        evt_tx.clone(),
                        session_max,
                    );
                }
            }
            res = &mut spot_secondary => {
                match res {
                    Ok(()) => warn!("spot-secondary task exited; restarting"),
                    Err(e) => warn!("spot-secondary task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    spot_secondary = spawn_gate_stream_path(
                        "spot-secondary",
                        &spot_ws_url,
                        secondary_ip.clone(),
                        credentials.clone(),
                        spot_channels.clone(),
                        shutdown_rx.clone(),
                        evt_tx.clone(),
                        session_max,
                    );
                }
            }
            // 合约连接重启
            res = &mut futures_primary => {
                match res {
                    Ok(()) => warn!("futures-primary task exited; restarting"),
                    Err(e) => warn!("futures-primary task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    futures_primary = spawn_gate_stream_path(
                        "futures-primary",
                        &futures_ws_url,
                        primary_ip.clone(),
                        credentials.clone(),
                        futures_channels.clone(),
                        shutdown_rx.clone(),
                        evt_tx.clone(),
                        session_max,
                    );
                }
            }
            res = &mut futures_secondary => {
                match res {
                    Ok(()) => warn!("futures-secondary task exited; restarting"),
                    Err(e) => warn!("futures-secondary task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    futures_secondary = spawn_gate_stream_path(
                        "futures-secondary",
                        &futures_ws_url,
                        secondary_ip.clone(),
                        credentials.clone(),
                        futures_channels.clone(),
                        shutdown_rx.clone(),
                        evt_tx.clone(),
                        session_max,
                    );
                }
            }
        }
    }

    info!("Gate account monitor shutdown complete");
    Ok(())
}

fn setup_signals(shutdown_tx: watch::Sender<bool>) {
    tokio::spawn(async move {
        if signal::ctrl_c().await.is_ok() {
            info!("Received Ctrl-C, shutting down...");
            let _ = shutdown_tx.send(true);
        }
    });
}

fn spawn_gate_borrow_interest_poll(
    credentials: GateCredentials,
    evt_tx: tokio::sync::mpsc::UnboundedSender<Bytes>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let client = Client::new();
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        let mut previous_symbols: HashSet<String> = HashSet::new();

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                _ = ticker.tick() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }

                    match fetch_borrow_interest(&client, &credentials).await {
                        Ok(msgs) => {
                            let mut current_symbols: HashSet<String> = HashSet::new();
                            for msg in msgs {
                                current_symbols.insert(msg.symbol.clone());
                                let payload = msg.to_bytes();
                                let event = BasicAccountEventMsg::create(
                                    BasicAccountEventType::BorrowInterest,
                                    BasicAccountScope::GateUnified,
                                    payload,
                                );
                                if let Err(err) = evt_tx.send(event.to_bytes()) {
                                    warn!("failed to send gate borrow interest msg: {}", err);
                                }
                            }

                            let now_ms = chrono::Utc::now().timestamp_millis();
                            for symbol in previous_symbols.difference(&current_symbols) {
                                let clear_msg = BasicBorrowInterestMsg::create(
                                    now_ms,
                                    symbol.clone(),
                                    0.0,
                                    0.0,
                                );
                                let event = BasicAccountEventMsg::create(
                                    BasicAccountEventType::BorrowInterest,
                                    BasicAccountScope::GateUnified,
                                    clear_msg.to_bytes(),
                                );
                                if let Err(err) = evt_tx.send(event.to_bytes()) {
                                    warn!("failed to send cleared gate borrow interest msg: {}", err);
                                }
                            }

                            previous_symbols = current_symbols;
                        }
                        Err(err) => {
                            warn!("fetch gate borrow interest failed: {:?}", err);
                        }
                    }
                }
            }
        }
        info!("gate borrow interest poller exiting");
    })
}

fn spawn_gate_positions_poll(
    credentials: GateCredentials,
    evt_tx: tokio::sync::mpsc::UnboundedSender<Bytes>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let client = Client::new();
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        let mut previous_positions: HashSet<(String, char)> = HashSet::new();

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                _ = ticker.tick() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }

                    match gate_rest_get_with_headers(
                        &client,
                        &credentials,
                        "/api/v4/futures/usdt/positions",
                        "",
                        &[("X-Gate-Size-Decimal", "1")],
                    )
                    .await
                    {
                        Ok((200, body)) => {
                            match parse_gate_positions_snapshot_with_meta(&body) {
                                Some(parsed) => {
                                    let mut current_positions: HashSet<(String, char)> = HashSet::new();
                                    forward_gate_position_snapshot(
                                        &parsed,
                                        &evt_tx,
                                        &mut current_positions,
                                    );

                                    let now_ms = chrono::Utc::now().timestamp_millis();
                                    for (inst_id, side) in previous_positions.difference(&current_positions) {
                                        let zero_position = BasicPositionMsg::create(
                                            now_ms,
                                            inst_id.clone(),
                                            *side,
                                            0.0,
                                        );
                                        let zero_position_event = BasicAccountEventMsg::create(
                                            BasicAccountEventType::PositionUpdate,
                                            BasicAccountScope::GateUnified,
                                            zero_position.to_bytes(),
                                        );
                                        let _ = evt_tx.send(zero_position_event.to_bytes());

                                        let zero_pnl = BasicUmUnrealizedMsg::create(
                                            now_ms,
                                            inst_id.clone(),
                                            *side,
                                            0.0,
                                        );
                                        let zero_pnl_event = BasicAccountEventMsg::create(
                                            BasicAccountEventType::UnrealizedPnlUpdate,
                                            BasicAccountScope::GateUnified,
                                            zero_pnl.to_bytes(),
                                        );
                                        let _ = evt_tx.send(zero_pnl_event.to_bytes());
                                    }

                                    previous_positions = current_positions;
                                }
                                None => warn!("gate positions poll parse failed; body_len={}", body.len()),
                            }
                        }
                        Ok((status, body)) => {
                            warn!("gate positions poll http {} body={}", status, body);
                        }
                        Err(err) => {
                            warn!("gate positions poll failed: {:?}", err);
                        }
                    }
                }
            }
        }
        info!("gate positions poller exiting");
    })
}

fn forward_gate_position_snapshot(
    parsed: &GatePositionsSnapshotParse,
    evt_tx: &tokio::sync::mpsc::UnboundedSender<Bytes>,
    current_positions: &mut HashSet<(String, char)>,
) {
    for payload in parsed.msgs.iter().cloned() {
        let event_type = get_basic_event_type(&payload);
        if event_type == BasicAccountEventType::PositionUpdate {
            if let Ok(msg) = BasicPositionMsg::from_bytes(&payload) {
                current_positions.insert((msg.inst_id.clone(), msg.position_side));
            }
        }
        let wrapped =
            BasicAccountEventMsg::create(event_type, BasicAccountScope::GateUnified, payload);
        let _ = evt_tx.send(wrapped.to_bytes());
    }
}

fn spawn_gate_stream_path(
    name: &'static str,
    ws_url: &str,
    local_ip: String,
    credentials: GateCredentials,
    channels: Vec<SubscribeChannel>,
    shutdown_rx: watch::Receiver<bool>,
    evt_tx: tokio::sync::mpsc::UnboundedSender<Bytes>,
    session_max: Option<Duration>,
) -> tokio::task::JoinHandle<()> {
    let ws_url = ws_url.to_string();
    tokio::spawn(async move {
        loop {
            info!(
                "[{}] connecting to {} (local_ip='{}')",
                name, ws_url, local_ip
            );

            // 创建原始消息广播通道
            let (raw_tx, mut raw_rx) = broadcast::channel::<Bytes>(8192);

            // 创建 MktConnection
            let mut conn = MktConnection::new(
                ws_url.clone(),
                serde_json::json!({}), // Gate.io 在连接后发送订阅消息
                raw_tx.clone(),
                shutdown_rx.clone(),
            );
            if !local_ip.is_empty() {
                conn.local_ip = Some(local_ip.clone());
            }

            // 创建 Gate 用户数据连接（每次重连时会重新生成带新签名的订阅消息）
            let mut runner = GateUserDataConnection::new(
                conn,
                credentials.clone(),
                channels.clone(),
                session_max,
            );

            // 启动消费者任务
            let mut consumer_shutdown = shutdown_rx.clone();
            let evt_tx_clone = evt_tx.clone();
            let local_ip_log = local_ip.clone();
            let parser = GateAccountEventParser::new();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        msg = raw_rx.recv() => {
                            match msg {
                                Ok(b) => {
                                    if let Ok(s) = std::str::from_utf8(&b) {
                                        let report = parser.parse_with_report(b.clone(), &evt_tx_clone);
                                        if should_log_gate_ws_text(s) && !report.complete {
                                            info!("[{}][ip={}] gate ws json: {}", name, local_ip_log, s);
                                        }
                                    } else {
                                        info!(
                                            "[{}][ip={}] gate ws bin: {} bytes",
                                            name,
                                            local_ip_log,
                                            b.len()
                                        );
                                        let _ = parser.parse_with_report(b.clone(), &evt_tx_clone);
                                    }
                                }
                                Err(broadcast::error::RecvError::Closed) => break,
                                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                    warn!("[{}] lagged: skipped {} msgs", name, skipped);
                                }
                            }
                        }
                        _ = consumer_shutdown.changed() => {
                            if *consumer_shutdown.borrow() { break; }
                        }
                    }
                }
            });

            // 运行连接直到退出（关闭或错误）
            if let Err(e) = runner.start_ws().await {
                error!("[{}] connection error: {}", name, e);
            }

            // 检查是否需要关闭
            if *shutdown_rx.borrow() {
                info!("[{}] shutdown signal received, exiting", name);
                break;
            }

            // 等待2秒后重连
            info!("[{}] connection closed, reconnecting in 2s...", name);
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    })
}

/// 打印解析后的账户事件
fn log_parsed_event(msg: &Bytes) {
    let Some((event_type, account_scope, payload)) = split_basic_account_event(msg.as_ref()) else {
        return;
    };

    if matches!(event_type, BasicAccountEventType::Error) {
        return;
    }

    match event_type {
        BasicAccountEventType::BalanceUpdate => {}
        BasicAccountEventType::BorrowInterest => {
            if let Ok(m) = BasicBorrowInterestMsg::from_bytes(&payload) {
                info!(
                    "Gate BorrowInterest: scope={} ts={} symbol={} borrowed={} interest={}",
                    account_scope.as_str(),
                    m.timestamp,
                    m.symbol,
                    m.borrowed,
                    m.interest
                );
            }
        }
        BasicAccountEventType::PositionUpdate => {
            if let Ok(m) = BasicPositionMsg::from_bytes(&payload) {
                debug!(
                    "Gate PositionUpdate: scope={} ts={} inst={} side={} pos={}",
                    account_scope.as_str(),
                    m.timestamp,
                    m.inst_id,
                    m.position_side,
                    m.position_amount
                );
            }
        }
        BasicAccountEventType::OrderUpdate => {
            if let Ok(m) = GateBasicOrderMsg::from_bytes(&payload) {
                let label = if m.execution_type == 5 {
                    "Gate TradeUpdate"
                } else {
                    "Gate OrderUpdate"
                };
                info!(
                    "{}: scope={} venue={} ts={} symbol={} oid={} cloid={} side={} type={} exec={} status={} maker={} px={} qty={} filled={} fill_px={}",
                    label,
                    account_scope.as_str(),
                    m.venue, m.event_time, m.symbol, m.order_id, m.client_order_id,
                    m.side, m.order_type, m.execution_type, m.order_status,
                    m.is_maker, m.price, m.quantity, m.cumulative_filled_quantity, m.last_executed_price
                );
            }
        }
        BasicAccountEventType::UnrealizedPnlUpdate => {
            if let Ok(m) = BasicUmUnrealizedMsg::from_bytes(&payload) {
                debug!(
                    "Gate UnrealizedPnl: scope={} ts={} inst={} side={} pnl={}",
                    account_scope.as_str(),
                    m.timestamp,
                    m.inst_id,
                    m.position_side,
                    m.unrealized_pnl
                );
            }
        }
        BasicAccountEventType::AccountRisk => {
            if let Ok(m) = BasicAccountRiskMsg::from_bytes(&payload) {
                info!(
                    "Gate AccountRisk: scope={} ts={} adj_eq_usd={:.2} actual_eq_usd={:.2} maint_margin_usd={:.2} initial_margin_usd={:.2} margin_ratio={:.6}",
                    account_scope.as_str(),
                    m.timestamp,
                    m.adj_equity_usd,
                    m.actual_equity_usd,
                    m.maintenance_margin_usd,
                    m.initial_margin_usd,
                    m.margin_ratio
                );
            }
        }
        _ => {
            info!(
                "Gate basic msg: scope={} type={:?}",
                account_scope.as_str(),
                event_type
            );
        }
    }
}

/// 统一的账户事件去重器
struct AccountEventDeduper {
    seen: HashSet<u64>,
    order: VecDeque<u64>,
    capacity: usize,
}

impl AccountEventDeduper {
    fn new(capacity: usize) -> Self {
        Self {
            seen: HashSet::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    /// 检查是否应该转发此消息（返回 true 表示应该转发，false 表示重复消息）
    fn should_forward(&mut self, msg: &Bytes) -> bool {
        let Some((event_type, account_scope, payload)) = split_basic_account_event(msg.as_ref())
        else {
            return true;
        };

        // 根据事件类型计算去重 key
        let key_opt = match event_type {
            BasicAccountEventType::BalanceUpdate => BasicBalanceMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_balance(&msg)),
            BasicAccountEventType::BorrowInterest => BasicBorrowInterestMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_borrow_interest(&msg)),
            BasicAccountEventType::PositionUpdate => BasicPositionMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_position(&msg)),
            BasicAccountEventType::OrderUpdate => GateBasicOrderMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_order(&msg)),
            BasicAccountEventType::UnrealizedPnlUpdate => {
                BasicUmUnrealizedMsg::from_bytes(&payload)
                    .ok()
                    .map(|msg| self.key_unrealized_pnl(&msg))
            }
            BasicAccountEventType::AccountRisk => BasicAccountRiskMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_account_risk(&msg)),
            BasicAccountEventType::TradeUpdateLite => BasicTradeLiteMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_trade_lite(&msg)),
            _ => return true,
        };

        let Some(key) = key_opt else {
            return true; // 解析失败，直接转发
        };

        let key = self.hash64(&[account_scope as u32 as u64, key]);

        // 检查是否重复
        if self.seen.contains(&key) {
            return false; // 重复消息，不转发
        }

        // 记录新消息
        self.seen.insert(key);
        self.order.push_back(key);

        // 容量控制
        if self.order.len() > self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.seen.remove(&old);
            }
        }

        true // 新消息，转发
    }

    fn hash64(&self, parts: &[u64]) -> u64 {
        let mut hasher = DefaultHasher::new();
        for p in parts {
            p.hash(&mut hasher);
        }
        hasher.finish()
    }

    fn hash_str64(&self, s: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }

    fn key_balance(&self, msg: &BasicBalanceMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::BalanceUpdate as u32 as u64,
            msg.timestamp as u64,
            self.hash_str64(&msg.symbol),
            msg.wallet.to_bits(),
        ])
    }

    fn key_borrow_interest(&self, msg: &BasicBorrowInterestMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::BorrowInterest as u32 as u64,
            msg.timestamp as u64,
            self.hash_str64(&msg.symbol),
            msg.borrowed.to_bits(),
            msg.interest.to_bits(),
        ])
    }

    fn key_order(&self, msg: &GateBasicOrderMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::OrderUpdate as u32 as u64,
            msg.order_id as u64,
            msg.client_order_id as u64,
            msg.event_time as u64,
            msg.order_status as u64,
            msg.cumulative_filled_quantity.to_bits(),
        ])
    }

    fn key_position(&self, msg: &BasicPositionMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::PositionUpdate as u32 as u64,
            msg.timestamp as u64,
            self.hash_str64(&msg.inst_id),
            msg.position_side as u8 as u64,
            msg.position_amount.to_bits() as u64,
        ])
    }

    fn key_unrealized_pnl(&self, msg: &BasicUmUnrealizedMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::UnrealizedPnlUpdate as u32 as u64,
            msg.timestamp as u64,
            self.hash_str64(&msg.inst_id),
            msg.position_side as u8 as u64,
            msg.unrealized_pnl.to_bits(),
        ])
    }

    fn key_account_risk(&self, msg: &BasicAccountRiskMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::AccountRisk as u32 as u64,
            msg.timestamp as u64,
            msg.adj_equity_usd.to_bits(),
            msg.maintenance_margin_usd.to_bits(),
            msg.margin_ratio.to_bits(),
        ])
    }

    fn key_trade_lite(&self, msg: &BasicTradeLiteMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::TradeUpdateLite as u32 as u64,
            msg.client_order_id as u64,
            self.hash_str64(msg.trade_id_str()),
            msg.event_time as u64,
            msg.last_executed_price.to_bits(),
            msg.last_executed_quantity.to_bits(),
        ])
    }
}
