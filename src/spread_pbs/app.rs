use anyhow::{bail, Context, Result};
use bytes::Bytes;
use std::collections::HashMap;
use std::rc::Rc;
use tokio::sync::{mpsc, watch};

use crate::common::mkt_msg::AskBidSpreadMsg;
use crate::common::time_util::get_timestamp_us;
use crate::mkt_pub::cfg::Config;
use crate::mkt_pub::sub_msg::SubscribeMsgs;
use crate::signal::common::TradingVenue;

use crate::spread_pbs::latency::LatencyKll;
use crate::spread_pbs::okex::{parse_bbo_tbt, OkexBboFrame};
use crate::spread_pbs::publisher::SpreadPublisher;
use crate::spread_pbs::ws::run_okex_public_ws;

const OKEX_PUBLIC_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";

pub struct SpreadPbsApp {
    config: Config,
}

impl SpreadPbsApp {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// 主入口：拉 OKex 全 symbol → 起双路 ws → 单 task 内 dedup + publish + KLL。
    ///
    /// 必须在 `LocalSet` 上下文里 await（main 用 `LocalSet::run_until`）。
    pub async fn run(self) -> Result<()> {
        let venue = self.config.venue;
        if !matches!(venue, TradingVenue::OkexMargin | TradingVenue::OkexFutures) {
            bail!(
                "spread_pbs 当前仅支持 okex venues，收到 {:?}（其他 venue 解析后续接入）",
                venue
            );
        }
        if !self.config.data_types.enable_ask_bid_spread {
            bail!("mkt_cfg.data_types.enable_ask_bid_spread 必须为 true");
        }

        let venue_slug: &'static str = venue.data_pub_slug();
        log::info!("spread_pbs starting venue={}", venue_slug);

        // ---- 拉 symbol list & 构造订阅消息 ----
        let sub_msgs = SubscribeMsgs::new(&self.config).await;
        let n_msgs = sub_msgs.get_ask_bid_spread_subscribe_msg_len();
        if n_msgs == 0 {
            bail!("没有 ask_bid_spread 订阅消息（symbol list 为空？）");
        }
        let subscribe_payloads: Vec<serde_json::Value> = (0..n_msgs)
            .map(|i| sub_msgs.get_ask_bid_spread_subscribe_msg(i).clone())
            .collect();
        log::info!(
            "spread_pbs[{}] subscribe payloads prepared: {} batches",
            venue_slug, n_msgs
        );

        // ---- IceOryx publisher ----
        let publisher = Rc::new(
            SpreadPublisher::new(venue_slug)
                .with_context(|| format!("create iceoryx publisher for {}", venue_slug))?,
        );

        // ---- 双路 ws ----
        let (primary_tx, mut primary_rx) = mpsc::unbounded_channel::<Bytes>();
        let (secondary_tx, mut secondary_rx) = mpsc::unbounded_channel::<Bytes>();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        tokio::task::spawn_local(run_okex_public_ws(
            "primary",
            OKEX_PUBLIC_WS_URL.to_string(),
            self.config.primary_local_ip.clone(),
            subscribe_payloads.clone(),
            primary_tx,
            shutdown_rx.clone(),
        ));
        tokio::task::spawn_local(run_okex_public_ws(
            "secondary",
            OKEX_PUBLIC_WS_URL.to_string(),
            self.config.secondary_local_ip.clone(),
            subscribe_payloads,
            secondary_tx,
            shutdown_rx.clone(),
        ));

        // ---- 单 task dedup + publish + KLL ----
        let mut state = ProcessState {
            dedup: HashMap::with_capacity(2048),
            latency: LatencyKll::new(venue_slug),
            publisher: publisher.clone(),
            published: 0u64,
            dropped_by_seq: 0u64,
            stats_due_at: tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        };

        loop {
            tokio::select! {
                biased;
                _ = tokio::signal::ctrl_c() => {
                    log::info!("spread_pbs[{}] SIGINT received, shutting down", venue_slug);
                    let _ = shutdown_tx.send(true);
                    break;
                }
                Some(frame) = primary_rx.recv() => {
                    state.handle_frame("primary", &frame);
                }
                Some(frame) = secondary_rx.recv() => {
                    state.handle_frame("secondary", &frame);
                }
            }
            state.tick_stats(venue_slug);
        }

        Ok(())
    }
}

struct ProcessState {
    dedup: HashMap<String, i64>,
    latency: LatencyKll,
    publisher: Rc<SpreadPublisher>,
    published: u64,
    dropped_by_seq: u64,
    stats_due_at: tokio::time::Instant,
}

impl ProcessState {
    fn handle_frame(&mut self, label: &'static str, raw: &[u8]) {
        // Most OKex public ws frames are short JSON text; reject non-utf8 immediately.
        let text = match std::str::from_utf8(raw) {
            Ok(s) => s,
            Err(_) => return,
        };
        // 跳过控制帧（subscribe ack / pong）。OKex 的 bbo-tbt 业务帧总是以 `{"arg":` 开头。
        if !text.contains("\"channel\":\"bbo-tbt\"") {
            return;
        }

        let frames = match parse_bbo_tbt(text) {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "spread_pbs[{}] parse_bbo_tbt failed: {:#} payload={}",
                    label, e, text
                );
                return;
            }
        };

        let local_us = get_timestamp_us();
        for f in frames {
            self.process_frame(local_us, f);
        }
    }

    fn process_frame(&mut self, local_us: i64, f: OkexBboFrame) {
        let prev = self.dedup.get(&f.symbol).copied().unwrap_or(i64::MIN);
        if f.seq_id <= prev {
            self.dropped_by_seq += 1;
            return;
        }
        self.dedup.insert(f.symbol.clone(), f.seq_id);

        let msg = AskBidSpreadMsg::create(
            f.symbol.clone(),
            f.ts_ms,
            f.bid_price,
            f.bid_amount,
            f.ask_price,
            f.ask_amount,
        );
        let bytes = msg.to_bytes();
        if let Err(e) = self.publisher.publish(&bytes) {
            log::warn!("spread_pbs publish failed: {:#}", e);
            return;
        }
        self.published += 1;

        let okex_us = f.ts_ms.saturating_mul(1000);
        let delta = (local_us - okex_us) as f64;
        self.latency.push(delta);
    }

    fn tick_stats(&mut self, venue_slug: &str) {
        let now = tokio::time::Instant::now();
        if now < self.stats_due_at {
            return;
        }
        log::info!(
            "spread_pbs[{}] stats published={} dropped_by_seq={} symbols_seen={}",
            venue_slug, self.published, self.dropped_by_seq, self.dedup.len()
        );
        self.stats_due_at = now + std::time::Duration::from_secs(30);
    }
}
