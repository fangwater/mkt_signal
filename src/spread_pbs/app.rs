use anyhow::{bail, Context, Result};
use bytes::Bytes;
use std::collections::HashMap;
use std::rc::Rc;
use tokio::sync::{mpsc, watch};

use crate::common::mkt_msg::AskBidSpreadMsg;
use crate::common::time_util::get_timestamp_us;
use crate::mkt_pub::cfg::Config;

use crate::spread_pbs::adapter::{create_adapter, BboFrame, VenueAdapter};
use crate::spread_pbs::latency::LatencyKll;
use crate::spread_pbs::publisher::SpreadPublisher;
use crate::spread_pbs::ws::{run_public_ws, WsLoopParams};

pub struct SpreadPbsApp {
    config: Config,
}

impl SpreadPbsApp {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// 主入口：拉 venue 全 symbol → 起双路 ws → 单 task 内 dedup + publish + KLL。
    ///
    /// 必须在 `LocalSet` 上下文里 await（`main` 用 `LocalSet::run_until`）。
    pub async fn run(self) -> Result<()> {
        let venue = self.config.venue;
        let venue_slug: &'static str = venue.data_pub_slug();

        let adapter = match create_adapter(venue) {
            Some(a) => Rc::<dyn VenueAdapter>::from(a),
            None => bail!(
                "spread_pbs 当前不支持 venue {:?}（仅 OKex/Binance/Bybit/Gate/Bitget × spot+futures）",
                venue
            ),
        };
        log::info!(
            "spread_pbs starting venue={} adapter={}",
            venue_slug,
            adapter.name()
        );

        // ---- 拉 symbol list & 构造订阅消息 ----
        let symbols = self
            .config
            .get_symbols()
            .await
            .with_context(|| format!("fetch symbols for {}", venue_slug))?;
        if symbols.is_empty() {
            bail!("symbol list 为空（{}）", venue_slug);
        }
        let subscribe_msgs = adapter.build_subscribe(&symbols);
        if subscribe_msgs.is_empty() {
            bail!("adapter.build_subscribe 返回空（{} symbols 数={}）", venue_slug, symbols.len());
        }
        log::info!(
            "spread_pbs[{}] symbols={} subscribe_batches={}",
            venue_slug,
            symbols.len(),
            subscribe_msgs.len()
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

        let url = adapter.ws_url();
        let primary_params = WsLoopParams {
            label: "primary",
            url: url.clone(),
            local_ip: self.config.primary_local_ip.clone(),
            subscribe_msgs: subscribe_msgs.clone(),
            keepalive: adapter.keepalive(),
        };
        let secondary_params = WsLoopParams {
            label: "secondary",
            url,
            local_ip: self.config.secondary_local_ip.clone(),
            subscribe_msgs,
            keepalive: adapter.keepalive(),
        };
        tokio::task::spawn_local(run_public_ws(primary_params, primary_tx, shutdown_rx.clone()));
        tokio::task::spawn_local(run_public_ws(
            secondary_params,
            secondary_tx,
            shutdown_rx.clone(),
        ));

        // ---- 单 task dedup + publish + KLL ----
        let mut state = ProcessState {
            adapter: Rc::clone(&adapter),
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
    adapter: Rc<dyn VenueAdapter>,
    dedup: HashMap<String, i64>,
    latency: LatencyKll,
    publisher: Rc<SpreadPublisher>,
    published: u64,
    dropped_by_seq: u64,
    stats_due_at: tokio::time::Instant,
}

impl ProcessState {
    fn handle_frame(&mut self, label: &'static str, raw: &[u8]) {
        let text = match std::str::from_utf8(raw) {
            Ok(s) => s,
            Err(_) => return,
        };

        let frames = match self.adapter.parse_frame(text) {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "spread_pbs[{}] adapter.parse_frame failed: {:#} payload={}",
                    label, e, text
                );
                return;
            }
        };
        if frames.is_empty() {
            return;
        }

        let local_us = get_timestamp_us();
        for f in frames {
            self.process_frame(local_us, f);
        }
    }

    fn process_frame(&mut self, local_us: i64, f: BboFrame) {
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

        // 仅在 venue 提供 ts 时统计端到端延迟（spot bookTicker 无 ts，跳过）
        if f.ts_ms > 0 {
            let okex_us = f.ts_ms.saturating_mul(1000);
            let delta = (local_us - okex_us) as f64;
            self.latency.push(delta);
        }
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
