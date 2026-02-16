//! Depth Publisher 应用主模块
//!
//! 订阅 mkt_pub 的 incremental 数据，维护订单簿，发布深度快照

use anyhow::Result;
use iceoryx2::active_request::ActiveRequest;
use iceoryx2::port::{server::Server, subscriber::Subscriber};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use super::cfg::DepthPubConfig;
use super::depth_msg::DepthMsg;
use super::orderbook::OrderBook;
use super::publisher::DepthMsgPublisher;
use super::query_msg::{
    resp_status_name, DepthQueryHeader, DepthQueryLoadTlenBatchReq, DepthQueryLoadTlenBatchResp,
    DepthQueryLoadTlenSingleReq, DepthQueryLoadTlenSingleResp, DepthQueryType, DEPTH_QUERY_PAYLOAD,
    RESP_STATUS_BAD_REQUEST, RESP_STATUS_OK, RESP_STATUS_PAYLOAD_TOO_LARGE,
    RESP_STATUS_UNSUPPORTED_TYPE,
};
use crate::signal::common::TradingVenue;
use crate::signal::venue_min_qty_table::VenueMinQtyTable;

/// IceOryx 增量消息缓冲区大小 (与 mkt_pub 一致)
const INC_MAX_BYTES: usize = 2048;
const TIMER_CHECK_EVERY_INCS: u64 = 500;
const IDLE_SLEEP_MICROS: u64 = 100;
/// 滑动窗口大小：用于去重的最近 update_id 数量
const DEDUP_WINDOW_SIZE: usize = 256;
const KEEPALIVE_PUSH_INTERVAL_MS: u64 = 1000;
const BTC_DEPTH25_LOG_INTERVAL_SECS: u64 = 30;
const PUBLISH_OUTCOME_LOG_INTERVAL_SECS: u64 = 10;
const DEPTH_QUERY_SERVICE_PREFIX: &str = "depth_queries";

type DepthQueryServer = Server<ipc::Service, [u8], (), [u8], ()>;
type DepthQueryActiveRequest = ActiveRequest<ipc::Service, [u8], (), [u8], ()>;

/// 每个 symbol 的状态
struct SymbolState {
    orderbook: OrderBook,
    last_push_time: Instant,
    /// 滑动窗口：存储最近处理过的 (update_id, chunk_index)，用于去重
    /// VecDeque 维护插入顺序（用于淘汰最旧的）
    recent_msg_keys: VecDeque<(i64, u8)>,
    /// HashSet 用于 O(1) 快速查找
    msg_key_set: HashSet<(i64, u8)>,
}

impl SymbolState {
    fn new() -> Self {
        Self {
            orderbook: OrderBook::new(),
            last_push_time: Instant::now(),
            recent_msg_keys: VecDeque::with_capacity(DEDUP_WINDOW_SIZE),
            msg_key_set: HashSet::with_capacity(DEDUP_WINDOW_SIZE),
        }
    }

    /// 检查 (update_id, chunk_index) 是否重复
    /// 返回 true 表示是重复的，应该跳过
    #[inline]
    fn is_duplicate(&mut self, update_id: i64, chunk_index: u8) -> bool {
        let key = (update_id, chunk_index);

        // O(1) 查找
        if self.msg_key_set.contains(&key) {
            return true;
        }

        // 不重复，加入窗口
        if self.recent_msg_keys.len() >= DEDUP_WINDOW_SIZE {
            // 淘汰最旧的
            if let Some(old_key) = self.recent_msg_keys.pop_front() {
                self.msg_key_set.remove(&old_key);
            }
        }
        self.recent_msg_keys.push_back(key);
        self.msg_key_set.insert(key);
        false
    }
}

/// Depth Publisher 应用
pub struct DepthPubApp {
    config: DepthPubConfig,
    venue: TradingVenue,
    venue_slug: String,
    publisher: DepthMsgPublisher,
    subscriber: Subscriber<ipc::Service, [u8; INC_MAX_BYTES], ()>,
    query_server: DepthQueryServer,
    min_qty_table: VenueMinQtyTable,
    /// symbol -> SymbolState
    symbols: HashMap<String, SymbolState>,
    /// 推送间隔
    push_interval: Duration,
    /// 统计
    update_count: u64,
    push_count: u64,
    publish_success_count: u64,
    publish_fail_invalid_count: u64,
    publish_fail_send_count: u64,
    publish_fail_missing_side_count: u64,
    publish_fail_crossed_book_count: u64,
    timer_check_counter: u64,
    idle_check_counter: u64,
    idle_check_every: u64,
    last_btc_depth25_log: Instant,
    last_publish_outcome_log: Instant,
}

impl DepthPubApp {
    /// 创建应用实例
    /// venue: 例如 TradingVenue::BinanceFutures
    pub async fn new(config: DepthPubConfig, venue: TradingVenue) -> Result<Self> {
        let venue_slug = venue.data_pub_slug();
        let push_interval = Duration::from_millis(KEEPALIVE_PUSH_INTERVAL_MS);
        let idle_check_every = std::cmp::max(
            1,
            (push_interval.as_micros() / IDLE_SLEEP_MICROS as u128) as u64,
        );

        let mut min_qty_table = VenueMinQtyTable::new(venue);
        min_qty_table.refresh().await?;

        // 创建发布器
        let publisher = DepthMsgPublisher::new(
            venue_slug,
            config.depth_levels.enable_depth25,
            config.depth_levels.enable_depth50,
        )?;

        // 创建订阅器
        let subscriber = Self::create_subscriber(publisher.node(), venue_slug)?;
        info!(
            "Subscribed to incremental channel: dat_pbs/{}/incremental",
            venue_slug
        );

        let query_server = Self::create_query_server(publisher.node(), venue_slug)?;

        info!(
            "DepthPubApp created for {}: keepalive_push_interval={}ms, depth25={}, depth50={}",
            venue_slug,
            KEEPALIVE_PUSH_INTERVAL_MS,
            config.depth_levels.enable_depth25,
            config.depth_levels.enable_depth50
        );

        Ok(Self {
            config,
            venue,
            venue_slug: venue_slug.to_string(),
            publisher,
            subscriber,
            query_server,
            min_qty_table,
            symbols: HashMap::new(),
            push_interval,
            update_count: 0,
            push_count: 0,
            publish_success_count: 0,
            publish_fail_invalid_count: 0,
            publish_fail_send_count: 0,
            publish_fail_missing_side_count: 0,
            publish_fail_crossed_book_count: 0,
            timer_check_counter: 0,
            idle_check_counter: 0,
            idle_check_every,
            last_btc_depth25_log: Instant::now(),
            last_publish_outcome_log: Instant::now(),
        })
    }

    /// 创建 IceOryx 订阅器
    fn create_subscriber(
        node: &Node<ipc::Service>,
        venue: &str,
    ) -> Result<Subscriber<ipc::Service, [u8; INC_MAX_BYTES], ()>> {
        let service_name = format!("dat_pbs/{}/incremental", venue);
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; INC_MAX_BYTES]>()
            .open()?;

        let subscriber = service.subscriber_builder().create()?;
        Ok(subscriber)
    }

    fn create_query_server(node: &Node<ipc::Service>, venue: &str) -> Result<DepthQueryServer> {
        let service_name = format!("{}/{}", DEPTH_QUERY_SERVICE_PREFIX, venue);
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .request_response::<[u8], [u8]>()
            .max_active_requests_per_client(64)
            .max_loaned_requests(64)
            .max_response_buffer_size(64)
            .max_clients(64)
            .max_servers(1)
            .open_or_create()?;

        let server = service
            .server_builder()
            .initial_max_slice_len(DEPTH_QUERY_PAYLOAD)
            .max_loaned_responses_per_request(1)
            .create()?;

        info!("Depth query sync server ready: {}", service_name);
        Ok(server)
    }

    /// 主循环
    pub fn run(&mut self) -> Result<()> {
        info!("DepthMsgApp[{}] starting main loop", self.venue_slug);
        let mut last_stats_time = Instant::now();
        let stats_interval = Duration::from_secs(60);

        loop {
            // 处理订阅器的消息
            let mut has_message = false;
            while let Some(sample) = self.subscriber.receive()? {
                has_message = true;
                // 复制数据以避免借用冲突
                let data = sample.payload().to_vec();
                self.process_message(&data);
            }

            while let Some(active_request) = self.query_server.receive()? {
                has_message = true;
                self.handle_query_request(active_request);
            }

            // 如果没有消息，短暂休眠避免 CPU 空转
            if !has_message {
                self.idle_check_counter += 1;
                if self.idle_check_counter >= self.idle_check_every {
                    self.idle_check_counter = 0;
                    self.check_timer_push();
                }
                std::thread::sleep(Duration::from_micros(IDLE_SLEEP_MICROS));
            } else {
                self.idle_check_counter = 0;
            }

            // 定期打印统计
            if last_stats_time.elapsed() >= stats_interval {
                self.log_stats();
                last_stats_time = Instant::now();
            }
        }
    }

    /// 处理增量消息
    fn process_message(&mut self, data: &[u8]) {
        // 解析消息类型
        if data.len() < 8 {
            return;
        }

        let msg_type = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        // OrderBookInc = 1005
        if msg_type != 1005 {
            return;
        }

        // 解析 symbol
        let symbol_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        if data.len() < 8 + symbol_len + 32 {
            return;
        }

        let symbol = match std::str::from_utf8(&data[8..8 + symbol_len]) {
            Ok(s) => s.to_string(),
            Err(_) => return,
        };

        // 解析 update_id 和 timestamp
        let mut offset = 8 + symbol_len;
        let _first_update_id = i64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);
        offset += 8;

        let final_update_id = i64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);
        offset += 8;

        let timestamp = i64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);
        offset += 8;

        // is_snapshot (1 byte) + padding (7 bytes, padding[0] is is_last, padding[1] is chunk_index)
        let is_snapshot = data[offset] != 0;
        let is_last = data[offset + 1] != 0;
        let chunk_index = data[offset + 2];
        offset += 8;

        // bids_count 和 asks_count
        if data.len() < offset + 8 {
            return;
        }
        let bids_count = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;
        let asks_count = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        // 解析 levels
        let total_levels = bids_count + asks_count;
        if data.len() < offset + total_levels * 16 {
            return;
        }

        let mut bids = Vec::with_capacity(bids_count);
        let mut asks = Vec::with_capacity(asks_count);

        for _ in 0..bids_count {
            let price = f64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);
            offset += 8;
            let amount = f64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);
            offset += 8;
            bids.push((price, amount));
        }

        for _ in 0..asks_count {
            let price = f64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);
            offset += 8;
            let amount = f64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);
            offset += 8;
            asks.push((price, amount));
        }

        // 更新订单簿
        let state = self
            .symbols
            .entry(symbol.clone())
            .or_insert_with(SymbolState::new);

        // 滑动窗口去重：检查 (update_id, chunk_index) 是否已处理过
        if state.is_duplicate(final_update_id, chunk_index) {
            debug!(
                "Duplicate msg (update_id={}, chunk_index={}) for {}, skipping",
                final_update_id, chunk_index, symbol
            );
            return;
        }

        if is_snapshot {
            state
                .orderbook
                .apply_snapshot(&bids, &asks, final_update_id, timestamp);
            debug!(
                "Snapshot applied for {}: {} bids, {} asks",
                symbol, bids_count, asks_count
            );
        } else {
            state
                .orderbook
                .apply_update(&bids, &asks, final_update_id, timestamp);
        }

        self.update_count += 1;

        if is_last {
            // 立即推送 (change-driven)
            self.push_depth(&symbol);
        }

        self.timer_check_counter += 1;
        if self.timer_check_counter >= TIMER_CHECK_EVERY_INCS {
            self.timer_check_counter = 0;
            self.check_timer_push();
        }
    }

    /// 检查定时推送
    fn check_timer_push(&mut self) {
        self.log_btc_depth25();
        self.log_publish_outcome_10s();

        let now = Instant::now();
        let symbols_to_push: Vec<String> = self
            .symbols
            .iter()
            .filter(|(_, state)| now.duration_since(state.last_push_time) >= self.push_interval)
            .map(|(symbol, _)| symbol.clone())
            .collect();

        for symbol in symbols_to_push {
            self.push_depth(&symbol);
        }
    }

    fn log_btc_depth25(&mut self) {
        if self.last_btc_depth25_log.elapsed() < Duration::from_secs(BTC_DEPTH25_LOG_INTERVAL_SECS)
        {
            return;
        }
        self.last_btc_depth25_log = Instant::now();

        for (symbol, state) in &self.symbols {
            let is_btc = symbol
                .get(0..3)
                .map(|s| s.eq_ignore_ascii_case("BTC"))
                .unwrap_or(false);
            if !is_btc {
                continue;
            }
            let (bids, asks) = state.orderbook.get_depth(25);
            info!(
                "DepthPubApp[{}] BTC depth25 {} bids={:?} asks={:?}",
                self.venue_slug, symbol, bids, asks
            );
        }
    }

    /// 推送深度快照
    fn push_depth(&mut self, symbol: &str) {
        let state = match self.symbols.get_mut(symbol) {
            Some(s) => s,
            None => return,
        };

        if !state.orderbook.is_valid() {
            self.publish_fail_invalid_count = self.publish_fail_invalid_count.saturating_add(1);
            if state.orderbook.bids.is_empty() || state.orderbook.asks.is_empty() {
                self.publish_fail_missing_side_count =
                    self.publish_fail_missing_side_count.saturating_add(1);
            } else {
                self.publish_fail_crossed_book_count =
                    self.publish_fail_crossed_book_count.saturating_add(1);
            }
            return;
        }

        let timestamp = state.orderbook.timestamp;
        let mut attempted_channels = 0u8;
        let mut sent_channels = 0u8;

        // Depth25
        if self.config.depth_levels.enable_depth25 {
            attempted_channels = attempted_channels.saturating_add(1);
            let (bids, asks) = state.orderbook.get_depth(25);
            let msg = DepthMsg::depth25(symbol.to_string(), timestamp, bids, asks);
            if self.publisher.publish_depth25(&msg) {
                sent_channels = sent_channels.saturating_add(1);
            }
        }

        // Depth50
        if self.config.depth_levels.enable_depth50 {
            attempted_channels = attempted_channels.saturating_add(1);
            let (bids, asks) = state.orderbook.get_depth(50);
            let msg = DepthMsg::depth50(symbol.to_string(), timestamp, bids, asks);
            if self.publisher.publish_depth50(&msg) {
                sent_channels = sent_channels.saturating_add(1);
            }
        }

        if attempted_channels == 0 || sent_channels > 0 {
            self.publish_success_count = self.publish_success_count.saturating_add(1);
        } else {
            self.publish_fail_send_count = self.publish_fail_send_count.saturating_add(1);
        }

        state.last_push_time = Instant::now();
        self.push_count += 1;
    }

    fn log_publish_outcome_10s(&mut self) {
        if self.last_publish_outcome_log.elapsed()
            < Duration::from_secs(PUBLISH_OUTCOME_LOG_INTERVAL_SECS)
        {
            return;
        }

        let fail_total = self
            .publish_fail_invalid_count
            .saturating_add(self.publish_fail_send_count);
        info!(
            "DepthMsgApp[{}] publish_outcome_10s: success={} fail_total={} fail_invalid={} fail_send={} fail_missing_side={} fail_crossed_book={}",
            self.venue_slug,
            self.publish_success_count,
            fail_total,
            self.publish_fail_invalid_count,
            self.publish_fail_send_count,
            self.publish_fail_missing_side_count,
            self.publish_fail_crossed_book_count
        );

        self.last_publish_outcome_log = Instant::now();
        self.publish_success_count = 0;
        self.publish_fail_invalid_count = 0;
        self.publish_fail_send_count = 0;
        self.publish_fail_missing_side_count = 0;
        self.publish_fail_crossed_book_count = 0;
    }

    fn handle_query_request(&mut self, active_request: DepthQueryActiveRequest) {
        let payload = active_request.payload();

        let header = match DepthQueryHeader::parse(payload) {
            Ok(h) => h,
            Err(err) => {
                warn!("Depth query parse failed: {err:#}");
                let _ = self.send_query_response(active_request, &[RESP_STATUS_BAD_REQUEST]);
                return;
            }
        };

        let mut resp = [0u8; DEPTH_QUERY_PAYLOAD];
        let payload_offset =
            match DepthQueryHeader::write(&mut resp, header.query_type, &header.symbol) {
                Ok(offset) => offset,
                Err(err) => {
                    warn!("Depth query response header build failed: {err:#}");
                    let _ = self.send_query_response(active_request, &[RESP_STATUS_BAD_REQUEST]);
                    return;
                }
            };

        let req_payload = &payload[payload_offset..];
        let status;
        let mut body_len = 0usize;

        match DepthQueryType::from_u8(header.query_type) {
            Some(DepthQueryType::LoadTlenSingle) => {
                let resp_payload = &mut resp[payload_offset..];
                let (st, written) =
                    self.handle_load_tlen_single_query(&header.symbol, req_payload, resp_payload);
                status = st;
                body_len = written;
            }
            Some(DepthQueryType::LoadTlenBatch) => {
                let resp_payload = &mut resp[payload_offset..];
                let (st, written) =
                    self.handle_load_tlen_batch_query(&header.symbol, req_payload, resp_payload);
                status = st;
                body_len = written;
            }
            _ => {
                let resp_payload = &mut resp[payload_offset..];
                if !resp_payload.is_empty() {
                    resp_payload[0] = RESP_STATUS_UNSUPPORTED_TYPE;
                    body_len = 1;
                }
                status = RESP_STATUS_UNSUPPORTED_TYPE;
            }
        }

        if body_len == 0 {
            let resp_payload = &mut resp[payload_offset..];
            if !resp_payload.is_empty() {
                resp_payload[0] = status;
                body_len = 1;
            }
        }

        debug!(
            "Depth query handled: venue={} symbol={} type={} status={}({})",
            self.venue_slug,
            header.symbol,
            header.query_type,
            status,
            resp_status_name(status)
        );

        let total_len = payload_offset + body_len;
        if let Err(err) = self.send_query_response(active_request, &resp[..total_len]) {
            warn!("Depth query response send failed: {err:#}");
        }
    }

    fn handle_load_tlen_single_query(
        &self,
        symbol: &str,
        req_payload: &[u8],
        resp_payload: &mut [u8],
    ) -> (u8, usize) {
        if resp_payload.len() < 1 + DepthQueryLoadTlenSingleResp::RESP_LEN {
            return (RESP_STATUS_PAYLOAD_TOO_LARGE, 0);
        }

        let req = match DepthQueryLoadTlenSingleReq::from_payload(req_payload) {
            Ok(ctx) => ctx,
            Err(err) => {
                warn!("Depth query load_tlen single req parse failed: {err:#}");
                return (RESP_STATUS_BAD_REQUEST, 1);
            }
        };

        let amount = self.query_tlen_amount(symbol, req.price);

        let resp = DepthQueryLoadTlenSingleResp {
            timestamp_us: req.timestamp_us,
            amount,
        };
        match resp.write_to(&mut resp_payload[1..]) {
            Ok(written) => {
                resp_payload[0] = RESP_STATUS_OK;
                (RESP_STATUS_OK, 1 + written)
            }
            Err(err) => {
                warn!("Depth query load_tlen single resp write failed: {err:#}");
                resp_payload[0] = RESP_STATUS_PAYLOAD_TOO_LARGE;
                (RESP_STATUS_PAYLOAD_TOO_LARGE, 1)
            }
        }
    }

    fn handle_load_tlen_batch_query(
        &self,
        symbol: &str,
        req_payload: &[u8],
        resp_payload: &mut [u8],
    ) -> (u8, usize) {
        if resp_payload.len() < 2 {
            return (RESP_STATUS_PAYLOAD_TOO_LARGE, 0);
        }

        let req = match DepthQueryLoadTlenBatchReq::from_payload(req_payload) {
            Ok(v) => v,
            Err(err) => {
                warn!("Depth query load_tlen batch req parse failed: {err:#}");
                return (RESP_STATUS_BAD_REQUEST, 1);
            }
        };

        let mut amounts = Vec::with_capacity(req.prices.len());
        for price in req.prices {
            amounts.push(self.query_tlen_amount(symbol, price));
        }

        match DepthQueryLoadTlenBatchResp::write_to(
            &mut resp_payload[1..],
            req.timestamp_us,
            &amounts,
        ) {
            Ok(written) => {
                resp_payload[0] = RESP_STATUS_OK;
                (RESP_STATUS_OK, 1 + written)
            }
            Err(err) => {
                warn!("Depth query load_tlen batch resp write failed: {err:#}");
                resp_payload[0] = RESP_STATUS_PAYLOAD_TOO_LARGE;
                (RESP_STATUS_PAYLOAD_TOO_LARGE, 1)
            }
        }
    }

    fn query_tlen_amount(&self, symbol: &str, raw_price: f64) -> f64 {
        if !raw_price.is_finite() {
            return -1.0;
        }

        let table_symbol_key = self.symbol_key_for_table(symbol);
        let Some(tick) = self.min_qty_table.price_tick(&table_symbol_key) else {
            return -1.0;
        };

        if !Self::is_price_on_tick(raw_price, tick) {
            return -1.0;
        }

        let orderbook_symbol = if self.symbols.contains_key(symbol) {
            symbol.to_string()
        } else {
            let upper = symbol.to_ascii_uppercase();
            if self.symbols.contains_key(&upper) {
                upper
            } else {
                return -1.0;
            }
        };

        let Some(state) = self.symbols.get(&orderbook_symbol) else {
            return -1.0;
        };
        if !state.orderbook.is_valid() {
            return -1.0;
        }

        state.orderbook.amount_at_price(raw_price).unwrap_or(0.0)
    }

    fn is_price_on_tick(price: f64, tick: f64) -> bool {
        if !tick.is_finite() || tick <= 0.0 {
            return false;
        }

        let steps = price / tick;
        if !steps.is_finite() {
            return false;
        }

        let nearest = steps.round();
        let reconstructed = nearest * tick;
        let tolerance = tick.abs() * 1e-9 + 1e-12;
        (reconstructed - price).abs() <= tolerance
    }

    fn send_query_response(
        &self,
        active_request: DepthQueryActiveRequest,
        payload: &[u8],
    ) -> Result<()> {
        let response = active_request.loan_slice_uninit(payload.len())?;
        let response = response.write_from_slice(payload);
        response.send()?;
        Ok(())
    }

    fn symbol_key_for_table(&self, symbol: &str) -> String {
        match self.venue {
            TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
                symbol.to_uppercase().replace("-SWAP", "").replace('-', "")
            }
            TradingVenue::GateMargin | TradingVenue::GateFutures => {
                symbol.to_uppercase().replace('_', "").replace('-', "")
            }
            _ => symbol.to_uppercase(),
        }
    }

    /// 打印统计
    fn log_stats(&mut self) {
        info!(
            "DepthMsgApp[{}] stats: symbols={}, updates={}, pushes={}",
            self.venue_slug,
            self.symbols.len(),
            self.update_count,
            self.push_count
        );
        self.publisher.log_stats();
        self.update_count = 0;
        self.push_count = 0;
    }
}
