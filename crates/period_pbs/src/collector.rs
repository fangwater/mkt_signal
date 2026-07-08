use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use ahash::AHashMap;
use anyhow::Result;

use crate::decode::{IncRecord, LevelRecord, TradeRecord};
use crate::pb;
use crate::period::{
    period_for_timestamp_ms, period_upper_bound_ms, DEFAULT_DELAY_MS, DEFAULT_PERIOD_MS,
};

#[derive(Debug, Clone)]
pub struct CollectorConfig {
    pub period_ms: i64,
    pub delay_ms: i64,
    pub poster_id: String,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            period_ms: DEFAULT_PERIOD_MS,
            delay_ms: DEFAULT_DELAY_MS,
            poster_id: default_poster_id(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompletedPeriod {
    pub period: i64,
    pub upper_bound_ms: i64,
    pub trade_count: usize,
    pub inc_count: usize,
    pub message: pb::PeriodMessage,
    pub encoded: Vec<u8>,
}

#[derive(Default)]
pub struct PeriodCollector {
    config: CollectorConfig,
    buckets: BTreeMap<i64, PeriodBucket>,
    pending_chunks: AHashMap<ChunkKey, PendingInc>,
    max_trade_ts_ms: Option<i64>,
    max_inc_ts_ms: Option<i64>,
    last_completed_period: Option<i64>,
}

#[derive(Default)]
struct PeriodBucket {
    symbols: BTreeMap<String, SymbolBucket>,
    trade_count: usize,
    inc_count: usize,
}

#[derive(Default)]
struct SymbolBucket {
    trades: Vec<TradeRecord>,
    incs: Vec<IncRecord>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct ChunkKey {
    symbol: String,
    first_update_id: i64,
    final_update_id: i64,
    timestamp: i64,
    is_snapshot: bool,
}

#[derive(Debug, Clone)]
struct PendingInc {
    symbol: String,
    first_update_id: i64,
    final_update_id: i64,
    timestamp: i64,
    timestamp_ms: i64,
    is_snapshot: bool,
    bids: Vec<LevelRecord>,
    asks: Vec<LevelRecord>,
    next_chunk_index: u8,
}

impl PeriodCollector {
    pub fn new(config: CollectorConfig) -> Self {
        Self {
            config,
            buckets: BTreeMap::new(),
            pending_chunks: AHashMap::new(),
            max_trade_ts_ms: None,
            max_inc_ts_ms: None,
            last_completed_period: None,
        }
    }

    pub fn push_trade(&mut self, trade: TradeRecord) -> Result<Vec<CompletedPeriod>> {
        self.observe_trade_ts(trade.timestamp_ms);
        let period = period_for_timestamp_ms(trade.timestamp_ms, self.config.period_ms);
        if self.is_completed_period(period) {
            return self.drain_ready();
        }
        let symbol = trade.symbol.clone();
        let bucket = self.buckets.entry(period).or_default();
        bucket.symbols.entry(symbol).or_default().trades.push(trade);
        bucket.trade_count += 1;
        self.drain_ready()
    }

    pub fn push_incremental(&mut self, inc: IncRecord) -> Result<Vec<CompletedPeriod>> {
        self.observe_inc_ts(inc.timestamp_ms);
        let period = period_for_timestamp_ms(inc.timestamp_ms, self.config.period_ms);
        if self.is_completed_period(period) {
            self.discard_pending_incremental(&inc);
            return self.drain_ready();
        }
        let Some(inc) = self.accept_incremental_chunk(inc) else {
            return self.drain_ready();
        };
        let symbol = inc.symbol.clone();
        let bucket = self.buckets.entry(period).or_default();
        bucket.symbols.entry(symbol).or_default().incs.push(inc);
        bucket.inc_count += 1;
        self.drain_ready()
    }

    pub fn drain_ready(&mut self) -> Result<Vec<CompletedPeriod>> {
        let (Some(trade_ts), Some(inc_ts)) = (self.max_trade_ts_ms, self.max_inc_ts_ms) else {
            return Ok(Vec::new());
        };
        let ready_before = trade_ts.min(inc_ts) - self.config.delay_ms;
        let ready_periods: Vec<i64> = self
            .buckets
            .keys()
            .copied()
            .take_while(|period| {
                period_upper_bound_ms(*period, self.config.period_ms) <= ready_before
            })
            .collect();

        let mut completed = Vec::with_capacity(ready_periods.len());
        for period in ready_periods {
            if let Some(bucket) = self.buckets.remove(&period) {
                completed.push(self.encode_bucket(period, bucket));
                self.last_completed_period = Some(
                    self.last_completed_period
                        .map_or(period, |last| last.max(period)),
                );
            }
        }
        self.prune_completed_pending_chunks();
        Ok(completed)
    }

    fn observe_trade_ts(&mut self, timestamp_ms: i64) {
        self.max_trade_ts_ms = Some(
            self.max_trade_ts_ms
                .map_or(timestamp_ms, |ts| ts.max(timestamp_ms)),
        );
    }

    fn observe_inc_ts(&mut self, timestamp_ms: i64) {
        self.max_inc_ts_ms = Some(
            self.max_inc_ts_ms
                .map_or(timestamp_ms, |ts| ts.max(timestamp_ms)),
        );
    }

    fn accept_incremental_chunk(&mut self, inc: IncRecord) -> Option<IncRecord> {
        if inc.chunk_index == 0 && inc.is_last {
            return Some(inc);
        }

        let key = ChunkKey {
            symbol: inc.symbol.clone(),
            first_update_id: inc.first_update_id,
            final_update_id: inc.final_update_id,
            timestamp: inc.timestamp,
            is_snapshot: inc.is_snapshot,
        };

        let pending = self
            .pending_chunks
            .entry(key.clone())
            .or_insert_with(|| PendingInc {
                symbol: inc.symbol.clone(),
                first_update_id: inc.first_update_id,
                final_update_id: inc.final_update_id,
                timestamp: inc.timestamp,
                timestamp_ms: inc.timestamp_ms,
                is_snapshot: inc.is_snapshot,
                bids: Vec::new(),
                asks: Vec::new(),
                next_chunk_index: 0,
            });

        if inc.chunk_index != pending.next_chunk_index {
            log::warn!(
                "period_pbs incremental chunk gap symbol={} ts={} expected={} got={}",
                inc.symbol,
                inc.timestamp_ms,
                pending.next_chunk_index,
                inc.chunk_index
            );
            pending.next_chunk_index = inc.chunk_index;
        }

        pending.bids.extend(inc.bids);
        pending.asks.extend(inc.asks);
        pending.next_chunk_index = inc.chunk_index.saturating_add(1);

        if inc.is_last {
            let pending = self
                .pending_chunks
                .remove(&key)
                .expect("pending chunk exists");
            return Some(IncRecord {
                symbol: pending.symbol,
                first_update_id: pending.first_update_id,
                final_update_id: pending.final_update_id,
                timestamp: pending.timestamp,
                timestamp_ms: pending.timestamp_ms,
                is_snapshot: pending.is_snapshot,
                is_last: true,
                chunk_index: 0,
                bids: pending.bids,
                asks: pending.asks,
            });
        }
        None
    }

    fn is_completed_period(&self, period: i64) -> bool {
        self.last_completed_period
            .is_some_and(|last_period| period <= last_period)
    }

    fn discard_pending_incremental(&mut self, inc: &IncRecord) {
        if inc.chunk_index == 0 && inc.is_last {
            return;
        }
        let key = ChunkKey {
            symbol: inc.symbol.clone(),
            first_update_id: inc.first_update_id,
            final_update_id: inc.final_update_id,
            timestamp: inc.timestamp,
            is_snapshot: inc.is_snapshot,
        };
        self.pending_chunks.remove(&key);
    }

    fn prune_completed_pending_chunks(&mut self) {
        let Some(last_completed_period) = self.last_completed_period else {
            return;
        };
        let period_ms = self.config.period_ms;
        self.pending_chunks.retain(|_, pending| {
            period_for_timestamp_ms(pending.timestamp_ms, period_ms) > last_completed_period
        });
    }

    fn encode_bucket(&self, period: i64, bucket: PeriodBucket) -> CompletedPeriod {
        let upper_bound_ms = period_upper_bound_ms(period, self.config.period_ms);
        let symbol_infos = bucket
            .symbols
            .into_iter()
            .map(|(symbol, bucket)| pb::SymbolInfo {
                symbol,
                trades: bucket
                    .trades
                    .into_iter()
                    .map(|trade| pb::TradeInfo {
                        timestamp: trade.timestamp,
                        side: trade.side.to_string(),
                        price: trade.price,
                        amount: trade.amount,
                    })
                    .collect(),
                incs: bucket
                    .incs
                    .into_iter()
                    .map(|inc| pb::IncrementOrderBookInfo {
                        timestamp: inc.timestamp,
                        is_snapshot: inc.is_snapshot,
                        bids: inc
                            .bids
                            .into_iter()
                            .map(|level| pb::PriceLevel {
                                price: level.price,
                                amount: level.amount,
                            })
                            .collect(),
                        asks: inc
                            .asks
                            .into_iter()
                            .map(|level| pb::PriceLevel {
                                price: level.price,
                                amount: level.amount,
                            })
                            .collect(),
                    })
                    .collect(),
            })
            .collect();

        let message = pb::PeriodMessage {
            period,
            ts: upper_bound_ms,
            post_ts: now_ms(),
            poster_id: self.config.poster_id.clone(),
            symbol_infos,
        };
        let encoded = pb::encode_period_message(&message);
        CompletedPeriod {
            period,
            upper_bound_ms,
            trade_count: bucket.trade_count,
            inc_count: bucket.inc_count,
            message,
            encoded,
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

pub fn default_poster_id() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            std::fs::read_to_string("/proc/sys/kernel/hostname")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::period::{DEFAULT_PERIOD_MS, INIT_TP_MS};

    fn cfg() -> CollectorConfig {
        CollectorConfig {
            period_ms: DEFAULT_PERIOD_MS,
            delay_ms: 5,
            poster_id: "test-host".to_string(),
        }
    }

    fn trade_record(timestamp_ms: i64, side: char) -> TradeRecord {
        TradeRecord {
            symbol: "BTCUSDT".to_string(),
            timestamp: timestamp_ms * 1_000,
            timestamp_ms,
            side,
            price: 100.0,
            amount: 1.0,
        }
    }

    fn inc_record(timestamp_ms: i64, update_id: i64) -> IncRecord {
        IncRecord {
            symbol: "BTCUSDT".to_string(),
            first_update_id: update_id,
            final_update_id: update_id,
            timestamp: timestamp_ms * 1_000,
            timestamp_ms,
            is_snapshot: false,
            is_last: true,
            chunk_index: 0,
            bids: Vec::new(),
            asks: Vec::new(),
        }
    }

    #[test]
    fn flushes_period_after_both_stream_watermarks_pass_delay() {
        let mut collector = PeriodCollector::new(cfg());
        let out = collector
            .push_trade(TradeRecord {
                symbol: "BTCUSDT".to_string(),
                timestamp: INIT_TP_MS + 10,
                timestamp_ms: INIT_TP_MS + 10,
                side: 'B',
                price: 100.0,
                amount: 1.0,
            })
            .expect("push trade");
        assert!(out.is_empty());

        let out = collector
            .push_trade(TradeRecord {
                symbol: "BTCUSDT".to_string(),
                timestamp: INIT_TP_MS + DEFAULT_PERIOD_MS + 5,
                timestamp_ms: INIT_TP_MS + DEFAULT_PERIOD_MS + 5,
                side: 'S',
                price: 101.0,
                amount: 2.0,
            })
            .expect("push trade watermark");
        assert!(out.is_empty());

        let out = collector
            .push_incremental(IncRecord {
                symbol: "BTCUSDT".to_string(),
                first_update_id: 2,
                final_update_id: 2,
                timestamp: INIT_TP_MS + DEFAULT_PERIOD_MS + 5,
                timestamp_ms: INIT_TP_MS + DEFAULT_PERIOD_MS + 5,
                is_snapshot: false,
                is_last: true,
                chunk_index: 0,
                bids: Vec::new(),
                asks: Vec::new(),
            })
            .expect("push inc watermark");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].period, 0);
        assert_eq!(out[0].message.ts, INIT_TP_MS + DEFAULT_PERIOD_MS);
        assert_eq!(out[0].trade_count, 1);
    }

    #[test]
    fn reassembles_incremental_chunks() {
        let mut collector = PeriodCollector::new(cfg());
        let first = IncRecord {
            symbol: "BTCUSDT".to_string(),
            first_update_id: 1,
            final_update_id: 2,
            timestamp: INIT_TP_MS + 10,
            timestamp_ms: INIT_TP_MS + 10,
            is_snapshot: false,
            is_last: false,
            chunk_index: 0,
            bids: vec![LevelRecord {
                price: 99.0,
                amount: 1.0,
            }],
            asks: Vec::new(),
        };
        let second = IncRecord {
            is_last: true,
            chunk_index: 1,
            bids: Vec::new(),
            asks: vec![LevelRecord {
                price: 101.0,
                amount: 2.0,
            }],
            ..first.clone()
        };

        assert!(collector.push_incremental(first).expect("first").is_empty());
        assert!(collector
            .push_incremental(second)
            .expect("second")
            .is_empty());
        assert!(collector
            .push_trade(TradeRecord {
                symbol: "BTCUSDT".to_string(),
                timestamp: INIT_TP_MS + DEFAULT_PERIOD_MS + 5,
                timestamp_ms: INIT_TP_MS + DEFAULT_PERIOD_MS + 5,
                side: 'B',
                price: 100.0,
                amount: 1.0,
            })
            .expect("trade watermark")
            .is_empty());
        let out = collector
            .push_incremental(IncRecord {
                symbol: "BTCUSDT".to_string(),
                first_update_id: 3,
                final_update_id: 3,
                timestamp: INIT_TP_MS + DEFAULT_PERIOD_MS + 5,
                timestamp_ms: INIT_TP_MS + DEFAULT_PERIOD_MS + 5,
                is_snapshot: false,
                is_last: true,
                chunk_index: 0,
                bids: Vec::new(),
                asks: Vec::new(),
            })
            .expect("inc watermark");
        assert_eq!(out.len(), 1);
        let info = &out[0].message.symbol_infos[0];
        assert_eq!(info.incs.len(), 1);
        assert_eq!(info.incs[0].bids.len(), 1);
        assert_eq!(info.incs[0].asks.len(), 1);
    }

    #[test]
    fn drops_late_records_after_period_completed() {
        let mut collector = PeriodCollector::new(cfg());

        assert!(collector
            .push_trade(trade_record(INIT_TP_MS + 10, 'B'))
            .expect("period0 trade")
            .is_empty());
        assert!(collector
            .push_incremental(inc_record(INIT_TP_MS + 20, 1))
            .expect("period0 inc")
            .is_empty());
        assert!(collector
            .push_trade(trade_record(INIT_TP_MS + DEFAULT_PERIOD_MS + 5, 'S'))
            .expect("period1 trade watermark")
            .is_empty());

        let out = collector
            .push_incremental(inc_record(INIT_TP_MS + DEFAULT_PERIOD_MS + 5, 2))
            .expect("period1 inc watermark");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].period, 0);

        assert!(collector
            .push_incremental(inc_record(INIT_TP_MS + 100, 3))
            .expect("late period0 inc")
            .is_empty());
        assert!(collector
            .push_trade(trade_record(INIT_TP_MS + 200, 'B'))
            .expect("late period0 trade")
            .is_empty());

        assert!(collector
            .push_trade(trade_record(INIT_TP_MS + 2 * DEFAULT_PERIOD_MS + 5, 'B'))
            .expect("period2 trade watermark")
            .is_empty());
        let out = collector
            .push_incremental(inc_record(INIT_TP_MS + 2 * DEFAULT_PERIOD_MS + 5, 4))
            .expect("period2 inc watermark");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].period, 1);
        assert_eq!(out[0].trade_count, 1);
        assert_eq!(out[0].inc_count, 1);
    }

    #[test]
    fn prunes_pending_chunks_for_completed_period() {
        let mut collector = PeriodCollector::new(cfg());
        let first_chunk = IncRecord {
            symbol: "BTCUSDT".to_string(),
            first_update_id: 10,
            final_update_id: 11,
            timestamp: (INIT_TP_MS + 10) * 1_000,
            timestamp_ms: INIT_TP_MS + 10,
            is_snapshot: false,
            is_last: false,
            chunk_index: 0,
            bids: vec![LevelRecord {
                price: 99.0,
                amount: 1.0,
            }],
            asks: Vec::new(),
        };
        let second_chunk = IncRecord {
            is_last: true,
            chunk_index: 1,
            bids: Vec::new(),
            asks: vec![LevelRecord {
                price: 101.0,
                amount: 2.0,
            }],
            ..first_chunk.clone()
        };

        assert!(collector
            .push_incremental(first_chunk)
            .expect("first period0 chunk")
            .is_empty());
        assert_eq!(collector.pending_chunks.len(), 1);
        assert!(collector
            .push_trade(trade_record(INIT_TP_MS + 20, 'B'))
            .expect("period0 trade")
            .is_empty());
        assert!(collector
            .push_trade(trade_record(INIT_TP_MS + DEFAULT_PERIOD_MS + 5, 'S'))
            .expect("period1 trade watermark")
            .is_empty());

        let out = collector
            .push_incremental(inc_record(INIT_TP_MS + DEFAULT_PERIOD_MS + 5, 12))
            .expect("period1 inc watermark");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].period, 0);
        assert_eq!(collector.pending_chunks.len(), 0);

        assert!(collector
            .push_incremental(second_chunk)
            .expect("late final period0 chunk")
            .is_empty());
        assert_eq!(collector.pending_chunks.len(), 0);
    }

    #[test]
    fn preserves_raw_timestamp_in_period_message_fields() {
        let mut collector = PeriodCollector::new(cfg());
        let raw_trade_ts = (INIT_TP_MS + 10) * 1_000 + 123;
        let raw_inc_ts = (INIT_TP_MS + 20) * 1_000 + 456;
        assert!(collector
            .push_trade(TradeRecord {
                symbol: "BTCUSDT".to_string(),
                timestamp: raw_trade_ts,
                timestamp_ms: INIT_TP_MS + 10,
                side: 'B',
                price: 100.0,
                amount: 1.0,
            })
            .expect("push raw trade")
            .is_empty());
        assert!(collector
            .push_incremental(IncRecord {
                symbol: "BTCUSDT".to_string(),
                first_update_id: 1,
                final_update_id: 1,
                timestamp: raw_inc_ts,
                timestamp_ms: INIT_TP_MS + 20,
                is_snapshot: false,
                is_last: true,
                chunk_index: 0,
                bids: Vec::new(),
                asks: Vec::new(),
            })
            .expect("push raw inc")
            .is_empty());
        assert!(collector
            .push_trade(TradeRecord {
                symbol: "BTCUSDT".to_string(),
                timestamp: (INIT_TP_MS + DEFAULT_PERIOD_MS + 5) * 1_000,
                timestamp_ms: INIT_TP_MS + DEFAULT_PERIOD_MS + 5,
                side: 'S',
                price: 101.0,
                amount: 1.0,
            })
            .expect("push trade watermark")
            .is_empty());
        let out = collector
            .push_incremental(IncRecord {
                symbol: "BTCUSDT".to_string(),
                first_update_id: 2,
                final_update_id: 2,
                timestamp: (INIT_TP_MS + DEFAULT_PERIOD_MS + 5) * 1_000,
                timestamp_ms: INIT_TP_MS + DEFAULT_PERIOD_MS + 5,
                is_snapshot: false,
                is_last: true,
                chunk_index: 0,
                bids: Vec::new(),
                asks: Vec::new(),
            })
            .expect("push inc watermark");

        assert_eq!(out.len(), 1);
        let info = &out[0].message.symbol_infos[0];
        assert_eq!(info.trades[0].timestamp, raw_trade_ts);
        assert_eq!(info.incs[0].timestamp, raw_inc_ts);
        assert_eq!(out[0].period, 0);
    }
}
