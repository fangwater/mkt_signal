//! Exec consumes the same venue-native public trade stream as factor publishers.
use crate::common::trade_msg_parser::parse_trade;
use crate::pre_trade::monitor_channel::MonitorChannel;
use anyhow::Result;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use order_common::TradingVenue;
use runtime_common::symbol_util::normalize_symbol_for_internal;
use runtime_common::time_util::get_timestamp_us;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Default)]
struct TradeCursor {
    timestamp_us: i64,
    trade_id: i64,
}

impl TradeCursor {
    fn accept(&mut self, timestamp_us: i64, trade_id: i64) -> bool {
        // Binance and Gate IDs increase per symbol. Reject replay and reordered data.
        if timestamp_us < self.timestamp_us || trade_id <= self.trade_id {
            return false;
        }
        self.timestamp_us = timestamp_us;
        self.trade_id = trade_id;
        true
    }
}

fn subscribe(venue: TradingVenue) -> Result<Subscriber<ipc::Service, [u8; 128], ()>> {
    let node = NodeBuilder::new()
        .name(&NodeName::new(&format!(
            "exec_pov_{}",
            venue.data_pub_slug().replace('-', "_")
        ))?)
        .create::<ipc::Service>()?;
    let service = node
        .service_builder(&ServiceName::new(&format!(
            "dat_pbs/{}/trade",
            venue.data_pub_slug()
        ))?)
        .publish_subscribe::<[u8; 128]>()
        .open()?;
    Ok(service.subscriber_builder().buffer_size(8192).create()?)
}

pub fn start(venue: TradingVenue) {
    tokio::task::spawn_local(async move {
        let mut cursors: HashMap<String, TradeCursor> = HashMap::new();
        loop {
            let subscriber = match subscribe(venue) {
                Ok(sub) => sub,
                Err(err) => {
                    log::warn!("Exec POV volume unavailable venue={venue:?}: {err:#}");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            log::info!(
                "Exec POV subscribed to dat_pbs/{}/trade",
                venue.data_pub_slug()
            );
            let mut failed = false;
            while !failed {
                for _ in 0..4096 {
                    let sample = match subscriber.receive() {
                        Ok(Some(sample)) => sample,
                        Ok(None) => break,
                        Err(err) => {
                            log::warn!("Exec POV trade receive failed: {err:?}");
                            failed = true;
                            break;
                        }
                    };
                    let Some(trade) = parse_trade(sample.payload(), venue) else {
                        continue;
                    };
                    let now_us = get_timestamp_us();
                    if trade.timestamp_us > now_us || trade.timestamp_us < now_us - 60_000_000 {
                        continue;
                    }
                    let symbol = normalize_symbol_for_internal(&trade.symbol);
                    if !cursors
                        .entry(symbol.clone())
                        .or_default()
                        .accept(trade.timestamp_us, trade.trade_id)
                    {
                        continue;
                    }
                    let mon = MonitorChannel::instance();
                    let Ok(base_qty) =
                        mon.qty_to_base_at_price(venue, &symbol, trade.amount, trade.price)
                    else {
                        continue;
                    };
                    mon.strategy_mgr().borrow_mut().observe_exec_market_trade(
                        venue,
                        &symbol,
                        trade.timestamp_us,
                        now_us,
                        base_qty,
                        trade.price,
                    );
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_and_reordered_trades_cannot_mint_volume() {
        let mut cursor = TradeCursor::default();
        assert!(cursor.accept(100, 7));
        assert!(!cursor.accept(100, 7));
        assert!(!cursor.accept(99, 6));
        assert!(cursor.accept(100, 8));
        assert!(!cursor.accept(101, 8));
        assert!(cursor.accept(101, 9));
    }
}
