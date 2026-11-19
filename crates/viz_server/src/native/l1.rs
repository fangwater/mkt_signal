//! Read-only L1 marks from the authorized market tunnel for CTP positions.
use super::{config, now_us};
use anyhow::{Context, Result};
use chrono::{DateTime, FixedOffset, Utc};
use cn_futures_pbs::{
    msg::{MarketDataMsg, SOURCE_RESET},
    sources::{zmq::ZmqSource, Source},
};
use cn_futures_state::{InstrumentKind, InstrumentMark, Snapshot, Text, Venue, MAX_POSITIONS};
use cn_market_proto::{
    cn_futures_market_client::CnFuturesMarketClient, Exchange, GetProductMultipliersRequest,
};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
    time::Duration,
};
use tokio::{task::JoinHandle, time};
use tonic::transport::Endpoint;

type Contract = (Venue, [u8; 64]);
type Multipliers = HashMap<(Venue, String), i32>;

pub struct Quotes {
    stale_us: u64,
    generation: u64,
    latest: HashMap<Contract, MarketDataMsg>,
    multipliers: Multipliers,
}

fn price(value: f64) -> Option<f64> {
    (value.is_finite() && value > 0.0 && value < f64::MAX).then_some(value)
}

fn contract(venue: Venue, symbol: &[u8]) -> Option<(Contract, &str)> {
    let product_len = symbol
        .iter()
        .take_while(|c| c.is_ascii_alphabetic())
        .count();
    let (product, month) = symbol.split_at(product_len);
    let digits = if venue == Venue::Czce { 3 } else { 4 };
    // A product multiplier applies to futures, never to look-alike options or spreads.
    if product.is_empty() || month.len() != digits || !month.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut key = [0; 64];
    key.get_mut(..symbol.len())?.copy_from_slice(symbol);
    key.make_ascii_lowercase();
    Some(((venue, key), std::str::from_utf8(product).ok()?))
}

impl Quotes {
    fn new(stale_after_ms: u64) -> Self {
        Self {
            stale_us: stale_after_ms * 1000,
            generation: 0,
            latest: HashMap::new(),
            multipliers: HashMap::new(),
        }
    }

    fn accept(&mut self, quote: MarketDataMsg) {
        self.generation = quote.source_generation;
        let bbo = price(quote.bid[0].price)
            .zip(price(quote.ask[0].price))
            .is_some_and(|(bid, ask)| {
                bid <= ask && quote.bid[0].quantity > 0 && quote.ask[0].quantity > 0
            });
        if quote.flags & SOURCE_RESET != 0
            || (!bbo && price(quote.last_price).is_none())
            || quote.exchange_ts_us <= 0
            || quote.trading_day == 0
            || quote.received_us == 0
        {
            return;
        }
        let Ok(venue) = std::str::from_utf8(quote.venue())
            .ok()
            .context("unknown quote venue")
            .and_then(Venue::parse)
        else {
            return;
        };
        let Some((key, _)) = contract(venue, quote.symbol()) else {
            return;
        };
        if let Some(last) = self.latest.get_mut(&key) {
            if (quote.trading_day, quote.exchange_ts_us) >= (last.trading_day, last.exchange_ts_us)
            {
                *last = quote;
            }
        } else if self.latest.len() < MAX_POSITIONS {
            self.latest.insert(key, quote);
        }
    }

    pub fn attach(&self, snapshot: &mut Snapshot) {
        if !snapshot.position_ready || !snapshot.marks.is_empty() {
            return;
        }
        let now = now_us();
        let day = snapshot.trading_day.as_str().parse().ok();
        let mut seen = HashSet::new();
        snapshot
            .marks
            .extend(snapshot.positions.iter().filter_map(|position| {
                let (key, product) = contract(position.venue, position.symbol.as_str().as_bytes())?;
                if !seen.insert(key) {
                    return None;
                }
                let quote = self
                    .latest
                    .get(&key)
                    .filter(|quote| Some(quote.trading_day) == day)?;
                let contract_multiple = *self
                    .multipliers
                    .get(&(position.venue, product.to_ascii_uppercase()))?;
                let native_time = DateTime::<Utc>::from_timestamp_micros(quote.exchange_ts_us)?
                    .with_timezone(&FixedOffset::east_opt(8 * 3600)?)
                    .format("%H:%M:%S%.3f")
                    .to_string();
                let paused = quote.source_generation != self.generation;
                snapshot.l1_connected |=
                    !paused && now >= quote.received_us && now - quote.received_us <= self.stale_us;
                Some(InstrumentMark {
                    venue: position.venue,
                    symbol: position.symbol,
                    kind: InstrumentKind::Future,
                    contract_multiple,
                    last_price: price(quote.last_price),
                    bid_price: price(quote.bid[0].price),
                    ask_price: price(quote.ask[0].price),
                    bid_lots: quote.bid[0].quantity.try_into().ok(),
                    ask_lots: quote.ask[0].quantity.try_into().ok(),
                    trading_day: quote.trading_day,
                    native_timestamp: None,
                    last_trade_timestamp: None,
                    native_time: Text::new(&native_time).ok()?,
                    last_trade_time: Text::default(),
                    received_us: quote.received_us,
                    initial: false,
                    paused,
                })
            }));
    }
}

async fn multipliers(port: u16) -> Result<Multipliers> {
    let endpoint = Endpoint::from_shared(format!("http://127.0.0.1:{port}"))?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(3));
    let mut client = CnFuturesMarketClient::new(endpoint.connect().await?);
    let reply = client
        .get_product_multipliers(GetProductMultipliersRequest { products: vec![] })
        .await?
        .into_inner();
    let mut map = HashMap::new();
    for product in reply.products {
        let exchange = Exchange::try_from(product.exchange)?;
        let venue = Venue::parse(exchange.as_str_name())?;
        let multiple = product.volume_multiple;
        if multiple.is_finite()
            && multiple > 0.0
            && multiple.fract() == 0.0
            && multiple <= f64::from(i32::MAX)
        {
            map.insert(
                (venue, product.product.to_ascii_uppercase()),
                multiple as i32,
            );
        }
    }
    anyhow::ensure!(!map.is_empty(), "no verified futures multipliers");
    Ok(map)
}

pub fn spawn(
    config: config::L1,
    stale_after_ms: u64,
) -> (Rc<RefCell<Quotes>>, [JoinHandle<()>; 2]) {
    let quotes = Rc::new(RefCell::new(Quotes::new(stale_after_ms)));
    let book = quotes.clone();
    let market = tokio::task::spawn_local(async move {
        let endpoint = format!("tcp://127.0.0.1:{}", config.zmq_port);
        loop {
            match ZmqSource::new(&endpoint, 4096) {
                Ok(mut source) => {
                    let mut interval = time::interval(Duration::from_millis(10));
                    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
                    let error = loop {
                        interval.tick().await;
                        let mut book = book.borrow_mut();
                        let mut error = None;
                        for _ in 0..512 {
                            match source.poll() {
                                Ok(Some(quote)) => book.accept(quote),
                                Ok(None) => break,
                                Err(e) => {
                                    error = Some(e);
                                    break;
                                }
                            }
                        }
                        if let Some(error) = error {
                            book.latest.clear();
                            break error;
                        }
                    };
                    log::warn!("Viz L1 stream restarting: {error:#}");
                }
                Err(error) => log::warn!("Viz L1 source unavailable: {error:#}"),
            }
            time::sleep(Duration::from_secs(3)).await;
        }
    });
    let book = quotes.clone();
    let metadata = tokio::task::spawn_local(async move {
        loop {
            let delay = match multipliers(config.metadata_port).await {
                Ok(rows) => {
                    book.borrow_mut().multipliers = rows;
                    3600
                }
                Err(error) => {
                    log::warn!("Viz L1 multipliers unavailable: {error:#}");
                    30
                }
            };
            time::sleep(Duration::from_secs(delay)).await;
        }
    });
    (quotes, [market, metadata])
}

#[cfg(test)]
mod tests {
    use super::*;
    use cn_futures_state::{Adapter, Position, PositionRow, Side};

    #[test]
    fn held_contract_marks_follow_quotes_without_faking_a_new_trading_day_or_timestamp() {
        let mut quotes = Quotes::new(15_000);
        quotes.multipliers.insert((Venue::Shfe, "RB".into()), 10);
        let mut state = Snapshot::empty(Adapter::Ctp, &"a".repeat(64)).unwrap();
        let symbol = Text::new("rb2701").unwrap();
        state.position_ready = true;
        state.trading_day = Text::new("20261119").unwrap();
        state.positions = [Side::Buy, Side::Sell]
            .map(|side| PositionRow {
                venue: Venue::Shfe,
                symbol,
                side,
                position: Position::default(),
            })
            .to_vec();
        let mut quote = MarketDataMsg {
            trading_day: 20261119,
            exchange_ts_us: 1_795_071_600_000_000,
            received_us: now_us(),
            source_generation: 1,
            last_price: 3140.0,
            ..Default::default()
        };
        quote.set_venue("SHFE").unwrap();
        quote.set_symbol(b"rb2701").unwrap();
        quotes.accept(quote);
        quotes.attach(&mut state);
        assert_eq!(state.marks.len(), 1);
        assert_eq!(state.marks[0].last_price, Some(3140.0));
        assert_eq!(state.marks[0].received_us, quote.received_us);
        assert_eq!(state.marks[0].contract_multiple, 10);
        assert!(state.l1_connected);

        state.marks.clear();
        state.l1_connected = false;
        quote.last_price = 3130.0;
        quote.exchange_ts_us -= 1_000;
        quotes.accept(quote);
        quotes.accept(MarketDataMsg::reset(2));
        quotes.attach(&mut state);
        assert_eq!(state.marks[0].last_price, Some(3140.0));
        assert!(state.marks[0].paused);
        assert!(!state.l1_connected);

        state.marks.clear();
        state.trading_day = Text::new("20261120").unwrap();
        quotes.attach(&mut state);
        assert!(state.marks.is_empty());
    }
}
