//! Read-only L1 marks from the authorized market tunnel for CTP positions.
use super::{config, now_us};
use anyhow::{Context, Result};
use chrono::{DateTime, FixedOffset, Utc};
use cn_futures_pbs::{
    msg::{MarketDataMsg, SOURCE_RESET},
    sources::{zmq::ZmqSource, Source},
};
use cn_futures_state::{
    InstrumentKind, InstrumentMark, PositionRow, Snapshot, Text, Venue, MAX_POSITIONS,
};
use cn_market_proto::{
    cn_futures_market_client::CnFuturesMarketClient, get_minute_bars_request::Range, Exchange,
    GetMinuteBarsRequest, GetMinuteBarsResponse, GetProductMultipliersRequest,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    task::JoinHandle,
    time::{self, Instant},
};
use tonic::transport::{Channel, Endpoint};

type Contract = (Venue, [u8; 64]);
type Multipliers = HashMap<(Venue, String), i32>;
pub type SharedQuotes = Arc<Mutex<Quotes>>;

pub struct Quotes {
    stale_us: u64,
    generation: u64,
    latest: HashMap<Contract, MarketDataMsg>,
    multipliers: Multipliers,
    metadata_us: u64,
    history: HashMap<(Contract, u32), History>,
}

struct History {
    symbol: Text<81>,
    retry: Instant,
    mark: Option<InstrumentMark>,
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
            metadata_us: 0,
            history: HashMap::new(),
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

    pub fn attach(&mut self, snapshot: &mut Snapshot) {
        if !snapshot.position_ready || !snapshot.marks.is_empty() {
            return;
        }
        let Ok(day) = snapshot.trading_day.as_str().parse() else {
            return;
        };
        let (marks, connected) = self.marks(day, &snapshot.positions);
        snapshot.marks = marks;
        snapshot.l1_connected |= connected;
    }

    pub fn marks(&mut self, day: u32, positions: &[PositionRow]) -> (Vec<InstrumentMark>, bool) {
        let now = now_us();
        if self.history.len() >= MAX_POSITIONS {
            self.history
                .retain(|(_, archived_day), _| *archived_day >= day);
        }
        let mut seen = HashSet::new();
        let mut connected = false;
        let marks = positions
            .iter()
            .filter_map(|position| {
                let (key, product) = contract(position.venue, position.symbol.as_str().as_bytes())?;
                if !seen.insert(key) {
                    return None;
                }
                let contract_multiple = *self
                    .multipliers
                    .get(&(position.venue, product.to_ascii_uppercase()))?;
                let mut mark = InstrumentMark {
                    venue: position.venue,
                    symbol: position.symbol,
                    kind: InstrumentKind::Future,
                    contract_multiple,
                    trading_day: day,
                    received_us: self.metadata_us,
                    last_price: None,
                    bid_price: None,
                    ask_price: None,
                    bid_lots: None,
                    ask_lots: None,
                    native_timestamp: None,
                    last_trade_timestamp: None,
                    native_time: Text::default(),
                    last_trade_time: Text::default(),
                    initial: false,
                    paused: false,
                };
                if let Some(quote) = self
                    .latest
                    .get(&key)
                    .filter(|quote| quote.trading_day == day)
                {
                    mark.native_time = local_clock(quote.exchange_ts_us, "%H:%M:%S%.3f")?;
                    mark.received_us = quote.received_us;
                    mark.last_price = price(quote.last_price);
                    mark.bid_price = price(quote.bid[0].price);
                    mark.ask_price = price(quote.ask[0].price);
                    mark.bid_lots = quote.bid[0].quantity.try_into().ok();
                    mark.ask_lots = quote.ask[0].quantity.try_into().ok();
                    mark.paused = quote.source_generation != self.generation;
                    connected |= !mark.paused
                        && now >= quote.received_us
                        && now - quote.received_us <= self.stale_us;
                } else {
                    let history_key = (key, day);
                    if self.history.len() < MAX_POSITIONS || self.history.contains_key(&history_key)
                    {
                        let history = self.history.entry(history_key).or_insert_with(|| History {
                            symbol: position.symbol,
                            retry: Instant::now(),
                            mark: None,
                        });
                        if let Some(archived) = history
                            .mark
                            .filter(|m| m.contract_multiple == contract_multiple)
                        {
                            mark = archived;
                        }
                    }
                }
                Some(mark)
            })
            .collect();
        (marks, connected)
    }

    fn history_request(&mut self) -> Option<(Contract, Text<81>, u32)> {
        let now = Instant::now();
        let ((key, day), history) = self.history.iter_mut().find(|((key, day), history)| {
            history.mark.is_none()
                && history.retry <= now
                && !self
                    .latest
                    .get(key)
                    .is_some_and(|quote| quote.trading_day == *day)
        })?;
        history.retry = now + Duration::from_secs(60);
        Some((*key, history.symbol, *day))
    }
}

fn local_clock<const N: usize>(us: i64, format: &str) -> Option<Text<N>> {
    let time = DateTime::<Utc>::from_timestamp_micros(us)?
        .with_timezone(&FixedOffset::east_opt(8 * 3600)?)
        .format(format)
        .to_string();
    Text::new(&time).ok()
}

fn market_client(port: u16) -> Result<CnFuturesMarketClient<Channel>> {
    let endpoint = Endpoint::from_shared(format!("http://127.0.0.1:{port}"))?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(3));
    Ok(CnFuturesMarketClient::new(endpoint.connect_lazy()))
}

async fn multipliers(client: &mut CnFuturesMarketClient<Channel>) -> Result<Multipliers> {
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

fn archived_mark(
    data: GetMinuteBarsResponse,
    exchange: Exchange,
    venue: Venue,
    symbol: Text<81>,
    day: u32,
    received_us: u64,
) -> Option<InstrumentMark> {
    let instrument = data.contract?;
    let state = data.data_state?;
    if instrument.exchange != exchange as i32
        || !symbol
            .as_str()
            .eq_ignore_ascii_case(&instrument.native_code)
        || instrument.volume_multiple != f64::from(instrument.volume_multiple as i32)
        || instrument.volume_multiple <= 0.0
    {
        return None;
    }
    let now_ms = i64::try_from(received_us / 1000).ok()?;
    for bar in data.bars.into_iter().rev() {
        let end = bar.open_time_ms.checked_add(60_000)?;
        let provider_closed = (!bar.provisional && day <= state.latest_trading_day)
            || (day == state.live_trading_day && end <= state.live_through_ms);
        if bar.trading_day != day || !provider_closed || end > now_ms {
            continue;
        }
        if let Some(book) = bar.book {
            let Some((bid, ask)) = book.bids.first().zip(book.asks.first()) else {
                continue;
            };
            let Some((bid_price, ask_price)) =
                bid.price.and_then(price).zip(ask.price.and_then(price))
            else {
                continue;
            };
            let lots = |volume: f64| {
                (volume > 0.0 && volume == f64::from(volume as i32)).then_some(volume as i32)
            };
            let Some((bid_lots, ask_lots)) = bid
                .volume_lots
                .and_then(lots)
                .zip(ask.volume_lots.and_then(lots))
            else {
                continue;
            };
            if bid_price > ask_price {
                continue;
            }
            let start: Text<5> = local_clock(bar.open_time_ms.checked_mul(1000)?, "%H:%M")?;
            let finish: Text<5> = local_clock(end.checked_mul(1000)?, "%H:%M")?;
            return Some(InstrumentMark {
                venue,
                symbol,
                kind: InstrumentKind::Future,
                contract_multiple: instrument.volume_multiple as i32,
                trading_day: day,
                received_us,
                initial: true,
                paused: false,
                last_price: None,
                bid_price: Some(bid_price),
                ask_price: Some(ask_price),
                bid_lots: Some(bid_lots),
                ask_lots: Some(ask_lots),
                native_time: Text::new(&format!("{}–{}", start.as_str(), finish.as_str())).ok()?,
                last_trade_time: Text::default(),
                native_timestamp: None,
                last_trade_timestamp: None,
            });
        }
    }
    None
}

pub fn spawn(config: config::L1, stale_after_ms: u64) -> (SharedQuotes, [JoinHandle<()>; 3]) {
    let quotes = Arc::new(Mutex::new(Quotes::new(stale_after_ms)));
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
                        let mut book = book.lock().unwrap_or_else(|e| e.into_inner());
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
        let mut client = match market_client(config.metadata_port) {
            Ok(client) => client,
            Err(error) => {
                log::warn!("Viz L1 metadata client unavailable: {error:#}");
                return;
            }
        };
        loop {
            let delay = match multipliers(&mut client).await {
                Ok(rows) => {
                    let mut book = book.lock().unwrap_or_else(|e| e.into_inner());
                    book.metadata_us = now_us();
                    book.multipliers = rows;
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
    let book = quotes.clone();
    let history = tokio::task::spawn_local(async move {
        let mut client = match market_client(config.metadata_port) {
            Ok(client) => client,
            Err(error) => {
                log::warn!("Viz L1 history client unavailable: {error:#}");
                return;
            }
        };
        let mut interval = time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let request = book
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .history_request();
            let Some((key, symbol, day)) = request else {
                continue;
            };
            let venue = key.0;
            let Some(exchange) = Exchange::from_str_name(venue.as_str()) else {
                continue;
            };
            let response = client
                .get_minute_bars(GetMinuteBarsRequest {
                    exchange: exchange as i32,
                    contract: symbol.as_str().to_owned(),
                    range: Some(Range::TradingDay(day)),
                    include_empty_minutes: true,
                })
                .await;
            match response {
                Ok(response) => {
                    let mark = archived_mark(
                        response.into_inner(),
                        exchange,
                        venue,
                        symbol,
                        day,
                        now_us(),
                    );
                    if let Some(history) = book
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .history
                        .get_mut(&(key, day))
                    {
                        history.mark = mark;
                    }
                }
                Err(error) => log::warn!(
                    "Viz historical L1 unavailable for {} {}: {}",
                    venue.as_str(),
                    symbol.as_str(),
                    error.code()
                ),
            }
        }
    });
    (quotes, [market, metadata, history])
}

#[cfg(test)]
mod tests {
    use super::*;
    use cn_futures_state::{Adapter, Position, PositionRow, Side};
    use cn_market_proto::{Book5, BookLevel, Contract as FeedContract, DataState, MinuteBar};

    #[test]
    fn archived_l1_uses_the_last_provider_closed_book_of_the_requested_day() {
        let start = 1_795_071_540_000;
        let day = 20261119;
        let book = |bid, ask| Book5 {
            bids: vec![BookLevel {
                price: Some(bid),
                volume_lots: Some(2.0),
            }],
            asks: vec![BookLevel {
                price: Some(ask),
                volume_lots: Some(3.0),
            }],
        };
        let bar = |open_time_ms, trading_day, bid, ask| MinuteBar {
            open_time_ms,
            trading_day,
            provisional: true,
            book: Some(book(bid, ask)),
            ..Default::default()
        };
        let data = GetMinuteBarsResponse {
            contract: Some(FeedContract {
                exchange: Exchange::Shfe as i32,
                native_code: "rb2701".into(),
                volume_multiple: 10.0,
                ..Default::default()
            }),
            data_state: Some(DataState {
                live_trading_day: day,
                live_through_ms: start + 60_000,
                ..Default::default()
            }),
            bars: vec![
                bar(start - 60_000, day, 100.0, 101.0),
                bar(start, day, 2982.0, 2983.0),
                bar(start + 60_000, day, 3000.0, 3001.0),
                bar(start + 120_000, day + 1, 4000.0, 4001.0),
            ],
        };
        let received_us = (start + 180_000) as u64 * 1000;
        let symbol = Text::new("rb2701").unwrap();
        let archived = archived_mark(
            data.clone(),
            Exchange::Shfe,
            Venue::Shfe,
            symbol,
            day,
            received_us,
        )
        .unwrap();
        assert!(archived.initial);
        assert_eq!(archived.trading_day, day);
        assert_eq!(archived.bid_price, Some(2982.0));
        assert_eq!(archived.ask_price, Some(2983.0));
        assert_eq!(archived.native_time.as_str(), "14:59–15:00");
        assert!(archived.last_trade_time.as_str().is_empty());
        assert!(archived_mark(
            data,
            Exchange::Shfe,
            Venue::Shfe,
            Text::new("rb2705").unwrap(),
            day,
            received_us
        )
        .is_none());
    }

    #[test]
    fn held_contract_marks_follow_quotes_without_faking_a_new_trading_day_or_timestamp() {
        let mut quotes = Quotes::new(15_000);
        quotes.multipliers.insert((Venue::Shfe, "RB".into()), 10);
        quotes.metadata_us = now_us();
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
        assert!(state.marks[0].last_price.is_none());
        assert_eq!(state.marks[0].contract_multiple, 10);
    }
}
