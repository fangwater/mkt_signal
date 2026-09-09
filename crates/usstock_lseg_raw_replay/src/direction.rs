//! Integer-price counterpart of direction_reference.py. One owner per RIC.
use crate::event_codec::classify_trade_direction;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decision {
    pub side: u8,
    pub method: u8,
    pub estimated: bool,
    pub forced: bool,
}

impl Decision {
    fn new(side: u8, method: u8) -> Self {
        Self {
            side,
            method,
            estimated: method != 9,
            forced: method == 1 || (6..=8).contains(&method),
        }
    }
}

#[derive(Clone)]
struct Quote {
    source: u64,
    price: i64,
    size: u64,
    venue: String,
}

#[derive(Clone, Copy)]
struct Trade {
    source: u64,
    price: i64,
    decision: Decision,
}

#[derive(Default)]
pub struct DirectionState {
    bid: BTreeMap<(u64, u64), Quote>,
    ask: BTreeMap<(u64, u64), Quote>,
    trades: BTreeMap<(u64, u64), Trade>,
}

impl DirectionState {
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    pub fn quote(
        &mut self,
        bid: bool,
        event: u64,
        source: u64,
        order: u64,
        price: i64,
        size: u64,
        venue: String,
    ) {
        let rows = if bid { &mut self.bid } else { &mut self.ask };
        rows.insert(
            (event, order),
            Quote {
                source,
                price,
                size,
                venue,
            },
        );
    }

    pub fn classify(
        &mut self,
        event: u64,
        source: u64,
        order: u64,
        price: i64,
        venue: &str,
        order_side: u16,
    ) -> Decision {
        if classify_trade_direction(venue, order_side).2 == 2 {
            return Decision::new(b'N', 9);
        }
        let result = if matches!(order_side, 1 | 2) {
            Decision::new(if order_side == 1 { b'S' } else { b'B' }, 1)
        } else {
            let prior = |rows: &BTreeMap<(u64, u64), Quote>| {
                rows.range(..(event, 0))
                    .rev()
                    .find(|((_, n), q)| *n < order && q.source <= source)
                    .map(|(_, q)| q.clone())
                    .filter(|q| q.price > 0 && q.size > 0 && q.size != u64::MAX)
            };
            let bid = prior(&self.bid);
            let ask = prior(&self.ask);
            let crossed = matches!((&bid, &ask), (Some(b), Some(a)) if b.price >= a.price);
            let buy = !crossed
                && ask
                    .as_ref()
                    .is_some_and(|q| !venue.is_empty() && q.venue == venue && price >= q.price);
            let sell = !crossed
                && bid
                    .as_ref()
                    .is_some_and(|q| !venue.is_empty() && q.venue == venue && price <= q.price);
            let normal = !crossed && bid.is_some() && ask.is_some();
            let quote_result = if buy != sell {
                Some(Decision::new(if buy { b'B' } else { b'S' }, 2))
            } else if normal {
                let b = bid.as_ref().unwrap().price;
                let a = ask.as_ref().unwrap().price;
                if price >= a || price <= b {
                    Some(Decision::new(if price >= a { b'B' } else { b'S' }, 3))
                } else {
                    let delta = i128::from(price) * 2 - i128::from(b) - i128::from(a);
                    (delta != 0).then(|| Decision::new(if delta > 0 { b'B' } else { b'S' }, 4))
                }
            } else {
                None
            };
            quote_result.unwrap_or_else(|| {
                let mut previous = None;
                for ((_, n), trade) in self.trades.range(..(event, 0)).rev() {
                    if *n >= order || trade.source > source {
                        continue;
                    }
                    if trade.price != price {
                        return Decision::new(
                            if price > trade.price { b'B' } else { b'S' },
                            if normal { 5 } else { 6 },
                        );
                    }
                    if previous.is_none() && trade.decision.method <= 6 {
                        previous = Some(trade.decision.side);
                    }
                }
                previous.map_or_else(|| Decision::new(b'B', 8), |side| Decision::new(side, 7))
            })
        };
        self.trades.insert(
            (event, order),
            Trade {
                source,
                price,
                decision: result,
            },
        );
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_python_on_identical_mixed_stream() {
        use std::io::Write;
        use std::process::{Command, Stdio};
        let mut state = DirectionState::default();
        let mut input = Vec::new();
        let mut expected = Vec::new();
        let mut rust_ns = 0_u128;
        for i in 1..=60_000_u64 {
            if i % 101 == 0 {
                state.reset();
                input.push(serde_json::json!(["reset"]));
            }
            let event = i.saturating_sub(i % 7);
            let price = 100 + (i * 17 % 11) as i64;
            let venue = ["NAS", "NYS", "ADF", "?"][(i % 4) as usize];
            if i % 3 == 0 {
                let bid = i % 2 == 0;
                let size = if i % 13 == 0 { 0 } else { 10 };
                let start = std::time::Instant::now();
                state.quote(bid, event, i, i, price, size, venue.to_string());
                rust_ns += start.elapsed().as_nanos();
                input.push(serde_json::json!([
                    "quote",
                    if bid { "bid" } else { "ask" },
                    event,
                    i,
                    i,
                    price,
                    size,
                    venue
                ]));
            } else {
                let order_side = if i % 11 == 0 {
                    (i % 2 + 1) as u16
                } else {
                    65535
                };
                let start = std::time::Instant::now();
                let result = state.classify(event, i, i, price, venue, order_side);
                rust_ns += start.elapsed().as_nanos();
                expected.push(serde_json::json!([
                    (result.side as char).to_string(),
                    result.method,
                    result.estimated,
                    result.forced
                ]));
                input.push(serde_json::json!([
                    "trade", event, i, i, price, venue, order_side
                ]));
            }
        }
        let mut child = Command::new("python").args(["-c", "import json,sys,time\nfrom direction_reference import DirectionState\ns=DirectionState(); out=[]; elapsed=0\nfor row in json.load(sys.stdin):\n if row[0]=='reset': s.reset(); continue\n start=time.perf_counter_ns()\n if row[0]=='quote': s.quote(*row[1:])\n else: out.append(s.classify(*row[1:]))\n elapsed+=time.perf_counter_ns()-start\nprint(json.dumps({'results':out,'elapsed_ns':elapsed}))"])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(&serde_json::to_vec(&input).unwrap())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success());
        let actual: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(actual["results"], serde_json::json!(expected));
        eprintln!("direction core benchmark: 60000 events, 40000 decisions; rust_ns={rust_ns}; python_ns={}", actual["elapsed_ns"]);
    }

    #[test]
    fn python_reference_cases() {
        let mut s = DirectionState::default();
        assert_eq!(s.classify(1, 1, 1, 100, "BAT", 1), Decision::new(b'S', 1));
        assert_eq!(s.classify(2, 2, 2, 100, "BAT", 2), Decision::new(b'B', 1));
        assert_eq!(s.classify(3, 3, 3, 100, "ADF", 2), Decision::new(b'N', 9));
        s.reset();
        s.quote(true, 1, 1, 1, 100, 1, "NAS".into());
        s.quote(false, 1, 1, 1, 110, 1, "NAS".into());
        assert_eq!(
            s.classify(2, 2, 2, 111, "NYS", 65535),
            Decision::new(b'B', 3)
        );
        assert_eq!(
            s.classify(3, 3, 3, 104, "NYS", 65535),
            Decision::new(b'S', 4)
        );
        assert_eq!(
            s.classify(4, 4, 4, 105, "NYS", 65535),
            Decision::new(b'B', 5)
        );
        s.quote(true, 5, 5, 5, 110, 1, "NAS".into());
        assert_eq!(
            s.classify(6, 6, 6, 109, "NAS", 65535),
            Decision::new(b'B', 6)
        );
        s.reset();
        s.quote(false, 10, 10, 1, 100, 1, "NAS".into());
        assert_eq!(s.classify(10, 10, 2, 100, "NAS", 65535).method, 8);
        assert_eq!(s.classify(11, 11, 3, 100, "NAS", 65535).method, 2);
        s.quote(false, 12, 12, 4, 100, 1, "NYS".into());
        assert_eq!(s.classify(13, 13, 5, 100, "NAS", 65535).method, 7);
        s.reset();
        assert_eq!(s.classify(14, 14, 6, 100, "NAS", 65535).method, 8);
    }
}
