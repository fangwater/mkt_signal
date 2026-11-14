use hyperliquid_l4::{book::OrderBook, decode::Decoded, wire::*, Decoder};
const SNAPSHOT: &[u8] = include_bytes!("fixtures/snapshot.json");
const UPDATES: &[u8] = include_bytes!("fixtures/updates.json");
fn header(kind: Kind, seq: u64, height: u64, previous: u64) -> Header {
    Header {
        epoch: [7; 16],
        sequence: seq,
        height,
        previous_height: previous,
        event_time_us: 1751427260000000,
        receipt_time_us: 1751427260000100,
        chunk_index: 0,
        chunk_count: 1,
        kind,
        coin: Coin::new("BTC").unwrap(),
    }
}
fn decode(raw: &[u8]) -> (Kind, u64, Vec<Event>) {
    let mut decoder = Decoder::new(16, 100).unwrap();
    let Decoded::Batch(batch) = decoder.decode(raw).unwrap() else {
        panic!("expected batch")
    };
    (batch.kind, batch.height, batch.events.to_vec())
}
fn encode(header: Header, events: &[Event]) -> Vec<u8> {
    let mut out = vec![0; HEADER_BYTES + events.len() * EVENT_BYTES];
    header.encode(events, &mut out).unwrap();
    out
}
#[test]
fn native_external_tags_exact_decimal_and_sparse_heights() {
    let (kind, height, events) = decode(SNAPSHOT);
    assert_eq!((kind, height), (Kind::Snapshot, 100));
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].oid, 1001);
    assert_eq!(events[0].size, 150_000_000);
    assert_eq!(events[0].order_time_us, 1751427259657000);
    let (kind, height, updates) = decode(UPDATES);
    assert_eq!((kind, height), (Kind::Update, 105));
    assert_eq!(updates[0].op, Op::New);
    assert_eq!(updates[0].insert_before, 1002);
    assert_eq!(updates[0].size, 50_000_000);
    assert_eq!(updates[1].original_size, 150_000_000);
    assert_eq!(updates[2].op, Op::Remove);
    assert_eq!(updates[0].user, [0x33; 20]);
}
#[test]
fn fifo_anchor_partial_fill_remove_and_empty_snapshot() {
    let (_, _, snapshot) = decode(SNAPSHOT);
    let (_, _, updates) = decode(UPDATES);
    let mut book = OrderBook::new(Coin::new("BTC").unwrap(), 100, 100).unwrap();
    assert!(book
        .ingest(&encode(header(Kind::Snapshot, 1, 100, 0), &snapshot))
        .unwrap());
    assert!(book
        .ingest(&encode(header(Kind::Update, 2, 105, 100), &updates))
        .unwrap());
    let ids: Vec<_> = book
        .orders_at_price(Side::Bid, decimal("106000").unwrap())
        .map(|e| e.oid)
        .collect();
    assert_eq!(ids, [1001, 1003, 1002]);
    assert_eq!(book.order(1001).unwrap().size, 125_000_000);
    assert!(book.order(2001).is_none());
    assert_eq!(
        book.level_size(Side::Bid, decimal("106000").unwrap()),
        Some(375_000_000)
    );
    let mut fresh = header(Kind::Snapshot, 1, 108, 0);
    fresh.epoch = [8; 16];
    assert!(book.ingest(&encode(fresh, &[])).unwrap());
    assert!(book.is_ready());
    assert!(book.is_empty());
}
#[test]
fn chunks_commit_atomically_and_missing_chunks_revoke_readiness() {
    let (_, _, snapshot) = decode(SNAPSHOT);
    let mut book = OrderBook::new(Coin::new("BTC").unwrap(), 100, 100).unwrap();
    let mut h = header(Kind::Snapshot, 1, 100, 0);
    h.chunk_count = 2;
    assert!(!book.ingest(&encode(h, &snapshot[..2])).unwrap());
    assert!(!book.is_ready());
    h.chunk_index = 1;
    assert!(book.ingest(&encode(h, &snapshot[2..])).unwrap());
    assert_eq!(book.len(), 3);
    let mut h = header(Kind::Update, 2, 105, 100);
    h.chunk_count = 2;
    let (_, _, updates) = decode(UPDATES);
    assert!(!book.ingest(&encode(h, &updates[..1])).unwrap());
    assert!(book
        .ingest(&encode(header(Kind::Update, 3, 106, 105), &[]))
        .is_err());
    assert!(!book.is_ready());
    assert!(book.order(1001).is_none());
}
#[test]
fn malformed_tail_mixed_coins_and_missing_status_emit_no_batch() {
    let mut decoder = Decoder::new(4, 100).unwrap();
    let text = std::str::from_utf8(UPDATES).unwrap();
    for bad in [
        text.replace("\"newSz\":\"1.25\"", "\"newSz\":\"NaN\""),
        text.replacen("\"coin\":\"BTC\"", "\"coin\":\"ETH\"", 1),
        text.replace("\"status\":\"open\"", "\"status\":\"canceled\""),
        format!("{text} false"),
    ] {
        assert!(decoder.decode(bad.as_bytes()).is_err());
        assert!(matches!(
            decoder.decode(SNAPSHOT).unwrap(),
            Decoded::Batch(_)
        ));
    }
}
#[test]
fn unsupported_precision_overflow_and_invalid_wire_are_rejected() {
    assert_eq!(decimal("0.00000001").unwrap(), 1);
    assert_eq!(decimal("184467440737.09551615").unwrap(), u64::MAX);
    for bad in [
        "0.000000001",
        "184467440737.09551616",
        "NaN",
        "1e2",
        "-1",
        "1.",
        ".1",
        "",
    ] {
        assert!(decimal(bad).is_err(), "{bad}");
    }
    let (_, _, events) = decode(SNAPSHOT);
    let raw = encode(header(Kind::Snapshot, 1, 100, 0), &events);
    let (h, bytes) = Header::decode(&raw).unwrap();
    assert_eq!(h.coin.as_str(), "BTC");
    assert_eq!(Event::decode(&bytes[..EVENT_BYTES]).unwrap(), events[0]);
    for len in 0..HEADER_BYTES {
        assert!(Header::decode(&raw[..len]).is_err());
    }
    let mut corrupt = raw.clone();
    corrupt[72..76].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(Header::decode(&corrupt).is_err());
}
#[test]
fn replay_and_mutation_inconsistency_require_new_snapshot() {
    let (_, _, snapshot) = decode(SNAPSHOT);
    let (_, _, mut updates) = decode(UPDATES);
    let mut book = OrderBook::new(Coin::new("BTC").unwrap(), 100, 100).unwrap();
    let initial = encode(header(Kind::Snapshot, 1, 100, 0), &snapshot);
    book.ingest(&initial).unwrap();
    updates[1].original_size = 0;
    assert!(book
        .ingest(&encode(header(Kind::Update, 2, 105, 100), &updates))
        .is_err());
    assert!(!book.is_ready());
    assert!(book.order(1003).is_none());
    book.ingest(&initial).unwrap();
    assert_eq!(book.len(), 3);
    assert!(book
        .ingest(&encode(header(Kind::Update, 3, 109, 105), &[]))
        .is_err());
    assert!(!book.is_ready());
}
#[test]
fn bounds_duplicate_oid_and_wrong_book_side_are_checked() {
    let mut small = Decoder::new(0, 2).unwrap();
    assert!(small.decode(SNAPSHOT).is_err());
    let text = std::str::from_utf8(SNAPSHOT).unwrap();
    let mut decoder = Decoder::new(4, 100).unwrap();
    assert!(decoder
        .decode(text.replace("\"oid\":1002", "\"oid\":1001").as_bytes())
        .is_err());
    assert!(decoder
        .decode(
            text.replacen("\"side\":\"B\"", "\"side\":\"A\"", 1)
                .as_bytes()
        )
        .is_err());
}

#[test]
fn field_order_is_irrelevant_and_terminal_statuses_can_be_zero() {
    let mut value: serde_json::Value = serde_json::from_slice(UPDATES).unwrap();
    let mut terminal = value["data"]["Updates"]["order_statuses"][0].clone();
    terminal["status"] = "filled".into();
    terminal["order"]["oid"] = 999.into();
    terminal["order"]["sz"] = "0.0".into();
    value["data"]["Updates"]["order_statuses"]
        .as_array_mut()
        .unwrap()
        .push(terminal);
    let mut rejected = value["data"]["Updates"]["order_statuses"][1].clone();
    rejected["status"] = "rejected".into();
    rejected["order"]["oid"] = 0.into();
    rejected["order"]["limitPx"] = "0.0".into();
    rejected["order"]["timestamp"] = 0.into();
    value["data"]["Updates"]["order_statuses"]
        .as_array_mut()
        .unwrap()
        .push(rejected);
    // Payload precedes channel, and sorted object fields put diffs before statuses.
    let raw = format!("{{\"data\":{},\"channel\":\"l4Book\"}}", value["data"]);
    let (_, height, events) = decode(raw.as_bytes());
    assert_eq!(height, 105);
    assert_eq!(events.len(), 3);
    let duplicate = raw.replacen("\"height\":105", "\"height\":105,\"height\":106", 1);
    assert!(Decoder::new(16, 100)
        .unwrap()
        .decode(duplicate.as_bytes())
        .is_err());
}

#[test]
fn new_epoch_recovers_missing_reset_and_freshness_is_explicit() {
    let (_, _, snapshot) = decode(SNAPSHOT);
    let mut book = OrderBook::new(Coin::new("BTC").unwrap(), 100, 100).unwrap();
    let mut h = header(Kind::Snapshot, 1, 100, 0);
    h.chunk_count = 2;
    book.ingest(&encode(h, &snapshot[..1])).unwrap();
    let mut fresh = header(Kind::Snapshot, 1, 110, 0);
    fresh.epoch = [9; 16];
    assert!(book.ingest(&encode(fresh, &snapshot)).unwrap());
    assert!(book.is_fresh(fresh.receipt_time_us + 1, 100));
    assert!(!book.is_fresh(fresh.receipt_time_us + 101, 100));
    assert!(!book.is_fresh(fresh.receipt_time_us - 1, 100));
}

#[test]
fn missing_optional_snapshot_user_is_enriched_by_a_later_diff() {
    let (_, _, mut snapshot) = decode(SNAPSHOT);
    let (_, _, updates) = decode(UPDATES);
    snapshot[0].flags &= !HAS_USER;
    snapshot[0].user = [0; 20];
    snapshot[2].flags &= !HAS_USER;
    snapshot[2].user = [0; 20];
    let mut book = OrderBook::new(Coin::new("BTC").unwrap(), 100, 100).unwrap();
    book.ingest(&encode(header(Kind::Snapshot, 1, 100, 0), &snapshot))
        .unwrap();
    book.ingest(&encode(header(Kind::Update, 2, 105, 100), &updates))
        .unwrap();
    assert_eq!(book.order(1001).unwrap().user, [0x11; 20]);
    assert_ne!(book.order(1001).unwrap().flags & HAS_USER, 0);
    assert!(book.order(2001).is_none());
    let bad = Event {
        oid: 1001,
        price: decimal("106000").unwrap(),
        size: 1,
        original_size: 125_000_000,
        user: [0x99; 20],
        flags: HAS_USER,
        op: Op::Update,
        ..Event::default()
    };
    assert!(book
        .ingest(&encode(header(Kind::Update, 3, 106, 105), &[bad]))
        .is_err());
    assert!(!book.is_ready());
}

#[test]
fn generated_queue_mutations_match_independent_vector_reference() {
    use std::collections::BTreeMap;
    let mut book = OrderBook::new(Coin::new("BTC").unwrap(), 1000, 1000).unwrap();
    book.ingest(&encode(header(Kind::Snapshot, 1, 100, 0), &[]))
        .unwrap();
    let mut reference = BTreeMap::<(u8, u64), Vec<(u64, u64)>>::new();
    let mut seed = 23_u64;
    let mut next_oid = 1;
    for index in 0..2000 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let side = if seed & 1 == 0 { Side::Bid } else { Side::Ask };
        let price = 100 + (seed >> 8) % 8;
        let queue = reference.entry((side as u8, price)).or_default();
        let event = if queue.is_empty() || seed.is_multiple_of(3) {
            let at = if queue.is_empty() {
                0
            } else {
                (seed >> 16) as usize % (queue.len() + 1)
            };
            let anchor = queue.get(at).map(|entry| entry.0);
            let event = Event {
                oid: next_oid,
                price,
                size: 10,
                side,
                op: Op::New,
                insert_before: anchor.unwrap_or(0),
                flags: if anchor.is_some() { HAS_ANCHOR } else { 0 },
                ..Event::default()
            };
            queue.insert(at, (next_oid, 10));
            next_oid += 1;
            event
        } else {
            let at = (seed >> 16) as usize % queue.len();
            let (oid, size) = queue[at];
            if seed & 4 == 0 {
                queue.remove(at);
                Event {
                    oid,
                    price,
                    op: Op::Remove,
                    ..Event::default()
                }
            } else {
                queue[at].1 += 1;
                Event {
                    oid,
                    price,
                    op: Op::Update,
                    size: size + 1,
                    original_size: size,
                    ..Event::default()
                }
            }
        };
        book.ingest(&encode(
            header(Kind::Update, index + 2, index + 101, index + 100),
            &[event],
        ))
        .unwrap();
        for (&(side, price), expected) in &reference {
            let side = if side == 1 { Side::Bid } else { Side::Ask };
            let actual: Vec<_> = book
                .orders_at_price(side, price)
                .map(|order| (order.oid, order.size))
                .collect();
            assert_eq!(&actual, expected, "step {index}");
        }
        assert_eq!(book.len(), reference.values().map(Vec::len).sum::<usize>());
    }
}
