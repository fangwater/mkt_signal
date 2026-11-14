use hyperliquid_l4::{book::OrderBook, decode::Decoded, wire::*, Decoder};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};
struct Count;
static ALLOCS: AtomicU64 = AtomicU64::new(0);
unsafe impl GlobalAlloc for Count {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.realloc(p, l, n)
    }
}
#[global_allocator]
static ALLOCATOR: Count = Count;
fn measure(label: &str, count: usize, repeats: usize, mut work: impl FnMut()) {
    for _ in 0..5 {
        work();
    }
    let mut times = Vec::with_capacity(repeats);
    let allocations = ALLOCS.load(Ordering::Relaxed);
    let start = Instant::now();
    for _ in 0..repeats {
        let tick = Instant::now();
        work();
        times.push(tick.elapsed().as_nanos());
    }
    let elapsed = start.elapsed();
    let allocations = ALLOCS.load(Ordering::Relaxed) - allocations;
    times.sort_unstable();
    println!(
        "{label} events_per_sec={:.0} p50_us={:.1} p99_us={:.1} warm_allocations_per_frame={:.2}",
        (count * repeats) as f64 / elapsed.as_secs_f64(),
        times[repeats / 2] as f64 / 1000.,
        times[(repeats * 99 / 100).min(repeats - 1)] as f64 / 1000.,
        allocations as f64 / repeats as f64
    );
    assert_eq!(allocations, 0, "{label}: warmed hot path allocated");
}
fn main() {
    let fixture: serde_json::Value =
        serde_json::from_slice(include_bytes!("../tests/fixtures/snapshot.json")).unwrap();
    for count in [1000, 100_000] {
        let mut value = fixture.clone();
        let proto = value["data"]["Snapshot"]["levels"][0][0].clone();
        let orders = (0..count)
            .map(|i| {
                let mut order = proto.clone();
                order["oid"] = (i + 1).into();
                order
            })
            .collect();
        value["data"]["Snapshot"]["levels"][0] = serde_json::Value::Array(orders);
        value["data"]["Snapshot"]["levels"][1] = serde_json::json!([]);
        let raw = serde_json::to_vec(&value).unwrap();
        let mut decoder = Decoder::new(count, count + 1).unwrap();
        let repeats = if count == 1000 { 200 } else { 20 };
        println!("snapshot orders={count} source_bytes={}", raw.len());
        measure("snapshot_decode", count, repeats, || {
            let Decoded::Batch(batch) = decoder.decode(black_box(&raw)).unwrap() else {
                panic!()
            };
            assert_eq!(batch.events.len(), count);
            black_box(batch.events);
        });
        let start = Instant::now();
        for _ in 0..repeats {
            black_box(serde_json::from_slice::<serde_json::Value>(black_box(&raw)).unwrap());
        }
        println!(
            "DOM_parse_only_events_per_sec={:.0}",
            (count * repeats) as f64 / start.elapsed().as_secs_f64()
        );
        if count == 1000 {
            let mut output = vec![0; MAX_CHUNK_BYTES];
            let coin = Coin::new("BTC").unwrap();
            let mut book = OrderBook::new(coin, count + 1, count + 1).unwrap();
            let mut header = Header {
                epoch: [1; 16],
                sequence: 1,
                height: 100,
                previous_height: 0,
                event_time_us: 1751427260000000,
                receipt_time_us: 1751427260000100,
                chunk_index: 0,
                chunk_count: 1,
                kind: Kind::Snapshot,
                coin,
            };
            let Decoded::Batch(snapshot) = decoder.decode(&raw).unwrap() else {
                panic!()
            };
            header.chunk_count = snapshot.events.len().div_ceil(256) as u32;
            for (index, chunk) in snapshot.events.chunks(256).enumerate() {
                header.chunk_index = index as u32;
                let len = header.encode(chunk, &mut output).unwrap();
                book.ingest(&output[..len]).unwrap();
            }
            let update_template: serde_json::Value =
                serde_json::from_slice(include_bytes!("../tests/fixtures/updates.json")).unwrap();
            let proto = update_template["data"]["Updates"]["book_diffs"][1].clone();
            let make_update = |from: &str, to: &str| {
                let diffs: Vec<_> = (0..count)
                    .map(|i| {
                        let mut diff = proto.clone();
                        diff["oid"] = (i + 1).into();
                        diff["raw_book_diff"]["update"]["origSz"] = from.into();
                        diff["raw_book_diff"]["update"]["newSz"] = to.into();
                        diff
                    })
                    .collect();
                serde_json::to_vec(&serde_json::json!({"channel":"l4Book","data":{"Updates":{"time":1751427260000_u64,"height":101,"order_statuses":[],"book_diffs":diffs}}})).unwrap()
            };
            let updates = [make_update("1.5", "1.25"), make_update("1.25", "1.5")];
            let mut turn = 0;
            measure("delta_decode", count, 1000, || {
                let Decoded::Batch(batch) = decoder.decode(&updates[turn % 2]).unwrap() else {
                    panic!()
                };
                black_box(batch.events);
                turn += 1;
            });
            turn = 0;
            measure("delta_decode_encode_apply", count, 1000, || {
                let Decoded::Batch(batch) = decoder.decode(&updates[turn % 2]).unwrap() else {
                    panic!()
                };
                header.sequence += 1;
                header.previous_height = header.height;
                header.height += 1;
                header.kind = Kind::Update;
                for (index, chunk) in batch.events.chunks(256).enumerate() {
                    header.chunk_index = index as u32;
                    let len = header.encode(chunk, &mut output).unwrap();
                    book.ingest(&output[..len]).unwrap();
                }
                black_box(book.order(1).unwrap().size);
                turn += 1;
            });
        }
    }
}
