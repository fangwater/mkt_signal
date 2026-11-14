use mkt_parsers::{bitget, gate};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct CountingAllocator;
thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.try_with(Cell::get).unwrap_or(false) {
            ALLOCATIONS.with(|n| n.set(n.get() + 1));
        }
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if COUNTING.try_with(Cell::get).unwrap_or(false) {
            ALLOCATIONS.with(|n| n.set(n.get() + 1));
        }
        System.realloc(ptr, layout, size)
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

// Layouts from the venue schemas. Deliberately distinct bid/ask values and
// matching/push timestamps catch swapped offsets and timestamp regressions.
fn fixture(bitget: bool, spot: bool, extra_root: usize) -> Vec<u8> {
    let base = if bitget { 64 } else { 59 };
    let mut raw = vec![0; 8 + base + extra_root];
    raw[0..2].copy_from_slice(&((base + extra_root) as u16).to_le_bytes());
    raw[2..4].copy_from_slice(&(if bitget { 1002_u16 } else { 1 }).to_le_bytes());
    raw[4..6].copy_from_slice(&1_u16.to_le_bytes());
    raw[6..8].copy_from_slice(&5_u16.to_le_bytes());
    let mut put = |offset: usize, value: i64| {
        raw[8 + offset..16 + offset].copy_from_slice(&value.to_le_bytes());
    };
    put(0, 1_700_000_000_000_000);
    if bitget {
        for (off, value) in [
            (8, 10001),
            (16, 2300),
            (24, 10002),
            (32, 4500),
            (42, 123),
            (50, 1_700_000_000_123_456),
        ] {
            put(off, value);
        }
        raw[8 + 40] = (-2_i8) as u8;
        raw[8 + 41] = (-3_i8) as u8;
    } else {
        put(9, 1_700_000_000_123_456);
        put(17, 123);
        let (bid, ask) = if spot { (27, 43) } else { (43, 27) };
        for (off, value) in [(bid, 10001), (bid + 8, 2300), (ask, 10002), (ask + 8, 4500)] {
            put(off, value);
        }
        raw[8 + 25] = (-2_i8) as u8;
        raw[8 + 26] = (-3_i8) as u8;
        let channel = if spot {
            "spot.book_ticker"
        } else {
            "futures.book_ticker"
        };
        raw.push(channel.len() as u8);
        raw.extend_from_slice(channel.as_bytes());
    }
    let symbol = if bitget { "BTCUSDT" } else { "BTC_USDT" };
    raw.push(symbol.len() as u8);
    raw.extend_from_slice(symbol.as_bytes());
    raw
}

#[test]
fn borrowed_bbo_is_allocation_free_and_preserves_wire_fields() {
    for (is_bitget, spot) in [(true, false), (false, false), (false, true)] {
        for extra in [0, 8] {
            let raw = fixture(is_bitget, spot, extra);
            ALLOCATIONS.with(|n| n.set(0));
            COUNTING.with(|v| v.set(true));
            let view = if is_bitget {
                bitget::parse_sbe_books1_view(&raw).unwrap().unwrap()
            } else {
                gate::parse_sbe_bbo_view(&raw, spot).unwrap()
            };
            let prices = view.prices().unwrap();
            COUNTING.with(|v| v.set(false));
            assert_eq!(ALLOCATIONS.with(Cell::get), 0);
            assert_eq!(view.seq_id, 123);
            assert_eq!(view.timestamp_us, 1_700_000_000_123_456);
            assert_eq!(view.symbol, if is_bitget { "BTCUSDT" } else { "BTC_USDT" });
            assert!(view.symbol.as_ptr() >= raw.as_ptr());
            assert!(view.symbol.as_ptr() < raw.as_ptr().wrapping_add(raw.len()));
            for (actual, expected) in prices.into_iter().zip([100.01, 2.3, 100.02, 4.5]) {
                assert!((actual - expected).abs() < 1e-12);
            }
            let old = if is_bitget {
                bitget::parse_sbe_books1(&raw).unwrap().remove(0).symbol
            } else if spot {
                gate::parse_spot_sbe_bbo(&raw).unwrap().symbol
            } else {
                gate::parse_sbe_bbo(&raw).unwrap().symbol
            };
            assert_eq!(old, "BTCUSDT");
        }
    }
}

#[test]
fn borrowed_bbo_rejects_every_truncation_wrong_schema_and_invalid_utf8() {
    for (is_bitget, spot) in [(true, false), (false, false), (false, true)] {
        let raw = fixture(is_bitget, spot, 0);
        let rejected = |raw: &[u8]| {
            if is_bitget {
                bitget::parse_sbe_books1_view(raw).ok().flatten().is_none()
            } else {
                gate::parse_sbe_bbo_view(raw, spot).is_none()
            }
        };
        for end in 0..raw.len() {
            assert!(rejected(&raw[..end]), "length={end}");
        }
        let mut broken = raw.clone();
        broken[4] = 99;
        assert!(rejected(&broken));
        let mut broken = raw;
        *broken.last_mut().unwrap() = 0xff;
        assert!(rejected(&broken));
    }
}

#[test]
#[ignore = "manual microbenchmark; run in release mode with --nocapture"]
fn benchmark_borrowed_and_owned_bbo() {
    use std::{hint::black_box, time::Instant};
    const N: u32 = 1_000_000;
    for (is_bitget, spot) in [(true, false), (false, false), (false, true)] {
        let raw = fixture(is_bitget, spot, 0);
        let start = Instant::now();
        for _ in 0..N {
            if is_bitget {
                black_box(bitget::parse_sbe_books1(black_box(&raw)).unwrap());
            } else if spot {
                black_box(gate::parse_spot_sbe_bbo(black_box(&raw)));
            } else {
                black_box(gate::parse_sbe_bbo(black_box(&raw)));
            }
        }
        let owned = start.elapsed().as_nanos() / N as u128;
        let start = Instant::now();
        for _ in 0..N {
            let view = if is_bitget {
                bitget::parse_sbe_books1_view(black_box(&raw))
                    .unwrap()
                    .unwrap()
            } else {
                gate::parse_sbe_bbo_view(black_box(&raw), spot).unwrap()
            };
            black_box((view.symbol, view.timestamp_us, view.seq_id, view.prices()));
        }
        let borrowed = start.elapsed().as_nanos() / N as u128;
        eprintln!(
            "bitget={is_bitget} spot={spot}: owned={owned}ns borrowed={borrowed}ns per frame"
        );
    }
}
