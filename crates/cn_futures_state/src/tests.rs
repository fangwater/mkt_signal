use super::*;
use std::time::Duration;

fn sample() -> Snapshot {
    let mut s = Snapshot::empty(Adapter::Yida, &"a".repeat(64)).unwrap();
    s.publisher_epoch = 1;
    s.sequence = 1;
    s.observed_us = publisher::now_us();
    s.position_ready = true;
    s.connected = true;
    s.ready = true;
    s.transport_ready = true;
    s.trading_day = Text::new("20261113").unwrap();
    s.positions.push(PositionRow {
        venue: Venue::Shfe,
        symbol: Text::new("cu2612").unwrap(),
        side: Side::Buy,
        position: Position {
            today: 2,
            yesterday: 3,
            frozen_today: 1,
            frozen_yesterday: 0,
        },
    });
    s
}
#[test]
fn fixed_text_and_framed_codec_reject_truncation_and_invalid_positions() {
    let s = sample();
    let encoded = bincode::serialize(&s).unwrap();
    let mut frame = Vec::from((encoded.len() as u32).to_le_bytes());
    frame.extend(encoded);
    let decoded = transport::decode(&frame).unwrap();
    assert_eq!(decoded.positions[0].position.today, 2);
    assert_eq!(decoded.positions[0].symbol.as_str(), "cu2612");
    assert!(transport::decode(&frame[..frame.len() - 1]).is_err());
    assert!(Text::<3>::new("铜ab").is_err());
    let mut bad = sample();
    bad.positions[0].position.frozen_today = 3;
    assert!(bad.validate().is_err());
    bad = sample();
    bad.position_ready = false;
    assert!(bad.validate().is_err());
    let json = serde_json::to_value(s).unwrap();
    assert_eq!(json["positions"][0]["symbol"], "cu2612");
}
#[test]
fn ipc_full_replacement_carries_freezing_flat_state_and_terminal_status() {
    let namespace = format!("native_positions_{}", publisher::now_us());
    let publisher = transport::StatePublisher::new(&namespace).unwrap();
    let subscriber = transport::StateSubscriber::open(&namespace).unwrap();
    let mut s = sample();
    publisher.publish(&s).unwrap();
    let first = subscriber.latest().unwrap().unwrap();
    assert_eq!(first.positions[0].position.frozen_today, 1);
    s.sequence = 2;
    s.positions[0].position.today = 1;
    s.positions[0].position.frozen_today = 0;
    publisher.publish(&s).unwrap();
    let second = subscriber.latest().unwrap().unwrap();
    assert_eq!(second.positions[0].position.today, 1);
    s.sequence = 3;
    s.positions.clear();
    publisher.publish(&s).unwrap();
    assert!(subscriber.latest().unwrap().unwrap().positions.is_empty());
    s.sequence = 4;
    s.runtime_status = RuntimeStatus::Stopped;
    s.connected = false;
    s.ready = false;
    s.transport_ready = false;
    publisher.publish(&s).unwrap();
    assert_eq!(
        subscriber.latest().unwrap().unwrap().runtime_status,
        RuntimeStatus::Stopped
    );
}
#[test]
fn bounded_background_publisher_replaces_pending_complete_snapshots() {
    let namespace = format!("native_mailbox_{}", publisher::now_us());
    let cfg = Config {
        namespace: namespace.clone(),
        interval_ms: 1000,
    };
    let mut producer = publisher::Producer::start(&cfg, Adapter::Yida, &"a".repeat(64)).unwrap();
    let subscriber = transport::StateSubscriber::open(&namespace).unwrap();
    for n in 1..=100 {
        producer
            .capture(true, |s| {
                s.trading_day = Text::new("20261113")?;
                s.position_ready = true;
                s.connected = true;
                s.ready = true;
                s.transport_ready = true;
                s.positions.push(PositionRow {
                    venue: Venue::Shfe,
                    symbol: Text::new("cu2612")?,
                    side: Side::Buy,
                    position: Position {
                        today: n,
                        ..Default::default()
                    },
                });
                Ok(())
            })
            .unwrap();
    }
    let mut last = None;
    for _ in 0..100 {
        if let Some(s) = subscriber.latest().unwrap() {
            last = Some(s);
        }
        if last.as_ref().is_some_and(|s| s.sequence == 100) {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let last = last.unwrap();
    assert_eq!(last.sequence, 100);
    assert_eq!(last.positions.len(), 1);
    assert_eq!(last.positions[0].position.today, 100);
    assert!(last.replaced_snapshots > 0);
    producer.finish().unwrap();
}

thread_local! {
    static COUNT_ALLOCATIONS:std::cell::Cell<bool>=const {std::cell::Cell::new(false)};
    static ALLOCATIONS:std::cell::Cell<usize>=const {std::cell::Cell::new(0)};
}
struct CountingAllocator;
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
unsafe impl std::alloc::GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let _ = COUNT_ALLOCATIONS.try_with(|enabled| {
            if enabled.get() {
                let _ = ALLOCATIONS.try_with(|n| n.set(n.get() + 1));
            }
        });
        std::alloc::System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        std::alloc::System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, size: usize) -> *mut u8 {
        let _ = COUNT_ALLOCATIONS.try_with(|enabled| {
            if enabled.get() {
                let _ = ALLOCATIONS.try_with(|n| n.set(n.get() + 1));
            }
        });
        std::alloc::System.realloc(ptr, layout, size)
    }
}
#[test]
fn snapshot_capture_does_not_allocate_or_wait_for_encoding() {
    let cfg = Config {
        namespace: format!("native_capture_{}", publisher::now_us()),
        interval_ms: 1000,
    };
    let mut producer = publisher::Producer::start(&cfg, Adapter::Yida, &"a".repeat(64)).unwrap();
    ALLOCATIONS.with(|n| n.set(0));
    COUNT_ALLOCATIONS.with(|enabled| enabled.set(true));
    for _ in 0..1000 {
        producer
            .capture(true, |s| {
                s.position_ready = true;
                s.trading_day = Text::new("20261113")?;
                s.positions.push(PositionRow {
                    venue: Venue::Shfe,
                    symbol: Text::new("cu2612")?,
                    side: Side::Buy,
                    position: Position {
                        today: 2,
                        yesterday: 3,
                        frozen_today: 1,
                        frozen_yesterday: 0,
                    },
                });
                Ok(())
            })
            .unwrap();
    }
    COUNT_ALLOCATIONS.with(|enabled| enabled.set(false));
    assert_eq!(ALLOCATIONS.with(|n| n.get()), 0);
    producer.finish().unwrap();
}

#[test]
fn failed_capture_invalidates_the_entire_position_snapshot() {
    let cfg = Config {
        namespace: format!("native_failed_capture_{}", publisher::now_us()),
        interval_ms: 1000,
    };
    let mut producer = publisher::Producer::start(&cfg, Adapter::Yida, &"a".repeat(64)).unwrap();
    let subscriber = transport::StateSubscriber::open(&cfg.namespace).unwrap();
    let result = producer.capture(true, |s| {
        s.positions.push(PositionRow {
            venue: Venue::Shfe,
            symbol: Text::new("cu2612")?,
            side: Side::Buy,
            position: Position::default(),
        });
        anyhow::bail!("synthetic capture failure after first row")
    });
    assert!(result.is_err());
    let mut last = None;
    for _ in 0..100 {
        last = subscriber.latest().unwrap();
        if last.is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let last = last.unwrap();
    assert_eq!(last.runtime_status, RuntimeStatus::Failed);
    assert!(!last.position_ready);
    assert!(last.positions.is_empty());
    producer.finish().unwrap();
}
#[test]
fn maximum_full_snapshot_fits_one_complete_ipc_message() {
    let mut s = sample();
    s.positions.clear();
    for i in 0..MAX_POSITIONS {
        s.positions.push(PositionRow {
            venue: Venue::Shfe,
            symbol: Text::new(&format!("{:0>81}", i)).unwrap(),
            side: Side::Buy,
            position: Position::default(),
        });
    }
    s.validate().unwrap();
    assert!(bincode::serialized_size(&s).unwrap() < (PAYLOAD - 4) as u64);
}
