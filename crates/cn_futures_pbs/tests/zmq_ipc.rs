use cn_futures_pbs::{
    ipc::{service_name, Publishers},
    msg::{MarketDataMsg, SOURCE_RESET, VENUES},
    sources::{zmq::ZmqSource, Source},
};
use iceoryx2::prelude::*;
use std::time::{Duration, Instant};

#[test]
fn zmq_to_ipc_preserves_snapshot_and_announces_continuity_boundaries() -> anyhow::Result<()> {
    let context = zmq::Context::new();
    let socket = context.socket(zmq::PUB)?;
    socket.set_linger(0)?;
    socket.bind("tcp://127.0.0.1:*")?;
    let endpoint = socket
        .get_last_endpoint()?
        .map_err(|_| anyhow::anyhow!("endpoint is not UTF8"))?;
    let mut source = ZmqSource::new(&endpoint, 1000)?;
    let root = format!(
        "cn_futures_pbs_test_{}_{}",
        std::process::id(),
        cn_futures_pbs::now_us()
    );
    let mut publishers = Publishers::new(&root)?;
    let node = NodeBuilder::new().create::<iceoryx2::service::ipc::Service>()?;
    let factory = node
        .service_builder(&ServiceName::new(&service_name(&root, "SHFE")?)?)
        .publish_subscribe::<MarketDataMsg>()
        .open()?;
    let subscriber = factory.subscriber_builder().create()?;
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut reset = false;
    let mut received = None;
    while Instant::now() < deadline && received.is_none() {
        socket.send(include_bytes!("fixtures/mdl_22_1.txt").as_slice(), 0)?;
        for _ in 0..20 {
            if let Some(msg) = source.poll()? {
                publishers.publish(msg)?;
            }
            while let Some(sample) = subscriber.receive()? {
                let msg = *sample.payload();
                if msg.flags & SOURCE_RESET != 0 {
                    reset = true;
                } else {
                    received = Some(msg);
                }
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    let msg = received.ok_or_else(|| anyhow::anyhow!("no local ZMQ/IPC snapshot"))?;
    assert!(reset);
    assert_eq!(msg.symbol(), b"ss2704");
    assert_eq!(msg.venue(), b"SHFE");
    assert_eq!(msg.bid[0].price, 14005.0);
    assert_eq!(msg.bid[0].quantity, 4);
    assert!(msg.sequence >= 2 && msg.publisher_epoch > 0 && msg.source_generation > 0);
    for _ in 0..64 {
        socket.send(include_bytes!("fixtures/mdl_22_1.txt").as_slice(), 0)?;
    }
    std::thread::sleep(Duration::from_millis(5));
    drop(socket);
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut disconnected = false;
    while Instant::now() < deadline {
        if source.poll()?.is_some_and(|event| {
            event.flags & SOURCE_RESET != 0 && event.source_generation > msg.source_generation
        }) {
            disconnected = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(disconnected);
    for _ in 0..20 {
        assert!(
            source.poll()?.is_none(),
            "buffered old quotes must not revive a disconnected stream"
        );
    }
    Ok(())
}

#[test]
#[ignore = "explicit read-only company-intranet feed; requires CN_FUTURES_LIVE_ZMQ_ENDPOINT"]
fn live_company_feed_reaches_all_six_ipc_venues() -> anyhow::Result<()> {
    let endpoint = std::env::var("CN_FUTURES_LIVE_ZMQ_ENDPOINT")?;
    cn_futures_pbs::config::Zmq {
        access_scope: "company_intranet".into(),
        endpoint: endpoint.clone(),
        receive_hwm: 100000,
    }
    .validate()?;
    let mut source = ZmqSource::new(&endpoint, 100000)?;
    let root = format!(
        "cn_futures_pbs_live_test_{}_{}",
        std::process::id(),
        cn_futures_pbs::now_us()
    );
    let mut publishers = Publishers::new(&root)?;
    let node = NodeBuilder::new().create::<iceoryx2::service::ipc::Service>()?;
    let mut subscribers = Vec::new();
    for venue in VENUES {
        let service = node
            .service_builder(&ServiceName::new(&service_name(&root, venue)?)?)
            .publish_subscribe::<MarketDataMsg>()
            .open()?;
        subscribers.push(service.subscriber_builder().create()?);
    }
    let mut counts = [0u64; 6];
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && counts.contains(&0) {
        if let Some(msg) = source.poll()? {
            publishers.publish(msg)?;
        }
        for (i, sub) in subscribers.iter().enumerate() {
            while let Some(sample) = sub.receive()? {
                let msg = sample.payload();
                if msg.flags & SOURCE_RESET != 0 {
                    continue;
                }
                assert_eq!(msg.venue(), VENUES[i].as_bytes());
                assert!(!msg.symbol().is_empty());
                assert!(msg.exchange_ts_us > 0 && msg.received_us > 0 && msg.sequence > 0);
                counts[i] += 1;
            }
        }
    }
    eprintln!("live company-intranet ZMQ -> IPC: {counts:?} venues={VENUES:?}");
    assert!(counts.iter().all(|n| *n > 0));
    Ok(())
}
