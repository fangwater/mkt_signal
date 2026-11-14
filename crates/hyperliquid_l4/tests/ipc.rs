use hyperliquid_l4::{ipc::IpcSink, runtime::Sink, wire::*};
use iceoryx2::{prelude::*, service::ipc};

#[test]
fn transport_loan_preserves_exact_binary_prefix() {
    let directory = std::env::temp_dir().join(format!("hl_l4_ipc_{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let mut config = iceoryx2::config::Config::default();
    config.global.set_root_path(
        &iceoryx2::prelude::Path::new(directory.as_os_str().as_encoded_bytes()).unwrap(),
    );
    {
        let service_name = "test/hyperliquid/l4";
        let mut sink = IpcSink::new_with_config(service_name, &config).unwrap();
        let node = NodeBuilder::new()
            .config(&config)
            .create::<ipc::Service>()
            .unwrap();
        let factory = node
            .service_builder(&ServiceName::new(service_name).unwrap())
            .publish_subscribe::<[u8]>()
            .open()
            .unwrap();
        let sub = factory.subscriber_builder().create().unwrap();
        let header = Header {
            epoch: [3; 16],
            sequence: 1,
            height: 123,
            previous_height: 0,
            event_time_us: 123000,
            receipt_time_us: 124000,
            chunk_index: 0,
            chunk_count: 1,
            kind: Kind::Snapshot,
            coin: Coin::new("BTC").unwrap(),
        };
        let event = Event {
            oid: 7,
            price: 1,
            size: 1,
            side: Side::Bid,
            ..Event::default()
        };
        sink.publish(&header, &[event]).unwrap();
        let received = sub.receive().unwrap().unwrap();
        let mut expected = vec![0; HEADER_BYTES + EVENT_BYTES];
        header.encode(&[event], &mut expected).unwrap();
        assert_eq!(received.payload(), expected);
    }
    std::fs::remove_dir_all(directory).unwrap();
}
