use cn_futures_pbs::{
    msg::{MarketDataMsg, SOURCE_RESET, VENUES},
    sources::{redis::RedisSource, redundant_redis::RedundantRedisSource, zmq, Source},
};
use redis::{Parser, Value};
use std::{
    io::Write,
    net::{SocketAddr, TcpListener, TcpStream},
    sync::mpsc,
    time::{Duration, Instant},
};

const FRAMES: [&[u8]; 6] = [
    include_bytes!("fixtures/mdl_21_1.txt"),
    include_bytes!("fixtures/mdl_22_1.txt"),
    include_bytes!("fixtures/mdl_22_3.txt"),
    include_bytes!("fixtures/mdl_23_1.txt"),
    include_bytes!("fixtures/mdl_24_1.txt"),
    include_bytes!("fixtures/mdl_26_1.txt"),
];

fn acknowledge(socket: &mut TcpStream, reply_to_ping: bool) {
    let mut parser = Parser::new();
    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    for (index, topic) in zmq::TOPICS.into_iter().enumerate() {
        assert_eq!(
            parser.parse_value(&mut *socket).unwrap(),
            Value::Array(vec![
                Value::BulkString(b"SUBSCRIBE".to_vec()),
                Value::BulkString(topic.as_bytes().to_vec())
            ])
        );
        write!(
            socket,
            "*3\r\n$9\r\nsubscribe\r\n${}\r\n{topic}\r\n:{}\r\n",
            topic.len(),
            index + 1
        )
        .unwrap();
    }
    assert_eq!(
        parser.parse_value(&mut *socket).unwrap(),
        Value::Array(vec![Value::BulkString(b"PING".to_vec())])
    );
    if reply_to_ping {
        socket.write_all(b"+PONG\r\n").unwrap();
    }
}

fn quote_reply(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut reply = format!(
        "*3\r\n$7\r\nmessage\r\n${}\r\n{topic}\r\n${}\r\n",
        topic.len(),
        payload.len()
    )
    .into_bytes();
    reply.extend_from_slice(payload);
    reply.extend_from_slice(b"\r\n");
    reply
}

fn wait_for_message(source: &mut impl Source) -> MarketDataMsg {
    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline {
        if let Some(message) = source.poll().unwrap() {
            return message;
        }
    }
    panic!("timed out waiting for Redis quote or reset");
}

#[test]
fn futures_l2_uses_the_already_subscribed_socket_to_cover_primary_disconnect() {
    let listeners = [
        TcpListener::bind("127.0.0.1:0").unwrap(),
        TcpListener::bind("127.0.0.1:0").unwrap(),
    ];
    let [primary, standby] = listeners.each_ref().map(|listener| {
        let SocketAddr::V4(endpoint) = listener.local_addr().unwrap() else {
            unreachable!();
        };
        endpoint
    });
    let (sent, observed) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let [mut primary, mut standby] = listeners.map(|listener| {
            let mut socket = listener.accept().unwrap().0;
            acknowledge(&mut socket, true);
            socket
        });
        let topic = zmq::TOPICS[1];
        let frame = &FRAMES[1][14..];
        let seq_field = frame.iter().rposition(|&b| b == b',').unwrap();
        let send = |stream: &mut TcpStream, seq: u64| {
            let mut payload = frame[..=seq_field].to_vec();
            payload.extend_from_slice(seq.to_string().as_bytes());
            payload.extend_from_slice(if seq == 102 { b"\r\n" } else { b"\n" });
            stream.write_all(&quote_reply(topic, &payload)).unwrap();
        };
        send(&mut primary, 100);
        observed.recv_timeout(Duration::from_secs(4)).unwrap();
        send(&mut standby, 100);
        send(&mut standby, 102);
        send(&mut standby, 105);
        drop(primary);
        observed.recv_timeout(Duration::from_secs(4)).unwrap();
    });

    let mut source = RedundantRedisSource::new(primary, standby).unwrap();
    let initial_reset = wait_for_message(&mut source);
    assert_ne!(initial_reset.flags & SOURCE_RESET, 0);
    let anchor = wait_for_message(&mut source);
    assert_eq!(anchor.source_sequence, 100);
    sent.send(()).unwrap();
    for seq in [102, 105] {
        let quote = wait_for_message(&mut source);
        assert_eq!(quote.flags & SOURCE_RESET, 0);
        assert_eq!(quote.source_generation, anchor.source_generation);
        assert_eq!(quote.source_sequence, seq);
    }
    sent.send(()).unwrap();
    server.join().unwrap();
}

#[test]
fn subscribes_to_all_venues_and_preserves_mdl_fields_across_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let SocketAddr::V4(endpoint) = listener.local_addr().unwrap() else {
        unreachable!();
    };
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        acknowledge(&mut socket, true);
        for (index, (topic, frame)) in zmq::TOPICS.into_iter().zip(FRAMES).enumerate() {
            let payload = if index == 1 { frame } else { &frame[14..] };
            let reply = quote_reply(topic, payload);
            if index == 0 {
                socket.write_all(&reply[..reply.len() - 20]).unwrap();
                std::thread::sleep(Duration::from_millis(160));
                socket.write_all(&reply[reply.len() - 20..]).unwrap();
            } else {
                socket.write_all(&reply).unwrap();
            }
        }
        socket
            .write_all(&quote_reply(zmq::TOPICS[0], FRAMES[1]))
            .unwrap();
        drop(socket);

        let (mut socket, _) = listener.accept().unwrap();
        acknowledge(&mut socket, true);
        socket
            .write_all(&quote_reply(zmq::TOPICS[1], &FRAMES[1][14..]))
            .unwrap();
    });
    let mut source = RedisSource::new(endpoint);
    let initial = wait_for_message(&mut source);
    assert_ne!(initial.flags & SOURCE_RESET, 0);
    for (venue, frame) in VENUES.into_iter().zip(FRAMES) {
        let actual = wait_for_message(&mut source);
        let expected = zmq::parse(frame, 0).unwrap();
        assert_eq!(actual.flags & SOURCE_RESET, 0);
        assert_eq!(actual.source_generation, initial.source_generation);
        assert_eq!(actual.venue(), venue.as_bytes());
        assert_eq!(actual.symbol(), expected.symbol());
        assert_eq!(actual.exchange_ts_us, expected.exchange_ts_us);
        assert_eq!(actual.source_sequence, expected.source_sequence);
        assert_eq!(actual.volume, expected.volume);
    }
    let mut generation = initial.source_generation;
    for _ in 0..3 {
        let reset = wait_for_message(&mut source);
        assert_ne!(reset.flags & SOURCE_RESET, 0);
        assert!(reset.source_generation > generation);
        generation = reset.source_generation;
    }
    let quote = wait_for_message(&mut source);
    assert_eq!(quote.flags & SOURCE_RESET, 0);
    assert_eq!(quote.venue(), b"SHFE");
    assert_eq!(quote.source_generation, generation);
    server.join().unwrap();
}

#[test]
fn unanswered_ping_resets_subscription_and_recovers_without_exiting() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let SocketAddr::V4(endpoint) = listener.local_addr().unwrap() else {
        unreachable!();
    };
    let server = std::thread::spawn(move || {
        let (mut silent, _) = listener.accept().unwrap();
        acknowledge(&mut silent, false);
        // Keep the old TCP connection open. Only its missing PONG should
        // make the client close it and subscribe on a new socket.
        let (mut socket, _) = listener.accept().unwrap();
        acknowledge(&mut socket, true);
        socket
            .write_all(&quote_reply(zmq::TOPICS[1], &FRAMES[1][14..]))
            .unwrap();
        drop(silent);
    });
    let mut source = RedisSource::new(endpoint);
    let subscribed = wait_for_message(&mut source);
    let disconnected = wait_for_message(&mut source);
    let reconnected = wait_for_message(&mut source);
    for mark in [subscribed, disconnected, reconnected] {
        assert_ne!(mark.flags & SOURCE_RESET, 0);
    }
    assert!(subscribed.source_generation < disconnected.source_generation);
    assert!(disconnected.source_generation < reconnected.source_generation);
    let quote = wait_for_message(&mut source);
    assert_eq!(quote.venue(), b"SHFE");
    assert_eq!(quote.source_generation, reconnected.source_generation);
    server.join().unwrap();
}
