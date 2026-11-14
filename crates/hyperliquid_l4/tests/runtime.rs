use futures_util::{SinkExt, StreamExt};
use hyperliquid_l4::{
    book::OrderBook,
    runtime::{run, Options, Sink},
    wire::*,
};
use std::{cell::RefCell, rc::Rc, time::Duration};
use tokio::sync::watch;
use tokio_tungstenite::{accept_async, tungstenite::Message};

fn options(url: String) -> Options {
    Options {
        ws_url: url,
        fallback_ws_url: None,
        coins: vec!["BTC".into()],
        reserve_events: 16,
        max_events: 100,
        max_frame_bytes: 1024 * 1024,
        chunk_events: 2,
        resnapshot_secs: 300,
        max_source_lag_ms: 0,
    }
}
struct Capture {
    raw: Rc<RefCell<Vec<Vec<u8>>>>,
    shutdown: watch::Sender<bool>,
    stop_at_height: u64,
    fail_chunk: bool,
    reset_count: usize,
}
impl Sink for Capture {
    fn publish(&mut self, h: &Header, events: &[Event]) -> anyhow::Result<()> {
        if self.fail_chunk && h.kind == Kind::Snapshot && h.chunk_index == 1 {
            self.fail_chunk = false;
            anyhow::bail!("injected transport failure");
        }
        let mut raw = vec![0; HEADER_BYTES + events.len() * EVENT_BYTES];
        h.encode(events, &mut raw)?;
        self.raw.borrow_mut().push(raw);
        if h.kind == Kind::Reset {
            self.reset_count += 1;
            if self.stop_at_height == 0 && self.reset_count >= 2 {
                self.shutdown.send(true)?;
            }
        }
        if h.kind == Kind::Update && h.height == self.stop_at_height {
            self.shutdown.send(true)?;
        }
        Ok(())
    }
}
async fn server(bad: bool) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        let request = ws.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(request.contains("\"type\":\"l4Book\""));
        ws.send(Message::Text(r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l4Book","coin":"BTC"}}}"#.into())).await.unwrap();
        ws.send(Message::Text(
            std::str::from_utf8(include_bytes!("fixtures/snapshot.json"))
                .unwrap()
                .into(),
        ))
        .await
        .unwrap();
        // Exact duplicate must not start another consumer snapshot transaction.
        ws.send(Message::Text(
            std::str::from_utf8(include_bytes!("fixtures/snapshot.json"))
                .unwrap()
                .into(),
        ))
        .await
        .unwrap();
        let update = std::str::from_utf8(include_bytes!("fixtures/updates.json")).unwrap();
        let update = if bad {
            update.replace("\"newSz\":\"1.25\"", "\"newSz\":\"invalid\"")
        } else {
            update.to_string()
        };
        let _ = ws.send(Message::Text(update.into())).await;
        while let Some(Ok(frame)) = ws.next().await {
            if matches!(frame, Message::Close(_)) {
                break;
            }
        }
    });
    (format!("ws://{address}/ws"), handle)
}
#[tokio::test(flavor = "current_thread")]
async fn actual_websocket_to_binary_chunks_and_consumer_book() {
    let (url, server) = server(false).await;
    let (tx, rx) = watch::channel(false);
    let raw = Rc::new(RefCell::new(Vec::new()));
    let sink = Capture {
        raw: raw.clone(),
        shutdown: tx,
        stop_at_height: 105,
        fail_chunk: false,
        reset_count: 0,
    };
    tokio::time::timeout(Duration::from_secs(3), run(options(url), sink, rx))
        .await
        .unwrap()
        .unwrap();
    server.abort();
    let raw = raw.borrow();
    let mut book = OrderBook::new(Coin::new("BTC").unwrap(), 100, 100).unwrap();
    let mut snapshots = 0;
    let mut updates = 0;
    for bytes in raw.iter() {
        let (h, _) = Header::decode(bytes).unwrap();
        if h.kind == Kind::Reset && h.sequence == 0 && snapshots > 0 {
            assert_eq!(book.height(), Some(105));
            assert_eq!(book.order(1001).unwrap().size, 125_000_000);
        }
        if h.chunk_index == 0 {
            if h.kind == Kind::Snapshot {
                snapshots += 1;
            }
            if h.kind == Kind::Update {
                updates += 1;
                assert_eq!(h.previous_height, 100);
                assert_eq!(h.sequence, 2);
            }
        }
        book.ingest(bytes).unwrap();
    }
    assert_eq!((snapshots, updates), (1, 1));
    assert!(!book.is_ready());
}
#[tokio::test(flavor = "current_thread")]
async fn malformed_upstream_invalidates_a_previously_ready_stream() {
    let (url, server) = server(true).await;
    let (tx, rx) = watch::channel(false);
    let raw = Rc::new(RefCell::new(Vec::new()));
    let sink = Capture {
        raw: raw.clone(),
        shutdown: tx,
        stop_at_height: 0,
        fail_chunk: false,
        reset_count: 0,
    };
    tokio::time::timeout(Duration::from_secs(3), run(options(url), sink, rx))
        .await
        .unwrap()
        .unwrap();
    server.abort();
    let raw = raw.borrow();
    assert!(raw
        .iter()
        .all(|b| Header::decode(b).unwrap().0.kind != Kind::Update));
    assert_eq!(
        Header::decode(raw.last().unwrap()).unwrap().0.kind,
        Kind::Reset
    );
}
#[tokio::test(flavor = "current_thread")]
async fn interrupted_publication_never_commits_partial_snapshot() {
    let (url, server) = server(false).await;
    let (tx, rx) = watch::channel(false);
    let raw = Rc::new(RefCell::new(Vec::new()));
    let sink = Capture {
        raw: raw.clone(),
        shutdown: tx,
        stop_at_height: 0,
        fail_chunk: true,
        reset_count: 0,
    };
    tokio::time::timeout(Duration::from_secs(3), run(options(url), sink, rx))
        .await
        .unwrap()
        .unwrap();
    server.abort();
    let mut book = OrderBook::new(Coin::new("BTC").unwrap(), 100, 100).unwrap();
    for bytes in raw.borrow().iter() {
        assert!(!book.ingest(bytes).unwrap());
    }
    assert!(!book.is_ready());
}
#[test]
fn require_node_source_and_bounded_explicit_perpetuals() {
    let mut cfg = options("wss://api.hyperliquid.xyz/ws".into());
    assert!(cfg.validate().is_err());
    cfg.ws_url = "ws://127.0.0.1:8000/ws".into();
    assert!(cfg.validate().is_ok());
    cfg.coins.push("BTC".into());
    assert!(cfg.validate().is_err());
    cfg.coins = vec!["@1".into()];
    assert!(cfg.validate().is_err());
    cfg.coins = vec!["xyz:TSLA".into()];
    assert!(cfg.validate().is_ok());
    cfg.chunk_events = usize::MAX;
    assert!(cfg.validate().is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn stale_source_snapshot_never_becomes_ready() {
    let (url, server) = server(false).await;
    let (tx, rx) = watch::channel(false);
    let raw = Rc::new(RefCell::new(Vec::new()));
    let sink = Capture {
        raw: raw.clone(),
        shutdown: tx,
        stop_at_height: 0,
        fail_chunk: false,
        reset_count: 0,
    };
    let mut cfg = options(url);
    cfg.max_source_lag_ms = 10_000;
    tokio::time::timeout(Duration::from_secs(3), run(cfg, sink, rx))
        .await
        .unwrap()
        .unwrap();
    server.abort();
    assert!(raw
        .borrow()
        .iter()
        .all(|bytes| Header::decode(bytes).unwrap().0.kind == Kind::Reset));
}
