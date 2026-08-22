use anyhow::{bail, Context, Result};
use clap::Parser;
use mkt_parsers::msg::mkt_msg::ModelMsg;

const DEFAULT_ENDPOINT: &str = "ipc:///tmp/predict_rnn_layer.ipc";
const DEFAULT_TOPIC_PREFIX: &str = "model_output/";

#[derive(Debug, Parser)]
#[command(name = "predict_rnn_zmq_ipc_demo")]
#[command(about = "Receive PredRNN ModelMsg payloads from a ZeroMQ IPC PUB socket")]
struct Args {
    /// ZeroMQ IPC endpoint published by predict_rnn_layer.
    #[arg(long, default_value = DEFAULT_ENDPOINT)]
    endpoint: String,

    /// ZMQ subscription prefix. An empty string subscribes to every topic.
    #[arg(long, default_value = DEFAULT_TOPIC_PREFIX)]
    topic: String,

    /// Exit after this many decoded messages. Zero runs until interrupted.
    #[arg(long, default_value_t = 1)]
    count: u64,

    /// Per-message receive timeout in milliseconds. Zero waits forever.
    #[arg(long, default_value_t = 10_000)]
    recv_timeout_ms: i32,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.endpoint.trim().is_empty() {
        bail!("endpoint must not be empty");
    }
    if args.recv_timeout_ms < 0 {
        bail!("recv_timeout_ms must be non-negative");
    }

    let context = zmq::Context::new();
    let subscriber = context
        .socket(zmq::SUB)
        .context("create ZeroMQ SUB socket failed")?;
    subscriber
        .set_linger(0)
        .context("set ZeroMQ linger failed")?;
    if args.recv_timeout_ms > 0 {
        subscriber
            .set_rcvtimeo(args.recv_timeout_ms)
            .context("set ZeroMQ receive timeout failed")?;
    }
    subscriber
        .set_subscribe(args.topic.as_bytes())
        .with_context(|| format!("subscribe to topic prefix {:?} failed", args.topic))?;
    subscriber.connect(args.endpoint.trim()).with_context(|| {
        format!(
            "connect to ZeroMQ IPC endpoint {} failed",
            args.endpoint.trim()
        )
    })?;

    println!(
        "predict_rnn_zmq_ipc_demo connected endpoint={} topic_prefix={:?} count={}",
        args.endpoint.trim(),
        args.topic,
        args.count
    );

    let mut received = 0u64;
    while args.count == 0 || received < args.count {
        let frames = match subscriber.recv_multipart(0) {
            Ok(frames) => frames,
            Err(zmq::Error::EAGAIN) => {
                bail!(
                    "timed out waiting {} ms for ModelMsg from {}",
                    args.recv_timeout_ms,
                    args.endpoint.trim()
                )
            }
            Err(err) => return Err(err).context("receive ZeroMQ multipart message failed"),
        };
        if frames.len() != 2 {
            bail!("expected 2 ZMQ frames, received {}", frames.len());
        }

        let topic = std::str::from_utf8(&frames[0]).context("ZMQ topic is not valid UTF-8")?;
        let msg = ModelMsg::from_bytes(&frames[1]).context("decode Rust ModelMsg failed")?;
        let expected_topic = format!("{}{}", DEFAULT_TOPIC_PREFIX, msg.symbol);
        if topic != expected_topic {
            bail!(
                "topic/symbol mismatch: topic={topic:?} expected={expected_topic:?} symbol={}",
                msg.symbol
            );
        }

        received = received.saturating_add(1);
        println!(
            "received={} topic={} symbol={} seq_no={} ts_in_ms={} ts_out_ms={} score={:.9} qtl={} ready={} status={} feature_dim={} payload_bytes={}",
            received,
            topic,
            msg.symbol,
            msg.seq_no,
            msg.ts_in_ms,
            msg.ts_out_ms,
            msg.score,
            msg.score_quantile
                .map(|value| format!("{value:.9}"))
                .unwrap_or_else(|| "none".to_string()),
            msg.score_ready,
            msg.status,
            msg.feature_dim,
            frames[1].len(),
        );
    }

    Ok(())
}
