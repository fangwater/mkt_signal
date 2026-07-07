//! 单独起一条 Binance WS 连接，做 session.logon 后按固定间隔发 protocol ping，
//! 观察 Binance 是否因 ping 过频主动断开。仅 logon + ping，不下任何单。
//!
//! 用法（env）：
//!   BINANCE_ED25519_API_KEY / BINANCE_ED25519_PRIVATE_KEY_PATH /
//!   BINANCE_ED25519_PRIVATE_KEY_PASSPHRASE  —— 与 trade_engine 相同
//!   PING_MS    ping 间隔毫秒（默认 100）
//!   RUN_SECS   最长运行秒数（默认 90）
//!   WS_URL     覆盖端点（默认 wss://ws-fapi.binance.com/ws-fapi/v1；
//!              现货用 wss://ws-api.binance.com:443/ws-api/v3）
//!
//! 实测结论（UM ws-fapi，2026-07-06）：Binance 有 ping/pong 洪泛保护，行为像
//! 令牌桶（容量约 5–6 帧、补充速率约 4/s）：
//!   - 250ms(4/s)  → 存活 90s
//!   - 200ms(5/s)  → 第 6 个 ping、~1s 即 Policy close 'too many ping/pong frames'
//!   - 150/100ms   → 同样第 6 个 ping 秒断
//! 即安全上限 ≈ 4/s；留余量建议 3/s(333ms)。故高频 WS ping 不可作丢包探针，
//! 低频(≤4/s)只够识别明显坏路径。

use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::time::{Duration, Instant};
use tokio::time;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::Message;
use trade_engine::binance_ws::{build_session_logon_payload, BinanceWsSigner};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let ping_ms = env_u64("PING_MS", 100);
    let run_secs = env_u64("RUN_SECS", 90);
    let url = std::env::var("WS_URL")
        .unwrap_or_else(|_| "wss://ws-fapi.binance.com/ws-fapi/v1".to_string());

    let api_key =
        std::env::var("BINANCE_ED25519_API_KEY").context("BINANCE_ED25519_API_KEY not set")?;
    let key_path = std::env::var("BINANCE_ED25519_PRIVATE_KEY_PATH")
        .context("BINANCE_ED25519_PRIVATE_KEY_PATH not set")?;
    let signer = BinanceWsSigner::from_ed25519_pem_path(&key_path)?;
    if !signer.uses_session_logon() {
        return Err(anyhow!("signer is not Ed25519; session.logon unavailable"));
    }

    println!(
        "[{}] connecting url={} ping_ms={} run_secs={} algo={}",
        now_ms(),
        url,
        ping_ms,
        run_secs,
        signer.algorithm()
    );

    let (mut ws, resp) = tokio_tungstenite::connect_async(&url)
        .await
        .with_context(|| format!("connect {}", url))?;
    println!("[{}] connected http_status={}", now_ms(), resp.status());

    // session.logon
    let logon = build_session_logon_payload(1, &api_key, &signer)?;
    ws.send(Message::Text(logon)).await?;
    println!("[{}] sent session.logon", now_ms());

    let start = Instant::now();
    let deadline = start + Duration::from_secs(run_secs);
    let mut ping_interval = time::interval(Duration::from_millis(ping_ms.max(1)));
    ping_interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    let mut ping_count: u64 = 0;
    let mut pong_count: u64 = 0;
    let mut logon_ok = false;
    let mut last_report = Instant::now();

    let outcome: String = loop {
        tokio::select! {
            biased;
            _ = time::sleep_until(deadline.into()) => {
                break format!(
                    "SURVIVED run_secs={} no disconnect (pings={} pongs={})",
                    run_secs, ping_count, pong_count
                );
            }
            _ = ping_interval.tick() => {
                if let Err(e) = ws.send(Message::Ping(Vec::new())).await {
                    break format!("send ping failed after {} pings: {}", ping_count, e);
                }
                ping_count += 1;
            }
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Pong(_))) => { pong_count += 1; }
                    Some(Ok(Message::Ping(p))) => { let _ = ws.send(Message::Pong(p)).await; }
                    Some(Ok(Message::Text(t))) => {
                        if !logon_ok {
                            logon_ok = true;
                            let head: String = t.chars().take(200).collect();
                            println!("[{}] first text (logon ack?): {}", now_ms(), head);
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let (code, reason) = frame
                            .map(|f| (f.code, f.reason.to_string()))
                            .unwrap_or((CloseCode::Normal, String::new()));
                        break format!(
                            "CLOSED by remote code={:?} reason='{}'",
                            code, reason
                        );
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => break format!("ws error: {}", e),
                    None => break "stream ended (no close frame)".to_string(),
                }
            }
        }

        if last_report.elapsed() >= Duration::from_secs(5) {
            last_report = Instant::now();
            println!(
                "[{}] alive t={:.1}s pings={} pongs={} logon_ok={}",
                now_ms(),
                start.elapsed().as_secs_f64(),
                ping_count,
                pong_count,
                logon_ok
            );
        }
    };

    println!(
        "[{}] RESULT after {:.1}s, pings={}, pongs={}: {}",
        now_ms(),
        start.elapsed().as_secs_f64(),
        ping_count,
        pong_count,
        outcome
    );
    Ok(())
}
