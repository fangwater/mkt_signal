use anyhow::Result;
use bytes::Bytes;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use mkt_signal::trade_engine::trade_request::BinanceNewUMOrderRequest;
use std::io::{self};
use std::time::{SystemTime, UNIX_EPOCH};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn main() -> Result<()> {
    env_logger::init();

    // 固定服务名与参数（无需环境变量）
    let service = "order_reqs/binance".to_string();
    let symbol = "SOLUSDT".to_string();
    let qty = "0.03".to_string();
    let position_side = "SHORT".to_string();

    // Iceoryx 发布
    let node = NodeBuilder::new()
        .name(&NodeName::new("demo_sender")?)
        .create::<ipc::Service>()?;
    let srv = node
        .service_builder(&ServiceName::new(&service)?)
        .publish_subscribe::<[u8; 4096]>()
        .open_or_create()?;
    let publisher = srv.publisher_builder().create()?;

    println!("interactive UM MARKET SHORT sender ready.");
    println!(
        "service={}, symbol={}, qty={}, position_side={}",
        service, symbol, qty, position_side
    );
    println!("Press Enter to SEND; type 'q' then Enter to quit.");

    let stdin = io::stdin();
    loop {
        let mut line = String::new();
        let _ = stdin.read_line(&mut line)?;
        let cmd = line.trim();
        if cmd.eq_ignore_ascii_case("q") {
            break;
        }

        let create_time = now_ms();
        let client_order_id = create_time; // 简单用时间戳做ID
        let qs = format!(
            "symbol={}&side=SELL&type=MARKET&quantity={}&positionSide={}&newClientOrderId={}",
            symbol, qty, position_side, client_order_id
        );
        let params = Bytes::from(qs);
        let req = BinanceNewUMOrderRequest::create(create_time, client_order_id, params);
        let bytes = req.to_bytes();

        let mut buf = [0u8; 4096];
        let n = bytes.len().min(4096);
        buf[..n].copy_from_slice(&bytes[..n]);
        let sample = publisher.loan_uninit()?;
        let sample = sample.write_payload(buf);
        sample.send()?;
        println!(
            "sent UM MARKET SHORT order: {} {} (client_order_id={})",
            symbol, qty, client_order_id
        );
    }

    println!("exit sender");
    Ok(())
}
