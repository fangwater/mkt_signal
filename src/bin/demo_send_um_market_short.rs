use anyhow::Result;
use bytes::Bytes;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use mkt_signal::trade_engine::trade_request::{BinanceNewUMOrderRequest};
use std::time::{SystemTime, UNIX_EPOCH};

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

fn main() -> Result<()> {
    env_logger::init();

    // 目标服务名（与引擎一致）
    let service = std::env::var("ORDER_REQ_SERVICE").unwrap_or_else(|_| "order_reqs/binance".to_string());
    // 交易参数（SOLUSDT 做空 0.01 市价单，双向持仓）
    let symbol = std::env::var("SYMBOL").unwrap_or_else(|_| "SOLUSDT".to_string());
    let qty = std::env::var("QTY").unwrap_or_else(|_| "0.01".to_string());
    let position_side = std::env::var("POSITION_SIDE").unwrap_or_else(|_| "SHORT".to_string());

    // 以 query-string 形式拼接（引擎侧会解析为键值对并补充签名参数）
    let create_time = now_ms();
    let client_order_id = create_time; // 简单用时间戳做ID
    let qs = format!(
        "symbol={}&side=SELL&type=MARKET&quantity={}&positionSide={}&newClientOrderId={}",
        symbol, qty, position_side, client_order_id
    );
    let params = Bytes::from(qs);
    let req = BinanceNewUMOrderRequest::create(create_time, client_order_id, params);
    let bytes = req.to_bytes();

    // Iceoryx 发布
    let node = NodeBuilder::new().name(&NodeName::new("demo_sender")?).create::<ipc::Service>()?;
    let srv = node
        .service_builder(&ServiceName::new(&service)?)
        .publish_subscribe::<[u8; 4096]>()
        .open_or_create()?;
    let publisher = srv.publisher_builder().create()?;

    let mut buf = [0u8; 4096];
    let n = bytes.len().min(4096);
    buf[..n].copy_from_slice(&bytes[..n]);
    let sample = publisher.loan_uninit()?;
    let sample = sample.write_payload(buf);
    sample.send()?;

    println!("sent UM MARKET SHORT order: {} {}", symbol, qty);
    Ok(())
}
