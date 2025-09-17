use anyhow::Result;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;

fn read_le_u32(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn read_le_i64(b: &[u8], o: usize) -> i64 {
    i64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}
fn read_le_u16(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes(b[o..o + 2].try_into().unwrap())
}

fn main() -> Result<()> {
    env_logger::init();
    let service =
        std::env::var("ORDER_RESP_SERVICE").unwrap_or_else(|_| "order_resps/binance".to_string());

    let node = NodeBuilder::new()
        .name(&NodeName::new("demo_resp_sub")?)
        .create::<ipc::Service>()?;
    let service = node
        .service_builder(&ServiceName::new(&service)?)
        .publish_subscribe::<[u8; 16384]>()
        .open_or_create()?;
    let subscriber = service.subscriber_builder().create()?;

    println!("listening responses on {} ...", service.name());

    loop {
        if let Some(sample) = subscriber.receive()? {
            let p = sample.payload();
            // 头部固定 40 字节
            if p.len() < 40 {
                continue;
            }
            let req_type = read_le_u32(p, 0);
            let local_recv_time = read_le_i64(p, 4);
            let client_order_id = read_le_i64(p, 12);
            let exchange = read_le_u32(p, 20);
            let status = read_le_u16(p, 24);
            let _reserved = read_le_u16(p, 26);
            let ip_used_weight_1m = read_le_u32(p, 28);
            let order_count_1m = read_le_u32(p, 32);
            let body_len = read_le_u32(p, 36) as usize;

            let actual_len = p
                .iter()
                .rposition(|&x| x != 0)
                .map(|pos| pos + 1)
                .unwrap_or(40);
            let avail = actual_len.saturating_sub(40);
            let take = std::cmp::min(avail, body_len);
            let body = &p[40..40 + take];

            println!(
                "resp: type={}, status={}, exch={}, order_id={}, ip_used_1m={}, order_cnt_1m={}, ts={}",
                req_type, status, exchange, client_order_id,
                if ip_used_weight_1m == u32::MAX { 0 } else { ip_used_weight_1m },
                if order_count_1m == u32::MAX { 0 } else { order_count_1m },
                local_recv_time,
            );
            match std::str::from_utf8(body) {
                Ok(s) => println!("json ({} bytes): {}", take, s),
                Err(_) => println!("json bytes ({} bytes)", take),
            }
        }
    }
}
