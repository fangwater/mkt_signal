use anyhow::Result;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;

fn read_le_u32(b: &[u8], o: usize) -> u32 { u32::from_le_bytes(b[o..o+4].try_into().unwrap()) }
fn read_le_i64(b: &[u8], o: usize) -> i64 { i64::from_le_bytes(b[o..o+8].try_into().unwrap()) }
fn read_le_u16(b: &[u8], o: usize) -> u16 { u16::from_le_bytes(b[o..o+2].try_into().unwrap()) }

fn main() -> Result<()> {
    env_logger::init();
    let service = std::env::var("ORDER_RESP_SERVICE").unwrap_or_else(|_| "order_resps/binance".to_string());

    let node = NodeBuilder::new().name(&NodeName::new("demo_resp_sub")?).create::<ipc::Service>()?;
    let service = node
        .service_builder(&ServiceName::new(&service)?)
        .publish_subscribe::<[u8; 8192]>()
        .open_or_create()?;
    let subscriber = service.subscriber_builder().create()?;

    println!("listening responses on {} ...", service.name());

    loop {
        if let Some(sample) = subscriber.receive()? {
            let p = sample.payload();
            // 解析统一头（32字节）
            if p.len() < 32 { continue; }
            let msg_type = read_le_u32(p, 0);
            let local_recv_time = read_le_i64(p, 4);
            let client_order_id = read_le_i64(p, 12);
            let exchange = read_le_u32(p, 20);
            let status = read_le_u16(p, 24);
            let body_format = read_le_u16(p, 26);
            let body_len = read_le_u32(p, 28) as usize;
            let body = &p[32..p.len()].to_vec();
            let body = &body[..body_len.min(body.len())];

            println!(
                "resp: msg_type={}, status={}, fmt={}, order_id={}, exch={}, ts={}",
                msg_type, status, body_format, client_order_id, exchange, local_recv_time
            );

            match body_format {
                0 => {
                    // 原始HTTP body，尝试按UTF-8打印
                    match std::str::from_utf8(body) {
                        Ok(s) => println!("raw body: {}", s),
                        Err(_) => println!("raw body ({} bytes)", body.len()),
                    }
                }
                1 => {
                    println!("typed body ({} bytes)", body.len());
                }
                2 => {
                    // 错误typed体：直接按UTF-8尝试打印错误消息尾部
                    match std::str::from_utf8(body) {
                        Ok(s) => println!("error typed: {}", s),
                        Err(_) => println!("error typed ({} bytes)", body.len()),
                    }
                }
                _ => println!("unknown body_format {}", body_format),
            }
        }
    }
}
