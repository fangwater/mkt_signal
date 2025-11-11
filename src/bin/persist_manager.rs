use anyhow::Result;
use mkt_signal::persist_manager::PersistManager;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    // 解析命令行参数：--port <端口号>
    let port = parse_port_arg().unwrap_or(8088);

    let manager = PersistManager::new(port);
    let local = tokio::task::LocalSet::new();
    local.run_until(manager.run()).await
}

fn parse_port_arg() -> Option<u16> {
    let args: Vec<String> = std::env::args().collect();
    for i in 0..args.len() {
        if args[i] == "--port" && i + 1 < args.len() {
            return args[i + 1].parse().ok();
        }
    }
    None
}
