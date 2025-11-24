use std::env;

/// 构造带有命名空间前缀的 IceOryx service name
///
/// # 规则
/// - `data_pubs/*` 保持不变（公用市场数据）
/// - 其他路径（如 `signal_pubs/*`, `account_pubs/*`）添加命名空间前缀
///
/// # 环境变量
/// - `ipc_namespace`: 必须存在，用于命名空间隔离（例如: "mkt1", "mkt2"）
///
/// # 示例
/// ```
/// // 环境变量: ipc_namespace=mkt1
///
/// // 市场数据（公用）：
/// build_service_name("data_pubs/binance-futures/trade")
/// // => "data_pubs/binance-futures/trade"
///
/// // 信号通道（隔离）：
/// build_service_name("signal_pubs/pre_trade")
/// // => "mkt1/signal_pubs/pre_trade"
///
/// // 账户数据（隔离）：
/// build_service_name("account_pubs/binance_pm")
/// // => "mkt1/account_pubs/binance_pm"
/// ```
///
/// # Panics
/// - 如果环境变量 `ipc_namespace` 未设置，将 panic
pub fn build_service_name(base_name: &str) -> String {
    // data_pubs 保持不变（公用）
    if base_name.starts_with("data_pubs/") {
        return base_name.to_string();
    }

    // 读取必须存在的命名空间环境变量
    let namespace = env::var("ipc_namespace")
        .expect("环境变量 ipc_namespace 未设置！请设置该变量以隔离 IceOryx 服务命名空间");

    format!("{}/{}", namespace, base_name)
}
