use std::env;

/// 构造带有命名空间前缀的 IceOryx service name
///
/// # 规则
/// - `dat_pbs/*` 保持不变（公用市场数据）
/// - `bridge/*` 保持不变（公用桥接市场数据）
/// - `factor_pub/*` 保持不变（公用因子流）
/// - 其他路径（如 `signal_pubs/*`, `viz_pubs/*`, `persist_pubs/*`, `account_pubs/*`）添加命名空间前缀
///
/// # 环境变量
/// - `IPC_NAMESPACE`: 必须存在，用于命名空间隔离（例如: "mkt1", "mkt2"）
///
/// # 示例
/// ```ignore
/// // 环境变量: IPC_NAMESPACE=mkt1
///
/// // 市场数据（公用）：
/// build_service_name("dat_pbs/binance-futures/trade")
/// // => "dat_pbs/binance-futures/trade"
///
/// // bridge 行情（公用）：
/// build_service_name("bridge/binance-futures/derivatives")
/// // => "bridge/binance-futures/derivatives"
///
/// // 因子流（公用）：
/// build_service_name("factor_pub/binance-futures/rl_vol")
/// // => "factor_pub/binance-futures/rl_vol"
///
/// // 信号通道（隔离）：
/// build_service_name("signal_pubs/pre_trade")
/// // => "mkt1/signal_pubs/pre_trade"
///
/// // 可视化通道（隔离）：
/// build_service_name("viz_pubs/pre_trade_exposure")
/// // => "mkt1/viz_pubs/pre_trade_exposure"
///
/// // 持久化通道（隔离）：
/// build_service_name("persist_pubs/order_update_record")
/// // => "mkt1/persist_pubs/order_update_record"
///
/// // 账户数据（隔离）：
/// build_service_name("account_pubs/binance_pm")
/// // => "mkt1/account_pubs/binance_pm"
/// ```
///
/// # Panics
/// - 如果环境变量 `IPC_NAMESPACE` 未设置，将 panic
pub fn build_service_name(base_name: &str) -> String {
    // dat_pbs / bridge / factor_pub 保持不变（公用）
    if base_name.starts_with("dat_pbs/")
        || base_name.starts_with("bridge/")
        || base_name.starts_with("factor_pub/")
    {
        return base_name.to_string();
    }

    // 读取必须存在的命名空间环境变量
    let namespace = env::var("IPC_NAMESPACE")
        .expect("环境变量 IPC_NAMESPACE 未设置！请设置该变量以隔离 IceOryx 服务命名空间");

    format!("{}/{}", namespace, base_name)
}
