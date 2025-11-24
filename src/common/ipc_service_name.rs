use std::env;

/// 构造带有当前工作目录前缀的 IceOryx service name
///
/// # 规则
/// - `data_pubs/*` 保持不变（公用市场数据）
/// - 其他路径（如 `signal_pubs/*`, `account_pubs/*`）添加目录前缀
///
/// # 示例
/// ```
/// // 当前目录: /home/ubuntu/crypto_mkt/mkt_signal
///
/// // 市场数据（公用）：
/// build_service_name("data_pubs/binance-futures/trade")
/// // => "data_pubs/binance-futures/trade"
///
/// // 信号通道（隔离）：
/// build_service_name("signal_pubs/pre_trade")
/// // => "/home/ubuntu/crypto_mkt/mkt_signal/signal_pubs/pre_trade"
///
/// // 账户数据（隔离）：
/// build_service_name("account_pubs/binance_pm")
/// // => "/home/ubuntu/crypto_mkt/mkt_signal/account_pubs/binance_pm"
/// ```
pub fn build_service_name(base_name: &str) -> String {
    // data_pubs 保持不变（公用）
    if base_name.starts_with("data_pubs/") {
        return base_name.to_string();
    }

    // 其他路径添加当前工作目录前缀
    let current_dir = env::current_dir()
        .ok()
        .and_then(|p| p.to_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "/tmp/mkt_signal".to_string());

    // 清理路径中的特殊字符（IceOryx service name 有字符限制）
    let sanitized_dir = sanitize_path(&current_dir);

    format!("{}/{}", sanitized_dir, base_name)
}

/// 清理路径字符串，使其适合作为 IceOryx service name 的一部分
///
/// IceOryx service name 限制：
/// - 最大长度通常为 255 字符
/// - 允许字符: a-z, A-Z, 0-9, _, -, /
fn sanitize_path(path: &str) -> String {
    // 移除开头的斜杠（避免双斜杠）
    let path = path.trim_start_matches('/');

    // 替换不允许的字符为下划线
    path.chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-' | '/' => c,
            _ => '_',
        })
        .collect()
}
