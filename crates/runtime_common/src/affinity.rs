use anyhow::{ensure, Result};

/// 把当前线程绑定到指定 CPU 核（sched_setaffinity）。
///
/// 适用于 `#[tokio::main(flavor = "current_thread")]` 这类单线程 runtime：
/// main 线程 + 所有 `spawn_local` 任务都跑在同一线程上，调一次即可覆盖整个进程的
/// 工作线程。启动脚本常配合 `taskset -c <core>` 做双重保险。
pub fn pin_to_core(core: usize) -> Result<()> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core, &mut set);

        let rc = libc::sched_setaffinity(
            0,
            std::mem::size_of::<libc::cpu_set_t>(),
            &set as *const libc::cpu_set_t,
        );
        ensure!(
            rc == 0,
            "sched_setaffinity(core={}) failed: errno={}",
            core,
            *libc::__errno_location()
        );
    }
    log::info!("pinned current thread to cpu core {}", core);
    Ok(())
}

/// 解析 CLI > ENV > None 的核号。不绑核，只解析。
///
/// - `cli_core`：来自 `--core <N>` 的 `Option<usize>`
/// - `env_var`：fallback 环境变量名（例：`"TRADE_SIGNAL_CORE"`），需能 parse 成 `usize`
///
/// 用于需要把核号传给其他线程（如 trade_engine 的 IPC thread）的场景。
pub fn resolve_core(cli_core: Option<usize>, env_var: &str) -> Option<usize> {
    cli_core.or_else(|| {
        std::env::var(env_var)
            .ok()
            .and_then(|raw| raw.trim().parse::<usize>().ok())
    })
}

/// 可选绑当前线程：CLI > ENV > 跳过。
///
/// 都未提供则记录一条 info 日志后返回 Ok，不绑核（兼容旧部署）。
pub fn maybe_pin_current_thread(cli_core: Option<usize>, env_var: &str) -> Result<()> {
    match resolve_core(cli_core, env_var) {
        Some(c) => pin_to_core(c),
        None => {
            log::info!(
                "cpu pinning skipped (no --core and ${} unset or invalid)",
                env_var
            );
            Ok(())
        }
    }
}
