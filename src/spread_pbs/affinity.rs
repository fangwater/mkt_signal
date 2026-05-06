use anyhow::{ensure, Result};

/// 把当前线程绑定到指定 CPU 核（sched_setaffinity）。
///
/// `current_thread` runtime 下 main + 所有 spawn_local 任务都跑在同一线程上，
/// 因此调用一次即可覆盖整个 spread_pbs 进程的工作线程。启动脚本同时用
/// `taskset -c <core>` 双重保险。
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
    log::info!("spread_pbs pinned to core {}", core);
    Ok(())
}
