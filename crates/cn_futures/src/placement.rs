//! Strict Linux placement for native trading runtimes. No policy is applied by config checks.
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlacementConfig {
    pub numa_node: usize,
    pub business_cpu: usize,
    pub sdk_cpus: Vec<usize>,
    pub background_cpus: Vec<usize>,
}
impl PlacementConfig {
    pub fn validate_fixed(&self) -> Result<()> {
        self.validate()?;
        ensure!(
            self.sdk_cpus.len() == 1 && self.background_cpus.len() == 1,
            "fixed CTP layout requires one SDK CPU and one background CPU per domain"
        );
        Ok(())
    }
    pub fn validate_combined(&self, md: &Self) -> Result<()> {
        self.validate_fixed()?;
        md.validate_fixed()?;
        ensure!(
            self.numa_node == md.numa_node
                && self.business_cpu == md.business_cpu
                && self.background_cpus == md.background_cpus,
            "combined CTP requires the same NUMA node, business CPU and background CPU"
        );
        ensure!(
            self.sdk_cpus != md.sdk_cpus,
            "Md and Trader SDKs require distinct physical cores"
        );
        Ok(())
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.numa_node < 4096,
            "NUMA node exceeds supported node mask"
        );
        ensure!(
            !self.sdk_cpus.is_empty() && !self.background_cpus.is_empty(),
            "explicit SDK and background CPU sets required"
        );
        let mut seen = BTreeSet::from([self.business_cpu]);
        for cpu in self.sdk_cpus.iter().chain(&self.background_cpus) {
            ensure!(
                seen.insert(*cpu),
                "business, SDK and background CPU sets must be disjoint"
            );
        }
        ensure!(
            seen.iter().all(|cpu| *cpu < 1024),
            "CPU exceeds Linux cpu_set_t capacity"
        );
        Ok(())
    }
}

fn parse_list(value: &str) -> Result<BTreeSet<usize>> {
    let mut output = BTreeSet::new();
    for part in value.trim().split(',').filter(|s| !s.is_empty()) {
        if let Some((start, end)) = part.split_once('-') {
            let start: usize = start.parse()?;
            let end: usize = end.parse()?;
            ensure!(start <= end && end < 65536, "invalid topology range");
            output.extend(start..=end);
        } else {
            output.insert(part.parse()?);
        }
    }
    ensure!(!output.is_empty(), "empty topology set");
    Ok(output)
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::cell::Cell;
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };
    const MPOL_BIND: i32 = 2;
    const MPOL_F_STATIC_NODES: i32 = 1 << 15;
    static NEXT_PLACEMENT: AtomicU64 = AtomicU64::new(1);
    thread_local! {
        static CALLBACK_PLACEMENT:Cell<u64>=const{Cell::new(0)};
        static THREAD_ID:Cell<libc::pid_t>=const{Cell::new(0)};
        static SDK_BOOTSTRAP:Cell<bool>=const{Cell::new(false)};
    }

    fn affinity() -> Result<libc::cpu_set_t> {
        let mut mask = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
        ensure!(
            unsafe { libc::sched_getaffinity(0, std::mem::size_of_val(&mask), &mut mask) } == 0,
            "read thread affinity: {}",
            std::io::Error::last_os_error()
        );
        Ok(mask)
    }
    fn cpu_mask(cpus: &[usize]) -> libc::cpu_set_t {
        let mut mask = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
        for cpu in cpus {
            unsafe { libc::CPU_SET(*cpu, &mut mask) };
        }
        mask
    }
    fn set_affinity(mask: &libc::cpu_set_t) -> bool {
        unsafe { libc::sched_setaffinity(0, std::mem::size_of_val(mask), mask) == 0 }
    }
    fn bind_memory(nodes: &[libc::c_ulong]) -> bool {
        unsafe {
            libc::syscall(
                libc::SYS_set_mempolicy,
                MPOL_BIND | MPOL_F_STATIC_NODES,
                nodes.as_ptr(),
                nodes.len() * libc::c_ulong::BITS as usize,
            ) == 0
        }
    }
    fn same_mask(left: &libc::cpu_set_t, right: &libc::cpu_set_t) -> bool {
        (0..1024).all(|cpu| unsafe { libc::CPU_ISSET(cpu, left) == libc::CPU_ISSET(cpu, right) })
    }
    struct AffinityScope {
        previous: Option<libc::cpu_set_t>,
    }
    impl AffinityScope {
        fn enter(mask: &libc::cpu_set_t) -> Result<Self> {
            let previous = affinity()?;
            ensure!(
                set_affinity(mask),
                "force CPU affinity: {}",
                std::io::Error::last_os_error()
            );
            let scope = Self {
                previous: Some(previous),
            };
            ensure!(
                same_mask(&affinity()?, mask),
                "kernel did not retain the requested CPU affinity"
            );
            Ok(scope)
        }
        fn restore(&mut self) -> Result<()> {
            if let Some(previous) = &self.previous {
                ensure!(
                    set_affinity(previous),
                    "restore native owner CPU affinity: {}",
                    std::io::Error::last_os_error()
                );
                ensure!(
                    same_mask(&affinity()?, previous),
                    "kernel did not restore native owner CPU affinity"
                );
                self.previous = None;
            }
            Ok(())
        }
    }
    impl Drop for AffinityScope {
        fn drop(&mut self) {
            if let Err(error) = self.restore() {
                eprintln!("failed to restore native thread affinity: {error}");
            }
        }
    }

    /// Shared only by native callbacks. First entry enforces both CPU and memory
    /// policy; a later callback outside the SDK CPU mask is rebound before use.
    pub struct CallbackPlacement {
        id: u64,
        business_id: u64,
        mask: libc::cpu_set_t,
        business: libc::cpu_set_t,
        owner: libc::pid_t,
        nodes: Vec<libc::c_ulong>,
    }
    impl CallbackPlacement {
        pub fn enforce(&self) -> bool {
            let tid = THREAD_ID.with(|cached| {
                if cached.get() == 0 {
                    cached.set(unsafe { libc::syscall(libc::SYS_gettid) as libc::pid_t });
                }
                cached.get()
            });
            if tid <= 0 {
                return false;
            }
            // Synchronous SDK callbacks belong to the business owner, not SDK workers.
            let bootstrap = SDK_BOOTSTRAP.with(Cell::get);
            let owner_callback = tid == self.owner && !bootstrap;
            let required = if owner_callback {
                &self.business
            } else {
                &self.mask
            };
            // Both SDKs share the business domain. Alternating synchronous
            // callbacks must not cause redundant affinity/NUMA syscalls there.
            let placement_id = if owner_callback {
                self.business_id
            } else {
                self.id
            };
            CALLBACK_PLACEMENT.with(|current| {
                let cpu = unsafe { libc::sched_getcpu() };
                let inside =
                    (0..1024).contains(&cpu) && unsafe { libc::CPU_ISSET(cpu as usize, required) };
                if current.get() == placement_id && inside {
                    return true;
                }
                if !set_affinity(required) || !bind_memory(&self.nodes) {
                    return false;
                }
                let mut actual = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
                if unsafe {
                    libc::sched_getaffinity(0, std::mem::size_of_val(&actual), &mut actual)
                } != 0
                    || !same_mask(&actual, required)
                {
                    return false;
                }
                current.set(placement_id);
                true
            })
        }
    }
    pub struct Placement {
        original_cpu: libc::cpu_set_t,
        original_policy: i32,
        original_nodes: Vec<libc::c_ulong>,
        sdk: Arc<CallbackPlacement>,
        market_data: Option<Arc<CallbackPlacement>>,
        background: libc::cpu_set_t,
        _single_owner: std::marker::PhantomData<std::rc::Rc<()>>,
    }
    impl Placement {
        pub fn enter(config: &PlacementConfig) -> Result<Self> {
            Self::enter_inner(config, None)
        }
        pub fn enter_combined(config: &PlacementConfig, md: &PlacementConfig) -> Result<Self> {
            config.validate_combined(md)?;
            Self::enter_inner(config, Some(md))
        }
        fn enter_inner(config: &PlacementConfig, md: Option<&PlacementConfig>) -> Result<Self> {
            config.validate()?;
            let node_cpus = parse_list(
                &std::fs::read_to_string(format!(
                    "/sys/devices/system/node/node{}/cpulist",
                    config.numa_node
                ))
                .context("read required NUMA node CPU list")?,
            )?;
            let original_cpu = affinity()?;
            let selected: Vec<_> = std::iter::once(config.business_cpu)
                .chain(config.sdk_cpus.iter().copied())
                .chain(config.background_cpus.iter().copied())
                .chain(md.into_iter().flat_map(|md| md.sdk_cpus.iter().copied()))
                .collect();
            for cpu in &selected {
                ensure!(
                    node_cpus.contains(cpu) && unsafe { libc::CPU_ISSET(*cpu, &original_cpu) },
                    "CPU {cpu} is outside the requested NUMA node or allowed affinity"
                );
            }
            let mut groups = vec![
                vec![config.business_cpu],
                config.sdk_cpus.clone(),
                config.background_cpus.clone(),
            ];
            if let Some(md) = md {
                groups.push(md.sdk_cpus.clone());
            }
            for (index, group) in groups.iter().enumerate() {
                for cpu in group {
                    let siblings = parse_list(
                        &std::fs::read_to_string(format!(
                            "/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list"
                        ))
                        .context("read physical-core sibling topology")?,
                    )?;
                    ensure!(
                        groups.iter().enumerate().all(|(other, cpus)| other == index
                            || cpus.iter().all(|c| !siblings.contains(c))),
                        "placement domains must not share an SMT physical core"
                    );
                }
            }
            let possible = parse_list(
                &std::fs::read_to_string("/sys/devices/system/node/possible")
                    .context("read NUMA node topology")?,
            )?;
            ensure!(
                possible.contains(&config.numa_node),
                "requested NUMA node is not possible on this host"
            );
            let status = std::fs::read_to_string("/proc/self/status")
                .context("read memory cgroup restrictions")?;
            let allowed = status
                .lines()
                .find_map(|line| line.strip_prefix("Mems_allowed_list:"))
                .context("missing allowed NUMA nodes")?;
            ensure!(
                parse_list(allowed)?.contains(&config.numa_node),
                "requested NUMA node is outside Mems_allowed_list"
            );
            let bits = possible.last().copied().context("no NUMA nodes")? + 1;
            let words = bits.div_ceil(libc::c_ulong::BITS as usize);
            let mut original_nodes = vec![0; words];
            let mut original_policy = 0i32;
            ensure!(
                unsafe {
                    libc::syscall(
                        libc::SYS_get_mempolicy,
                        &mut original_policy,
                        original_nodes.as_mut_ptr(),
                        words * libc::c_ulong::BITS as usize,
                        std::ptr::null_mut::<c_void>(),
                        0,
                    )
                } == 0,
                "read NUMA policy: {}",
                std::io::Error::last_os_error()
            );
            let mut nodes = vec![0; words];
            nodes[config.numa_node / libc::c_ulong::BITS as usize] |=
                1 << (config.numa_node % libc::c_ulong::BITS as usize);
            let sdk = Arc::new(CallbackPlacement {
                id: NEXT_PLACEMENT.fetch_add(1, Ordering::Relaxed),
                business_id: NEXT_PLACEMENT.fetch_add(1, Ordering::Relaxed),
                mask: cpu_mask(&config.sdk_cpus),
                business: cpu_mask(&[config.business_cpu]),
                owner: unsafe { libc::syscall(libc::SYS_gettid) as libc::pid_t },
                nodes,
            });
            let placement = Self {
                original_cpu,
                original_policy,
                original_nodes,
                market_data: md.map(|md| {
                    Arc::new(CallbackPlacement {
                        id: NEXT_PLACEMENT.fetch_add(1, Ordering::Relaxed),
                        business_id: sdk.business_id,
                        mask: cpu_mask(&md.sdk_cpus),
                        business: sdk.business,
                        owner: sdk.owner,
                        nodes: sdk.nodes.clone(),
                    })
                }),
                sdk,
                background: cpu_mask(&config.background_cpus),
                _single_owner: std::marker::PhantomData,
            };
            ensure!(
                bind_memory(&placement.sdk.nodes),
                "force NUMA MPOL_BIND failed: {}",
                std::io::Error::last_os_error()
            );
            ensure!(
                set_affinity(&cpu_mask(&[config.business_cpu])),
                "force business CPU failed: {}",
                std::io::Error::last_os_error()
            );
            ensure!(
                same_mask(&affinity()?, &cpu_mask(&[config.business_cpu])),
                "business CPU binding did not take effect"
            );
            Ok(placement)
        }
        pub fn callback(&self) -> Arc<CallbackPlacement> {
            self.sdk.clone()
        }
        pub fn on_sdk<T>(&self, call: impl FnOnce() -> Result<T>) -> Result<T> {
            self.on_domain(&self.sdk, call)
        }
        pub fn market_data_callback(&self) -> Result<Arc<CallbackPlacement>> {
            self.market_data
                .clone()
                .context("combined CTP placement required")
        }
        pub fn on_market_data_sdk<T>(&self, call: impl FnOnce() -> Result<T>) -> Result<T> {
            self.on_domain(
                self.market_data
                    .as_ref()
                    .context("combined CTP placement required")?,
                call,
            )
        }
        fn on_domain<T>(
            &self,
            domain: &CallbackPlacement,
            call: impl FnOnce() -> Result<T>,
        ) -> Result<T> {
            let mut scope = AffinityScope::enter(&domain.mask)?;
            struct RestoreBootstrap(bool);
            impl Drop for RestoreBootstrap {
                fn drop(&mut self) {
                    SDK_BOOTSTRAP.with(|state| state.set(self.0));
                }
            }
            // Keep inheritance on SDK CPUs until Create/Init has finished spawning workers,
            // even if that SDK synchronously invokes OnFrontConnected in the caller.
            let bootstrap = RestoreBootstrap(SDK_BOOTSTRAP.with(|state| state.replace(true)));
            let result = call();
            drop(bootstrap);
            scope.restore()?;
            result
        }
        pub fn on_background<T>(&self, call: impl FnOnce() -> Result<T>) -> Result<T> {
            let mut scope = AffinityScope::enter(&self.background)?;
            let result = call();
            scope.restore()?;
            result
        }
    }
    use std::ffi::c_void;
    impl Drop for Placement {
        fn drop(&mut self) {
            let basic = self.original_policy & !(MPOL_F_STATIC_NODES | (1 << 14));
            let (ptr, bits) = if basic == 0 || basic == 4 {
                (std::ptr::null(), 0)
            } else {
                (
                    self.original_nodes.as_ptr(),
                    self.original_nodes.len() * libc::c_ulong::BITS as usize,
                )
            };
            if unsafe { libc::syscall(libc::SYS_set_mempolicy, self.original_policy, ptr, bits) }
                != 0
            {
                eprintln!("failed to restore native NUMA policy");
            }
            if !set_affinity(&self.original_cpu) {
                eprintln!("failed to restore native CPU policy");
            }
        }
    }
}
#[cfg(target_os = "linux")]
pub use linux::{CallbackPlacement, Placement};

#[cfg(not(target_os = "linux"))]
mod unsupported {
    use super::*;
    pub struct CallbackPlacement;
    impl CallbackPlacement {
        pub fn enforce(&self) -> bool {
            false
        }
    }
    pub struct Placement;
    impl Placement {
        pub fn enter(_: &PlacementConfig) -> Result<Self> {
            anyhow::bail!("strict native CPU/NUMA placement requires Linux")
        }
        pub fn enter_combined(_: &PlacementConfig, _: &PlacementConfig) -> Result<Self> {
            anyhow::bail!("strict native CPU/NUMA placement requires Linux")
        }
        pub fn market_data_callback(&self) -> Result<std::sync::Arc<CallbackPlacement>> {
            anyhow::bail!("strict native CPU/NUMA placement requires Linux")
        }
        pub fn on_market_data_sdk<T>(&self, _: impl FnOnce() -> Result<T>) -> Result<T> {
            anyhow::bail!("strict native CPU/NUMA placement requires Linux")
        }
        pub fn callback(&self) -> std::sync::Arc<CallbackPlacement> {
            std::sync::Arc::new(CallbackPlacement)
        }
        pub fn on_sdk<T>(&self, f: impl FnOnce() -> Result<T>) -> Result<T> {
            f()
        }
        pub fn on_background<T>(&self, f: impl FnOnce() -> Result<T>) -> Result<T> {
            f()
        }
    }
}
#[cfg(not(target_os = "linux"))]
pub use unsupported::{CallbackPlacement, Placement};

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn combined_layout_requires_one_shared_business_core_and_four_distinct_domains() {
        let trade = PlacementConfig {
            numa_node: 0,
            business_cpu: 0,
            sdk_cpus: vec![1],
            background_cpus: vec![3],
        };
        let mut md = PlacementConfig {
            sdk_cpus: vec![2],
            ..trade.clone()
        };
        trade.validate_combined(&md).unwrap();
        md.sdk_cpus = vec![1];
        assert!(trade.validate_combined(&md).is_err());
        md.sdk_cpus = vec![2, 4];
        assert!(trade.validate_combined(&md).is_err());
        md.sdk_cpus = vec![2];
        md.business_cpu = 4;
        assert!(trade.validate_combined(&md).is_err());
        md.business_cpu = 0;
        md.numa_node = 1;
        assert!(trade.validate_combined(&md).is_err());
    }
    #[test]
    fn validates_disjoint_sets_without_applying_a_policy() {
        let mut config = PlacementConfig {
            numa_node: 0,
            business_cpu: 1,
            sdk_cpus: vec![2],
            background_cpus: vec![3],
        };
        config.validate().unwrap();
        config.sdk_cpus.push(1);
        assert!(config.validate().is_err());
        assert_eq!(parse_list("0-2,7\n").unwrap(), BTreeSet::from([0, 1, 2, 7]));
        assert!(parse_list("4-2").is_err());
    }
}
