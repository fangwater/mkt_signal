//! Real OS placement checks without loading any SDK or executing any CTP path.
use crate::placement::{Placement, PlacementConfig};
use std::{collections::BTreeSet, path::Path};

fn affinity() -> Vec<usize> {
    let mut mask = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    assert_eq!(
        unsafe { libc::sched_getaffinity(0, std::mem::size_of_val(&mask), &mut mask) },
        0
    );
    (0..1024)
        .filter(|cpu| unsafe { libc::CPU_ISSET(*cpu, &mask) })
        .collect()
}
fn policy() -> (i32, [libc::c_ulong; 64]) {
    let mut mode = 0;
    let mut nodes = [0; 64];
    assert_eq!(
        unsafe {
            libc::syscall(
                libc::SYS_get_mempolicy,
                &mut mode,
                nodes.as_mut_ptr(),
                4096,
                std::ptr::null_mut::<libc::c_void>(),
                0,
            )
        },
        0
    );
    (mode, nodes)
}
fn topology() -> Option<PlacementConfig> {
    let allowed = affinity();
    for entry in std::fs::read_dir("/sys/devices/system/node").ok()? {
        let entry = entry.ok()?;
        let Some(node) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.strip_prefix("node"))
            .and_then(|s| s.parse::<usize>().ok())
        else {
            continue;
        };
        let mut seen = BTreeSet::new();
        let mut cores = Vec::new();
        for cpu in &allowed {
            if !entry.path().join(format!("cpu{cpu}")).exists() {
                continue;
            }
            let path = format!("/sys/devices/system/cpu/cpu{cpu}/topology");
            let key = (
                std::fs::read_to_string(Path::new(&path).join("physical_package_id")).ok()?,
                std::fs::read_to_string(Path::new(&path).join("core_id")).ok()?,
            );
            if seen.insert(key) {
                cores.push(*cpu);
            }
            if cores.len() == 5 {
                return Some(PlacementConfig {
                    numa_node: node,
                    business_cpu: cores[0],
                    sdk_cpus: cores[1..3].to_vec(),
                    background_cpus: cores[3..].to_vec(),
                });
            }
        }
    }
    None
}
fn assert_bound(node: usize) {
    let (mode, nodes) = policy();
    assert_eq!(mode & 0x3fff, 2); // MPOL_BIND, ignoring optional policy flags.
    let width = libc::c_ulong::BITS as usize;
    assert_eq!(nodes.iter().map(|n| n.count_ones()).sum::<u32>(), 1);
    assert_ne!(nodes[node / width] & (1 << (node % width)), 0);
}

#[test]
fn native_domains_enforce_affinity_numa_callback_ownership_and_restore() {
    let Some(config) = topology() else {
        eprintln!("placement check requires five allowed physical cores on one NUMA node");
        return;
    };
    let original_affinity = affinity();
    let original_policy = policy();
    let placed = Placement::enter(&config).unwrap();
    assert_eq!(affinity(), vec![config.business_cpu]);
    assert_bound(config.numa_node);
    let callback = placed.callback();
    placed
        .on_sdk(|| {
            assert_eq!(affinity(), config.sdk_cpus);
            assert!(callback.enforce()); // Synchronous bootstrap callback stays on SDK CPUs.
            assert_eq!(affinity(), config.sdk_cpus);
            Ok(())
        })
        .unwrap();
    assert_eq!(affinity(), vec![config.business_cpu]);
    assert!(callback.enforce()); // Synchronous order callback belongs to the business owner.
    assert_eq!(affinity(), vec![config.business_cpu]);
    let sdk = config.sdk_cpus.clone();
    let node = config.numa_node;
    std::thread::spawn(move || {
        assert!(callback.enforce()); // A new worker initially inherits business affinity.
        assert_eq!(affinity(), sdk);
        assert_bound(node);
    })
    .join()
    .unwrap();
    placed
        .on_background(|| {
            let expected = config.background_cpus.clone();
            std::thread::spawn(move || {
                assert_eq!(affinity(), expected);
                assert_bound(node);
            })
            .join()
            .unwrap();
            Ok(())
        })
        .unwrap();
    assert_eq!(affinity(), vec![config.business_cpu]);
    drop(placed);
    assert_eq!(affinity(), original_affinity);
    assert_eq!(policy(), original_policy);
}

#[test]
fn invalid_domains_fail_before_changing_thread_policy() {
    let Some(mut config) = topology() else {
        return;
    };
    let original_affinity = affinity();
    let original_policy = policy();
    config.background_cpus.push(config.business_cpu);
    assert!(Placement::enter(&config).is_err());
    assert_eq!(affinity(), original_affinity);
    assert_eq!(policy(), original_policy);
}
