//! Read-only inventory and navigation for independent CTA factor processes.

use anyhow::{ensure, Context, Result};
use chrono::Utc;
pub use mkt_signal::cta_factor_calc::config::Worker;
use mkt_signal::cta_factor_calc::config::{Combination, Config, Plan};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Component, Path, PathBuf};

const MAX_FILE: u64 = 32 * 1024 * 1024;

pub fn config_url(worker: Worker, combination: Option<&str>) -> String {
    let mut url = format!(
        "/cta/cta-prep-binance-futures/config/?worker={}",
        worker.name()
    );
    if let Some(name) = combination {
        url.push('#');
        url.push_str(name);
    }
    url
}

#[derive(Serialize)]
pub struct CombinationView {
    pub name: String,
    pub config_url: String,
    pub factor_count: usize,
    pub symbols: Vec<String>,
    pub intervals: Vec<String>,
    pub position_strategies: Vec<String>,
}

#[derive(Serialize)]
pub struct ProcessView {
    pub name: &'static str,
    pub config_url: String,
    pub configured: bool,
    pub unique_factor_count: usize,
    pub topics: Vec<String>,
    pub combinations: Vec<CombinationView>,
    pub updated_ms: Option<i64>,
    pub stale: bool,
    pub warming: bool,
    pub plan_matches_config: bool,
    pub errors: Vec<String>,
}

pub struct Snapshot {
    pub plan: Plan,
    /// Relative to the factor environment. Includes frequency position ledgers;
    /// Kafka history and credentials are never returned by HTTP.
    pub files: BTreeMap<PathBuf, Vec<u8>>,
    pub runtime: Option<Value>,
    pub warnings: Vec<String>,
}

fn clean_path(path: &Path) -> Result<PathBuf> {
    // Resolve lexical `..` from history_dir without following symlinks.
    let mut clean = PathBuf::new();
    for part in path.components() {
        match part {
            Component::ParentDir => {
                ensure!(clean.pop(), "factor path escapes its root");
            }
            Component::CurDir => {}
            other => clean.push(other.as_os_str()),
        }
    }
    Ok(clean)
}

fn relative(root: &Path, path: &Path) -> Result<PathBuf> {
    let clean = clean_path(path)?;
    let rel = clean
        .strip_prefix(root)
        .context("factor path is outside its environment")?;
    ensure!(!rel.as_os_str().is_empty(), "factor file path is empty");
    Ok(rel.to_path_buf())
}

fn secure_path(root: &Path, rel: &Path) -> Result<PathBuf> {
    ensure!(
        fs::symlink_metadata(root)?.is_dir(),
        "invalid factor environment"
    );
    let mut path = root.to_path_buf();
    for part in rel.components() {
        ensure!(
            matches!(part, Component::Normal(_)),
            "invalid factor relative path"
        );
        path.push(part.as_os_str());
        ensure!(
            !fs::symlink_metadata(&path)?.file_type().is_symlink(),
            "factor path contains a symlink"
        );
    }
    Ok(path)
}

fn read(root: &Path, rel: &Path) -> Result<Vec<u8>> {
    let path = secure_path(root, rel)?;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let before = file.metadata()?;
    ensure!(
        before.is_file() && before.len() <= MAX_FILE,
        "invalid or oversized factor file"
    );
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(MAX_FILE + 1)
        .read_to_end(&mut bytes)?;
    let after = file.metadata()?;
    ensure!(
        bytes.len() as u64 <= MAX_FILE
            && before.len() == after.len()
            && before.modified()? == after.modified()?,
        "factor file changed while reading"
    );
    Ok(bytes)
}

fn combination_files(root: &Path, rel: &Path) -> Result<Vec<PathBuf>> {
    let dir = secure_path(root, rel)?;
    let mut files = fs::read_dir(dir)?
        .map(|entry| entry.map(|e| rel.join(e.file_name())))
        .collect::<std::io::Result<Vec<_>>>()?;
    files.retain(|p| p.extension().is_some_and(|s| s == "json"));
    ensure!(files.len() <= 8192, "too many factor combination files");
    files.sort();
    Ok(files)
}

/// Only editable parameter objects; excludes runtime ledgers and factor history.
pub fn parameter_values(env: &Path, worker: Worker) -> Result<BTreeMap<String, Value>> {
    let root = clean_path(env)?;
    let config = Config::load(&root.join(worker.default_config()))?;
    let rel = relative(&root, &config.combinations_dir)?;
    let mut values = BTreeMap::new();
    for path in combination_files(&root, &rel)? {
        let bytes = read(&root, &path)?;
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .context("invalid parameter filename")?;
        let value = if name == "symbol_groups.json" {
            serde_json::to_value(serde_json::from_slice::<
                mkt_signal::cta_factor_calc::config::SymbolGroups,
            >(&bytes)?)?
        } else {
            serde_json::to_value(serde_json::from_slice::<Combination>(&bytes)?)?
        };
        values.insert(format!("{}/{name}", worker.name()), value);
    }
    Ok(values)
}

pub fn snapshot(env: &Path, worker: Worker) -> Result<Snapshot> {
    let root = if env.is_absolute() {
        env.to_path_buf()
    } else {
        std::env::current_dir()?.join(env)
    };
    // Canonicalizing would silently accept a symlinked environment.
    let root = clean_path(&root)?;
    let config_rel = Path::new(worker.default_config());
    let bytes = read(&root, config_rel).context("read shared factor configuration")?;
    let config = Config::load(&root.join(config_rel))
        .map_err(|_| anyhow::anyhow!("invalid shared factor configuration"))?;
    let combinations_rel = relative(&root, &config.combinations_dir)?;
    let names = combination_files(&root, &combinations_rel)?;
    let mut files = BTreeMap::from([(config_rel.to_path_buf(), bytes)]);
    let mut combinations = Vec::new();
    let mut symbol_groups = mkt_signal::cta_factor_calc::config::SymbolGroups::default();
    let mut total = 0;
    for path in &names {
        let data = read(&root, path)?;
        total += data.len();
        ensure!(
            total <= 64 * 1024 * 1024,
            "factor configuration snapshot is too large"
        );
        if path
            .file_name()
            .is_some_and(|s| s == mkt_signal::cta_factor_calc::config::SYMBOL_GROUPS_FILE)
        {
            symbol_groups = serde_json::from_slice(&data).context("decode symbol groups")?;
        } else {
            let combination: Combination =
                serde_json::from_slice(&data).context("decode factor combination")?;
            ensure!(
                path.file_stem().and_then(|s| s.to_str()) == Some(&combination.name),
                "factor combination filename does not match name"
            );
            combinations.push(combination);
        }
        files.insert(path.clone(), data);
    }
    let plan = Plan::merge_with_symbol_groups(combinations, symbol_groups)?;
    worker.validate_plan(&plan)?;
    ensure!(
        names == combination_files(&root, &combinations_rel)?,
        "factor combination inventory changed while reading"
    );
    for (path, data) in &files {
        ensure!(
            &read(&root, path)? == data,
            "factor configuration changed while reading"
        );
    }
    let positions_rel = relative(&root, &config.history_dir.join("positions"))?;
    if root.join(&positions_rel).exists() {
        for path in combination_files(&root, &positions_rel)? {
            let bytes = read(&root, &path)?;
            total += bytes.len();
            ensure!(
                total <= 64 * 1024 * 1024,
                "factor and position snapshot is too large"
            );
            let table: Value =
                serde_json::from_slice(&bytes).context("decode factor-owned position ledger")?;
            ensure!(
                table["strategy_name"].as_str() == path.file_stem().and_then(|s| s.to_str()),
                "position ledger filename/identity mismatch"
            );
            files.insert(path, bytes);
        }
    }
    let mut warnings = Vec::new();
    let runtime = (|| -> Result<Value> {
        let state = relative(&root, &config.history_dir.join("state.json"))?;
        let data = read(&root, &state)?;
        let value: Value = serde_json::from_slice(&data).context("decode factor process status")?;
        ensure!(
            value.get("worker").and_then(Value::as_str) == Some(worker.name()),
            "factor status belongs to another process"
        );
        files.insert(state, data);
        Ok(value)
    })();
    let runtime = match runtime {
        Ok(value) => Some(value),
        Err(err) => {
            warnings.push(format!("共享因子运行状态不可用：{err:#}"));
            None
        }
    };
    Ok(Snapshot {
        plan,
        files,
        runtime,
        warnings,
    })
}

pub fn view(snapshot: Result<&Snapshot>, worker: Worker) -> ProcessView {
    let mut view = ProcessView {
        name: worker.name(),
        config_url: config_url(worker, None),
        configured: false,
        unique_factor_count: 0,
        topics: Vec::new(),
        combinations: Vec::new(),
        updated_ms: None,
        stale: true,
        warming: false,
        plan_matches_config: false,
        errors: Vec::new(),
    };
    let snapshot = match snapshot {
        Ok(snapshot) => snapshot,
        Err(err) => {
            view.errors
                .push(format!("共享因子配置不可用（不代表信号已停止）：{err:#}"));
            return view;
        }
    };
    view.configured = true;
    view.unique_factor_count = snapshot.plan.calculations.len();
    view.topics = snapshot.plan.topics();
    view.errors = snapshot.warnings.clone();
    for name in snapshot.plan.combinations.keys() {
        let jobs = snapshot
            .plan
            .calculations
            .values()
            .filter(|c| c.combinations.contains(name))
            .collect::<Vec<_>>();
        view.combinations.push(CombinationView {
            name: name.clone(),
            config_url: config_url(worker, Some(name)),
            factor_count: jobs.len(),
            symbols: jobs
                .iter()
                .map(|c| c.id.symbol.clone())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect(),
            intervals: jobs
                .iter()
                .map(|c| c.id.interval.name().to_string())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect(),
            position_strategies: Vec::new(),
        });
    }
    if let Some(runtime) = &snapshot.runtime {
        view.updated_ms = runtime.get("updated_ms").and_then(Value::as_i64);
        view.stale = view.updated_ms.is_none_or(|ts| {
            let age = Utc::now().timestamp_millis().saturating_sub(ts);
            ts <= 0 || age > 180_000 || age < -30_000
        });
        view.warming = runtime
            .get("warming")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        view.plan_matches_config = runtime
            .get("plan")
            .is_some_and(|p| serde_json::to_value(&snapshot.plan).is_ok_and(|wanted| p == &wanted));
        if let Some(error) = runtime.get("error").and_then(Value::as_str) {
            view.errors.push(error.to_string());
        }
        if !view.plan_matches_config {
            view.errors.push("因子生效计划与磁盘配置尚未一致".into());
        }
        if view.stale {
            view.errors
                .push("共享因子状态超过三分钟未更新或时间无效".into());
        }
    }
    view
}

#[cfg(test)]
pub struct TestEnvironment(pub PathBuf);
#[cfg(test)]
impl Drop for TestEnvironment {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
#[cfg(test)]
pub fn test_environment() -> TestEnvironment {
    test_environment_for(Worker::Perp)
}

#[cfg(test)]
pub fn test_environment_for(worker: Worker) -> TestEnvironment {
    let root = std::env::temp_dir().join(format!("delist-factor-{}", uuid::Uuid::new_v4()));
    let source = Path::new(env!("CARGO_MANIFEST_DIR"));
    let directory = if worker == Worker::Perp {
        "config/cta_prep_binance_futures"
    } else {
        "config/cta_factor_spread"
    };
    fs::create_dir_all(root.join(directory)).unwrap();
    fs::copy(
        source.join(worker.default_config()),
        root.join(worker.default_config()),
    )
    .unwrap();
    for entry in fs::read_dir(source.join(directory)).unwrap() {
        let entry = entry.unwrap();
        fs::copy(entry.path(), root.join(directory).join(entry.file_name())).unwrap();
    }
    TestEnvironment(root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn unified_catalog_has_both_groups_without_claiming_a_live_process() {
        let env = test_environment();
        let snapshot = snapshot(&env.0.join("config/.."), Worker::Perp).unwrap();
        let view = view(Ok(&snapshot), Worker::Perp);
        assert_eq!(view.name, "cta-prep-binance-futures");
        assert_eq!(view.unique_factor_count, 404);
        assert_eq!(view.topics, ["binance-futures"]);
        let counts = view
            .combinations
            .iter()
            .map(|c| (c.name.as_str(), c.factor_count))
            .collect::<Vec<_>>();
        assert_eq!(
            &counts[..3],
            [
                ("binance-futures_15min", 180),
                ("binance-futures_1min", 61),
                ("binance-futures_5min", 63)
            ]
        );
        assert_eq!(
            counts[3..]
                .iter()
                .map(|(name, _)| *name)
                .collect::<Vec<_>>(),
            [
                "last-20_prep02_binance-futures_15min",
                "last-20_prep02_binance-futures_1min",
                "last-20_prep02_binance-futures_5min",
                "top-20_prep02_binance-futures_15min",
                "top-20_prep02_binance-futures_1min",
                "top-20_prep02_binance-futures_5min",
                "top-5_prep02_binance-futures_15min",
                "top-5_prep02_binance-futures_1min",
                "top-5_prep02_binance-futures_5min"
            ]
        );
        assert_eq!(
            counts
                .iter()
                .filter(|(_, n)| *n == 0)
                .map(|(name, _)| *name)
                .collect::<Vec<_>>(),
            ["top-20_prep02_binance-futures_5min"]
        );
        assert!(view.stale && !view.plan_matches_config);
        assert!(snapshot.files.keys().all(|p| p.starts_with("config")));
        assert!(snapshot.files.contains_key(Path::new(
            "config/cta_prep_binance_futures/symbol_groups.json"
        )));
    }

    #[test]
    fn runtime_must_match_the_active_plan_and_staleness_is_separate_from_config() {
        let env = test_environment();
        let mut snapshot = snapshot(&env.0, Worker::Perp).unwrap();
        let state = serde_json::json!({"worker":Worker::Perp, "updated_ms":Utc::now().timestamp_millis(), "warming":true, "plan":snapshot.plan});
        fs::create_dir_all(env.0.join("data/cta_prep_binance_futures")).unwrap();
        fs::write(
            env.0.join("data/cta_prep_binance_futures/state.json"),
            serde_json::to_vec(&state).unwrap(),
        )
        .unwrap();
        snapshot = super::snapshot(&env.0, Worker::Perp).unwrap();
        let v = view(Ok(&snapshot), Worker::Perp);
        assert!(v.configured && !v.stale && v.warming && v.plan_matches_config);
        snapshot.runtime.as_mut().unwrap()["plan"] = Value::Null;
        let v = view(Ok(&snapshot), Worker::Perp);
        assert!(!v.plan_matches_config && !v.errors.is_empty());
        snapshot.runtime.as_mut().unwrap()["updated_ms"] = Value::from(1);
        assert!(view(Ok(&snapshot), Worker::Perp).stale);
    }

    #[test]
    fn rejects_paths_outside_environment_and_symlinked_configuration() {
        assert!(relative(
            Path::new("/home/worker"),
            Path::new("/home/worker/config/../../secret")
        )
        .is_err());
        let root = std::env::temp_dir().join(format!("delist-factor-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        symlink(env!("CARGO_MANIFEST_DIR"), root.join("linked")).unwrap();
        assert!(secure_path(
            &root,
            Path::new("linked/config/cta_prep_binance_futures.toml")
        )
        .is_err());
        assert!(snapshot(&root.join("linked"), Worker::Perp).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
