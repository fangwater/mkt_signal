//! Read-only CTA combination, shared factor process and Manager/Exec mapping.

use super::cta_factors;
use anyhow::{bail, Context, Result};
use chrono::Utc;
use cta_factors::Worker;
use mkt_signal::common::delist_accounts::{
    build_account_views_for_venues, AccountRiskView, AccountSpec, RedisSite,
};
use mkt_signal::common::delist_risk::RiskQueryResponse;
use mkt_signal::common::exchange_info::ListingIndex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio_postgres::{IsolationLevel, NoTls};

#[derive(Clone)]
pub struct Settings {
    pub env_root: PathBuf,
    pub factor_env: PathBuf,
    pub spread_factor_env: PathBuf,
    pub jp_database: Option<String>,
}
impl Settings {
    pub(crate) fn factor_environment(&self, worker: Worker) -> &Path {
        if worker == Worker::Perp {
            &self.factor_env
        } else {
            &self.spread_factor_env
        }
    }
}

#[derive(Deserialize)]
struct Publication {
    manager_url: String,
}

pub(crate) struct Signal {
    pub(crate) slug: String,
    pub(crate) combination_name: String,
    pub(crate) worker: Worker,
    pub(crate) venues: Vec<String>,
    enabled: bool,
    pub(crate) bar_interval: String,
    updated_ms: i64,
    pub(crate) stale: bool,
    pub(crate) names: BTreeSet<String>,
    symbols: BTreeSet<String>,
    publishes_to_jp: bool,
}

#[derive(Deserialize)]
struct ManagerConfig {
    #[serde(default)]
    sources: Vec<Source>,
}

#[derive(Deserialize)]
struct Source {
    id: String,
    #[serde(default)]
    alias: Option<String>,
    #[serde(default)]
    gateway_prefix: Option<String>,
    #[serde(default = "default_enabled")]
    enabled: bool,
}
fn default_enabled() -> bool {
    true
}

#[derive(Clone, Debug)]
struct Binding {
    source: String,
    binding_name: String,
    strategy: String,
    order_strategy: String,
    shares: f64,
    source_enabled: bool,
    alias: String,
}

#[derive(Clone, Debug)]
struct Follow {
    source: String,
    virtual_name: String,
    binding_name: String,
    strategy: String,
    order_strategy: String,
    shares: f64,
    multiplier: f64,
}

struct ManagerData {
    site: &'static str,
    sources: BTreeMap<String, Source>,
    bindings: Vec<Binding>,
    follows: Vec<Follow>,
}

#[derive(Serialize)]
pub struct ManagerStatus {
    site: &'static str,
    ok: bool,
    error: Option<String>,
}

#[derive(Serialize)]
pub struct ExecView {
    site: &'static str,
    source_id: String,
    alias: String,
    active: bool,
    configured: bool,
    pending: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    viz_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    config_url: Option<String>,
    virtual_accounts: Vec<String>,
}

#[derive(Serialize)]
pub struct StrategyView {
    slug: String,
    name: String,
    factor_worker: &'static str,
    kind: &'static str,
    risk_by_venue: Vec<AccountRiskView>,
    factor_count: Option<usize>,
    enabled: bool,
    bar_interval: String,
    updated_ms: i64,
    stale: bool,
    errors: Vec<String>,
    accounts: Vec<ExecView>,
    risk: AccountRiskView,
}

#[derive(Serialize)]
pub struct Response {
    as_of_ms: i64,
    managers: Vec<ManagerStatus>,
    strategies: Vec<StrategyView>,
    factor_processes: Vec<cta_factors::ProcessView>,
    errors: Vec<String>,
}

/// Catalog availability does not prove announcement/detail/schedule availability.
pub fn apply_source_health(
    response: &mut Response,
    status: &mkt_signal::common::delist_store::StatusBook,
) {
    let now = chrono::Utc::now().timestamp_millis();
    for strategy in &mut response.strategies {
        for view in &mut strategy.risk_by_venue {
            let venue = &view.venues[0];
            let schedule = format!("schedule:{venue}");
            let sources: Vec<&str> = match venue.as_str() {
                "okex-margin" => vec![
                    "okex_spot_exchange_info",
                    &schedule,
                    "okex_announcements",
                    "okex_article_detail",
                ],
                "okex-futures" => vec![
                    "okex_futures_exchange_info",
                    &schedule,
                    "okex_announcements",
                    "okex_article_detail",
                ],
                "binance-margin" => vec![
                    "binance_cms",
                    "binance_spot_delist",
                    "binance_margin_delist",
                ],
                "binance-futures" | "binance-coin-futures" => vec!["binance_cms", &schedule],
                _ => continue,
            };
            for source in sources {
                let fresh = status.source(source).is_some_and(|s| {
                    s.ok && s
                        .last_success_ms
                        .is_some_and(|ms| now.saturating_sub(ms) < 2 * 86_400_000)
                });
                if !fresh {
                    let detail = status
                        .source(source)
                        .and_then(|s| s.last_error.as_deref())
                        .unwrap_or("未就绪或数据已过期");
                    strategy
                        .errors
                        .push(format!("{venue} 风险源 {source}: {detail}"));
                    if view.risk_n == 0 {
                        view.tone = "uncovered".into();
                    }
                    if strategy.risk.risk_n == 0 {
                        strategy.risk.tone = "uncovered".into();
                    }
                }
            }
        }
    }
}

#[derive(Deserialize, Default)]
struct WorkerPositions {
    #[serde(default)]
    tables: Vec<WorkerTable>,
    #[serde(default)]
    manager_publications: Vec<Publication>,
    publication_error: Option<String>,
}

#[derive(Deserialize)]
struct WorkerTable {
    strategy_name: String,
    combination_name: String,
    interval: String,
    enabled: bool,
    #[serde(default)]
    retired: bool,
    #[serde(default)]
    retiring: bool,
    #[serde(default)]
    targets: BTreeMap<String, serde_json::Value>,
}

fn current_worker_signals(
    snapshot: &cta_factors::Snapshot,
    worker: Worker,
) -> Result<Vec<(Signal, Vec<String>)>> {
    let runtime = snapshot.runtime.as_ref();
    let updated_ms = runtime.and_then(|r| r["updated_ms"].as_i64()).unwrap_or(0);
    let age = Utc::now().timestamp_millis().saturating_sub(updated_ms);
    // A 15-minute table can legitimately keep its last decision for 15 minutes.
    // The shared worker heartbeat determines process freshness.
    let stale = updated_ms <= 0 || age > 180_000 || age < -30_000;
    let positions: WorkerPositions = match runtime.and_then(|r| r.get("position_tables")) {
        Some(value) if !value.is_null() => {
            serde_json::from_value(value.clone()).context("decode worker position tables")?
        }
        _ => WorkerPositions::default(),
    };
    let mut tables = BTreeMap::new();
    for table in &positions.tables {
        anyhow::ensure!(
            tables.insert(table.strategy_name.as_str(), table).is_none(),
            "duplicate worker position strategy"
        );
    }
    let publishes_to_jp = positions.manager_publications.iter().any(|p| {
        reqwest::Url::parse(&p.manager_url).ok().is_some_and(|u| {
            matches!(
                (u.host_str(), u.port_or_known_default()),
                (Some("127.0.0.1" | "localhost" | "::1"), Some(4191 | 18201))
            )
        })
    });
    let mut signals = Vec::new();
    for combination in snapshot.plan.combinations.values() {
        let Some(configured_signal) = &combination.signal else {
            continue;
        };
        let name = combination.manager_strategy_name();
        let mut issues = Vec::new();
        let mut symbols: BTreeSet<String> = combination.signal_table()?.into_keys().collect();
        let table = tables.get(name.as_str()).copied();
        let mut table_matches = false;
        if let Some(table) = table {
            table_matches = table.combination_name == combination.name
                && table.interval == combination.interval.name();
            if !table_matches {
                issues.push("仓位表身份或周期与当前组合不一致".into());
            }
            if table.enabled != configured_signal.enabled && !table.retiring && !table.retired {
                issues.push("仓位表开关与当前配置尚未同步".into());
            }
            for target in table.targets.keys() {
                let symbol = target.rsplit(':').next().unwrap_or("");
                anyhow::ensure!(
                    symbol.ends_with("USDT")
                        && symbol.len() > 4
                        && symbol.len() <= 32
                        && symbol
                            .bytes()
                            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()),
                    "invalid position target symbol"
                );
                symbols.insert(symbol.into());
            }
        } else {
            issues.push("当前组合的运行仓位表尚未生成".into());
        }
        if let Some(error) = runtime.and_then(|r| r["positions_error"].as_str()) {
            issues.push(format!("仓位计算异常：{error}"));
        }
        if let Some(error) = &positions.publication_error {
            issues.push(format!("Manager 仓位发布异常：{error}"));
        }
        let mut venues = vec![combination.venue.name().to_string()];
        if let Some(reference) = combination.reference_venue {
            venues.push(reference.name().into());
        }
        signals.push((
            Signal {
                slug: name.clone(),
                combination_name: combination.name.clone(),
                worker,
                venues,
                enabled: table.is_some_and(|t| t.enabled && !t.retired && !t.retiring),
                bar_interval: combination.interval.name().into(),
                updated_ms,
                stale: stale || !table_matches,
                names: BTreeSet::from([name]),
                symbols,
                publishes_to_jp,
            },
            issues,
        ));
    }
    Ok(signals)
}

fn safe_gateway(prefix: Option<&str>) -> Option<String> {
    let p = prefix?;
    if p.len() < 2
        || p.len() > 128
        || !p.starts_with('/')
        || !p[1..]
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return None;
    }
    Some(format!("{p}/"))
}

async fn manager_sources(settings: &Settings) -> Result<BTreeMap<String, Source>> {
    let data = tokio::fs::read(
        settings
            .env_root
            .join("crypto_cta_manager/config/cta-manager.toml"),
    )
    .await
    .context("read Manager sources")?;
    if data.len() > 1_048_576 {
        bail!("Manager sources file is too large");
    }
    let config: ManagerConfig = toml::from_str(std::str::from_utf8(&data)?)
        .map_err(|_| anyhow::anyhow!("decode Manager sources failed"))?;
    let mut result = BTreeMap::new();
    for source in config.sources {
        if source.id.is_empty() || result.insert(source.id.clone(), source).is_some() {
            bail!("invalid or duplicate Manager source");
        }
    }
    Ok(result)
}

async fn manager(url: Option<&str>, settings: &Settings) -> Result<ManagerData> {
    let url = url.context("Manager read-only database is not configured")?;
    let sources = manager_sources(settings).await?;
    let (mut client, connection) =
        tokio::time::timeout(Duration::from_secs(5), tokio_postgres::connect(url, NoTls))
            .await
            .context("Manager database connect timed out")?
            .map_err(|_| anyhow::anyhow!("Manager database connection failed"))?;
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let result = tokio::time::timeout(Duration::from_secs(8), async {
        let tx = client.build_transaction().isolation_level(IsolationLevel::RepeatableRead).read_only(true).start().await.context("start read-only Manager snapshot")?;
        let binding_rows = tx.query("SELECT b.source_id,b.position_strategy_name,b.shares,s.enabled,COALESCE(s.display_alias,s.account_label),b.binding_name,b.order_strategy_name FROM cta_account_strategy_bindings b JOIN cta_order_sources s ON s.source_id=b.source_id", &[]).await.context("read Manager account bindings")?;
        let follow_rows = tx.query("SELECT f.source_id,v.name,b.position_strategy_name,b.shares,f.multiplier,b.binding_name,b.order_strategy_name FROM cta_account_follows f JOIN cta_virtual_accounts v ON v.virtual_id=f.virtual_id JOIN cta_virtual_account_bindings b ON b.virtual_id=v.virtual_id", &[]).await.context("read Manager virtual follows")?;
        tx.commit().await.context("finish read-only Manager snapshot")?;
        let bindings = binding_rows.into_iter().map(|r| Binding { source:r.get(0), strategy:r.get(1), shares:r.get(2), source_enabled:r.get(3), alias:r.get(4), binding_name:r.get(5), order_strategy:r.get(6) }).collect();
        let follows = follow_rows.into_iter().map(|r| Follow { source:r.get(0), virtual_name:r.get(1), strategy:r.get(2), shares:r.get(3), multiplier:r.get(4), binding_name:r.get(5), order_strategy:r.get(6) }).collect();
        Ok(ManagerData { site: "jp", sources, bindings, follows })
    }).await.context("Manager snapshot timed out")?;
    connection_task.abort();
    result
}

fn account_views(signal: &Signal, manager: &ManagerData) -> Vec<ExecView> {
    let mut ids = BTreeSet::new();
    for b in &manager.bindings {
        if signal.names.contains(&b.strategy) {
            ids.insert(b.source.clone());
        }
    }
    for f in &manager.follows {
        if signal.names.contains(&f.strategy) {
            ids.insert(f.source.clone());
        }
    }
    ids.into_iter()
        .map(|id| {
            let matches: Vec<_> = manager
                .bindings
                .iter()
                .filter(|b| b.source == id && signal.names.contains(&b.strategy))
                .collect();
            let follows: Vec<_> = manager
                .follows
                .iter()
                .filter(|f| f.source == id && signal.names.contains(&f.strategy))
                .collect();
            let source = manager.sources.get(&id);
            let active = source.is_some_and(|s| s.enabled)
                && matches.iter().any(|b| b.shares > 0.0 && b.source_enabled);
            let configured = follows.iter().any(|f| f.shares > 0.0 && f.multiplier > 0.0);
            let mut expected = BTreeMap::<(&str, &str, &str), f64>::new();
            for f in &follows {
                *expected
                    .entry((&f.binding_name, &f.strategy, &f.order_strategy))
                    .or_default() += f.shares * f.multiplier;
            }
            let pending = expected
                .into_iter()
                .any(|((name, strategy, order), desired)| {
                    let actual = matches
                        .iter()
                        .find(|b| {
                            b.binding_name == name
                                && b.strategy == strategy
                                && b.order_strategy == order
                        })
                        .map_or(0.0, |b| b.shares);
                    (actual - desired).abs() > 1e-9 * desired.abs().max(1.0)
                });
            let viz = source.and_then(|s| {
                if s.enabled {
                    safe_gateway(s.gateway_prefix.as_deref())
                } else {
                    None
                }
            });
            let config = viz.as_ref().map(|v| format!("{}config", v));
            let alias = source
                .and_then(|s| s.alias.clone())
                .or_else(|| matches.first().map(|b| b.alias.clone()))
                .unwrap_or_else(|| id.clone());
            let virtual_accounts = follows
                .iter()
                .map(|f| f.virtual_name.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            ExecView {
                site: manager.site,
                source_id: id,
                alias,
                active,
                configured,
                pending,
                viz_url: viz,
                config_url: config,
                virtual_accounts,
            }
        })
        .collect()
}

fn signal_risk_views(
    signal: &Signal,
    risk: &RiskQueryResponse,
    listings: &ListingIndex,
) -> (AccountRiskView, Vec<AccountRiskView>) {
    let mut views = Vec::new();
    for venue in &signal.venues {
        let exchange = venue.split('-').next().unwrap_or("unknown");
        let spec = AccountSpec {
            slug: signal.slug.clone(),
            alias: signal.combination_name.clone(),
            exchange: exchange.into(),
            kind: "cta".into(),
            host: "jp".into(),
            site: RedisSite::Jp,
            config_url: cta_factors::config_url(signal.worker, Some(&signal.combination_name)),
        };
        let universes = BTreeMap::from([(signal.slug.clone(), Ok(signal.symbols.clone()))]);
        let scope = BTreeMap::from([(signal.slug.clone(), vec![venue.clone()])]);
        views.extend(build_account_views_for_venues(
            &[spec],
            risk,
            listings,
            &universes,
            &scope,
        ));
    }
    let mut aggregate = views[0].clone();
    let mut hits = BTreeMap::new();
    for view in &views {
        for hit in &view.hits {
            let entry = hits.entry(hit.symbol.clone()).or_insert_with(|| {
                let mut hit = hit.clone();
                hit.events.clear();
                hit
            });
            entry.events.extend(hit.events.clone());
            if hit.tone == "risk" {
                entry.tone = "risk".into();
                entry.listing = hit.listing.clone();
            }
        }
    }
    aggregate.hits = hits.into_values().collect();
    aggregate.venues = signal.venues.clone();
    aggregate.risk_n = aggregate
        .hits
        .iter()
        .filter(|hit| hit.tone == "risk")
        .count();
    aggregate.tone = if aggregate.risk_n > 0 {
        "risk"
    } else if views.iter().any(|v| v.tone == "error") {
        "error"
    } else if views.iter().any(|v| v.tone == "uncovered") {
        "uncovered"
    } else {
        "ok"
    }
    .into();
    (aggregate, views)
}

pub async fn load(
    settings: &Settings,
    risk: &RiskQueryResponse,
    listings: &ListingIndex,
) -> Response {
    let mut signals = Vec::new();
    let mut errors = Vec::new();
    let mut factor_processes = Vec::new();
    for (worker, environment) in [
        (Worker::Perp, &settings.factor_env),
        (Worker::Spread, &settings.spread_factor_env),
    ] {
        let snapshot = cta_factors::snapshot(environment, worker);
        let mut process = cta_factors::view(
            snapshot.as_ref().map_err(|e| anyhow::anyhow!("{e:#}")),
            worker,
        );
        if let Ok(snapshot) = &snapshot {
            match current_worker_signals(snapshot, worker) {
                Ok(rows) => signals.extend(rows),
                Err(err) => errors.push(format!("{} 仓位状态读取失败：{err:#}", worker.name())),
            }
        }
        for combination in &mut process.combinations {
            combination.position_strategies = signals
                .iter()
                .map(|(signal, _)| signal)
                .filter(|signal| {
                    signal.worker == worker && signal.combination_name == combination.name
                })
                .map(|signal| signal.slug.clone())
                .collect();
        }
        factor_processes.push(process);
    }
    let jp = manager(settings.jp_database.as_deref(), settings).await;
    let mut managers = Vec::new();
    let mut available = Vec::new();
    for (site, result) in [("jp", jp)] {
        match result {
            Ok(data) => {
                managers.push(ManagerStatus {
                    site,
                    ok: true,
                    error: None,
                });
                available.push(data);
            }
            Err(e) => managers.push(ManagerStatus {
                site,
                ok: false,
                error: Some(format!("{e:#}")),
            }),
        }
    }
    let mut owners = BTreeMap::<String, Vec<String>>::new();
    for (s, _) in &signals {
        for name in &s.names {
            owners.entry(name.clone()).or_default().push(s.slug.clone());
        }
    }
    let mut strategies = Vec::new();
    for (mut signal, mut issues) in signals {
        let factor_process = factor_processes
            .iter()
            .find(|p| p.name == signal.worker.name());
        let factor_combination = factor_process.and_then(|p| {
            p.combinations
                .iter()
                .find(|c| c.name == signal.combination_name)
        });
        if let Some(combination) = factor_combination {
            signal.symbols.extend(combination.symbols.iter().cloned());
            if combination.position_strategies.len() > 1 {
                issues.push("同一因子组合关联多个仓位策略，请核对运行身份".into());
            }
        } else if factor_process.is_some_and(|p| p.configured) {
            issues.push("共享因子清单未包含此组合，当前风险范围来自信号快照".into());
        }
        if signal.stale {
            issues.push("计算进程状态超过 3 分钟未更新或仓位表尚未就绪".into());
        }
        let before = signal.names.len();
        signal
            .names
            .retain(|n| owners.get(n.as_str()).is_some_and(|v| v.len() == 1));
        if signal.names.len() != before {
            issues.push("策略标识与其他信号重复，重复部分未归属".into());
        }
        if !signal.publishes_to_jp {
            issues.push("未发现向日本 Manager 的发布配置".into());
        }
        let mut accounts = Vec::new();
        for m in &available {
            accounts.extend(account_views(&signal, m));
        }
        if accounts.iter().any(|a| a.pending) {
            issues.push("Manager 跟随配置与执行绑定尚未一致".into());
        }
        if accounts.iter().any(|a| a.viz_url.is_none()) {
            issues.push("部分执行账户缺少可用的面板入口".into());
        }
        let (mut risk_view, risk_by_venue) = signal_risk_views(&signal, risk, listings);
        risk_view.viz_url = None;
        risk_view.config_url = Some(cta_factors::config_url(
            signal.worker,
            Some(&signal.combination_name),
        ));
        strategies.push(StrategyView {
            name: signal.combination_name,
            factor_worker: signal.worker.name(),
            kind: if signal.worker == Worker::Perp {
                "prep"
            } else {
                "spread"
            },
            risk_by_venue,
            factor_count: factor_combination.map(|c| c.factor_count),
            slug: signal.slug,
            enabled: signal.enabled,
            bar_interval: signal.bar_interval,
            updated_ms: signal.updated_ms,
            stale: signal.stale,
            errors: issues,
            accounts,
            risk: risk_view,
        });
    }
    errors.sort();
    Response {
        as_of_ms: Utc::now().timestamp_millis(),
        managers,
        strategies,
        factor_processes,
        errors,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_signal::cta_spread::config::SpreadMode;

    fn write_worker_runtime(env: &Path, worker: Worker) {
        let snapshot = cta_factors::snapshot(env, worker).unwrap();
        let config =
            mkt_signal::cta_factor_calc::config::Config::load(&env.join(worker.default_config()))
                .unwrap();
        let tables = snapshot
            .plan
            .combinations
            .values()
            .filter_map(|c| {
                c.signal.as_ref().map(|signal| {
                    serde_json::json!({
                        "strategy_name": c.manager_strategy_name(), "combination_name":c.name,
                        "interval": c.interval.name(), "enabled":signal.enabled,
                        "updated_ts_us":Utc::now().timestamp_micros()-1_200_000_000_i64,
                        "targets":{"UNLISTEDUSDT":{"qty":1,"signal":0}}
                    })
                })
            })
            .collect::<Vec<_>>();
        std::fs::create_dir_all(&config.history_dir).unwrap();
        std::fs::write(
            config.history_dir.join("state.json"),
            serde_json::to_vec(&serde_json::json!({
                "worker":worker.name(), "updated_ms":Utc::now().timestamp_millis(),
                "warming":false, "plan":snapshot.plan, "positions_error":null,
                "position_tables":{"tables":tables,"publication_error":null,
                    "manager_publications":[{"manager_url":"http://127.0.0.1:18201/api"}]}
            }))
            .unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn spread_retains_either_legs_known_risk_when_catalogs_are_unavailable() {
        use mkt_signal::common::announcement_llm::{LlmAction, LlmExtractInput};
        use mkt_signal::common::delist_risk::RiskBook;
        let signal = Signal {
            slug: "okex-binance-cta-spread-rx01".into(),
            combination_name: "test".into(),
            worker: Worker::Spread,
            venues: vec!["okex-futures".into(), "binance-futures".into()],
            enabled: true,
            bar_interval: "1m".into(),
            updated_ms: 0,
            stale: false,
            names: BTreeSet::new(),
            symbols: BTreeSet::from(["BTCUSDT".into()]),
            publishes_to_jp: true,
        };
        let mut book = RiskBook::default();
        let (empty, views) = signal_risk_views(
            &signal,
            &book.query(&Default::default()),
            &ListingIndex::default(),
        );
        assert_eq!(empty.tone, "uncovered");
        assert!(views.iter().all(|v| v.tone == "uncovered"));
        let response_for = |risk, risk_by_venue| Response {
            as_of_ms: 0,
            managers: vec![],
            factor_processes: vec![],
            errors: vec![],
            strategies: vec![StrategyView {
                slug: signal.slug.clone(),
                name: "test".into(),
                factor_worker: "test",
                kind: "spread",
                risk_by_venue,
                factor_count: None,
                enabled: true,
                bar_interval: "1m".into(),
                updated_ms: 0,
                stale: false,
                errors: vec![],
                accounts: vec![],
                risk,
            }],
        };
        // Test source health independently of the catalog result computed above.
        let mut ready_risk = empty.clone();
        ready_risk.tone = "ok".into();
        let mut ready_views = views.clone();
        for view in &mut ready_views {
            view.tone = "ok".into();
        }
        let mut healthy = mkt_signal::common::delist_store::StatusBook::default();
        for source in [
            "okex_futures_exchange_info",
            "schedule:okex-futures",
            "okex_announcements",
            "okex_article_detail",
            "binance_cms",
            "schedule:binance-futures",
        ] {
            healthy.mark_ok(source, "fetch");
        }
        let mut response = response_for(ready_risk.clone(), ready_views.clone());
        apply_source_health(&mut response, &healthy);
        assert_eq!(response.strategies[0].risk.tone, "ok");
        assert!(response.strategies[0].errors.is_empty());
        healthy.mark_err("binance_cms", "fetch", "offline");
        let mut response = response_for(ready_risk, ready_views);
        apply_source_health(&mut response, &healthy);
        assert_eq!(response.strategies[0].risk.tone, "uncovered");
        assert_eq!(response.strategies[0].risk_by_venue[0].tone, "ok");
        assert_eq!(response.strategies[0].risk_by_venue[1].tone, "uncovered");
        for exchange in ["okex", "binance"] {
            let input = LlmExtractInput {
                exchange: exchange.into(),
                id: exchange.into(),
                title: "future delist".into(),
                url: "https://www.okx.com/help/test".into(),
                published_ms: 1,
                body: String::new(),
            };
            let utc = (Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
            book.ingest_llm(
                &input,
                true,
                &[LlmAction {
                    action: "delist".into(),
                    venue: format!("{exchange}-futures"),
                    exchange: exchange.into(),
                    utc,
                    assets: vec![],
                    symbols: vec!["BTCUSDT".into()],
                    note: String::new(),
                }],
            );
            let (aggregate, views) = signal_risk_views(
                &signal,
                &book.query(&Default::default()),
                &ListingIndex::default(),
            );
            assert_eq!(aggregate.tone, "risk");
            let mut response = response_for(aggregate.clone(), views.clone());
            apply_source_health(
                &mut response,
                &mkt_signal::common::delist_store::StatusBook::default(),
            );
            assert_eq!(response.strategies[0].risk.tone, "risk");
            assert!(!response.strategies[0].errors.is_empty());
            assert!(
                views
                    .iter()
                    .find(|v| v.exchange == exchange)
                    .unwrap()
                    .risk_n
                    > 0
            );
            assert_eq!(
                aggregate.hits[0].events.len(),
                if exchange == "okex" { 1 } else { 2 }
            );
        }
    }

    #[tokio::test]
    async fn current_spread_and_prep_are_discovered_without_legacy_signal_directories() {
        let prep = cta_factors::test_environment();
        let spread = cta_factors::test_environment_for(Worker::Spread);
        let slug = "okex-binance-cta-spread-rx01";
        let name = SpreadMode::OkexBinance.manager_strategy_name();
        let run = prep.0.join(slug).join("run");
        std::fs::create_dir_all(&run).unwrap();
        let status = serde_json::json!({"updated_ts_us":Utc::now().timestamp_micros(),"enabled":true,"bar_interval":"1m","mode":"okex_binance","manager_strategy_name":name,"combination_name":name,"symbol_factors":{"BTCUSDT":{}},"strategies":{"factor_036":{"ETHUSDT":{"cash_qty":1,"perpetual_qty":-1}}},"manager_publications":[]});
        std::fs::write(
            run.join("cta_cross_spread_status.json"),
            serde_json::to_vec(&status).unwrap(),
        )
        .unwrap();
        let mut combination: mkt_signal::cta_factor_calc::config::Combination =
            serde_json::from_slice(
                &std::fs::read(
                    spread
                        .0
                        .join("config/cta_factor_spread/okex-futures_binance-futures_1min.json"),
                )
                .unwrap(),
            )
            .unwrap();
        combination.name = name.into();
        std::fs::remove_file(
            spread
                .0
                .join("config/cta_factor_spread/okex-futures_binance-futures_1min.json"),
        )
        .unwrap();
        std::fs::write(
            spread
                .0
                .join(format!("config/cta_factor_spread/{name}.json")),
            serde_json::to_vec(&combination).unwrap(),
        )
        .unwrap();
        let settings = Settings {
            env_root: prep.0.clone(),
            factor_env: prep.0.clone(),
            spread_factor_env: spread.0.clone(),
            jp_database: None,
        };
        write_worker_runtime(&prep.0, Worker::Perp);
        write_worker_runtime(&spread.0, Worker::Spread);
        let risk = super::super::RiskBook::default().query(&Default::default());
        let response = load(&settings, &risk, &ListingIndex::default()).await;
        assert!(response.errors.is_empty());
        assert_eq!(response.factor_processes.len(), 2);
        assert!(response.factor_processes[0]
            .config_url
            .contains("worker=cta-prep-binance-futures"));
        assert!(response.factor_processes[1]
            .config_url
            .contains("worker=cta-factor-spread"));
        assert!(response.factor_processes.iter().all(|p| !p.stale));
        assert!(
            response.factor_processes[1].configured,
            "{:?}",
            response.factor_processes[1].errors
        );
        let row = response
            .strategies
            .iter()
            .find(|s| s.kind == "spread")
            .unwrap();
        assert_eq!(row.kind, "spread");
        assert_eq!(row.factor_worker, "cta-factor-spread");
        // Two formula inputs also require one cross-market spread per symbol.
        assert_eq!(row.factor_count, Some(4));
        assert!(row
            .risk
            .config_url
            .as_ref()
            .unwrap()
            .contains("worker=cta-factor-spread#"));
        assert!(row.risk.symbols.contains(&"UNLISTEDUSDT".into()));
        assert!(row.errors.is_empty(), "{:?}", row.errors);
        assert!(response
            .strategies
            .iter()
            .all(|s| !s.slug.starts_with("binance-cta-perp-")));
        assert_eq!(row.risk_by_venue.len(), 2);
        assert_eq!(row.risk_by_venue[0].venues, ["okex-futures"]);
        assert_eq!(row.risk.tone, "uncovered");
        // Removing the deployed Spread catalog must retain its navigation and
        // the independent Prep snapshot rather than claim zero active factors.
        std::fs::remove_file(spread.0.join("config/cta_factor_spread.toml")).unwrap();
        let response = load(&settings, &risk, &ListingIndex::default()).await;
        assert!(response.factor_processes[0].configured);
        assert!(!response.factor_processes[1].configured);
        assert!(response.factor_processes[1]
            .config_url
            .contains("worker=cta-factor-spread"));
    }

    fn signal() -> Signal {
        Signal {
            slug: "binance-cta-perp-rx01".into(),
            combination_name: "binance-futures_15min".into(),
            worker: Worker::Perp,
            venues: vec!["binance-futures".into()],
            enabled: true,
            bar_interval: "15m".into(),
            updated_ms: Utc::now().timestamp_millis(),
            stale: false,
            names: BTreeSet::from(["rx01_a".into(), "rx01_b".into()]),
            symbols: BTreeSet::from(["BTCUSDT".into()]),
            publishes_to_jp: true,
        }
    }
    fn source(id: &str, gateway: &str) -> Source {
        Source {
            id: id.into(),
            alias: Some(id.into()),
            gateway_prefix: Some(gateway.into()),
            enabled: true,
        }
    }
    fn binding(id: &str, strategy: &str, shares: f64) -> Binding {
        Binding {
            source: id.into(),
            binding_name: strategy.into(),
            strategy: strategy.into(),
            order_strategy: "order".into(),
            shares,
            source_enabled: true,
            alias: id.into(),
        }
    }

    #[tokio::test]
    async fn current_groups_replace_retired_signals_and_use_the_worker_heartbeat() {
        let env = cta_factors::test_environment();
        for (rx, interval) in [("rx01", "15m"), ("rx03", "1m"), ("rx04", "5m")] {
            let run = env.0.join(format!("binance-cta-perp-{rx}/run"));
            std::fs::create_dir_all(&run).unwrap();
            let state = serde_json::json!({
                "updated_ts_us":Utc::now().timestamp_micros(), "enabled":true,
                "bar_interval":interval, "manager_strategy_name":format!("cta-prep-bn-{}-pos",interval.replace('m',"min")),
                "strategies":{"symbol_mean":{"UNLISTEDUSDT":{}}}, "symbol_factors":{},
                "manager_publications":[{"manager_url":"http://127.0.0.1:4191/manager"}]
            });
            std::fs::write(
                run.join("cta_perp_status.json"),
                serde_json::to_vec(&state).unwrap(),
            )
            .unwrap();
        }
        let settings = Settings {
            env_root: env.0.clone(),
            factor_env: env.0.clone(),
            spread_factor_env: env.0.join("spread"),
            jp_database: None,
        };
        write_worker_runtime(&env.0, Worker::Perp);
        let risk = super::super::RiskBook::default()
            .query(&mkt_signal::common::delist_risk::RiskQuery::default());
        let response = load(&settings, &risk, &ListingIndex::default()).await;
        assert_eq!(response.strategies.len(), 13);
        assert_eq!(response.factor_processes[0].unique_factor_count, 8);
        assert!(response.factor_processes[0]
            .combinations
            .iter()
            .all(|c| c.position_strategies.len() == 1));
        for row in response.strategies {
            assert!(!row.stale);
            assert_eq!(row.factor_worker, "cta-prep-binance-futures");
            assert!(row.factor_count.is_some());
            assert!(row.risk.symbols.contains(&"UNLISTEDUSDT".into()));
            assert!(
                row.slug.starts_with("cta-prep01-")
                    || ["top-5", "top-20", "last-20"]
                        .iter()
                        .any(|group| row.slug.starts_with(&format!("cta-{group}-prep02-")))
            );
            assert!(row.errors.is_empty(), "{:?}", row.errors);
        }
    }
    #[test]
    fn missing_or_wrong_runtime_tables_cannot_claim_current_strategy_health() {
        let env = cta_factors::test_environment();
        write_worker_runtime(&env.0, Worker::Perp);
        let mut snapshot = cta_factors::snapshot(&env.0, Worker::Perp).unwrap();
        snapshot.runtime.as_mut().unwrap()["position_tables"]["tables"][0]["strategy_name"] =
            serde_json::json!("retired_strategy");
        let rows = current_worker_signals(&snapshot, Worker::Perp).unwrap();
        assert_eq!(rows.iter().filter(|(s, _)| s.stale).count(), 1);
        assert!(rows
            .iter()
            .any(|(_, errors)| errors.iter().any(|s| s.contains("尚未生成"))));
        snapshot.runtime.as_mut().unwrap()["updated_ms"] =
            serde_json::json!(Utc::now().timestamp_millis() - 181_000);
        assert!(current_worker_signals(&snapshot, Worker::Perp)
            .unwrap()
            .iter()
            .all(|(s, _)| s.stale));
        let runtime = snapshot.runtime.as_mut().unwrap();
        let duplicate = runtime["position_tables"]["tables"][1].clone();
        runtime["position_tables"]["tables"]
            .as_array_mut()
            .unwrap()
            .push(duplicate);
        assert!(current_worker_signals(&snapshot, Worker::Perp).is_err());
    }

    #[test]
    fn maps_multiple_real_accounts_and_detects_unsynchronized_follow() {
        let data = ManagerData {
            site: "jp",
            sources: BTreeMap::from([
                ("exec_a".into(), source("exec_a", "/exec_trade01")),
                ("exec_b".into(), source("exec_b", "/exec_trade02")),
            ]),
            bindings: vec![
                binding("exec_a", "rx01_a", 1.0),
                binding("exec_b", "rx01_b", 0.0),
                binding("other", "different_strategy", 1.0),
            ],
            follows: vec![Follow {
                source: "exec_b".into(),
                virtual_name: "rx01".into(),
                binding_name: "rx01_b".into(),
                strategy: "rx01_b".into(),
                order_strategy: "order".into(),
                shares: 1.0,
                multiplier: 1.0,
            }],
        };
        let rows = account_views(&signal(), &data);
        assert_eq!(rows.len(), 2);
        assert!(rows[0].active && !rows[0].pending);
        assert_eq!(rows[0].viz_url.as_deref(), Some("/exec_trade01/"));
        assert!(!rows[1].active && rows[1].pending && rows[1].configured);
        assert_eq!(rows[1].virtual_accounts, vec!["rx01"]);
    }
    #[test]
    fn disabled_or_unknown_sources_do_not_claim_active_and_bad_urls_are_rejected() {
        let mut disabled = source("disabled", "/exec_trade01");
        disabled.enabled = false;
        let data = ManagerData {
            site: "jp",
            sources: BTreeMap::from([
                ("disabled".into(), disabled),
                ("unsafe".into(), source("unsafe", "//evil.example")),
            ]),
            bindings: vec![
                binding("disabled", "rx01_a", 1.0),
                binding("unsafe", "rx01_b", 1.0),
                binding("missing", "rx01_a", 1.0),
            ],
            follows: vec![],
        };
        let rows = account_views(&signal(), &data);
        assert_eq!(rows.len(), 3);
        assert!(!rows[0].active && rows[0].viz_url.is_none());
        assert!(!rows[1].active && rows[1].viz_url.is_none());
        assert!(rows[2].active && rows[2].viz_url.is_none());
        assert_eq!(
            safe_gateway(Some("/exec_trade02")).as_deref(),
            Some("/exec_trade02/")
        );
    }

    #[test]
    fn detects_changed_follower_multiplier_even_when_both_shares_are_positive() {
        let data = ManagerData {
            site: "jp",
            sources: BTreeMap::from([("exec_a".into(), source("exec_a", "/exec_trade01"))]),
            bindings: vec![binding("exec_a", "rx01_a", 1.0)],
            follows: vec![Follow {
                source: "exec_a".into(),
                virtual_name: "rx01".into(),
                binding_name: "rx01_a".into(),
                strategy: "rx01_a".into(),
                order_strategy: "order".into(),
                shares: 1.0,
                multiplier: 2.0,
            }],
        };
        let rows = account_views(&signal(), &data);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].active && rows[0].pending);
    }
}
