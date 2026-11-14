//! FR returns are read from NAV's PnL endpoint; no independent PnL calculation.
use super::*;

const MAX_AGE_MS: i64 = 2 * 60 * 60 * 1_000;
const THRESHOLD: f64 = 0.10;
const SEVEN_DAYS_MS: i64 = 7 * 86_400_000;

#[derive(Clone, Default, Serialize)]
pub struct Report {
    pub checked_ms: i64,
    pub catalog_current: bool,
    pub accounts: Vec<Account>,
}

#[derive(Clone, Serialize)]
pub struct Account {
    pub slug: String,
    pub alias: String,
    pub annualized_return: Option<f64>,
    pub covered_ms: Option<i64>,
    pub annualized_return_30d: Option<f64>,
    pub covered_30d_ms: Option<i64>,
}

#[derive(Serialize)]
pub struct View {
    pub checked_ms: i64,
    pub catalog_current: bool,
    pub alerts: Vec<Account>,
    pub unavailable_count: usize,
}

impl Report {
    pub fn current(mut self, specs: &[AccountSpec], catalog_current: bool, now: i64) -> Self {
        let slugs: BTreeSet<_> = specs
            .iter()
            .filter(|s| s.kind == "funding_rate")
            .map(|s| s.slug.as_str())
            .collect();
        let reported: BTreeSet<_> = self.accounts.iter().map(|a| a.slug.as_str()).collect();
        self.catalog_current = self.catalog_current
            && catalog_current
            && !slugs.is_empty()
            && slugs == reported
            && self.checked_ms > 0
            && now.saturating_sub(self.checked_ms) <= MAX_AGE_MS;
        if !self.catalog_current {
            for account in &mut self.accounts {
                account.annualized_return = None;
                account.annualized_return_30d = None;
            }
        }
        self
    }

    pub fn view(&self) -> View {
        let mut alerts = Vec::new();
        let mut unavailable_count = 0;
        if self.catalog_current {
            for account in &self.accounts {
                match (account.covered_ms, account.annualized_return) {
                    (Some(covered), _) if covered < SEVEN_DAYS_MS => continue,
                    (Some(_), Some(value)) if value < THRESHOLD => alerts.push(account.clone()),
                    (Some(_), Some(_)) => {}
                    _ => unavailable_count += 1,
                }
            }
        }
        alerts.sort_by(|a, b| {
            a.annualized_return
                .unwrap_or_default()
                .total_cmp(&b.annualized_return.unwrap_or_default())
        });
        View {
            checked_ms: self.checked_ms,
            catalog_current: self.catalog_current,
            alerts,
            unavailable_count,
        }
    }

    pub fn summary(&self) -> String {
        if !self.catalog_current {
            return "FR 年化：数据或 FR 名单不可用/过期".into();
        }
        let view = self.view();
        let mut lines = Vec::new();
        if !view.alerts.is_empty() {
            lines.push(format!("⚠ {} 个 FR 近7日年化低于10%", view.alerts.len()));
            lines.push("账户 | 7日年化 | 30日年化".into());
        }
        for account in &view.alerts {
            lines.push(format!(
                "{} | {} | {}",
                account.slug,
                format_return(account.annualized_return, account.covered_ms, 7),
                format_return(account.annualized_return_30d, account.covered_30d_ms, 30)
            ));
        }
        if !view.alerts.is_empty() {
            lines.push("* 覆盖天数不足；-- 表示不可用".into());
        }
        if view.unavailable_count > 0 {
            lines.push(format!(
                "FR 年化数据不可用：{} 个账户",
                view.unavailable_count
            ));
        }
        lines.join("\n")
    }
}

fn format_return(value: Option<f64>, covered_ms: Option<i64>, days: i64) -> String {
    match value {
        Some(value) => format!(
            "{}{}",
            if days == 7 && (0.09995..0.1).contains(&value) {
                "<10.00%".to_string()
            } else {
                format!("{:.2}%", value * 100.0)
            },
            if covered_ms.unwrap_or(0) < days * 86_400_000 {
                "*"
            } else {
                ""
            }
        ),
        None => "--".into(),
    }
}

fn parse_window(value: &serde_json::Value) -> (Option<f64>, Option<i64>) {
    let covered = value["coveredMs"].as_i64().filter(|v| *v > 0);
    let annualized = value["annualizedReturn"].as_f64().filter(|v| v.is_finite());
    (annualized.filter(|_| covered.is_some()), covered)
}

async fn read_account(client: &Client, state: &AppState, account: &AccountSpec) -> Account {
    let mut result = Account {
        slug: account.slug.clone(),
        alias: account.alias.clone(),
        annualized_return: None,
        covered_ms: None,
        annualized_return_30d: None,
        covered_30d_ms: None,
    };
    let base = state.nav_strategies_url.trim_end_matches('/');
    // Slugs were validated by the NAV catalog loader.
    let url = format!("{base}/{}/pnl", account.slug);
    for attempt in 0..2 {
        let token = state.nav_session.read().await.clone();
        let mut request = client.get(&url).query(&[("maxPoints", "200")]);
        if let Some(token) = token {
            request = request.header(reqwest::header::COOKIE, format!("nav_session={token}"));
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(_) => break,
        };
        if response.status() == reqwest::StatusCode::UNAUTHORIZED && attempt == 0 {
            refresh_nav_account_catalog(state, client).await;
            continue;
        }
        if !response.status().is_success() {
            break;
        }
        if let Ok(value) = response.json::<serde_json::Value>().await {
            (result.annualized_return, result.covered_ms) =
                parse_window(&value["performance"]["rolling7d"]);
            (result.annualized_return_30d, result.covered_30d_ms) =
                parse_window(&value["performance"]["rolling30d"]);
        }
        break;
    }
    result
}

pub async fn refresh(state: &AppState, client: &Client) {
    let _guard = state.fr_yield_lock.lock().await;
    refresh_locked(state, client).await;
}

pub async fn ensure_current(state: &AppState, client: &Client) {
    let _guard = state.fr_yield_lock.lock().await;
    if !report(state).await.catalog_current {
        refresh_locked(state, client).await;
    }
}

async fn refresh_locked(state: &AppState, client: &Client) {
    let checked_ms = Utc::now().timestamp_millis();
    let specs = state.accounts.read().await.clone();
    let current = nav_accounts_ready(state).await;
    let mut accounts = Vec::new();
    for account in specs.iter().filter(|s| s.kind == "funding_rate") {
        accounts.push(read_account(client, state, account).await);
    }
    *state.fr_yield.write().await = Report {
        checked_ms,
        catalog_current: current,
        accounts,
    };
}

pub async fn report(state: &AppState) -> Report {
    let report = state.fr_yield.read().await.clone();
    let specs = state.accounts.read().await.clone();
    report.current(
        &specs,
        nav_accounts_ready(state).await,
        Utc::now().timestamp_millis(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn threshold_missing_and_partial_history_are_explicit() {
        let report = Report {
            checked_ms: 1,
            catalog_current: true,
            accounts: vec![
                Account {
                    slug: "low".into(),
                    alias: "low".into(),
                    annualized_return: Some(0.099),
                    covered_ms: Some(7 * 86_400_000),
                    annualized_return_30d: Some(0.2),
                    covered_30d_ms: Some(30 * 86_400_000),
                },
                Account {
                    slug: "building".into(),
                    alias: "building".into(),
                    annualized_return: Some(-0.5),
                    covered_ms: Some(86_400_000),
                    annualized_return_30d: Some(-0.5),
                    covered_30d_ms: Some(86_400_000),
                },
                Account {
                    slug: "equal".into(),
                    alias: "equal".into(),
                    annualized_return: Some(0.1),
                    covered_ms: Some(7 * 86_400_000),
                    annualized_return_30d: Some(0.03),
                    covered_30d_ms: Some(7 * 86_400_000),
                },
                Account {
                    slug: "missing".into(),
                    alias: "missing".into(),
                    annualized_return: None,
                    covered_ms: None,
                    annualized_return_30d: None,
                    covered_30d_ms: None,
                },
            ],
        };
        let text = report.summary();
        assert!(text.contains("low | 9.90% | 20.00%"));
        assert!(!text.contains("equal"));
        assert!(!text.contains("building"));
        assert!(!text.contains("missing |"));
        assert!(text.contains("数据不可用：1 个账户"));
        assert_eq!(report.view().alerts.len(), 1);
    }
    #[test]
    fn stale_or_changed_catalog_cannot_be_reported_as_healthy() {
        let report = Report {
            checked_ms: 1,
            catalog_current: true,
            accounts: vec![],
        }
        .current(&[], true, MAX_AGE_MS + 2);
        assert!(!report.catalog_current);
        assert!(report.summary().contains("不可用/过期"));
    }

    #[test]
    fn no_alert_or_table_when_every_account_is_healthy_or_under_seven_days() {
        let report = Report {
            checked_ms: 1,
            catalog_current: true,
            accounts: vec![
                Account {
                    slug: "healthy".into(),
                    alias: "healthy".into(),
                    annualized_return: Some(0.10),
                    covered_ms: Some(SEVEN_DAYS_MS),
                    annualized_return_30d: None,
                    covered_30d_ms: None,
                },
                Account {
                    slug: "building".into(),
                    alias: "building".into(),
                    annualized_return: Some(-0.2),
                    covered_ms: Some(SEVEN_DAYS_MS - 1),
                    annualized_return_30d: None,
                    covered_30d_ms: None,
                },
            ],
        };
        assert!(report.summary().is_empty());
        let view = report.view();
        assert!(view.alerts.is_empty());
        assert_eq!(view.unavailable_count, 0);
    }

    #[test]
    fn window_requires_valid_coverage_independently() {
        assert_eq!(
            parse_window(&serde_json::json!({"annualizedReturn": 0.2, "coveredMs": 10})),
            (Some(0.2), Some(10))
        );
        assert_eq!(
            parse_window(&serde_json::json!({"annualizedReturn": 0.2, "coveredMs": 0})),
            (None, None)
        );
        assert_eq!(parse_window(&serde_json::Value::Null), (None, None));
    }
}
