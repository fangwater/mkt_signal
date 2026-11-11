//! FR returns are read from NAV's PnL endpoint; no independent PnL calculation.
use super::*;

const MAX_AGE_MS: i64 = 2 * 60 * 60 * 1_000;
const THRESHOLD: f64 = 0.10;

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
            }
        }
        self
    }

    pub fn summary(&self) -> String {
        let low: Vec<_> = self
            .accounts
            .iter()
            .filter_map(|a| {
                a.annualized_return.filter(|v| *v < THRESHOLD).map(|v| {
                    format!(
                        "{} ({:.2}%){}",
                        a.slug,
                        v * 100.0,
                        if a.covered_ms.unwrap_or(0) < 7 * 86_400_000 {
                            " [不足7天]"
                        } else {
                            ""
                        }
                    )
                })
            })
            .collect();
        let unknown: Vec<_> = self
            .accounts
            .iter()
            .filter(|a| a.annualized_return.is_none())
            .map(|a| a.slug.as_str())
            .collect();
        let mut lines = Vec::new();
        if !low.is_empty() {
            lines.push(format!("⚠ 近7日年化低于10%：{}", low.join("、")));
        }
        if !self.catalog_current {
            lines.push("近7日年化：数据或 FR 名单不可用/过期".into());
        } else if !unknown.is_empty() {
            lines.push(format!("近7日年化不可用：{}", unknown.join("、")));
        }
        if lines.is_empty() {
            lines.push("近7日年化：所有 FR 均不低于10%".into());
        }
        lines.join("\n")
    }
}

async fn read_account(client: &Client, state: &AppState, account: &AccountSpec) -> Account {
    let mut result = Account {
        slug: account.slug.clone(),
        alias: account.alias.clone(),
        annualized_return: None,
        covered_ms: None,
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
            let rolling = &value["performance"]["rolling7d"];
            result.annualized_return = rolling["annualizedReturn"]
                .as_f64()
                .filter(|v| v.is_finite());
            result.covered_ms = rolling["coveredMs"].as_i64().filter(|v| *v > 0);
            if result.covered_ms.is_none() {
                result.annualized_return = None;
            }
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
                    covered_ms: Some(86_400_000),
                },
                Account {
                    slug: "equal".into(),
                    alias: "equal".into(),
                    annualized_return: Some(0.1),
                    covered_ms: Some(7 * 86_400_000),
                },
                Account {
                    slug: "missing".into(),
                    alias: "missing".into(),
                    annualized_return: None,
                    covered_ms: None,
                },
            ],
        };
        let text = report.summary();
        assert!(text.contains("low (9.90%) [不足7天]"));
        assert!(!text.contains("equal"));
        assert!(text.contains("不可用：missing"));
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
}
