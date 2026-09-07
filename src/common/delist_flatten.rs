//! Final-24-hour FR delist flatten decisions and fixed-script execution.

use anyhow::{bail, Context, Result};
use chrono::DateTime;
use serde::Serialize;
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::Command;
use tokio::time;

use crate::common::delist_accounts::{mounted_accounts, RedisSite};
use crate::common::delist_dump::PositionDumpCandidate;
use crate::common::delist_risk::normalize_symbol;

const OUTPUT_LIMIT: usize = 8_000;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct FlattenCandidate {
    pub account_slug: String,
    pub exchange: String,
    pub symbol: String,
    pub delist_utc: String,
    pub deadline_ms: i64,
    pub remaining_ms: i64,
    pub snapshot_ms: i64,
    pub open_usdt: f64,
    pub hedge_usdt: f64,
    pub position_usdt: f64,
    pub manual_threshold_usdt: f64,
    pub disposition: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FlattenRunOutput {
    pub command: Vec<String>,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub success: bool,
}

#[derive(Debug, Clone)]
pub struct FlattenExecutor {
    env_root: PathBuf,
    timeout: Duration,
}

impl FlattenExecutor {
    pub fn new(env_root: PathBuf, timeout: Duration) -> Self {
        Self { env_root, timeout }
    }

    pub async fn run(&self, candidate: &FlattenCandidate) -> Result<FlattenRunOutput> {
        let (cwd, script, args) = self.command_parts(candidate)?;
        if !cwd.is_dir() {
            bail!("flatten account directory is missing: {}", cwd.display());
        }
        if !script.is_file() {
            bail!("flatten script is missing: {}", script.display());
        }
        let mut command = Command::new("python3");
        command
            .current_dir(&cwd)
            .arg(&script)
            .args(&args)
            .env_remove("BINANCE_API_KEY")
            .env_remove("BINANCE_API_SECRET")
            .env_remove("BITGET_API_KEY")
            .env_remove("BITGET_API_SECRET")
            .env_remove("BITGET_PASSPHRASE")
            .env_remove("BITGET_API_PASSPHRASE")
            .env_remove("GATE_API_KEY")
            .env_remove("GATE_API_SECRET")
            .env("PYTHONUNBUFFERED", "1")
            .kill_on_drop(true);

        let output = time::timeout(self.timeout, command.output())
            .await
            .with_context(|| {
                format!(
                    "flatten timed out after {}s account={} symbol={}",
                    self.timeout.as_secs(),
                    candidate.account_slug,
                    candidate.symbol
                )
            })?
            .with_context(|| {
                format!(
                    "start flatten account={} symbol={}",
                    candidate.account_slug, candidate.symbol
                )
            })?;
        let mut display = vec!["python3".to_string(), script.display().to_string()];
        display.extend(args);
        Ok(FlattenRunOutput {
            command: display,
            exit_code: output.status.code(),
            stdout: truncate_output(&output.stdout),
            stderr: truncate_output(&output.stderr),
            success: output.status.success(),
        })
    }

    pub fn command(&self, candidate: &FlattenCandidate) -> Result<Vec<String>> {
        let (_cwd, script, args) = self.command_parts(candidate)?;
        let mut display = vec!["python3".to_string(), script.display().to_string()];
        display.extend(args);
        Ok(display)
    }

    fn command_parts(
        &self,
        candidate: &FlattenCandidate,
    ) -> Result<(PathBuf, PathBuf, Vec<String>)> {
        let symbol = validate_symbol(&candidate.symbol)?;
        let spec = mounted_accounts()
            .iter()
            .find(|spec| spec.slug == candidate.account_slug)
            .context("flatten account is not mounted")?;
        if spec.kind != "funding_rate"
            || !matches!(spec.site, RedisSite::Jp)
            || spec.exchange != candidate.exchange
            || !matches!(spec.exchange, "binance" | "bitget" | "gate")
        {
            bail!("flatten account/exchange is not supported");
        }
        let cwd = self.env_root.join(spec.slug);
        let script = cwd
            .join("scripts")
            .join(format!("flatten_{}_pm.py", spec.exchange));
        Ok((
            cwd,
            script,
            vec![
                "--symbols".to_string(),
                symbol,
                "--mode".to_string(),
                "clear".to_string(),
                "--execute".to_string(),
            ],
        ))
    }
}

pub fn flatten_candidates(
    positioned: &[PositionDumpCandidate],
    now_ms: i64,
    window_ms: i64,
    manual_threshold_usdt: f64,
) -> Vec<FlattenCandidate> {
    let mut out = Vec::new();
    for candidate in positioned {
        let Some(delist_utc) = candidate.delist_utc.as_deref() else {
            continue;
        };
        let Ok(deadline) = DateTime::parse_from_rfc3339(delist_utc) else {
            continue;
        };
        let deadline_ms = deadline.timestamp_millis();
        let remaining_ms = deadline_ms.saturating_sub(now_ms);
        if remaining_ms <= 0 || remaining_ms > window_ms.max(0) {
            continue;
        }
        let position_usdt = candidate.open_usdt.abs().max(candidate.hedge_usdt.abs());
        if !position_usdt.is_finite() || position_usdt <= 0.0 {
            continue;
        }
        out.push(FlattenCandidate {
            account_slug: candidate.account_slug.clone(),
            exchange: candidate.exchange.clone(),
            symbol: candidate.symbol.clone(),
            delist_utc: delist_utc.to_string(),
            deadline_ms,
            remaining_ms,
            snapshot_ms: candidate.snapshot_ms,
            open_usdt: candidate.open_usdt,
            hedge_usdt: candidate.hedge_usdt,
            position_usdt,
            manual_threshold_usdt,
            disposition: if position_usdt > manual_threshold_usdt {
                "manual"
            } else {
                "auto"
            }
            .to_string(),
        });
    }
    out.sort_by(|left, right| {
        left.deadline_ms
            .cmp(&right.deadline_ms)
            .then_with(|| left.account_slug.cmp(&right.account_slug))
            .then_with(|| left.symbol.cmp(&right.symbol))
    });
    out.dedup_by(|left, right| {
        left.account_slug == right.account_slug
            && left.symbol == right.symbol
            && left.deadline_ms == right.deadline_ms
    });
    out
}

pub fn audit_dedup_key(candidate: &FlattenCandidate, trigger: &str) -> String {
    format!(
        "{trigger}:{}:{}:{}",
        candidate.account_slug, candidate.symbol, candidate.deadline_ms
    )
}

fn validate_symbol(symbol: &str) -> Result<String> {
    let normalized = normalize_symbol(symbol);
    if normalized != symbol
        || normalized.len() > 32
        || !normalized.ends_with("USDT")
        || !normalized.chars().all(|char| char.is_ascii_alphanumeric())
    {
        bail!("invalid flatten symbol");
    }
    Ok(normalized)
}

fn truncate_output(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    if text.chars().count() <= OUTPUT_LIMIT {
        text.into_owned()
    } else {
        text.chars().take(OUTPUT_LIMIT).collect::<String>() + "..."
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::delist_accounts::AccountHitEvent;

    fn positioned(position: f64, deadline: &str) -> PositionDumpCandidate {
        PositionDumpCandidate {
            account_slug: "gate_fr_arb02".to_string(),
            exchange: "gate".to_string(),
            redis_site: "jp".to_string(),
            symbol: "TSLAXUSDT".to_string(),
            event: AccountHitEvent {
                venue: "gate-futures".to_string(),
                action: "disable_open".to_string(),
                utc: "2026-09-08T23:30:00Z".to_string(),
                status: "upcoming".to_string(),
                listing: "listed".to_string(),
                title: "test".to_string(),
                url: String::new(),
            },
            snapshot_ms: 1,
            open_usdt: position,
            hedge_usdt: -position,
            impacted_position_usdt: position,
            threshold_usdt: 0.0,
            delist_utc: Some(deadline.to_string()),
        }
    }

    #[test]
    fn selects_only_future_final_24h_and_splits_threshold() {
        let now = DateTime::parse_from_rfc3339("2026-09-08T08:00:00Z")
            .unwrap()
            .timestamp_millis();
        let positioned = vec![
            positioned(999.0, "2026-09-09T07:59:00Z"),
            positioned(1_000.0, "2026-09-09T07:57:00Z"),
            positioned(1_001.0, "2026-09-09T07:58:00Z"),
            positioned(500.0, "2026-09-09T08:01:00Z"),
            positioned(500.0, "2026-09-08T07:59:00Z"),
        ];
        let candidates = flatten_candidates(&positioned, now, 86_400_000, 1_000.0);
        assert_eq!(candidates.len(), 3);
        assert_eq!(candidates[0].position_usdt, 1_000.0);
        assert_eq!(candidates[0].disposition, "auto");
        assert_eq!(candidates[1].disposition, "manual");
        assert_eq!(candidates[2].disposition, "auto");
    }

    #[tokio::test]
    async fn executor_uses_fixed_account_script_and_clear_mode() {
        let root = std::env::temp_dir().join(format!("delist-flatten-test-{}", std::process::id()));
        let scripts = root.join("gate_fr_arb02/scripts");
        std::fs::create_dir_all(&scripts).unwrap();
        std::fs::write(
            scripts.join("flatten_gate_pm.py"),
            "import json,sys; print(json.dumps(sys.argv[1:]))\n",
        )
        .unwrap();
        let candidate = flatten_candidates(
            &[positioned(500.0, "2026-09-09T07:00:00Z")],
            DateTime::parse_from_rfc3339("2026-09-08T08:00:00Z")
                .unwrap()
                .timestamp_millis(),
            86_400_000,
            1_000.0,
        )
        .remove(0);
        let output = FlattenExecutor::new(root.clone(), Duration::from_secs(5))
            .run(&candidate)
            .await
            .unwrap();
        assert!(output.success);
        assert!(output.stdout.contains("--symbols"));
        assert!(output.stdout.contains("TSLAXUSDT"));
        assert!(output.stdout.contains("--mode"));
        assert!(output.stdout.contains("clear"));
        assert!(output.stdout.contains("--execute"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_non_symbol_input() {
        assert!(validate_symbol("TSLAXUSDT;rm").is_err());
        assert!(validate_symbol("../TSLAXUSDT").is_err());
        assert_eq!(validate_symbol("TSLAXUSDT").unwrap(), "TSLAXUSDT");
    }
}
