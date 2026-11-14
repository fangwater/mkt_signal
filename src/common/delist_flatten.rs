//! Final-24-hour FR delist flatten decisions and fixed-script execution.

use anyhow::{bail, Context, Result};
use chrono::DateTime;
use serde::Serialize;
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::Command;
use tokio::time;

use crate::common::delist_accounts::{AccountSpec, RedisSite};
use crate::common::delist_dump::{PositionDumpCandidate, POSITION_REMOVAL_THRESHOLD_USDT};
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
    sg_ssh_host: Option<String>,
}

impl FlattenExecutor {
    pub fn new(env_root: PathBuf, timeout: Duration) -> Self {
        Self {
            env_root,
            timeout,
            sg_ssh_host: None,
        }
    }

    pub fn with_sg_ssh_host(mut self, host: Option<String>) -> Result<Self> {
        if let Some(host) = &host {
            if host.is_empty()
                || host.starts_with('-')
                || !host
                    .bytes()
                    .all(|ch| ch.is_ascii_alphanumeric() || b".-_@".contains(&ch))
            {
                bail!("invalid SG flatten SSH host");
            }
        }
        self.sg_ssh_host = host;
        Ok(self)
    }

    pub async fn run(
        &self,
        candidate: &FlattenCandidate,
        account: &AccountSpec,
    ) -> Result<FlattenRunOutput> {
        let display = self.command(candidate, account)?;
        let mut command = Command::new(&display[0]);
        if account.site == RedisSite::Jp {
            let (cwd, script, _) = self.command_parts(candidate, account)?;
            if !cwd.is_dir() {
                bail!("flatten account directory is missing: {}", cwd.display());
            }
            if !script.is_file() {
                bail!("flatten script is missing: {}", script.display());
            }
            command.current_dir(cwd);
        }
        command
            .args(&display[1..])
            .env_remove("BINANCE_API_KEY")
            .env_remove("BINANCE_API_SECRET")
            .env_remove("BITGET_API_KEY")
            .env_remove("BITGET_API_SECRET")
            .env_remove("BITGET_PASSPHRASE")
            .env_remove("BITGET_API_PASSPHRASE")
            .env_remove("GATE_API_KEY")
            .env_remove("GATE_API_SECRET")
            .env_remove("BYBIT_API_KEY")
            .env_remove("BYBIT_API_SECRET")
            .env("PYTHONUNBUFFERED", "1")
            .kill_on_drop(true);

        // The remote timeout also terminates the Python process if SSH disconnects.
        // Allow SSH connection setup and timeout's TERM/KILL grace locally.
        let wait = self.timeout
            + if account.site == RedisSite::Sg {
                Duration::from_secs(20)
            } else {
                Duration::ZERO
            };
        let output = time::timeout(wait, command.output())
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
        Ok(FlattenRunOutput {
            command: display,
            exit_code: output.status.code(),
            stdout: truncate_output(&output.stdout),
            stderr: truncate_output(&output.stderr),
            success: output.status.success(),
        })
    }

    pub fn command(
        &self,
        candidate: &FlattenCandidate,
        account: &AccountSpec,
    ) -> Result<Vec<String>> {
        let (cwd, script, args) = self.command_parts(candidate, account)?;
        if account.site == RedisSite::Sg {
            let host = self
                .sg_ssh_host
                .as_ref()
                .context("SG flatten SSH host is not configured")?;
            let remote = format!(
                "cd -- {} && exec timeout --signal=TERM --kill-after=5s {}s python3 {} {}",
                shell_quote(&cwd.to_string_lossy()),
                self.timeout.as_secs().max(1),
                shell_quote(&script.to_string_lossy()),
                args.iter()
                    .map(|arg| shell_quote(arg))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
            return Ok(vec![
                "ssh".into(),
                "-T".into(),
                "-o".into(),
                "BatchMode=yes".into(),
                "-o".into(),
                "ConnectTimeout=10".into(),
                "--".into(),
                host.clone(),
                remote,
            ]);
        }
        let mut display = vec!["python3".to_string(), script.display().to_string()];
        display.extend(args);
        Ok(display)
    }

    fn command_parts(
        &self,
        candidate: &FlattenCandidate,
        spec: &AccountSpec,
    ) -> Result<(PathBuf, PathBuf, Vec<String>)> {
        let symbol = validate_symbol(&candidate.symbol)?;
        if spec.slug != candidate.account_slug
            || spec.kind != "funding_rate"
            || spec.exchange != candidate.exchange
            || !spec.slug.starts_with(&format!("{}_fr_", spec.exchange))
            || !spec
                .slug
                .bytes()
                .all(|ch| ch.is_ascii_alphanumeric() || b"_-".contains(&ch))
            || !matches!(
                (spec.site, spec.exchange.as_str()),
                (RedisSite::Jp, "binance" | "bitget" | "gate") | (RedisSite::Sg, "bybit")
            )
        {
            bail!("flatten account/exchange is not supported");
        }
        let cwd = self.env_root.join(&spec.slug);
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

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
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
        if !position_usdt.is_finite() || position_usdt < POSITION_REMOVAL_THRESHOLD_USDT {
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

    #[test]
    fn excludes_dust_without_hiding_either_material_leg() {
        let now = DateTime::parse_from_rfc3339("2026-10-22T10:00:00Z")
            .unwrap()
            .timestamp_millis();
        for (open, hedge, expected) in [
            (0.0, 0.0, false),
            (0.0057, 0.0, false),
            (0.0, -0.0057, false),
            (0.999, -0.999, false),
            (1.0, 0.0, true),
            (0.0, -1.0, true),
            (5.0, -5.0, true),
        ] {
            let mut position = positioned(0.0, "2026-10-23T09:00:00Z");
            position.open_usdt = open;
            position.hedge_usdt = hedge;
            let candidates = flatten_candidates(&[position], now, 86_400_000, 1_000.0);
            assert_eq!(
                !candidates.is_empty(),
                expected,
                "open={open} hedge={hedge}"
            );
        }
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
        let account = AccountSpec {
            slug: "gate_fr_arb02".to_string(),
            alias: "gate".to_string(),
            exchange: "gate".to_string(),
            kind: "funding_rate".to_string(),
            host: "local".to_string(),
            site: RedisSite::Jp,
            config_url: String::new(),
        };
        let output = FlattenExecutor::new(root.clone(), Duration::from_secs(5))
            .run(&candidate, &account)
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
    fn sg_bybit_uses_remote_env_with_bounded_execution() {
        let mut candidate = flatten_candidates(
            &[positioned(500.0, "2026-10-23T09:00:00Z")],
            DateTime::parse_from_rfc3339("2026-10-23T08:00:00Z")
                .unwrap()
                .timestamp_millis(),
            86_400_000,
            1_000.0,
        )
        .remove(0);
        candidate.account_slug = "bybit_fr_arb01".into();
        candidate.exchange = "bybit".into();
        let mut account = AccountSpec {
            slug: candidate.account_slug.clone(),
            alias: "Bybit".into(),
            exchange: "bybit".into(),
            kind: "funding_rate".into(),
            host: "sg".into(),
            site: RedisSite::Sg,
            config_url: String::new(),
        };
        let executor = FlattenExecutor::new("/home/ubuntu".into(), Duration::from_secs(300));
        assert!(executor
            .command(&candidate, &account)
            .unwrap_err()
            .to_string()
            .contains("not configured"));
        assert!(executor
            .clone()
            .with_sg_ssh_host(Some("sg;false".into()))
            .is_err());
        let executor = executor.with_sg_ssh_host(Some("sg".into())).unwrap();
        let command = executor.command(&candidate, &account).unwrap();
        assert_eq!(command[0], "ssh");
        assert_eq!(command[7], "sg");
        assert_eq!(command[8], "cd -- '/home/ubuntu/bybit_fr_arb01' && exec timeout --signal=TERM --kill-after=5s 300s python3 '/home/ubuntu/bybit_fr_arb01/scripts/flatten_bybit_pm.py' '--symbols' 'TSLAXUSDT' '--mode' 'clear' '--execute'");
        account.kind = "intra_exchange".into();
        assert!(executor.command(&candidate, &account).is_err());
        account.kind = "funding_rate".into();
        account.site = RedisSite::Jp;
        assert!(executor.command(&candidate, &account).is_err());
        account.site = RedisSite::Sg;
        account.slug = "bybit_fr_../../other".into();
        candidate.account_slug = account.slug.clone();
        assert!(executor.command(&candidate, &account).is_err());
    }

    #[test]
    fn rejects_non_symbol_input() {
        assert!(validate_symbol("TSLAXUSDT;rm").is_err());
        assert!(validate_symbol("../TSLAXUSDT").is_err());
        assert_eq!(validate_symbol("TSLAXUSDT").unwrap(), "TSLAXUSDT");
    }
}
