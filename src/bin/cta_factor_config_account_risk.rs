//! Account-scoped Exec limits edited through the authenticated CTA page.
use super::*;
use redis::Commands;
use runtime_common::redis_client::RedisSettings;
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

pub struct Settings {
    pub catalog: PathBuf,
    pub sources: BTreeSet<String>,
    pub redis: RedisSettings,
}

#[derive(Clone, Deserialize)]
struct Account {
    id: String,
    #[serde(default)]
    alias: Option<String>,
    venue: String,
    #[serde(default = "enabled")]
    enabled: bool,
}
fn enabled() -> bool {
    true
}
#[derive(Deserialize)]
struct Catalog {
    sources: Vec<Account>,
}

#[derive(Serialize)]
pub struct Snapshot {
    source_id: String,
    alias: String,
    venue: String,
    max_leverage: Option<f64>,
    expected_value: Option<String>,
    error: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Save {
    source_id: String,
    max_leverage: f64,
    expected_value: Option<String>,
}

impl Settings {
    pub fn validate(&self) -> Result<()> {
        ensure!(!self.sources.is_empty(), "Exec risk account scope is empty");
        ensure!(
            self.sources.iter().all(|id| !id.is_empty()
                && id.len() <= 128
                && id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))),
            "invalid Exec risk source ID"
        );
        self.accounts()?;
        Ok(())
    }

    fn accounts(&self) -> Result<Vec<Account>> {
        let catalog: Catalog = toml::from_str(
            &std::fs::read_to_string(&self.catalog).context("read Exec account catalog")?,
        )
        .map_err(|_| anyhow::anyhow!("Exec account catalog format is invalid"))?;
        let mut selected = BTreeMap::new();
        for account in catalog.sources {
            if !self.sources.contains(&account.id) {
                continue;
            }
            ensure!(
                account.enabled,
                "Exec risk account {} is disabled",
                account.id
            );
            runtime_common::exec_market::parse_venue(&account.venue)?;
            ensure!(
                !selected.contains_key(&account.id),
                "duplicate Exec risk account"
            );
            selected.insert(account.id.clone(), account);
        }
        ensure!(
            selected.len() == self.sources.len(),
            "configured Exec risk account is missing from the account catalog"
        );
        Ok(selected.into_values().collect())
    }

    fn connection(&self) -> Result<redis::Connection> {
        let client = redis::Client::open(self.redis.connection_url())
            .map_err(|_| anyhow::anyhow!("invalid account risk Redis connection settings"))?;
        let conn = client
            .get_connection_with_timeout(Duration::from_secs(2))
            .map_err(|_| anyhow::anyhow!("account risk Redis connection failed"))?;
        conn.set_read_timeout(Some(Duration::from_secs(2)))?;
        conn.set_write_timeout(Some(Duration::from_secs(2)))?;
        Ok(conn)
    }

    pub fn read(&self) -> Result<Vec<Snapshot>> {
        let mut conn = self.connection()?;
        self.accounts()?
            .iter()
            .map(|account| snapshot(&mut conn, account))
            .collect()
    }

    fn save(
        &self,
        request: Save,
        headers: &HeaderMap,
        audit_dir: Option<&std::path::Path>,
    ) -> Result<Snapshot> {
        ensure!(
            request.max_leverage.is_finite() && request.max_leverage >= 0.0,
            "最大杠杆必须是大于等于 0 的有限数值，0 表示关闭"
        );
        let account = self
            .accounts()?
            .into_iter()
            .find(|a| a.id == request.source_id)
            .context("该执行账户不在可配置范围内")?;
        let key = risk_key(&account);
        let write = || -> Result<()> {
            let mut conn = self.connection()?;
            let result: i32 = redis::Script::new(
                r#"
                if redis.call('EXISTS',KEYS[1]) == 0 then return -1 end
                local current = redis.call('HGET',KEYS[1],'max_leverage')
                if ARGV[1] == '1' then
                    if current ~= ARGV[2] then return 0 end
                elseif current ~= false then return 0 end
                redis.call('HSET',KEYS[1],'max_leverage',ARGV[3])
                return 1
            "#,
            )
            .key(&key)
            .arg(if request.expected_value.is_some() {
                "1"
            } else {
                "0"
            })
            .arg(request.expected_value.as_deref().unwrap_or(""))
            .arg(request.max_leverage.to_string())
            .invoke(&mut conn)?;
            ensure!(result != -1, "账户风控参数不存在，未创建新的风险配置");
            if result == 0 {
                return Err(CatalogConflict("账户杠杆已被修改，请重新读取后保存").into());
            }
            Ok(())
        };
        if let Some(root) = audit_dir {
            parameter_audit::apply(
                root,
                request_metadata(headers, "exec", &account.id, "/api/account-risk"),
                "redis",
                || {
                    let mut conn = self.connection()?;
                    let values: BTreeMap<String, String> = conn.hgetall(&key)?;
                    ensure!(!values.is_empty(), "账户风控参数不存在");
                    Ok(BTreeMap::from([(
                        key.clone(),
                        parameter_audit::normalise_redis("hash", json!(values)),
                    )]))
                },
                write,
            )?;
        } else {
            write()?;
        }
        snapshot(&mut self.connection()?, &account)
    }
}

fn risk_key(account: &Account) -> String {
    format!("{}:{}:pre_trade_risk_params", account.id, account.venue)
}

fn snapshot(conn: &mut redis::Connection, account: &Account) -> Result<Snapshot> {
    let values: BTreeMap<String, String> = conn.hgetall(risk_key(account))?;
    ensure!(!values.is_empty(), "账户 {} 的风控参数不存在", account.id);
    let raw = values.get("max_leverage").cloned();
    let value = raw
        .as_deref()
        .map(|v| v.parse::<f64>().ok().filter(|v| v.is_finite() && *v >= 0.0))
        .unwrap_or(Some(0.0));
    Ok(Snapshot {
        source_id: account.id.clone(),
        alias: account.alias.clone().unwrap_or_else(|| account.id.clone()),
        venue: account.venue.clone(),
        max_leverage: value,
        expected_value: raw,
        error: value
            .is_none()
            .then(|| "当前 Redis 阈值无效，请修正后保存".into()),
    })
}

pub async fn read(State(app): State<Arc<App>>) -> ApiResult {
    tokio::task::spawn_blocking(move || -> Result<_> {
        match &app.account_risk {
            Some(settings) => Ok(json!({"accounts":settings.read()?})),
            None => Ok(json!({"accounts":[]})),
        }
    })
    .await
    .map_err(|e| error(e.into()))?
    .map(Json)
    .map_err(error)
}

pub async fn save(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(request): Json<Save>,
) -> ApiResult {
    same_origin(&headers).map_err(error)?;
    tokio::task::spawn_blocking(move || -> Result<_> {
        let settings = app.account_risk.as_ref().context("账户风控编辑尚未配置")?;
        Ok(json!({"account":settings.save(request,&headers,app.audit_dir.as_deref())?}))
    })
    .await
    .map_err(|e| error(e.into()))?
    .map(Json)
    .map_err(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        root: PathBuf,
        process: std::process::Child,
        settings: Settings,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = self.process.kill();
            let _ = self.process.wait();
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
    fn fixture() -> Fixture {
        let root = std::env::temp_dir().join(format!("exec-risk-ui-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let process = std::process::Command::new(
            std::env::var("TEST_REDIS_SERVER").unwrap_or_else(|_| "redis-server".into()),
        )
        .args([
            "--bind",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--save",
            "",
            "--appendonly",
            "no",
            "--dir",
            root.to_str().unwrap(),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
        let catalog = root.join("accounts.toml");
        std::fs::write(
            &catalog,
            r#"
            [[sources]]
            id="rx01"
            alias="RX01"
            venue="binance-futures"
            [[sources]]
            id="okx01"
            alias="OKX"
            venue="okex-futures"
            [[sources]]
            id="unselected"
            venue="binance-futures"
        "#,
        )
        .unwrap();
        let settings = Settings {
            catalog,
            sources: BTreeSet::from(["rx01".into(), "okx01".into()]),
            redis: RedisSettings {
                port,
                ..Default::default()
            },
        };
        let fixture = Fixture {
            root,
            process,
            settings,
        };
        for _ in 0..100 {
            if fixture.settings.connection().is_ok() {
                return fixture;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("isolated test Redis failed to start");
    }

    #[test]
    fn account_risk_saves_preserve_each_exchange_other_limits_and_reject_stale_edits() {
        let f = fixture();
        f.settings.validate().unwrap();
        let mut conn = f.settings.connection().unwrap();
        let bn = "rx01:binance-futures:pre_trade_risk_params";
        let ok = "okx01:okex-futures:pre_trade_risk_params";
        let _: () = conn
            .hset_multiple(
                bn,
                &[("max_leverage", "1"), ("exec_order_rate_limit_10s", "300")],
            )
            .unwrap();
        let _: () = conn.hset(ok, "max_leverage", "2").unwrap();
        let _: () = conn.expire(bn, 60).unwrap();
        let rows = f.settings.read().unwrap();
        assert_eq!(rows.len(), 2);
        let headers = HeaderMap::from_iter([(
            "x-parameter-actor".parse().unwrap(),
            "fixture-user".parse().unwrap(),
        )]);
        let saved = f
            .settings
            .save(
                Save {
                    source_id: "rx01".into(),
                    max_leverage: 0.75,
                    expected_value: Some("1".into()),
                },
                &headers,
                Some(&f.root.join("audit")),
            )
            .unwrap();
        assert_eq!(saved.max_leverage, Some(0.75));
        assert_eq!(conn.hget::<_, _, String>(ok, "max_leverage").unwrap(), "2");
        assert_eq!(
            conn.hget::<_, _, String>(bn, "exec_order_rate_limit_10s")
                .unwrap(),
            "300"
        );
        assert!(conn.ttl::<_, i64>(bn).unwrap() > 0);
        let records = parameter_audit::read_batch(&f.root.join("audit"), 0).unwrap();
        assert_eq!(
            records.records[0].event.metadata.actor.as_deref(),
            Some("fixture-user")
        );
        assert_eq!(records.records[0].event.metadata.family, "exec");
        assert_eq!(records.records[0].event.objects.len(), 1);
        let _: () = conn.hset(bn, "max_leverage", "1.25").unwrap();
        let error = f
            .settings
            .save(
                Save {
                    source_id: "rx01".into(),
                    max_leverage: 2.0,
                    expected_value: Some("0.75".into()),
                },
                &headers,
                None,
            )
            .err()
            .unwrap();
        assert!(error.is::<CatalogConflict>());
        assert_eq!(
            conn.hget::<_, _, String>(bn, "max_leverage").unwrap(),
            "1.25"
        );
        for (id, value) in [("rx01", -1.0), ("rx01", f64::NAN), ("unselected", 1.0)] {
            assert!(f
                .settings
                .save(
                    Save {
                        source_id: id.into(),
                        max_leverage: value,
                        expected_value: None
                    },
                    &headers,
                    None
                )
                .is_err());
        }
        let _: () = conn.del(ok).unwrap();
        assert!(f
            .settings
            .save(
                Save {
                    source_id: "okx01".into(),
                    max_leverage: 1.0,
                    expected_value: None
                },
                &headers,
                None
            )
            .is_err());
        assert!(!conn.exists::<_, bool>(ok).unwrap());
    }

    #[test]
    fn account_risk_missing_disabled_and_invalid_values_remain_factual() {
        let f = fixture();
        let mut conn = f.settings.connection().unwrap();
        let accounts = f.settings.accounts().unwrap();
        let account = accounts.iter().find(|a| a.id == "rx01").unwrap();
        let key = risk_key(account);
        let _: () = conn.hset(&key, "exec_order_rate_limit_10s", "300").unwrap();
        assert_eq!(
            snapshot(&mut conn, account).unwrap().max_leverage,
            Some(0.0)
        );
        let saved = f
            .settings
            .save(
                Save {
                    source_id: "rx01".into(),
                    max_leverage: 1.0,
                    expected_value: None,
                },
                &HeaderMap::new(),
                None,
            )
            .unwrap();
        assert_eq!(saved.max_leverage, Some(1.0));
        let _: () = conn.hset(&key, "max_leverage", "NaN").unwrap();
        let invalid = snapshot(&mut conn, account).unwrap();
        assert!(invalid.max_leverage.is_none());
        assert!(invalid.error.is_some());
        let saved = f
            .settings
            .save(
                Save {
                    source_id: "rx01".into(),
                    max_leverage: 0.0,
                    expected_value: invalid.expected_value,
                },
                &HeaderMap::new(),
                None,
            )
            .unwrap();
        assert_eq!(saved.max_leverage, Some(0.0));
    }
}
