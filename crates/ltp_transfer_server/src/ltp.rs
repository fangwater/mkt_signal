use super::model::{
    decimal_cmp, scalar, Account, Balance, Catalog, Currency, Record, TransferRequest,
};
use anyhow::{bail, ensure, Context, Result};
use chrono::{TimeZone, Utc};
use reqwest::{Client, Method};
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use trade_engine::ltp_rest::sign_params;

#[derive(Clone)]
pub struct Ltp {
    pub http: Client,
    pub base: String,
    pub key: String,
    pub secret: String,
}

pub struct Reply {
    pub http_status: u16,
    pub body: Value,
}

impl Reply {
    pub fn success(&self) -> bool {
        (200..300).contains(&self.http_status)
            && matches!(
                self.body.get("code").and_then(Value::as_i64),
                Some(200 | 200000)
            )
    }

    pub fn data(self) -> Result<Value> {
        ensure!(self.success(), "{}", self.error());
        self.body.get("data").cloned().context("LTP 响应缺少 data")
    }

    pub fn error(&self) -> String {
        format!(
            "LTP HTTP {} / {}：{}",
            self.http_status,
            self.body
                .get("code")
                .map(Value::to_string)
                .unwrap_or_default(),
            self.body
                .get("message")
                .or_else(|| self.body.get("msg"))
                .and_then(Value::as_str)
                .unwrap_or("响应无效")
        )
    }
}

impl Ltp {
    pub async fn request(
        &self,
        method: Method,
        path: &str,
        params: BTreeMap<String, String>,
        body: Option<Value>,
    ) -> Result<Reply> {
        let nonce = Utc::now().timestamp().to_string();
        let signature = sign_params(&self.secret, &params, &nonce)?;
        let mut request = self
            .http
            .request(method, format!("{}{path}", self.base))
            .header("X-MBX-APIKEY", &self.key)
            .header("nonce", nonce)
            .header("ts", Utc::now().timestamp_micros().to_string())
            .header("signature", signature)
            .header("Content-Type", "application/json");
        request = if let Some(body) = body {
            request.json(&body)
        } else {
            request.query(&params)
        };
        let response = request
            .send()
            .await
            .with_context(|| format!("LTP 请求 {path} 未得到确定响应"))?;
        let http_status = response.status().as_u16();
        let body = response
            .json::<Value>()
            .await
            .context("无法解析 LTP 响应，结果可能尚未确定")?;
        Ok(Reply { http_status, body })
    }

    pub async fn get(&self, path: &str, params: BTreeMap<String, String>) -> Result<Value> {
        self.request(Method::GET, path, params, None).await?.data()
    }

    pub async fn submit(
        &self,
        request: &TransferRequest,
        id: &str,
        funding: (bool, bool),
    ) -> Result<Reply> {
        let body = request.ltp_request(id, funding)?;
        let params = body
            .as_object()
            .context("transfer request object")?
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    scalar(value).unwrap_or_else(|| value.to_string()),
                )
            })
            .collect();
        // No automatic retry: every transport failure must be resolved by a GET.
        self.request(Method::POST, "/api/v1/transfer/apply", params, Some(body))
            .await
    }

    pub async fn catalog(&self) -> Result<Catalog> {
        let (legacy, trading) = tokio::try_join!(
            self.get("/api/v1/tradeAccount/list", BTreeMap::new()),
            self.get(
                "/api/v2/account/assets/trading/account-view",
                params(&[("hideDust", "false"), ("page", "1"), ("pageSize", "1000")])
            )
        )?;
        let legacy = legacy.as_array().context("LTP 账户列表格式无效")?;
        let rows = trading
            .get("list")
            .and_then(Value::as_array)
            .context("LTP Trading 账户列表格式无效")?;
        let total = trading
            .get("totalSize")
            .and_then(Value::as_u64)
            .context("LTP 账户目录缺少 totalSize")?;
        ensure!(
            total as usize == rows.len(),
            "账户目录超过单页容量，暂不允许使用不完整目录划转"
        );
        let mut accounts = Vec::new();
        let mut seen = BTreeSet::new();
        let mut warnings = Vec::new();
        for row in rows {
            if row.get("status").and_then(Value::as_str) != Some("ACTIVE") {
                continue;
            }
            let id = field(row, "id")?;
            ensure!(seen.insert(id.clone()), "LTP 账户目录含重复 ID");
            let kind = match row.get("primeType").and_then(Value::as_str) {
                Some("dma") => "dma",
                Some("rapidx") => "portfolio",
                _ => bail!("未知 LTP 账户类别"),
            };
            let venue = row
                .get("venue")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_uppercase();
            let mode = row.get("accountMode").and_then(Value::as_str).unwrap_or("");
            let types: Vec<&str> = if kind == "portfolio" {
                vec!["BINANCE", "OKX", "EDX"]
            } else {
                match (venue.as_str(), mode) {
                    ("BINANCE", "CLASSIC") => vec![
                        "SPOT",
                        "MARGIN_CROSS",
                        "U_MARGINED_FUTURES",
                        "C_MARGINED_FUTURES",
                    ],
                    ("BINANCE", "PM_1" | "PM_2" | "PM_3") => vec!["SPOT", "MARGIN_CROSS"],
                    ("OKX", "1" | "2" | "3" | "4") => vec!["FUNDING", "TRADING"],
                    _ => {
                        warnings.push(format!("{venue} {id} 的钱包模式 {mode} 尚未接入划转"));
                        Vec::new()
                    }
                }
            };
            let name = ["accountName", "accountNote"]
                .iter()
                .find_map(|key| {
                    row.get(key)
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                })
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    format!(
                        "{} {id}",
                        if kind == "portfolio" {
                            "RapidX"
                        } else {
                            &venue
                        }
                    )
                });
            accounts.push(Account {
                id,
                name,
                kind: kind.into(),
                venue,
                account_types: types.into_iter().map(str::to_owned).collect(),
            });
        }
        for row in legacy {
            if row.get("label").and_then(Value::as_str) == Some("Funding Account") {
                let id = field(row, "accountId")?;
                ensure!(seen.insert(id.clone()), "Funding 账户 ID 重复");
                accounts.push(Account {
                    id,
                    name: "LTP Funding Account".into(),
                    kind: "funding".into(),
                    venue: "LTP".into(),
                    account_types: vec!["LTP_FUNDING".into()],
                });
            }
        }
        let mut currencies = Vec::new();
        if let Some(account) = accounts.iter().find(|a| !a.account_types.is_empty()) {
            match self
                .currency_metadata(
                    Some(&route_name(account, &account.account_types[0])),
                    None,
                    None,
                )
                .await
            {
                Ok(value) => currencies = currency_list(&value, false)?,
                Err(error) => warnings.push(format!("币种目录暂不可用：{error}")),
            }
        }
        Ok(Catalog {
            accounts,
            currencies,
            warnings,
            updated_at: Utc::now(),
        })
    }

    pub async fn balances(&self, account: &Account, account_type: &str) -> Result<Vec<Balance>> {
        if account.kind == "portfolio" {
            let data = self.get("/api/v1/user/asset", BTreeMap::new()).await?;
            let rows = data
                .get(&account.id)
                .and_then(Value::as_array)
                .context("LTP 未返回此 Portfolio 的资产，不能将其视为零余额")?;
            return rows
                .iter()
                .filter(|row| row.get("exchangeType").and_then(Value::as_str) == Some(account_type))
                .map(|row| {
                    Ok(Balance {
                        currency: field(row, "coin")?,
                        total: field(row, "balance")?,
                        available: nonnegative(field(row, "maxTransferable")?)?,
                        network: None,
                    })
                })
                .collect();
        }
        let data = self
            .get(
                "/api/v1/asset/getAccountCurrencyInfo",
                params(&[("accountId", &account.id)]),
            )
            .await?;
        let rows = data.as_array().context("LTP 余额响应格式无效")?;
        let mut balances = Vec::new();
        let mut matched = false;
        for row in rows {
            if field(row, "accountId")? != account.id {
                bail!("LTP 余额响应含其他账户");
            }
            if account.kind != "funding"
                && wallet_type(
                    &account.venue,
                    row.get("accountType").and_then(Value::as_str).unwrap_or(""),
                ) != Some(account_type)
            {
                continue;
            }
            matched = true;
            for asset in row
                .get("assetInfo")
                .and_then(Value::as_array)
                .context("LTP 余额缺少 assetInfo")?
            {
                let available = nonnegative(field(asset, "available")?)?;
                let available = match asset.get("withdrawAvailable").and_then(scalar) {
                    Some(value) => {
                        let value = nonnegative(value)?;
                        if decimal_cmp(&value, &available)? == Ordering::Less {
                            value
                        } else {
                            available
                        }
                    }
                    None => available,
                };
                balances.push(Balance {
                    currency: field(asset, "currency")?,
                    total: field(asset, "totalBalance")?,
                    available,
                    network: asset
                        .get("network")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                        .map(str::to_owned),
                });
            }
        }
        ensure!(matched, "LTP 未返回所选钱包，不能将其视为零余额");
        Ok(balances)
    }

    pub async fn currency_metadata(
        &self,
        from: Option<&str>,
        to: Option<&str>,
        currency: Option<&str>,
    ) -> Result<Value> {
        let mut values = BTreeMap::new();
        for (key, value) in [
            ("fromAccount", from),
            ("toAccount", to),
            ("currency", currency),
        ] {
            if let Some(value) = value {
                values.insert(key.into(), value.into());
            }
        }
        self.get("/api/v1/asset/currencies", values).await
    }

    pub async fn route_metadata(
        &self,
        from: &Account,
        from_type: &str,
        to: &Account,
        to_type: &str,
        currency: Option<&str>,
    ) -> Result<Value> {
        let from_name = route_name(from, from_type);
        let to_name = route_name(to, to_type);
        // With both account filters this is an outgoing route query. LTP
        // reports its availability through withdrawEnable; depositEnable is
        // for incoming queries and does not gate this transfer direction.
        let metadata = self
            .currency_metadata(Some(&from_name), Some(&to_name), currency)
            .await?;
        outgoing_networks(&metadata)
    }

    pub async fn query_record(&self, record: &Record) -> Result<Record> {
        let data = self
            .get(
                "/api/v1/transfer/get",
                params(&[("clientOrderId", &record.client_order_id)]),
            )
            .await?;
        let mut refreshed = record.clone();
        ensure!(
            field(&data, "clientOrderId")? == record.client_order_id,
            "LTP 查询返回了其他请求的结果"
        );
        for (key, expected) in [
            ("fromTradeAccountId", &record.request.from_account_id),
            ("toTradeAccountId", &record.request.to_account_id),
            ("currency", &record.request.currency),
        ] {
            ensure!(
                field(&data, key)? == *expected,
                "LTP 查询的 {key} 与原请求不一致"
            );
        }
        ensure!(
            decimal_cmp(&field(&data, "amount")?, &record.request.amount)? == Ordering::Equal,
            "LTP 查询金额与原请求不一致"
        );
        apply_result(&mut refreshed, &data)?;
        Ok(refreshed)
    }

    pub async fn history(&self, page: u32, page_size: u32) -> Result<Vec<Record>> {
        let data = self
            .get(
                "/api/v1/transfer/list",
                params(&[
                    ("page", &page.to_string()),
                    ("pageSize", &page_size.to_string()),
                ]),
            )
            .await?;
        data.as_array()
            .context("LTP 划转历史格式无效")?
            .iter()
            .map(remote_record)
            .collect()
    }
}

pub fn params(items: &[(&str, &str)]) -> BTreeMap<String, String> {
    items
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

pub fn field(value: &Value, name: &str) -> Result<String> {
    value
        .get(name)
        .and_then(scalar)
        .with_context(|| format!("LTP 响应缺少 {name}"))
}

fn nonnegative(value: String) -> Result<String> {
    if value.starts_with('-') {
        return Ok("0".into());
    }
    decimal_cmp(&value, "0")?;
    Ok(value)
}

fn wallet_type<'a>(venue: &str, raw: &'a str) -> Option<&'a str> {
    match (venue, raw) {
        ("BINANCE", "Spot") => Some("SPOT"),
        ("BINANCE", "Margin" | "Margin-PM") => Some("MARGIN_CROSS"),
        ("BINANCE", "USDT/C-M") => Some("U_MARGINED_FUTURES"),
        ("BINANCE", "Coin-M") => Some("C_MARGINED_FUTURES"),
        ("OKX", "Funding") => Some("FUNDING"),
        ("OKX", "Trading") => Some("TRADING"),
        (
            _,
            "SPOT" | "MARGIN_CROSS" | "U_MARGINED_FUTURES" | "C_MARGINED_FUTURES" | "FUNDING"
            | "TRADING",
        ) => Some(raw),
        _ => None,
    }
}

pub fn route_name(account: &Account, account_type: &str) -> String {
    if account.kind == "funding" {
        return "Funding Account".into();
    }
    let venue = if account.kind == "portfolio" {
        account_type
    } else {
        &account.venue
    };
    let display = match venue {
        "BINANCE" => "Binance",
        "OKX" | "OKEX" => "OKX",
        other => other,
    };
    if account.kind == "portfolio" {
        format!("Trading Portfolio-{display}")
    } else {
        display.into()
    }
}

fn enabled(value: Option<&Value>) -> bool {
    value == Some(&Value::Bool(true)) || value.and_then(Value::as_str) == Some("true")
}

fn metadata_map(data: &Value) -> Result<BTreeMap<String, Vec<Value>>> {
    let mut output: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for row in data.as_array().context("币种网络响应格式无效")? {
        for (key, value) in row.as_object().context("币种网络对象无效")? {
            if matches!(key.as_str(), "fromAccount" | "toAccount") {
                continue;
            }
            let networks = value
                .get("networks")
                .and_then(Value::as_array)
                .context("币种网络缺少 networks")?;
            ensure!(
                networks
                    .iter()
                    .all(|n| n.is_object() && n.get("network").and_then(Value::as_str).is_some()),
                "币种网络条目格式无效"
            );
            output
                .entry(key.clone())
                .or_default()
                .extend(networks.iter().cloned());
        }
    }
    Ok(output)
}

pub fn currency_list(metadata: &Value, require_enabled: bool) -> Result<Vec<Currency>> {
    Ok(metadata_map(metadata)?
        .into_iter()
        .filter_map(|(currency, networks)| {
            let networks: BTreeSet<String> = networks
                .iter()
                .filter(|n| !require_enabled || enabled(n.get("withdrawEnable")))
                .filter_map(|n| n.get("network").and_then(Value::as_str).map(str::to_owned))
                .collect();
            (!networks.is_empty()).then_some(Currency {
                currency,
                networks: networks.into_iter().collect(),
            })
        })
        .collect())
}

fn outgoing_networks(metadata: &Value) -> Result<Value> {
    let mut row = serde_json::Map::new();
    for (currency, networks) in metadata_map(metadata)? {
        let accepted: Vec<_> = networks
            .into_iter()
            .filter(|n| enabled(n.get("withdrawEnable")))
            .collect();
        if !accepted.is_empty() {
            row.insert(currency, json!({"networks": accepted}));
        }
    }
    Ok(Value::Array(vec![Value::Object(row)]))
}

pub fn network_details(metadata: &Value, currency: &str, network: &str) -> Result<Value> {
    metadata_map(metadata)?
        .get(currency)
        .and_then(|rows| {
            rows.iter()
                .find(|row| row.get("network").and_then(Value::as_str) == Some(network))
        })
        .cloned()
        .context("所选币种或网络当前不支持此方向的划转")
}

pub fn apply_result(record: &mut Record, data: &Value) -> Result<()> {
    record.transfer_id = Some(field(data, "transferId")?);
    record.status = match data.get("status").and_then(Value::as_u64) {
        Some(0) => "pending",
        Some(1) => "success",
        Some(2) => "failed",
        _ => bail!("LTP 划转状态未知"),
    }
    .into();
    record.reason = data
        .get("reason")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);
    record.actual_rapid_transfer = data.get("rapidTransfer").and_then(Value::as_bool);
    record.amount_received = data.get("amountReceived").and_then(scalar);
    record.network_fee = data.get("networkFee").and_then(scalar);
    record.fee_currency = data.get("feeCurrency").and_then(scalar);
    record.updated_at = Utc::now();
    Ok(())
}

pub fn remote_record(data: &Value) -> Result<Record> {
    let transfer_id = field(data, "transferId")?;
    let created_at = data
        .get("createdAt")
        .and_then(Value::as_i64)
        .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
        .context("LTP 历史时间无效")?;
    let mut record = Record {
        client_order_id: data
            .get("clientOrderId")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("ltp-{transfer_id}")),
        transfer_id: Some(transfer_id),
        request: TransferRequest {
            from_account_id: field(data, "fromTradeAccountId")?,
            to_account_id: field(data, "toTradeAccountId")?,
            from_account_type: data
                .get("fromAccountType")
                .and_then(Value::as_str)
                .unwrap_or("")
                .into(),
            to_account_type: data
                .get("toAccountType")
                .and_then(Value::as_str)
                .unwrap_or("")
                .into(),
            currency: field(data, "currency")?,
            amount: field(data, "amount")?,
            network: data
                .get("network")
                .and_then(Value::as_str)
                .map(str::to_owned),
            rapid_transfer: false,
        },
        status: String::new(),
        created_at,
        updated_at: Utc::now(),
        actor: "LTP 历史记录".into(),
        reason: None,
        actual_rapid_transfer: None,
        amount_received: None,
        network_fee: None,
        fee_currency: None,
    };
    apply_result(&mut record, data)?;
    Ok(record)
}
