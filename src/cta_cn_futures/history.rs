use super::config::Config;
use anyhow::{ensure, Context, Result};
use chrono::NaiveDate;
use cn_market_proto::{
    cn_futures_market_client::CnFuturesMarketClient, get_minute_bars_request::Range, Contract,
    GetMinuteBarsRequest, GetMinuteBarsResponse, GetSizeThresholdsRequest,
    GetTradingSessionsRequest, MinuteBar, TimeRange,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Dominant {
    pub contract: String,
    pub trading_day: u32,
    pub source_trading_day: u32,
    pub scheduled_next: String,
}
impl Dominant {
    pub fn for_day(&self, day: u32) -> Result<Self> {
        ensure!(
            day >= self.trading_day,
            "live trading day predates dominant selection"
        );
        if day == self.trading_day {
            return Ok(self.clone());
        }
        ensure!(
            self.trading_day == self.source_trading_day,
            "dominant REST has not advanced beyond the preceding live trading day"
        );
        Ok(Self {
            contract: self.scheduled_next.clone(),
            trading_day: day,
            ..self.clone()
        })
    }
}
fn native_contract(cfg: &Config, code: &str) -> Result<String> {
    let prefix = cfg.native_prefix();
    let (product, month) = code
        .split_at_checked(prefix.len())
        .context("missing dominant month")?;
    ensure!(
        product.eq_ignore_ascii_case(&prefix)
            && matches!(month.len(), 3 | 4)
            && month.bytes().all(|b| b.is_ascii_digit())
            && month[month.len() - 2..]
                .parse::<u8>()
                .is_ok_and(|m| (1..=12).contains(&m)),
        "invalid dominant for {}/{}: {code}",
        cfg.exchange,
        cfg.symbol
    );
    let month = if cfg.exchange == "CZCE" {
        &month[month.len() - 3..]
    } else {
        ensure!(month.len() == 4, "invalid SHFE maturity: {code}");
        month
    };
    Ok(format!("{prefix}{month}"))
}
pub async fn dominant(cfg: &Config) -> Result<Dominant> {
    let (exchange, _) = cfg.exchange_codes()?;
    let response: serde_json::Value = reqwest::Client::new()
        .get(&cfg.dominant_url)
        .timeout(std::time::Duration::from_secs(8))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let rows = response["products"]
        .as_array()
        .context("invalid dominant response")?;
    let matches: Vec<_> = rows
        .iter()
        .filter(|r| {
            r["exchange"] == exchange.as_str()
                && r["product"]
                    .as_str()
                    .is_some_and(|s| s.eq_ignore_ascii_case(&cfg.symbol))
        })
        .collect();
    ensure!(
        matches.len() == 1,
        "REST must return one {}/{} dominant row",
        cfg.exchange,
        cfg.symbol
    );
    let row = matches[0];
    ensure!(
        row["trading_day"] == response["latest_trading_day"],
        "mixed-day dominant response"
    );
    let day = NaiveDate::parse_from_str(
        row["trading_day"]
            .as_str()
            .context("missing dominant trading day")?,
        "%Y-%m-%d",
    )?;
    let contract = native_contract(
        cfg,
        row["dominant"]
            .as_str()
            .context("missing current dominant")?,
    )?;
    let trading_day = day.format("%Y%m%d").to_string().parse()?;
    Ok(Dominant {
        contract,
        trading_day,
        source_trading_day: trading_day,
        scheduled_next: native_contract(
            cfg,
            row["scheduled_next"]
                .as_str()
                .context("missing next dominant")?,
        )?,
    })
}
/// Network history retained only in memory. Repeated tail requests replace
/// provisional minutes until they agree with the independently captured ZMQ feed.
#[derive(Default)]
pub struct History {
    pub minutes: BTreeMap<i64, MinuteBar>,
    pub contract: Option<Contract>,
    pub through_ms: i64,
    pub session_snapshot: String,
}
impl History {
    pub async fn fetch(
        &mut self,
        cfg: &Config,
        main: &Dominant,
        start: i64,
        end: i64,
    ) -> Result<()> {
        let mut client = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            CnFuturesMarketClient::connect(cfg.history_grpc.clone()),
        )
        .await??
        .max_decoding_message_size(64 * 1024 * 1024);
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            client.get_minute_bars(GetMinuteBarsRequest {
                exchange: cfg.exchange_codes()?.1 as i32,
                contract: main.contract.clone(),
                include_empty_minutes: true,
                range: Some(Range::Time(TimeRange {
                    start_ms: start,
                    end_ms: end,
                })),
            }),
        )
        .await?;
        match response {
            Ok(response) => self.merge(cfg, main, response.into_inner(), start, end),
            Err(error) if self.minutes.is_empty() && error.code() as i32 == 5 => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
    pub fn merge(
        &mut self,
        cfg: &Config,
        main: &Dominant,
        response: GetMinuteBarsResponse,
        start: i64,
        end: i64,
    ) -> Result<()> {
        let metadata = response
            .contract
            .context("gRPC omitted contract metadata")?;
        ensure!(
            metadata.exchange == cfg.exchange_codes()?.1 as i32
                && metadata.product == cfg.symbol
                && native_contract(cfg, &metadata.native_code)? == main.contract,
            "gRPC returned another contract"
        );
        ensure!(
            metadata.volume_multiple.is_finite()
                && metadata.volume_multiple > 0.
                && metadata.price_tick.is_finite()
                && metadata.price_tick > 0.,
            "gRPC returned invalid multiplier/tick"
        );
        if let Some(old) = &self.contract {
            ensure!(
                old.volume_multiple == metadata.volume_multiple
                    && old.price_tick == metadata.price_tick,
                "gRPC contract units changed during catch-up"
            );
        }
        let state = response.data_state.context("gRPC omitted data coverage")?;
        ensure!(
            state.latest_trading_day >= main.source_trading_day,
            "gRPC archive is behind the dominant source day"
        );
        let mut last = None;
        for row in &response.bars {
            ensure!(
                row.open_time_ms >= start
                    && row.open_time_ms <= end
                    && row.open_time_ms % 60_000 == 0
                    && last.is_none_or(|t| row.open_time_ms > t),
                "gRPC returned invalid or unordered minute bounds"
            );
            last = Some(row.open_time_ms);
        }
        // Replace this requested interval atomically: a provisional rebuild
        // can remove a previously returned row as well as revise its fields.
        self.minutes.retain(|ts, _| *ts < start || *ts > end);
        self.contract = Some(metadata);
        self.through_ms = state.live_through_ms;
        self.session_snapshot = state.session_snapshot_as_of;
        for row in response.bars {
            self.minutes.insert(row.open_time_ms, row);
        }
        Ok(())
    }
    pub fn multiple(&self) -> Result<f64> {
        Ok(self
            .contract
            .as_ref()
            .context("history has no contract metadata")?
            .volume_multiple)
    }
}

pub async fn parameters(cfg: &Config) -> Result<super::live::Context> {
    let (exchange, grpc_exchange) = cfg.exchange_codes()?;
    let load = async {
        let mut client = CnFuturesMarketClient::connect(cfg.history_grpc.clone())
            .await
            .context("connect native futures parameter gRPC")?;
        let thresholds = client
            .get_size_thresholds(GetSizeThresholdsRequest {
                exchange: grpc_exchange as i32,
                product: cfg.symbol.clone(),
            })
            .await
            .with_context(|| format!("GetSizeThresholds {}/{}", cfg.exchange, cfg.symbol))?
            .into_inner();
        let sessions = client
            .get_trading_sessions(GetTradingSessionsRequest {
                exchange: grpc_exchange as i32,
                product: cfg.symbol.clone(),
            })
            .await
            .with_context(|| format!("GetTradingSessions {}/{}", cfg.exchange, cfg.symbol))?
            .into_inner();
        super::live::Context::new(exchange, &cfg.symbol, thresholds, sessions)
    };
    tokio::time::timeout(std::time::Duration::from_secs(30), load).await?
}
