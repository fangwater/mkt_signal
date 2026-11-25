//! Cold restart shared by the CTP and Yida runtimes.
//!
//! Some venue conditions cannot be repaired inside a live SDK session: the
//! instance must be destroyed and the process started again, which replays the
//! tested startup path (durable recovery, full venue snapshot, unresolved-order
//! check). The runtime drains evidence, records `restart_required`, and exits
//! with [`ColdRestartRequired::EXIT_STATUS`]; the environment's systemd unit
//! restarts only that status (`RestartForceExitStatus=75`). Every other failure
//! keeps `Restart=no` for an operator.

/// A condition that only a new SDK session can recover.
#[derive(Debug)]
pub enum ColdRestartRequired {
    /// CTP account query timed out or kept failing.
    AccountQuery(&'static str),
    /// No transport, login/recovery or control response within the response timeout.
    TransportOrControlTimeout,
    /// CTP MD login returned no valid trading day.
    MarketDataTradingDay,
    /// The venue server restarted; every datum of the old SDK instance is invalid
    /// (Yida `YD_AE_ServerRestarted`).
    ServerRestarted,
    /// The broker counter answered a bootstrap or control request with CTP error 7
    /// (「还没有初始化」), e.g. while it re-initialises after the daily close.
    VenueNotInitialized,
    /// A non-account CTP query answered error 90 (「查询未就绪，请稍后重试」),
    /// e.g. the trade query while the counter reloads after the daily close.
    QueryNotReady(&'static str),
}

impl ColdRestartRequired {
    /// `EX_TEMPFAIL`: retry later.
    pub const EXIT_STATUS: i32 = 75;

    /// Short machine-readable reason for the `restart_required` record.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::AccountQuery(_) => "account_query",
            Self::TransportOrControlTimeout => "transport_or_control_timeout",
            Self::MarketDataTradingDay => "market_data_trading_day",
            Self::ServerRestarted => "server_restarted",
            Self::VenueNotInitialized => "venue_not_initialized",
            Self::QueryNotReady(_) => "query_not_ready",
        }
    }

    /// Operator-facing status while the process drains.
    pub fn observation_reason(&self) -> &'static str {
        match self {
            Self::AccountQuery(_) => "account query unavailable; cold restart required",
            Self::TransportOrControlTimeout => {
                "transport or control response timed out; cold restart required"
            }
            Self::MarketDataTradingDay => {
                "market-data trading day unavailable; cold restart required"
            }
            Self::ServerRestarted => "venue server restarted; cold restart required",
            Self::VenueNotInitialized => "venue counter not initialized; cold restart required",
            Self::QueryNotReady(_) => "venue query not ready; cold restart required",
        }
    }
}

impl std::fmt::Display for ColdRestartRequired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::AccountQuery(message) => message,
            Self::TransportOrControlTimeout => {
                "transport, login/recovery or control response timed out; retry after SDK restart"
            }
            Self::MarketDataTradingDay => {
                "MD login returned no valid trading day; retry after SDK restart"
            }
            Self::ServerRestarted => {
                "venue server restarted; old SDK data is invalid; retry after SDK restart"
            }
            Self::VenueNotInitialized => {
                "CTP counter not initialized (error 7); retry after SDK restart"
            }
            Self::QueryNotReady(kind) => {
                return write!(
                    f,
                    "CTP {kind} query not ready (error 90); retry after SDK restart"
                );
            }
        })
    }
}

impl std::error::Error for ColdRestartRequired {}

/// Process exit status for a runtime error: 75 for a cold restart, else 1.
pub fn exit_status(error: &anyhow::Error) -> i32 {
    if error.is::<ColdRestartRequired>() {
        ColdRestartRequired::EXIT_STATUS
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_cold_restart_errors_exit_with_tempfail() {
        let restart =
            anyhow::Error::from(ColdRestartRequired::ServerRestarted).context("Yida session ended");
        assert_eq!(exit_status(&restart), 75);
        assert_eq!(exit_status(&anyhow::anyhow!("login failed")), 1);
    }
}
