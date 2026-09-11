use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use order_common::trade_error_code::rapidx::{describe_error_code, describe_http_status};
use runtime_common::exchange::Exchange;
use runtime_common::execution_backend::{rapidx_portfolio_id, ExecBackend};
use serde::Deserialize;
use serde_json::Value;
use trade_engine::ltp_rest::{LtpLoanRepayRequest, LtpRestClient};

#[derive(Debug, Parser)]
#[command(about = "Query and repay RapidX/LTP portfolio loans")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum LoanOrderType {
    Borrow,
    Repay,
}

impl LoanOrderType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Borrow => "borrow",
            Self::Repay => "repay",
        }
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Query real-time loan balances and account LTV.
    Info,
    /// Query all currencies enabled for borrowing.
    Config,
    /// Query portfolio loan capacity on one exchange.
    MaxLoan {
        #[arg(long, value_parser = ["BINANCE", "OKX"])]
        exchange: String,
    },
    /// Query borrow/repay order history.
    History {
        #[arg(long)]
        coin: Option<String>,
        #[arg(long, value_enum)]
        order_type: Option<LoanOrderType>,
        #[arg(long)]
        client_order_id: Option<String>,
        #[arg(long)]
        start_time_ms: Option<i64>,
        #[arg(long)]
        end_time_ms: Option<i64>,
        #[arg(long, default_value_t = 1)]
        page: u32,
        #[arg(long, default_value_t = 1000)]
        page_size: u32,
    },
    /// Repay exactly one loan request. Dry-run unless --execute is supplied.
    Repay {
        #[arg(long, value_parser = ["BINANCE", "OKX"])]
        exchange: String,
        #[arg(long)]
        coin: String,
        #[arg(long)]
        amount: String,
        #[arg(long)]
        client_order_id: String,
        #[arg(long)]
        execute: bool,
    },
}

#[derive(Debug, Deserialize)]
struct Envelope {
    code: Option<i32>,
    #[serde(default)]
    message: String,
    #[serde(default)]
    msg: String,
    data: Option<Value>,
}

fn validate_coin(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 20
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        bail!("coin must be 1..20 uppercase ASCII letters or digits");
    }
    Ok(())
}

fn validate_client_order_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        bail!("client order id must be 1..64 safe ASCII characters");
    }
    Ok(())
}

fn validate_two_decimal_amount(value: &str) -> Result<()> {
    let Some((whole, fraction)) = value.split_once('.') else {
        bail!("amount must use exactly two decimal places");
    };
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() != 2
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || !value.bytes().any(|byte| matches!(byte, b'1'..=b'9'))
    {
        bail!("amount must be a positive plain decimal with exactly two decimal places");
    }
    Ok(())
}

fn validate_history(
    start_time_ms: Option<i64>,
    end_time_ms: Option<i64>,
    page: u32,
    page_size: u32,
) -> Result<()> {
    if page == 0 || !(1..=1000).contains(&page_size) {
        bail!("--page must be positive and --page-size must be in 1..=1000");
    }
    if start_time_ms.is_some_and(|value| value <= 0) || end_time_ms.is_some_and(|value| value <= 0)
    {
        bail!("timestamps must be positive Unix milliseconds");
    }
    if let (Some(start), Some(end)) = (start_time_ms, end_time_ms) {
        if start >= end {
            bail!("--start-time-ms must be earlier than --end-time-ms");
        }
    }
    Ok(())
}

fn print_submit_response(http_status: u16, body: &str) -> Result<()> {
    let envelope: Envelope = serde_json::from_str(body).with_context(|| {
        format!(
            "decode RapidX response: HTTP {} ({})",
            http_status,
            describe_http_status(http_status).unwrap_or("unclassified HTTP status")
        )
    })?;
    let code = envelope.code.unwrap_or_default();
    let message = if envelope.message.is_empty() {
        envelope.msg.as_str()
    } else {
        envelope.message.as_str()
    };
    if http_status != 200 || !matches!(code, 200 | 200000) {
        bail!(
            "RapidX loan API failed: http_status={} http_description={} code={} code_description={} message={}",
            http_status,
            describe_http_status(http_status).unwrap_or("none"),
            code,
            describe_error_code(code).unwrap_or("unknown RapidX error"),
            message
        );
    }
    println!(
        "[response] http_status={} code={} message={} data={}",
        http_status,
        code,
        message,
        envelope.data.unwrap_or(Value::Null)
    );
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    if ExecBackend::for_exchange(Exchange::Binance)? != ExecBackend::Ltp {
        bail!("rapidx_loan requires Binance execution backend ltp");
    }
    let portfolio_id = rapidx_portfolio_id()?;
    let client = LtpRestClient::from_env()?;

    match args.command {
        Command::Info => {
            println!("[plan] portfolio_id={portfolio_id} action=loan_info read_only=true");
            println!("{}", client.fetch_loan_info().await?);
        }
        Command::Config => {
            println!("[plan] portfolio_id={portfolio_id} action=loan_config read_only=true");
            println!("{}", client.fetch_loan_config().await?);
        }
        Command::MaxLoan { exchange } => {
            println!(
                "[plan] portfolio_id={portfolio_id} action=max_loan exchange={exchange} read_only=true"
            );
            println!("{}", client.fetch_loan_capacity(&exchange).await?);
        }
        Command::History {
            coin,
            order_type,
            client_order_id,
            start_time_ms,
            end_time_ms,
            page,
            page_size,
        } => {
            validate_history(start_time_ms, end_time_ms, page, page_size)?;
            if let Some(value) = coin.as_deref() {
                validate_coin(value)?;
            }
            if let Some(value) = client_order_id.as_deref() {
                validate_client_order_id(value)?;
            }
            println!(
                "[plan] portfolio_id={} action=loan_history coin={:?} type={:?} client_order_id={:?} page={} page_size={} read_only=true",
                portfolio_id, coin, order_type, client_order_id, page, page_size
            );
            println!(
                "{}",
                client
                    .fetch_loan_orders(
                        coin.as_deref(),
                        order_type.map(LoanOrderType::as_str),
                        client_order_id.as_deref(),
                        start_time_ms,
                        end_time_ms,
                        page,
                        page_size,
                    )
                    .await?
            );
        }
        Command::Repay {
            exchange,
            coin,
            amount,
            client_order_id,
            execute,
        } => {
            validate_coin(&coin)?;
            validate_two_decimal_amount(&amount)?;
            validate_client_order_id(&client_order_id)?;
            println!(
                "[plan] portfolio_id={} action=repay exchange={} coin={} amount={} client_order_id={} execute={}",
                portfolio_id, exchange, coin, amount, client_order_id, execute
            );
            if !execute {
                println!("[plan] dry-run only; add --execute to submit exactly once");
                return Ok(());
            }
            let request = LtpLoanRepayRequest {
                exchange,
                coin,
                amount,
                client_order_id: Some(client_order_id),
            };
            let response = match client.repay_loan(&request).await {
                Ok(response) => response,
                Err(error) => bail!(
                    "RapidX repayment result is unknown; do not retry. Query loan history by clientOrderId first: {error:#}"
                ),
            };
            print_submit_response(response.0, &response.1)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repayment_amount_requires_two_decimal_places() {
        for value in ["0.01", "1.00", "5000.10"] {
            assert!(validate_two_decimal_amount(value).is_ok(), "{value}");
        }
        for value in ["0.00", "1", "1.0", "1.000", "-1.00", "1e2"] {
            assert!(validate_two_decimal_amount(value).is_err(), "{value}");
        }
    }

    #[test]
    fn validates_history_bounds() {
        assert!(validate_history(Some(1), Some(2), 1, 1000).is_ok());
        assert!(validate_history(Some(2), Some(1), 1, 100).is_err());
        assert!(validate_history(None, None, 0, 100).is_err());
    }
}
