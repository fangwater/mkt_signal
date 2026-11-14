use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use order_common::trade_error_code::rapidx::{describe_error_code, describe_http_status};
use runtime_common::execution_backend::rapidx_portfolio_id;
use serde::Deserialize;
use serde_json::Value;
use trade_engine::ltp_rest::{LtpRestClient, LtpTransferRequest};

#[derive(Debug, Parser)]
#[command(about = "Apply or query RapidX/LTP asset transfers")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Submit one transfer. Dry-run unless --execute is supplied.
    Apply {
        #[arg(long)]
        from_trade_account_id: u64,
        #[arg(long)]
        to_trade_account_id: u64,
        #[arg(long)]
        from_account_type: String,
        #[arg(long)]
        to_account_type: String,
        #[arg(long)]
        currency: String,
        #[arg(long)]
        amount: String,
        #[arg(long)]
        network: Option<String>,
        #[arg(long)]
        rapid_transfer: bool,
        #[arg(long)]
        client_order_id: String,
        #[arg(long)]
        loan_trans: bool,
        #[arg(long)]
        execute: bool,
    },
    /// Query one transfer by exactly one identifier.
    Get {
        #[arg(long, conflicts_with = "client_order_id")]
        transfer_id: Option<u64>,
        #[arg(long, conflicts_with = "transfer_id")]
        client_order_id: Option<String>,
    },
    /// List transfer history. Defaults to page 1 with 100 rows.
    List {
        #[arg(long)]
        currency: Option<String>,
        #[arg(long)]
        status: Option<u8>,
        #[arg(long)]
        start_time_ms: Option<i64>,
        #[arg(long)]
        end_time_ms: Option<i64>,
        #[arg(long, default_value_t = 1)]
        page: u32,
        #[arg(long, default_value_t = 100)]
        page_size: u32,
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

fn validate_token(name: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'-')
    {
        bail!("--{name} must contain uppercase ASCII letters, digits, or '-'");
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

fn validate_positive_decimal(value: &str) -> Result<()> {
    let mut parts = value.split('.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next();
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || fraction
            .is_some_and(|part| part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()))
        || parts.next().is_some()
        || !value.bytes().any(|byte| matches!(byte, b'1'..=b'9'))
    {
        bail!("--amount must be a positive plain decimal string");
    }
    Ok(())
}

fn validate_list(
    status: Option<u8>,
    start_time_ms: Option<i64>,
    end_time_ms: Option<i64>,
    page: u32,
    page_size: u32,
) -> Result<()> {
    if status.is_some_and(|value| value > 2) {
        bail!("--status must be 0 (pending), 1 (success), or 2 (failed)");
    }
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

fn print_response(http_status: u16, body: &str) -> Result<()> {
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
            "RapidX transfer API failed: http_status={} http_description={} code={} code_description={} message={}",
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
    let portfolio_id = rapidx_portfolio_id()?;
    let client = LtpRestClient::from_env()?;

    let response = match args.command {
        Command::Apply {
            from_trade_account_id,
            to_trade_account_id,
            from_account_type,
            to_account_type,
            currency,
            amount,
            network,
            rapid_transfer,
            client_order_id,
            loan_trans,
            execute,
        } => {
            validate_token("from-account-type", &from_account_type)?;
            validate_token("to-account-type", &to_account_type)?;
            validate_token("currency", &currency)?;
            validate_positive_decimal(&amount)?;
            validate_client_order_id(&client_order_id)?;
            if let Some(value) = network.as_deref() {
                validate_token("network", value)?;
            }
            println!(
                "[plan] portfolio_id={} from_account_id={} from_type={} to_account_id={} to_type={} currency={} amount={} rapid_transfer={} loan_trans={} client_order_id={} execute={}",
                portfolio_id,
                from_trade_account_id,
                from_account_type,
                to_trade_account_id,
                to_account_type,
                currency,
                amount,
                rapid_transfer,
                loan_trans,
                client_order_id,
                execute
            );
            if !execute {
                println!("[plan] dry-run only; add --execute to submit exactly once");
                return Ok(());
            }
            let request = LtpTransferRequest {
                from_trade_account_id,
                to_trade_account_id,
                from_account_type,
                to_account_type,
                currency,
                amount,
                network,
                rapid_transfer,
                client_order_id,
                loan_trans,
            };
            match client.apply_transfer(&request).await {
                Ok(response) => response,
                Err(error) => bail!(
                    "RapidX transfer submission result is unknown; do not retry. Query clientOrderId={} first: {error:#}",
                    request.client_order_id
                ),
            }
        }
        Command::Get {
            transfer_id,
            client_order_id,
        } => {
            if transfer_id.is_none() && client_order_id.is_none() {
                bail!("get requires --transfer-id or --client-order-id");
            }
            if let Some(value) = client_order_id.as_deref() {
                validate_client_order_id(value)?;
            }
            println!(
                "[plan] portfolio_id={} action=get transfer_id={:?} client_order_id={:?} read_only=true",
                portfolio_id, transfer_id, client_order_id
            );
            client
                .get_transfer(transfer_id, client_order_id.as_deref())
                .await?
        }
        Command::List {
            currency,
            status,
            start_time_ms,
            end_time_ms,
            page,
            page_size,
        } => {
            validate_list(status, start_time_ms, end_time_ms, page, page_size)?;
            if let Some(value) = currency.as_deref() {
                validate_token("currency", value)?;
            }
            println!(
                "[plan] portfolio_id={} action=list currency={:?} status={:?} page={} page_size={} read_only=true",
                portfolio_id, currency, status, page, page_size
            );
            client
                .list_transfers(
                    currency.as_deref(),
                    status,
                    start_time_ms,
                    end_time_ms,
                    page,
                    page_size,
                )
                .await?
        }
    };
    print_response(response.0, &response.1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_decimal_without_float_rounding() {
        for value in ["1", "0.01", "100.00000001"] {
            assert!(validate_positive_decimal(value).is_ok(), "{value}");
        }
        for value in ["0", "0.0", "-1", "1e3", ".1", "1.", "1.2.3"] {
            assert!(validate_positive_decimal(value).is_err(), "{value}");
        }
    }

    #[test]
    fn validates_transfer_history_bounds() {
        assert!(validate_list(Some(2), Some(1), Some(2), 1, 1000).is_ok());
        assert!(validate_list(Some(3), None, None, 1, 100).is_err());
        assert!(validate_list(None, Some(2), Some(1), 1, 100).is_err());
        assert!(validate_list(None, None, None, 0, 100).is_err());
    }
}
