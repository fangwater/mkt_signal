use runtime_common::ws_connection::{MktConnection, MktConnectionHandler};

use bytes::Bytes;
use runtime_common::exchange::Exchange;
use tokio::sync::{broadcast, watch};

/// 根据交易所类型构造相应的连接处理器（带IP绑定）
///
/// `venue_label` 仅供 BinanceConnection 用作日志前缀（aster 共用 binance 实现），
/// 其它交易所目前忽略此参数。
pub fn construct_connection_with_ip(
    exchange: Exchange,
    url: String,
    subscribe_msg: serde_json::Value,
    tx: broadcast::Sender<Bytes>,
    global_shutdown_rx: watch::Receiver<bool>,
    local_ip: String,
    venue_label: &str,
) -> anyhow::Result<Box<dyn MktConnectionHandler>> {
    use crate::connection::binance_conn::BinanceConnection;
    use crate::connection::bitget_conn::BitgetConnection;
    use crate::connection::bybit_conn::BybitConnection;
    use crate::connection::gate_conn::GateConnection;
    use crate::connection::hyperliquid_conn::HyperliquidConnection;
    use crate::connection::okex_conn::OkexConnection;

    let gate_is_futures = url.contains("fx-ws.gateio.ws") || url.contains("/futures");
    let mut base_connection = MktConnection::new(url, subscribe_msg, tx, global_shutdown_rx);
    base_connection.local_ip = Some(local_ip);

    match exchange {
        Exchange::Binance => Ok(Box::new(BinanceConnection::new(
            base_connection,
            venue_label,
        ))),
        Exchange::Aster => Ok(Box::new(BinanceConnection::new(
            base_connection,
            venue_label,
        ))),
        Exchange::Okex => Ok(Box::new(OkexConnection::new(base_connection))),
        Exchange::Bybit => Ok(Box::new(BybitConnection::new(base_connection))),
        Exchange::Bitget => Ok(Box::new(BitgetConnection::new(base_connection))),
        Exchange::Gate => Ok(Box::new(GateConnection::new(
            base_connection,
            gate_is_futures,
        ))),
        Exchange::Hyperliquid => Ok(Box::new(HyperliquidConnection::new(base_connection))),
    }
}

/// 根据交易所类型构造相应的连接处理器
///
/// `venue_label` 仅供 BinanceConnection 用作日志前缀（aster 共用 binance 实现），
/// 其它交易所目前忽略此参数。
#[allow(unused)]
pub fn construct_connection(
    exchange: Exchange,
    url: String,
    subscribe_msg: serde_json::Value,
    tx: broadcast::Sender<Bytes>,
    global_shutdown_rx: watch::Receiver<bool>,
    venue_label: &str,
) -> anyhow::Result<Box<dyn MktConnectionHandler>> {
    use crate::connection::binance_conn::BinanceConnection;
    use crate::connection::bitget_conn::BitgetConnection;
    use crate::connection::bybit_conn::BybitConnection;
    use crate::connection::gate_conn::GateConnection;
    use crate::connection::hyperliquid_conn::HyperliquidConnection;
    use crate::connection::okex_conn::OkexConnection;

    let gate_is_futures = url.contains("fx-ws.gateio.ws") || url.contains("/futures");
    let base_connection = MktConnection::new(url, subscribe_msg, tx, global_shutdown_rx);

    match exchange {
        Exchange::Binance => Ok(Box::new(BinanceConnection::new(
            base_connection,
            venue_label,
        ))),
        Exchange::Aster => Ok(Box::new(BinanceConnection::new(
            base_connection,
            venue_label,
        ))),
        Exchange::Okex => Ok(Box::new(OkexConnection::new(base_connection))),
        Exchange::Bybit => Ok(Box::new(BybitConnection::new(base_connection))),
        Exchange::Bitget => Ok(Box::new(BitgetConnection::new(base_connection))),
        Exchange::Gate => Ok(Box::new(GateConnection::new(
            base_connection,
            gate_is_futures,
        ))),
        Exchange::Hyperliquid => Ok(Box::new(HyperliquidConnection::new(base_connection))),
    }
}
