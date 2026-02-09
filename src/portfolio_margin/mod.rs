//! 统一账户（Portfolio Margin）相关模块。
//!
//! 本目录下的模块用于对接交易所统一账户（例如币安 PM）的账户数据：
//! - `listen_key`: 管理 listenKey 的创建/保活/删除与重建
//! - `binance_user_stream`: 基于 listenKey 的用户数据 WebSocket 连接
//! - `okex_auth`: OKEx WebSocket 鉴权模块
//! - `okex_user_stream`: OKEx 用户数据 WebSocket 连接
//! - `okex_rest`: OKEx REST 辅助工具（签名、账户拉取等）
//! - `gate_auth`: Gate.io WebSocket 鉴权模块
//! - `gate_user_stream`: Gate.io 用户数据 WebSocket 连接
//! - `pm_forwarder`: 将原始账户数据转发到 Iceoryx 的 `account_pubs/<exchange>/pm`
//!
//! 说明：统一账户的用户数据流通过 URL 携带 listenKey 鉴权，不需要发送订阅报文；
//! 每条 WS 连接最长不超过 24 小时，应定期断开重连；listenKey 需要每 30 分钟保活。
pub mod binance_spot_ws_api_user_stream;
pub mod binance_user_stream;
pub mod gate_auth;
pub mod gate_user_stream;
pub mod listen_key;
pub mod okex_auth;
pub mod okex_rest;
pub mod okex_user_stream;
pub mod pm_forwarder;
