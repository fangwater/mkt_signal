use bytes::Bytes;
use tokio::sync::mpsc;

/// Parser trait - 直接使用mpsc发送解析结果
pub trait Parser: Send {
    /// 解析消息并通过mpsc发送
    /// 返回成功发送的消息数量
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize;
}
