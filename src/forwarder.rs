//转发器，对收到的消息进行处理，并根据一定的方式转发
use zmq::{Context, Socket, SocketType};
use crate::mkt_msg::MktMsg;
use bytes::Bytes;
use crate::cfg::{ZmqProxyCfg, Config};
use std::time::Duration;
use tokio::time::sleep;
use log::{info, warn, error, debug};
use std::collections::HashMap;

//zmq_forwarder,通过ipc和tcp转发消息
pub struct ZmqForwarder{
    zmq_config: ZmqProxyCfg,
    is_primary: bool,
    #[allow(dead_code)]
    context: Context,
    ipc_socket: Socket,
    tcp_socket: Socket,
    ipc_count: u64,
    tcp_count: u64,
    ipc_dropped: u64,
    tcp_dropped: u64,
    topic_counts: HashMap<String, u64>,
}

impl ZmqForwarder {
    pub fn new(config: &Config) -> Result<Self, zmq::Error> {
        // 创建ZMQ上下文，设置1个I/O线程（与C++版本一致）
        let context = Context::new();
        context.set_io_threads(1)?;
        let ipc_socket = context.socket(SocketType::PUB)?;
        let tcp_socket = context.socket(SocketType::PUB)?;

        // 设置水位线
        ipc_socket.set_sndhwm(config.get_zmq_proxy().hwm as i32)?;
        tcp_socket.set_sndhwm(config.get_zmq_proxy().hwm as i32)?;

        let mut forwarder = Self {
            zmq_config: config.get_zmq_proxy(),
            is_primary: config.is_primary,
            context,
            ipc_socket,
            tcp_socket,
            ipc_count: 0,
            tcp_count: 0,
            ipc_dropped: 0,
            tcp_dropped: 0,
            topic_counts: HashMap::new(),
        };

        forwarder.bind()?;
        Ok(forwarder)
    }

    fn bind(&mut self) -> Result<(), zmq::Error> {
        // 连接 IPC 和 TCP
        let ipc_addr = format!("ipc://{}", self.zmq_config.ipc_path);
        let bind_addr = if self.is_primary {
            format!("tcp://{}", self.zmq_config.primary_addr)
        } else {
            format!("tcp://{}", self.zmq_config.secondary_addr)
        };

        match self.ipc_socket.bind(&ipc_addr) {
            Ok(_) => {
                match self.tcp_socket.bind(&bind_addr) {
                    Ok(_) => {
                        info!("ZmqForwarder bind success, ipc: {}, tcp: {}, is_primary: {}", 
                              self.zmq_config.ipc_path, bind_addr, self.is_primary);
                        Ok(())
                    }
                    Err(e) => {
                        error!("ZmqForwarder TCP bind failed, addr: {}, error: {}", 
                               bind_addr, e);
                        Err(e)
                    }
                }
            }
            Err(e) => {
                error!("ZmqForwarder IPC connect failed, path: {}, error: {}", 
                       self.zmq_config.ipc_path, e);
                Err(e)
            }
        }
    }

    pub async fn send_msg(&mut self, msg: Bytes, topic: &str) -> bool {
        // PUB模式下，发送失败通常是缓冲区满，重试意义不大，直接快速失败
        let mut ipc_success = false;

        // 尝试IPC发送
        match self.ipc_socket.send(&msg[..], zmq::DONTWAIT) {
            Ok(_) => {
                self.ipc_count += 1;
                // 更新topic统计
                *self.topic_counts.entry(topic.to_string()).or_insert(0) += 1;
                ipc_success = true;
                debug!("IPC send success, count: {}, topic: {}", self.ipc_count, topic);

                // IPC发送成功后，尝试TCP发送
                match self.tcp_socket.send(&msg[..], zmq::DONTWAIT) {
                    Ok(_) => {
                        self.tcp_count += 1;
                        debug!("TCP send success, count: {}, topic: {}", self.tcp_count, topic);
                    }
                    Err(e) => {
                        // TCP发送失败直接忽略
                        if e == zmq::Error::EAGAIN {
                            self.tcp_dropped += 1;
                            debug!("TCP send failed (buffer full), dropped: {}, topic: {}", self.tcp_dropped, topic);
                        } else {
                            debug!("TCP send error: {}, topic: {}", e, topic);
                        }
                    }
                }
            }
            Err(e) => {
                if e == zmq::Error::EAGAIN {
                    // PUB模式下EAGAIN通常是缓冲区满，重试无意义，直接丢弃
                    self.ipc_dropped += 1;
                    debug!("IPC send failed (buffer full), dropped: {}, topic: {}", self.ipc_dropped, topic);
                } else {
                    // 其他错误
                    warn!("IPC send error: {}, topic: {}", e, topic);
                }
            }
        }

        ipc_success
    }
    
    pub async fn send_tp_reset_msg(&mut self, topic: &str) -> bool {
        let tp_reset_msg = MktMsg::tp_reset();
        let msg_bytes = tp_reset_msg.to_bytes();
        
        info!("Sending tp reset message for topic: {}...", topic);
        
        // 每隔3s重试，一直到ipc成功发送
        loop {
            match self.ipc_socket.send(&msg_bytes[..], zmq::DONTWAIT) {
                Ok(_) => {
                    self.ipc_count += 1;
                    // 更新topic统计
                    *self.topic_counts.entry(topic.to_string()).or_insert(0) += 1;
                    info!("Send tp reset msg success, ipc_count: {}, topic: {}", self.ipc_count, topic);
                    return true;
                }
                Err(e) => {
                    warn!("Send tp reset msg failed for topic: {}, retrying in 3s... Error: {}", topic, e);
                    sleep(Duration::from_secs(3)).await;
                }
            }
        }
    }

    pub fn log_stats(&mut self) {
        // 打印基础统计信息
        info!("stats: ipc_count: {}, tcp_count: {}, ipc_dropped: {}, tcp_dropped: {}", 
              self.ipc_count, self.tcp_count, self.ipc_dropped, self.tcp_dropped);
        
        // 打印各topic的消息条数
        if !self.topic_counts.is_empty() {
            let mut topic_stats = String::new();
            for (topic, count) in &self.topic_counts {
                if !topic_stats.is_empty() {
                    topic_stats.push_str(", ");
                }
                topic_stats.push_str(&format!("{}: {}", topic, count));
            }
            info!("topic_stats: {}", topic_stats);
        }
        
        // 清零所有统计信息
        self.ipc_count = 0;
        self.tcp_count = 0;
        self.ipc_dropped = 0;
        self.tcp_dropped = 0;
        self.topic_counts.clear();
    }
}

impl Drop for ZmqForwarder {
    fn drop(&mut self) {
        info!("ZmqForwarder stopping...");
        
        // 关闭socket
        let _ = self.ipc_socket.set_linger(0); // 立即关闭，不等待未发送的消息
        let _ = self.tcp_socket.set_linger(0);
        info!("ZmqForwarder sockets configured for immediate close");
        
        info!("ZmqForwarder stop success");
    }
}
