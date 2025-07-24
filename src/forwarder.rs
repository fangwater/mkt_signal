//转发器，对收到的消息进行处理，并根据一定的方式转发
use zmq::{Context, Socket, SocketType};
use crate::mkt_msg::MktMsg;
use bytes::Bytes;
use crate::cfg::{ZmqProxyCfg, Config};
use std::time::Duration;
use tokio::time::sleep;
use log::{info, warn, error, debug};

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

    pub async fn send_msg(&mut self, msg: Bytes) -> bool {
        const MAX_RETRIES: usize = 3;
        const RETRY_DELAY_MS: u64 = 1000;
        let mut retry_count = 0;
        let mut ipc_success = false;

        while !ipc_success && retry_count < MAX_RETRIES {
            // 先尝试IPC发送
            match self.ipc_socket.send(&msg[..], zmq::DONTWAIT) {
                Ok(_) => {
                    self.ipc_count += 1;
                    ipc_success = true;
                    debug!("IPC send success, count: {}", self.ipc_count);

                    // IPC发送成功后，尝试TCP发送
                    match self.tcp_socket.send(&msg[..], zmq::DONTWAIT) {
                        Ok(_) => {
                            self.tcp_count += 1;
                            debug!("TCP send success, count: {}", self.tcp_count);
                        }
                        Err(e) => {
                            // TCP发送失败直接忽略
                            if e != zmq::Error::EAGAIN {
                                debug!("TCP send error: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    if e == zmq::Error::EAGAIN {
                        // IPC对端未启动，需要重试
                        warn!("IPC peer not ready (EAGAIN), attempt {}/{}, error: {}", 
                              retry_count + 1, MAX_RETRIES, e);
                        if retry_count < MAX_RETRIES - 1 {
                            sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                        }
                    } else {
                        // 其他错误
                        error!("IPC error on attempt {}/{}, error: {}", 
                               retry_count + 1, MAX_RETRIES, e);
                    }
                }
            }
            retry_count += 1;
        }

        if !ipc_success {
            error!("Failed to send IPC message after {} attempts", MAX_RETRIES);
            false
        } else {
            true
        }
    }
    
    pub async fn send_tp_reset_msg(&mut self) -> bool {
        let tp_reset_msg = MktMsg::tp_reset();
        let msg_bytes = tp_reset_msg.to_bytes();
        
        info!("Sending tp reset message...");
        
        // 每隔3s重试，一直到ipc成功发送
        loop {
            match self.ipc_socket.send(&msg_bytes[..], zmq::DONTWAIT) {
                Ok(_) => {
                    self.ipc_count += 1;
                    info!("Send tp reset msg success, ipc_count: {}", self.ipc_count);
                    return true;
                }
                Err(e) => {
                    warn!("Send tp reset msg failed, retrying in 3s... Error: {}", e);
                    sleep(Duration::from_secs(3)).await;
                }
            }
        }
    }

    pub fn log_stats(&mut self) {
        info!("stats: ipc_count: {}, tcp_count: {}", self.ipc_count, self.tcp_count);
        self.ipc_count = 0;
        self.tcp_count = 0;
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
