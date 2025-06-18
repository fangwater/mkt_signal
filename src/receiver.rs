//测试用的zmq receiver
use zmq::{Context, Socket, SocketType};
use crate::cfg::{Config, ZmqProxyCfg};
use log::{info, warn, error};
use std::time::Duration;
use tokio::sync::watch;
/// 接收器，用于接收zmq消息，用于测试
#[allow(dead_code)]
pub struct ZmqReceiver{
    zmq_config: ZmqProxyCfg,
    #[allow(dead_code)]
    context: Context,
    ipc_socket: Socket,
    receive_count: u64,
    receiver_shutdown_rx: watch::Receiver<bool>,
}

#[allow(dead_code)]
impl ZmqReceiver {
    pub fn new(config: &Config, receiver_shutdown_rx: watch::Receiver<bool>) -> Result<Self, zmq::Error> {
        // 创建ZMQ上下文
        let context = Context::new();
        context.set_io_threads(1)?;
        
        // 创建PULL socket用于接收消息
        let ipc_socket = context.socket(SocketType::PULL)?;
        
        // 设置接收水位线
        ipc_socket.set_rcvhwm(config.zmq_proxy.hwm as i32)?;
        
        let mut receiver = Self {
            zmq_config: config.zmq_proxy.clone(),
            context,
            ipc_socket,
            receive_count: 0,
            receiver_shutdown_rx: receiver_shutdown_rx,
        };
        
        receiver.bind()?;
        Ok(receiver)
    }
    
    fn bind(&mut self) -> Result<(), zmq::Error> {
        let ipc_addr = format!("ipc://{}", self.zmq_config.ipc_path);
        
        match self.ipc_socket.bind(&ipc_addr) {
            Ok(_) => {
                info!("ZmqReceiver bind success, ipc: {}", ipc_addr);
                Ok(())
            }
            Err(e) => {
                error!("ZmqReceiver IPC bind failed, path: {}, error: {}", ipc_addr, e);
                Err(e)
            }
        }
    }
    
    pub fn start_receiving(&mut self) {
        info!("ZmqReceiver started, listening on ipc://{}", self.zmq_config.ipc_path);
        
        loop {
            // 检查关闭信号
            if *self.receiver_shutdown_rx.borrow() {
                info!("ZmqReceiver received shutdown signal, stopping...");
                break;
            }
            
            // 接收消息
            match self.ipc_socket.recv_bytes(zmq::DONTWAIT) {
                Ok(msg) => {
                    self.receive_count += 1;
                    self.process_message(&msg);
                }
                Err(e) => {
                    if e == zmq::Error::EAGAIN {
                        // 没有消息可接收，短暂睡眠后继续
                        std::thread::sleep(Duration::from_millis(1));
                    } else {
                        error!("Error receiving message: {}", e);
                        std::thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        }
        info!("ZmqReceiver stopped gracefully");
    }
    
    fn process_message(&self, _msg: &[u8]) {
        // 处理接收到的消息
    }
}

impl Drop for ZmqReceiver {
    fn drop(&mut self) {
        info!("ZmqReceiver stopping...");
        let ipc_addr = format!("ipc://{}", self.zmq_config.ipc_path);
        
        if let Err(e) = self.ipc_socket.unbind(&ipc_addr) {
            warn!("Failed to unbind IPC socket: {}", e);
        } else {
            info!("ZmqReceiver ipc_socket unbind success");
        }
        
        info!("ZmqReceiver stopped");
    }
}
