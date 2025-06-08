//测试用的zmq receiver
use zmq::{Context, Socket, SocketType};
use crate::cfg::{Config, ZmqProxyCfg};
use log::{info, warn, error};
use tokio::time::{sleep, Duration};
use tokio::sync::watch;
/// 接收器，用于接收zmq消息，用于测试
pub struct ZmqReceiver{
    zmq_config: ZmqProxyCfg,
    #[allow(dead_code)]
    context: Context,
    ipc_socket: Socket,
    receive_count: u64,
    global_shutdown: watch::Receiver<bool>,
}

impl ZmqReceiver {
    pub fn new(config: &Config, global_shutdown: watch::Receiver<bool>) -> Result<Self, zmq::Error> {
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
            global_shutdown: global_shutdown,
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
    
    pub async fn start_receiving(&mut self) {
        info!("ZmqReceiver started, listening on ipc://{}", self.zmq_config.ipc_path);
        
        loop {
            let mut global_shutdown_clone = self.global_shutdown.clone();
            tokio::select! {
                _ = global_shutdown_clone.changed() => {
                    if *global_shutdown_clone.borrow() {
                        info!("ZmqReceiver received shutdown signal, stopping...");
                        break;
                    }
                }
                _ = async {
                    match self.ipc_socket.recv_bytes(zmq::DONTWAIT) {
                        Ok(msg) => {
                            self.receive_count += 1;
                            self.process_message(&msg).await;
                        }
                        Err(e) => {
                            if e == zmq::Error::EAGAIN {
                                // 没有消息可接收，短暂睡眠后继续
                                sleep(Duration::from_millis(1)).await;
                            } else {
                                error!("Error receiving message: {}", e);
                                sleep(Duration::from_millis(100)).await;
                            }
                        }
                    }
                } => {}
            }
        }
        info!("ZmqReceiver stopped gracefully");
    }
    
    async fn process_message(&self, _msg: &[u8]) {
        //
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
