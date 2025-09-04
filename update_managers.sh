#!/bin/bash

# 更新mkt_manager.rs
sed -i '' 's/use tokio::sync::{broadcast, watch, Notify};/use tokio::sync::{mpsc, watch, Notify};/' src/connection/mkt_manager.rs
sed -i '' 's/broadcast::Sender<Bytes>/mpsc::UnboundedSender<Bytes>/g' src/connection/mkt_manager.rs
sed -i '' 's/broadcast::channel(.*)/mpsc::unbounded_channel()/g' src/connection/mkt_manager.rs
sed -i '' 's/broadcast::error::RecvError/mpsc::error::TryRecvError/g' src/connection/mkt_manager.rs
sed -i '' 's/raw_rx.recv()/raw_rx.recv()/g' src/connection/mkt_manager.rs
sed -i '' 's/snapshot_raw_rx.recv()/snapshot_raw_rx.recv()/g' src/connection/mkt_manager.rs

# 更新kline_manager.rs  
sed -i '' 's/use tokio::sync::{broadcast, watch};/use tokio::sync::{mpsc, watch};/' src/connection/kline_manager.rs
sed -i '' 's/broadcast::Sender<Bytes>/mpsc::UnboundedSender<Bytes>/g' src/connection/kline_manager.rs
sed -i '' 's/broadcast::channel(.*)/mpsc::unbounded_channel()/g' src/connection/kline_manager.rs
sed -i '' 's/broadcast::error::RecvError/mpsc::error::TryRecvError/g' src/connection/kline_manager.rs

echo "Updated manager files to use mpsc instead of broadcast"
