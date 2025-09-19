use bytes::{Bytes, BytesMut, Buf, BufMut};

//币安单所正向套利策略信号上下文，开仓和平仓使用
#[derive(Clone, Debug)]
pub struct BinSingleForwardArbSigCtx {
    pub spot_symbol: String,      // 现货交易symbol
    pub futures_symbol: String,   // 合约交易symbol  
    pub spot_update_id: u64,      // idx
    pub future_update_time: i64,  // exchange time
    pub spot_best: [f64; 2],      // 现货最优 [bid, ask]
    pub future_best: [f64; 2],    // 期货最优 [bid, ask]
}

//币安单所正向套利信号，对冲执行上下文
#[derive(Clone, Debug)]
pub struct BinSingleForwardArbHedgeCtx {
    pub spot_symbol: String,      // 现货交易symbol
    pub futures_symbol: String,   // 合约交易symbol  
    pub spot_update_id: u64,      // idx
    pub future_update_time: i64,  // exchange time
    pub spot_best: [f64; 2],      // 现货最优 [bid, ask]
    pub future_best: [f64; 2],    // 期货最优 [bid, ask]
}

impl BinSingleForwardArbSigCtx {
    pub fn new(
        spot_symbol: String,
        futures_symbol: String,
        spot_update_id: u64,
        future_update_time: i64,
        spot_best: [f64; 2],
        future_best: [f64; 2],
    ) -> Self {
        BinSingleForwardArbSigCtx {
            spot_symbol,
            futures_symbol,
            spot_update_id,
            future_update_time,
            spot_best,
            future_best,
        }
    }
    
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        
        // Write spot_symbol
        buf.put_u32_le(self.spot_symbol.len() as u32);
        buf.put_slice(self.spot_symbol.as_bytes());
        
        // Write futures_symbol
        buf.put_u32_le(self.futures_symbol.len() as u32);
        buf.put_slice(self.futures_symbol.as_bytes());
        
        // Write spot_update_id
        buf.put_u64_le(self.spot_update_id);
        
        // Write future_update_time
        buf.put_i64_le(self.future_update_time);
        
        // Write spot_best
        buf.put_f64_le(self.spot_best[0]);
        buf.put_f64_le(self.spot_best[1]);
        
        // Write future_best
        buf.put_f64_le(self.future_best[0]);
        buf.put_f64_le(self.future_best[1]);
        
        buf.freeze()
    }
    
    pub fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        // Read spot_symbol
        if bytes.remaining() < 4 {
            return Err("Not enough bytes for spot_symbol length".to_string());
        }
        let spot_symbol_len = bytes.get_u32_le() as usize;
        if bytes.remaining() < spot_symbol_len {
            return Err("Not enough bytes for spot_symbol".to_string());
        }
        let spot_symbol = String::from_utf8(bytes.copy_to_bytes(spot_symbol_len).to_vec())
            .map_err(|e| format!("Invalid UTF-8 for spot_symbol: {}", e))?;
        
        // Read futures_symbol
        if bytes.remaining() < 4 {
            return Err("Not enough bytes for futures_symbol length".to_string());
        }
        let futures_symbol_len = bytes.get_u32_le() as usize;
        if bytes.remaining() < futures_symbol_len {
            return Err("Not enough bytes for futures_symbol".to_string());
        }
        let futures_symbol = String::from_utf8(bytes.copy_to_bytes(futures_symbol_len).to_vec())
            .map_err(|e| format!("Invalid UTF-8 for futures_symbol: {}", e))?;
        
        // Read spot_update_id
        if bytes.remaining() < 8 {
            return Err("Not enough bytes for spot_update_id".to_string());
        }
        let spot_update_id = bytes.get_u64_le();
        
        // Read future_update_time
        if bytes.remaining() < 8 {
            return Err("Not enough bytes for future_update_time".to_string());
        }
        let future_update_time = bytes.get_i64_le();
        
        // Read spot_best
        if bytes.remaining() < 16 {
            return Err("Not enough bytes for spot_best".to_string());
        }
        let spot_best = [bytes.get_f64_le(), bytes.get_f64_le()];
        
        // Read future_best
        if bytes.remaining() < 16 {
            return Err("Not enough bytes for future_best".to_string());
        }
        let future_best = [bytes.get_f64_le(), bytes.get_f64_le()];
        
        Ok(BinSingleForwardArbSigContext {
            spot_symbol,
            futures_symbol,
            spot_update_id,
            future_update_time,
            spot_best,
            future_best,
        })
    }
}