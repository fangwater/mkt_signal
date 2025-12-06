use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum AccountEventType {
    ConditionalOrderUpdate = 2001,
    OpenOrderLoss = 2002,
    AccountPosition = 2003,
    LiabilityChange = 2004,
    ExecutionReport = 2005,
    OrderTradeUpdate = 2006,
    AccountUpdateBalance = 2007,
    AccountUpdatePosition = 2008,
    AccountConfigUpdate = 2009,
    BalanceUpdate = 2010,
    AccountUpdateFlush = 2011,
    OkexBalanceSnapshot = 2012,
    Error = 3333,
}

#[allow(dead_code)]
pub struct AccountEventMsg {
    pub msg_type: AccountEventType,
    pub msg_length: u32,
    pub data: Bytes,
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct ConditionalOrderMsg {
    pub msg_type: AccountEventType,
    pub symbol_length: u32,
    pub symbol: String,
    pub strategy_id: i64,
    pub order_id: i64,
    pub trade_time: i64,
    pub event_time: i64,
    pub update_time: i64,
    pub side: char,
    pub position_side: char,
    pub reduce_only: bool,
    pub close_position: bool,
    pub padding: [u8; 4],
    pub quantity: f64,
    pub price: f64,
    pub stop_price: f64,
    pub activation_price: f64,
    pub callback_rate: f64,
    pub strategy_type_length: u32,
    pub strategy_type: String,
    pub order_status_length: u32,
    pub order_status: String,
    pub custom_id_length: u32,
    pub custom_id: String,
    pub time_in_force_length: u32,
    pub time_in_force: String,
    pub trigger_type_length: u32,
    pub trigger_type: String,
}

impl ConditionalOrderMsg {
    pub fn create(
        symbol: String,
        strategy_id: i64,
        order_id: i64,
        trade_time: i64,
        event_time: i64,
        update_time: i64,
        side: char,
        position_side: char,
        reduce_only: bool,
        close_position: bool,
        quantity: f64,
        price: f64,
        stop_price: f64,
        activation_price: f64,
        callback_rate: f64,
        strategy_type: String,
        order_status: String,
        custom_id: String,
        time_in_force: String,
        trigger_type: String,
    ) -> Self {
        Self {
            msg_type: AccountEventType::ConditionalOrderUpdate,
            symbol_length: symbol.len() as u32,
            symbol,
            strategy_id,
            order_id,
            trade_time,
            event_time,
            update_time,
            side,
            position_side,
            reduce_only,
            close_position,
            padding: [0u8; 4],
            quantity,
            price,
            stop_price,
            activation_price,
            callback_rate,
            strategy_type_length: strategy_type.len() as u32,
            strategy_type,
            order_status_length: order_status.len() as u32,
            order_status,
            custom_id_length: custom_id.len() as u32,
            custom_id,
            time_in_force_length: time_in_force.len() as u32,
            time_in_force,
            trigger_type_length: trigger_type.len() as u32,
            trigger_type,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4
            + 4
            + self.symbol_length as usize
            + 8 * 5
            + 8
            + 8 * 5
            + 4
            + self.strategy_type_length as usize
            + 4
            + self.order_status_length as usize
            + 4
            + self.custom_id_length as usize
            + 4
            + self.time_in_force_length as usize
            + 4
            + self.trigger_type_length as usize;

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.msg_type as u32);
        buf.put_u32_le(self.symbol_length);
        buf.put(self.symbol.as_bytes());

        buf.put_i64_le(self.strategy_id);
        buf.put_i64_le(self.order_id);
        buf.put_i64_le(self.trade_time);
        buf.put_i64_le(self.event_time);
        buf.put_i64_le(self.update_time);

        buf.put_u8(self.side as u8);
        buf.put_u8(self.position_side as u8);
        buf.put_u8(if self.reduce_only { 1 } else { 0 });
        buf.put_u8(if self.close_position { 1 } else { 0 });
        buf.put(&self.padding[..]);

        buf.put_f64_le(self.quantity);
        buf.put_f64_le(self.price);
        buf.put_f64_le(self.stop_price);
        buf.put_f64_le(self.activation_price);
        buf.put_f64_le(self.callback_rate);

        buf.put_u32_le(self.strategy_type_length);
        buf.put(self.strategy_type.as_bytes());

        buf.put_u32_le(self.order_status_length);
        buf.put(self.order_status.as_bytes());

        buf.put_u32_le(self.custom_id_length);
        buf.put(self.custom_id.as_bytes());

        buf.put_u32_le(self.time_in_force_length);
        buf.put(self.time_in_force.as_bytes());

        buf.put_u32_le(self.trigger_type_length);
        buf.put(self.trigger_type.as_bytes());

        buf.freeze()
    }
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct OpenOrderLossMsg {
    pub msg_type: AccountEventType,
    pub event_time: i64,
    pub asset_length: u32,
    pub padding: [u8; 4],
    pub asset: String,
    pub amount: f64,
}

impl OpenOrderLossMsg {
    pub fn create(event_time: i64, asset: String, amount: f64) -> Self {
        Self {
            msg_type: AccountEventType::OpenOrderLoss,
            event_time,
            asset_length: asset.len() as u32,
            padding: [0u8; 4],
            asset,
            amount,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4 + 8 + 4 + 4 + self.asset_length as usize + 8;

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.event_time);
        buf.put_u32_le(self.asset_length);
        buf.put(&self.padding[..]);
        buf.put(self.asset.as_bytes());
        buf.put_f64_le(self.amount);

        buf.freeze()
    }
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct AccountPositionMsg {
    pub msg_type: AccountEventType,
    pub event_time: i64,
    pub update_time: i64,
    pub update_id: i64,
    pub asset_length: u32,
    pub padding: [u8; 4],
    pub asset: String,
    pub free_balance: f64,
    pub locked_balance: f64,
}

impl AccountPositionMsg {
    pub fn create(
        event_time: i64,
        update_time: i64,
        update_id: i64,
        asset: String,
        free_balance: f64,
        locked_balance: f64,
    ) -> Self {
        Self {
            msg_type: AccountEventType::AccountPosition,
            event_time,
            update_time,
            update_id,
            asset_length: asset.len() as u32,
            padding: [0u8; 4],
            asset,
            free_balance,
            locked_balance,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4 + 8 + 8 + 8 + 4 + 4 + self.asset_length as usize + 8 + 8;

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.event_time);
        buf.put_i64_le(self.update_time);
        buf.put_i64_le(self.update_id);
        buf.put_u32_le(self.asset_length);
        buf.put(&self.padding[..]);
        buf.put(self.asset.as_bytes());
        buf.put_f64_le(self.free_balance);
        buf.put_f64_le(self.locked_balance);

        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + 8 + 8 + 8 + 4 + 4 + 8 + 8;
        if data.len() < MIN_SIZE {
            anyhow::bail!("account position msg too short: {}", data.len());
        }

        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != AccountEventType::AccountPosition as u32 {
            anyhow::bail!("invalid account position msg type: {}", msg_type);
        }

        let event_time = cursor.get_i64_le();
        let update_time = cursor.get_i64_le();
        let update_id = cursor.get_i64_le();
        let asset_length = cursor.get_u32_le();
        let mut padding = [0u8; 4];
        cursor.copy_to_slice(&mut padding);

        if cursor.remaining() < asset_length as usize + 8 + 8 {
            anyhow::bail!("account position truncated before asset/balances");
        }
        let asset = String::from_utf8(cursor.copy_to_bytes(asset_length as usize).to_vec())?;
        let free_balance = cursor.get_f64_le();
        let locked_balance = cursor.get_f64_le();

        Ok(Self {
            msg_type: AccountEventType::AccountPosition,
            event_time,
            update_time,
            update_id,
            asset_length,
            padding,
            asset,
            free_balance,
            locked_balance,
        })
    }
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct LiabilityChangeMsg {
    pub msg_type: AccountEventType,
    pub event_time: i64,
    pub transaction_id: i64,
    pub asset_length: u32,
    pub type_length: u32,
    pub asset: String,
    pub liability_type: String,
    pub principal: f64,
    pub interest: f64,
    pub total_liability: f64,
}

impl LiabilityChangeMsg {
    pub fn create(
        event_time: i64,
        transaction_id: i64,
        asset: String,
        liability_type: String,
        principal: f64,
        interest: f64,
        total_liability: f64,
    ) -> Self {
        Self {
            msg_type: AccountEventType::LiabilityChange,
            event_time,
            transaction_id,
            asset_length: asset.len() as u32,
            type_length: liability_type.len() as u32,
            asset,
            liability_type,
            principal,
            interest,
            total_liability,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size =
            4 + 8 + 8 + 4 + 4 + self.asset_length as usize + self.type_length as usize + 8 + 8 + 8;

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.event_time);
        buf.put_i64_le(self.transaction_id);
        buf.put_u32_le(self.asset_length);
        buf.put_u32_le(self.type_length);
        buf.put(self.asset.as_bytes());
        buf.put(self.liability_type.as_bytes());
        buf.put_f64_le(self.principal);
        buf.put_f64_le(self.interest);
        buf.put_f64_le(self.total_liability);

        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + 8 + 8 + 4 + 4 + 8 + 8 + 8;
        if data.len() < MIN_SIZE {
            anyhow::bail!("liability change msg too short: {}", data.len());
        }

        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != AccountEventType::LiabilityChange as u32 {
            anyhow::bail!("invalid liability change msg type: {}", msg_type);
        }

        let event_time = cursor.get_i64_le();
        let transaction_id = cursor.get_i64_le();
        let asset_length = cursor.get_u32_le();
        let type_length = cursor.get_u32_le();

        if cursor.remaining() < asset_length as usize + type_length as usize + 8 + 8 + 8 {
            anyhow::bail!("liability change msg truncated");
        }

        let asset = String::from_utf8(cursor.copy_to_bytes(asset_length as usize).to_vec())?;
        let liability_type =
            String::from_utf8(cursor.copy_to_bytes(type_length as usize).to_vec())?;
        let principal = cursor.get_f64_le();
        let interest = cursor.get_f64_le();
        let total_liability = cursor.get_f64_le();

        Ok(Self {
            msg_type: AccountEventType::LiabilityChange,
            event_time,
            transaction_id,
            asset_length,
            type_length,
            asset,
            liability_type,
            principal,
            interest,
            total_liability,
        })
    }
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct ExecutionReportMsg {
    pub msg_type: AccountEventType,
    pub event_time: i64,
    pub transaction_time: i64,
    pub order_id: i64,
    pub trade_id: i64,
    pub order_creation_time: i64,
    pub working_time: i64,
    pub update_id: i64,
    pub symbol_length: u32,
    pub symbol: String,
    pub client_order_id: i64,
    pub side: char, // 'B' for BUY, 'S' for SELL
    pub is_maker: bool,
    pub is_working: bool,
    pub padding: [u8; 6],
    pub price: f64,
    pub quantity: f64,
    pub last_executed_quantity: f64,
    pub cumulative_filled_quantity: f64,
    pub last_executed_price: f64,
    pub commission_amount: f64,
    pub cumulative_quote: f64,
    pub last_quote: f64,
    pub quote_order_quantity: f64,
    pub order_type_length: u32,
    pub order_type: String,
    pub time_in_force_length: u32,
    pub time_in_force: String,
    pub execution_type_length: u32,
    pub execution_type: String,
    pub order_status_length: u32,
    pub order_status: String,
    pub commission_asset_length: u32,
    pub commission_asset: String,
    // extension: raw clientOrderId string (optional)
    pub client_order_id_str_length: u32,
    pub client_order_id_str: String,
}

impl ExecutionReportMsg {
    pub fn create(
        event_time: i64,
        transaction_time: i64,
        order_id: i64,
        trade_id: i64,
        order_creation_time: i64,
        working_time: i64,
        update_id: i64,
        symbol: String,
        client_order_id: i64,
        side: char,
        is_maker: bool,
        is_working: bool,
        price: f64,
        quantity: f64,
        last_executed_quantity: f64,
        cumulative_filled_quantity: f64,
        last_executed_price: f64,
        commission_amount: f64,
        cumulative_quote: f64,
        last_quote: f64,
        quote_order_quantity: f64,
        order_type: String,
        time_in_force: String,
        execution_type: String,
        order_status: String,
        commission_asset: String,
        client_order_id_str: Option<String>,
    ) -> Self {
        let client_order_id_str = client_order_id_str.unwrap_or_default();
        Self {
            msg_type: AccountEventType::ExecutionReport,
            event_time,
            transaction_time,
            order_id,
            trade_id,
            order_creation_time,
            working_time,
            update_id,
            symbol_length: symbol.len() as u32,
            symbol,
            client_order_id,
            side,
            is_maker,
            is_working,
            padding: [0u8; 6],
            price,
            quantity,
            last_executed_quantity,
            cumulative_filled_quantity,
            last_executed_price,
            commission_amount,
            cumulative_quote,
            last_quote,
            quote_order_quantity,
            order_type_length: order_type.len() as u32,
            order_type,
            time_in_force_length: time_in_force.len() as u32,
            time_in_force,
            execution_type_length: execution_type.len() as u32,
            execution_type,
            order_status_length: order_status.len() as u32,
            order_status,
            commission_asset_length: commission_asset.len() as u32,
            commission_asset,
            client_order_id_str_length: client_order_id_str.len() as u32,
            client_order_id_str,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4
            + 8 * 8
            + 4
            + self.symbol_length as usize
            + 8
            + 1
            + 1
            + 1
            + 6
            + 8 * 9
            + 4
            + self.order_type_length as usize
            + 4
            + self.time_in_force_length as usize
            + 4
            + self.execution_type_length as usize
            + 4
            + self.order_status_length as usize
            + 4
            + self.commission_asset_length as usize
            + 4
            + self.client_order_id_str_length as usize;

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.event_time);
        buf.put_i64_le(self.transaction_time);
        buf.put_i64_le(self.order_id);
        buf.put_i64_le(self.trade_id);
        buf.put_i64_le(self.order_creation_time);
        buf.put_i64_le(self.working_time);
        buf.put_i64_le(self.update_id);

        buf.put_u32_le(self.symbol_length);
        buf.put(self.symbol.as_bytes());

        buf.put_i64_le(self.client_order_id);

        buf.put_u8(self.side as u8);
        buf.put_u8(if self.is_maker { 1 } else { 0 });
        buf.put_u8(if self.is_working { 1 } else { 0 });
        buf.put(&self.padding[..]);

        buf.put_f64_le(self.price);
        buf.put_f64_le(self.quantity);
        buf.put_f64_le(self.last_executed_quantity);
        buf.put_f64_le(self.cumulative_filled_quantity);
        buf.put_f64_le(self.last_executed_price);
        buf.put_f64_le(self.commission_amount);
        buf.put_f64_le(self.cumulative_quote);
        buf.put_f64_le(self.last_quote);
        buf.put_f64_le(self.quote_order_quantity);

        buf.put_u32_le(self.order_type_length);
        buf.put(self.order_type.as_bytes());

        buf.put_u32_le(self.time_in_force_length);
        buf.put(self.time_in_force.as_bytes());

        buf.put_u32_le(self.execution_type_length);
        buf.put(self.execution_type.as_bytes());

        buf.put_u32_le(self.order_status_length);
        buf.put(self.order_status.as_bytes());

        buf.put_u32_le(self.commission_asset_length);
        buf.put(self.commission_asset.as_bytes());

        // extension
        buf.put_u32_le(self.client_order_id_str_length);
        buf.put(self.client_order_id_str.as_bytes());

        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + 8 * 8 + 4 + 1 + 1 + 1 + 6 + 8 * 9 + 4 * 5;
        if data.len() < MIN_SIZE {
            anyhow::bail!("execution report msg too short: {}", data.len());
        }

        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != AccountEventType::ExecutionReport as u32 {
            anyhow::bail!("invalid execution report msg type: {}", msg_type);
        }

        let event_time = cursor.get_i64_le();
        let transaction_time = cursor.get_i64_le();
        let order_id = cursor.get_i64_le();
        let trade_id = cursor.get_i64_le();
        let order_creation_time = cursor.get_i64_le();
        let working_time = cursor.get_i64_le();
        let update_id = cursor.get_i64_le();

        let symbol_length = cursor.get_u32_le();

        if cursor.remaining() < symbol_length as usize + 1 + 1 + 1 + 6 + 8 {
            anyhow::bail!("execution report truncated before symbol/client_order_id");
        }

        let symbol = String::from_utf8(cursor.copy_to_bytes(symbol_length as usize).to_vec())?;
        let client_order_id = cursor.get_i64_le();

        let side = cursor.get_u8() as char;
        let is_maker = cursor.get_u8() != 0;
        let is_working = cursor.get_u8() != 0;
        let mut padding = [0u8; 6];
        cursor.copy_to_slice(&mut padding);

        if cursor.remaining() < 8 * 9 + 4 {
            anyhow::bail!("execution report truncated before numeric fields");
        }

        let price = cursor.get_f64_le();
        let quantity = cursor.get_f64_le();
        let last_executed_quantity = cursor.get_f64_le();
        let cumulative_filled_quantity = cursor.get_f64_le();
        let last_executed_price = cursor.get_f64_le();
        let commission_amount = cursor.get_f64_le();
        let cumulative_quote = cursor.get_f64_le();
        let last_quote = cursor.get_f64_le();
        let quote_order_quantity = cursor.get_f64_le();

        let order_type_length = cursor.get_u32_le();
        if cursor.remaining() < order_type_length as usize + 4 {
            anyhow::bail!("execution report truncated before order_type");
        }
        let order_type =
            String::from_utf8(cursor.copy_to_bytes(order_type_length as usize).to_vec())?;

        let time_in_force_length = cursor.get_u32_le();
        if cursor.remaining() < time_in_force_length as usize + 4 {
            anyhow::bail!("execution report truncated before time_in_force");
        }
        let time_in_force =
            String::from_utf8(cursor.copy_to_bytes(time_in_force_length as usize).to_vec())?;

        let execution_type_length = cursor.get_u32_le();
        if cursor.remaining() < execution_type_length as usize + 4 {
            anyhow::bail!("execution report truncated before execution_type");
        }
        let execution_type = String::from_utf8(
            cursor
                .copy_to_bytes(execution_type_length as usize)
                .to_vec(),
        )?;

        let order_status_length = cursor.get_u32_le();
        if cursor.remaining() < order_status_length as usize + 4 {
            anyhow::bail!("execution report truncated before order_status");
        }
        let order_status =
            String::from_utf8(cursor.copy_to_bytes(order_status_length as usize).to_vec())?;

        let commission_asset_length = cursor.get_u32_le();
        if cursor.remaining() < commission_asset_length as usize {
            anyhow::bail!("execution report truncated before commission_asset");
        }
        let commission_asset = String::from_utf8(
            cursor
                .copy_to_bytes(commission_asset_length as usize)
                .to_vec(),
        )?;

        // optional extension
        let (client_order_id_str_length, client_order_id_str) = if cursor.remaining() >= 4 {
            let len = cursor.get_u32_le();
            if cursor.remaining() < len as usize {
                anyhow::bail!(
                    "execution report truncated before client_order_id_str: need {} have {}",
                    len,
                    cursor.remaining()
                );
            }
            let s = String::from_utf8(cursor.copy_to_bytes(len as usize).to_vec())?;
            (len, s)
        } else {
            (0, String::new())
        };

        Ok(Self {
            msg_type: AccountEventType::ExecutionReport,
            event_time,
            transaction_time,
            order_id,
            trade_id,
            order_creation_time,
            working_time,
            update_id,
            symbol_length,
            symbol,
            client_order_id,
            side,
            is_maker,
            is_working,
            padding,
            price,
            quantity,
            last_executed_quantity,
            cumulative_filled_quantity,
            last_executed_price,
            commission_amount,
            cumulative_quote,
            last_quote,
            quote_order_quantity,
            order_type_length,
            order_type,
            time_in_force_length,
            time_in_force,
            execution_type_length,
            execution_type,
            order_status_length,
            order_status,
            commission_asset_length,
            commission_asset,
            client_order_id_str_length,
            client_order_id_str,
        })
    }
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct OrderTradeUpdateMsg {
    pub msg_type: AccountEventType,
    pub event_time: i64,
    pub transaction_time: i64,
    pub order_id: i64,
    pub trade_id: i64,
    pub strategy_id: i64,
    pub symbol_length: u32,
    pub symbol: String,
    pub client_order_id: i64,
    pub side: char,
    pub position_side: char,
    pub is_maker: bool,
    pub reduce_only: bool,
    pub padding: [u8; 4],
    pub price: f64,
    pub quantity: f64,
    pub average_price: f64,
    pub stop_price: f64,
    pub last_executed_quantity: f64,
    pub cumulative_filled_quantity: f64,
    pub last_executed_price: f64,
    pub commission_amount: f64,
    pub buy_notional: f64,
    pub sell_notional: f64,
    pub realized_profit: f64,
    pub order_type_length: u32,
    pub order_type: String,
    pub time_in_force_length: u32,
    pub time_in_force: String,
    pub execution_type_length: u32,
    pub execution_type: String,
    pub order_status_length: u32,
    pub order_status: String,
    pub commission_asset_length: u32,
    pub commission_asset: String,
    pub strategy_type_length: u32,
    pub strategy_type: String,
    pub business_unit_length: u32,
    pub business_unit: String,
    // extension: raw clientOrderId string (optional)
    pub client_order_id_str_length: u32,
    pub client_order_id_str: String,
}

impl OrderTradeUpdateMsg {
    pub fn create(
        event_time: i64,
        transaction_time: i64,
        order_id: i64,
        trade_id: i64,
        strategy_id: i64,
        symbol: String,
        client_order_id: i64,
        side: char,
        position_side: char,
        is_maker: bool,
        reduce_only: bool,
        price: f64,
        quantity: f64,
        average_price: f64,
        stop_price: f64,
        last_executed_quantity: f64,
        cumulative_filled_quantity: f64,
        last_executed_price: f64,
        commission_amount: f64,
        buy_notional: f64,
        sell_notional: f64,
        realized_profit: f64,
        order_type: String,
        time_in_force: String,
        execution_type: String,
        order_status: String,
        commission_asset: String,
        strategy_type: String,
        business_unit: String,
        client_order_id_str: Option<String>,
    ) -> Self {
        let client_order_id_str = client_order_id_str.unwrap_or_default();
        Self {
            msg_type: AccountEventType::OrderTradeUpdate,
            event_time,
            transaction_time,
            order_id,
            trade_id,
            strategy_id,
            symbol_length: symbol.len() as u32,
            symbol,
            client_order_id,
            side,
            position_side,
            is_maker,
            reduce_only,
            padding: [0u8; 4],
            price,
            quantity,
            average_price,
            stop_price,
            last_executed_quantity,
            cumulative_filled_quantity,
            last_executed_price,
            commission_amount,
            buy_notional,
            sell_notional,
            realized_profit,
            order_type_length: order_type.len() as u32,
            order_type,
            time_in_force_length: time_in_force.len() as u32,
            time_in_force,
            execution_type_length: execution_type.len() as u32,
            execution_type,
            order_status_length: order_status.len() as u32,
            order_status,
            commission_asset_length: commission_asset.len() as u32,
            commission_asset,
            strategy_type_length: strategy_type.len() as u32,
            strategy_type,
            business_unit_length: business_unit.len() as u32,
            business_unit,
            client_order_id_str_length: client_order_id_str.len() as u32,
            client_order_id_str,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4
            + 8 * 6
            + 4
            + self.symbol_length as usize
            + 8
            + 1
            + 1
            + 1
            + 1
            + 4
            + 8 * 11
            + 4
            + self.order_type_length as usize
            + 4
            + self.time_in_force_length as usize
            + 4
            + self.execution_type_length as usize
            + 4
            + self.order_status_length as usize
            + 4
            + self.commission_asset_length as usize
            + 4
            + self.strategy_type_length as usize
            + 4
            + self.business_unit_length as usize
            + 4
            + self.client_order_id_str_length as usize;

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.event_time);
        buf.put_i64_le(self.transaction_time);
        buf.put_i64_le(self.order_id);
        buf.put_i64_le(self.trade_id);
        buf.put_i64_le(self.strategy_id);

        buf.put_u32_le(self.symbol_length);
        buf.put(self.symbol.as_bytes());

        buf.put_i64_le(self.client_order_id);

        buf.put_u8(self.side as u8);
        buf.put_u8(self.position_side as u8);
        buf.put_u8(if self.is_maker { 1 } else { 0 });
        buf.put_u8(if self.reduce_only { 1 } else { 0 });
        buf.put(&self.padding[..]);

        buf.put_f64_le(self.price);
        buf.put_f64_le(self.quantity);
        buf.put_f64_le(self.average_price);
        buf.put_f64_le(self.stop_price);
        buf.put_f64_le(self.last_executed_quantity);
        buf.put_f64_le(self.cumulative_filled_quantity);
        buf.put_f64_le(self.last_executed_price);
        buf.put_f64_le(self.commission_amount);
        buf.put_f64_le(self.buy_notional);
        buf.put_f64_le(self.sell_notional);
        buf.put_f64_le(self.realized_profit);

        buf.put_u32_le(self.order_type_length);
        buf.put(self.order_type.as_bytes());

        buf.put_u32_le(self.time_in_force_length);
        buf.put(self.time_in_force.as_bytes());

        buf.put_u32_le(self.execution_type_length);
        buf.put(self.execution_type.as_bytes());

        buf.put_u32_le(self.order_status_length);
        buf.put(self.order_status.as_bytes());

        buf.put_u32_le(self.commission_asset_length);
        buf.put(self.commission_asset.as_bytes());

        buf.put_u32_le(self.strategy_type_length);
        buf.put(self.strategy_type.as_bytes());

        buf.put_u32_le(self.business_unit_length);
        buf.put(self.business_unit.as_bytes());

        // extension
        buf.put_u32_le(self.client_order_id_str_length);
        buf.put(self.client_order_id_str.as_bytes());

        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + 8 * 6 + 4 + 8 + 1 + 1 + 1 + 1 + 4 + 8 * 11 + 4 * 7;
        if data.len() < MIN_SIZE {
            anyhow::bail!("order trade update msg too short: {}", data.len());
        }

        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != AccountEventType::OrderTradeUpdate as u32 {
            anyhow::bail!("invalid order trade update msg type: {}", msg_type);
        }

        let event_time = cursor.get_i64_le();
        let transaction_time = cursor.get_i64_le();
        let order_id = cursor.get_i64_le();
        let trade_id = cursor.get_i64_le();
        let strategy_id = cursor.get_i64_le();

        let symbol_length = cursor.get_u32_le();

        if cursor.remaining() < symbol_length as usize + 1 + 1 + 1 + 1 + 4 + 8 {
            anyhow::bail!("order trade update truncated before symbol/client order id");
        }

        let symbol = String::from_utf8(cursor.copy_to_bytes(symbol_length as usize).to_vec())?;
        let client_order_id = cursor.get_i64_le();

        let side = cursor.get_u8() as char;
        let position_side = cursor.get_u8() as char;
        let is_maker = cursor.get_u8() != 0;
        let reduce_only = cursor.get_u8() != 0;
        let mut padding = [0u8; 4];
        cursor.copy_to_slice(&mut padding);

        if cursor.remaining() < 8 * 11 + 4 {
            anyhow::bail!("order trade update truncated before numeric fields");
        }

        let price = cursor.get_f64_le();
        let quantity = cursor.get_f64_le();
        let average_price = cursor.get_f64_le();
        let stop_price = cursor.get_f64_le();
        let last_executed_quantity = cursor.get_f64_le();
        let cumulative_filled_quantity = cursor.get_f64_le();
        let last_executed_price = cursor.get_f64_le();
        let commission_amount = cursor.get_f64_le();
        let buy_notional = cursor.get_f64_le();
        let sell_notional = cursor.get_f64_le();
        let realized_profit = cursor.get_f64_le();

        let order_type_length = cursor.get_u32_le();
        if cursor.remaining() < order_type_length as usize + 4 {
            anyhow::bail!("order trade update truncated before order_type");
        }
        let order_type =
            String::from_utf8(cursor.copy_to_bytes(order_type_length as usize).to_vec())?;

        let time_in_force_length = cursor.get_u32_le();
        if cursor.remaining() < time_in_force_length as usize + 4 {
            anyhow::bail!("order trade update truncated before time_in_force");
        }
        let time_in_force =
            String::from_utf8(cursor.copy_to_bytes(time_in_force_length as usize).to_vec())?;

        let execution_type_length = cursor.get_u32_le();
        if cursor.remaining() < execution_type_length as usize + 4 {
            anyhow::bail!("order trade update truncated before execution_type");
        }
        let execution_type = String::from_utf8(
            cursor
                .copy_to_bytes(execution_type_length as usize)
                .to_vec(),
        )?;

        let order_status_length = cursor.get_u32_le();
        if cursor.remaining() < order_status_length as usize + 4 {
            anyhow::bail!("order trade update truncated before order_status");
        }
        let order_status =
            String::from_utf8(cursor.copy_to_bytes(order_status_length as usize).to_vec())?;

        let commission_asset_length = cursor.get_u32_le();
        if cursor.remaining() < commission_asset_length as usize + 4 {
            anyhow::bail!("order trade update truncated before commission_asset");
        }
        let commission_asset = String::from_utf8(
            cursor
                .copy_to_bytes(commission_asset_length as usize)
                .to_vec(),
        )?;

        let strategy_type_length = cursor.get_u32_le();
        if cursor.remaining() < strategy_type_length as usize + 4 {
            anyhow::bail!("order trade update truncated before strategy_type");
        }
        let strategy_type =
            String::from_utf8(cursor.copy_to_bytes(strategy_type_length as usize).to_vec())?;

        let business_unit_length = cursor.get_u32_le();
        if cursor.remaining() < business_unit_length as usize {
            anyhow::bail!("order trade update truncated before business_unit");
        }
        let business_unit =
            String::from_utf8(cursor.copy_to_bytes(business_unit_length as usize).to_vec())?;

        // optional extension
        let (client_order_id_str_length, client_order_id_str) = if cursor.remaining() >= 4 {
            let len = cursor.get_u32_le();
            if cursor.remaining() < len as usize {
                anyhow::bail!(
                    "order trade update truncated before client_order_id_str: need {} have {}",
                    len,
                    cursor.remaining()
                );
            }
            let s = String::from_utf8(cursor.copy_to_bytes(len as usize).to_vec())?;
            (len, s)
        } else {
            (0, String::new())
        };

        Ok(Self {
            msg_type: AccountEventType::OrderTradeUpdate,
            event_time,
            transaction_time,
            order_id,
            trade_id,
            strategy_id,
            symbol_length,
            symbol,
            client_order_id,
            side,
            position_side,
            is_maker,
            reduce_only,
            padding,
            price,
            quantity,
            average_price,
            stop_price,
            last_executed_quantity,
            cumulative_filled_quantity,
            last_executed_price,
            commission_amount,
            buy_notional,
            sell_notional,
            realized_profit,
            order_type_length,
            order_type,
            time_in_force_length,
            time_in_force,
            execution_type_length,
            execution_type,
            order_status_length,
            order_status,
            commission_asset_length,
            commission_asset,
            strategy_type_length,
            strategy_type,
            business_unit_length,
            business_unit,
            client_order_id_str_length,
            client_order_id_str,
        })
    }
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct AccountUpdateBalanceMsg {
    pub msg_type: AccountEventType,
    pub event_time: i64,
    pub transaction_time: i64,
    pub asset_length: u32,
    pub reason_length: u32,
    pub business_unit_length: u32,
    pub padding: [u8; 4],
    pub asset: String,
    pub reason: String,
    pub business_unit: String,
    pub wallet_balance: f64,
    pub cross_wallet_balance: f64,
    pub balance_change: f64,
}

impl AccountUpdateBalanceMsg {
    pub fn create(
        event_time: i64,
        transaction_time: i64,
        asset: String,
        reason: String,
        business_unit: String,
        wallet_balance: f64,
        cross_wallet_balance: f64,
        balance_change: f64,
    ) -> Self {
        Self {
            msg_type: AccountEventType::AccountUpdateBalance,
            event_time,
            transaction_time,
            asset_length: asset.len() as u32,
            reason_length: reason.len() as u32,
            business_unit_length: business_unit.len() as u32,
            padding: [0u8; 4],
            asset,
            reason,
            business_unit,
            wallet_balance,
            cross_wallet_balance,
            balance_change,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4
            + 8
            + 8
            + 4
            + 4
            + 4
            + 4
            + self.asset_length as usize
            + self.reason_length as usize
            + self.business_unit_length as usize
            + 8
            + 8
            + 8;

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.event_time);
        buf.put_i64_le(self.transaction_time);
        buf.put_u32_le(self.asset_length);
        buf.put_u32_le(self.reason_length);
        buf.put_u32_le(self.business_unit_length);
        buf.put(&self.padding[..]);
        buf.put(self.asset.as_bytes());
        buf.put(self.reason.as_bytes());
        buf.put(self.business_unit.as_bytes());
        buf.put_f64_le(self.wallet_balance);
        buf.put_f64_le(self.cross_wallet_balance);
        buf.put_f64_le(self.balance_change);

        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + 8 + 8 + 4 + 4 + 4 + 4 + 8 + 8 + 8;
        if data.len() < MIN_SIZE {
            anyhow::bail!("account update balance msg too short: {}", data.len());
        }

        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != AccountEventType::AccountUpdateBalance as u32 {
            anyhow::bail!("invalid account msg type: {}", msg_type);
        }

        let event_time = cursor.get_i64_le();
        let transaction_time = cursor.get_i64_le();
        let asset_length = cursor.get_u32_le();
        let reason_length = cursor.get_u32_le();
        let business_unit_length = cursor.get_u32_le();
        let mut padding = [0u8; 4];
        cursor.copy_to_slice(&mut padding);

        let expected =
            asset_length as usize + reason_length as usize + business_unit_length as usize + 24;
        if cursor.len() < expected {
            anyhow::bail!(
                "account update balance truncated: have {} expect {}",
                cursor.len(),
                expected
            );
        }

        let asset_bytes = cursor.copy_to_bytes(asset_length as usize);
        let reason_bytes = cursor.copy_to_bytes(reason_length as usize);
        let business_bytes = cursor.copy_to_bytes(business_unit_length as usize);
        let asset = String::from_utf8(asset_bytes.to_vec())?;
        let reason = String::from_utf8(reason_bytes.to_vec())?;
        let business_unit = String::from_utf8(business_bytes.to_vec())?;
        let wallet_balance = cursor.get_f64_le();
        let cross_wallet_balance = cursor.get_f64_le();
        let balance_change = cursor.get_f64_le();

        Ok(Self {
            msg_type: AccountEventType::AccountUpdateBalance,
            event_time,
            transaction_time,
            asset_length,
            reason_length,
            business_unit_length,
            padding,
            asset,
            reason,
            business_unit,
            wallet_balance,
            cross_wallet_balance,
            balance_change,
        })
    }
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct PositionMsg {
    pub msg_type: AccountEventType,
    pub event_time: i64,
    pub transaction_time: i64,
    pub symbol_length: u32,
    pub reason_length: u32,
    pub business_unit_length: u32,
    pub position_side: char,
    pub padding: [u8; 3],
    pub symbol: String,
    pub reason: String,
    pub business_unit: String,
    pub position_amount: f64,
    pub entry_price: f64,
    pub accumulated_realized: f64,
    pub unrealized_pnl: f64,
    pub breakeven_price: f64,
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct AccountUpdateFlushMsg {
    pub msg_type: AccountEventType,
    pub event_time: i64,
    pub hash: u64,
    pub scope_length: u32,
    pub padding: [u8; 4],
    pub scope: String,
}

impl AccountUpdateFlushMsg {
    pub fn create(event_time: i64, hash: u64, scope: String) -> Self {
        Self {
            msg_type: AccountEventType::AccountUpdateFlush,
            event_time,
            hash,
            scope_length: scope.len() as u32,
            padding: [0u8; 4],
            scope,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4 + 8 + 8 + 4 + 4 + self.scope_length as usize;
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.event_time);
        buf.put_u64_le(self.hash);
        buf.put_u32_le(self.scope_length);
        buf.put(&self.padding[..]);
        buf.put(self.scope.as_bytes());
        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + 8 + 8 + 4 + 4;
        if data.len() < MIN_SIZE {
            anyhow::bail!("account update flush msg too short: {}", data.len());
        }
        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != AccountEventType::AccountUpdateFlush as u32 {
            anyhow::bail!("invalid account msg type: {}", msg_type);
        }
        let event_time = cursor.get_i64_le();
        let hash = cursor.get_u64_le();
        let scope_length = cursor.get_u32_le();
        let mut padding = [0u8; 4];
        cursor.copy_to_slice(&mut padding);
        if cursor.len() < scope_length as usize {
            anyhow::bail!(
                "account update flush scope length mismatch: {}",
                scope_length
            );
        }
        let scope_bytes = cursor.copy_to_bytes(scope_length as usize);
        let scope = String::from_utf8(scope_bytes.to_vec())?;
        Ok(Self {
            msg_type: AccountEventType::AccountUpdateFlush,
            event_time,
            hash,
            scope_length,
            padding,
            scope,
        })
    }
}

impl PositionMsg {
    pub fn create(
        event_time: i64,
        transaction_time: i64,
        symbol: String,
        reason: String,
        business_unit: String,
        position_side: char,
        position_amount: f64,
        entry_price: f64,
        accumulated_realized: f64,
        unrealized_pnl: f64,
        breakeven_price: f64,
    ) -> Self {
        Self {
            msg_type: AccountEventType::AccountUpdatePosition,
            event_time,
            transaction_time,
            symbol_length: symbol.len() as u32,
            reason_length: reason.len() as u32,
            business_unit_length: business_unit.len() as u32,
            position_side,
            padding: [0u8; 3],
            symbol,
            reason,
            business_unit,
            position_amount,
            entry_price,
            accumulated_realized,
            unrealized_pnl,
            breakeven_price,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4
            + 8
            + 8
            + 4
            + 4
            + 4
            + 1
            + 3
            + self.symbol_length as usize
            + self.reason_length as usize
            + self.business_unit_length as usize
            + 8
            + 8
            + 8
            + 8
            + 8;

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.event_time);
        buf.put_i64_le(self.transaction_time);
        buf.put_u32_le(self.symbol_length);
        buf.put_u32_le(self.reason_length);
        buf.put_u32_le(self.business_unit_length);
        buf.put_u8(self.position_side as u8);
        buf.put(&self.padding[..]);
        buf.put(self.symbol.as_bytes());
        buf.put(self.reason.as_bytes());
        buf.put(self.business_unit.as_bytes());
        buf.put_f64_le(self.position_amount);
        buf.put_f64_le(self.entry_price);
        buf.put_f64_le(self.accumulated_realized);
        buf.put_f64_le(self.unrealized_pnl);
        buf.put_f64_le(self.breakeven_price);

        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + 8 + 8 + 4 + 4 + 4 + 1 + 3 + 8 * 5;
        if data.len() < MIN_SIZE {
            anyhow::bail!("account update position msg too short: {}", data.len());
        }

        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != AccountEventType::AccountUpdatePosition as u32 {
            anyhow::bail!("invalid account msg type: {}", msg_type);
        }

        let event_time = cursor.get_i64_le();
        let transaction_time = cursor.get_i64_le();
        let symbol_length = cursor.get_u32_le();
        let reason_length = cursor.get_u32_le();
        let business_unit_length = cursor.get_u32_le();
        let position_side = cursor.get_u8() as char;
        let mut padding = [0u8; 3];
        cursor.copy_to_slice(&mut padding);

        let expected =
            symbol_length as usize + reason_length as usize + business_unit_length as usize + 40;
        if cursor.len() < expected {
            anyhow::bail!(
                "account update position truncated: have {} expect {}",
                cursor.len(),
                expected
            );
        }

        let symbol_bytes = cursor.copy_to_bytes(symbol_length as usize);
        let reason_bytes = cursor.copy_to_bytes(reason_length as usize);
        let business_bytes = cursor.copy_to_bytes(business_unit_length as usize);
        let symbol = String::from_utf8(symbol_bytes.to_vec())?;
        let reason = String::from_utf8(reason_bytes.to_vec())?;
        let business_unit = String::from_utf8(business_bytes.to_vec())?;
        let position_amount = cursor.get_f64_le();
        let entry_price = cursor.get_f64_le();
        let accumulated_realized = cursor.get_f64_le();
        let unrealized_pnl = cursor.get_f64_le();
        let breakeven_price = cursor.get_f64_le();

        Ok(Self {
            msg_type: AccountEventType::AccountUpdatePosition,
            event_time,
            transaction_time,
            symbol_length,
            reason_length,
            business_unit_length,
            position_side,
            padding,
            symbol,
            reason,
            business_unit,
            position_amount,
            entry_price,
            accumulated_realized,
            unrealized_pnl,
            breakeven_price,
        })
    }
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct AccountConfigUpdateMsg {
    pub msg_type: AccountEventType,
    pub event_time: i64,
    pub transaction_time: i64,
    pub leverage: i32,
    pub symbol_length: u32,
    pub business_unit_length: u32,
    pub padding: [u8; 4],
    pub symbol: String,
    pub business_unit: String,
}

impl AccountConfigUpdateMsg {
    pub fn create(
        event_time: i64,
        transaction_time: i64,
        symbol: String,
        business_unit: String,
        leverage: i32,
    ) -> Self {
        Self {
            msg_type: AccountEventType::AccountConfigUpdate,
            event_time,
            transaction_time,
            leverage,
            symbol_length: symbol.len() as u32,
            business_unit_length: business_unit.len() as u32,
            padding: [0u8; 4],
            symbol,
            business_unit,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4
            + 8
            + 8
            + 4
            + 4
            + 4
            + 4
            + self.symbol_length as usize
            + self.business_unit_length as usize;

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.event_time);
        buf.put_i64_le(self.transaction_time);
        buf.put_i32_le(self.leverage);
        buf.put_u32_le(self.symbol_length);
        buf.put_u32_le(self.business_unit_length);
        buf.put(&self.padding[..]);
        buf.put(self.symbol.as_bytes());
        buf.put(self.business_unit.as_bytes());

        buf.freeze()
    }
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BalanceUpdateMsg {
    pub msg_type: AccountEventType,
    pub event_time: i64,
    pub transaction_time: i64,
    pub update_id: i64,
    pub asset_length: u32,
    pub padding: [u8; 4],
    pub asset: String,
    pub delta: f64,
}

impl BalanceUpdateMsg {
    pub fn create(
        event_time: i64,
        transaction_time: i64,
        update_id: i64,
        asset: String,
        delta: f64,
    ) -> Self {
        Self {
            msg_type: AccountEventType::BalanceUpdate,
            event_time,
            transaction_time,
            update_id,
            asset_length: asset.len() as u32,
            padding: [0u8; 4],
            asset,
            delta,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4 + 8 + 8 + 8 + 4 + 4 + self.asset_length as usize + 8;

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.event_time);
        buf.put_i64_le(self.transaction_time);
        buf.put_i64_le(self.update_id);
        buf.put_u32_le(self.asset_length);
        buf.put(&self.padding[..]);
        buf.put(self.asset.as_bytes());
        buf.put_f64_le(self.delta);

        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + 8 + 8 + 8 + 4 + 4 + 8; // type + event + tx + update + len + padding + delta
        if data.len() < MIN_SIZE {
            anyhow::bail!("balance update msg too short: {}", data.len());
        }

        let msg_type = get_event_type(data);
        if msg_type != AccountEventType::BalanceUpdate {
            anyhow::bail!("unexpected account msg type: {:?}", msg_type);
        }

        let mut cursor = Bytes::copy_from_slice(data);
        cursor.advance(4); // msg_type already validated
        let event_time = cursor.get_i64_le();
        let transaction_time = cursor.get_i64_le();
        let update_id = cursor.get_i64_le();
        let asset_length = cursor.get_u32_le();
        let mut padding = [0u8; 4];
        cursor.copy_to_slice(&mut padding);

        if cursor.len() < asset_length as usize + 8 {
            anyhow::bail!("balance update asset length mismatch: {}", asset_length);
        }
        let asset_bytes = cursor.copy_to_bytes(asset_length as usize);
        let asset = String::from_utf8(asset_bytes.to_vec())?;
        let delta = cursor.get_f64_le();

        Ok(Self {
            msg_type,
            event_time,
            transaction_time,
            update_id,
            asset_length,
            padding,
            asset,
            delta,
        })
    }
}

/// OKEx balance snapshot（精简格式）
///
/// - msg_type: OkexBalanceSnapshot
/// - event_time: 来自 pTime
/// - update_time: 来自 balData.uTime
/// - symbol: balData.ccy
/// - balance: balData.cashBal
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct OkexBalanceMsg {
    pub msg_type: AccountEventType,
    pub event_time: i64,
    pub update_time: i64,
    pub symbol_length: u32,
    pub padding: [u8; 4],
    pub symbol: String,
    pub balance: f64,
}

impl OkexBalanceMsg {
    pub fn create(event_time: i64, update_time: i64, symbol: String, balance: f64) -> Self {
        Self {
            msg_type: AccountEventType::OkexBalanceSnapshot,
            event_time,
            update_time,
            symbol_length: symbol.len() as u32,
            padding: [0u8; 4],
            symbol,
            balance,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4 + 8 + 8 + 4 + 4 + self.symbol_length as usize + 8;
        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.event_time);
        buf.put_i64_le(self.update_time);
        buf.put_u32_le(self.symbol_length);
        buf.put(&self.padding[..]);
        buf.put(self.symbol.as_bytes());
        buf.put_f64_le(self.balance);

        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + 8 + 8 + 4 + 4 + 8;
        if data.len() < MIN_SIZE {
            anyhow::bail!("okex balance msg too short: {}", data.len());
        }

        let msg_type = get_event_type(data);
        if msg_type != AccountEventType::OkexBalanceSnapshot {
            anyhow::bail!("unexpected account msg type: {:?}", msg_type);
        }

        let mut cursor = Bytes::copy_from_slice(data);
        cursor.advance(4); // msg_type
        let event_time = cursor.get_i64_le();
        let update_time = cursor.get_i64_le();
        let symbol_length = cursor.get_u32_le();
        let mut padding = [0u8; 4];
        cursor.copy_to_slice(&mut padding);

        if cursor.len() < symbol_length as usize + 8 {
            anyhow::bail!("okex balance symbol length mismatch: {}", symbol_length);
        }

        let symbol_bytes = cursor.copy_to_bytes(symbol_length as usize);
        let symbol = String::from_utf8(symbol_bytes.to_vec())?;
        let balance = cursor.get_f64_le();

        Ok(Self {
            msg_type,
            event_time,
            update_time,
            symbol_length,
            padding,
            symbol,
            balance,
        })
    }
}

impl AccountEventMsg {
    pub fn create(msg_type: AccountEventType, data: Bytes) -> Self {
        Self {
            msg_type,
            msg_length: data.len() as u32,
            data,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8 + self.data.len());
        buf.put_u32_le(self.msg_type as u32);
        buf.put_u32_le(self.msg_length);
        buf.put(self.data.clone());
        buf.freeze()
    }
}

#[allow(dead_code)]
#[inline]
pub fn get_event_type(data: &[u8]) -> AccountEventType {
    let event_type_u32 = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);

    match event_type_u32 {
        2001 => AccountEventType::ConditionalOrderUpdate,
        2002 => AccountEventType::OpenOrderLoss,
        2003 => AccountEventType::AccountPosition,
        2004 => AccountEventType::LiabilityChange,
        2005 => AccountEventType::ExecutionReport,
        2006 => AccountEventType::OrderTradeUpdate,
        2007 => AccountEventType::AccountUpdateBalance,
        2008 => AccountEventType::AccountUpdatePosition,
        2009 => AccountEventType::AccountConfigUpdate,
        2010 => AccountEventType::BalanceUpdate,
        2011 => AccountEventType::AccountUpdateFlush,
        2012 => AccountEventType::OkexBalanceSnapshot,
        _ => AccountEventType::Error,
    }
}
