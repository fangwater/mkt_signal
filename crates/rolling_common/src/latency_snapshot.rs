//! trade_engine 定期发布的延迟分位数 IPC 快照消息。
//!
//! - **触发**：venue 进程内每 30s 一次（不依赖单桶满 10000 才输出）。
//! - **服务名**：`<IPC_NAMESPACE>/te_pubs/<venue>/latency`，载荷
//!   `[u8; LATENCY_SNAPSHOT_PAYLOAD_LEN]`（512 字节）。`IPC_NAMESPACE` 让多 te
//!   实例互不冲突；`te_pubs` 前缀声明发布源是 trade_engine。
//!   `spread_pbs` 复用同一载荷格式，但服务名固定为
//!   `spread_pbs/<venue>/latency`，不加 `IPC_NAMESPACE`。
//! - **载荷**：定长 `LatencySnapshotMsg`（`repr(C)`），通过 iceoryx2
//!   `publish_subscribe` 推出。
//! - **语义**：本周期里所有非空桶的统计；空桶不占位（`n_buckets` 标记有效条数，
//!   后续 slot 为 0 填充——消费者只读前 `n_buckets` 条）。
//!
//! 字段单位、ID 取值见下方常量。

/// IPC 消息类型编号。
pub const LATENCY_SNAPSHOT_MSG_TYPE: u32 = 7100;
/// schema 版本号，遇到字段变动时 +1。
pub const LATENCY_SNAPSHOT_SCHEMA_VER: u32 = 1;
/// 单条消息可容纳的桶数。当前 trade_engine 一个 venue 最多 10 个桶（5 metric × 2 action）。
pub const LATENCY_SNAPSHOT_MAX_BUCKETS: usize = 10;
/// 整条消息的字节数（即 iceoryx2 service 的载荷大小）。
pub const LATENCY_SNAPSHOT_PAYLOAD_LEN: usize = std::mem::size_of::<LatencySnapshotMsg>();

// metric_id —— 哪个延迟度量
pub const METRIC_ID_IPC_TO_WS: u8 = 0; // T1 − T0
pub const METRIC_ID_UPLINK: u8 = 1; // T2 − T1
pub const METRIC_ID_SERVER: u8 = 2; // T3 − T2
pub const METRIC_ID_DOWNLINK: u8 = 3; // T4 − T3
pub const METRIC_ID_RTT: u8 = 4; // T4 − T1
pub const METRIC_ID_SPREAD_NET: u8 = 10; // spread_pbs: recv_us − event_time_us
pub const METRIC_ID_SPREAD_E2E: u8 = 11; // spread_pbs: accepted_us − event_time_us

// action_id —— 下单还是撤单
pub const ACTION_ID_NEW: u8 = 0;
pub const ACTION_ID_CANCEL: u8 = 1;
pub const ACTION_ID_MARKET_DATA: u8 = 2;

/// 单个桶的快照（48 字节，`repr(C)` + 8 字节对齐）。
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct LatencyBucketStat {
    pub metric_id: u8,
    pub action_id: u8,
    pub _pad: [u8; 6],
    pub n: u64,
    pub p50_us: i64,
    pub p90_us: i64,
    pub p95_us: i64,
    pub p99_us: i64,
}

/// venue 级延迟快照消息。
///
/// 头部 32 字节 + `LATENCY_SNAPSHOT_MAX_BUCKETS` × 48 字节桶数组 = 512 字节。
#[repr(C)]
pub struct LatencySnapshotMsg {
    pub msg_type: u32,
    pub schema_ver: u32,
    pub venue_id: u32, // 与 `Exchange::to_u8` 对齐（u32 化）
    pub n_buckets: u32,
    pub snapshot_time_us: i64,
    pub _reserved: u64,
    pub buckets: [LatencyBucketStat; LATENCY_SNAPSHOT_MAX_BUCKETS],
}

impl LatencySnapshotMsg {
    pub fn new(venue_id: u32, snapshot_time_us: i64) -> Self {
        Self {
            msg_type: LATENCY_SNAPSHOT_MSG_TYPE,
            schema_ver: LATENCY_SNAPSHOT_SCHEMA_VER,
            venue_id,
            n_buckets: 0,
            snapshot_time_us,
            _reserved: 0,
            buckets: [LatencyBucketStat::default(); LATENCY_SNAPSHOT_MAX_BUCKETS],
        }
    }

    /// 转成定长字节数组，供 iceoryx2 `send_copy`。`repr(C)` 保证布局稳定。
    pub fn into_bytes(self) -> [u8; LATENCY_SNAPSHOT_PAYLOAD_LEN] {
        // SAFETY: `LatencySnapshotMsg` is `#[repr(C)]` with no padding-only
        // fields uninitialized; size_of equals LATENCY_SNAPSHOT_PAYLOAD_LEN.
        unsafe { std::mem::transmute(self) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_size_matches_constant() {
        // 512 bytes = 32 (header) + 10 * 48 (buckets)
        assert_eq!(LATENCY_SNAPSHOT_PAYLOAD_LEN, 512);
        assert_eq!(std::mem::size_of::<LatencyBucketStat>(), 48);
    }

    #[test]
    fn into_bytes_roundtrip_header() {
        let mut msg = LatencySnapshotMsg::new(4, 1_700_000_000_000_000);
        msg.n_buckets = 1;
        msg.buckets[0] = LatencyBucketStat {
            metric_id: METRIC_ID_RTT,
            action_id: ACTION_ID_NEW,
            _pad: [0; 6],
            n: 9999,
            p50_us: 42,
            p90_us: 80,
            p95_us: 110,
            p99_us: 350,
        };
        let bytes = msg.into_bytes();
        // 头部前 4 字节是 msg_type（u32 LE）
        assert_eq!(
            u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            LATENCY_SNAPSHOT_MSG_TYPE
        );
        assert_eq!(u32::from_le_bytes(bytes[4..8].try_into().unwrap()), 1); // schema_ver
        assert_eq!(u32::from_le_bytes(bytes[8..12].try_into().unwrap()), 4); // venue_id
        assert_eq!(u32::from_le_bytes(bytes[12..16].try_into().unwrap()), 1); // n_buckets
    }
}
