use anyhow::{bail, Result};

pub const SUMMARY_FIDS: [(u16, &str); 26] = [
    (1, "PROD_PERM"),
    (3, "DSPLY_NAME"),
    (15, "CURRENCY"),
    (17, "ACTIV_DATE"),
    (53, "TRD_UNITS"),
    (78, "OFFCL_CODE"),
    (198, "LOT_SIZE_A"),
    (259, "RECORDTYPE"),
    (1709, "RDN_EXCHD2"),
    (3183, "LIST_MKT"),
    (3422, "PROV_SYMB"),
    (3423, "PR_RNK_RUL"),
    (3425, "OR_RNK_RUL"),
    (3694, "MNEMONIC"),
    (3984, "TRD_TYPE"),
    (4148, "TIMACT_MS"),
    (5357, "CONTEXT_ID"),
    (6401, "DDS_DSO_ID"),
    (6480, "SPS_SP_RIC"),
    (6516, "BOOK_STATE"),
    (6519, "MKT_OR_RUL"),
    (6614, "TRD_STATUS"),
    (6618, "HALT_DATE"),
    (6619, "HALT_TIME"),
    (14269, "TIMACT_NS"),
    (14319, "HALT_TM_NS"),
];

pub const ENTRY_FIDS_OLD: [(u16, &str); 7] = [
    (3427, "ORDER_PRC"),
    (3428, "ORDER_SIDE"),
    (3430, "NO_ORD"),
    (4356, "ACC_SIZE"),
    (6527, "LV_TIM_MS"),
    (6528, "LV_TIM_MSP"),
    (6529, "LV_DATE"),
];
pub const ENTRY_TIME_NS_FID: (u16, &str) = (14268, "LV_TIM_NS");

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageClass {
    Refresh = 1,
    Update = 2,
    Status = 3,
}

impl MessageClass {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "REFRESH" => Ok(Self::Refresh),
            "UPDATE" => Ok(Self::Update),
            "STATUS" => Ok(Self::Status),
            _ => bail!("unsupported MBP message class {value:?}"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Refresh => "REFRESH",
            Self::Update => "UPDATE",
            Self::Status => "STATUS",
        }
    }
}

impl TryFrom<u8> for MessageClass {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Refresh),
            2 => Ok(Self::Update),
            3 => Ok(Self::Status),
            _ => bail!("invalid MBP message class code {value}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum LevelAction {
    Add = 1,
    Update = 2,
    Delete = 3,
}

impl LevelAction {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "ADD" => Ok(Self::Add),
            "UPDATE" => Ok(Self::Update),
            "DELETE" => Ok(Self::Delete),
            _ => bail!("unsupported MBP level action {value:?}"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Add => "ADD",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }
}

impl TryFrom<u8> for LevelAction {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Add),
            2 => Ok(Self::Update),
            3 => Ok(Self::Delete),
            _ => bail!("invalid MBP level action code {value}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
#[repr(u8)]
pub enum Side {
    Bid = 1,
    Ask = 2,
}

impl TryFrom<u8> for Side {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Bid),
            2 => Ok(Self::Ask),
            _ => bail!("invalid MBP side code {value}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SummaryField {
    pub fid: u16,
    /// `None` means that the FID was present but its source value was empty.
    pub value: Option<String>,
    /// The source enum string is retained when non-empty.
    pub enum_value: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SummaryDelta {
    /// Sorted in `SUMMARY_FIDS` bit order, never source order.
    pub fields: Vec<SummaryField>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LevelDelta {
    pub action: LevelAction,
    pub side: Side,
    pub price_e9: i64,
    /// `None` only for DELETE, whose source row has no child FIDs.
    pub no_ord: Option<u32>,
    pub acc_size: Option<u32>,
    pub level_time_ms: Option<u32>,
    pub level_time_msp: Option<u32>,
    pub level_date: Option<i32>,
    /// Direct `LV_TIM_NS`; absent in the historical seven-FID layout.
    pub level_time_ns: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalMessage {
    pub ric: String,
    pub ts_utc_ns: u64,
    pub source_row: u64,
    /// `None` for the four audited standalone STATUS messages.
    pub source_sequence: Option<u64>,
    pub gmt_offset_minutes: i16,
    pub message_class: MessageClass,
    pub update_type: String,
    pub pe_code: u16,
    pub template_number: Option<u16>,
    /// `None` preserves the four audited messages that have no Summary row.
    pub summary: Option<SummaryDelta>,
    /// Source order is preserved; all entries remain atomic with this message.
    pub entries: Vec<LevelDelta>,
}

impl LogicalMessage {
    pub fn is_book_image(&self) -> bool {
        self.message_class == MessageClass::Refresh && !self.entries.is_empty()
    }
}

pub fn summary_index(fid: u16) -> Option<usize> {
    SUMMARY_FIDS.iter().position(|(known, _)| *known == fid)
}

pub fn summary_name(fid: u16) -> Option<&'static str> {
    summary_index(fid).map(|index| SUMMARY_FIDS[index].1)
}
