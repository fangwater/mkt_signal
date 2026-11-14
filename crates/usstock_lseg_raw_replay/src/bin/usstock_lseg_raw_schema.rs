use anyhow::Result;
use usstock_lseg_raw_replay::event_codec::{
    wire_layout_fields, wire_layouts, SLOT_ENUM_LEN, SLOT_HEADER_LEN, SLOT_LEN, SLOT_VALUE_LEN,
};

fn main() -> Result<()> {
    for (msg_type, name, value_len) in wire_layouts() {
        println!("0x{msg_type:02x}\t{name}\t{value_len}");
        for (index, (fid, field)) in wire_layout_fields(msg_type)?.iter().enumerate() {
            let offset = SLOT_HEADER_LEN + index * SLOT_LEN;
            println!(
                "  {offset}\t{SLOT_VALUE_LEN}\tFID {fid} {field} value; {}\t{SLOT_ENUM_LEN}\tenum",
                offset + SLOT_VALUE_LEN
            );
        }
    }
    Ok(())
}
