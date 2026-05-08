//! 各交易所的 [`crate::pre_trade::auto_repay_service::Repayer`] 实现。

pub mod binance;
pub mod bybit;
pub mod gate;

pub use binance::BinanceRepayer;
pub use bybit::BybitRepayer;
pub use gate::GateRepayer;

use prettytable::format::{FormatBuilder, LinePosition, LineSeparator};
use prettytable::{Cell, Row, Table};

/// 三线表（top / header-divider / bottom 三条横线，无竖线）。
/// 风格参考 `kline_pub::app::log_kline_table`，但去掉竖线对齐学术 "三线表" 惯例。
pub(crate) fn build_three_line_table(headers: &[&str]) -> Table {
    let mut table = Table::new();
    let format = FormatBuilder::new()
        .padding(1, 1)
        .column_separator(' ')
        .borders(' ')
        .separator(LinePosition::Top, LineSeparator::new('-', '-', '-', '-'))
        .separator(LinePosition::Title, LineSeparator::new('-', '-', '-', '-'))
        .separator(LinePosition::Bottom, LineSeparator::new('-', '-', '-', '-'))
        .build();
    table.set_format(format);
    table.set_titles(Row::from(
        headers
            .iter()
            .map(|h| Cell::new(h).style_spec("c"))
            .collect::<Vec<_>>(),
    ));
    table
}
