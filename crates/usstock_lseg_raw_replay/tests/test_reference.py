from pathlib import Path

from reference import audit, iter_messages
from quote_reference import (
    MISSING_PRICE,
    MISSING_SIZE,
    _timestamp_ns,
    compress_quotes,
    is_quote_ripple,
    route_quotes,
)
from event_reference import (
    CorrectionValue,
    TradeValue,
    encode_exact_slots,
    is_empty_closing_run,
    is_range_update_only,
)
from reference import Field


FIXTURE = Path(__file__).parent / "fixtures" / "raw_small.csv"
QUOTE_FIXTURE = Path(__file__).parent / "fixtures" / "merged-Data-part-000000-shard-000000.csv"


def test_parser_preserves_outer_message_boundaries() -> None:
    with FIXTURE.open(encoding="utf-8", newline="") as stream:
        messages = list(iter_messages(stream))
    assert len(messages) == 3
    assert [len(message.fields) for message in messages] == [2, 3, 1]
    assert messages[0].fields[0].name == "BID"
    assert messages[1].update_type == "TRADE"
    assert messages[2].message_class == "REFRESH"


def test_audit_counts_templates_and_signatures() -> None:
    result = audit(FIXTURE)
    assert result["logical_messages"] == 3
    assert result["physical_rows"] == 9
    assert result["message_classes"] == {"UPDATE": 2, "REFRESH": 1}
    assert result["distinct_fids"] == 6
    assert result["distinct_ordered_fid_signatures"] == 3
    assert result["distinct_message_combinations"] == 3
    quote = next(
        example
        for key, example in result["first_example_by_combination"].items()
        if key.startswith("UPDATE|QUOTE|")
    )
    assert [field["fid"] for field in quote["fields"]] == [22, 25]


def test_quote_reference_selects_global_second_before_venue_routing() -> None:
    winners = compress_quotes(QUOTE_FIXTURE, part=0, shard=0)
    assert len(winners) == 3
    venues, states = route_quotes(winners)
    assert set(venues) == {"v:AAPL.O:IEX", "v:AAPL.O:NAS"}
    assert len(venues["v:AAPL.O:IEX"]) == 1
    assert venues["v:AAPL.O:IEX"][0].bid == 100_110_000_000
    assert venues["v:AAPL.O:IEX"][0].ask == 100_210_000_000
    assert len(venues["v:AAPL.O:NAS"]) == 1
    assert venues["v:AAPL.O:NAS"][0].ask == MISSING_PRICE
    assert venues["v:AAPL.O:NAS"][0].ask_size == MISSING_SIZE
    assert set(states) == {"i:AAPL.O"}
    assert states["i:AAPL.O"] == [
        (1625097601000000000, False, True, 77),
        (1625097602000000000, True, True, 94),
    ]


def test_quote_reference_validates_ripple_only_message() -> None:
    with QUOTE_FIXTURE.open(encoding="utf-8", newline="") as stream:
        messages = iter_messages(stream)
        assert is_quote_ripple(next(messages))
        assert is_empty_closing_run(next(messages))
        assert is_range_update_only(next(messages))


def test_reference_timestamp_keeps_nanoseconds() -> None:
    assert _timestamp_ns("2021-07-01T00:00:00.000000001Z") == 1625097600000000001


def test_event_codec_matches_rust_golden_bytes() -> None:
    trade = TradeValue(1, 2, 3, 4, 5, 6, 7, 8, b"@F  ", 9, 10)
    assert trade.encode().hex() == (
        "0100000000000000020000000000000003000000040000000500000000000000"
        "0600000000000000070000000000000008000000000000004046202009000a00"
    )
    correction = CorrectionValue(1, 2, 3, 4, 5, 6, 7, 8, b"@  I", 9, 10, 11)
    assert correction.encode().hex() == (
        "0100000000000000020000000000000003000000040000000500000000000000"
        "0600000000000000070000000000000008000000000000004020204909000a00"
        "0b00000000000000"
    )


def test_exact_slots_distinguish_absent_from_explicit_empty() -> None:
    encoded = encode_exact_slots(
        1,
        2,
        {"NEWS": Field(28, "NEWS", "", "")},
        ["NEWS", "NEWS_TIME"],
    )
    assert encoded[16:48] == b"\0" * 32
    assert encoded[48:80] == b"\xff" * 32
