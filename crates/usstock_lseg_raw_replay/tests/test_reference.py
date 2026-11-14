from pathlib import Path

from reference import audit, iter_messages
from quote_reference import (
    MISSING_CODE,
    MISSING_PRICE,
    MISSING_SIZE,
    _timestamp_ns,
    compress_quotes,
    is_quote_ripple,
    route_quotes,
)
from event_reference import (
    classify_trade_direction,
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


def test_quote_reference_accepts_partial_quote_without_quote_time(tmp_path: Path) -> None:
    path = tmp_path / "partial_quote.csv"
    path.write_text(
        "#RIC,Domain,Date-Time,GMT Offset,Type,MsgClass/FID number,UpdateType/Action,FID Name,FID Value,FID Enum String,PE Code,Template Number,Key/Msg Sequence Number,Number of FIDs\n"
        "AAPL.O,Market Price,2022-05-04T18:42:40.718847845Z,-4,Raw,UPDATE,QUOTE,,,,48064,,0,14\n"
        ",,,,FID,11683,,BIDFINMMID,,\n"
        ",,,,FID,22,,BID,161,\n"
        ",,,,FID,6579,,BID_COND_N,R,\n"
        ",,,,FID,3298,,BIDXID,43,NAS\n"
        ",,,,FID,296,,ASK_MMID1,NYS,\n"
        ",,,,FID,11684,,ASKFINMMID,,\n"
        ",,,,FID,118,,PRC_QL_CD,0,\"   \"\n"
        ",,,,FID,3264,,PRC_QL3,0,\"   \"\n"
        ",,,,FID,30,,BIDSIZE,5,\n"
        ",,,,FID,3297,,ASKXID,2,NYS\n"
        ",,,,FID,31,,ASKSIZE,1,\n"
        ",,,,FID,6580,,ASK_COND_N,R,\n"
        ",,,,FID,25,,ASK,161.03,\n"
        ",,,,FID,293,,BID_MMID1,NAS,\n",
        encoding="utf-8",
    )
    winners = compress_quotes(path, part=0, shard=65)
    quote = winners[("AAPL.O", 1651689760000000000)]
    assert quote.bid == 161_000_000_000
    assert quote.ask == 161_030_000_000
    assert quote.bid_venue == "NAS"
    assert quote.ask_venue == "NYS"


def test_quote_reference_accepts_retail_interest_metadata(tmp_path: Path) -> None:
    path = tmp_path / "retail_interest_quote.csv"
    path.write_text(
        "#RIC,Domain,Date-Time,GMT Offset,Type,MsgClass/FID number,UpdateType/Action,FID Name,FID Value,FID Enum String,PE Code,Template Number,Key/Msg Sequence Number,Number of FIDs\n"
        "ABBV.N,Market Price,2021-07-22T14:47:49.427579075Z,-4,Raw,UPDATE,QUOTE,,,,6562,,51600,11\n"
        ",,,,FID,22,,BID,117.44,\n"
        ",,,,FID,25,,ASK,117.48,\n"
        ",,,,FID,30,,BIDSIZE,3,\n"
        ",,,,FID,31,,ASKSIZE,1,\n"
        ",,,,FID,118,,PRC_QL_CD,60,\"R  \"\n"
        ",,,,FID,3264,,PRC_QL3,60,\"R  \"\n"
        ",,,,FID,8935,,RETAIL_INT,3,\"A  \"\n"
        ",,,,FID,1501,,STOCK_TYPE,B,\n"
        ",,,,FID,6513,,SETL_TYPE,5,NRM\n"
        ",,,,FID,6516,,BOOK_STATE,1,N\n"
        ",,,,FID,3855,,QUOTIM_MS,53269413,\n",
        encoding="utf-8",
    )
    winners = compress_quotes(path, part=0, shard=79)
    quote = winners[("ABBV.N", 1626965269000000000)]
    assert quote.bid == 117_440_000_000
    assert quote.ask == 117_480_000_000
    assert quote.bid_venue == ""
    assert quote.ask_venue == ""
    venues, states = route_quotes(winners)
    assert venues == {}
    assert states == {"i:ABBV.N": [(1626965269000000000, False, False, 60)]}


def test_quote_reference_merges_incremental_sides_with_nanosecond_metadata(tmp_path: Path) -> None:
    path = tmp_path / "incremental_quote.csv"
    path.write_text(
        "#RIC,Domain,Date-Time,GMT Offset,Type,MsgClass/FID number,UpdateType/Action,FID Name,FID Value,FID Enum String,PE Code,Template Number,Key/Msg Sequence Number,Number of FIDs\n"
        "ARKG.BAT,Market Price,2022-02-24T17:20:05.247730047Z,-5,Raw,UPDATE,QUOTE,,,,5054,,24768,4\n"
        ",,,,FID,22,,BID,44,\n"
        ",,,,FID,30,,BIDSIZE,2,\n"
        ",,,,FID,14264,,BID_TIM_NS,17:20:05.221000000,\n"
        ",,,,FID,14265,,QUOTIM_NS,17:20:05.221000000,\n"
        "ARKG.BAT,Market Price,2022-02-24T17:20:05.983749317Z,-5,Raw,UPDATE,QUOTE,,,,5054,,24880,4\n"
        ",,,,FID,25,,ASK,44.01,\n"
        ",,,,FID,31,,ASKSIZE,200,\n"
        ",,,,FID,14263,,ASK_TIM_NS,17:20:05.966000000,\n"
        ",,,,FID,14265,,QUOTIM_NS,17:20:05.966000000,\n",
        encoding="utf-8",
    )
    winners = compress_quotes(path, part=0, shard=161)
    quote = winners[("ARKG.BAT", 1645723205000000000)]
    assert quote.bid == 44_000_000_000
    assert quote.bid_size == 2
    assert quote.ask == 44_010_000_000
    assert quote.ask_size == 200
    assert quote.quality_code == MISSING_CODE


def test_reference_timestamp_keeps_nanoseconds() -> None:
    assert _timestamp_ns("2021-07-01T00:00:00.000000001Z") == 1625097600000000001


def test_event_codec_matches_rust_golden_bytes() -> None:
    trade = TradeValue(1, 2, 3, 4, 5, 6, 7, 8, b"@F  ", 9, 10)
    assert trade.encode().hex() == (
        "0100000000000000020000000000000003000000040000000500000000000000"
        "0600000000000000070000000000000008000000000000004046202009000a00"
        "ffffffffffffffffffffffffffffffffffffffffffffffffffff4e030000ffffffffffffffffffffffffffff00000000"
    )
    correction = CorrectionValue(1, 2, 3, 4, 5, 6, 7, 8, b"@  I", 9, 10, 11)
    assert correction.encode().hex() == (
        "0100000000000000020000000000000003000000040000000500000000000000"
        "0600000000000000070000000000000008000000000000004020204909000a00"
        "0b00000000000000"
    )


def test_trade_direction_evidence_is_not_automatically_aggressor() -> None:
    assert classify_trade_direction("BAT", 1) == (ord("N"), 4, 1)
    assert classify_trade_direction("ADF", 1) == (ord("N"), 1, 2)
    assert classify_trade_direction("NAS", 65535) == (ord("N"), 2, 1)
    assert classify_trade_direction("", 65535) == (ord("N"), 3, 0)


def test_exact_slots_distinguish_absent_from_explicit_empty() -> None:
    encoded = encode_exact_slots(
        1,
        2,
        {"NEWS": Field(28, "NEWS", "", "")},
        ["NEWS", "NEWS_TIME"],
    )
    assert encoded[16:48] == b"\0" * 32
    assert encoded[48:80] == b"\xff" * 32
