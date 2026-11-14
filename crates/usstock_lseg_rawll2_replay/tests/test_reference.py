from pathlib import Path

from reference import census, iter_messages


FIXTURE = Path(__file__).parent / "fixtures" / "rawll2_small.csv"


def test_python_baseline_preserves_patch_order_and_empty_semantics() -> None:
    messages = list(iter_messages(FIXTURE))
    assert [field.name for field in messages[0].fields] == ["CURRENCY", "BEST_BID1"]
    assert messages[1].fields[0].enum_value == "  "
    assert census(FIXTURE) == {
        "messages": 2,
        "by_ric": {"ABBV.N": 2},
        "fids": {"15:CURRENCY": 1, "436:BEST_BID1": 1, "6614:TRD_STATUS": 1},
    }
