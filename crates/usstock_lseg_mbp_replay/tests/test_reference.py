from pathlib import Path

import json

from reference import encode_key, encode_message, iter_messages, replay


def test_reference_replays_refresh_and_map_actions() -> None:
    result = replay(Path(__file__).parent / "fixtures" / "mbp_small.csv")

    assert result["messages_by_ric"] == {"AAA.BAT": 3, "BBB.BAT": 2}
    assert result["final_depth_by_ric"] == {"AAA.BAT": 2, "BBB.BAT": 0}
    assert result["counts"]["book_images"] == 1
    assert result["counts"]["refresh_without_entries"] == 1
    assert result["counts"]["update_missing"] == 1
    assert result["counts"]["delete_missing"] == 1


def test_reference_matches_shared_binary_golden_vectors() -> None:
    fixtures = Path(__file__).parent / "fixtures"
    expected = json.loads((fixtures / "mbp_small_golden.json").read_text())
    actual = [
        {"key": encode_key(message).hex(), "value": encode_message(message).hex()}
        for message in iter_messages(fixtures / "mbp_small.csv")
    ]
    assert actual == expected
