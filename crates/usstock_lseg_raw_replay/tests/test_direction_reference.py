from direction_reference import DirectionState


def test_reverse_offexchange_and_unknown():
    s = DirectionState()
    assert s.classify(1, 1, 1, 100, "BAT", 1) == ("S", 1, True, True)
    assert s.classify(2, 2, 2, 100, "BAT", 2) == ("B", 1, True, True)
    assert s.classify(3, 3, 3, 100, "ADF", 2) == ("N", 9, False, False)
    s.reset()
    assert s.classify(4, 4, 4, 100, "?") == ("B", 8, True, True)


def test_causality_clear_and_resets():
    s = DirectionState()
    s.quote("ask", 10, 10, 1, 100, 1, "NAS")
    assert s.classify(10, 10, 2, 100, "NAS")[1] == 8
    assert s.classify(11, 11, 3, 100, "NAS")[1] == 2
    s.quote("ask", 12, 12, 4, 100, 1, "NYS")
    assert s.classify(13, 13, 5, 100, "NAS")[1] == 7
    s.reset()
    assert s.classify(14, 14, 6, 100, "NAS")[1] == 8


def test_touch_midpoint_tick_and_locked():
    s = DirectionState()
    s.quote("bid", 1, 1, 1, 100, 1, "NAS")
    s.quote("ask", 1, 1, 1, 110, 1, "NAS")
    assert s.classify(2, 2, 2, 111, "NYS")[1] == 3
    assert s.classify(3, 3, 3, 104, "NYS")[:2] == ("S", 4)
    assert s.classify(4, 4, 4, 105, "NYS")[:2] == ("B", 5)
    s.quote("bid", 5, 5, 5, 110, 1, "NAS")
    assert s.classify(6, 6, 6, 109, "NAS")[:2] == ("B", 6)


def test_future_source_not_used_and_clear_not_resurrected():
    s = DirectionState()
    s.quote("ask", 1, 10, 1, 100, 1, "NAS")
    assert s.classify(3, 3, 2, 100, "NAS")[1] == 8
    s.quote("ask", 4, 11, 3, 0, 0, "NAS")
    assert s.classify(5, 12, 4, 100, "NAS")[1] == 8
