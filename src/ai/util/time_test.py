from .time import truncate_time_ms, TimeGranularity


def test_truncate_hour() -> None:
    # UTC Tuesday, February 21, 2023 11:27:55.123 PM
    t1 = 1677022075123
    # UTC Tuesday, February 21, 2023 11:00:00 PM
    expected = 1677020400000

    truncated = truncate_time_ms(t1, TimeGranularity.H)
    assert truncated == expected


def test_truncate_day() -> None:
    # UTC Tuesday, February 21, 2023 11:27:55.123 PM
    t1 = 1677022075123
    # UTC Tuesday, February 21, 2023 0:00:00
    expected = 1676937600000

    truncated = truncate_time_ms(t1, TimeGranularity.D)
    assert truncated == expected
