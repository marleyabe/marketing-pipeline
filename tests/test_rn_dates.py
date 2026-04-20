"""RN01-04: date helpers + range parsing."""
from datetime import date
from unittest.mock import patch

import pytest

from src.api.dates import lastmonth, lastweek, parse_range, yesterday


@patch("src.api.dates.today_utc")
def test_rn01_yesterday_is_d_minus_1(mock_today):
    """RN01: yesterday = D-1 UTC."""
    mock_today.return_value = date(2026, 4, 20)
    start, end = yesterday()
    assert start == date(2026, 4, 19)
    assert end == date(2026, 4, 19)


@patch("src.api.dates.today_utc")
def test_rn02_lastweek_is_mon_to_sun_previous(mock_today):
    """RN02: lastweek = segunda a domingo da semana anterior."""
    mock_today.return_value = date(2026, 4, 20)  # Monday
    start, end = lastweek()
    assert start == date(2026, 4, 13)
    assert end == date(2026, 4, 19)
    assert start.weekday() == 0
    assert end.weekday() == 6


@patch("src.api.dates.today_utc")
def test_rn02_lastweek_from_midweek(mock_today):
    mock_today.return_value = date(2026, 4, 23)  # Thursday
    start, end = lastweek()
    assert start == date(2026, 4, 13)
    assert end == date(2026, 4, 19)


@patch("src.api.dates.today_utc")
def test_rn03_lastmonth_full_prev_month(mock_today):
    """RN03: lastmonth = dia 1 ao ultimo dia do mes anterior."""
    mock_today.return_value = date(2026, 4, 15)
    start, end = lastmonth()
    assert start == date(2026, 3, 1)
    assert end == date(2026, 3, 31)


@patch("src.api.dates.today_utc")
def test_rn03_lastmonth_january_rolls_to_december(mock_today):
    mock_today.return_value = date(2026, 1, 10)
    start, end = lastmonth()
    assert start == date(2025, 12, 1)
    assert end == date(2025, 12, 31)


def test_rn04_range_end_missing_defaults_to_start():
    """RN04: end-date ausente = mesmo dia."""
    start, end = parse_range(date(2026, 4, 16), None)
    assert start == end == date(2026, 4, 16)


def test_rn04_range_end_must_be_ge_start():
    with pytest.raises(ValueError):
        parse_range(date(2026, 4, 16), date(2026, 4, 15))


def test_rn04_range_valid():
    start, end = parse_range(date(2026, 4, 1), date(2026, 4, 16))
    assert start == date(2026, 4, 1)
    assert end == date(2026, 4, 16)
