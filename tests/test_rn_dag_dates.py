"""RN16: resolução de datas nas DAGs de extração."""
from datetime import date, datetime
from unittest.mock import patch

import pytest

from dags._date_range import resolve_target_dates


def test_rn16_both_empty_returns_yesterday():
    fixed_now = datetime(2026, 4, 20, 12, 0, 0)
    with patch("dags._date_range.datetime") as mock_datetime:
        mock_datetime.utcnow.return_value = fixed_now
        result = resolve_target_dates(None, None)
    assert result == [date(2026, 4, 19)]


def test_rn16_both_filled_returns_range():
    result = resolve_target_dates("2026-04-01", "2026-04-03")
    assert result == [date(2026, 4, 1), date(2026, 4, 2), date(2026, 4, 3)]


def test_rn16_same_day_range():
    result = resolve_target_dates("2026-04-10", "2026-04-10")
    assert result == [date(2026, 4, 10)]


def test_rn16_only_start_raises():
    with pytest.raises(ValueError, match="ambos"):
        resolve_target_dates("2026-04-01", None)


def test_rn16_only_end_raises():
    with pytest.raises(ValueError, match="ambos"):
        resolve_target_dates(None, "2026-04-01")


def test_rn16_end_before_start_raises():
    with pytest.raises(ValueError, match=">="):
        resolve_target_dates("2026-04-10", "2026-04-01")
