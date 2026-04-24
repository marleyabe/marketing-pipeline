"""Backfill resiliente: retry transiente + commit por partição."""

from unittest.mock import MagicMock, patch

import pytest
from google.ads.googleads.errors import GoogleAdsException
from grpc import StatusCode

from dags._extractor import ExtractorSpec, _run_partitions
from dags._retry import TRANSIENT_GRPC_CODES, _is_transient, call_with_retry


def _make_ads_error(code: StatusCode) -> GoogleAdsException:
    grpc_err = MagicMock()
    grpc_err.code.return_value = code
    error = GoogleAdsException.__new__(GoogleAdsException)
    error.error = grpc_err
    return error


def test_transient_codes_include_recommended():
    # Google Ads docs: retry em UNAVAILABLE/DEADLINE/INTERNAL/UNKNOWN/ABORTED/RESOURCE_EXHAUSTED
    expected = {
        StatusCode.UNAVAILABLE, StatusCode.DEADLINE_EXCEEDED,
        StatusCode.INTERNAL, StatusCode.UNKNOWN, StatusCode.ABORTED,
        StatusCode.RESOURCE_EXHAUSTED,
    }
    assert expected == TRANSIENT_GRPC_CODES


def test_is_transient_for_unavailable():
    assert _is_transient(_make_ads_error(StatusCode.UNAVAILABLE)) is True


def test_is_transient_false_for_invalid_argument():
    assert _is_transient(_make_ads_error(StatusCode.INVALID_ARGUMENT)) is False


def test_is_transient_false_for_plain_exception():
    assert _is_transient(ValueError("foo")) is False


def test_call_with_retry_returns_on_first_success():
    fn = MagicMock(return_value="ok")
    assert call_with_retry(fn, "label") == "ok"
    assert fn.call_count == 1


def test_call_with_retry_retries_transient_then_succeeds():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise _make_ads_error(StatusCode.UNAVAILABLE)
        return "ok"

    with patch("dags._retry.time.sleep"):
        assert call_with_retry(flaky, "label", max_attempts=5, base_seconds=0) == "ok"
    assert calls["n"] == 3


def test_call_with_retry_reraises_after_exhausted():
    fn = MagicMock(side_effect=_make_ads_error(StatusCode.RESOURCE_EXHAUSTED))
    with patch("dags._retry.time.sleep"):
        with pytest.raises(GoogleAdsException):
            call_with_retry(fn, "label", max_attempts=3, base_seconds=0)
    assert fn.call_count == 3


def test_call_with_retry_does_not_retry_fatal():
    fn = MagicMock(side_effect=_make_ads_error(StatusCode.PERMISSION_DENIED))
    with pytest.raises(GoogleAdsException):
        call_with_retry(fn, "label", max_attempts=5, base_seconds=0)
    assert fn.call_count == 1


def _spec(fetch_date) -> ExtractorSpec:
    return ExtractorSpec(
        platform="google_ads",
        bronze_table="google_ads_raw",
        conflict_keys=["date", "customer_id"],
        init_api=lambda: None,
        list_accounts=lambda _api: ["a1", "a2"],
        fetch_date=fetch_date,
    )


def test_run_partitions_commits_each_partition():
    fetch_calls: list[tuple] = []

    def fake_fetch(_api, accounts, date_iso):
        fetch_calls.append((tuple(accounts), date_iso))
        return [{"x": 1}]

    loads: list[list[dict]] = []

    def fake_load(rows, _spec):
        loads.append(list(rows))
        return len(rows)

    with patch("dags._extractor.load_to_bronze", side_effect=fake_load):
        total = _run_partitions(_spec(fake_fetch), None, ["a1", "a2"], ["2026-04-01", "2026-04-02"])

    assert total == 4
    # 4 partições × 1 linha cada = 4 commits independentes
    assert len(loads) == 4
    # Ordem: cronológica oldest→newest, loop de contas interno
    assert fetch_calls == [
        (("a1",), "2026-04-01"), (("a2",), "2026-04-01"),
        (("a1",), "2026-04-02"), (("a2",), "2026-04-02"),
    ]


def test_run_partitions_continues_on_partial_failure():
    def fake_fetch(_api, accounts, date_iso):
        if accounts == ["a1"] and date_iso == "2026-04-01":
            raise _make_ads_error(StatusCode.RESOURCE_EXHAUSTED)
        return [{"x": 1}]

    loads: list[list[dict]] = []

    def fake_load(rows, _spec):
        loads.append(list(rows))
        return len(rows)

    with patch("dags._extractor.load_to_bronze", side_effect=fake_load):
        with pytest.raises(RuntimeError, match="incompleto"):
            _run_partitions(_spec(fake_fetch), None, ["a1", "a2"], ["2026-04-01", "2026-04-02"])

    # 3 partições OK foram comitadas mesmo com 1 falha (maximiza volume)
    assert len(loads) == 3


def test_run_partitions_raises_with_failed_sample_in_message():
    def fake_fetch(_api, accounts, date_iso):
        raise _make_ads_error(StatusCode.RESOURCE_EXHAUSTED)

    with patch("dags._extractor.load_to_bronze", return_value=0):
        with pytest.raises(RuntimeError) as exc:
            _run_partitions(_spec(fake_fetch), None, ["a1"], ["2026-04-01"])
    assert "2026-04-01/a1" in str(exc.value)
