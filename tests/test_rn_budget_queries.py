"""budget.queries: read/upsert via FakeConn."""

from datetime import datetime

from src.api.budget.queries import read_client_budget, upsert_client_budget


ROW = ("123", "google_ads", 5000.0, "BRL", True, datetime(2026, 4, 1), datetime(2026, 4, 22))


def test_read_client_budget_returns_none_when_absent(fake_conn):
    conn = fake_conn([[]])
    assert read_client_budget(conn, "999", "google_ads") is None


def test_read_client_budget_parses_row(fake_conn):
    conn = fake_conn([[ROW]])
    out = read_client_budget(conn, "123", "google_ads")
    assert out is not None
    assert out.monthly_budget == 5000.0
    assert out.currency == "BRL"
    assert out.active is True


def test_upsert_client_budget_returns_persisted_row(fake_conn):
    conn = fake_conn([[ROW]])
    out = upsert_client_budget(conn, "123", "google_ads", 5000.0, "BRL", True)
    assert out.account_id == "123"
    assert out.monthly_budget == 5000.0
