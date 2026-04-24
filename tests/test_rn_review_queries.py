"""reviews.queries: SQL via FakeConn (rows_queue). Isolado de DB real."""

from datetime import date

from src.api.reviews.queries import (
    fetch_account_overview,
    fetch_account_performance,
    fetch_budget_pacing_row,
    fetch_campaign_status_rows,
    fetch_top_keywords_with_roas,
)

S, E = date(2026, 4, 1), date(2026, 4, 22)


def test_fetch_account_overview_returns_none_when_missing(fake_conn):
    conn = fake_conn([[]])
    assert fetch_account_overview(conn, "999") is None


def test_fetch_account_overview_returns_first_platform(fake_conn):
    conn = fake_conn([[("123", "Acme", "google_ads")]])
    out = fetch_account_overview(conn, "123")
    assert out is not None
    assert out.account_id == "123"
    assert out.platform == "google_ads"


def test_fetch_account_performance_computes_derivatives(fake_conn):
    conn = fake_conn([[(1000, 100, 200.0, 10.0, 600.0)]])
    out = fetch_account_performance(conn, "123", S, E)
    assert out.ctr == 10.0  # 100/1000*100
    assert out.cpc == 2.0  # 200/100
    assert out.cpa == 20.0  # 200/10
    assert out.roas == 3.0  # 600/200


def test_fetch_account_performance_handles_zero_divisors(fake_conn):
    conn = fake_conn([[(0, 0, 0.0, 0.0, 0.0)]])
    out = fetch_account_performance(conn, "123", S, E)
    assert out.ctr is None
    assert out.cpc is None
    assert out.cpa is None
    assert out.roas is None


def test_fetch_campaign_status_rows_computes_roas(fake_conn):
    conn = fake_conn([[
        ("Camp A", "ENABLED", 100.0, 5.0, 250.0),
        ("Camp B", "PAUSED", 50.0, 0.0, 0.0),
    ]])
    out = fetch_campaign_status_rows(conn, "123", S, E)
    assert out[0].roas == 2.5
    assert out[1].roas == 0.0


def test_fetch_top_keywords_with_roas_parses_row(fake_conn):
    conn = fake_conn([[(
        "kw1", "EXACT", "Camp A", "AG1",
        1000, 100, 200.0, 10.0, 400.0, 7.5,
    )]])
    out = fetch_top_keywords_with_roas(conn, "123", S, E)
    assert out[0].keyword_text == "kw1"
    assert out[0].roas == 2.0
    assert out[0].quality_score == 7.5


def test_fetch_budget_pacing_row_returns_unknown_when_absent(fake_conn):
    conn = fake_conn([[]])
    out = fetch_budget_pacing_row(conn, "123")
    assert out.pace_flag == "unknown"
    assert out.monthly_budget is None
    assert out.spent_mtd == 0.0


def test_fetch_budget_pacing_row_propagates_values(fake_conn):
    conn = fake_conn([[(5000.0, "BRL", 3000.0, 60.0, 50.0, "over")]])
    out = fetch_budget_pacing_row(conn, "123")
    assert out.pace_flag == "over"
    assert out.pct_consumed == 60.0
    assert out.monthly_budget == 5000.0
