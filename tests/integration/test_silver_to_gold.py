import os

import pytest

from src.db.schema import initialize_schemas
from src.loaders.postgres_loader import PostgresBronzeLoader
from src.transformers.sql_runner import SQLRunner

SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "sql")


def _fetchone(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def _fetchall(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


@pytest.fixture
def setup_pipeline(memory_connection, sample_meta_ads_data, sample_google_ads_data):
    initialize_schemas(memory_connection)
    loader = PostgresBronzeLoader(memory_connection)
    loader.load(sample_meta_ads_data, "meta_ads_raw", source="meta_ads")
    loader.load(sample_google_ads_data, "google_ads_raw", source="google_ads")

    runner = SQLRunner(memory_connection, SQL_DIR)
    runner.run_all()

    return memory_connection


class TestSilverToGold:
    def test_daily_performance_aggregates_by_account_date(self, setup_pipeline):
        rows = _fetchall(
            setup_pipeline,
            "SELECT account_id, date, impressions, clicks, spend "
            "FROM gold.daily_performance ORDER BY account_id",
        )
        assert len(rows) == 2

    def test_daily_performance_meta_aggregation(self, setup_pipeline):
        row = _fetchone(
            setup_pipeline,
            "SELECT impressions, clicks, spend FROM gold.daily_performance "
            "WHERE account_id = 'act_123'",
        )
        assert row[0] == 3000
        assert row[1] == 130
        assert row[2] == 300.50

    def test_daily_performance_google_aggregation(self, setup_pipeline):
        row = _fetchone(
            setup_pipeline,
            "SELECT impressions, clicks, spend FROM gold.daily_performance "
            "WHERE account_id = '789012'",
        )
        assert row[0] == 4500
        assert row[1] == 180
        assert row[2] == 450.0

    def test_daily_performance_calculates_ctr(self, setup_pipeline):
        row = _fetchone(
            setup_pipeline,
            "SELECT ctr FROM gold.daily_performance WHERE account_id = '789012'",
        )
        assert row[0] == 4.0

    def test_daily_performance_calculates_cost_per_conversion(self, setup_pipeline):
        row = _fetchone(
            setup_pipeline,
            "SELECT cost_per_conversion FROM gold.daily_performance "
            "WHERE account_id = '789012'",
        )
        assert float(row[0]) == 19.57

    def test_reports_daily_has_correct_fields(self, setup_pipeline):
        cols = _fetchall(
            setup_pipeline,
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'gold' AND table_name = 'reports_daily' "
            "ORDER BY ordinal_position",
        )
        column_names = [c[0] for c in cols]
        assert "investimento" in column_names
        assert "impressoes" in column_names
        assert "cliques" in column_names
        assert "conversoes" in column_names
        assert "custo_por_conversao" in column_names
        assert "taxa_de_conversao" in column_names

    def test_alerts_daily_exists(self, setup_pipeline):
        row = _fetchone(setup_pipeline, "SELECT COUNT(*) FROM gold.alerts_daily")
        assert row[0] >= 0
