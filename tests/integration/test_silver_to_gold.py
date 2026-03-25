import os

import pytest

from src.db.schema import initialize_schemas
from src.loaders.duckdb_loader import DuckDBBronzeLoader
from src.transformers.sql_runner import SQLRunner

SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "sql")


@pytest.fixture
def setup_pipeline(memory_connection, sample_meta_ads_data, sample_google_ads_data):
    initialize_schemas(memory_connection)
    loader = DuckDBBronzeLoader(memory_connection)
    loader.load(sample_meta_ads_data, "meta_ads_raw", source="meta_ads")
    loader.load(sample_google_ads_data, "google_ads_keywords_raw", source="google_ads_keywords")

    runner = SQLRunner(memory_connection, SQL_DIR)
    runner.run_all()

    return memory_connection


class TestSilverToGold:
    def test_daily_performance_aggregates_by_account_date(self, setup_pipeline):
        conn = setup_pipeline
        rows = conn.execute(
            "SELECT account_id, date, impressions, clicks, spend "
            "FROM gold.daily_performance ORDER BY account_id"
        ).fetchall()
        # 2 accounts: act_123 (meta) and 789012 (google)
        assert len(rows) == 2

    def test_daily_performance_meta_aggregation(self, setup_pipeline):
        conn = setup_pipeline
        row = conn.execute(
            "SELECT impressions, clicks, spend FROM gold.daily_performance "
            "WHERE account_id = 'act_123'"
        ).fetchone()
        assert row[0] == 3000  # 1000 + 2000
        assert row[1] == 130   # 50 + 80
        assert row[2] == 300.50  # 100.50 + 200.00

    def test_daily_performance_google_aggregation(self, setup_pipeline):
        conn = setup_pipeline
        row = conn.execute(
            "SELECT impressions, clicks, spend FROM gold.daily_performance "
            "WHERE account_id = '789012'"
        ).fetchone()
        assert row[0] == 4500  # 1500 + 3000
        assert row[1] == 180   # 60 + 120
        assert row[2] == 450.0  # 150 + 300

    def test_daily_performance_calculates_ctr(self, setup_pipeline):
        conn = setup_pipeline
        ctr = conn.execute(
            "SELECT ctr FROM gold.daily_performance WHERE account_id = '789012'"
        ).fetchone()[0]
        # 180 / 4500 * 100 = 4.0
        assert ctr == 4.0

    def test_daily_performance_calculates_cost_per_conversion(self, setup_pipeline):
        conn = setup_pipeline
        cpc_conv = conn.execute(
            "SELECT cost_per_conversion FROM gold.daily_performance "
            "WHERE account_id = '789012'"
        ).fetchone()[0]
        # 450 / 23 = 19.57
        assert cpc_conv == 19.57

    def test_reports_daily_has_correct_fields(self, setup_pipeline):
        conn = setup_pipeline
        columns = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'gold' AND table_name = 'reports_daily' "
            "ORDER BY ordinal_position"
        ).fetchall()
        column_names = [c[0] for c in columns]
        assert "investimento" in column_names
        assert "impressoes" in column_names
        assert "cliques" in column_names
        assert "conversoes" in column_names
        assert "custo_por_conversao" in column_names
        assert "taxa_de_conversao" in column_names

    def test_alerts_daily_exists(self, setup_pipeline):
        conn = setup_pipeline
        # With single day data, previous day doesn't exist so change_pct = 0
        count = conn.execute(
            "SELECT COUNT(*) FROM gold.alerts_daily"
        ).fetchone()[0]
        assert count >= 0  # view exists and is queryable
