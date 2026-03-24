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
    loader.load(sample_google_ads_data, "google_ads_raw", source="google_ads")

    runner = SQLRunner(memory_connection, SQL_DIR)
    runner.run_silver()

    return memory_connection


class TestBronzeToSilver:
    def test_silver_meta_ads_deduplicates(self, setup_pipeline):
        conn = setup_pipeline
        count = conn.execute("SELECT COUNT(*) FROM silver.meta_ads").fetchone()[0]
        assert count == 2  # 2 unique ads

    def test_silver_google_ads_converts_cost(self, setup_pipeline):
        conn = setup_pipeline
        spend = conn.execute(
            "SELECT spend FROM silver.google_ads WHERE campaign_id = 'g_camp_1'"
        ).fetchone()[0]
        assert spend == 150.0  # 150000000 / 1_000_000

    def test_silver_nulls_replaced_with_defaults(self, setup_pipeline):
        conn = setup_pipeline
        result = conn.execute(
            "SELECT impressions, clicks, spend FROM silver.meta_ads"
        ).fetchall()
        for row in result:
            assert row[0] >= 0  # impressions
            assert row[1] >= 0  # clicks
            assert row[2] >= 0.0  # spend

    def test_silver_unified_merges_platforms(self, setup_pipeline):
        conn = setup_pipeline
        platforms = conn.execute(
            "SELECT DISTINCT platform FROM silver.unified_campaigns ORDER BY platform"
        ).fetchall()
        assert [p[0] for p in platforms] == ["google_ads", "meta_ads"]

    def test_silver_unified_row_count(self, setup_pipeline):
        conn = setup_pipeline
        count = conn.execute(
            "SELECT COUNT(*) FROM silver.unified_campaigns"
        ).fetchone()[0]
        assert count == 4  # 2 meta + 2 google

    def test_silver_standardizes_date(self, setup_pipeline):
        conn = setup_pipeline
        dates = conn.execute(
            "SELECT DISTINCT date FROM silver.unified_campaigns"
        ).fetchall()
        for row in dates:
            assert row[0] is not None
