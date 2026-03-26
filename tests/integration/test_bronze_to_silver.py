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
    runner.run_silver()

    return memory_connection


class TestBronzeToSilver:
    def test_silver_meta_ads_deduplicates(self, setup_pipeline):
        row = _fetchone(setup_pipeline, "SELECT COUNT(*) FROM silver.meta_ads")
        assert row[0] == 2

    def test_silver_google_ads_spend_per_campaign(self, setup_pipeline):
        row = _fetchone(
            setup_pipeline,
            "SELECT spend FROM silver.google_ads WHERE campaign_id = 'g_camp_1'",
        )
        assert row[0] == 150.0

    def test_silver_google_ads_aggregates_multiple_keywords_into_one_campaign_row(self, memory_connection):
        initialize_schemas(memory_connection)
        loader = PostgresBronzeLoader(memory_connection)
        loader.load([
            {
                "customer_id": "789012", "customer_name": "Cliente B",
                "campaign_id": "gc1", "campaign_name": "Camp Google",
                "ad_group_id": "ag1", "ad_group_name": "Grupo 1",
                "keyword_id": "kw1", "keyword_text": "palavra 1", "match_type": "BROAD",
                "impressions": 1000, "clicks": 40, "spend": 100.0, "conversions": 5.0,
                "date": "2026-03-22",
            },
            {
                "customer_id": "789012", "customer_name": "Cliente B",
                "campaign_id": "gc1", "campaign_name": "Camp Google",
                "ad_group_id": "ag1", "ad_group_name": "Grupo 1",
                "keyword_id": "kw2", "keyword_text": "palavra 2", "match_type": "EXACT",
                "impressions": 500, "clicks": 20, "spend": 50.0, "conversions": 3.0,
                "date": "2026-03-22",
            },
        ], "google_ads_raw", source="google_ads")

        SQLRunner(memory_connection, SQL_DIR).run_silver()

        rows = _fetchall(
            memory_connection,
            "SELECT impressions, clicks, spend, conversions FROM silver.google_ads WHERE campaign_id = 'gc1'",
        )

        assert len(rows) == 1
        assert rows[0][0] == 1500
        assert rows[0][1] == 60
        assert rows[0][2] == 150.0
        assert rows[0][3] == 8.0

    def test_silver_nulls_replaced_with_defaults(self, setup_pipeline):
        rows = _fetchall(
            setup_pipeline,
            "SELECT impressions, clicks, spend FROM silver.meta_ads",
        )
        for row in rows:
            assert row[0] >= 0
            assert row[1] >= 0
            assert row[2] >= 0.0

    def test_silver_unified_merges_platforms(self, setup_pipeline):
        platforms = _fetchall(
            setup_pipeline,
            "SELECT DISTINCT platform FROM silver.unified_campaigns ORDER BY platform",
        )
        assert [p[0] for p in platforms] == ["google_ads", "meta_ads"]

    def test_silver_unified_row_count(self, setup_pipeline):
        row = _fetchone(setup_pipeline, "SELECT COUNT(*) FROM silver.unified_campaigns")
        assert row[0] == 4

    def test_silver_standardizes_date(self, setup_pipeline):
        dates = _fetchall(setup_pipeline, "SELECT DISTINCT date FROM silver.unified_campaigns")
        for row in dates:
            assert row[0] is not None
