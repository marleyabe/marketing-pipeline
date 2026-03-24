import os
import tempfile

import duckdb
import pytest


@pytest.fixture
def memory_connection():
    """DuckDB in-memory connection for unit tests."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def file_connection(tmp_path):
    """DuckDB file-based connection for tests that need persistence."""
    db_path = str(tmp_path / "test.duckdb")
    conn = duckdb.connect(db_path)
    yield conn, db_path
    conn.close()


@pytest.fixture
def sample_meta_ads_data():
    """Sample raw data mimicking Meta Ads API response."""
    return [
        {
            "account_id": "act_123",
            "account_name": "Cliente A",
            "campaign_id": "camp_1",
            "campaign_name": "Campanha 1",
            "ad_id": "ad_1",
            "ad_name": "Anuncio 1",
            "impressions": "1000",
            "clicks": "50",
            "spend": "100.50",
            "date_start": "2026-03-22",
            "date_stop": "2026-03-22",
            "actions": '[{"action_type": "link_click", "value": "50"}, {"action_type": "onsite_conversion.messaging_conversation_started_7d", "value": "10"}]',
        },
        {
            "account_id": "act_123",
            "account_name": "Cliente A",
            "campaign_id": "camp_2",
            "campaign_name": "Campanha 2",
            "ad_id": "ad_2",
            "ad_name": "Anuncio 2",
            "impressions": "2000",
            "clicks": "80",
            "spend": "200.00",
            "date_start": "2026-03-22",
            "date_stop": "2026-03-22",
            "actions": '[{"action_type": "link_click", "value": "80"}, {"action_type": "offsite_conversion.fb_pixel_purchase", "value": "5"}]',
        },
    ]


@pytest.fixture
def sample_google_ads_data():
    """Sample raw data mimicking Google Ads API response."""
    return [
        {
            "customer_id": "789012",
            "customer_name": "Cliente B",
            "campaign_id": "g_camp_1",
            "campaign_name": "Google Campanha 1",
            "impressions": 1500,
            "clicks": 60,
            "cost_micros": 150000000,
            "conversions": 8.0,
            "date": "2026-03-22",
        },
        {
            "customer_id": "789012",
            "customer_name": "Cliente B",
            "campaign_id": "g_camp_2",
            "campaign_name": "Google Campanha 2",
            "impressions": 3000,
            "clicks": 120,
            "cost_micros": 300000000,
            "conversions": 15.0,
            "date": "2026-03-22",
        },
    ]
