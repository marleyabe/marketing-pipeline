import os
import uuid

import psycopg2
import pytest

_TEST_HOST = os.environ.get("TEST_POSTGRES_HOST", "localhost")
_TEST_PORT = int(os.environ.get("TEST_POSTGRES_PORT", "5432"))
_TEST_USER = os.environ.get("TEST_POSTGRES_USER", "pipeline")
_TEST_PASSWORD = os.environ.get("TEST_POSTGRES_PASSWORD", "pipeline")

_ADMIN_DSN = f"postgresql://{_TEST_USER}:{_TEST_PASSWORD}@{_TEST_HOST}:{_TEST_PORT}/postgres"


@pytest.fixture
def memory_connection():
    """Creates a fresh temporary PostgreSQL database for each test, drops it afterwards."""
    db_name = f"test_{uuid.uuid4().hex[:12]}"

    admin = psycopg2.connect(_ADMIN_DSN)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{db_name}"')
    admin.close()

    dsn = f"postgresql://{_TEST_USER}:{_TEST_PASSWORD}@{_TEST_HOST}:{_TEST_PORT}/{db_name}"
    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    yield conn

    conn.close()

    admin = psycopg2.connect(_ADMIN_DSN)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE "{db_name}"')
    admin.close()


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
            "device_platform": "mobile_web",
            "publisher_platform": "facebook",
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
            "device_platform": "desktop",
            "publisher_platform": "instagram",
        },
    ]


@pytest.fixture
def sample_google_ads_data():
    """Sample raw data mimicking Google Ads Keywords API response."""
    return [
        {
            "customer_id": "789012",
            "customer_name": "Cliente B",
            "campaign_id": "g_camp_1",
            "campaign_name": "Google Campanha 1",
            "ad_group_id": "ag_1",
            "ad_group_name": "Grupo de Anúncios 1",
            "keyword_id": "kw_1",
            "keyword_text": "palavra chave 1",
            "match_type": "BROAD",
            "impressions": 1500,
            "clicks": 60,
            "spend": 150.0,
            "conversions": 8.0,
            "view_through_conversions": 1.0,
            "all_conversions": 10.0,
            "search_impression_share": 0.85,
            "quality_score": 7,
            "device": "MOBILE",
            "date": "2026-03-22",
        },
        {
            "customer_id": "789012",
            "customer_name": "Cliente B",
            "campaign_id": "g_camp_2",
            "campaign_name": "Google Campanha 2",
            "ad_group_id": "ag_2",
            "ad_group_name": "Grupo de Anúncios 2",
            "keyword_id": "kw_2",
            "keyword_text": "palavra chave 2",
            "match_type": "EXACT",
            "impressions": 3000,
            "clicks": 120,
            "spend": 300.0,
            "conversions": 15.0,
            "view_through_conversions": 2.0,
            "all_conversions": 18.0,
            "search_impression_share": 0.92,
            "quality_score": 9,
            "device": "DESKTOP",
            "date": "2026-03-22",
        },
    ]
