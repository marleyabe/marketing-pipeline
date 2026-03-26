import json

import pytest

from src.db.schema import initialize_schemas
from src.loaders.postgres_loader import PostgresBronzeLoader


@pytest.fixture
def loader(memory_connection):
    initialize_schemas(memory_connection)
    return PostgresBronzeLoader(memory_connection)


def _count(conn, table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]


def _fetchone(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


class TestPostgresBronzeLoader:
    def test_load_meta_ads_creates_rows(self, loader, memory_connection, sample_meta_ads_data):
        loader.load(sample_meta_ads_data, "meta_ads_raw")
        assert _count(memory_connection, "bronze.meta_ads_raw") == 2

    def test_load_appends_data(self, loader, memory_connection, sample_meta_ads_data):
        loader.load(sample_meta_ads_data, "meta_ads_raw")
        loader.load(sample_meta_ads_data, "meta_ads_raw")
        assert _count(memory_connection, "bronze.meta_ads_raw") == 4

    def test_load_adds_extracted_at(self, loader, memory_connection, sample_meta_ads_data):
        loader.load(sample_meta_ads_data, "meta_ads_raw")
        row = _fetchone(memory_connection, "SELECT _extracted_at FROM bronze.meta_ads_raw LIMIT 1")
        assert row[0] is not None

    def test_load_adds_source_column(self, loader, memory_connection, sample_meta_ads_data):
        loader.load(sample_meta_ads_data, "meta_ads_raw", source="meta_ads")
        row = _fetchone(memory_connection, "SELECT DISTINCT _source FROM bronze.meta_ads_raw")
        assert row[0] == "meta_ads"

    def test_load_google_ads_creates_rows(self, loader, memory_connection, sample_google_ads_data):
        loader.load(sample_google_ads_data, "google_ads_raw", source="google_ads")
        assert _count(memory_connection, "bronze.google_ads_raw") == 2

    def test_load_preserves_json_actions(self, loader, memory_connection, sample_meta_ads_data):
        loader.load(sample_meta_ads_data, "meta_ads_raw")
        row = _fetchone(memory_connection, "SELECT actions FROM bronze.meta_ads_raw WHERE ad_id = 'ad_1'")
        actions = json.loads(row[0])
        assert len(actions) == 2
        assert actions[0]["action_type"] == "link_click"

    def test_load_empty_data_is_noop(self, loader, memory_connection):
        loader.load([], "meta_ads_raw")
        assert _count(memory_connection, "bronze.meta_ads_raw") == 0
