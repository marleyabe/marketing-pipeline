import json

import pytest

from src.db.schema import initialize_schemas
from src.loaders.duckdb_loader import DuckDBBronzeLoader


@pytest.fixture
def loader(memory_connection):
    initialize_schemas(memory_connection)
    return DuckDBBronzeLoader(memory_connection)


class TestDuckDBBronzeLoader:
    def test_load_meta_ads_creates_rows(self, loader, memory_connection, sample_meta_ads_data):
        loader.load(sample_meta_ads_data, "meta_ads_raw")
        count = memory_connection.execute(
            "SELECT COUNT(*) FROM bronze.meta_ads_raw"
        ).fetchone()[0]
        assert count == 2

    def test_load_appends_data(self, loader, memory_connection, sample_meta_ads_data):
        loader.load(sample_meta_ads_data, "meta_ads_raw")
        loader.load(sample_meta_ads_data, "meta_ads_raw")
        count = memory_connection.execute(
            "SELECT COUNT(*) FROM bronze.meta_ads_raw"
        ).fetchone()[0]
        assert count == 4

    def test_load_adds_extracted_at(self, loader, memory_connection, sample_meta_ads_data):
        loader.load(sample_meta_ads_data, "meta_ads_raw")
        result = memory_connection.execute(
            "SELECT _extracted_at FROM bronze.meta_ads_raw LIMIT 1"
        ).fetchone()
        assert result[0] is not None

    def test_load_adds_source_column(self, loader, memory_connection, sample_meta_ads_data):
        loader.load(sample_meta_ads_data, "meta_ads_raw", source="meta_ads")
        result = memory_connection.execute(
            "SELECT DISTINCT _source FROM bronze.meta_ads_raw"
        ).fetchone()
        assert result[0] == "meta_ads"

    def test_load_google_ads_creates_rows(self, loader, memory_connection, sample_google_ads_data):
        loader.load(sample_google_ads_data, "google_ads_raw", source="google_ads")
        count = memory_connection.execute(
            "SELECT COUNT(*) FROM bronze.google_ads_raw"
        ).fetchone()[0]
        assert count == 2

    def test_load_preserves_json_actions(self, loader, memory_connection, sample_meta_ads_data):
        loader.load(sample_meta_ads_data, "meta_ads_raw")
        result = memory_connection.execute(
            "SELECT actions FROM bronze.meta_ads_raw WHERE ad_id = 'ad_1'"
        ).fetchone()
        actions = json.loads(result[0])
        assert len(actions) == 2
        assert actions[0]["action_type"] == "link_click"

    def test_load_empty_data_is_noop(self, loader, memory_connection):
        loader.load([], "meta_ads_raw")
        count = memory_connection.execute(
            "SELECT COUNT(*) FROM bronze.meta_ads_raw"
        ).fetchone()[0]
        assert count == 0
