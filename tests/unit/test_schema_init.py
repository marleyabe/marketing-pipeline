import duckdb
import pytest

from src.db.schema import initialize_schemas


class TestInitializeSchemas:
    def test_creates_bronze_silver_gold_schemas(self, memory_connection):
        initialize_schemas(memory_connection)

        schemas = memory_connection.execute(
            "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name"
        ).fetchall()
        schema_names = [s[0] for s in schemas]

        assert "bronze" in schema_names
        assert "silver" in schema_names
        assert "gold" in schema_names

    def test_creates_bronze_meta_ads_table(self, memory_connection):
        initialize_schemas(memory_connection)

        columns = memory_connection.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'bronze' AND table_name = 'meta_ads_raw' "
            "ORDER BY ordinal_position"
        ).fetchall()
        column_names = [c[0] for c in columns]

        assert "account_id" in column_names
        assert "campaign_id" in column_names
        assert "ad_id" in column_names
        assert "impressions" in column_names
        assert "clicks" in column_names
        assert "spend" in column_names
        assert "actions" in column_names
        assert "date_start" in column_names
        assert "_extracted_at" in column_names
        assert "_source" in column_names

    def test_creates_bronze_google_ads_table(self, memory_connection):
        initialize_schemas(memory_connection)

        columns = memory_connection.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'bronze' AND table_name = 'google_ads_raw' "
            "ORDER BY ordinal_position"
        ).fetchall()
        column_names = [c[0] for c in columns]

        assert "customer_id" in column_names
        assert "campaign_id" in column_names
        assert "keyword_id" in column_names
        assert "keyword_text" in column_names
        assert "match_type" in column_names
        assert "impressions" in column_names
        assert "clicks" in column_names
        assert "spend" in column_names
        assert "conversions" in column_names
        assert "view_through_conversions" in column_names
        assert "all_conversions" in column_names
        assert "search_impression_share" in column_names
        assert "quality_score" in column_names
        assert "device" in column_names
        assert "date" in column_names
        assert "_extracted_at" in column_names
        assert "_source" in column_names

    def test_is_idempotent(self, memory_connection):
        initialize_schemas(memory_connection)
        initialize_schemas(memory_connection)

        schemas = memory_connection.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name IN ('bronze', 'silver', 'gold')"
        ).fetchall()
        assert len(schemas) == 3
