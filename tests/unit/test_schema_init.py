import pytest

from src.db.schema import initialize_schemas


def _column_names(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "ORDER BY ordinal_position",
            [schema, table],
        )
        return [row[0] for row in cur.fetchall()]


def _schema_names(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name")
        return [row[0] for row in cur.fetchall()]


class TestInitializeSchemas:
    def test_creates_bronze_silver_gold_schemas(self, memory_connection):
        initialize_schemas(memory_connection)
        schemas = _schema_names(memory_connection)
        assert "bronze" in schemas
        assert "silver" in schemas
        assert "gold" in schemas

    def test_creates_bronze_meta_ads_table(self, memory_connection):
        initialize_schemas(memory_connection)
        cols = _column_names(memory_connection, "bronze", "meta_ads_raw")
        assert "account_id" in cols
        assert "campaign_id" in cols
        assert "ad_id" in cols
        assert "impressions" in cols
        assert "clicks" in cols
        assert "spend" in cols
        assert "actions" in cols
        assert "date_start" in cols
        assert "_extracted_at" in cols
        assert "_source" in cols

    def test_creates_bronze_google_ads_table(self, memory_connection):
        initialize_schemas(memory_connection)
        cols = _column_names(memory_connection, "bronze", "google_ads_raw")
        assert "customer_id" in cols
        assert "campaign_id" in cols
        assert "keyword_id" in cols
        assert "keyword_text" in cols
        assert "match_type" in cols
        assert "impressions" in cols
        assert "clicks" in cols
        assert "spend" in cols
        assert "conversions" in cols
        assert "view_through_conversions" in cols
        assert "all_conversions" in cols
        assert "search_impression_share" in cols
        assert "quality_score" in cols
        assert "device" in cols
        assert "date" in cols
        assert "_extracted_at" in cols
        assert "_source" in cols

    def test_creates_bronze_google_ads_demographics_table(self, memory_connection):
        initialize_schemas(memory_connection)
        cols = _column_names(memory_connection, "bronze", "google_ads_demographics_raw")
        assert "customer_id" in cols
        assert "campaign_id" in cols
        assert "ad_group_id" in cols
        assert "dimension_type" in cols
        assert "dimension_value" in cols
        assert "impressions" in cols
        assert "clicks" in cols
        assert "spend" in cols
        assert "conversions" in cols
        assert "date" in cols
        assert "_extracted_at" in cols
        assert "_source" in cols

    def test_creates_bronze_meta_ads_demographics_table(self, memory_connection):
        initialize_schemas(memory_connection)
        cols = _column_names(memory_connection, "bronze", "meta_ads_demographics_raw")
        assert "account_id" in cols
        assert "campaign_id" in cols
        assert "ad_id" in cols
        assert "age" in cols
        assert "gender" in cols
        assert "impressions" in cols
        assert "clicks" in cols
        assert "spend" in cols
        assert "actions" in cols
        assert "date_start" in cols
        assert "_extracted_at" in cols
        assert "_source" in cols

    def test_meta_ads_raw_has_device_and_placement_columns(self, memory_connection):
        initialize_schemas(memory_connection)
        cols = _column_names(memory_connection, "bronze", "meta_ads_raw")
        assert "device_platform" in cols
        assert "publisher_platform" in cols

    def test_is_idempotent(self, memory_connection):
        initialize_schemas(memory_connection)
        initialize_schemas(memory_connection)

        with memory_connection.cursor() as cur:
            cur.execute(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name IN ('bronze', 'silver', 'gold')"
            )
            schemas = cur.fetchall()
        assert len(schemas) == 3
