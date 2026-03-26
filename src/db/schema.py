import duckdb


def initialize_schemas(conn: duckdb.DuckDBPyConnection) -> None:
    """Create bronze/silver/gold schemas and base tables."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
    conn.execute("CREATE SCHEMA IF NOT EXISTS gold")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze.meta_ads_raw (
            account_id VARCHAR,
            account_name VARCHAR,
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            ad_id VARCHAR,
            ad_name VARCHAR,
            impressions BIGINT,
            clicks BIGINT,
            spend DOUBLE,
            date_start DATE,
            date_stop DATE,
            actions VARCHAR,
            _extracted_at TIMESTAMP DEFAULT current_timestamp,
            _source VARCHAR DEFAULT 'meta_ads'
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze.google_ads_raw (
            customer_id VARCHAR,
            customer_name VARCHAR,
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            ad_group_id VARCHAR,
            ad_group_name VARCHAR,
            keyword_id VARCHAR,
            keyword_text VARCHAR,
            match_type VARCHAR,
            impressions BIGINT,
            clicks BIGINT,
            spend DOUBLE,
            conversions DOUBLE,
            view_through_conversions DOUBLE,
            all_conversions DOUBLE,
            search_impression_share DOUBLE,
            quality_score INTEGER,
            device VARCHAR,
            date DATE,
            _extracted_at TIMESTAMP DEFAULT current_timestamp,
            _source VARCHAR DEFAULT 'google_ads'
        )
    """)
