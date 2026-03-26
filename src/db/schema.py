import psycopg2.extensions


def initialize_schemas(conn: psycopg2.extensions.connection) -> None:
    """Create bronze/silver/gold schemas and base tables."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        cur.execute("CREATE SCHEMA IF NOT EXISTS silver")
        cur.execute("CREATE SCHEMA IF NOT EXISTS gold")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS bronze.meta_ads_raw (
                account_id VARCHAR,
                account_name VARCHAR,
                campaign_id VARCHAR,
                campaign_name VARCHAR,
                ad_id VARCHAR,
                ad_name VARCHAR,
                impressions BIGINT,
                clicks BIGINT,
                spend DOUBLE PRECISION,
                date_start DATE,
                date_stop DATE,
                actions VARCHAR,
                device_platform VARCHAR,
                publisher_platform VARCHAR,
                _extracted_at TIMESTAMP DEFAULT current_timestamp,
                _source VARCHAR DEFAULT 'meta_ads'
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS bronze.meta_ads_demographics_raw (
                account_id VARCHAR,
                account_name VARCHAR,
                campaign_id VARCHAR,
                campaign_name VARCHAR,
                ad_id VARCHAR,
                ad_name VARCHAR,
                age VARCHAR,
                gender VARCHAR,
                impressions BIGINT,
                clicks BIGINT,
                spend DOUBLE PRECISION,
                date_start DATE,
                date_stop DATE,
                actions VARCHAR,
                _extracted_at TIMESTAMP DEFAULT current_timestamp,
                _source VARCHAR DEFAULT 'meta_ads'
            )
        """)

        cur.execute("""
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
                spend DOUBLE PRECISION,
                conversions DOUBLE PRECISION,
                view_through_conversions DOUBLE PRECISION,
                all_conversions DOUBLE PRECISION,
                search_impression_share DOUBLE PRECISION,
                quality_score INTEGER,
                device VARCHAR,
                date DATE,
                _extracted_at TIMESTAMP DEFAULT current_timestamp,
                _source VARCHAR DEFAULT 'google_ads'
            )
        """)

        # Migrate existing tables that may have been created before new columns were added
        for col, col_type in [
            ("view_through_conversions", "DOUBLE PRECISION"),
            ("all_conversions", "DOUBLE PRECISION"),
            ("search_impression_share", "DOUBLE PRECISION"),
            ("quality_score", "INTEGER"),
            ("device", "VARCHAR"),
        ]:
            cur.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema='bronze' AND table_name='google_ads_raw' AND column_name='{col}'
                    ) THEN
                        ALTER TABLE bronze.google_ads_raw ADD COLUMN {col} {col_type};
                    END IF;
                END$$;
            """)

        for col, col_type in [
            ("device_platform", "VARCHAR"),
            ("publisher_platform", "VARCHAR"),
        ]:
            cur.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema='bronze' AND table_name='meta_ads_raw' AND column_name='{col}'
                    ) THEN
                        ALTER TABLE bronze.meta_ads_raw ADD COLUMN {col} {col_type};
                    END IF;
                END$$;
            """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS bronze.google_ads_demographics_raw (
                customer_id VARCHAR,
                customer_name VARCHAR,
                campaign_id VARCHAR,
                campaign_name VARCHAR,
                ad_group_id VARCHAR,
                ad_group_name VARCHAR,
                dimension_type VARCHAR,
                dimension_value VARCHAR,
                impressions BIGINT,
                clicks BIGINT,
                spend DOUBLE PRECISION,
                conversions DOUBLE PRECISION,
                date DATE,
                _extracted_at TIMESTAMP DEFAULT current_timestamp,
                _source VARCHAR DEFAULT 'google_ads'
            )
        """)

    conn.commit()
