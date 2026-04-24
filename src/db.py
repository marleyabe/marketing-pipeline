import os

import psycopg2

SCHEMAS = ["bronze", "silver", "gold", "ops", "users"]

BRONZE_META_ADS_RAW = """
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
    actions TEXT,
    device_platform VARCHAR,
    publisher_platform VARCHAR,
    _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source VARCHAR DEFAULT 'meta_ads'
)
"""

BRONZE_META_ADS_RAW_UNIQUE = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'bronze' AND indexname = 'bronze_meta_ads_raw_natural_key'
    ) THEN
        DELETE FROM bronze.meta_ads_raw target
        USING (
            SELECT ctid,
                   ROW_NUMBER() OVER (
                       PARTITION BY date_start, account_id, ad_id, device_platform, publisher_platform
                       ORDER BY _extracted_at DESC
                   ) AS rn
            FROM bronze.meta_ads_raw
        ) ranked
        WHERE target.ctid = ranked.ctid AND ranked.rn > 1;

        CREATE UNIQUE INDEX bronze_meta_ads_raw_natural_key
        ON bronze.meta_ads_raw (date_start, account_id, ad_id, device_platform, publisher_platform);
    END IF;
END $$;
"""

BRONZE_GOOGLE_ADS_RAW = """
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
    _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source VARCHAR DEFAULT 'google_ads'
)
"""

BRONZE_GOOGLE_ADS_RAW_UNIQUE = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'bronze' AND indexname = 'bronze_google_ads_raw_natural_key'
    ) THEN
        DELETE FROM bronze.google_ads_raw target
        USING (
            SELECT ctid,
                   ROW_NUMBER() OVER (
                       PARTITION BY date, customer_id, campaign_id, ad_group_id, keyword_id, device
                       ORDER BY _extracted_at DESC
                   ) AS rn
            FROM bronze.google_ads_raw
        ) ranked
        WHERE target.ctid = ranked.ctid AND ranked.rn > 1;

        CREATE UNIQUE INDEX bronze_google_ads_raw_natural_key
        ON bronze.google_ads_raw (date, customer_id, campaign_id, ad_group_id, keyword_id, device);
    END IF;
END $$;
"""

# Colunas novas em google_ads_raw para alimentar review de conta
# (status, ROAS). ADD COLUMN IF NOT EXISTS torna a migração idempotente.
BRONZE_GOOGLE_ADS_RAW_ALTER = """
ALTER TABLE bronze.google_ads_raw
    ADD COLUMN IF NOT EXISTS campaign_status VARCHAR,
    ADD COLUMN IF NOT EXISTS ad_group_status VARCHAR,
    ADD COLUMN IF NOT EXISTS criterion_status VARCHAR,
    ADD COLUMN IF NOT EXISTS conversion_value DOUBLE PRECISION
"""

BRONZE_GOOGLE_SEARCH_TERMS_RAW = """
CREATE TABLE IF NOT EXISTS bronze.google_search_terms_raw (
    customer_id VARCHAR,
    customer_name VARCHAR,
    campaign_id VARCHAR,
    campaign_name VARCHAR,
    ad_group_id VARCHAR,
    ad_group_name VARCHAR,
    search_term VARCHAR,
    search_term_status VARCHAR,
    matched_keyword_text VARCHAR,
    matched_keyword_match_type VARCHAR,
    impressions BIGINT,
    clicks BIGINT,
    spend DOUBLE PRECISION,
    conversions DOUBLE PRECISION,
    conversion_value DOUBLE PRECISION,
    date DATE,
    _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source VARCHAR DEFAULT 'google_ads'
)
"""

# Index sem COALESCE: o extractor normaliza None -> '' antes de inserir, o que
# permite ON CONFLICT bater na chave natural. Com COALESCE na expressão,
# ON CONFLICT (cols) não casaria.
BRONZE_GOOGLE_SEARCH_TERMS_RAW_UNIQUE = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'bronze' AND indexname = 'bronze_google_search_terms_raw_natural_key'
    ) THEN
        CREATE UNIQUE INDEX bronze_google_search_terms_raw_natural_key
        ON bronze.google_search_terms_raw (
            date, customer_id, ad_group_id, search_term,
            matched_keyword_text, matched_keyword_match_type
        );
    END IF;
END $$;
"""

# Negativas são snapshot por dia. Cada task stampa snapshot_date para permitir
# histórico. scope distingue negativa de campanha vs ad_group.
BRONZE_GOOGLE_NEGATIVES_RAW = """
CREATE TABLE IF NOT EXISTS bronze.google_negatives_raw (
    customer_id VARCHAR,
    campaign_id VARCHAR,
    campaign_name VARCHAR,
    ad_group_id VARCHAR,
    ad_group_name VARCHAR,
    criterion_id VARCHAR,
    criterion_text VARCHAR,
    match_type VARCHAR,
    scope VARCHAR,
    snapshot_date DATE,
    _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source VARCHAR DEFAULT 'google_ads'
)
"""

# Negativas de campanha vêm com ad_group_id='' (não NULL) do extractor, para
# permitir ON CONFLICT casar. Sem isso, cada re-run duplicaria linhas de campanha.
BRONZE_GOOGLE_NEGATIVES_RAW_UNIQUE = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'bronze' AND indexname = 'bronze_google_negatives_raw_natural_key'
    ) THEN
        CREATE UNIQUE INDEX bronze_google_negatives_raw_natural_key
        ON bronze.google_negatives_raw (
            snapshot_date, customer_id, scope, criterion_id,
            ad_group_id, campaign_id
        );
    END IF;
END $$;
"""

CLIENT_BUDGET = """
CREATE TABLE IF NOT EXISTS ops.client_budget (
    account_id TEXT NOT NULL,
    platform TEXT NOT NULL CHECK (platform IN ('google_ads', 'meta_ads')),
    monthly_budget NUMERIC(14, 2) NOT NULL CHECK (monthly_budget > 0),
    currency TEXT NOT NULL DEFAULT 'BRL',
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, platform)
)
"""

MANAGED_ACCOUNTS = """
CREATE TABLE IF NOT EXISTS ops.managed_accounts (
    account_id TEXT NOT NULL,
    platform TEXT NOT NULL CHECK (platform IN ('google_ads', 'meta_ads')),
    account_name TEXT,
    enabled SMALLINT NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, platform)
)
"""

APP_USERS = """
CREATE TABLE IF NOT EXISTS users.app_users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    name TEXT,
    role TEXT NOT NULL CHECK (role IN ('admin', 'user')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    disabled_at TIMESTAMP
)
"""

API_KEYS = """
CREATE TABLE IF NOT EXISTS ops.api_keys (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    user_id INTEGER REFERENCES users.app_users(id),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMP,
    revoked_at TIMESTAMP
)
"""

API_KEYS_ALTER_USER_ID = """
ALTER TABLE ops.api_keys
    ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users.app_users(id)
"""

API_AUDIT_LOG = """
CREATE TABLE IF NOT EXISTS ops.api_audit_log (
    id SERIAL PRIMARY KEY,
    api_key_id INTEGER REFERENCES ops.api_keys(id),
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    ip TEXT,
    at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""


def get_pg(autocommit: bool = True) -> psycopg2.extensions.connection:
    connection = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )
    connection.autocommit = autocommit
    return connection


def init_schemas(connection: psycopg2.extensions.connection) -> None:
    with connection.cursor() as cursor:
        for schema_name in SCHEMAS:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        cursor.execute(APP_USERS)
        cursor.execute(BRONZE_META_ADS_RAW)
        cursor.execute(BRONZE_META_ADS_RAW_UNIQUE)
        cursor.execute(BRONZE_GOOGLE_ADS_RAW)
        cursor.execute(BRONZE_GOOGLE_ADS_RAW_UNIQUE)
        cursor.execute(BRONZE_GOOGLE_ADS_RAW_ALTER)
        cursor.execute(BRONZE_GOOGLE_SEARCH_TERMS_RAW)
        cursor.execute(BRONZE_GOOGLE_SEARCH_TERMS_RAW_UNIQUE)
        cursor.execute(BRONZE_GOOGLE_NEGATIVES_RAW)
        cursor.execute(BRONZE_GOOGLE_NEGATIVES_RAW_UNIQUE)
        cursor.execute(MANAGED_ACCOUNTS)
        cursor.execute(CLIENT_BUDGET)
        cursor.execute(API_KEYS)
        cursor.execute(API_KEYS_ALTER_USER_ID)
        cursor.execute(API_AUDIT_LOG)
    if not connection.autocommit:
        connection.commit()
