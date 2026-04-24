{{ config(schema='silver', materialized='table') }}

-- Dedupe por (date, account_id, ad_group_id, search_term, matched_keyword).
-- Agregação via SUM garante consistência se a API retornar múltiplos
-- matched_keyword para o mesmo search_term no mesmo dia.
WITH deduplicated AS (
    SELECT
        customer_id,
        customer_name,
        campaign_id,
        campaign_name,
        ad_group_id,
        ad_group_name,
        search_term,
        search_term_status,
        matched_keyword_text,
        matched_keyword_match_type,
        impressions,
        clicks,
        spend,
        conversions,
        conversion_value,
        date,
        _extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, ad_group_id, search_term,
                         COALESCE(matched_keyword_text, ''),
                         COALESCE(matched_keyword_match_type, ''),
                         CAST(date AS VARCHAR)
            ORDER BY _extracted_at DESC
        ) AS rn
    FROM {{ source('bronze', 'google_search_terms_raw') }}
    WHERE search_term IS NOT NULL AND search_term <> ''
)
SELECT
    CAST(customer_id AS VARCHAR) AS account_id,
    CAST(customer_name AS VARCHAR) AS account_name,
    CAST(campaign_id AS VARCHAR) AS campaign_id,
    CAST(campaign_name AS VARCHAR) AS campaign_name,
    CAST(ad_group_id AS VARCHAR) AS ad_group_id,
    CAST(ad_group_name AS VARCHAR) AS ad_group_name,
    CAST(search_term AS VARCHAR) AS search_term,
    CAST(search_term_status AS VARCHAR) AS search_term_status,
    CAST(matched_keyword_text AS VARCHAR) AS matched_keyword_text,
    CAST(matched_keyword_match_type AS VARCHAR) AS matched_keyword_match_type,
    CAST(date AS DATE) AS date,
    CAST(COALESCE(impressions, 0) AS BIGINT) AS impressions,
    CAST(COALESCE(clicks, 0) AS BIGINT) AS clicks,
    CAST(COALESCE(spend, 0.0) AS DOUBLE PRECISION) AS spend,
    CAST(COALESCE(conversions, 0.0) AS DOUBLE PRECISION) AS conversions,
    CAST(COALESCE(conversion_value, 0.0) AS DOUBLE PRECISION) AS conversion_value,
    _extracted_at,
    'google_ads' AS platform
FROM deduplicated
WHERE rn = 1
