{{ config(schema='silver', materialized='table') }}

WITH deduplicated AS (
    SELECT
        customer_id,
        customer_name,
        campaign_id,
        campaign_name,
        campaign_status,
        ad_group_id,
        ad_group_name,
        ad_group_status,
        keyword_id,
        criterion_status,
        keyword_text,
        match_type,
        device,
        impressions,
        clicks,
        spend,
        conversions,
        conversion_value,
        quality_score,
        date,
        _extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, campaign_id, ad_group_id, keyword_id, device, CAST(date AS VARCHAR)
            ORDER BY _extracted_at DESC
        ) AS rn
    FROM {{ source('bronze', 'google_ads_raw') }}
    WHERE keyword_text IS NOT NULL AND keyword_text <> ''
)
SELECT
    CAST(customer_id AS VARCHAR) AS account_id,
    CAST(customer_name AS VARCHAR) AS account_name,
    CAST(campaign_id AS VARCHAR) AS campaign_id,
    CAST(campaign_name AS VARCHAR) AS campaign_name,
    CAST(ad_group_id AS VARCHAR) AS ad_group_id,
    CAST(ad_group_name AS VARCHAR) AS ad_group_name,
    CAST(keyword_id AS VARCHAR) AS keyword_id,
    CAST(keyword_text AS VARCHAR) AS keyword_text,
    CAST(match_type AS VARCHAR) AS match_type,
    CAST(date AS DATE) AS date,
    CAST(SUM(COALESCE(impressions, 0)) AS BIGINT) AS impressions,
    CAST(SUM(COALESCE(clicks, 0)) AS BIGINT) AS clicks,
    CAST(SUM(COALESCE(spend, 0.0)) AS DOUBLE PRECISION) AS spend,
    CAST(SUM(COALESCE(conversions, 0.0)) AS DOUBLE PRECISION) AS conversions,
    CAST(SUM(COALESCE(conversion_value, 0.0)) AS DOUBLE PRECISION) AS conversion_value,
    CAST(AVG(quality_score) AS DOUBLE PRECISION) AS quality_score,
    MAX(campaign_status) AS campaign_status,
    MAX(ad_group_status) AS ad_group_status,
    MAX(criterion_status) AS criterion_status,
    MAX(_extracted_at) AS _extracted_at,
    'google_ads' AS platform
FROM deduplicated
WHERE rn = 1
GROUP BY
    customer_id, customer_name, campaign_id, campaign_name,
    ad_group_id, ad_group_name, keyword_id, keyword_text, match_type, date
