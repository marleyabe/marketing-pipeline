{{ config(schema='silver', materialized='table') }}

WITH deduplicated AS (
    SELECT
        customer_id,
        customer_name,
        campaign_id,
        campaign_name,
        keyword_id,
        device,
        impressions,
        clicks,
        spend,
        conversions,
        view_through_conversions,
        all_conversions,
        search_impression_share,
        quality_score,
        date,
        _extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, campaign_id, keyword_id, device, CAST(date AS VARCHAR)
            ORDER BY _extracted_at DESC
        ) AS rn
    FROM {{ source('bronze', 'google_ads_raw') }}
)
SELECT
    CAST(customer_id AS VARCHAR) AS account_id,
    CAST(customer_name AS VARCHAR) AS account_name,
    CAST(campaign_id AS VARCHAR) AS campaign_id,
    CAST(campaign_name AS VARCHAR) AS campaign_name,
    CAST(NULL AS VARCHAR) AS ad_id,
    CAST(NULL AS VARCHAR) AS ad_name,
    CAST(device AS VARCHAR) AS device,
    CAST(SUM(COALESCE(impressions, 0)) AS BIGINT) AS impressions,
    CAST(SUM(COALESCE(clicks, 0)) AS BIGINT) AS clicks,
    CAST(SUM(COALESCE(spend, 0.0)) AS DOUBLE PRECISION) AS spend,
    CAST(date AS DATE) AS date,
    CAST(SUM(COALESCE(conversions, 0.0)) AS DOUBLE PRECISION) AS conversions,
    CAST(SUM(COALESCE(view_through_conversions, 0.0)) AS DOUBLE PRECISION) AS view_through_conversions,
    CAST(SUM(COALESCE(all_conversions, 0.0)) AS DOUBLE PRECISION) AS all_conversions,
    CAST(AVG(search_impression_share) AS DOUBLE PRECISION) AS search_impression_share,
    CAST(AVG(quality_score) AS DOUBLE PRECISION) AS quality_score,
    CAST(NULL AS VARCHAR) AS actions_raw,
    MAX(_extracted_at) AS _extracted_at,
    'google_ads' AS platform
FROM deduplicated
WHERE rn = 1
GROUP BY customer_id, customer_name, campaign_id, campaign_name, device, date
