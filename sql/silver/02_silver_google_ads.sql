CREATE OR REPLACE TABLE silver.google_ads AS
WITH deduplicated AS (
    SELECT
        customer_id,
        customer_name,
        campaign_id,
        campaign_name,
        impressions,
        clicks,
        spend,
        conversions,
        date,
        _extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, campaign_id, CAST(date AS VARCHAR)
            ORDER BY _extracted_at DESC
        ) AS rn
    FROM bronze.google_ads_raw
)
SELECT
    CAST(customer_id AS VARCHAR) AS account_id,
    CAST(customer_name AS VARCHAR) AS account_name,
    CAST(campaign_id AS VARCHAR) AS campaign_id,
    CAST(campaign_name AS VARCHAR) AS campaign_name,
    NULL::VARCHAR AS ad_id,
    NULL::VARCHAR AS ad_name,
    COALESCE(impressions, 0)::BIGINT AS impressions,
    COALESCE(clicks, 0)::BIGINT AS clicks,
    COALESCE(spend, 0.0)::DOUBLE AS spend,
    CAST(date AS DATE) AS date,
    COALESCE(conversions, 0.0)::DOUBLE AS conversions,
    NULL::VARCHAR AS actions_raw,
    _extracted_at,
    'google_ads' AS platform
FROM deduplicated
WHERE rn = 1;
