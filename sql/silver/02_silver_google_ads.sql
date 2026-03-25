CREATE OR REPLACE TABLE silver.google_ads AS
WITH deduplicated AS (
    SELECT
        customer_id,
        customer_name,
        campaign_id,
        campaign_name,
        keyword_id,
        impressions,
        clicks,
        spend,
        conversions,
        date,
        _extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, campaign_id, keyword_id, CAST(date AS VARCHAR)
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
    SUM(COALESCE(impressions, 0))::BIGINT AS impressions,
    SUM(COALESCE(clicks, 0))::BIGINT AS clicks,
    SUM(COALESCE(spend, 0.0))::DOUBLE AS spend,
    CAST(date AS DATE) AS date,
    SUM(COALESCE(conversions, 0.0))::DOUBLE AS conversions,
    NULL::VARCHAR AS actions_raw,
    MAX(_extracted_at) AS _extracted_at,
    'google_ads' AS platform
FROM deduplicated
WHERE rn = 1
GROUP BY customer_id, customer_name, campaign_id, campaign_name, date;
