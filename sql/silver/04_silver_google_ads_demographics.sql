DROP TABLE IF EXISTS silver.google_ads_demographics;
CREATE TABLE silver.google_ads_demographics AS
WITH deduplicated AS (
    SELECT
        customer_id,
        customer_name,
        campaign_id,
        campaign_name,
        ad_group_id,
        ad_group_name,
        dimension_type,
        dimension_value,
        impressions,
        clicks,
        spend,
        conversions,
        date,
        _extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, campaign_id, ad_group_id, dimension_type, dimension_value, CAST(date AS VARCHAR)
            ORDER BY _extracted_at DESC
        ) AS rn
    FROM bronze.google_ads_demographics_raw
)
SELECT
    CAST(customer_id AS VARCHAR) AS account_id,
    CAST(customer_name AS VARCHAR) AS account_name,
    CAST(campaign_id AS VARCHAR) AS campaign_id,
    CAST(campaign_name AS VARCHAR) AS campaign_name,
    CAST(ad_group_id AS VARCHAR) AS ad_group_id,
    CAST(ad_group_name AS VARCHAR) AS ad_group_name,
    CAST(dimension_type AS VARCHAR) AS dimension_type,
    CAST(dimension_value AS VARCHAR) AS dimension_value,
    SUM(COALESCE(impressions, 0))::BIGINT AS impressions,
    SUM(COALESCE(clicks, 0))::BIGINT AS clicks,
    SUM(COALESCE(spend, 0.0))::DOUBLE PRECISION AS spend,
    SUM(COALESCE(conversions, 0.0))::DOUBLE PRECISION AS conversions,
    CAST(date AS DATE) AS date,
    MAX(_extracted_at) AS _extracted_at
FROM deduplicated
WHERE rn = 1
GROUP BY customer_id, customer_name, campaign_id, campaign_name, ad_group_id, ad_group_name, dimension_type, dimension_value, date;
