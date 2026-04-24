{{ config(schema='gold', materialized='table') }}

SELECT
    account_id,
    account_name,
    campaign_name,
    ad_group_name,
    keyword_text,
    match_type,
    date,
    CAST(SUM(impressions) AS BIGINT) AS impressions,
    CAST(SUM(clicks) AS BIGINT) AS clicks,
    CAST(SUM(spend) AS DOUBLE PRECISION) AS spend,
    CAST(SUM(conversions) AS DOUBLE PRECISION) AS conversions,
    CAST(SUM(COALESCE(conversion_value, 0.0)) AS DOUBLE PRECISION) AS conversion_value,
    ROUND((SUM(COALESCE(conversion_value, 0.0)) / NULLIF(SUM(spend), 0))::numeric, 4) AS roas,
    CAST(AVG(quality_score) AS DOUBLE PRECISION) AS quality_score,
    MAX(campaign_status) AS campaign_status,
    MAX(ad_group_status) AS ad_group_status,
    MAX(criterion_status) AS criterion_status
FROM {{ ref('google_ads_keywords_dedup') }}
GROUP BY account_id, account_name, campaign_name, ad_group_name, keyword_text, match_type, date
HAVING SUM(spend) > 0
