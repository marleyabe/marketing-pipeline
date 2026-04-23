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
    CAST(SUM(conversions) AS DOUBLE PRECISION) AS conversions
FROM {{ ref('google_ads_keywords_dedup') }}
GROUP BY account_id, account_name, campaign_name, ad_group_name, keyword_text, match_type, date
HAVING SUM(spend) > 0
