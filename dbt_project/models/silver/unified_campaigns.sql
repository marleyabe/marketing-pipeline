{{ config(schema='silver', materialized='table') }}

WITH meta AS (
    SELECT
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        date,
        platform,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend,
        SUM(conversions) AS conversions
    FROM {{ ref('meta_ads') }}
    GROUP BY account_id, account_name, campaign_id, campaign_name, date, platform
),
google AS (
    SELECT
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        date,
        platform,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend,
        SUM(conversions) AS conversions
    FROM {{ ref('google_ads') }}
    GROUP BY account_id, account_name, campaign_id, campaign_name, date, platform
)
SELECT * FROM meta
UNION ALL
SELECT * FROM google
