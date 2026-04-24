{{ config(schema='silver', materialized='table') }}

-- conversion_value vem só do Google hoje. Meta agrega 0.0 para manter UNION alinhado;
-- quando houver purchase_value no silver.meta_ads, trocar aqui.
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
        SUM(conversions) AS conversions,
        CAST(0.0 AS DOUBLE PRECISION) AS conversion_value
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
        SUM(conversions) AS conversions,
        SUM(COALESCE(conversion_value, 0.0)) AS conversion_value
    FROM {{ ref('google_ads') }}
    GROUP BY account_id, account_name, campaign_id, campaign_name, date, platform
)
SELECT * FROM meta
UNION ALL
SELECT * FROM google
