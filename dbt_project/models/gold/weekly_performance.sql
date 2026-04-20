{{ config(schema='gold', materialized='table') }}

SELECT
    account_id,
    account_name,
    platform,
    CAST(DATE_TRUNC('week', date) AS DATE) AS week_start,
    MIN(date) AS period_start,
    MAX(date) AS period_end,
    CAST(SUM(impressions) AS BIGINT) AS impressions,
    CAST(SUM(clicks) AS BIGINT) AS clicks,
    CAST(SUM(spend) AS DOUBLE PRECISION) AS spend,
    CAST(SUM(conversions) AS DOUBLE PRECISION) AS conversions,
    ROUND((SUM(clicks) * 100.0 / NULLIF(SUM(impressions), 0))::numeric, 2) AS ctr,
    ROUND((SUM(spend) / NULLIF(SUM(clicks), 0))::numeric, 2) AS cpc,
    ROUND((SUM(spend) / NULLIF(SUM(conversions), 0))::numeric, 2) AS cost_per_conversion,
    ROUND((SUM(conversions) * 100.0 / NULLIF(SUM(clicks), 0))::numeric, 2) AS conversion_rate
FROM {{ ref('unified_campaigns') }}
GROUP BY account_id, account_name, platform, DATE_TRUNC('week', date)
HAVING SUM(spend) > 0
