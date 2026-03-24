CREATE OR REPLACE TABLE gold.weekly_performance AS
SELECT
    account_id,
    account_name,
    platform,
    DATE_TRUNC('week', date)::DATE AS week_start,
    MIN(date) AS period_start,
    MAX(date) AS period_end,
    SUM(impressions)::BIGINT AS impressions,
    SUM(clicks)::BIGINT AS clicks,
    SUM(spend)::DOUBLE AS spend,
    SUM(conversions)::DOUBLE AS conversions,
    CASE WHEN SUM(impressions) > 0
        THEN ROUND(SUM(clicks) * 100.0 / SUM(impressions), 2)
        ELSE 0.0
    END AS ctr,
    CASE WHEN SUM(clicks) > 0
        THEN ROUND(SUM(spend) / SUM(clicks), 2)
        ELSE 0.0
    END AS cpc,
    CASE WHEN SUM(conversions) > 0
        THEN ROUND(SUM(spend) / SUM(conversions), 2)
        ELSE 0.0
    END AS cost_per_conversion,
    CASE WHEN SUM(clicks) > 0
        THEN ROUND(SUM(conversions) * 100.0 / SUM(clicks), 2)
        ELSE 0.0
    END AS conversion_rate
FROM silver.unified_campaigns
GROUP BY account_id, account_name, platform, DATE_TRUNC('week', date);
