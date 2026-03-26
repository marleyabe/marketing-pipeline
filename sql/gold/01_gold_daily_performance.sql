DROP TABLE IF EXISTS gold.daily_performance;
CREATE TABLE gold.daily_performance AS
SELECT
    account_id,
    account_name,
    date,
    platform,
    SUM(impressions)::BIGINT AS impressions,
    SUM(clicks)::BIGINT AS clicks,
    SUM(spend)::DOUBLE PRECISION AS spend,
    SUM(conversions)::DOUBLE PRECISION AS conversions,
    CASE WHEN SUM(impressions) > 0
        THEN ROUND((SUM(clicks) * 100.0 / SUM(impressions))::NUMERIC, 2)
        ELSE 0.0
    END AS ctr,
    CASE WHEN SUM(clicks) > 0
        THEN ROUND((SUM(spend) / SUM(clicks))::NUMERIC, 2)
        ELSE 0.0
    END AS cpc,
    CASE WHEN SUM(conversions) > 0
        THEN ROUND((SUM(spend) / SUM(conversions))::NUMERIC, 2)
        ELSE 0.0
    END AS cost_per_conversion,
    CASE WHEN SUM(clicks) > 0
        THEN ROUND((SUM(conversions) * 100.0 / SUM(clicks))::NUMERIC, 2)
        ELSE 0.0
    END AS conversion_rate
FROM silver.unified_campaigns
GROUP BY account_id, account_name, date, platform;
