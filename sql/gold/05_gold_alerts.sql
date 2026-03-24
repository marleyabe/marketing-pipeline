CREATE OR REPLACE TABLE gold.alerts_daily AS
WITH current_day AS (
    SELECT * FROM gold.daily_performance
),
previous_day AS (
    SELECT
        account_id,
        date + INTERVAL 1 DAY AS next_date,
        spend AS prev_spend,
        conversions AS prev_conversions
    FROM gold.daily_performance
)
SELECT
    c.account_id,
    c.account_name,
    c.date,
    c.platform,
    c.spend,
    p.prev_spend,
    c.conversions,
    p.prev_conversions,
    CASE WHEN p.prev_spend > 0
        THEN ROUND(((c.spend - p.prev_spend) / p.prev_spend) * 100, 2)
        ELSE 0.0
    END AS spend_change_pct,
    CASE WHEN p.prev_conversions > 0
        THEN ROUND(((c.conversions - p.prev_conversions) / p.prev_conversions) * 100, 2)
        ELSE 0.0
    END AS conversion_change_pct
FROM current_day c
LEFT JOIN previous_day p
    ON c.account_id = p.account_id
    AND c.date = CAST(p.next_date AS DATE);
