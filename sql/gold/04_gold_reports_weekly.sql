CREATE OR REPLACE TABLE gold.reports_weekly AS
SELECT
    account_id,
    account_name,
    platform,
    week_start,
    period_start,
    period_end,
    spend AS investimento,
    impressions AS impressoes,
    clicks AS cliques,
    conversions AS conversoes,
    cost_per_conversion AS custo_por_conversao,
    conversion_rate AS taxa_de_conversao
FROM gold.weekly_performance;
