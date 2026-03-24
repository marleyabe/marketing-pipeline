CREATE OR REPLACE TABLE gold.reports_daily AS
SELECT
    account_id,
    account_name,
    date,
    platform,
    spend AS investimento,
    impressions AS impressoes,
    clicks AS cliques,
    conversions AS conversoes,
    cost_per_conversion AS custo_por_conversao,
    conversion_rate AS taxa_de_conversao
FROM gold.daily_performance;
