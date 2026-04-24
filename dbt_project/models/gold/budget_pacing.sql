{{ config(schema='gold', materialized='view') }}

-- Pacing = spend mês-corrente vs monthly_budget cadastrado em ops.client_budget.
-- Baseia-se em dias corridos (não úteis) porque Google/Meta gastam incluindo
-- fins de semana. pace_flag:
--   over      = pct_consumed > pct do mês decorrido + 10pp
--   under     = pct_consumed < pct do mês decorrido - 10pp
--   on_track  = dentro da faixa
WITH current_month AS (
    SELECT
        DATE_TRUNC('month', CURRENT_DATE)::DATE AS month_start,
        (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month - 1 day')::DATE AS month_end,
        EXTRACT(DAY FROM (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month - 1 day'))::INT AS days_in_month,
        EXTRACT(DAY FROM CURRENT_DATE)::INT AS day_of_month
),
spend_mtd AS (
    SELECT
        p.account_id,
        p.platform,
        SUM(p.spend) AS spent_mtd
    FROM {{ ref('daily_performance') }} p
    CROSS JOIN current_month cm
    WHERE p.date BETWEEN cm.month_start AND cm.month_end
    GROUP BY p.account_id, p.platform
),
joined AS (
    SELECT
        b.account_id,
        b.platform,
        b.monthly_budget::DOUBLE PRECISION AS monthly_budget,
        b.currency,
        COALESCE(s.spent_mtd, 0.0) AS spent_mtd,
        cm.day_of_month,
        cm.days_in_month
    FROM {{ source('ops', 'client_budget') }} b
    LEFT JOIN spend_mtd s
        ON s.account_id = b.account_id AND s.platform = b.platform
    CROSS JOIN current_month cm
    WHERE b.active = TRUE
)
SELECT
    account_id,
    platform,
    monthly_budget,
    currency,
    ROUND(spent_mtd::numeric, 2) AS spent_mtd,
    ROUND((spent_mtd * 100.0 / NULLIF(monthly_budget, 0))::numeric, 2) AS pct_consumed,
    ROUND((day_of_month * 100.0 / NULLIF(days_in_month, 0))::numeric, 2) AS days_elapsed_pct,
    CASE
        WHEN monthly_budget = 0 THEN 'unknown'
        WHEN (spent_mtd * 100.0 / monthly_budget) > (day_of_month * 100.0 / days_in_month) + 10 THEN 'over'
        WHEN (spent_mtd * 100.0 / monthly_budget) < (day_of_month * 100.0 / days_in_month) - 10 THEN 'under'
        ELSE 'on_track'
    END AS pace_flag
FROM joined
