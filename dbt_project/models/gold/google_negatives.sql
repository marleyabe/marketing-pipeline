{{ config(schema='gold', materialized='table') }}

-- Gold = snapshot mais recente por (account, scope, criterion). Basta usar a
-- linha com snapshot_date MAX do silver. Mantém campaign_id/ad_group_id para
-- permitir cobertura por escopo no endpoint /review.
WITH latest_snapshot AS (
    SELECT MAX(snapshot_date) AS snapshot_date
    FROM {{ ref('google_negatives_dedup') }}
)
SELECT
    n.account_id,
    n.campaign_id,
    n.campaign_name,
    n.ad_group_id,
    n.ad_group_name,
    n.criterion_id,
    n.criterion_text,
    n.match_type,
    n.scope,
    n.snapshot_date
FROM {{ ref('google_negatives_dedup') }} n
JOIN latest_snapshot l ON n.snapshot_date = l.snapshot_date
