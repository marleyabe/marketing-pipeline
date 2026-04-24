{{ config(schema='silver', materialized='table') }}

-- Negativas snapshot diário. Dedupe pela chave natural
-- (snapshot_date, customer_id, scope, criterion_id) pega o registro mais recente.
WITH deduplicated AS (
    SELECT
        customer_id,
        campaign_id,
        campaign_name,
        ad_group_id,
        ad_group_name,
        criterion_id,
        criterion_text,
        match_type,
        scope,
        snapshot_date,
        _extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY snapshot_date, customer_id, scope, criterion_id,
                         COALESCE(ad_group_id, ''), COALESCE(campaign_id, '')
            ORDER BY _extracted_at DESC
        ) AS rn
    FROM {{ source('bronze', 'google_negatives_raw') }}
    WHERE criterion_text IS NOT NULL AND criterion_text <> ''
)
SELECT
    CAST(customer_id AS VARCHAR) AS account_id,
    CAST(campaign_id AS VARCHAR) AS campaign_id,
    CAST(campaign_name AS VARCHAR) AS campaign_name,
    CAST(ad_group_id AS VARCHAR) AS ad_group_id,
    CAST(ad_group_name AS VARCHAR) AS ad_group_name,
    CAST(criterion_id AS VARCHAR) AS criterion_id,
    CAST(criterion_text AS VARCHAR) AS criterion_text,
    CAST(match_type AS VARCHAR) AS match_type,
    CAST(scope AS VARCHAR) AS scope,
    CAST(snapshot_date AS DATE) AS snapshot_date,
    _extracted_at,
    'google_ads' AS platform
FROM deduplicated
WHERE rn = 1
