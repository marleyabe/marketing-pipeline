{{ config(schema='silver', materialized='table') }}

WITH deduplicated AS (
    SELECT
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        ad_id,
        ad_name,
        impressions,
        clicks,
        spend,
        date_start,
        actions,
        device_platform,
        publisher_platform,
        _extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY account_id, ad_id, device_platform, publisher_platform, CAST(date_start AS VARCHAR)
            ORDER BY _extracted_at DESC
        ) AS rn
    FROM {{ source('bronze', 'meta_ads_raw') }}
),
filtered AS (
    SELECT * FROM deduplicated WHERE rn = 1
)
SELECT
    CAST(account_id AS VARCHAR) AS account_id,
    CAST(account_name AS VARCHAR) AS account_name,
    CAST(campaign_id AS VARCHAR) AS campaign_id,
    CAST(campaign_name AS VARCHAR) AS campaign_name,
    ad_id,
    ad_name,
    CAST(COALESCE(impressions, 0) AS BIGINT) AS impressions,
    CAST(COALESCE(clicks, 0) AS BIGINT) AS clicks,
    CAST(COALESCE(spend, 0.0) AS DOUBLE PRECISION) AS spend,
    CAST(date_start AS DATE) AS date,
    CAST(device_platform AS VARCHAR) AS device_platform,
    CAST(publisher_platform AS VARCHAR) AS publisher_platform,
    COALESCE((
        SELECT SUM((elem->>'value')::DOUBLE PRECISION)
        FROM jsonb_array_elements(
            CASE WHEN actions IS NULL OR actions = '' THEN '[]'::jsonb ELSE actions::jsonb END
        ) AS elem
        WHERE elem->>'action_type' NOT IN ('link_click','landing_page_view','page_engagement','post_engagement')
    ), 0.0) AS conversions,
    CAST(actions AS VARCHAR) AS actions_raw,
    _extracted_at,
    'meta_ads' AS platform
FROM filtered
