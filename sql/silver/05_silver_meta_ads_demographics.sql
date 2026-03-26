CREATE OR REPLACE TABLE silver.meta_ads_demographics AS
WITH deduplicated AS (
    SELECT
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        ad_id,
        ad_name,
        age,
        gender,
        impressions,
        clicks,
        spend,
        actions,
        date_start,
        _extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY account_id, ad_id, age, gender, CAST(date_start AS VARCHAR)
            ORDER BY _extracted_at DESC
        ) AS rn
    FROM bronze.meta_ads_demographics_raw
)
SELECT
    CAST(account_id AS VARCHAR) AS account_id,
    CAST(account_name AS VARCHAR) AS account_name,
    CAST(campaign_id AS VARCHAR) AS campaign_id,
    CAST(campaign_name AS VARCHAR) AS campaign_name,
    CAST(ad_id AS VARCHAR) AS ad_id,
    CAST(ad_name AS VARCHAR) AS ad_name,
    CAST(age AS VARCHAR) AS age,
    CAST(gender AS VARCHAR) AS gender,
    SUM(COALESCE(impressions, 0))::BIGINT AS impressions,
    SUM(COALESCE(clicks, 0))::BIGINT AS clicks,
    SUM(COALESCE(spend, 0.0))::DOUBLE AS spend,
    COALESCE(
        CASE
            WHEN actions IS NOT NULL AND actions != '' AND actions != '[]'
            THEN list_sum(
                list_transform(
                    list_filter(
                        from_json(actions, '["json"]'),
                        x -> json_extract_string(x, '$.action_type') NOT IN (
                            'link_click', 'landing_page_view',
                            'page_engagement', 'post_engagement'
                        )
                    ),
                    x -> CAST(json_extract_string(x, '$.value') AS DOUBLE)
                )
            )
            ELSE 0.0
        END,
        0.0
    ) AS conversions,
    CAST(date_start AS DATE) AS date,
    MAX(_extracted_at) AS _extracted_at
FROM deduplicated
WHERE rn = 1
GROUP BY account_id, account_name, campaign_id, campaign_name, ad_id, ad_name, age, gender, date_start, actions;
