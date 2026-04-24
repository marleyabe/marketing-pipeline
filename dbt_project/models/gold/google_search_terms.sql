{{ config(schema='gold', materialized='table') }}

-- Search terms com spend > 0. RN14 equivalente: só exponho o que tem custo.
-- Inclui roas para permitir buckets top_by_spend_no_conv e high_roas na review.
SELECT
    account_id,
    account_name,
    campaign_name,
    ad_group_name,
    search_term,
    search_term_status,
    matched_keyword_text,
    matched_keyword_match_type,
    date,
    CAST(SUM(impressions) AS BIGINT) AS impressions,
    CAST(SUM(clicks) AS BIGINT) AS clicks,
    CAST(SUM(spend) AS DOUBLE PRECISION) AS spend,
    CAST(SUM(conversions) AS DOUBLE PRECISION) AS conversions,
    CAST(SUM(COALESCE(conversion_value, 0.0)) AS DOUBLE PRECISION) AS conversion_value,
    ROUND((SUM(COALESCE(conversion_value, 0.0)) / NULLIF(SUM(spend), 0))::numeric, 4) AS roas
FROM {{ ref('google_search_terms_dedup') }}
GROUP BY account_id, account_name, campaign_name, ad_group_name,
         search_term, search_term_status, matched_keyword_text,
         matched_keyword_match_type, date
HAVING SUM(spend) > 0
