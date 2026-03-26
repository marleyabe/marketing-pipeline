DROP TABLE IF EXISTS silver.unified_campaigns;
CREATE TABLE silver.unified_campaigns AS
SELECT
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    impressions,
    clicks,
    spend,
    date,
    conversions,
    platform
FROM silver.meta_ads

UNION ALL

SELECT
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    impressions,
    clicks,
    spend,
    date,
    conversions,
    platform
FROM silver.google_ads;
