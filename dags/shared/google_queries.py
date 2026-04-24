"""Queries GAQL isoladas do código da DAG para facilitar mock em testes."""


KEYWORD_PERFORMANCE_QUERY = """
    SELECT
        customer.id,
        customer.descriptive_name,
        campaign.id,
        campaign.name,
        campaign.status,
        ad_group.id,
        ad_group.name,
        ad_group.status,
        ad_group_criterion.criterion_id,
        ad_group_criterion.status,
        ad_group_criterion.keyword.text,
        ad_group_criterion.keyword.match_type,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.conversions,
        metrics.conversions_value,
        metrics.view_through_conversions,
        metrics.all_conversions,
        metrics.search_impression_share,
        ad_group_criterion.quality_info.quality_score,
        segments.device,
        segments.date
    FROM keyword_view
    WHERE segments.date = '{date}'
      AND ad_group_criterion.status != 'REMOVED'
"""


SEARCH_TERMS_QUERY = """
    SELECT
        customer.id,
        customer.descriptive_name,
        campaign.id,
        campaign.name,
        ad_group.id,
        ad_group.name,
        search_term_view.search_term,
        search_term_view.status,
        segments.keyword.info.text,
        segments.keyword.info.match_type,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.conversions,
        metrics.conversions_value,
        segments.date
    FROM search_term_view
    WHERE segments.date = '{date}'
"""


# Negativas são snapshot (não têm granularidade de data na API). Cada task carimba
# o snapshot_date passado ao extrair.
NEGATIVE_CAMPAIGN_QUERY = """
    SELECT
        customer.id,
        campaign.id,
        campaign.name,
        campaign_criterion.criterion_id,
        campaign_criterion.keyword.text,
        campaign_criterion.keyword.match_type
    FROM campaign_criterion
    WHERE campaign_criterion.type = 'KEYWORD'
      AND campaign_criterion.negative = TRUE
"""


NEGATIVE_AD_GROUP_QUERY = """
    SELECT
        customer.id,
        campaign.id,
        campaign.name,
        ad_group.id,
        ad_group.name,
        ad_group_criterion.criterion_id,
        ad_group_criterion.keyword.text,
        ad_group_criterion.keyword.match_type
    FROM ad_group_criterion
    WHERE ad_group_criterion.type = 'KEYWORD'
      AND ad_group_criterion.negative = TRUE
      AND ad_group_criterion.status != 'REMOVED'
"""
