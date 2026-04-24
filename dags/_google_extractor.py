"""Extração Google Ads sem dependência de Airflow.

Isola cliente/mappers/fetchers para permitir chamada direta em scripts, testes
e manual runs, sem arrastar Airflow pra dentro do import.
"""

import logging
import os

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

from dags._google_queries import (
    KEYWORD_PERFORMANCE_QUERY,
    NEGATIVE_AD_GROUP_QUERY,
    NEGATIVE_CAMPAIGN_QUERY,
    SEARCH_TERMS_QUERY,
)
from dags._retry import call_with_retry

logger = logging.getLogger(__name__)


def credentials() -> dict:
    return {
        "developer_token": os.environ["GOOGLE_DEVELOPER_TOKEN"],
        "client_id": os.environ["GOOGLE_CLIENT_ID"],
        "client_secret": os.environ["GOOGLE_CLIENT_SECRET"],
        "refresh_token": os.environ["GOOGLE_REFRESH_TOKEN"],
        "login_customer_id": os.environ["GOOGLE_LOGIN_CUSTOMER_ID"],
    }


def build_client() -> GoogleAdsClient:
    return GoogleAdsClient.load_from_dict({**credentials(), "use_proto_plus": True})


def list_accounts(client: GoogleAdsClient) -> list[str]:
    manager_id = os.environ["GOOGLE_LOGIN_CUSTOMER_ID"].replace("-", "")
    query = """
        SELECT customer_client.id
        FROM customer_client
        WHERE customer_client.status = 'ENABLED'
          AND customer_client.manager = FALSE
          AND customer_client.hidden = FALSE
    """
    rows = _stream(client, manager_id, query, "list_accounts")
    return [str(row.customer_client.id) for row in rows]


def _stream_once(
    client: GoogleAdsClient, customer_id: str, query: str,
) -> list:
    # search_stream: conexão persistente, sem paginação. Materializamos a lista
    # dentro do escopo do retry para que uma falha a meio do stream re-execute
    # tudo de forma limpa.
    service = client.get_service("GoogleAdsService")
    rows: list = []
    for batch in service.search_stream(customer_id=customer_id, query=query):
        rows.extend(batch.results)
    return rows


def _stream(
    client: GoogleAdsClient, customer_id: str, query: str, label: str,
) -> list:
    try:
        return call_with_retry(
            lambda: _stream_once(client, customer_id, query),
            label=f"{label} account={customer_id}",
        )
    except GoogleAdsException as error:
        logger.error("google_ads %s falhou account=%s: %s", label, customer_id, error)
        raise


def _keyword_identifiers(row) -> dict:
    return {
        "customer_id": str(row.customer.id),
        "customer_name": row.customer.descriptive_name,
        "campaign_id": str(row.campaign.id),
        "campaign_name": row.campaign.name,
        "campaign_status": row.campaign.status.name,
        "ad_group_id": str(row.ad_group.id),
        "ad_group_name": row.ad_group.name,
        "ad_group_status": row.ad_group.status.name,
        "keyword_id": str(row.ad_group_criterion.criterion_id),
        "criterion_status": row.ad_group_criterion.status.name,
        "keyword_text": row.ad_group_criterion.keyword.text,
        "match_type": row.ad_group_criterion.keyword.match_type.name,
    }


def _keyword_metrics(row) -> dict:
    return {
        "impressions": row.metrics.impressions,
        "clicks": row.metrics.clicks,
        "spend": row.metrics.cost_micros / 1_000_000,
        "conversions": row.metrics.conversions,
        "conversion_value": row.metrics.conversions_value,
        "view_through_conversions": row.metrics.view_through_conversions,
        "all_conversions": row.metrics.all_conversions,
        "search_impression_share": row.metrics.search_impression_share,
        "quality_score": row.ad_group_criterion.quality_info.quality_score,
    }


def _map_keyword_row(row) -> dict:
    return {
        **_keyword_identifiers(row),
        **_keyword_metrics(row),
        "device": row.segments.device.name,
        "date": row.segments.date,
    }


def _fetch_keywords(client: GoogleAdsClient, customer_id: str, target_date: str) -> list[dict]:
    query = KEYWORD_PERFORMANCE_QUERY.format(date=target_date)
    rows = _stream(client, customer_id, query, "keyword_view")
    mapped = [_map_keyword_row(row) for row in rows]
    logger.info("google_ads keywords account=%s date=%s rows=%d", customer_id, target_date, len(mapped))
    return mapped


def extract_keywords(client: GoogleAdsClient, account_ids: list[str], target_date: str) -> list[dict]:
    results: list[dict] = []
    for customer_id in account_ids:
        results.extend(_fetch_keywords(client, customer_id, target_date))
    return results


def _search_term_identifiers(row) -> dict:
    # matched_keyword_* podem vir vazios — normalizamos para '' (não NULL) para
    # que a chave natural composta funcione em ON CONFLICT.
    return {
        "customer_id": str(row.customer.id),
        "customer_name": row.customer.descriptive_name,
        "campaign_id": str(row.campaign.id),
        "campaign_name": row.campaign.name,
        "ad_group_id": str(row.ad_group.id),
        "ad_group_name": row.ad_group.name,
        "search_term": row.search_term_view.search_term,
        "search_term_status": row.search_term_view.status.name,
        "matched_keyword_text": row.segments.keyword.info.text or "",
        "matched_keyword_match_type": row.segments.keyword.info.match_type.name or "",
    }


def _map_search_term_row(row) -> dict:
    return {
        **_search_term_identifiers(row),
        "impressions": row.metrics.impressions,
        "clicks": row.metrics.clicks,
        "spend": row.metrics.cost_micros / 1_000_000,
        "conversions": row.metrics.conversions,
        "conversion_value": row.metrics.conversions_value,
        "date": row.segments.date,
    }


def _fetch_search_terms(client: GoogleAdsClient, customer_id: str, target_date: str) -> list[dict]:
    query = SEARCH_TERMS_QUERY.format(date=target_date)
    rows = _stream(client, customer_id, query, "search_term_view")
    mapped = [_map_search_term_row(row) for row in rows]
    logger.info("google_ads search_terms account=%s date=%s rows=%d", customer_id, target_date, len(mapped))
    return mapped


def extract_search_terms(client: GoogleAdsClient, account_ids: list[str], target_date: str) -> list[dict]:
    results: list[dict] = []
    for customer_id in account_ids:
        results.extend(_fetch_search_terms(client, customer_id, target_date))
    return results


def _map_campaign_negative(row, snapshot_date: str) -> dict:
    # ad_group_id/name = '' (não NULL) para permitir ON CONFLICT casar a chave
    # natural em runs repetidos — ver bronze_google_negatives_raw_natural_key.
    return {
        "customer_id": str(row.customer.id),
        "campaign_id": str(row.campaign.id),
        "campaign_name": row.campaign.name,
        "ad_group_id": "",
        "ad_group_name": "",
        "criterion_id": str(row.campaign_criterion.criterion_id),
        "criterion_text": row.campaign_criterion.keyword.text,
        "match_type": row.campaign_criterion.keyword.match_type.name,
        "scope": "campaign",
        "snapshot_date": snapshot_date,
    }


def _map_ad_group_negative(row, snapshot_date: str) -> dict:
    return {
        "customer_id": str(row.customer.id),
        "campaign_id": str(row.campaign.id),
        "campaign_name": row.campaign.name,
        "ad_group_id": str(row.ad_group.id),
        "ad_group_name": row.ad_group.name,
        "criterion_id": str(row.ad_group_criterion.criterion_id),
        "criterion_text": row.ad_group_criterion.keyword.text,
        "match_type": row.ad_group_criterion.keyword.match_type.name,
        "scope": "ad_group",
        "snapshot_date": snapshot_date,
    }


def _fetch_negatives(client: GoogleAdsClient, customer_id: str, snapshot_date: str) -> list[dict]:
    campaign_rows = _stream(client, customer_id, NEGATIVE_CAMPAIGN_QUERY, "campaign_negatives")
    ad_group_rows = _stream(client, customer_id, NEGATIVE_AD_GROUP_QUERY, "ad_group_negatives")
    mapped = [_map_campaign_negative(r, snapshot_date) for r in campaign_rows]
    mapped += [_map_ad_group_negative(r, snapshot_date) for r in ad_group_rows]
    logger.info("google_ads negatives account=%s rows=%d", customer_id, len(mapped))
    return mapped


def extract_negatives(client: GoogleAdsClient, account_ids: list[str], snapshot_date: str) -> list[dict]:
    results: list[dict] = []
    for customer_id in account_ids:
        results.extend(_fetch_negatives(client, customer_id, snapshot_date))
    return results
