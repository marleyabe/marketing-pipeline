"""Google Ads — extractor + DAG. 1 DAG fundindo daily + backfill via params."""

import logging
import os
from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models.param import Param
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

from dags._date_range import resolve_target_dates
from dags.callbacks.discord_alert import notify_discord_failure
from src.db import get_pg, init_schemas
from src.loader import load_bronze

logger = logging.getLogger(__name__)

PLATFORM = "google_ads"
BRONZE_TABLE = "google_ads_raw"
BRONZE_CONFLICT_KEYS = ["date", "customer_id", "campaign_id", "ad_group_id", "keyword_id", "device"]
bronze_google_dataset = Dataset("ads2u/bronze/google_ads")


KEYWORD_PERFORMANCE_QUERY = """
    SELECT
        customer.id,
        customer.descriptive_name,
        campaign.id,
        campaign.name,
        ad_group.id,
        ad_group.name,
        ad_group_criterion.criterion_id,
        ad_group_criterion.keyword.text,
        ad_group_criterion.keyword.match_type,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.conversions,
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


def _credentials() -> dict:
    return {
        "developer_token": os.environ["GOOGLE_DEVELOPER_TOKEN"],
        "client_id": os.environ["GOOGLE_CLIENT_ID"],
        "client_secret": os.environ["GOOGLE_CLIENT_SECRET"],
        "refresh_token": os.environ["GOOGLE_REFRESH_TOKEN"],
        "login_customer_id": os.environ["GOOGLE_LOGIN_CUSTOMER_ID"],
    }


def _get_client() -> GoogleAdsClient:
    return GoogleAdsClient.load_from_dict({**_credentials(), "use_proto_plus": True})


def _list_accounts(client: GoogleAdsClient) -> list[str]:
    manager_id = os.environ["GOOGLE_LOGIN_CUSTOMER_ID"].replace("-", "")
    service = client.get_service("GoogleAdsService")
    query = """
        SELECT customer_client.id
        FROM customer_client
        WHERE customer_client.status = 'ENABLED'
          AND customer_client.manager = FALSE
          AND customer_client.hidden = FALSE
    """
    rows = service.search(customer_id=manager_id, query=query)
    return [str(row.customer_client.id) for row in rows]


def _extract(client: GoogleAdsClient, account_ids: list[str], target_date: str) -> list[dict]:
    service = client.get_service("GoogleAdsService")
    results: list[dict] = []
    for customer_id in account_ids:
        try:
            rows = service.search(
                customer_id=customer_id,
                query=KEYWORD_PERFORMANCE_QUERY.format(date=target_date),
            )
            account_rows = 0
            for row in rows:
                results.append({
                    "customer_id": str(row.customer.id),
                    "customer_name": row.customer.descriptive_name,
                    "campaign_id": str(row.campaign.id),
                    "campaign_name": row.campaign.name,
                    "ad_group_id": str(row.ad_group.id),
                    "ad_group_name": row.ad_group.name,
                    "keyword_id": str(row.ad_group_criterion.criterion_id),
                    "keyword_text": row.ad_group_criterion.keyword.text,
                    "match_type": row.ad_group_criterion.keyword.match_type.name,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "spend": row.metrics.cost_micros / 1_000_000,
                    "conversions": row.metrics.conversions,
                    "view_through_conversions": row.metrics.view_through_conversions,
                    "all_conversions": row.metrics.all_conversions,
                    "search_impression_share": row.metrics.search_impression_share,
                    "quality_score": row.ad_group_criterion.quality_info.quality_score,
                    "device": row.segments.device.name,
                    "date": row.segments.date,
                })
                account_rows += 1
            logger.info(
                "google_ads extract account=%s date=%s rows=%d",
                customer_id, target_date, account_rows,
            )
        except GoogleAdsException as error:
            logger.error(
                "google_ads API falhou account=%s date=%s: %s",
                customer_id, target_date, error,
            )
            raise
    return results


def _load(rows: list[dict]) -> int:
    connection = get_pg()
    init_schemas(connection)
    try:
        return load_bronze(
            connection,
            rows,
            BRONZE_TABLE,
            source=PLATFORM,
            conflict_columns=BRONZE_CONFLICT_KEYS,
        )
    finally:
        connection.close()


default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": notify_discord_failure,
}


@dag(
    dag_id="google_ads",
    schedule="0 5 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["extract", "google_ads"],
    default_args=default_args,
    params={
        "start_date": Param(default=None, type=["null", "string"], format="date"),
        "end_date": Param(default=None, type=["null", "string"], format="date"),
    },
)
def google_ads():
    @task(outlets=[bronze_google_dataset])
    def extract(**context) -> int:
        target_dates = resolve_target_dates(
            context["params"].get("start_date"),
            context["params"].get("end_date"),
        )
        logger.info("google_ads target_dates=%s", [d.isoformat() for d in target_dates])
        client = _get_client()
        account_ids = _list_accounts(client)
        logger.info("google_ads accounts=%d", len(account_ids))
        rows: list[dict] = []
        for target_date in target_dates:
            rows.extend(_extract(client, account_ids, target_date.isoformat()))
        loaded = _load(rows)
        logger.info("google_ads loaded rows=%d", loaded)
        return loaded

    extract()


google_ads()
