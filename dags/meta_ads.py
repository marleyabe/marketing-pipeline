"""Meta Ads — extractor + DAG. 1 DAG fundindo daily + backfill via params."""

import json
import logging
import os
from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models.param import Param
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi
from facebook_business.exceptions import FacebookRequestError

from dags._date_range import resolve_target_dates
from dags.callbacks.discord_alert import notify_discord_failure
from src.db import get_pg, init_schemas
from src.loader import load_bronze

logger = logging.getLogger(__name__)

PLATFORM = "meta_ads"
BRONZE_TABLE = "meta_ads_raw"
BRONZE_CONFLICT_KEYS = ["date_start", "account_id", "ad_id", "device_platform", "publisher_platform"]
bronze_meta_dataset = Dataset("ads2u/bronze/meta_ads")

INSIGHT_FIELDS = [
    AdsInsights.Field.account_id,
    AdsInsights.Field.account_name,
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.ad_id,
    AdsInsights.Field.ad_name,
    AdsInsights.Field.impressions,
    AdsInsights.Field.clicks,
    AdsInsights.Field.spend,
    AdsInsights.Field.date_start,
    AdsInsights.Field.date_stop,
    AdsInsights.Field.actions,
]


def _credentials() -> dict:
    return {
        "app_id": os.environ["META_APP_ID"],
        "app_secret": os.environ["META_APP_SECRET"],
        "access_token": os.environ["META_ACCESS_TOKEN"],
    }


def _init_api() -> None:
    FacebookAdsApi.init(**_credentials())


def _list_accounts() -> list[str]:
    connection = get_pg()
    init_schemas(connection)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT account_id FROM ops.managed_accounts "
                "WHERE platform = %s AND enabled = 1",
                [PLATFORM],
            )
            rows = cursor.fetchall()
    finally:
        connection.close()
    return [row[0] for row in rows]


def _extract(account_ids: list[str], target_date: str) -> list[dict]:
    results: list[dict] = []
    for account_id in account_ids:
        full_account_id = (
            account_id if account_id.startswith("act_") else f"act_{account_id}"
        )
        try:
            insights = AdAccount(full_account_id).get_insights(
                fields=INSIGHT_FIELDS,
                params={
                    "level": "ad",
                    "time_range": {"since": target_date, "until": target_date},
                    "breakdowns": ["device_platform", "publisher_platform"],
                },
            )
            account_rows = 0
            for insight in insights:
                data = insight.export_all_data()
                actions = data.get("actions", [])
                if isinstance(actions, list):
                    actions = json.dumps(actions)
                results.append({
                    "account_id": data.get("account_id", ""),
                    "account_name": data.get("account_name", ""),
                    "campaign_id": data.get("campaign_id", ""),
                    "campaign_name": data.get("campaign_name", ""),
                    "ad_id": data.get("ad_id", ""),
                    "ad_name": data.get("ad_name", ""),
                    "impressions": int(data.get("impressions", 0)),
                    "clicks": int(data.get("clicks", 0)),
                    "spend": float(data.get("spend", 0.0)),
                    "date_start": data.get("date_start", ""),
                    "date_stop": data.get("date_stop", ""),
                    "actions": actions,
                    "device_platform": data.get("device_platform", ""),
                    "publisher_platform": data.get("publisher_platform", ""),
                })
                account_rows += 1
            logger.info(
                "meta_ads extract account=%s date=%s rows=%d",
                account_id, target_date, account_rows,
            )
        except FacebookRequestError as error:
            logger.error(
                "meta_ads API falhou account=%s date=%s: %s",
                account_id, target_date, error,
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
    dag_id="meta_ads",
    schedule="0 5 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["extract", "meta_ads"],
    default_args=default_args,
    params={
        "start_date": Param(default=None, type=["null", "string"], format="date"),
        "end_date": Param(default=None, type=["null", "string"], format="date"),
    },
)
def meta_ads():
    @task(outlets=[bronze_meta_dataset])
    def extract(**context) -> int:
        target_dates = resolve_target_dates(
            context["params"].get("start_date"),
            context["params"].get("end_date"),
        )
        logger.info("meta_ads target_dates=%s", [d.isoformat() for d in target_dates])
        _init_api()
        account_ids = _list_accounts()
        logger.info("meta_ads accounts=%d", len(account_ids))
        rows: list[dict] = []
        for target_date in target_dates:
            rows.extend(_extract(account_ids, target_date.isoformat()))
        loaded = _load(rows)
        logger.info("meta_ads loaded rows=%d", loaded)
        return loaded

    extract()


meta_ads()
