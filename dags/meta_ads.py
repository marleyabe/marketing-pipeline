"""Meta Ads — extractor + DAG. 1 DAG fundindo daily + backfill via params."""

import json
import logging
import os
from datetime import datetime
from typing import Any

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.user import User
from facebook_business.api import FacebookAdsApi
from facebook_business.exceptions import FacebookRequestError

from dags.shared.extractor import (
    ExtractorSpec,
    date_range_params,
    default_args,
    run_extraction,
)

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


def _list_accounts(_: Any) -> list[str]:
    accounts = User(fbid="me").get_ad_accounts(fields=["account_id", "account_status"])
    return [a["account_id"] for a in accounts if a.get("account_status") == 1]


def _normalize_account_id(account_id: str) -> str:
    return account_id if account_id.startswith("act_") else f"act_{account_id}"


def _map_insight(data: dict) -> dict:
    actions = data.get("actions", [])
    if isinstance(actions, list):
        actions = json.dumps(actions)
    return {
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
    }


def _fetch_account(account_id: str, target_date: str) -> list[dict]:
    full_account_id = _normalize_account_id(account_id)
    try:
        insights = AdAccount(full_account_id).get_insights(
            fields=INSIGHT_FIELDS,
            params={
                "level": "ad",
                "time_range": {"since": target_date, "until": target_date},
                "breakdowns": ["device_platform", "publisher_platform"],
            },
        )
    except FacebookRequestError as error:
        logger.error("meta_ads API falhou account=%s date=%s: %s", account_id, target_date, error)
        raise
    mapped = [_map_insight(insight.export_all_data()) for insight in insights]
    logger.info("meta_ads extract account=%s date=%s rows=%d", account_id, target_date, len(mapped))
    return mapped


def _extract(_: Any, account_ids: list[str], target_date: str) -> list[dict]:
    results: list[dict] = []
    for account_id in account_ids:
        results.extend(_fetch_account(account_id, target_date))
    return results


def _init_and_return_none() -> None:
    _init_api()
    return None


SPEC = ExtractorSpec(
    platform=PLATFORM,
    bronze_table=BRONZE_TABLE,
    conflict_keys=BRONZE_CONFLICT_KEYS,
    init_api=_init_and_return_none,
    list_accounts=_list_accounts,
    fetch_date=_extract,
)


@dag(
    dag_id="meta_ads",
    schedule="0 5 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["extract", "meta_ads"],
    default_args=default_args(),
    params=date_range_params(),
)
def meta_ads() -> None:
    @task(outlets=[bronze_meta_dataset])
    def extract(**context) -> int:
        return run_extraction(SPEC, context)

    extract()


meta_ads()
