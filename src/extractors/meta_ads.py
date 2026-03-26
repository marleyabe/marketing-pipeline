import json

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.user import User
from facebook_business.api import FacebookAdsApi

from src.extractors.base import BaseExtractor

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


class MetaAdsExtractor(BaseExtractor):
    def __init__(self, credentials: dict):
        self._credentials = credentials

    def _init_api(self):
        FacebookAdsApi.init(
            app_id=self._credentials["app_id"],
            app_secret=self._credentials["app_secret"],
            access_token=self._credentials["access_token"],
        )

    def list_accounts(self) -> list[dict]:
        self._init_api()
        me = User(fbid="me")
        accounts = me.get_ad_accounts(
            fields=["account_id", "name", "account_status"],
        )
        return [
            {"account_id": acc["account_id"], "account_name": acc["name"]}
            for acc in accounts
            if acc["account_status"] == 1
        ]

    def extract(self, account_ids: list[str], date: str) -> list[dict]:
        self._init_api()
        results = []

        for account_id in account_ids:
            act_id = account_id if account_id.startswith("act_") else f"act_{account_id}"
            account = AdAccount(act_id)
            insights = account.get_insights(
                fields=INSIGHT_FIELDS,
                params={
                    "level": "ad",
                    "time_range": {"since": date, "until": date},
                    "breakdowns": ["device_platform", "publisher_platform"],
                },
            )
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

        return results

    def extract_demographics(self, account_ids: list[str], date: str) -> list[dict]:
        self._init_api()
        results = []

        for account_id in account_ids:
            act_id = account_id if account_id.startswith("act_") else f"act_{account_id}"
            account = AdAccount(act_id)
            insights = account.get_insights(
                fields=INSIGHT_FIELDS,
                params={
                    "level": "ad",
                    "time_range": {"since": date, "until": date},
                    "breakdowns": ["age", "gender"],
                },
            )
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
                    "age": data.get("age", ""),
                    "gender": data.get("gender", ""),
                    "impressions": int(data.get("impressions", 0)),
                    "clicks": int(data.get("clicks", 0)),
                    "spend": float(data.get("spend", 0.0)),
                    "date_start": data.get("date_start", ""),
                    "date_stop": data.get("date_stop", ""),
                    "actions": actions,
                })

        return results
