import logging

from google.ads.googleads.client import GoogleAdsClient

from src.extractors.base import BaseExtractor

logger = logging.getLogger(__name__)

CAMPAIGN_PERFORMANCE_QUERY = """
    SELECT
        customer.id,
        customer.descriptive_name,
        campaign.id,
        campaign.name,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.conversions,
        segments.date
    FROM campaign
    WHERE segments.date = '{date}'
"""


class GoogleAdsExtractor(BaseExtractor):
    def __init__(self, credentials: dict):
        self._credentials = credentials

    def _get_client(self) -> GoogleAdsClient:
        return GoogleAdsClient.load_from_dict({
            "developer_token": self._credentials["developer_token"],
            "client_id": self._credentials["client_id"],
            "client_secret": self._credentials["client_secret"],
            "refresh_token": self._credentials["refresh_token"],
            "login_customer_id": self._credentials["login_customer_id"],
            "use_proto_plus": True,
        })

    def list_accounts(self) -> list[dict]:
        client = self._get_client()
        service = client.get_service("GoogleAdsService")

        query = """
            SELECT
                customer_client.id,
                customer_client.descriptive_name,
                customer_client.manager,
                customer_client.status
            FROM customer_client
            WHERE customer_client.status = 'ENABLED'
        """

        rows = service.search(
            customer_id=self._credentials["login_customer_id"],
            query=query,
        )

        return [
            {
                "customer_id": str(row.customer_client.id),
                "customer_name": row.customer_client.descriptive_name,
            }
            for row in rows
            if not row.customer_client.manager
            and row.customer_client.status.name == "ENABLED"
        ]

    def extract(self, account_ids: list[str], date: str) -> list[dict]:
        client = self._get_client()
        service = client.get_service("GoogleAdsService")
        results = []

        for customer_id in account_ids:
            try:
                query = CAMPAIGN_PERFORMANCE_QUERY.format(date=date)
                rows = service.search(customer_id=customer_id, query=query)

                for row in rows:
                    results.append({
                        "customer_id": str(row.customer.id),
                        "customer_name": row.customer.descriptive_name,
                        "campaign_id": str(row.campaign.id),
                        "campaign_name": row.campaign.name,
                        "impressions": row.metrics.impressions,
                        "clicks": row.metrics.clicks,
                        "spend": row.metrics.cost_micros / 1_000_000,
                        "conversions": row.metrics.conversions,
                        "date": row.segments.date,
                    })
            except Exception:
                logger.exception("Error extracting Google Ads for customer %s", customer_id)

        return results
