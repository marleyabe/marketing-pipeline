import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from callbacks.discord_alert import send_discord_alert

DAG_ID = "backfill_meta_ads"


@dag(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["backfill", "meta"],
    params={"days_back": 30},
    on_failure_callback=send_discord_alert,
)
def backfill_meta_ads():

    @task
    def list_accounts() -> list[dict]:
        from src.extractors.meta_ads import MetaAdsExtractor

        extractor = MetaAdsExtractor({
            "app_id": os.environ["META_APP_ID"],
            "app_secret": os.environ["META_APP_SECRET"],
            "access_token": os.environ["META_ACCESS_TOKEN"],
        })
        return extractor.list_accounts()

    @task
    def extract_account(account: dict) -> list[dict]:
        from airflow.sdk import get_current_context
        from src.extractors.meta_ads import MetaAdsExtractor

        context = get_current_context()
        days_back = int(context.get("params", {}).get("days_back", 30))
        today = datetime.now().date()
        dates = [
            (today - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(1, days_back + 1)
        ]

        extractor = MetaAdsExtractor({
            "app_id": os.environ["META_APP_ID"],
            "app_secret": os.environ["META_APP_SECRET"],
            "access_token": os.environ["META_ACCESS_TOKEN"],
        })

        results = []
        for date in dates:
            results.extend(extractor.extract([account["account_id"]], date))
        return results

    @task
    def load(batches: list[list[dict]]) -> None:
        from src.db.connection import get_connection
        from src.db.schema import initialize_schemas
        from src.loaders.postgres_loader import PostgresBronzeLoader

        conn = get_connection(os.environ["DATABASE_URL"])
        initialize_schemas(conn)
        loader = PostgresBronzeLoader(conn)

        for batch in batches:
            loader.load(batch, "meta_ads_raw", source="meta_ads")

        conn.close()

    accounts = list_accounts()
    extracted = extract_account.expand(account=accounts)
    load(extracted)


dag = backfill_meta_ads()
