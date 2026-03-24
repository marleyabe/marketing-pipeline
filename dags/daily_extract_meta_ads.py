import os
from datetime import datetime

from airflow.sdk import dag, task

DAG_ID = "daily_extract_meta_ads"


@dag(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["extract", "meta"],
)
def daily_extract_meta_ads():

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
        logical_date = context.get("logical_date")
        ds = logical_date.strftime("%Y-%m-%d") if logical_date else datetime.now().strftime("%Y-%m-%d")
        extractor = MetaAdsExtractor({
            "app_id": os.environ["META_APP_ID"],
            "app_secret": os.environ["META_APP_SECRET"],
            "access_token": os.environ["META_ACCESS_TOKEN"],
        })
        return extractor.extract([account["account_id"]], ds)

    @task
    def load(batches: list[list[dict]]) -> None:
        from src.db.connection import get_connection
        from src.db.schema import initialize_schemas
        from src.loaders.duckdb_loader import DuckDBBronzeLoader

        conn = get_connection(os.environ.get("DUCKDB_PATH", "data/ads2u.duckdb"))
        initialize_schemas(conn)
        loader = DuckDBBronzeLoader(conn)

        for batch in batches:
            loader.load(batch, "meta_ads_raw", source="meta_ads")

        conn.close()

    accounts = list_accounts()
    extracted = extract_account.expand(account=accounts)
    load(extracted)


dag = daily_extract_meta_ads()
