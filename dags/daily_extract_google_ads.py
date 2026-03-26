import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from callbacks.discord_alert import send_discord_alert

DAG_ID = "daily_extract_google_ads"


@dag(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["extract", "google"],
    on_failure_callback=send_discord_alert,
)
def daily_extract_google_ads():

    @task
    def list_accounts() -> list[dict]:
        from src.extractors.google_ads import GoogleAdsExtractor

        extractor = GoogleAdsExtractor({
            "developer_token": os.environ["GOOGLE_DEVELOPER_TOKEN"],
            "client_id": os.environ["GOOGLE_CLIENT_ID"],
            "client_secret": os.environ["GOOGLE_CLIENT_SECRET"],
            "refresh_token": os.environ["GOOGLE_REFRESH_TOKEN"],
            "login_customer_id": os.environ["GOOGLE_LOGIN_CUSTOMER_ID"],
        })
        return extractor.list_accounts()

    @task(retries=3, retry_delay=timedelta(minutes=5))
    def extract_account(account: dict) -> list[dict]:
        from airflow.sdk import get_current_context
        from src.extractors.google_ads import GoogleAdsExtractor

        context = get_current_context()
        logical_date = context.get("logical_date")
        ds = logical_date.strftime("%Y-%m-%d") if logical_date else datetime.now().strftime("%Y-%m-%d")
        extractor = GoogleAdsExtractor({
            "developer_token": os.environ["GOOGLE_DEVELOPER_TOKEN"],
            "client_id": os.environ["GOOGLE_CLIENT_ID"],
            "client_secret": os.environ["GOOGLE_CLIENT_SECRET"],
            "refresh_token": os.environ["GOOGLE_REFRESH_TOKEN"],
            "login_customer_id": os.environ["GOOGLE_LOGIN_CUSTOMER_ID"],
        })
        return extractor.extract([account["customer_id"]], ds)

    @task
    def load(batches: list[list[dict]]) -> None:
        from src.db.connection import get_connection
        from src.db.schema import initialize_schemas
        from src.loaders.postgres_loader import PostgresBronzeLoader

        conn = get_connection(os.environ["DATABASE_URL"])
        initialize_schemas(conn)
        loader = PostgresBronzeLoader(conn)

        for batch in batches:
            loader.load(batch, "google_ads_raw", source="google_ads")

        conn.close()

    accounts = list_accounts()
    extracted = extract_account.expand(account=accounts)
    load(extracted)


dag = daily_extract_google_ads()
