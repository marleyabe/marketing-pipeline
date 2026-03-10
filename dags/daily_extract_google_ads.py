import os
from datetime import datetime

import pandas as pd
import pendulum
from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient
from sqlalchemy import create_engine, text

from airflow.sdk import dag, task, get_current_context

load_dotenv()


POSTGRES_CONN = (
    f"postgresql+psycopg2://{os.environ['POSTGRES_USERNAME']}:{os.environ['POSTGRES_PASSWORD']}"
    f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DATABASE']}"
)
BRONZE_TABLE = "bronze.google_ads"


def get_google_ads_client():
    credentials = {
        "developer_token": os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
        "client_id": os.environ["GOOGLE_ADS_CLIENT_ID"],
        "client_secret": os.environ["GOOGLE_ADS_CLIENT_SECRET"],
        "refresh_token": os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
        "login_customer_id": os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"].replace("-", ""),
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(credentials)


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["google_ads", "bronze", "extract"],
)
def daily_extract_google_ads():
    """
    Extrai palavras-chave ativas de todas as contas do MCC do Google Ads
    e salva na camada Bronze do PostgreSQL.
    """

    @task()
    def list_accounts() -> list[str]:
        """Lista todas as contas de anúncio acessíveis no MCC."""
        client = get_google_ads_client()
        login_customer_id = os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"]

        query = """
            SELECT
                customer_client.id,
                customer_client.descriptive_name,
                customer_client.manager,
                customer_client.status
            FROM customer_client
            WHERE customer_client.status = 'ENABLED'
                AND customer_client.manager = FALSE
        """

        ga_service = client.get_service("GoogleAdsService")
        response = ga_service.search(customer_id=login_customer_id, query=query)

        account_ids = []
        for row in response:
            account_id = str(row.customer_client.id)
            name = row.customer_client.descriptive_name
            print(f"Conta encontrada: {account_id} - {name}")
            account_ids.append(account_id)

        print(f"Total de contas encontradas: {len(account_ids)}")
        return account_ids

    @task()
    def extract_keywords(account_ids: list[str]) -> list[dict]:
        """Extrai todas as palavras-chave ativas de cada conta."""
        context = get_current_context()
        extraction_date = str(context["data_interval_start"].date())

        client = get_google_ads_client()

        query = """
            SELECT
                customer.id,
                customer.descriptive_name,
                campaign.id,
                campaign.name,
                campaign.status,
                ad_group.id,
                ad_group.name,
                ad_group_criterion.criterion_id,
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                ad_group_criterion.status,
                ad_group_criterion.quality_info.quality_score,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.average_cpc
            FROM keyword_view
            WHERE ad_group_criterion.status = 'ENABLED'
                AND campaign.status = 'ENABLED'
                AND ad_group.status = 'ENABLED'
                AND segments.date DURING YESTERDAY
        """

        ga_service = client.get_service("GoogleAdsService")
        all_keywords = []

        for account_id in account_ids:
            print(f"Extraindo keywords da conta: {account_id}")
            try:
                response = ga_service.search(customer_id=account_id, query=query)
                for row in response:
                    all_keywords.append({
                        "customer_id": row.customer.id,
                        "customer_name": row.customer.descriptive_name,
                        "campaign_id": row.campaign.id,
                        "campaign_name": row.campaign.name,
                        "campaign_status": row.campaign.status.name,
                        "ad_group_id": row.ad_group.id,
                        "ad_group_name": row.ad_group.name,
                        "criterion_id": row.ad_group_criterion.criterion_id,
                        "keyword_text": row.ad_group_criterion.keyword.text,
                        "match_type": row.ad_group_criterion.keyword.match_type.name,
                        "keyword_status": row.ad_group_criterion.status.name,
                        "quality_score": row.ad_group_criterion.quality_info.quality_score,
                        "impressions": row.metrics.impressions,
                        "clicks": row.metrics.clicks,
                        "cost_micros": row.metrics.cost_micros,
                        "cost": row.metrics.cost_micros / 1_000_000,
                        "conversions": row.metrics.conversions,
                        "average_cpc": row.metrics.average_cpc / 1_000_000,
                        "extraction_date": extraction_date,
                    })
            except Exception as e:
                print(f"Erro na conta {account_id}: {e}")

        print(f"Total de keywords extraídas: {len(all_keywords)}")
        return all_keywords

    @task()
    def load_to_postgres(keywords: list[dict]):
        """Salva as palavras-chave na camada Bronze do PostgreSQL."""
        if not keywords:
            print("Nenhuma keyword para salvar.")
            return

        df = pd.DataFrame(keywords)
        df["extracted_at"] = datetime.utcnow()

        engine = create_engine(POSTGRES_CONN)

        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))

        df.to_sql(
            name="google_ads",
            schema="bronze",
            con=engine,
            if_exists="append",
            index=False,
        )

        print(f"{len(df)} keywords salvas na tabela {BRONZE_TABLE}")

    account_ids = list_accounts()
    keywords = extract_keywords(account_ids)
    load_to_postgres(keywords)


daily_extract_google_ads()
