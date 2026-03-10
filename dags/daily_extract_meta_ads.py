import os
from datetime import datetime

import pandas as pd
import pendulum
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from airflow.sdk import dag, task, get_current_context

# Importações do SDK do Facebook
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount

load_dotenv()

POSTGRES_CONN = (
    f"postgresql+psycopg2://{os.environ['POSTGRES_USERNAME']}:{os.environ['POSTGRES_PASSWORD']}"
    f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DATABASE']}"
)
BRONZE_TABLE = "bronze.meta_ads_performance"

def init_meta_api():
    """Inicializa a sessão da API do Meta."""
    FacebookAdsApi.init(
        app_id=os.environ["META_APP_ID"],
        app_secret=os.environ["META_APP_SECRET"],
        access_token=os.environ["META_ACCESS_TOKEN"]
    )

@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["meta_ads", "bronze", "extract"],
)
def daily_extract_meta_ads():

    @task()
    def list_accounts() -> list[str]:
        """Lista contas de anúncio ativas."""
        init_meta_api()
        me = User(fbid='me')
        accounts = me.get_ad_accounts(fields=['name', 'account_status'])

        # account_status 1 = ACTIVE
        account_ids = [acc['id'] for acc in accounts if acc.get('account_status') == 1]
        print(f"Total de contas ativas encontradas: {len(account_ids)}")
        return account_ids

    @task()
    def extract_ads(account_ids: list[str]) -> list[dict]:
        """Extrai métricas (Insights) no nível do anúncio de cada conta."""
        context = get_current_context()
        extraction_date = str(context["data_interval_start"].date())
        init_meta_api()

        fields = [
            'ad_id', 'ad_name', 'adset_id', 'adset_name',
            'campaign_id', 'campaign_name', 'impressions',
            'clicks', 'spend'
        ]
        
        # Parâmetros para buscar dados de D-1
        params = {
            'level': 'ad',
            'date_preset': 'yesterday'
        }

        all_ads = []

        for account_id in account_ids:
            print(f"Extraindo ads da conta: {account_id}")
            try:
                acc = AdAccount(account_id)
                insights = acc.get_insights(fields=fields, params=params)

                for row in insights:
                    all_ads.append({
                        "account_id": account_id,
                        "ad_id": row.get('ad_id'),
                        "ad_name": row.get('ad_name'),
                        "adset_id": row.get('adset_id'),
                        "adset_name": row.get('adset_name'),
                        "campaign_id": row.get('campaign_id'),
                        "campaign_name": row.get('campaign_name'),
                        "impressions": int(row.get('impressions', 0)),
                        "clicks": int(row.get('clicks', 0)),
                        "spend": float(row.get('spend', 0.0)),
                        "extraction_date": extraction_date,
                    })
            except Exception as e:
                print(f"Erro na conta {account_id}: {e}")

        print(f"Total de ads extraídos: {len(all_ads)}")
        return all_ads

    @task()
    def load_to_postgres(ads: list[dict]):
        """Salva os dados extraídos no PostgreSQL."""
        if not ads:
            print("Nenhum ad para salvar.")
            return

        df = pd.DataFrame(ads)
        df["extracted_at"] = datetime.utcnow()

        engine = create_engine(POSTGRES_CONN)

        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))

        df.to_sql(
            name="meta_ads_performance",
            schema="bronze",
            con=engine,
            if_exists="append",
            index=False,
        )

        print(f"{len(df)} ads salvos na tabela {BRONZE_TABLE}")

    account_ids = list_accounts()
    ads = extract_ads(account_ids)
    load_to_postgres(ads)

daily_extract_meta_ads()