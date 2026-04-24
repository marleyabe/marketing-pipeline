"""Google Ads DAG. Três extrações encadeadas; só a última emite o Dataset
que dispara o daily_transform (dbt).

Lógica de API fica em dags.shared.google_extractor para permitir execução sem Airflow.
"""

from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task

from dags.shared.extractor import (
    ExtractorSpec,
    date_range_params,
    default_args,
    run_extraction,
    run_snapshot_extraction,
)
from dags.shared.google_extractor import (
    build_client,
    extract_keywords,
    extract_negatives,
    extract_search_terms,
    list_accounts,
)

PLATFORM = "google_ads"
BRONZE_KEYWORDS_TABLE = "google_ads_raw"
BRONZE_KEYWORDS_CONFLICT = ["date", "customer_id", "campaign_id", "ad_group_id", "keyword_id", "device"]
BRONZE_SEARCH_TERMS_TABLE = "google_search_terms_raw"
BRONZE_SEARCH_TERMS_CONFLICT = ["date", "customer_id", "ad_group_id", "search_term", "matched_keyword_text", "matched_keyword_match_type"]
BRONZE_NEGATIVES_TABLE = "google_negatives_raw"
BRONZE_NEGATIVES_CONFLICT = ["snapshot_date", "customer_id", "scope", "criterion_id", "ad_group_id", "campaign_id"]

bronze_google_dataset = Dataset("ads2u/bronze/google_ads")


KEYWORDS_SPEC = ExtractorSpec(
    platform=PLATFORM,
    bronze_table=BRONZE_KEYWORDS_TABLE,
    conflict_keys=BRONZE_KEYWORDS_CONFLICT,
    init_api=build_client,
    list_accounts=list_accounts,
    fetch_date=extract_keywords,
)

SEARCH_TERMS_SPEC = ExtractorSpec(
    platform=PLATFORM,
    bronze_table=BRONZE_SEARCH_TERMS_TABLE,
    conflict_keys=BRONZE_SEARCH_TERMS_CONFLICT,
    init_api=build_client,
    list_accounts=list_accounts,
    fetch_date=extract_search_terms,
)

NEGATIVES_SPEC = ExtractorSpec(
    platform=PLATFORM,
    bronze_table=BRONZE_NEGATIVES_TABLE,
    conflict_keys=BRONZE_NEGATIVES_CONFLICT,
    init_api=build_client,
    list_accounts=list_accounts,
    fetch_date=extract_negatives,
)


@dag(
    dag_id="google_ads",
    schedule="0 5 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["extract", "google_ads"],
    default_args=default_args(),
    params=date_range_params(),
)
def google_ads() -> None:
    @task()
    def extract_keywords_task(**context) -> int:
        return run_extraction(KEYWORDS_SPEC, context)

    @task()
    def extract_search_terms_task(**context) -> int:
        return run_extraction(SEARCH_TERMS_SPEC, context)

    @task(outlets=[bronze_google_dataset])
    def snapshot_negatives_task() -> int:
        return run_snapshot_extraction(NEGATIVES_SPEC)

    extract_keywords_task() >> extract_search_terms_task() >> snapshot_negatives_task()


google_ads()
