"""dbt silver + gold via Cosmos. Trigger por Datasets dos extractors."""

import os
from datetime import datetime
from pathlib import Path

from airflow.datasets import Dataset
from airflow.decorators import dag
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

bronze_google_dataset = Dataset("ads2u/bronze/google_ads")
bronze_meta_dataset = Dataset("ads2u/bronze/meta_ads")

DBT_ROOT = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_project"))

profile_config = ProfileConfig(
    profile_name="ads2u",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_marketing",
        profile_args={"schema": "public"},
    ),
)


@dag(
    dag_id="daily_transform",
    schedule=[bronze_google_dataset, bronze_meta_dataset],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["daily", "transform", "dbt"],
)
def daily_transform():
    DbtTaskGroup(
        group_id="transform",
        project_config=ProjectConfig(str(DBT_ROOT)),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/home/airflow/.local/bin/dbt"
        ),
    )


daily_transform()
