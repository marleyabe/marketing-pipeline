import os
from datetime import datetime

from airflow.sdk import dag, task

from callbacks.discord_alert import send_discord_alert

DAG_ID = "daily_transform"

SQL_DIR = os.environ.get("SQL_DIR", "/opt/pipeline/sql")


@dag(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["transform"],
    on_failure_callback=send_discord_alert,
)
def daily_transform():

    @task
    def run_silver():
        from src.db.connection import get_connection
        from src.transformers.sql_runner import SQLRunner

        conn = get_connection(os.environ["DATABASE_URL"])
        SQLRunner(conn, SQL_DIR).run_silver()
        conn.close()

    @task
    def run_gold():
        from src.db.connection import get_connection
        from src.transformers.sql_runner import SQLRunner

        conn = get_connection(os.environ["DATABASE_URL"])
        SQLRunner(conn, SQL_DIR).run_gold()
        conn.close()

    run_silver() >> run_gold()


dag = daily_transform()
