import os
from datetime import datetime

from airflow.sdk import dag, task

DAG_ID = "daily_reports"


@dag(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["reports"],
)
def daily_reports():

    @task
    def generate_daily():
        from airflow.sdk import get_current_context
        from src.db.connection import get_connection
        from src.reports.daily import DailyReportGenerator

        context = get_current_context()
        logical_date = context.get("logical_date")
        report_date = logical_date.date() if logical_date else datetime.now().date()

        conn = get_connection(os.environ.get("DUCKDB_PATH", "data/ads2u.duckdb"))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold.generated_reports (
                account_id VARCHAR,
                account_name VARCHAR,
                report_type VARCHAR,
                report_date DATE,
                report_text VARCHAR,
                created_at TIMESTAMP DEFAULT current_timestamp
            )
        """)

        generator = DailyReportGenerator(conn)
        reports = generator.generate(report_date)
        conn.close()
        return len(reports)

    @task
    def generate_weekly():
        from airflow.sdk import get_current_context
        from src.db.connection import get_connection
        from src.reports.weekly import WeeklyReportGenerator

        context = get_current_context()
        logical_date = context.get("logical_date")
        report_date = logical_date.date() if logical_date else datetime.now().date()

        # Only generate weekly report on Sundays
        if report_date.weekday() != 6:
            return 0

        conn = get_connection(os.environ.get("DUCKDB_PATH", "data/ads2u.duckdb"))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold.generated_reports (
                account_id VARCHAR,
                account_name VARCHAR,
                report_type VARCHAR,
                report_date DATE,
                report_text VARCHAR,
                created_at TIMESTAMP DEFAULT current_timestamp
            )
        """)

        generator = WeeklyReportGenerator(conn)
        reports = generator.generate(report_date)
        conn.close()
        return len(reports)

    generate_daily() >> generate_weekly()


dag = daily_reports()
