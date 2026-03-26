import os
from datetime import datetime

from airflow.sdk import dag, task

DAG_ID = "daily_alerts"


@dag(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["alerts"],
)
def daily_alerts():

    @task
    def detect_alerts():
        from airflow.sdk import get_current_context
        from src.alerts.detector import AlertDetector
        from src.db.connection import get_connection

        context = get_current_context()
        logical_date = context.get("logical_date")
        check_date = logical_date.date() if logical_date else datetime.now().date()

        conn = get_connection(os.environ["DATABASE_URL"])
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS gold.active_alerts (
                    account_id VARCHAR,
                    account_name VARCHAR,
                    date DATE,
                    alert_type VARCHAR,
                    metric_name VARCHAR,
                    current_value DOUBLE PRECISION,
                    previous_value DOUBLE PRECISION,
                    change_pct DOUBLE PRECISION,
                    severity VARCHAR,
                    created_at TIMESTAMP DEFAULT current_timestamp
                )
            """)
        conn.commit()

        detector = AlertDetector(conn)
        alerts = detector.detect(check_date)
        conn.close()
        return len(alerts)

    detect_alerts()


dag = daily_alerts()
