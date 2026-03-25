import os
from datetime import date

import pytest

from src.alerts.detector import AlertDetector
from src.db.schema import initialize_schemas
from src.loaders.duckdb_loader import DuckDBBronzeLoader
from src.transformers.sql_runner import SQLRunner

SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "sql")


@pytest.fixture
def setup_with_two_days(memory_connection):
    """Load two days of data with a spend drop on day 2."""
    initialize_schemas(memory_connection)
    loader = DuckDBBronzeLoader(memory_connection)

    # Day 1: normal performance
    day1_meta = [
        {
            "account_id": "act_123", "account_name": "Cliente A",
            "campaign_id": "c1", "campaign_name": "C1",
            "ad_id": "a1", "ad_name": "A1",
            "impressions": "1000", "clicks": "50", "spend": "100.00",
            "date_start": "2026-03-21", "date_stop": "2026-03-21",
            "actions": '[{"action_type": "purchase", "value": "10"}]',
        }
    ]
    # Day 2: 60% spend drop
    day2_meta = [
        {
            "account_id": "act_123", "account_name": "Cliente A",
            "campaign_id": "c1", "campaign_name": "C1",
            "ad_id": "a1", "ad_name": "A1",
            "impressions": "500", "clicks": "20", "spend": "40.00",
            "date_start": "2026-03-22", "date_stop": "2026-03-22",
            "actions": '[{"action_type": "purchase", "value": "3"}]',
        }
    ]

    loader.load(day1_meta, "meta_ads_raw", source="meta_ads")
    loader.load(day2_meta, "meta_ads_raw", source="meta_ads")

    # Need at least empty google table for transforms
    loader.load([], "google_ads_raw", source="google_ads")

    SQLRunner(memory_connection, SQL_DIR).run_all()

    memory_connection.execute("""
        CREATE TABLE IF NOT EXISTS gold.active_alerts (
            account_id VARCHAR,
            account_name VARCHAR,
            date DATE,
            alert_type VARCHAR,
            metric_name VARCHAR,
            current_value DOUBLE,
            previous_value DOUBLE,
            change_pct DOUBLE,
            severity VARCHAR,
            created_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    return memory_connection


class TestAlertDetector:
    def test_detects_spend_drop(self, setup_with_two_days):
        detector = AlertDetector(setup_with_two_days)
        alerts = detector.detect(date(2026, 3, 22))

        spend_alerts = [a for a in alerts if a["metric_name"] == "spend"]
        assert len(spend_alerts) > 0
        assert spend_alerts[0]["change_pct"] < -30

    def test_severity_critical_for_large_drop(self, setup_with_two_days):
        detector = AlertDetector(setup_with_two_days)
        alerts = detector.detect(date(2026, 3, 22))

        spend_alerts = [a for a in alerts if a["metric_name"] == "spend"]
        assert spend_alerts[0]["severity"] == "critical"  # -60% > 50%

    def test_alerts_stored_in_gold_table(self, setup_with_two_days):
        detector = AlertDetector(setup_with_two_days)
        detector.detect(date(2026, 3, 22))

        count = setup_with_two_days.execute(
            "SELECT COUNT(*) FROM gold.active_alerts"
        ).fetchone()[0]
        assert count > 0

    def test_no_alert_when_within_threshold(self, memory_connection):
        """Small drop (10%) should not trigger alert."""
        initialize_schemas(memory_connection)
        loader = DuckDBBronzeLoader(memory_connection)

        day1 = [{
            "account_id": "act_999", "account_name": "Normal",
            "campaign_id": "c1", "campaign_name": "C1",
            "ad_id": "a1", "ad_name": "A1",
            "impressions": "1000", "clicks": "50", "spend": "100.00",
            "date_start": "2026-03-21", "date_stop": "2026-03-21",
            "actions": '[{"action_type": "purchase", "value": "10"}]',
        }]
        day2 = [{
            "account_id": "act_999", "account_name": "Normal",
            "campaign_id": "c1", "campaign_name": "C1",
            "ad_id": "a1", "ad_name": "A1",
            "impressions": "900", "clicks": "45", "spend": "90.00",
            "date_start": "2026-03-22", "date_stop": "2026-03-22",
            "actions": '[{"action_type": "purchase", "value": "9"}]',
        }]

        loader.load(day1, "meta_ads_raw", source="meta_ads")
        loader.load(day2, "meta_ads_raw", source="meta_ads")
        loader.load([], "google_ads_raw", source="google_ads")

        SQLRunner(memory_connection, SQL_DIR).run_all()

        memory_connection.execute("""
            CREATE TABLE IF NOT EXISTS gold.active_alerts (
                account_id VARCHAR, account_name VARCHAR, date DATE,
                alert_type VARCHAR, metric_name VARCHAR,
                current_value DOUBLE, previous_value DOUBLE,
                change_pct DOUBLE, severity VARCHAR,
                created_at TIMESTAMP DEFAULT current_timestamp
            )
        """)

        detector = AlertDetector(memory_connection)
        alerts = detector.detect(date(2026, 3, 22))
        assert len(alerts) == 0  # 10% drop is within threshold
