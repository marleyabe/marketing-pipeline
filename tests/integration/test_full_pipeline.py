"""End-to-end test: mock API data → bronze → silver → gold → reports → alerts."""

import os
from datetime import date

import pytest

from src.alerts.detector import AlertDetector
from src.db.schema import initialize_schemas
from src.loaders.duckdb_loader import DuckDBBronzeLoader
from src.reports.daily import DailyReportGenerator
from src.reports.weekly import WeeklyReportGenerator
from src.transformers.sql_runner import SQLRunner

SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "sql")


@pytest.fixture
def full_pipeline(memory_connection):
    """Load two days of data for both platforms and run full pipeline."""
    conn = memory_connection
    initialize_schemas(conn)
    loader = DuckDBBronzeLoader(conn)

    # --- Day 1: 2026-03-21 (Saturday) ---
    meta_day1 = [
        {
            "account_id": "act_100",
            "account_name": "Cliente Alpha",
            "campaign_id": "c1",
            "campaign_name": "Camp Meta 1",
            "ad_id": "a1",
            "ad_name": "Ad 1",
            "impressions": "5000",
            "clicks": "200",
            "spend": "500.00",
            "date_start": "2026-03-21",
            "date_stop": "2026-03-21",
            "actions": '[{"action_type": "purchase", "value": "20"}]',
        },
    ]
    google_day1 = [
        {
            "customer_id": "g_200",
            "customer_name": "Cliente Beta",
            "campaign_id": "gc1",
            "campaign_name": "Camp Google 1",
            "ad_group_id": "ag_1",
            "ad_group_name": "Grupo 1",
            "keyword_id": "kw_1",
            "keyword_text": "palavra chave 1",
            "match_type": "BROAD",
            "impressions": 3000,
            "clicks": 150,
            "spend": 300.0,
            "conversions": 12.0,
            "date": "2026-03-21",
        },
    ]

    # --- Day 2: 2026-03-22 (Sunday) - Meta drops 60%, Google stable ---
    meta_day2 = [
        {
            "account_id": "act_100",
            "account_name": "Cliente Alpha",
            "campaign_id": "c1",
            "campaign_name": "Camp Meta 1",
            "ad_id": "a1",
            "ad_name": "Ad 1",
            "impressions": "2000",
            "clicks": "80",
            "spend": "200.00",
            "date_start": "2026-03-22",
            "date_stop": "2026-03-22",
            "actions": '[{"action_type": "purchase", "value": "5"}]',
        },
    ]
    google_day2 = [
        {
            "customer_id": "g_200",
            "customer_name": "Cliente Beta",
            "campaign_id": "gc1",
            "campaign_name": "Camp Google 1",
            "ad_group_id": "ag_1",
            "ad_group_name": "Grupo 1",
            "keyword_id": "kw_1",
            "keyword_text": "palavra chave 1",
            "match_type": "BROAD",
            "impressions": 2800,
            "clicks": 140,
            "spend": 280.0,
            "conversions": 11.0,
            "date": "2026-03-22",
        },
    ]

    loader.load(meta_day1, "meta_ads_raw", source="meta_ads")
    loader.load(google_day1, "google_ads_keywords_raw", source="google_ads_keywords")
    loader.load(meta_day2, "meta_ads_raw", source="meta_ads")
    loader.load(google_day2, "google_ads_keywords_raw", source="google_ads_keywords")

    # Run all SQL transforms (silver + gold)
    SQLRunner(conn, SQL_DIR).run_all()

    # Create output tables
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
    conn.execute("""
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

    return conn


class TestFullPipelineE2E:
    """Verify full data flow from bronze to reports and alerts."""

    # --- Bronze layer ---

    def test_bronze_has_both_platforms(self, full_pipeline):
        conn = full_pipeline
        meta_count = conn.execute(
            "SELECT COUNT(*) FROM bronze.meta_ads_raw"
        ).fetchone()[0]
        google_count = conn.execute(
            "SELECT COUNT(*) FROM bronze.google_ads_keywords_raw"
        ).fetchone()[0]
        assert meta_count == 2  # 2 days
        assert google_count == 2

    # --- Silver layer ---

    def test_silver_unified_has_both_platforms(self, full_pipeline):
        conn = full_pipeline
        platforms = conn.execute(
            "SELECT DISTINCT platform FROM silver.unified_campaigns ORDER BY platform"
        ).fetchall()
        assert [p[0] for p in platforms] == ["google_ads", "meta_ads"]

    def test_silver_unified_has_all_rows(self, full_pipeline):
        conn = full_pipeline
        count = conn.execute(
            "SELECT COUNT(*) FROM silver.unified_campaigns"
        ).fetchone()[0]
        assert count == 4  # 2 meta + 2 google

    # --- Gold layer ---

    def test_gold_daily_performance_exists(self, full_pipeline):
        conn = full_pipeline
        count = conn.execute(
            "SELECT COUNT(*) FROM gold.daily_performance"
        ).fetchone()[0]
        assert count == 4  # 2 accounts x 2 days

    def test_gold_daily_performance_meta_day2(self, full_pipeline):
        conn = full_pipeline
        row = conn.execute(
            "SELECT impressions, clicks, spend, conversions "
            "FROM gold.daily_performance "
            "WHERE account_id = 'act_100' AND date = '2026-03-22'"
        ).fetchone()
        assert row[0] == 2000
        assert row[1] == 80
        assert row[2] == 200.0
        assert row[3] == 5.0

    def test_gold_reports_daily_has_portuguese_columns(self, full_pipeline):
        conn = full_pipeline
        columns = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'gold' AND table_name = 'reports_daily' "
            "ORDER BY ordinal_position"
        ).fetchall()
        names = [c[0] for c in columns]
        for expected in ["investimento", "impressoes", "cliques", "conversoes"]:
            assert expected in names

    def test_gold_alerts_daily_detects_spend_drop(self, full_pipeline):
        conn = full_pipeline
        row = conn.execute(
            "SELECT spend_change_pct FROM gold.alerts_daily "
            "WHERE account_id = 'act_100' AND date = '2026-03-22'"
        ).fetchone()
        assert row is not None
        assert row[0] == -60.0  # 200/500 - 1 = -60%

    # --- Daily reports ---

    def test_daily_report_generated_for_each_account(self, full_pipeline):
        conn = full_pipeline
        generator = DailyReportGenerator(conn)
        reports = generator.generate(date(2026, 3, 22))
        assert len(reports) == 2  # Meta + Google accounts

    def test_daily_report_contains_account_name(self, full_pipeline):
        conn = full_pipeline
        generator = DailyReportGenerator(conn)
        reports = generator.generate(date(2026, 3, 22))
        texts = [r["report_text"] for r in reports]
        assert any("Cliente Alpha" in t for t in texts)
        assert any("Cliente Beta" in t for t in texts)

    def test_daily_report_stored_in_gold(self, full_pipeline):
        conn = full_pipeline
        DailyReportGenerator(conn).generate(date(2026, 3, 22))
        count = conn.execute(
            "SELECT COUNT(*) FROM gold.generated_reports WHERE report_type = 'daily'"
        ).fetchone()[0]
        assert count == 2

    # --- Alerts ---

    def test_alert_detector_finds_critical_spend_drop(self, full_pipeline):
        conn = full_pipeline
        detector = AlertDetector(conn)
        alerts = detector.detect(date(2026, 3, 22))
        spend_alerts = [a for a in alerts if a["metric_name"] == "spend"]
        # act_100 had -60% spend drop → critical
        meta_alert = [a for a in spend_alerts if a["account_id"] == "act_100"]
        assert len(meta_alert) == 1
        assert meta_alert[0]["severity"] == "critical"

    def test_alert_detector_finds_conversion_drop(self, full_pipeline):
        conn = full_pipeline
        detector = AlertDetector(conn)
        alerts = detector.detect(date(2026, 3, 22))
        conv_alerts = [a for a in alerts if a["metric_name"] == "conversions"]
        # act_100: 5 vs 20 = -75% → critical
        meta_conv = [a for a in conv_alerts if a["account_id"] == "act_100"]
        assert len(meta_conv) == 1
        assert meta_conv[0]["severity"] == "critical"

    def test_no_alert_for_stable_account(self, full_pipeline):
        conn = full_pipeline
        detector = AlertDetector(conn)
        alerts = detector.detect(date(2026, 3, 22))
        # g_200: spend -6.7%, conversions -8.3% → within threshold
        google_alerts = [a for a in alerts if a["account_id"] == "g_200"]
        assert len(google_alerts) == 0

    def test_alerts_stored_in_active_alerts(self, full_pipeline):
        conn = full_pipeline
        AlertDetector(conn).detect(date(2026, 3, 22))
        count = conn.execute(
            "SELECT COUNT(*) FROM gold.active_alerts"
        ).fetchone()[0]
        assert count > 0
