import os
from datetime import date

import pytest

from src.db.schema import initialize_schemas
from src.loaders.postgres_loader import PostgresBronzeLoader
from src.reports.weekly import WeeklyReportGenerator
from src.transformers.sql_runner import SQLRunner

SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "sql")


@pytest.fixture
def setup(memory_connection, sample_meta_ads_data, sample_google_ads_data):
    initialize_schemas(memory_connection)
    loader = PostgresBronzeLoader(memory_connection)
    loader.load(sample_meta_ads_data, "meta_ads_raw", source="meta_ads")
    loader.load(sample_google_ads_data, "google_ads_raw", source="google_ads")
    SQLRunner(memory_connection, SQL_DIR).run_all()

    with memory_connection.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gold.generated_reports (
                account_id VARCHAR,
                account_name VARCHAR,
                report_type VARCHAR,
                report_date DATE,
                report_text VARCHAR,
                generated_at TIMESTAMP DEFAULT current_timestamp
            )
        """)
    memory_connection.commit()
    return memory_connection


class TestWeeklyReportGenerator:
    def test_generates_one_report_per_account(self, setup):
        gen = WeeklyReportGenerator(setup)
        reports = gen.generate(date(2026, 3, 22))
        assert len(reports) == 2

    def test_report_format_contains_required_fields(self, setup):
        gen = WeeklyReportGenerator(setup)
        reports = gen.generate(date(2026, 3, 22))
        report = reports[0]

        assert "Relatório Semanal" in report["report_text"] or "Relatorio Semanal" in report["report_text"]
        assert "Período:" in report["report_text"] or "Periodo:" in report["report_text"]
        assert "Investimento:" in report["report_text"]
        assert "R$" in report["report_text"]

    def test_report_stored_in_gold_table(self, setup):
        gen = WeeklyReportGenerator(setup)
        gen.generate(date(2026, 3, 22))

        with setup.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM gold.generated_reports WHERE report_type = 'weekly'")
            count = cur.fetchone()[0]
        assert count == 2
