import os
from datetime import date

import pytest

from src.db.schema import initialize_schemas
from src.loaders.duckdb_loader import DuckDBBronzeLoader
from src.reports.daily import DailyReportGenerator
from src.transformers.sql_runner import SQLRunner

SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "sql")


@pytest.fixture
def setup(memory_connection, sample_meta_ads_data, sample_google_ads_data):
    initialize_schemas(memory_connection)
    loader = DuckDBBronzeLoader(memory_connection)
    loader.load(sample_meta_ads_data, "meta_ads_raw", source="meta_ads")
    loader.load(sample_google_ads_data, "google_ads_raw", source="google_ads")
    SQLRunner(memory_connection, SQL_DIR).run_all()

    # Create generated_reports table
    memory_connection.execute("""
        CREATE TABLE IF NOT EXISTS gold.generated_reports (
            account_id VARCHAR,
            account_name VARCHAR,
            report_type VARCHAR,
            report_date DATE,
            report_text VARCHAR,
            generated_at TIMESTAMP DEFAULT current_timestamp
        )
    """)
    return memory_connection


class TestDailyReportGenerator:
    def test_generates_one_report_per_account(self, setup):
        gen = DailyReportGenerator(setup)
        reports = gen.generate(date(2026, 3, 22))
        assert len(reports) == 2

    def test_report_format_contains_required_fields(self, setup):
        gen = DailyReportGenerator(setup)
        reports = gen.generate(date(2026, 3, 22))
        report = reports[0]

        assert "Data:" in report["report_text"]
        assert "Investimento:" in report["report_text"]
        assert "R$" in report["report_text"]
        assert "Impressões:" in report["report_text"] or "Impressoes:" in report["report_text"]
        assert "Cliques:" in report["report_text"]
        assert "Conversões:" in report["report_text"] or "Conversoes:" in report["report_text"]
        assert "%" in report["report_text"]

    def test_report_stored_in_gold_table(self, setup):
        gen = DailyReportGenerator(setup)
        gen.generate(date(2026, 3, 22))

        count = setup.execute(
            "SELECT COUNT(*) FROM gold.generated_reports WHERE report_type = 'daily'"
        ).fetchone()[0]
        assert count == 2

    def test_report_date_format_dd_mm_yyyy(self, setup):
        gen = DailyReportGenerator(setup)
        reports = gen.generate(date(2026, 3, 22))
        assert "22/03/2026" in reports[0]["report_text"]
