import os

import pytest

from src.db.schema import initialize_schemas
from src.transformers.sql_runner import SQLRunner


@pytest.fixture
def runner(memory_connection, tmp_path):
    initialize_schemas(memory_connection)
    return SQLRunner(memory_connection, str(tmp_path))


class TestSQLRunner:
    def test_executes_sql_from_file(self, runner, memory_connection, tmp_path):
        sql_dir = tmp_path / "silver"
        sql_dir.mkdir()
        (sql_dir / "test.sql").write_text(
            "CREATE OR REPLACE VIEW silver.test_view AS SELECT 1 AS val"
        )

        runner.run_silver()

        result = memory_connection.execute(
            "SELECT val FROM silver.test_view"
        ).fetchone()
        assert result[0] == 1

    def test_executes_in_alphabetical_order(self, runner, memory_connection, tmp_path):
        sql_dir = tmp_path / "silver"
        sql_dir.mkdir()
        # b.sql runs first, creates table; a.sql would fail if it ran first
        (sql_dir / "01_first.sql").write_text(
            "CREATE OR REPLACE VIEW silver.step1 AS SELECT 1 AS step"
        )
        (sql_dir / "02_second.sql").write_text(
            "CREATE OR REPLACE VIEW silver.step2 AS SELECT step + 1 AS step FROM silver.step1"
        )

        runner.run_silver()

        result = memory_connection.execute(
            "SELECT step FROM silver.step2"
        ).fetchone()
        assert result[0] == 2

    def test_run_all_executes_silver_then_gold(self, runner, memory_connection, tmp_path):
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        (silver_dir / "01.sql").write_text(
            "CREATE OR REPLACE VIEW silver.base AS SELECT 10 AS val"
        )

        gold_dir = tmp_path / "gold"
        gold_dir.mkdir()
        (gold_dir / "01.sql").write_text(
            "CREATE OR REPLACE VIEW gold.agg AS SELECT val * 2 AS val FROM silver.base"
        )

        runner.run_all()

        result = memory_connection.execute(
            "SELECT val FROM gold.agg"
        ).fetchone()
        assert result[0] == 20

    def test_is_idempotent(self, runner, memory_connection, tmp_path):
        sql_dir = tmp_path / "silver"
        sql_dir.mkdir()
        (sql_dir / "test.sql").write_text(
            "CREATE OR REPLACE VIEW silver.idem AS SELECT 1 AS val"
        )

        runner.run_silver()
        runner.run_silver()  # second run should not error

        result = memory_connection.execute(
            "SELECT val FROM silver.idem"
        ).fetchone()
        assert result[0] == 1

    def test_handles_sql_error(self, runner, tmp_path):
        sql_dir = tmp_path / "silver"
        sql_dir.mkdir()
        (sql_dir / "bad.sql").write_text("SELECT * FROM nonexistent_table")

        with pytest.raises(Exception):
            runner.run_silver()
