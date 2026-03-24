import importlib
import os
import sys

import pytest


DAG_FILES = [
    "daily_extract_meta_ads",
    "daily_extract_google_ads",
    "daily_transform",
    "daily_reports",
    "daily_alerts",
]


class TestDagImports:
    """Verify all DAG files can be imported without errors."""

    @pytest.mark.parametrize("dag_module", DAG_FILES)
    def test_dag_imports_without_error(self, dag_module):
        dags_dir = os.path.join(os.path.dirname(__file__), "..", "..", "dags")
        sys.path.insert(0, dags_dir)
        try:
            mod = importlib.import_module(dag_module)
            assert hasattr(mod, "dag") or hasattr(mod, "DAG_ID")
        finally:
            sys.path.pop(0)


class TestDagProperties:
    @pytest.fixture(autouse=True)
    def _setup_path(self):
        dags_dir = os.path.join(os.path.dirname(__file__), "..", "..", "dags")
        sys.path.insert(0, dags_dir)
        yield
        sys.path.pop(0)

    def test_extract_meta_has_daily_schedule(self):
        mod = importlib.import_module("daily_extract_meta_ads")
        assert mod.dag.schedule == "@daily"

    def test_extract_google_has_daily_schedule(self):
        mod = importlib.import_module("daily_extract_google_ads")
        assert mod.dag.schedule == "@daily"

    def test_transform_has_daily_schedule(self):
        mod = importlib.import_module("daily_transform")
        assert mod.dag.schedule == "@daily"

    def test_reports_has_daily_schedule(self):
        mod = importlib.import_module("daily_reports")
        assert mod.dag.schedule == "@daily"

    def test_alerts_has_daily_schedule(self):
        mod = importlib.import_module("daily_alerts")
        assert mod.dag.schedule == "@daily"
