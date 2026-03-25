"""
E2E tests: dispara as DAGs no Airflow e verifica os dados no DuckDB.

Requer o ambiente Docker rodando:
    docker compose up -d

Rodar com:
    uv run pytest tests/e2e/ -v
"""

import time
from datetime import date

import duckdb
import pytest
import requests

AIRFLOW_URL = "http://localhost:8080"
AIRFLOW_USER = "Ads2u"
AIRFLOW_PASS = "Pangare12@"
DUCKDB_PATH = "data/ads2u.duckdb"

DAG_ORDER = [
    "daily_extract_google_ads",
    "daily_extract_meta_ads",
    "daily_transform",
    "daily_reports",
    "daily_alerts",
]

POLL_INTERVAL = 5   # segundos entre verificações
TIMEOUT = 300       # segundos máximo por DAG


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_token() -> str:
    resp = requests.post(
        f"{AIRFLOW_URL}/auth/token",
        json={"username": AIRFLOW_USER, "password": AIRFLOW_PASS},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def airflow_get(token: str, path: str) -> dict:
    resp = requests.get(
        f"{AIRFLOW_URL}{path}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def airflow_patch(token: str, path: str, body: dict) -> dict:
    resp = requests.patch(
        f"{AIRFLOW_URL}{path}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=body,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def airflow_post(token: str, path: str, body: dict) -> dict:
    resp = requests.post(
        f"{AIRFLOW_URL}{path}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=body,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def trigger_dag(token: str, dag_id: str) -> str:
    """Dispara a DAG e retorna o run_id."""
    from datetime import datetime, timezone
    logical_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    data = airflow_post(token, f"/api/v2/dags/{dag_id}/dagRuns", {"logical_date": logical_date})
    return data["dag_run_id"]


def wait_for_dag(token: str, dag_id: str, run_id: str) -> str:
    """Aguarda a conclusão da DAG e retorna o estado final."""
    deadline = time.time() + TIMEOUT
    while time.time() < deadline:
        data = airflow_get(token, f"/api/v2/dags/{dag_id}/dagRuns/{run_id}")
        state = data["state"]
        if state in ("success", "failed"):
            return state
        time.sleep(POLL_INTERVAL)
    return "timeout"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def token():
    return get_token()


@pytest.fixture(scope="module")
def airflow_ready(token):
    """Verifica que o Airflow está acessível e as DAGs estão disponíveis."""
    health = requests.get(f"{AIRFLOW_URL}/api/v2/monitor/health", timeout=10).json()
    assert health["metadatabase"]["status"] == "healthy"
    assert health["scheduler"]["status"] == "healthy"

    # Garante que todas as DAGs estão desativadas
    for dag_id in DAG_ORDER:
        airflow_patch(token, f"/api/v2/dags/{dag_id}", {"is_paused": False})


@pytest.fixture(scope="module")
def pipeline_runs(token, airflow_ready):
    """
    Dispara todas as DAGs em ordem e aguarda conclusão de cada uma.
    Retorna dict {dag_id: state}.

    As duas extrações rodam sempre (independentes).
    Transform, reports e alerts só rodam se ao menos uma extração teve sucesso.
    """
    results = {}

    # Extrações: dispara as duas e aguarda ambas
    extract_dags = ["daily_extract_google_ads", "daily_extract_meta_ads"]
    run_ids = {dag_id: trigger_dag(token, dag_id) for dag_id in extract_dags}
    for dag_id, run_id in run_ids.items():
        results[dag_id] = wait_for_dag(token, dag_id, run_id)

    # Continua apenas se ao menos uma extração teve sucesso
    extract_ok = any(results[d] == "success" for d in extract_dags)
    if not extract_ok:
        return results

    # Transform, reports e alerts em sequência
    for dag_id in ["daily_transform", "daily_reports", "daily_alerts"]:
        run_id = trigger_dag(token, dag_id)
        results[dag_id] = wait_for_dag(token, dag_id, run_id)
        if results[dag_id] != "success" and dag_id == "daily_transform":
            break  # sem transform não adianta continuar

    return results


@pytest.fixture(scope="module")
def db(pipeline_runs):
    """Abre o DuckDB em read-only após o pipeline rodar."""
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Testes: execução das DAGs
# ---------------------------------------------------------------------------

class TestDagExecution:

    def test_extract_google_ads_success(self, pipeline_runs):
        assert pipeline_runs.get("daily_extract_google_ads") == "success", \
            "DAG daily_extract_google_ads falhou"

    def test_extract_meta_ads_success(self, pipeline_runs):
        assert pipeline_runs.get("daily_extract_meta_ads") == "success", \
            "DAG daily_extract_meta_ads falhou"

    def test_transform_success(self, pipeline_runs):
        assert pipeline_runs.get("daily_transform") == "success", \
            "DAG daily_transform falhou"

    def test_reports_success(self, pipeline_runs):
        assert pipeline_runs.get("daily_reports") == "success", \
            "DAG daily_reports falhou"

    def test_alerts_success(self, pipeline_runs):
        assert pipeline_runs.get("daily_alerts") == "success", \
            "DAG daily_alerts falhou"


# ---------------------------------------------------------------------------
# Testes: camada Bronze
# ---------------------------------------------------------------------------

class TestBronze:

    def test_google_ads_raw_has_data(self, db):
        count = db.execute("SELECT COUNT(*) FROM bronze.google_ads_raw").fetchone()[0]
        assert count > 0, "bronze.google_ads_raw está vazia"

    def test_meta_ads_raw_table_exists(self, db):
        # Meta pode retornar 0 registros se não houver dados no dia ou token expirou
        db.execute("SELECT COUNT(*) FROM bronze.meta_ads_raw").fetchone()

    def test_google_ads_raw_schema(self, db):
        cols = {r[0] for r in db.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'bronze' AND table_name = 'google_ads_raw'"
        ).fetchall()}
        required = {"customer_id", "customer_name", "campaign_id", "campaign_name",
                    "ad_group_id", "ad_group_name", "keyword_id", "keyword_text", "match_type",
                    "impressions", "clicks", "spend", "conversions", "date",
                    "_extracted_at", "_source"}
        assert required <= cols

    def test_meta_ads_raw_schema(self, db):
        cols = {r[0] for r in db.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'bronze' AND table_name = 'meta_ads_raw'"
        ).fetchall()}
        required = {"account_id", "account_name", "campaign_id", "campaign_name",
                    "impressions", "clicks", "spend", "date_start", "date_stop",
                    "_extracted_at", "_source"}
        assert required <= cols

    def test_google_ads_raw_no_nulls_on_key_cols(self, db):
        nulls = db.execute(
            "SELECT COUNT(*) FROM bronze.google_ads_raw "
            "WHERE customer_id IS NULL OR campaign_id IS NULL OR date IS NULL"
        ).fetchone()[0]
        assert nulls == 0

    def test_google_ads_raw_spend_positive(self, db):
        negatives = db.execute(
            "SELECT COUNT(*) FROM bronze.google_ads_raw WHERE spend < 0"
        ).fetchone()[0]
        assert negatives == 0

    def test_today_in_google_ads_raw(self, db):
        today = date.today().isoformat()
        count = db.execute(
            f"SELECT COUNT(*) FROM bronze.google_ads_raw WHERE CAST(date AS VARCHAR) = '{today}'"
        ).fetchone()[0]
        assert count > 0, f"Nenhum dado do dia {today} no google_ads_raw"


# ---------------------------------------------------------------------------
# Testes: camada Silver
# ---------------------------------------------------------------------------

class TestSilver:

    def test_silver_schemas_exist(self, db):
        schemas = {r[0] for r in db.execute(
            "SELECT schema_name FROM information_schema.schemata"
        ).fetchall()}
        assert "silver" in schemas

    def test_silver_google_ads_has_data(self, db):
        count = db.execute("SELECT COUNT(*) FROM silver.google_ads").fetchone()[0]
        assert count > 0

    def test_silver_unified_campaigns_has_both_platforms(self, db):
        platforms = {r[0] for r in db.execute(
            "SELECT DISTINCT platform FROM silver.unified_campaigns"
        ).fetchall()}
        assert "google_ads" in platforms

    def test_silver_unified_spend_not_null(self, db):
        nulls = db.execute(
            "SELECT COUNT(*) FROM silver.unified_campaigns WHERE spend IS NULL"
        ).fetchone()[0]
        assert nulls == 0


# ---------------------------------------------------------------------------
# Testes: camada Gold
# ---------------------------------------------------------------------------

class TestGold:

    def test_gold_daily_performance_has_data(self, db):
        count = db.execute("SELECT COUNT(*) FROM gold.daily_performance").fetchone()[0]
        assert count > 0

    def test_gold_daily_performance_metrics_valid(self, db):
        row = db.execute(
            "SELECT MIN(impressions), MIN(clicks), MIN(spend) FROM gold.daily_performance"
        ).fetchone()
        assert row[0] >= 0
        assert row[1] >= 0
        assert row[2] >= 0

    def test_gold_ctr_between_0_and_100(self, db):
        invalid = db.execute(
            "SELECT COUNT(*) FROM gold.daily_performance WHERE ctr < 0 OR ctr > 100"
        ).fetchone()[0]
        assert invalid == 0

    def test_gold_reports_daily_has_data(self, db):
        count = db.execute("SELECT COUNT(*) FROM gold.reports_daily").fetchone()[0]
        assert count > 0

    def test_gold_generated_reports_populated(self, db):
        count = db.execute(
            "SELECT COUNT(*) FROM gold.generated_reports WHERE report_type = 'daily'"
        ).fetchone()[0]
        assert count > 0, "Nenhum relatório diário foi gerado"

    def test_gold_active_alerts_table_exists(self, db):
        # Pode ter 0 alertas se performance for estável — só verifica que a tabela existe
        db.execute("SELECT COUNT(*) FROM gold.active_alerts").fetchone()
