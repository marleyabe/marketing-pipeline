"""Base compartilhada das DAGs de extração (load + orquestração)."""

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Callable

from airflow.models.param import Param

from dags._date_range import resolve_target_dates
from dags.callbacks.discord_alert import notify_discord_failure
from src.db import get_pg, init_schemas
from src.loader import load_bronze

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExtractorSpec:
    platform: str
    bronze_table: str
    conflict_keys: list[str]
    init_api: Callable[[], Any]
    list_accounts: Callable[[Any], list[str]]
    fetch_date: Callable[[Any, list[str], str], list[dict]]


def default_args() -> dict:
    return {
        "owner": "data-eng",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": notify_discord_failure,
    }


def date_range_params() -> dict:
    return {
        "start_date": Param(default=None, type=["null", "string"], format="date"),
        "end_date": Param(default=None, type=["null", "string"], format="date"),
    }


def load_to_bronze(rows: list[dict], spec: ExtractorSpec) -> int:
    connection = get_pg()
    init_schemas(connection)
    try:
        return load_bronze(
            connection, rows, spec.bronze_table,
            source=spec.platform, conflict_columns=spec.conflict_keys,
        )
    finally:
        connection.close()


def run_extraction(spec: ExtractorSpec, context: dict) -> int:
    target_dates = resolve_target_dates(
        context["params"].get("start_date"),
        context["params"].get("end_date"),
    )
    logger.info("%s target_dates=%s", spec.platform, [d.isoformat() for d in target_dates])
    api_context = spec.init_api()
    accounts = spec.list_accounts(api_context)
    logger.info("%s accounts=%d", spec.platform, len(accounts))
    rows: list[dict] = []
    for target_date in target_dates:
        rows.extend(spec.fetch_date(api_context, accounts, target_date.isoformat()))
    loaded = load_to_bronze(rows, spec)
    logger.info("%s loaded rows=%d", spec.platform, loaded)
    return loaded


def run_snapshot_extraction(spec: ExtractorSpec) -> int:
    """Para recursos que não têm granularidade de data na API (ex.: negativas).

    Extrai o estado atual uma única vez e carimba snapshot_date = hoje.
    Exemplo: run_snapshot_extraction(NEGATIVES_SPEC)
    """
    api_context = spec.init_api()
    accounts = spec.list_accounts(api_context)
    snapshot_date = date.today().isoformat()
    logger.info("%s snapshot accounts=%d date=%s", spec.platform, len(accounts), snapshot_date)
    rows = spec.fetch_date(api_context, accounts, snapshot_date)
    loaded = load_to_bronze(rows, spec)
    logger.info("%s snapshot loaded rows=%d", spec.platform, loaded)
    return loaded
