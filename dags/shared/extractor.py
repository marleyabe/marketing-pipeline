"""Base compartilhada das DAGs de extração (load + orquestração).

Backfill conservador (feat/google-backfill-resiliente):
- Commit por partição (data, conta): falha parcial preserva partições já
  carregadas, graças à idempotência via UPSERT nas chaves naturais.
- Backoff+jitter em erros transientes (ver dags/_retry.py).
- Timeout configurável via EXTRACTOR_TASK_TIMEOUT_HOURS (default 6h).
- Falhas por partição são coletadas e re-emitidas no fim: maximiza o volume
  de dados trazidos numa única execução.
"""

import logging
import os
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Callable

from airflow.models.param import Param

from dags.shared.date_range import resolve_target_dates
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


def _task_timeout() -> timedelta:
    hours = float(os.environ.get("EXTRACTOR_TASK_TIMEOUT_HOURS", "6"))
    return timedelta(hours=hours)


def default_args() -> dict:
    return {
        "owner": "data-eng",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "execution_timeout": _task_timeout(),
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


def _extract_partition(
    spec: ExtractorSpec, api_context: Any, account: str, date_iso: str,
) -> int:
    rows = spec.fetch_date(api_context, [account], date_iso)
    return load_to_bronze(rows, spec)


def _run_partitions(
    spec: ExtractorSpec,
    api_context: Any,
    accounts: list[str],
    dates_iso: list[str],
) -> int:
    total_loaded = 0
    failed: list[tuple[str, str]] = []
    total = len(dates_iso) * len(accounts)
    done = 0
    for date_iso in dates_iso:
        for account in accounts:
            done += 1
            try:
                loaded = _extract_partition(spec, api_context, account, date_iso)
                total_loaded += loaded
                logger.info(
                    "%s partition date=%s account=%s loaded=%d progress=%d/%d",
                    spec.platform, date_iso, account, loaded, done, total,
                )
            except Exception:  # noqa: BLE001 — continuar partições seguintes
                # Coletamos e seguimos. Airflow retry reprocessa tudo; UPSERT
                # é idempotente, então partições OK não duplicam.
                logger.exception(
                    "%s partition date=%s account=%s FAILED",
                    spec.platform, date_iso, account,
                )
                failed.append((date_iso, account))
    if failed:
        sample = ", ".join(f"{d}/{a}" for d, a in failed[:5])
        raise RuntimeError(
            f"{spec.platform} backfill incompleto: {len(failed)}/{total} "
            f"partições falharam (amostra: {sample})",
        )
    return total_loaded


def run_extraction(spec: ExtractorSpec, context: dict) -> int:
    target_dates = resolve_target_dates(
        context["params"].get("start_date"),
        context["params"].get("end_date"),
    )
    dates_iso = [d.isoformat() for d in target_dates]
    logger.info("%s target_dates=%s", spec.platform, dates_iso)
    api_context = spec.init_api()
    accounts = spec.list_accounts(api_context)
    logger.info("%s accounts=%d dates=%d", spec.platform, len(accounts), len(dates_iso))
    loaded = _run_partitions(spec, api_context, accounts, dates_iso)
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
    loaded = _run_partitions(spec, api_context, accounts, [snapshot_date])
    logger.info("%s snapshot loaded rows=%d", spec.platform, loaded)
    return loaded
