"""Callback de falha enviando alerta para Discord via webhook."""

import json
import logging
import os
from urllib import request as urllib_request
from urllib.error import URLError

logger = logging.getLogger(__name__)

DISCORD_WEBHOOK_ENV = "DISCORD_WEBHOOK_URL"
HTTP_TIMEOUT_SECONDS = 10
MAX_FIELD_LENGTH = 1000


def _truncate(value: str, limit: int = MAX_FIELD_LENGTH) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def _build_payload(context: dict) -> dict:
    task_instance = context.get("task_instance")
    dag_id = getattr(task_instance, "dag_id", "unknown_dag") if task_instance else "unknown_dag"
    task_id = getattr(task_instance, "task_id", "unknown_task") if task_instance else "unknown_task"
    run_id = getattr(task_instance, "run_id", "unknown_run") if task_instance else "unknown_run"
    try_number = getattr(task_instance, "try_number", "?") if task_instance else "?"
    log_url = getattr(task_instance, "log_url", "") if task_instance else ""
    exception = context.get("exception")
    reason = _truncate(str(exception)) if exception else "Sem exceção capturada"
    fields = [
        {"name": "DAG", "value": str(dag_id), "inline": True},
        {"name": "Task", "value": str(task_id), "inline": True},
        {"name": "Tentativa", "value": str(try_number), "inline": True},
        {"name": "Run", "value": _truncate(str(run_id)), "inline": False},
        {"name": "Erro", "value": f"```{reason}```", "inline": False},
    ]
    if log_url:
        fields.append({"name": "Logs", "value": log_url, "inline": False})
    return {
        "username": "Airflow Alerts",
        "embeds": [
            {
                "title": f"Falha na DAG {dag_id}",
                "color": 15158332,
                "fields": fields,
            }
        ],
    }


def notify_discord_failure(context: dict) -> None:
    webhook_url = os.environ.get(DISCORD_WEBHOOK_ENV)
    if not webhook_url:
        logger.warning("DISCORD_WEBHOOK_URL não configurado; alerta ignorado.")
        return
    payload = _build_payload(context)
    body = json.dumps(payload).encode("utf-8")
    request = urllib_request.Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib_request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            if response.status >= 300:
                logger.error("Discord webhook retornou status %s", response.status)
    except URLError as error:
        logger.error("Falha enviando alerta Discord: %s", error)
