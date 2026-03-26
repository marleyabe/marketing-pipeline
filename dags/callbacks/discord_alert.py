import os

import requests


def send_discord_alert(context: dict) -> None:
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        return

    dag_id = context["dag"].dag_id
    run_id = context.get("run_id", "—")

    dag_run = context.get("dag_run")
    execution_date = (
        dag_run.logical_date.strftime("%Y-%m-%d %H:%M UTC")
        if dag_run and dag_run.logical_date
        else "—"
    )

    message = {
        "content": (
            f"🔴 **Falha no pipeline**\n"
            f"**DAG:** `{dag_id}`\n"
            f"**Run ID:** `{run_id}`\n"
            f"**Execução:** {execution_date}"
        )
    }

    try:
        requests.post(webhook_url, json=message, timeout=10)
    except Exception:
        pass
