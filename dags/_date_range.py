"""Resolução de datas comum às DAGs de extração (RN16)."""

from datetime import date, datetime, timedelta


def resolve_target_dates(
    start_date_str: str | None, end_date_str: str | None
) -> list[date]:
    if not start_date_str and not end_date_str:
        return [datetime.utcnow().date() - timedelta(days=1)]
    if not start_date_str or not end_date_str:
        raise ValueError("Preencha ambos start_date e end_date, ou nenhum")
    start = date.fromisoformat(start_date_str)
    end = date.fromisoformat(end_date_str)
    if end < start:
        raise ValueError("end_date deve ser >= start_date")
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]
