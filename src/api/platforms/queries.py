from datetime import date

from src.api.platforms.schema import DailyMetricsRow

COLUMN_LIST = (
    "account_id, account_name, date, platform, impressions, clicks, spend, "
    "conversions, ctr, cpc, cost_per_conversion, conversion_rate"
)
COLUMN_KEYS = [
    "account_id", "account_name", "date", "platform", "impressions", "clicks", "spend",
    "conversions", "ctr", "cpc", "cost_per_conversion", "conversion_rate",
]


def daily_range(
    connection,
    platform_db: str,
    start: date,
    end: date,
    account_id: str | None = None,
) -> list[DailyMetricsRow]:
    query = (
        f"SELECT {COLUMN_LIST} FROM gold.daily_performance "
        "WHERE platform = %s AND date BETWEEN %s AND %s"
    )
    parameters: list = [platform_db, start, end]
    if account_id:
        query += " AND account_id = %s"
        parameters.append(account_id)
    query += " ORDER BY date, account_id"
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, parameters)
            rows = cursor.fetchall()
    except Exception:
        return []
    return [DailyMetricsRow(**dict(zip(COLUMN_KEYS, row, strict=True))) for row in rows]
