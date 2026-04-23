from datetime import date

from src.api.platforms.schema import DailyMetricsRow, KeywordRow

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


KEYWORD_COLUMNS = [
    "account_id", "account_name", "campaign_name", "ad_group_name",
    "keyword_text", "match_type", "impressions", "clicks", "spend", "conversions",
]


def keywords_range(
    connection,
    start: date,
    end: date,
    account_id: str | None = None,
) -> list[KeywordRow]:
    query = """
        SELECT customer_id, MAX(customer_name), MAX(campaign_name), MAX(ad_group_name),
               keyword_text, MAX(match_type),
               CAST(SUM(impressions) AS BIGINT),
               CAST(SUM(clicks) AS BIGINT),
               CAST(SUM(spend) AS DOUBLE PRECISION),
               CAST(SUM(conversions) AS DOUBLE PRECISION)
        FROM bronze.google_ads_raw
        WHERE date BETWEEN %s AND %s
          AND keyword_text IS NOT NULL AND keyword_text <> ''
    """
    parameters: list = [start, end]
    if account_id:
        query += " AND customer_id = %s"
        parameters.append(account_id)
    query += (
        " GROUP BY customer_id, keyword_text, ad_group_name, campaign_name"
        " ORDER BY customer_id, SUM(impressions) DESC"
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, parameters)
            rows = cursor.fetchall()
    except Exception:
        return []
    return [KeywordRow(**dict(zip(KEYWORD_COLUMNS, row, strict=True))) for row in rows]
