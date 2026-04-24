"""SQL específico da revisão de conta. Cada função responde por uma seção do JSON.

Todas recebem `connection` injetada (DI) para permitir FakePgConnection nos testes.
SQL vive em constantes no topo para manter as funções dentro de 20 linhas (CLAUDE.md)
e permitir inspeção sem ler a lógica de parse.
"""

from datetime import date

from src.api.reviews.schema import (
    AccountOverview,
    AccountPerformance,
    BudgetPacing,
    CampaignRow,
    KeywordRow,
    NegativesCoverage,
    SearchTermBuckets,
    SearchTermRow,
)

TOP_KEYWORDS_LIMIT = 20
TOP_SEARCH_TERMS_LIMIT = 15

SQL_ACCOUNT_OVERVIEW = (
    "SELECT account_id, MAX(account_name), MIN(platform) "
    "FROM gold.daily_performance WHERE account_id = %s GROUP BY account_id"
)

SQL_ACCOUNT_PERFORMANCE = """
    SELECT
        COALESCE(SUM(impressions), 0),
        COALESCE(SUM(clicks), 0),
        COALESCE(SUM(spend), 0.0),
        COALESCE(SUM(conversions), 0.0),
        COALESCE(SUM(conversion_value), 0.0)
    FROM gold.daily_performance
    WHERE account_id = %s AND date BETWEEN %s AND %s
"""

SQL_CAMPAIGN_ROWS = """
    SELECT
        campaign_name,
        MAX(campaign_status) AS status,
        SUM(spend), SUM(conversions), SUM(conversion_value)
    FROM gold.google_ads_keywords
    WHERE account_id = %s AND date BETWEEN %s AND %s
    GROUP BY campaign_name
    ORDER BY SUM(spend) DESC
"""

SQL_TOP_KEYWORDS = """
    SELECT keyword_text, MAX(match_type),
           MAX(campaign_name), MAX(ad_group_name),
           SUM(impressions), SUM(clicks), SUM(spend),
           SUM(conversions), SUM(conversion_value),
           AVG(quality_score)
    FROM gold.google_ads_keywords
    WHERE account_id = %s AND date BETWEEN %s AND %s
    GROUP BY keyword_text
    ORDER BY SUM(spend) DESC
    LIMIT %s
"""

SQL_SEARCH_TERMS_TEMPLATE = """
    SELECT search_term, MAX(campaign_name), MAX(ad_group_name),
           MAX(matched_keyword_text), MAX(matched_keyword_match_type),
           MAX(search_term_status),
           SUM(impressions), SUM(clicks), SUM(spend),
           SUM(conversions), SUM(conversion_value)
    FROM gold.google_search_terms
    WHERE account_id = %s AND date BETWEEN %s AND %s
    GROUP BY search_term
    {having}
    ORDER BY {order}
    LIMIT %s
"""

SQL_NEGATIVES_COUNT = "SELECT COUNT(*) FROM gold.google_negatives WHERE account_id = %s"

# Uma campanha está "sem negativas" se nenhuma linha em gold.google_negatives
# (seja scope campaign ou ad_group) referencia o campaign_name dela.
SQL_CAMPAIGNS_WITHOUT_NEGATIVES = """
    SELECT DISTINCT k.campaign_name
    FROM gold.google_ads_keywords k
    WHERE k.account_id = %s AND k.date BETWEEN %s AND %s
      AND NOT EXISTS (
          SELECT 1 FROM gold.google_negatives n
          WHERE n.account_id = k.account_id
            AND n.campaign_name = k.campaign_name
      )
    ORDER BY k.campaign_name
"""

SQL_BUDGET_PACING = """
    SELECT monthly_budget, currency, spent_mtd,
           pct_consumed, days_elapsed_pct, pace_flag
    FROM gold.budget_pacing
    WHERE account_id = %s
    ORDER BY platform LIMIT 1
"""


def fetch_account_overview(connection, account_id: str) -> AccountOverview | None:
    """Retorna identificação e plataforma da conta em gold.daily_performance."""
    with connection.cursor() as cursor:
        cursor.execute(SQL_ACCOUNT_OVERVIEW, [account_id])
        row = cursor.fetchone()
    if row is None:
        return None
    return AccountOverview(account_id=row[0], account_name=row[1], platform=row[2])


def fetch_account_performance(
    connection, account_id: str, start: date, end: date,
) -> AccountPerformance:
    """Métricas agregadas do período. Exemplo: (..., date(2026,4,1), date(2026,4,22))."""
    with connection.cursor() as cursor:
        cursor.execute(SQL_ACCOUNT_PERFORMANCE, [account_id, start, end])
        row = cursor.fetchone()
    impressions, clicks, spend, conversions, conversion_value = row
    return _compose_performance(
        int(impressions or 0), int(clicks or 0),
        float(spend or 0), float(conversions or 0), float(conversion_value or 0),
    )


def _compose_performance(
    impressions: int, clicks: int, spend: float,
    conversions: float, conversion_value: float,
) -> AccountPerformance:
    ctr = (clicks / impressions * 100) if impressions else None
    cpc = (spend / clicks) if clicks else None
    cpa = (spend / conversions) if conversions else None
    roas = (conversion_value / spend) if spend else None
    return AccountPerformance(
        impressions=impressions, clicks=clicks, spend=spend,
        conversions=conversions, conversion_value=conversion_value,
        ctr=round(ctr, 2) if ctr is not None else None,
        cpc=round(cpc, 2) if cpc is not None else None,
        cpa=round(cpa, 2) if cpa is not None else None,
        roas=round(roas, 4) if roas is not None else None,
    )


def fetch_campaign_status_rows(
    connection, account_id: str, start: date, end: date,
) -> list[CampaignRow]:
    """Uma linha por campanha no período com status e ROAS."""
    with connection.cursor() as cursor:
        cursor.execute(SQL_CAMPAIGN_ROWS, [account_id, start, end])
        rows = cursor.fetchall()
    return [_campaign_row(row) for row in rows]


def _campaign_row(row: tuple) -> CampaignRow:
    name, status, spend, conversions, conversion_value = row
    spend_f = float(spend or 0)
    value_f = float(conversion_value or 0)
    roas = (value_f / spend_f) if spend_f else None
    return CampaignRow(
        campaign_name=name, status=status,
        spend=spend_f, conversions=float(conversions or 0),
        conversion_value=value_f,
        roas=round(roas, 4) if roas is not None else None,
    )


def fetch_top_keywords_with_roas(
    connection, account_id: str, start: date, end: date,
) -> list[KeywordRow]:
    """Top keywords por spend com ROAS e quality_score. Limite = TOP_KEYWORDS_LIMIT."""
    with connection.cursor() as cursor:
        cursor.execute(SQL_TOP_KEYWORDS, [account_id, start, end, TOP_KEYWORDS_LIMIT])
        rows = cursor.fetchall()
    return [_keyword_row(row) for row in rows]


def _keyword_row(row: tuple) -> KeywordRow:
    (keyword_text, match_type, campaign_name, ad_group_name,
     impressions, clicks, spend, conversions, conversion_value, quality_score) = row
    spend_f = float(spend or 0)
    value_f = float(conversion_value or 0)
    roas = (value_f / spend_f) if spend_f else None
    return KeywordRow(
        keyword_text=keyword_text, match_type=match_type,
        campaign_name=campaign_name, ad_group_name=ad_group_name,
        impressions=int(impressions or 0), clicks=int(clicks or 0),
        spend=spend_f, conversions=float(conversions or 0),
        conversion_value=value_f,
        roas=round(roas, 4) if roas is not None else None,
        quality_score=float(quality_score) if quality_score is not None else None,
    )


def fetch_search_terms_buckets(
    connection, account_id: str, start: date, end: date,
) -> SearchTermBuckets:
    """Dois buckets: top spend sem conversão (candidatos a negativar) e top ROAS."""
    wasted = _fetch_search_terms(connection, account_id, start, end, only_zero_conv=True)
    winners = _fetch_search_terms(connection, account_id, start, end, only_zero_conv=False)
    return SearchTermBuckets(
        top_by_spend_no_conv=wasted,
        high_roas=[row for row in winners if row.roas and row.roas > 1.0],
    )


def _fetch_search_terms(
    connection, account_id: str, start: date, end: date, only_zero_conv: bool,
) -> list[SearchTermRow]:
    having = "HAVING SUM(conversions) = 0" if only_zero_conv else ""
    order = "SUM(spend) DESC" if only_zero_conv else (
        "(SUM(conversion_value) / NULLIF(SUM(spend), 0)) DESC NULLS LAST"
    )
    query = SQL_SEARCH_TERMS_TEMPLATE.format(having=having, order=order)
    with connection.cursor() as cursor:
        cursor.execute(query, [account_id, start, end, TOP_SEARCH_TERMS_LIMIT])
        rows = cursor.fetchall()
    return [_search_term_row(row) for row in rows]


def _search_term_row(row: tuple) -> SearchTermRow:
    (search_term, campaign_name, ad_group_name,
     matched_keyword_text, matched_keyword_match_type, status,
     impressions, clicks, spend, conversions, conversion_value) = row
    spend_f = float(spend or 0)
    value_f = float(conversion_value or 0)
    roas = (value_f / spend_f) if spend_f else None
    return SearchTermRow(
        search_term=search_term,
        campaign_name=campaign_name, ad_group_name=ad_group_name,
        matched_keyword_text=matched_keyword_text,
        matched_keyword_match_type=matched_keyword_match_type,
        status=status,
        impressions=int(impressions or 0), clicks=int(clicks or 0),
        spend=spend_f, conversions=float(conversions or 0),
        roas=round(roas, 4) if roas is not None else None,
    )


def fetch_negatives_coverage(
    connection, account_id: str, start: date, end: date,
) -> NegativesCoverage:
    """Conta total de negativas e lista campanhas com spend no período sem nenhuma."""
    total = _count_negatives(connection, account_id)
    uncovered = _campaigns_without_negatives(connection, account_id, start, end)
    return NegativesCoverage(total_negatives=total, campaigns_without_negatives=uncovered)


def _count_negatives(connection, account_id: str) -> int:
    with connection.cursor() as cursor:
        cursor.execute(SQL_NEGATIVES_COUNT, [account_id])
        row = cursor.fetchone()
    return int(row[0]) if row else 0


def _campaigns_without_negatives(
    connection, account_id: str, start: date, end: date,
) -> list[str]:
    with connection.cursor() as cursor:
        cursor.execute(SQL_CAMPAIGNS_WITHOUT_NEGATIVES, [account_id, start, end])
        rows = cursor.fetchall()
    return [row[0] for row in rows if row[0]]


def fetch_budget_pacing_row(connection, account_id: str) -> BudgetPacing:
    """Linha de gold.budget_pacing. Sem cadastro ⇒ pace_flag = 'unknown'."""
    with connection.cursor() as cursor:
        cursor.execute(SQL_BUDGET_PACING, [account_id])
        row = cursor.fetchone()
    return _pacing_row(row) if row is not None else _pacing_absent()


def _pacing_absent() -> BudgetPacing:
    return BudgetPacing(
        monthly_budget=None, currency=None, spent_mtd=0.0,
        pct_consumed=None, days_elapsed_pct=None, pace_flag="unknown",
    )


def _pacing_row(row: tuple) -> BudgetPacing:
    monthly_budget, currency, spent_mtd, pct_consumed, days_elapsed_pct, pace_flag = row
    return BudgetPacing(
        monthly_budget=float(monthly_budget) if monthly_budget is not None else None,
        currency=currency,
        spent_mtd=float(spent_mtd or 0),
        pct_consumed=float(pct_consumed) if pct_consumed is not None else None,
        days_elapsed_pct=float(days_elapsed_pct) if days_elapsed_pct is not None else None,
        pace_flag=pace_flag,
    )
