from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query

from src.api.auth import require_api_key
from src.api.dates import lastmonth, parse_range
from src.api.deps import pg
from src.api.reviews.queries import (
    fetch_account_overview,
    fetch_account_performance,
    fetch_budget_pacing_row,
    fetch_campaign_status_rows,
    fetch_negatives_coverage,
    fetch_search_terms_buckets,
    fetch_top_keywords_with_roas,
)
from src.api.reviews.review_flags import evaluate_all
from src.api.reviews.schema import AccountReview, ReviewPeriod

router = APIRouter(
    prefix="/review",
    tags=["review"],
    dependencies=[Depends(require_api_key)],
)


def _resolve_period(start_date: date | None, end_date: date | None) -> tuple[date, date]:
    if start_date is None:
        return lastmonth()
    try:
        return parse_range(start_date, end_date)
    except ValueError as exception:
        raise HTTPException(status_code=400, detail=str(exception)) from exception


def _build_review(connection, account_id: str, start: date, end: date) -> AccountReview:
    account = fetch_account_overview(connection, account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found in gold.daily_performance")
    perf = fetch_account_performance(connection, account_id, start, end)
    campaigns = fetch_campaign_status_rows(connection, account_id, start, end)
    keywords = fetch_top_keywords_with_roas(connection, account_id, start, end)
    terms = fetch_search_terms_buckets(connection, account_id, start, end)
    negatives = fetch_negatives_coverage(connection, account_id, start, end)
    budget = fetch_budget_pacing_row(connection, account_id)
    signals = evaluate_all(perf, budget, campaigns, terms, negatives)
    return AccountReview(
        account=account, period=ReviewPeriod(start=start, end=end),
        performance=perf, campaigns=campaigns, keywords_top=keywords,
        search_terms=terms, negatives=negatives, budget=budget, signals=signals,
    )


@router.get("/{account_id}", response_model=AccountReview)
def get_review(
    account_id: str,
    start_date: date | None = Query(default=None, alias="start-date"),
    end_date: date | None = Query(default=None, alias="end-date"),
    connection=Depends(pg),
) -> AccountReview:
    start, end = _resolve_period(start_date, end_date)
    return _build_review(connection, account_id, start, end)
