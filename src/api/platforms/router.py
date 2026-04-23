from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query

from src.api.auth import require_api_key
from src.api.dates import lastmonth, lastweek, parse_range, yesterday
from src.api.deps import pg
from src.api.platforms.queries import daily_range, keywords_range
from src.api.platforms.schema import SLUG_TO_DB, DailyMetricsRow, KeywordRow


def _parse_or_400(start_date: date, end_date: date | None) -> tuple[date, date]:
    try:
        return parse_range(start_date, end_date)
    except ValueError as exception:
        raise HTTPException(status_code=400, detail=str(exception)) from exception


def _register_keywords_route(router: APIRouter) -> None:
    @router.get("/keywords", response_model=list[KeywordRow])
    def _keywords(
        start_date: date = Query(..., alias="start-date"),
        end_date: date | None = Query(default=None, alias="end-date"),
        account_id: str | None = None,
        connection=Depends(pg),
    ):
        start, end = _parse_or_400(start_date, end_date)
        return keywords_range(connection, start, end, account_id)


def _register_window_routes(router: APIRouter, platform_db: str) -> None:
    windows = {"yesterday": yesterday, "lastweek": lastweek, "lastmonth": lastmonth}
    for path, window_fn in windows.items():
        def _make(window=window_fn):
            def handler(account_id: str | None = None, connection=Depends(pg)):
                start, end = window()
                return daily_range(connection, platform_db, start, end, account_id)
            return handler
        router.add_api_route(
            f"/{path}", _make(), methods=["GET"], response_model=list[DailyMetricsRow],
        )


def _register_range_route(router: APIRouter, platform_db: str) -> None:
    @router.get("", response_model=list[DailyMetricsRow])
    def _range(
        start_date: date = Query(..., alias="start-date"),
        end_date: date | None = Query(default=None, alias="end-date"),
        account_id: str | None = None,
        connection=Depends(pg),
    ):
        start, end = _parse_or_400(start_date, end_date)
        return daily_range(connection, platform_db, start, end, account_id)


def build_platform_router(slug: str) -> APIRouter:
    platform_db = SLUG_TO_DB[slug]
    router = APIRouter(prefix=f"/{slug}", tags=[slug], dependencies=[Depends(require_api_key)])
    if slug == "google":
        _register_keywords_route(router)
    _register_window_routes(router, platform_db)
    _register_range_route(router, platform_db)
    return router
