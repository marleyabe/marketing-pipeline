from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query

from src.api.auth import require_api_key
from src.api.dates import lastmonth, lastweek, parse_range, yesterday
from src.api.deps import pg
from src.api.platforms.queries import daily_range
from src.api.platforms.schema import SLUG_TO_DB, DailyMetricsRow


def build_platform_router(slug: str) -> APIRouter:
    platform_db = SLUG_TO_DB[slug]
    router = APIRouter(prefix=f"/{slug}", tags=[slug], dependencies=[Depends(require_api_key)])

    @router.get("/yesterday", response_model=list[DailyMetricsRow])
    def _yesterday(account_id: str | None = None, connection=Depends(pg)):
        start, end = yesterday()
        return daily_range(connection, platform_db, start, end, account_id)

    @router.get("/lastweek", response_model=list[DailyMetricsRow])
    def _lastweek(account_id: str | None = None, connection=Depends(pg)):
        start, end = lastweek()
        return daily_range(connection, platform_db, start, end, account_id)

    @router.get("/lastmonth", response_model=list[DailyMetricsRow])
    def _lastmonth(account_id: str | None = None, connection=Depends(pg)):
        start, end = lastmonth()
        return daily_range(connection, platform_db, start, end, account_id)

    @router.get("", response_model=list[DailyMetricsRow])
    def _range(
        start_date: date = Query(..., alias="start-date"),
        end_date: date | None = Query(default=None, alias="end-date"),
        account_id: str | None = None,
        connection=Depends(pg),
    ):
        try:
            start, end = parse_range(start_date, end_date)
        except ValueError as exception:
            raise HTTPException(status_code=400, detail=str(exception)) from exception
        return daily_range(connection, platform_db, start, end, account_id)

    return router
