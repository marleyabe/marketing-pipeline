from datetime import UTC, date, datetime, timedelta


def today_utc() -> date:
    return datetime.now(UTC).date()


def yesterday() -> tuple[date, date]:
    yesterday_date = today_utc() - timedelta(days=1)
    return yesterday_date, yesterday_date


def lastweek() -> tuple[date, date]:
    today = today_utc()
    this_monday = today - timedelta(days=today.weekday())
    start = this_monday - timedelta(days=7)
    end = start + timedelta(days=6)
    return start, end


def lastmonth() -> tuple[date, date]:
    today = today_utc()
    first_of_this_month = today.replace(day=1)
    end = first_of_this_month - timedelta(days=1)
    start = end.replace(day=1)
    return start, end


def parse_range(start_date: date | None, end_date: date | None) -> tuple[date, date]:
    if start_date is None:
        raise ValueError("start-date required")
    end = end_date if end_date is not None else start_date
    if end < start_date:
        raise ValueError("end-date must be >= start-date")
    return start_date, end
