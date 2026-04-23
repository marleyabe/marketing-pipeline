from datetime import date
from typing import Literal

from pydantic import BaseModel

PlatformSlug = Literal["google", "meta"]
PlatformDB = Literal["google_ads", "meta_ads"]

SLUG_TO_DB: dict[str, str] = {"google": "google_ads", "meta": "meta_ads"}


class DailyMetricsRow(BaseModel):
    account_id: str
    account_name: str | None
    date: date
    platform: PlatformDB
    impressions: int
    clicks: int
    spend: float
    conversions: float
    ctr: float | None
    cpc: float | None
    cost_per_conversion: float | None
    conversion_rate: float | None


class KeywordRow(BaseModel):
    account_id: str
    account_name: str | None
    campaign_name: str | None
    ad_group_name: str | None
    keyword_text: str
    match_type: str | None
    impressions: int
    clicks: int
    spend: float
    conversions: float
