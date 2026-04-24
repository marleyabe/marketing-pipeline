from datetime import date
from typing import Literal

from pydantic import BaseModel

PaceFlag = Literal["over", "under", "on_track", "unknown"]
Severity = Literal["info", "warning", "critical"]


class ReviewPeriod(BaseModel):
    start: date
    end: date


class AccountOverview(BaseModel):
    account_id: str
    account_name: str | None
    platform: str


class AccountPerformance(BaseModel):
    impressions: int
    clicks: int
    spend: float
    conversions: float
    conversion_value: float
    ctr: float | None
    cpc: float | None
    cpa: float | None
    roas: float | None


class CampaignRow(BaseModel):
    campaign_name: str | None
    status: str | None
    spend: float
    conversions: float
    conversion_value: float
    roas: float | None


class KeywordRow(BaseModel):
    keyword_text: str
    match_type: str | None
    campaign_name: str | None
    ad_group_name: str | None
    impressions: int
    clicks: int
    spend: float
    conversions: float
    conversion_value: float
    roas: float | None
    quality_score: float | None


class SearchTermRow(BaseModel):
    search_term: str
    campaign_name: str | None
    ad_group_name: str | None
    matched_keyword_text: str | None
    matched_keyword_match_type: str | None
    status: str | None
    impressions: int
    clicks: int
    spend: float
    conversions: float
    roas: float | None


class SearchTermBuckets(BaseModel):
    top_by_spend_no_conv: list[SearchTermRow]
    high_roas: list[SearchTermRow]


class NegativesCoverage(BaseModel):
    total_negatives: int
    campaigns_without_negatives: list[str]


class BudgetPacing(BaseModel):
    monthly_budget: float | None
    currency: str | None
    spent_mtd: float
    pct_consumed: float | None
    days_elapsed_pct: float | None
    pace_flag: PaceFlag


class ReviewSignal(BaseModel):
    code: str
    severity: Severity
    message: str


class AccountReview(BaseModel):
    account: AccountOverview
    period: ReviewPeriod
    performance: AccountPerformance
    campaigns: list[CampaignRow]
    keywords_top: list[KeywordRow]
    search_terms: SearchTermBuckets
    negatives: NegativesCoverage
    budget: BudgetPacing
    signals: list[ReviewSignal]
