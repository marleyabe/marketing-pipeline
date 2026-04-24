"""review_flags: regras puras. Sem DB. Unitário como F.I.R.S.T exige."""

from src.api.reviews.review_flags import (
    budget_over_pace,
    budget_under_pace,
    campaigns_with_zero_conversions,
    campaigns_without_negatives,
    evaluate_all,
    roas_below_one,
    search_terms_wasting_spend,
)
from src.api.reviews.schema import (
    AccountPerformance,
    BudgetPacing,
    CampaignRow,
    NegativesCoverage,
    SearchTermBuckets,
    SearchTermRow,
)


def _perf(spend: float, roas: float | None) -> AccountPerformance:
    return AccountPerformance(
        impressions=100, clicks=10, spend=spend, conversions=1.0,
        conversion_value=spend * (roas or 0), ctr=10.0, cpc=1.0,
        cpa=spend, roas=roas,
    )


def test_roas_below_one_fires_when_critical():
    signal = roas_below_one(_perf(spend=200.0, roas=0.5))
    assert signal is not None
    assert signal.code == "roas_below_one"
    assert signal.severity == "critical"


def test_roas_below_one_skips_when_spend_insignificant():
    signal = roas_below_one(_perf(spend=10.0, roas=0.2))
    assert signal is None


def test_roas_below_one_skips_when_roas_healthy():
    signal = roas_below_one(_perf(spend=500.0, roas=1.5))
    assert signal is None


def test_roas_below_one_skips_when_roas_none():
    signal = roas_below_one(_perf(spend=500.0, roas=None))
    assert signal is None


def _pacing(flag: str, pct: float | None = 80.0, elapsed: float | None = 50.0) -> BudgetPacing:
    return BudgetPacing(
        monthly_budget=1000.0, currency="BRL", spent_mtd=pct or 0,
        pct_consumed=pct, days_elapsed_pct=elapsed, pace_flag=flag,
    )


def test_budget_over_pace_fires_only_when_over():
    assert budget_over_pace(_pacing("over")) is not None
    assert budget_over_pace(_pacing("on_track")) is None
    assert budget_over_pace(_pacing("under")) is None


def test_budget_under_pace_fires_only_when_under():
    assert budget_under_pace(_pacing("under")) is not None
    assert budget_under_pace(_pacing("on_track")) is None


def _campaign(name: str, spend: float, conversions: float) -> CampaignRow:
    return CampaignRow(
        campaign_name=name, status="ENABLED", spend=spend,
        conversions=conversions, conversion_value=0.0, roas=None,
    )


def test_campaigns_with_zero_conversions_fires_when_offenders_exist():
    campaigns = [
        _campaign("A", 100.0, 0.0),
        _campaign("B", 200.0, 5.0),
        _campaign("C", 10.0, 0.0),  # spend < threshold
    ]
    signal = campaigns_with_zero_conversions(campaigns)
    assert signal is not None
    assert "A" in signal.message
    assert "C" not in signal.message


def test_campaigns_with_zero_conversions_none_when_all_convert():
    campaigns = [_campaign("A", 100.0, 5.0)]
    assert campaigns_with_zero_conversions(campaigns) is None


def _search_term(name: str, spend: float) -> SearchTermRow:
    return SearchTermRow(
        search_term=name, campaign_name=None, ad_group_name=None,
        matched_keyword_text=None, matched_keyword_match_type=None, status=None,
        impressions=100, clicks=10, spend=spend, conversions=0.0, roas=0.0,
    )


def test_search_terms_wasting_spend_needs_min_count():
    buckets = SearchTermBuckets(
        top_by_spend_no_conv=[_search_term(f"t{i}", 10.0) for i in range(4)],
        high_roas=[],
    )
    assert search_terms_wasting_spend(buckets) is None


def test_search_terms_wasting_spend_fires_above_threshold():
    buckets = SearchTermBuckets(
        top_by_spend_no_conv=[_search_term(f"t{i}", 20.0) for i in range(6)],
        high_roas=[],
    )
    signal = search_terms_wasting_spend(buckets)
    assert signal is not None
    assert "120" in signal.message  # 6 * 20


def test_campaigns_without_negatives_fires_only_when_list_not_empty():
    with_gaps = NegativesCoverage(total_negatives=3, campaigns_without_negatives=["A", "B"])
    covered = NegativesCoverage(total_negatives=10, campaigns_without_negatives=[])
    assert campaigns_without_negatives(with_gaps) is not None
    assert campaigns_without_negatives(covered) is None


def test_evaluate_all_combines_signals():
    signals = evaluate_all(
        performance=_perf(spend=300.0, roas=0.5),
        pacing=_pacing("over"),
        campaigns=[_campaign("A", 100.0, 0.0), _campaign("B", 100.0, 0.0)],
        buckets=SearchTermBuckets(
            top_by_spend_no_conv=[_search_term(f"t{i}", 30.0) for i in range(6)],
            high_roas=[],
        ),
        coverage=NegativesCoverage(total_negatives=0, campaigns_without_negatives=["A"]),
    )
    codes = {s.code for s in signals}
    assert {"roas_below_one", "budget_over_pace", "campaigns_with_zero_conversions",
            "search_terms_wasting_spend", "campaigns_without_negatives"} <= codes


def test_evaluate_all_returns_empty_when_healthy():
    signals = evaluate_all(
        performance=_perf(spend=500.0, roas=3.0),
        pacing=_pacing("on_track"),
        campaigns=[_campaign("A", 100.0, 5.0)],
        buckets=SearchTermBuckets(top_by_spend_no_conv=[], high_roas=[]),
        coverage=NegativesCoverage(total_negatives=20, campaigns_without_negatives=[]),
    )
    assert signals == []
