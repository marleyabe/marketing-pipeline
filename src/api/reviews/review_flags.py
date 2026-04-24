"""Regras puras (sem I/O) que viram ReviewSignal no JSON.

Cada função recebe apenas o necessário e retorna ReviewSignal | None.
Permite teste unitário como funções puras. Thresholds centralizados aqui.
"""

from src.api.reviews.schema import (
    AccountPerformance,
    BudgetPacing,
    CampaignRow,
    NegativesCoverage,
    ReviewSignal,
    SearchTermBuckets,
)

ROAS_CRITICAL = 1.0
ZERO_CONV_SPEND_MIN = 50.0  # abaixo disso não vale gerar alerta
WASTED_SEARCH_TERMS_MIN_COUNT = 5


def roas_below_one(performance: AccountPerformance) -> ReviewSignal | None:
    """ROAS < 1.0 no período com spend relevante = operação no vermelho."""
    if performance.roas is None or performance.spend < ZERO_CONV_SPEND_MIN:
        return None
    if performance.roas >= ROAS_CRITICAL:
        return None
    return ReviewSignal(
        code="roas_below_one", severity="critical",
        message=(
            f"ROAS de {performance.roas:.2f} com spend R$ {performance.spend:.2f} "
            f"— a conta está gastando mais do que fatura em conversão atribuída."
        ),
    )


def budget_over_pace(pacing: BudgetPacing) -> ReviewSignal | None:
    """Spend MTD avançando mais rápido que o mês — risco de estourar o teto."""
    if pacing.pace_flag != "over" or pacing.pct_consumed is None:
        return None
    return ReviewSignal(
        code="budget_over_pace", severity="warning",
        message=(
            f"Spend do mês em {pacing.pct_consumed:.1f}% com apenas "
            f"{pacing.days_elapsed_pct:.1f}% do mês decorrido."
        ),
    )


def budget_under_pace(pacing: BudgetPacing) -> ReviewSignal | None:
    """Consumo muito abaixo do esperado — budget subutilizado."""
    if pacing.pace_flag != "under" or pacing.pct_consumed is None:
        return None
    return ReviewSignal(
        code="budget_under_pace", severity="info",
        message=(
            f"Spend do mês em {pacing.pct_consumed:.1f}% com "
            f"{pacing.days_elapsed_pct:.1f}% do mês decorrido — veicular mais."
        ),
    )


def campaigns_with_zero_conversions(campaigns: list[CampaignRow]) -> ReviewSignal | None:
    """Campanhas ativas gastando sem converter. Ignora gastos irrisórios."""
    offenders = [
        c.campaign_name for c in campaigns
        if c.spend >= ZERO_CONV_SPEND_MIN and c.conversions == 0 and c.campaign_name
    ]
    if not offenders:
        return None
    return ReviewSignal(
        code="campaigns_with_zero_conversions", severity="warning",
        message=f"{len(offenders)} campanhas com spend sem nenhuma conversão: {', '.join(offenders[:5])}",
    )


def search_terms_wasting_spend(buckets: SearchTermBuckets) -> ReviewSignal | None:
    """Muitos search terms reais gastando sem converter = falta negativar."""
    wasted = buckets.top_by_spend_no_conv
    if len(wasted) < WASTED_SEARCH_TERMS_MIN_COUNT:
        return None
    total_wasted = sum(t.spend for t in wasted)
    return ReviewSignal(
        code="search_terms_wasting_spend", severity="warning",
        message=(
            f"{len(wasted)} search terms com R$ {total_wasted:.2f} de spend "
            f"sem conversão — candidatos a negativa."
        ),
    )


def campaigns_without_negatives(coverage: NegativesCoverage) -> ReviewSignal | None:
    """Campanhas com spend mas sem qualquer palavra negativa cadastrada."""
    uncovered = coverage.campaigns_without_negatives
    if not uncovered:
        return None
    return ReviewSignal(
        code="campaigns_without_negatives", severity="info",
        message=f"{len(uncovered)} campanhas sem negativas: {', '.join(uncovered[:5])}",
    )


def evaluate_all(
    performance: AccountPerformance,
    pacing: BudgetPacing,
    campaigns: list[CampaignRow],
    buckets: SearchTermBuckets,
    coverage: NegativesCoverage,
) -> list[ReviewSignal]:
    """Roda todas as regras e retorna apenas as que dispararam."""
    candidates = [
        roas_below_one(performance),
        budget_over_pace(pacing),
        budget_under_pace(pacing),
        campaigns_with_zero_conversions(campaigns),
        search_terms_wasting_spend(buckets),
        campaigns_without_negatives(coverage),
    ]
    return [signal for signal in candidates if signal is not None]
