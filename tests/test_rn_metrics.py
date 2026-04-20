"""RN10-11: derived metrics null-on-zero + bronze isolation."""
from pathlib import Path


GOLD_DIR = Path(__file__).parent.parent / "dbt_project" / "models" / "gold"


def test_rn10_daily_uses_nullif_for_divisors():
    """RN10: ctr/cpc/conv_rate/cpa usam NULLIF p/ divisor 0 -> NULL."""
    sql = (GOLD_DIR / "daily_performance.sql").read_text()
    assert sql.count("NULLIF(SUM(impressions), 0)") >= 1
    assert sql.count("NULLIF(SUM(clicks), 0)") >= 2  # cpc + conversion_rate
    assert sql.count("NULLIF(SUM(conversions), 0)") >= 1  # cost_per_conversion


def test_rn10_weekly_uses_nullif_for_divisors():
    sql = (GOLD_DIR / "weekly_performance.sql").read_text()
    assert "NULLIF(SUM(impressions), 0)" in sql
    assert sql.count("NULLIF(SUM(clicks), 0)") >= 2
    assert "NULLIF(SUM(conversions), 0)" in sql


def test_rn11_api_queries_never_reference_bronze():
    """RN11: Bronze nunca retornado. Nenhuma query da API acessa bronze.*."""
    api_dir = Path(__file__).parent.parent / "src" / "api"
    for py in api_dir.rglob("*.py"):
        content = py.read_text()
        assert "bronze." not in content, f"{py} referencia bronze."
        assert "FROM bronze" not in content, f"{py} lê de bronze"


def test_rn11_api_only_reads_gold_for_metrics():
    router = (Path(__file__).parent.parent / "src" / "api" / "platforms" / "queries.py").read_text()
    assert "gold.daily_performance" in router


def test_rn14_daily_filters_spend_zero():
    """RN14: gold só linhas com spend > 0 (HAVING SUM(spend) > 0)."""
    sql = (GOLD_DIR / "daily_performance.sql").read_text()
    assert "HAVING SUM(spend) > 0" in sql


def test_rn14_weekly_filters_spend_zero():
    sql = (GOLD_DIR / "weekly_performance.sql").read_text()
    assert "HAVING SUM(spend) > 0" in sql
