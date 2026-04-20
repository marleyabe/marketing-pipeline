"""RN05: cliente por id retorna plataformas onde existe."""
import pytest
from fastapi import HTTPException

from src.api.users.router import get_cliente, list_clientes


def test_rn05_cliente_in_both_platforms(fake_conn):
    connection = fake_conn([[("123", "Acme", ["google_ads", "meta_ads"])]])
    output = get_cliente("123", connection=connection)
    assert output.account_id == "123"
    assert output.platforms == ["google_ads", "meta_ads"]


def test_rn05_cliente_in_one_platform(fake_conn):
    connection = fake_conn([[("456", "Beta", ["google_ads"])]])
    output = get_cliente("456", connection=connection)
    assert output.platforms == ["google_ads"]


def test_rn05_cliente_not_found(fake_conn):
    connection = fake_conn([[]])
    with pytest.raises(HTTPException) as exception_info:
        get_cliente("999", connection=connection)
    assert exception_info.value.status_code == 404


def test_list_clientes_aggregates_by_id(fake_conn):
    connection = fake_conn([
        [
            ("123", "Acme", ["google_ads", "meta_ads"]),
            ("456", "Beta", ["meta_ads"]),
        ]
    ])
    output = list_clientes(connection=connection)
    assert len(output) == 2
    assert output[0].platforms == ["google_ads", "meta_ads"]
    assert output[1].platforms == ["meta_ads"]


def test_rn15_clientes_reads_from_gold():
    """RN15: /clientes consulta gold (spend > 0), não managed_accounts."""
    from pathlib import Path

    source = (Path(__file__).parent.parent / "src" / "api" / "users" / "router.py").read_text()
    assert "gold.daily_performance" in source
    assert "ops.managed_accounts" not in source
