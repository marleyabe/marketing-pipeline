"""RN09, RN13: admin-only + email unique."""
from datetime import datetime

import pytest
from fastapi import HTTPException

from src.api.users.router import create_user, list_users
from src.api.users.schema import UserIn


def test_rn13_createuser_rejects_duplicate_email(fake_conn):
    """RN13: email unico — insert falha com 409 se ja existe."""
    connection = fake_conn([[(1,)]])  # SELECT 1 retorna row => conflito
    with pytest.raises(HTTPException) as exception_info:
        create_user(UserIn(email="a@b.com", role="user"), connection=connection)
    assert exception_info.value.status_code == 409


def test_rn13_createuser_inserts_new(fake_conn):
    connection = fake_conn([
        [],  # SELECT 1 vazio (não existe)
        [(10, "a@b.com", "Alice", "user", datetime(2026, 4, 20), None)],  # INSERT RETURNING
    ])
    output = create_user(UserIn(email="a@b.com", name="Alice", role="user"), connection=connection)
    assert output.id == 10
    assert output.email == "a@b.com"
    assert output.role == "user"


def test_rn09_list_users_returns_all(fake_conn):
    connection = fake_conn([
        [
            (1, "a@b.com", "A", "admin", datetime(2026, 4, 1), None),
            (2, "c@d.com", "C", "user", datetime(2026, 4, 5), None),
        ]
    ])
    output = list_users(connection=connection)
    assert len(output) == 2
    assert output[0].role == "admin"
    assert output[1].role == "user"
