"""RN06-09, RN12: token lifecycle + role checks."""
from datetime import datetime, timedelta

import pytest
from fastapi import HTTPException

from src.api.auth import _validate_token, hash_api_key, is_token_expired


def test_rn06_token_not_expired_within_14_days():
    """RN06: token ativo dentro de 14 dias."""
    now = datetime(2026, 4, 20, 12, 0, 0)
    last_used_at = now - timedelta(days=13, hours=23)
    assert is_token_expired(last_used_at, now) is False


def test_rn06_token_expired_after_14_days():
    """RN06: token inativo > 14 dias expira."""
    now = datetime(2026, 4, 20, 12, 0, 0)
    last_used_at = now - timedelta(days=14, minutes=1)
    assert is_token_expired(last_used_at, now) is True


def test_rn06_fresh_token_never_expired():
    """RN06: token nunca usado (last_used_at=None) não expira."""
    now = datetime(2026, 4, 20, 12, 0, 0)
    assert is_token_expired(None, now) is False


def test_rn07_token_use_refreshes_validity(fake_conn, mock_request):
    """RN07: uso atualiza last_used_at (rolling)."""
    from src.api.auth import _record_usage

    connection = fake_conn([[], []])
    cursor_operations = []
    original_cursor = connection.cursor

    def tracked_cursor():
        cursor = original_cursor()
        original_execute = cursor.execute

        def execute_track(query, parameters=None):
            cursor_operations.append((query, parameters))
            return original_execute(query, parameters)

        cursor.execute = execute_track
        return cursor

    connection.cursor = tracked_cursor
    _record_usage(connection, mock_request, key_id=7)
    assert any(
        "UPDATE ops.api_keys SET last_used_at" in operation[0]
        for operation in cursor_operations
    )
    assert any(
        "INSERT INTO ops.api_audit_log" in operation[0]
        for operation in cursor_operations
    )


def test_rn08_revoked_token_rejected(fake_conn):
    """RN08: token revoked_at IS NOT NULL rejeita.
    SQL já filtra revoked_at IS NULL — row não encontrada => 403."""
    connection = fake_conn([[]])
    now = datetime(2026, 4, 20)
    with pytest.raises(HTTPException) as exception_info:
        _validate_token(connection, "anykey", now)
    assert exception_info.value.status_code == 403
    assert "Invalid" in exception_info.value.detail


def test_rn09_admin_role_passes(fake_conn):
    connection = fake_conn([[(1, 5, None, "admin", None)]])
    now = datetime(2026, 4, 20)
    key_id, user_id, role = _validate_token(connection, "key", now)
    assert role == "admin"


def test_rn09_user_role_passes_validate_not_admin_check(fake_conn):
    """role user passa validate; require_admin é gate separado."""
    connection = fake_conn([[(1, 5, None, "user", None)]])
    now = datetime(2026, 4, 20)
    _, _, role = _validate_token(connection, "key", now)
    assert role == "user"


def test_rn09_require_admin_rejects_non_admin():
    from src.api.auth import require_admin

    with pytest.raises(HTTPException) as exception_info:
        require_admin(principal={"key_id": 1, "user_id": 2, "role": "user"})
    assert exception_info.value.status_code == 403


def test_rn09_require_admin_accepts_admin():
    from src.api.auth import require_admin

    principal = {"key_id": 1, "user_id": 2, "role": "admin"}
    assert require_admin(principal=principal) is principal


def test_rn12_disabled_user_token_rejected(fake_conn):
    """RN12: user com disabled_at rejeita."""
    connection = fake_conn([[(1, 5, None, "user", datetime(2026, 1, 1))]])
    now = datetime(2026, 4, 20)
    with pytest.raises(HTTPException) as exception_info:
        _validate_token(connection, "key", now)
    assert exception_info.value.status_code == 403
    assert "disabled" in exception_info.value.detail.lower()


def test_hash_pepper_deterministic():
    hash_first = hash_api_key("abc")
    hash_second = hash_api_key("abc")
    assert hash_first == hash_second


def test_hash_differs_per_key():
    assert hash_api_key("abc") != hash_api_key("abd")
