import hashlib
import os
import secrets
from datetime import UTC, datetime, timedelta

from fastapi import Depends, Header, HTTPException, Request, status

from src.api.deps import pg

TOKEN_INACTIVITY_DAYS = 14


def generate_api_key() -> str:
    return secrets.token_urlsafe(32)


def hash_api_key(raw_key: str) -> str:
    pepper = os.environ.get("API_KEY_PEPPER", "change-me-random-string")
    return hashlib.sha256(f"{raw_key}{pepper}".encode()).hexdigest()


def is_token_expired(last_used_at: datetime | None, now: datetime) -> bool:
    if last_used_at is None:
        return False
    return (now - last_used_at) > timedelta(days=TOKEN_INACTIVITY_DAYS)


def _validate_token(connection, raw_key: str, now: datetime) -> tuple[int, int | None, str | None]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT api_keys.id, api_keys.user_id, api_keys.last_used_at,
                   app_users.role, app_users.disabled_at
            FROM ops.api_keys AS api_keys
            LEFT JOIN users.app_users AS app_users ON app_users.id = api_keys.user_id
            WHERE api_keys.key_hash = %s AND api_keys.revoked_at IS NULL
            """,
            [hash_api_key(raw_key)],
        )
        row = cursor.fetchone()
    if row is None:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid API key")
    key_id, user_id, last_used_at, role, disabled_at = row
    if disabled_at is not None:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="User disabled")
    if is_token_expired(last_used_at, now):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Token expired (inactivity)")
    return key_id, user_id, role


def _record_usage(connection, request: Request, key_id: int) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE ops.api_keys SET last_used_at = CURRENT_TIMESTAMP WHERE id = %s",
            [key_id],
        )
        cursor.execute(
            "INSERT INTO ops.api_audit_log (api_key_id, method, path, status_code, ip) "
            "VALUES (%s, %s, %s, %s, %s)",
            [key_id, request.method, request.url.path, 200,
             request.client.host if request.client else None],
        )


def require_api_key(
    request: Request,
    raw_key: str | None = Header(default=None, alias="X-API-Key"),
    connection=Depends(pg),
) -> dict:
    if not raw_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing X-API-Key")
    now = datetime.now(UTC).replace(tzinfo=None)
    key_id, user_id, role = _validate_token(connection, raw_key, now)
    _record_usage(connection, request, key_id)
    return {"key_id": key_id, "user_id": user_id, "role": role}


def require_admin(principal: dict = Depends(require_api_key)) -> dict:
    if principal.get("role") != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin role required")
    return principal
