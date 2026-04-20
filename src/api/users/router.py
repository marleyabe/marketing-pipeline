from fastapi import APIRouter, Depends, HTTPException

from src.api.auth import require_admin, require_api_key
from src.api.deps import pg
from src.api.users.schema import ClienteOut, UserIn, UserOut

router = APIRouter(tags=["users"])


@router.get("/users", response_model=list[UserOut], dependencies=[Depends(require_admin)])
def list_users(connection=Depends(pg)):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT id, email, name, role, created_at, disabled_at "
            "FROM users.app_users ORDER BY id"
        )
        rows = cursor.fetchall()
    column_keys = ["id", "email", "name", "role", "created_at", "disabled_at"]
    return [UserOut(**dict(zip(column_keys, row, strict=True))) for row in rows]


@router.post(
    "/createuser",
    response_model=UserOut,
    status_code=201,
    dependencies=[Depends(require_admin)],
)
def create_user(body: UserIn, connection=Depends(pg)):
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1 FROM users.app_users WHERE email = %s", [body.email])
        if cursor.fetchone():
            raise HTTPException(status_code=409, detail="Email already registered")
        cursor.execute(
            "INSERT INTO users.app_users (email, name, role) VALUES (%s, %s, %s) "
            "RETURNING id, email, name, role, created_at, disabled_at",
            [body.email, body.name, body.role],
        )
        row = cursor.fetchone()
    column_keys = ["id", "email", "name", "role", "created_at", "disabled_at"]
    return UserOut(**dict(zip(column_keys, row, strict=True)))


@router.get("/clientes", response_model=list[ClienteOut], dependencies=[Depends(require_api_key)])
def list_clientes(connection=Depends(pg)):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT account_id, MAX(account_name), "
            "ARRAY_AGG(DISTINCT platform ORDER BY platform) "
            "FROM gold.daily_performance "
            "GROUP BY account_id ORDER BY account_id"
        )
        rows = cursor.fetchall()
    return [
        ClienteOut(account_id=row[0], account_name=row[1], platforms=list(row[2]))
        for row in rows
    ]


@router.get(
    "/clientes/{account_id}",
    response_model=ClienteOut,
    dependencies=[Depends(require_api_key)],
)
def get_cliente(account_id: str, connection=Depends(pg)):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT account_id, MAX(account_name), "
            "ARRAY_AGG(DISTINCT platform ORDER BY platform) "
            "FROM gold.daily_performance WHERE account_id = %s "
            "GROUP BY account_id",
            [account_id],
        )
        row = cursor.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Cliente not found")
    return ClienteOut(account_id=row[0], account_name=row[1], platforms=list(row[2]))
