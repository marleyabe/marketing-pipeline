"""SQL isolado para ops.client_budget. Reusado pelo router e pelo review."""

from src.api.budget.schema import ClientBudgetOut

BUDGET_COLUMNS = [
    "account_id", "platform", "monthly_budget", "currency",
    "active", "created_at", "updated_at",
]

SQL_READ_BUDGET = (
    f"SELECT {', '.join(BUDGET_COLUMNS)} FROM ops.client_budget "
    "WHERE account_id = %s AND platform = %s"
)

SQL_UPSERT_BUDGET = """
    INSERT INTO ops.client_budget
        (account_id, platform, monthly_budget, currency, active, updated_at)
    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (account_id, platform) DO UPDATE SET
        monthly_budget = EXCLUDED.monthly_budget,
        currency = EXCLUDED.currency,
        active = EXCLUDED.active,
        updated_at = CURRENT_TIMESTAMP
    RETURNING account_id, platform, monthly_budget, currency,
              active, created_at, updated_at
"""


def _row_to_budget(row: tuple) -> ClientBudgetOut:
    mapping = dict(zip(BUDGET_COLUMNS, row, strict=True))
    mapping["monthly_budget"] = float(mapping["monthly_budget"])
    return ClientBudgetOut(**mapping)


def read_client_budget(connection, account_id: str, platform: str) -> ClientBudgetOut | None:
    """Budget cadastrado para (account_id, platform). None se não existir."""
    with connection.cursor() as cursor:
        cursor.execute(SQL_READ_BUDGET, [account_id, platform])
        row = cursor.fetchone()
    return _row_to_budget(row) if row is not None else None


def upsert_client_budget(
    connection,
    account_id: str,
    platform: str,
    monthly_budget: float,
    currency: str,
    active: bool,
) -> ClientBudgetOut:
    """Cria ou atualiza o budget do cliente. Atualiza updated_at em toda escrita."""
    with connection.cursor() as cursor:
        cursor.execute(
            SQL_UPSERT_BUDGET,
            [account_id, platform, monthly_budget, currency, active],
        )
        row = cursor.fetchone()
    return _row_to_budget(row)
