from fastapi import APIRouter, Depends, HTTPException

from src.api.auth import require_api_key
from src.api.budget.queries import read_client_budget, upsert_client_budget
from src.api.budget.schema import ClientBudgetIn, ClientBudgetOut
from src.api.deps import pg

router = APIRouter(
    prefix="/budget",
    tags=["budget"],
    dependencies=[Depends(require_api_key)],
)


@router.get("/{account_id}", response_model=list[ClientBudgetOut])
def list_budget(account_id: str, connection=Depends(pg)) -> list[ClientBudgetOut]:
    google = read_client_budget(connection, account_id, "google_ads")
    meta = read_client_budget(connection, account_id, "meta_ads")
    return [b for b in (google, meta) if b is not None]


@router.get("/{account_id}/{platform}", response_model=ClientBudgetOut)
def get_budget(account_id: str, platform: str, connection=Depends(pg)) -> ClientBudgetOut:
    budget = read_client_budget(connection, account_id, platform)
    if budget is None:
        raise HTTPException(status_code=404, detail="Budget not found")
    return budget


@router.put("/{account_id}", response_model=ClientBudgetOut)
def put_budget(
    account_id: str, body: ClientBudgetIn, connection=Depends(pg),
) -> ClientBudgetOut:
    return upsert_client_budget(
        connection, account_id, body.platform,
        body.monthly_budget, body.currency, body.active,
    )
