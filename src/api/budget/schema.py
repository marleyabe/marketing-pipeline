from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

Platform = Literal["google_ads", "meta_ads"]


class ClientBudgetIn(BaseModel):
    platform: Platform
    monthly_budget: float = Field(gt=0)
    currency: str = "BRL"
    active: bool = True


class ClientBudgetOut(BaseModel):
    account_id: str
    platform: Platform
    monthly_budget: float
    currency: str
    active: bool
    created_at: datetime
    updated_at: datetime
