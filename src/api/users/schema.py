from datetime import datetime
from typing import Literal

from pydantic import BaseModel, EmailStr

Role = Literal["admin", "user"]


class UserOut(BaseModel):
    id: int
    email: EmailStr
    name: str | None
    role: Role
    created_at: datetime
    disabled_at: datetime | None


class UserIn(BaseModel):
    email: EmailStr
    name: str | None = None
    role: Role = "user"


class ClienteOut(BaseModel):
    account_id: str
    account_name: str | None
    platforms: list[str]
