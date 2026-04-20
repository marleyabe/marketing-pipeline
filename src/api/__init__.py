import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import APIRouter, Depends, FastAPI
from pydantic import BaseModel

from src.api.deps import pg
from src.api.platforms.router import build_platform_router
from src.api.users.router import router as users_router
from src.db import SCHEMAS, get_pg, init_schemas


class HealthOut(BaseModel):
    status: str
    postgres: bool
    schemas: list[str]


health_router = APIRouter(tags=["health"])


@health_router.get("/health", response_model=HealthOut)
def health(connection=Depends(pg)):
    try:
        with connection.cursor() as cursor:
            placeholders = ",".join(["%s"] * len(SCHEMAS))
            cursor.execute(
                f"SELECT schema_name FROM information_schema.schemata "
                f"WHERE schema_name IN ({placeholders}) ORDER BY schema_name",
                SCHEMAS,
            )
            schemas = [row[0] for row in cursor.fetchall()]
        return HealthOut(status="ok", postgres=True, schemas=schemas)
    except Exception:
        return HealthOut(status="degraded", postgres=False, schemas=[])


@asynccontextmanager
async def lifespan(app: FastAPI):
    connection = get_pg()
    init_schemas(connection)
    connection.close()
    yield


app = FastAPI(title="Ads2u Marketing Pipeline API", version="0.4.0", lifespan=lifespan)
app.include_router(health_router)
app.include_router(build_platform_router("google"))
app.include_router(build_platform_router("meta"))
app.include_router(users_router)


if __name__ == "__main__":
    uvicorn.run(
        "src.api:app",
        host=os.environ.get("API_HOST", "0.0.0.0"),  # nosec B104
        port=int(os.environ.get("API_PORT", "8000")),
    )
