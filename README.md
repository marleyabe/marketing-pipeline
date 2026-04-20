# Ads2u — Marketing Pipeline

Extrai Google Ads + Meta Ads diário, transforma Bronze/Silver/Gold (Postgres) via dbt + Airflow + Cosmos, expõe via FastAPI.

## Stack

| Componente | Escolha |
|---|---|
| Orquestração | Airflow 2.10 (LocalExecutor) |
| dbt em Airflow | astronomer-cosmos |
| Banco | Postgres 16 (DB `airflow` = metadata, DB `marketing` = bronze/silver/gold/ops/users) |
| API | FastAPI + uvicorn |
| Auth | API Key SHA256+pepper, rolling 14d |

## Subir

```bash
cp .env.example .env
docker compose up -d --build
```

- Airflow UI: http://localhost:8080 (admin/admin)
- API Swagger: http://localhost:8000/docs
- Health: http://localhost:8000/health

## Criar API key

```bash
# Sem user
docker compose exec api python scripts/create_api_key.py --name minha-maquina

# Vinculado a user (admin precisa existir antes)
docker compose exec api python scripts/create_api_key.py --name admin1 --user-id 1
```

## Endpoints

| Método | Rota | Auth |
|--------|------|------|
| GET | `/health` | pública |
| GET | `/google/{yesterday\|lastweek\|lastmonth}` | token |
| GET | `/google?start-date=YYYY-MM-DD[&end-date=YYYY-MM-DD][&account_id=X]` | token |
| GET | `/meta/{yesterday\|lastweek\|lastmonth}` | token |
| GET | `/meta?start-date=...` | token |
| GET | `/clientes` | token |
| GET | `/clientes/{id}` | token |
| GET | `/users` | admin |
| POST | `/createuser` | admin |

## Cadastrar managed_account (extração)

Sem endpoint REST. Via SQL ou CLI:

```bash
docker compose exec postgres psql -U airflow marketing \
  -c "INSERT INTO ops.managed_accounts (account_id, platform, account_name) VALUES ('act_123','meta_ads','Cliente A');"
```

## DAGs

1 DAG por plataforma. Cron diário pega ontem; trigger manual com params faz range (RN16).

| DAG | Schedule | Descrição |
|---|---|---|
| `google_ads` | `0 5 * * *` | Extract Google → bronze. Params `start_date`/`end_date` (ISO) opcionais |
| `meta_ads` | `0 5 * * *` | Extract Meta → bronze. Params idem |
| `daily_transform` | Datasets ↑ | dbt silver + gold (Cosmos), dispara após ambos extractors |

### Backfill via trigger

```bash
docker compose exec airflow airflow dags trigger google_ads \
  --conf '{"start_date":"2026-04-01","end_date":"2026-04-15"}'
```

### Regras de datas (RN16)

| start_date | end_date | Resultado |
|------------|----------|-----------|
| vazio | vazio | ontem (D-1 UTC) |
| preenchido | preenchido | range start..end |
| só 1 preenchido | — | erro |

## Schemas Postgres (DB `marketing`)

| Schema | Conteúdo |
|--------|----------|
| `bronze` | `meta_ads_raw`, `google_ads_raw` — cru das APIs |
| `silver` | `meta_ads`, `google_ads`, `unified_campaigns` — tipado + dedupe |
| `gold` | `daily_performance`, `weekly_performance` — agregado, filtra `spend > 0` (RN14) |
| `ops` | `managed_accounts`, `api_keys`, `api_audit_log` |
| `users` | `app_users` |

## Estrutura

```
src/
  api/
    __init__.py        FastAPI app + health
    auth.py            API key + admin gate (RN06-09, RN12)
    deps.py            pg dependency
    dates.py           yesterday/lastweek/lastmonth/range (RN01-04)
    platforms/         router factory google + meta
    users/             /users, /createuser, /clientes
  db.py                schemas + DDL
  loader.py            load_bronze (psycopg2 execute_values)
dags/
  google_ads.py        SDK + 1 DAG (params start_date/end_date, ambos vazios = ontem)
  meta_ads.py          idem Meta
  daily_transform.py   dbt (Cosmos), trigger por Datasets
dbt_project/
  models/silver/  models/gold/
  profiles.yml         postgres target
scripts/
  create_api_key.py
tests/
  test_rn_*.py         34 testes focados em regras de negócio
docs/
  SDD.md  BUSINESS_RULES.md  REQUIREMENTS.md
```

## Testes

```bash
pytest tests/ -v
```

Cobertura: 16 RNs, 40 testes. Mocks `pg` + `dags/_date_range` puro. Sem stack rodando.

## Mutation testing (mutmut)

Escopado aos módulos de RN pura: `src/api/dates.py`, `src/api/auth.py`, `dags/_date_range.py`. Config em `setup.cfg`.

```bash
# Rodar dentro do container (Python 3.12/3.13)
docker compose exec api python -m mutmut run
docker compose exec api python -m mutmut results
docker compose exec api python -m mutmut show <id>
```

> **Aviso:** mutmut 3.5 + Python 3.14 conflita com `multiprocessing.set_start_method`. Rodar via Docker (Python ≤3.13) ou venv com Python ≤3.13.

## Docs

- [SDD.md](docs/SDD.md) — visão arquitetural
- [BUSINESS_RULES.md](docs/BUSINESS_RULES.md) — RN01-15 + testes
- [REQUIREMENTS.md](docs/REQUIREMENTS.md) — REQs + endpoints + env vars
