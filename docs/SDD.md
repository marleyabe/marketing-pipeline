# SDD — Marketing Pipeline

## Objetivo

Pipeline ELT extrai Meta Ads + Google Ads → Postgres (medallion) → API REST. Sem UI.

## Stack

| Camada | Tecnologia |
|--------|-----------|
| Orquestração | Airflow 2.10 (LocalExecutor) |
| Storage | Postgres 16 |
| Transformação | dbt 1.8 |
| API | FastAPI 0.115 |
| Auth | API Key (SHA256 + pepper) |

## Schemas Postgres

| Schema | Conteúdo |
|--------|----------|
| `airflow` | Metadados Airflow |
| `bronze` | Dados crus extraídos (`meta_ads_raw`, `google_ads_raw`) |
| `silver` | Dados tratados (dbt: `google_ads`, `meta_ads`, `unified_campaigns`) |
| `gold` | Métricas com RN aplicada (`daily_performance`, `weekly_performance`); filtra `spend > 0` (RN14) |
| `ops` | `managed_accounts` (catálogo p/ extractors), `api_keys`, `api_audit_log` |
| `users` | `app_users` |

> `ops.managed_accounts` é fonte para extractors (quais contas puxar). API não consulta — `/clientes` lê `gold.daily_performance` (RN15).

## Arquitetura

```
Meta/Google APIs
      ↓ (DAG google_ads | meta_ads — SDK + extract inline)
   Bronze (raw)
      ↓ (daily_transform → dbt silver)
   Silver (tratado)
      ↓ (dbt gold + RN)
    Gold
      ↓ (FastAPI)
   Cliente
```

## Estrutura

```
src/
  api/
    __init__.py        FastAPI app
    auth.py            API key + admin gate
    deps.py            pg dependency
    dates.py           yesterday/lastweek/lastmonth/range
    platforms/         endpoints google + meta (factory)
    users/             /users, /createuser, /clientes
  db.py                schemas + DDL + get_pg
  loader.py            load_bronze (psycopg2 execute_values)
dags/
  google_ads.py        SDK + DAG fundida (params start_date/end_date)
  meta_ads.py          idem
  daily_transform.py   dbt Cosmos, trigger por Datasets
  _date_range.py       resolve_target_dates (RN16) — puro, testável
dbt_project/           silver + gold models
scripts/create_api_key.py
tests/                 1 arquivo por categoria RN (40 testes)
```

## Fluxo de auth

1. Admin cria user (`/createuser` ou seed manual)
2. Admin gera token: `python scripts/create_api_key.py --name X [--user-id N]`
3. Cliente envia `X-API-Key: <raw>` em todo request
4. Server: hash + lookup `ops.api_keys` → checa `revoked_at`, `disabled_at`, idade `last_used_at`
5. Hit válido atualiza `last_used_at` (rolling 14d)

## DAGs Airflow

1 arquivo por plataforma + 1 transform. SDK + lógica inline na DAG (autocontido).

| DAG | Schedule | Função |
|-----|----------|--------|
| `google_ads` | `0 5 * * *` + manual | Extract Google → bronze. Params `start_date`/`end_date` (RN16). Outlet Dataset |
| `meta_ads` | `0 5 * * *` + manual | Extract Meta → bronze. Params idem. Outlet Dataset |
| `daily_transform` | Datasets ↑ | dbt silver + gold (Cosmos). Dispara após ambos extractors |

**Coordenação:** Airflow Datasets — extract publica `ads2u/bronze/<platform>`, transform agenda nos 2.

**Backfill:** trigger manual da própria DAG com params; sem DAG separada.

## Princípios

- **KISS**: zero camada sem uso real
- **DRY**: factory de routers para plataformas
- **YAGNI**: sem session, sem refresh token, sem UI
- **Test-first p/ RN**: cada regra tem teste dedicado

## Cobertura RN → camada

| RN | Camada |
|----|--------|
| RN01-04 | `src/api/dates.py` |
| RN05, RN15 | `src/api/users/router.py` (gold query) |
| RN06-09, RN12 | `src/api/auth.py` |
| RN10, RN14 | `dbt_project/models/gold/*.sql` |
| RN11 | layout — API só lê `gold.*` |
| RN13 | `src/api/users/router.py` (`create_user`) |
| RN16 | `dags/_date_range.py` (`resolve_target_dates`) |
