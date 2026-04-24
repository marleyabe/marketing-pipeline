# SDD — Marketing Pipeline

## Objetivo

ELT Meta Ads + Google Ads → Postgres (medallion) → API REST com revisão automática de conta. Sem UI.

## Stack

| Camada | Tech |
|---|---|
| Orquestração | Airflow 2.10 (LocalExecutor) |
| Storage | Postgres 16 |
| Transform | dbt 1.8 via Cosmos |
| API | FastAPI 0.115 |
| Auth | API Key SHA256 + pepper, rolling 14d |

## Schemas

| Schema | Conteúdo |
|---|---|
| `bronze` | Cru: `meta_ads_raw`, `google_ads_raw`, `google_search_terms_raw`, `google_negatives_raw` |
| `silver` | Tipado + dedupe: `meta_ads`, `google_ads`, `google_ads_keywords_dedup`, `google_search_terms_dedup`, `google_negatives_dedup`, `unified_campaigns` |
| `gold` | Com RN: `daily_performance` (inclui ROAS), `weekly_performance`, `google_ads_keywords`, `google_search_terms`, `google_negatives` (snapshot mais recente), `budget_pacing` (view) |
| `ops` | `managed_accounts`, `client_budget`, `api_keys`, `api_audit_log` |
| `users` | `app_users` |

`ops.managed_accounts` alimenta extractors. `/clientes` lê `gold.daily_performance` (RN15). `/review` lê apenas gold (RN11 + REQ14).

## Arquitetura

```
Google/Meta APIs
   ↓ DAG extract (keywords + search_terms + negatives) → bronze
   ↓ daily_transform (dbt silver → gold)
   gold + ops.client_budget
   ↓ FastAPI (review_flags) → JSON
   MCP / dashboard
```

## DAG google_ads

3 tasks encadeadas; dataset emitido no último passo:

1. `extract_keywords_task` — `keyword_view` → `bronze.google_ads_raw`
2. `extract_search_terms_task` — `search_term_view` → `bronze.google_search_terms_raw`
3. `snapshot_negatives_task` — `campaign_criterion` + `ad_group_criterion` (negative=TRUE) → `bronze.google_negatives_raw` (stamp `snapshot_date`)

Lógica de API em `dags/_google_extractor.py` (sem Airflow), GAQL em `dags/_google_queries.py`. Backfill via `--conf` com `start_date`/`end_date` (RN16).

## /review — fluxo

1. `_resolve_period` → se ausente, `lastmonth()`
2. `fetch_account_overview` valida conta
3. Executa em paralelo conceitual: performance, campaigns, top keywords, search_terms buckets, negatives coverage, budget pacing
4. `evaluate_all` aplica regras puras → signals
5. Devolve `AccountReview` (Pydantic)

Cada `fetch_*` em `src/api/reviews/queries.py`; SQL vive em constantes no topo do módulo. Flags em `src/api/reviews/review_flags.py` (puras, testáveis).

## Auth

1. Admin cria user (`/createuser` ou seed)
2. `scripts/create_api_key.py --name X [--user-id N]` emite raw
3. Cliente envia `X-API-Key: <raw>`
4. Server: hash + lookup → checa `revoked_at`, `disabled_at`, `last_used_at` (RN06–08, RN12)
5. Hit válido rola janela 14d (RN07)

## Princípios

- KISS · DRY · YAGNI
- Test-first nas RNs puras
- Lógica 3rd-party atrás de módulo fino (extractor por plataforma)
- Funções ≤ 20 linhas, arquivos < 500, tipos explícitos (CLAUDE.md)

## Cobertura RN → camada

| RN | Camada |
|----|--------|
| RN01-04 | `src/api/dates.py` |
| RN05, RN15 | `src/api/users/router.py` |
| RN06-09, RN12 | `src/api/auth.py` |
| RN10, RN14 | `dbt_project/models/gold/*.sql` |
| RN11, REQ14 | layout — API lê só `gold.*` |
| RN13 | `src/api/users/router.py` |
| RN16 | `dags/_date_range.py` |
| RN17 | `src/api/reviews/` |
| RN18 | `src/api/budget/` + `gold/budget_pacing.sql` |
| RN19 | `dags/_google_extractor.py` + `silver/google_{search_terms,negatives}_dedup.sql` |
| RN20 | `src/api/reviews/review_flags.py` |
