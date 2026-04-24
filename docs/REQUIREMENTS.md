# Requisitos

| ID | Requisito |
|----|-----------|
| REQ01 | API FastAPI porta 8000 |
| REQ02 | Rate limit por segundo (slowapi, 50 rps/token) |
| REQ03 | Postgres unificado. Schemas: `airflow`, `users`, `ops`, `bronze`, `silver`, `gold` |
| REQ04 | DAGs autocontidas; lógica de API externa em `dags/_*_extractor.py` (sem Airflow) para testabilidade |
| REQ05 | Medallion Bronze/Silver/Gold |
| REQ06 | KISS, DRY, YAGNI |
| REQ07 | Testes focam RN, mock DB/APIs com fakes nomeadas |
| REQ08 | Pastas por domínio: `platforms/`, `users/`, `reviews/`, `budget/` |
| REQ09 | Endpoint plataforma via factory `build_platform_router(slug)` |
| REQ10 | API token via `scripts/create_api_key.py` |
| REQ11 | Datas ISO `YYYY-MM-DD` |
| REQ12 | Sem UI |
| REQ13 | Mutation testing escopado a `src/api/dates.py`, `src/api/auth.py`, `dags/_date_range.py` (setup.cfg) |
| REQ14 | `/review` consome apenas gold; sem joins com bronze/silver |

## Endpoints

Ver [README](../README.md#endpoints) — tabela canônica. Detalhes de campos em cada rota via `/docs` (Swagger).

## Variáveis de ambiente

| Var | Uso |
|---|---|
| `POSTGRES_HOST/PORT/USER/PASSWORD/DB` | Postgres |
| `API_KEY_PEPPER` | Salt SHA256 dos tokens |
| `META_APP_ID/SECRET/ACCESS_TOKEN` | Meta Ads API |
| `GOOGLE_DEVELOPER_TOKEN/CLIENT_ID/SECRET/REFRESH_TOKEN/LOGIN_CUSTOMER_ID` | Google Ads API |
| `AIRFLOW_FERNET_KEY` | Fernet Airflow |

## Cadastro manual

| Tabela | Via | Nota |
|---|---|---|
| `ops.managed_accounts` | SQL | Catálogo de quais contas puxar (sem REST) |
| `ops.client_budget` | `PUT /budget/{id}` | Teto mensal + moeda; alimenta `gold.budget_pacing` |

## Fora de escopo

- `/login`, sessões curtas, refresh token
- CRUD REST de managed_accounts
- Metas CPA/ROAS alvo por cliente (fase 2)
- Histórico de mudanças (change history API)
- Meta Ads: status de campanha, search terms equivalentes, budget pacing (fase 2)
- UI web, multi-tenant, cache Redis
