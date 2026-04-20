# Requisitos

| ID | Requisito |
|----|-----------|
| REQ01 | API FastAPI porta 8000 |
| REQ02 | Sem limite diário; rate limit por segundo (slowapi, 50 rps/token) |
| REQ03 | Postgres unificado. Schemas: `airflow`, `users`, `ops`, `bronze`, `silver`, `gold` |
| REQ04 | DAGs Airflow autocontidas (1 arquivo = 1 plataforma com SDK + DAG inline) |
| REQ04b | DAG fundida: cron diário pega ontem; params `start_date`+`end_date` permitem backfill (RN16) |
| REQ05 | Medallion: Bronze cru, Silver tratado, Gold com RN |
| REQ06 | KISS, DRY, YAGNI |
| REQ07 | Testes focam RN, mock de DB/APIs externas |
| REQ08 | Pastas separadas: `src/api/platforms/`, `src/api/users/` |
| REQ09 | Novos endpoints plataforma via factory `build_platform_router(slug)` |
| REQ10 | API token via CLI `scripts/create_api_key.py` |
| REQ11 | Datas ISO `YYYY-MM-DD` em todos params |
| REQ12 | Sem UI |
| REQ13 | Mutation testing (mutmut) escopado aos módulos de RN pura: `src/api/dates.py`, `src/api/auth.py`, `dags/_date_range.py`. Config em `setup.cfg`. |

## Endpoints

| Método | Rota | Auth | Descrição |
|--------|------|------|-----------|
| GET | `/health` | pública | status + schemas |
| GET | `/google/yesterday` | token | métricas D-1 google |
| GET | `/google/lastweek` | token | métricas semana anterior |
| GET | `/google/lastmonth` | token | métricas mês anterior |
| GET | `/google?start-date=X[&end-date=Y]&account_id=Z` | token | range; ausente=dia único |
| GET | `/meta/yesterday\|lastweek\|lastmonth` | token | idem para meta |
| GET | `/meta?start-date=X[&end-date=Y]` | token | idem range |
| GET | `/clientes` | token | lista clientes que tiveram `spend > 0` + plataformas (RN15) |
| GET | `/clientes/{id}` | token | cliente + plataformas onde gastou (RN05, RN15) |
| GET | `/users` | admin | lista users |
| POST | `/createuser` | admin | cria user |

## Variáveis de ambiente

| Var | Uso |
|-----|-----|
| `POSTGRES_HOST/PORT/USER/PASSWORD/DB` | Postgres |
| `API_KEY_PEPPER` | Salt SHA256 dos tokens |
| `META_APP_ID/SECRET/ACCESS_TOKEN` | Meta Ads API |
| `GOOGLE_DEVELOPER_TOKEN/CLIENT_ID/SECRET/REFRESH_TOKEN/LOGIN_CUSTOMER_ID` | Google Ads API |
| `AIRFLOW_FERNET_KEY` | Fernet Airflow |

## Cadastro de managed_accounts

Sem endpoint REST. Insert direto no Postgres:

```sql
INSERT INTO ops.managed_accounts (account_id, platform, account_name)
VALUES ('act_123', 'meta_ads', 'Cliente A');
```

Usado pelas DAGs `google_ads` / `meta_ads` p/ saber quais contas puxar. Sem fallback API — se vazio, DAG retorna 0 rows.

## DAGs

| DAG | Schedule | Trigger manual |
|-----|----------|----------------|
| `google_ads` | `0 5 * * *` (ontem) | params `{"start_date":"YYYY-MM-DD","end_date":"YYYY-MM-DD"}` p/ backfill |
| `meta_ads` | `0 5 * * *` | idem |
| `daily_transform` | Datasets `ads2u/bronze/google_ads`, `ads2u/bronze/meta_ads` | manual também |

```bash
docker compose exec airflow airflow dags trigger google_ads \
  --conf '{"start_date":"2026-04-01","end_date":"2026-04-15"}'
```

## Fora de escopo

- `/login`, `/logout`, sessões curtas
- CRUD `/accounts` REST (gerenciado via SQL)
- DAG separada de backfill (fundida na principal via params)
- Fallback de descoberta de contas via API (managed_accounts é fonte única)
- `src/extractors/` (lógica inline na DAG)
- UI web
- Multi-tenant
- Refresh tokens
- Cache Redis
