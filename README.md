# Ads2u — Marketing Pipeline

Extrai Google Ads + Meta Ads diário, transforma Bronze/Silver/Gold via dbt + Airflow, expõe em FastAPI com revisão automática de conta (`/review`) pronta para MCP.

## Stack

Airflow 2.10 · dbt 1.8 (Cosmos) · Postgres 16 · FastAPI · API Key SHA256+pepper rolling 14d.

## Subir

```bash
cp .env.example .env
docker compose up -d --build
```

- Airflow UI: http://localhost:8080
- API: http://localhost:8000/docs · `/health`

## API key

```bash
docker compose exec api python scripts/create_api_key.py --name minha-maquina [--user-id 1]
```

Request: header `X-API-Key: <raw>`.

## Endpoints

| Método | Rota | Auth |
|---|---|---|
| GET | `/health` | pública |
| GET | `/review/{account_id}[?start-date=&end-date=]` | token |
| GET/PUT | `/budget/{account_id}[/{platform}]` | token |
| GET | `/google[/{yesterday\|lastweek\|lastmonth}]` `[?start-date=&end-date=&account_id=]` | token |
| GET | `/google/keywords?start-date=[&end-date=&account_id=]` | token |
| GET | `/meta[/{yesterday\|lastweek\|lastmonth}]` `[?start-date=&end-date=]` | token |
| GET | `/clientes[/{id}]` | token |
| GET | `/users` · POST `/createuser` | admin |

Range ausente ⇒ últimos 30 dias (review) ou dia único (demais). Datas ISO `YYYY-MM-DD`.

## Backfill

```bash
docker compose exec airflow airflow dags trigger google_ads \
  --conf '{"start_date":"2026-04-01","end_date":"2026-04-15"}'
```

Ambos vazios = ontem. Só um preenchido = erro (RN16).

## Testes

```bash
pytest -q           # suite completa
python -m mutmut run   # mutation testing nos módulos de RN pura (setup.cfg)
```

## Docs

- [BUSINESS_RULES](docs/BUSINESS_RULES.md) — RNs + testes
- [REQUIREMENTS](docs/REQUIREMENTS.md) — REQs + env vars + fora de escopo
- [SDD](docs/SDD.md) — arquitetura + fluxo + cobertura
