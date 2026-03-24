# Plano: Ads2u Marketing Pipeline

## Contexto

Agência de tráfego pago com 20+ contas de clientes no Meta Ads (Business Manager) e Google Ads (MCC) precisa automatizar a coleta de dados, geração de relatórios diários/semanais e alertas de desempenho. O projeto anterior foi descartado para recomeçar do zero com abordagem TDD.

**Decisões técnicas:**
- **Banco**: DuckDB (analytics) + PostgreSQL (apenas metadata do Airflow)
- **Orquestrador**: Airflow 3.x com CeleryExecutor
- **Transformações**: SQL direto no DuckDB via Python (sem DBT)
- **Contas**: Auto-descoberta via Business Manager (Meta) e MCC (Google)
- **Config**: Defaults globais (sem YAML de clientes)
- **Escopo**: Coleta → Banco → Relatórios → Alertas (sem dashboards, sem bot/IA)
- **Entrega**: Relatórios e alertas ficam no banco apenas
- **Alertas**: Queda percentual vs período anterior
- **Coleta**: 1x/dia
- **Infra**: VPS

---

## Estrutura do Projeto

```
marketing-pipeline/
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yaml
├── pyproject.toml
├── requirements.txt
├── context.md
├── plan.md
├── config/
│   └── airflow.cfg
├── data/                          # DuckDB files (gitignored)
├── sql/
│   ├── silver/
│   │   ├── silver_meta_ads.sql
│   │   ├── silver_google_ads.sql
│   │   └── silver_unified_campaigns.sql
│   └── gold/
│       ├── gold_daily_performance.sql
│       ├── gold_weekly_performance.sql
│       ├── gold_reports_daily.sql
│       ├── gold_reports_weekly.sql
│       └── gold_alerts.sql
├── src/
│   ├── __init__.py
│   ├── db/
│   │   ├── __init__.py
│   │   ├── connection.py          # DuckDB connection manager
│   │   └── schema.py             # Schema init (bronze/silver/gold)
│   ├── extractors/
│   │   ├── __init__.py
│   │   ├── base.py               # Abstract base extractor
│   │   ├── meta_ads.py           # Meta Ads API client
│   │   └── google_ads.py         # Google Ads API client
│   ├── loaders/
│   │   ├── __init__.py
│   │   └── duckdb_loader.py      # Bronze layer loader
│   ├── transformers/
│   │   ├── __init__.py
│   │   └── sql_runner.py         # Executa SQLs de transformação
│   ├── reports/
│   │   ├── __init__.py
│   │   ├── daily.py
│   │   └── weekly.py
│   └── alerts/
│       ├── __init__.py
│       └── detector.py
├── dags/
│   ├── daily_extract_meta_ads.py
│   ├── daily_extract_google_ads.py
│   ├── daily_transform.py
│   ├── daily_reports.py
│   └── daily_alerts.py
├── tests/
│   ├── conftest.py
│   ├── unit/
│   │   ├── test_duckdb_connection.py
│   │   ├── test_schema_init.py
│   │   ├── test_meta_extractor.py
│   │   ├── test_google_extractor.py
│   │   ├── test_duckdb_loader.py
│   │   ├── test_sql_runner.py
│   │   ├── test_daily_report.py
│   │   ├── test_weekly_report.py
│   │   ├── test_alert_detector.py
│   │   └── test_dag_structure.py
│   └── integration/
│       ├── test_bronze_to_silver.py
│       ├── test_silver_to_gold.py
│       └── test_full_pipeline.py
└── plugins/
```

---

## Templates dos Relatórios

**Diário:**
```
**Nome da Conta**
* Data: dd/mm/yyyy
* Investimento: R$0,0
* Impressões: 0
* Cliques: 0
* Conversões: 0
* Custo por conversão: R$0,0
* Taxa de Conversão: 0%
```

**Semanal:**
```
📊 Relatório Semanal – Nome da Conta
📅 Período: dd/mm a dd/mm

📢 Impressões: 0
🖱️ Cliques: 0
🎯 Conversões: 0
💲 Custo por conversão: R$ 0,0
💰 Investimento: R$ 0,0
```

---

## Fases de Implementação

### Fase 0: Fundação e Infraestrutura de Testes

**Objetivo:** Estrutura do projeto, config de testes, conexão DuckDB.

**Testes primeiro (TDD):**
- `test_duckdb_connection.py`: conexão retorna objeto válido, cria arquivo .duckdb, modo in-memory, conexão reutilizável
- `test_schema_init.py`: schemas bronze/silver/gold criados, tabelas bronze corretas, init idempotente

**Implementar:**
- `pyproject.toml` — pytest config, dependências (pytest, pytest-cov, duckdb, pandas)
- `src/db/connection.py` — connection manager DuckDB (file ou :memory:)
- `src/db/schema.py` — CREATE SCHEMA IF NOT EXISTS + DDL tabelas bronze
- `tests/conftest.py` — fixtures (DuckDB in-memory, dados de exemplo)
- `.env.example`, `.gitignore`, `requirements.txt`

**Verificação:** `pytest tests/unit/test_duckdb_connection.py tests/unit/test_schema_init.py -v`

---

### Fase 1: Extractors (APIs Mockadas)

**Objetivo:** Classes de extração que auto-descobrem contas e extraem dados. Desacopladas do Airflow.

**Testes primeiro:**
- `test_meta_extractor.py`: auto-descobre todas contas ativas do Business Manager, retorna schema correto, trata erro de API por conta, response vazio, coerção de tipos
- `test_google_extractor.py`: auto-descobre contas enabled não-manager do MCC, schema correto, converte cost_micros, erro isolado por conta

**Implementar:**
- `src/extractors/base.py` — `BaseExtractor` abstrato com `list_accounts()` e `extract()`
- `src/extractors/meta_ads.py` — `MetaAdsExtractor`: auto-descobre via `User.get_ad_accounts()`, preserva `actions` JSON
- `src/extractors/google_ads.py` — `GoogleAdsExtractor`: auto-descobre via GAQL no MCC, converte cost_micros

**Verificação:** `pytest tests/unit/test_meta_extractor.py tests/unit/test_google_extractor.py -v`

---

### Fase 2: Bronze Layer Loader

**Objetivo:** Loader genérico que grava no DuckDB bronze.

**Testes primeiro:**
- `test_duckdb_loader.py`: cria tabela se não existe, append funciona, adiciona `_extracted_at` e `_source`, preserva JSON, empty data é noop, deduplicação por chave natural

**Implementar:**
- `src/loaders/duckdb_loader.py` — `DuckDBBronzeLoader`: carrega list[dict] via DataFrame, adiciona metadata columns

**Verificação:** `pytest tests/unit/test_duckdb_loader.py -v`

---

### Fase 3: Transformações SQL — Silver e Gold

**Objetivo:** Transformações medallion via SQL puro no DuckDB.

**Testes primeiro:**
- `test_sql_runner.py`: executa SQL de arquivo, executa em ordem, trata erros, idempotente
- `test_bronze_to_silver.py`: deduplica, converte cost_micros, padroniza datas, substitui nulls, pivota actions JSON para coluna `conversions`
- `test_silver_to_gold.py`: agregações corretas dia/semana, CTR/CPC/custo por conversão calculados

**Implementar:**
- `src/transformers/sql_runner.py` — `SQLRunner`: lê .sql e executa no DuckDB em ordem
- **sql/silver/**
  - `silver_meta_ads.sql` — deduplica, parseia actions JSON, extrai conversões totais
  - `silver_google_ads.sql` — deduplica, converte cost_micros
  - `silver_unified_campaigns.sql` — UNION ALL com schema comum
- **sql/gold/**
  - `gold_daily_performance.sql` — agrega por (account_id, date), calcula métricas derivadas
  - `gold_weekly_performance.sql` — agrega por (account_id, semana)
  - `gold_reports_daily.sql` — pré-formata relatório diário
  - `gold_reports_weekly.sql` — pré-formata relatório semanal
  - `gold_alerts.sql` — compara período atual vs anterior, detecta quedas

**Defaults globais de alertas (constantes no código):**
- Queda de investimento > 30% → warning
- Queda de investimento > 50% → critical
- Queda de conversões > 50% → warning
- Queda de conversões > 70% → critical

**Verificação:** `pytest tests/unit/test_sql_runner.py tests/integration/test_bronze_to_silver.py tests/integration/test_silver_to_gold.py -v`

---

### Fase 4: Relatórios e Alertas

**Objetivo:** Geração de relatórios formatados e detecção de alertas, gravados no banco.

**Testes primeiro:**
- `test_daily_report.py`: formato correto (R$, dd/mm/yyyy, %), um por conta, gravado em `gold.generated_reports`
- `test_weekly_report.py`: formato semanal, período segunda-domingo
- `test_alert_detector.py`: queda detectada, severidade correta, gravado em `gold.active_alerts`

**Implementar:**
- `src/reports/daily.py` — lê gold, formata template, grava em `gold.generated_reports`
- `src/reports/weekly.py` — idem formato semanal
- `src/alerts/detector.py` — lê `gold.gold_alerts`, grava em `gold.active_alerts`

**Verificação:** `pytest tests/unit/test_daily_report.py tests/unit/test_weekly_report.py tests/unit/test_alert_detector.py -v`

---

### Fase 5: Airflow DAGs

**Objetivo:** DAGs finas que orquestram `src/`.

**Testes primeiro:**
- `test_dag_structure.py`: DAGs carregam sem erro, schedule `@daily`, dependências corretas

**Implementar:**
- `dags/daily_extract_meta_ads.py` — dynamic task mapping `.expand()` para todas as contas
- `dags/daily_extract_google_ads.py` — idem
- `dags/daily_transform.py` — `SQLRunner` (silver → gold)
- `dags/daily_reports.py` — diário + semanal aos domingos
- `dags/daily_alerts.py` — diário + semanal às segundas

**Verificação:** `pytest tests/unit/test_dag_structure.py -v` + Airflow UI

---

### Fase 6: Testes de Integração E2E

**Objetivo:** Pipeline completo com dados sintéticos.

**Testes:**
- `test_full_pipeline.py`: E2E completo (mock API → bronze → silver → gold → relatório → alerta)

**Verificação:** `pytest tests/integration/ -v` + `pytest --cov=src --cov-report=html`

---

### Fase 7: Docker

**Objetivo:** Docker funcional para deploy em VPS.

**Implementar:**
- `docker-compose.yaml` — PostgreSQL + Redis + Airflow services + volume `data/`
- `Dockerfile` — Airflow com DuckDB, `src/` no PYTHONPATH
- `.gitignore` final

---

## Sequência de Dependências

```
Fase 0 (Fundação) → Fase 1 (Extractors) → Fase 2 (Loader) → Fase 3 (SQL Transforms)
                                                                       ↓
                                                                 Fase 4 (Relatórios/Alertas)
                                                                       ↓
                                                                 Fase 5 (Airflow DAGs)
                                                                       ↓
                                                                 Fase 6 (Integração E2E)
                                                                       ↓
                                                                 Fase 7 (Docker)
```

---

## Riscos e Mitigações

| Risco | Mitigação |
|---|---|
| **DuckDB single-writer** | Transformações rodam depois de todos os writes |
| **Rate limits Meta/Google (20+ contas)** | Retry exponential backoff; Airflow `pool` |
| **Ordem dos SQLs** | `SQLRunner` executa em ordem alfabética |


# Validar tokens
https://console.cloud.google.com/
https://developers.google.com/oauthplayground
https://developers.facebook.com/apps/