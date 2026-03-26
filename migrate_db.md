# Migração DuckDB → PostgreSQL

## Visão Geral

O pipeline usa DuckDB como banco analítico com arquitetura medallion (bronze/silver/gold). A migração substitui o DuckDB por PostgreSQL mantendo a mesma estrutura de schemas e lógica de negócio.

**Impacto por área:**

| Área | Esforço | Motivo |
|---|---|---|
| `connection.py` | Baixo | Trocar driver |
| `schema.py` | Baixo | Ajuste de tipos e DDL |
| `duckdb_loader.py` | Médio | `INSERT BY NAME` não existe no Postgres |
| `sql_runner.py` | Baixo | Cursor + commit explícito |
| SQLs silver (meta) | Alto | Funções JSON/lista exclusivas do DuckDB |
| SQLs silver (google) | Baixo | Apenas casting e DDL |
| SQLs gold | Baixo | `CREATE OR REPLACE TABLE` + `DATE_TRUNC` |
| `detector.py` / reports | Baixo | Placeholder `?` → `%s` |
| DAGs | Baixo | Variável de ambiente |
| Testes | Médio | Sem `:memory:`, precisa de Postgres de teste |

---

## Passo 1 — Dependências

**`requirements.txt` e `pyproject.toml`:**

```diff
- duckdb>=1.2.0
+ psycopg2-binary>=2.9
+ psycopg2>=2.9        # produção (compilado, sem bundled libs)
```

> Use `psycopg2-binary` em dev/CI e `psycopg2` na imagem Docker de produção.

---

## Passo 2 — Variáveis de Ambiente

**`.env.example`:**

```diff
- DUCKDB_PATH=data/ads2u.duckdb
+ DATABASE_URL=postgresql://ads2u:senha@localhost:5432/marketing_pipeline
```

Ou variáveis separadas (mais compatível com Airflow):

```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=marketing_pipeline
POSTGRES_USER=ads2u
POSTGRES_PASSWORD=senha
```

---

## Passo 3 — `src/db/connection.py`

```python
import time
import psycopg2
import psycopg2.extensions


def get_connection(
    dsn: str = "",
    retries: int = 10,
    delay: float = 3.0,
) -> psycopg2.extensions.connection:
    """Return a psycopg2 connection, retrying on transient errors."""
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(dsn)
            conn.autocommit = False
            return conn
        except psycopg2.OperationalError as e:
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise
```

Nos DAGs, passar `os.environ["DATABASE_URL"]` no lugar de `DUCKDB_PATH`.

---

## Passo 4 — `src/db/schema.py`

Duas mudanças:

**4a. `CREATE OR REPLACE TABLE` não existe no Postgres**

```diff
- CREATE OR REPLACE TABLE silver.meta_ads AS ...
+ DROP TABLE IF EXISTS silver.meta_ads;
+ CREATE TABLE silver.meta_ads AS ...
```

Aplicar em todos os arquivos `sql/silver/` e `sql/gold/`.

**4b. Tipo `DOUBLE` → `DOUBLE PRECISION`**

```diff
- spend DOUBLE,
+ spend DOUBLE PRECISION,
```

O casting `::DOUBLE` nos SQLs também deve virar `::DOUBLE PRECISION` (ou `::FLOAT8`).

**4c. Execução do schema**

O `schema.py` atual chama `conn.execute(sql)` diretamente. Com psycopg2:

```python
with conn.cursor() as cur:
    cur.execute(sql)
conn.commit()
```

---

## Passo 5 — `src/loaders/duckdb_loader.py`

O `INSERT INTO ... BY NAME SELECT * FROM df` é exclusivo do DuckDB — usa o DataFrame registrado na sessão.

**Substituir por `psycopg2.extras.execute_values`:**

```python
from datetime import datetime
import psycopg2
import psycopg2.extras
import psycopg2.extensions
import pandas as pd


class PostgresBronzeLoader:
    def __init__(self, connection: psycopg2.extensions.connection):
        self._conn = connection

    def load(
        self,
        data: list[dict],
        table_name: str,
        schema: str = "bronze",
        source: str = "",
    ) -> None:
        if not data:
            return

        df = pd.DataFrame(data)
        df["_extracted_at"] = datetime.now()
        df["_source"] = source

        columns = list(df.columns)
        col_names = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))

        rows = [tuple(row) for row in df.itertuples(index=False)]

        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO {schema}.{table_name} ({col_names}) VALUES %s",
                rows,
            )
        self._conn.commit()
```

> Renomear o arquivo para `postgres_loader.py` e atualizar os imports nos DAGs.

---

## Passo 6 — `src/transformers/sql_runner.py`

```python
import psycopg2.extensions


class SQLRunner:
    def __init__(self, connection: psycopg2.extensions.connection):
        self._conn = connection

    def _run_folder(self, folder: str) -> None:
        for sql_file in sorted(Path(folder).glob("*.sql")):
            sql = sql_file.read_text()
            with self._conn.cursor() as cur:
                cur.execute(sql)
            self._conn.commit()
```

---

## Passo 7 — SQLs Silver: `01_silver_meta_ads.sql` e `05_silver_meta_ads_demographics.sql`

Este é o passo mais trabalhoso. As funções `from_json`, `list_filter`, `list_transform`, `list_sum` e lambdas (`x -> ...`) **não existem no PostgreSQL**.

**Equivalente PostgreSQL** para o bloco de conversões:

```sql
-- DuckDB (original):
COALESCE(
    CASE
        WHEN actions IS NOT NULL AND actions != '' AND actions != '[]'
        THEN list_sum(
            list_transform(
                list_filter(
                    from_json(actions, '["json"]'),
                    x -> json_extract_string(x, '$.action_type') NOT IN (
                        'link_click', 'landing_page_view',
                        'page_engagement', 'post_engagement'
                    )
                ),
                x -> CAST(json_extract_string(x, '$.value') AS DOUBLE)
            )
        )
        ELSE 0.0
    END,
    0.0
) AS conversions
```

```sql
-- PostgreSQL (substituto):
COALESCE(
    (
        SELECT SUM((elem->>'value')::DOUBLE PRECISION)
        FROM jsonb_array_elements(
            CASE
                WHEN actions IS NOT NULL AND actions != '' AND actions != '[]'
                THEN actions::jsonb
                ELSE '[]'::jsonb
            END
        ) AS elem
        WHERE (elem->>'action_type') NOT IN (
            'link_click', 'landing_page_view',
            'page_engagement', 'post_engagement'
        )
    ),
    0.0
) AS conversions
```

A coluna `actions` no bronze deve ser `TEXT` (já é `VARCHAR` — compatível).

**Cabeçalho do arquivo:** substituir `CREATE OR REPLACE TABLE` conforme Passo 4a.

---

## Passo 8 — SQLs Silver: `02`, `03`, `04`

Apenas dois ajustes em cada:

1. `CREATE OR REPLACE TABLE` → `DROP TABLE IF EXISTS; CREATE TABLE`
2. `::DOUBLE` → `::DOUBLE PRECISION`

O restante (`::BIGINT`, `::VARCHAR`, `::DATE`, `ROW_NUMBER() OVER`, `COALESCE`, `UNION ALL`, `DATE_TRUNC`) é idêntico no PostgreSQL.

---

## Passo 9 — SQLs Gold

Mesmos dois ajustes do Passo 8 em todos os arquivos `sql/gold/`.

`DATE_TRUNC('week', date)::DATE` funciona igual no PostgreSQL.

---

## Passo 10 — `src/alerts/detector.py` e `src/reports/`

Trocar placeholder `?` por `%s` em todas as queries parametrizadas:

```diff
- self._conn.execute(SQL_QUERY, [check_date]).fetchall()
+ with self._conn.cursor() as cur:
+     cur.execute(SQL_QUERY, [check_date])
+     rows = cur.fetchall()
```

```diff
- self._conn.execute("INSERT INTO ... VALUES (?, ?, ?)", [a, b, c])
+ with self._conn.cursor() as cur:
+     cur.execute("INSERT INTO ... VALUES (%s, %s, %s)", [a, b, c])
+ self._conn.commit()
```

---

## Passo 11 — DAGs

```diff
- conn = get_connection(os.environ.get("DUCKDB_PATH", "data/ads2u.duckdb"))
+ conn = get_connection(os.environ["DATABASE_URL"])
```

---

## Passo 12 — Testes

O padrão `:memory:` do DuckDB não existe no PostgreSQL. Opções:

**Opção A (recomendada): `pytest-postgresql` com banco temporário**

```
uv add pytest-postgresql --dev
```

`tests/conftest.py`:

```python
import pytest
import psycopg2
from pytest_postgresql import factories

postgresql_proc = factories.postgresql_proc()
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
def conn(postgresql):
    yield postgresql


@pytest.fixture
def conn_with_schema(conn):
    from src.db.schema import init_schema
    init_schema(conn)
    yield conn
```

**Opção B: Docker Compose dedicado para testes**

`docker-compose.test.yml`:

```yaml
services:
  postgres-test:
    image: postgres:16
    environment:
      POSTGRES_DB: marketing_pipeline_test
      POSTGRES_USER: ads2u
      POSTGRES_PASSWORD: test
    ports:
      - "5433:5432"
```

Rodar testes com:
```bash
docker compose -f docker-compose.test.yml up -d
DATABASE_URL=postgresql://ads2u:test@localhost:5433/marketing_pipeline_test pytest
```

**Renomear arquivos de teste:**

```
tests/unit/test_duckdb_connection.py  → test_pg_connection.py
tests/unit/test_duckdb_loader.py      → test_pg_loader.py
```

---

## Passo 13 — `docker-compose.yaml` (produção)

O Postgres já existe no compose para o Airflow. Adicionar banco de dados dedicado ao pipeline:

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: marketing_pipeline
      POSTGRES_USER: ads2u
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

volumes:
  postgres_data:
```

Remover o volume `data/` (arquivos `.duckdb`).

---

## Ordem de Execução

```
Passo 1-2  (deps + env)
    ↓
Passo 3    (connection.py)     ← testes: test_pg_connection.py
    ↓
Passo 4    (schema.py)         ← testes: test_schema_init.py
    ↓
Passo 5    (loader)            ← testes: test_pg_loader.py
    ↓
Passo 6    (sql_runner)        ← testes: test_sql_runner.py
    ↓
Passo 7-9  (SQLs silver/gold)  ← testes: integration/
    ↓
Passo 10   (alerts + reports)  ← testes: test_alert_detector, test_*_report
    ↓
Passo 11   (DAGs)              ← testes: test_dag_structure.py
    ↓
Passo 12   (testes fixtures)
    ↓
Passo 13   (docker-compose)
```

---

## Checklist

- [ ] Substituir `duckdb` por `psycopg2-binary` em `requirements.txt` e `pyproject.toml`
- [ ] Adicionar `DATABASE_URL` ao `.env.example`
- [ ] Reescrever `src/db/connection.py`
- [ ] Ajustar `src/db/schema.py` (cursor, commit, tipos)
- [ ] Reescrever `src/loaders/duckdb_loader.py` → `postgres_loader.py`
- [ ] Ajustar `src/transformers/sql_runner.py` (cursor, commit)
- [ ] Reescrever JSON em `sql/silver/01_silver_meta_ads.sql`
- [ ] Reescrever JSON em `sql/silver/05_silver_meta_ads_demographics.sql`
- [ ] Ajustar DDL em `sql/silver/02`, `03`, `04`
- [ ] Ajustar DDL em todos `sql/gold/`
- [ ] Trocar `?` por `%s` em `src/alerts/detector.py`
- [ ] Trocar `?` por `%s` em `src/reports/daily.py` e `weekly.py`
- [ ] Atualizar imports nos DAGs (`DUCKDB_PATH` → `DATABASE_URL`)
- [ ] Reescrever fixtures em `tests/conftest.py`
- [ ] Renomear arquivos de teste (`duckdb_*` → `pg_*`)
- [ ] Atualizar `docker-compose.yaml`
- [ ] Rodar `pytest --cov=src` e garantir cobertura anterior
