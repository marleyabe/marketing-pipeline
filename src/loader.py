from datetime import datetime

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values


def _build_rows(data: list[dict], source: str) -> tuple[list[str], list[tuple]]:
    now = datetime.now()
    payload_columns = list(data[0].keys())
    columns = payload_columns + ["_extracted_at", "_source"]
    values = [
        tuple(row.get(column) for column in payload_columns) + (now, source)
        for row in data
    ]
    return columns, values


def _insert_query(schema: str, table: str, columns: list[str]) -> sql.Composed:
    column_identifiers = sql.SQL(", ").join(sql.Identifier(column) for column in columns)
    return sql.SQL("INSERT INTO {schema}.{table} ({columns}) VALUES %s").format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
        columns=column_identifiers,
    )


def _upsert_query(base: sql.Composed, columns: list[str], conflict_columns: list[str]) -> sql.Composed:
    update_columns = [column for column in columns if column not in conflict_columns]
    update_clause = sql.SQL(", ").join(
        sql.SQL("{column} = EXCLUDED.{column}").format(column=sql.Identifier(column))
        for column in update_columns
    )
    conflict_target = sql.SQL(", ").join(
        sql.Identifier(column) for column in conflict_columns
    )
    return sql.SQL("{base} ON CONFLICT ({conflict}) DO UPDATE SET {updates}").format(
        base=base, conflict=conflict_target, updates=update_clause,
    )


def load_bronze(
    connection: psycopg2.extensions.connection,
    data: list[dict],
    table: str,
    source: str,
    schema: str = "bronze",
    conflict_columns: list[str] | None = None,
) -> int:
    if not data:
        return 0
    columns, values = _build_rows(data, source)
    query = _insert_query(schema, table, columns)
    if conflict_columns:
        query = _upsert_query(query, columns, conflict_columns)
    with connection.cursor() as cursor:
        execute_values(cursor, query.as_string(cursor), values)
    if not connection.autocommit:
        connection.commit()
    return len(data)
