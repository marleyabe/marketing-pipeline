from datetime import datetime

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values


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
    now = datetime.now()
    payload_columns = list(data[0].keys())
    columns = payload_columns + ["_extracted_at", "_source"]
    values = [
        tuple(row.get(column) for column in payload_columns) + (now, source)
        for row in data
    ]
    column_identifiers = sql.SQL(", ").join(sql.Identifier(column) for column in columns)
    base_query = sql.SQL("INSERT INTO {schema}.{table} ({columns}) VALUES %s").format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
        columns=column_identifiers,
    )
    if conflict_columns:
        update_columns = [column for column in columns if column not in conflict_columns]
        update_clause = sql.SQL(", ").join(
            sql.SQL("{column} = EXCLUDED.{column}").format(column=sql.Identifier(column))
            for column in update_columns
        )
        conflict_target = sql.SQL(", ").join(
            sql.Identifier(column) for column in conflict_columns
        )
        query = sql.SQL("{base} ON CONFLICT ({conflict}) DO UPDATE SET {updates}").format(
            base=base_query,
            conflict=conflict_target,
            updates=update_clause,
        )
    else:
        query = base_query
    with connection.cursor() as cursor:
        execute_values(cursor, query.as_string(cursor), values)
    if not connection.autocommit:
        connection.commit()
    return len(data)
