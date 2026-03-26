from datetime import datetime

import psycopg2.extensions
import psycopg2.extras
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
        """Load data into a bronze table."""
        if not data:
            return

        df = pd.DataFrame(data)
        df["_extracted_at"] = datetime.now()
        df["_source"] = source

        columns = list(df.columns)
        col_names = ", ".join(columns)
        rows = [tuple(row) for row in df.itertuples(index=False)]

        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO {schema}.{table_name} ({col_names}) VALUES %s",
                rows,
            )
        self._conn.commit()
