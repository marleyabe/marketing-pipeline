from datetime import datetime

import duckdb
import pandas as pd


class DuckDBBronzeLoader:
    def __init__(self, connection: duckdb.DuckDBPyConnection):
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

        self._conn.execute(
            f"INSERT INTO {schema}.{table_name} BY NAME SELECT * FROM df"
        )
