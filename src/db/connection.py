import time
import duckdb


def get_connection(path: str = ":memory:", retries: int = 10, delay: float = 3.0) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection to the given path, retrying on lock conflicts."""
    for attempt in range(retries):
        try:
            return duckdb.connect(path)
        except duckdb.IOException as e:
            if "lock" in str(e).lower() and attempt < retries - 1:
                time.sleep(delay)
            else:
                raise
