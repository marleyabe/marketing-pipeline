import time

import psycopg2
import psycopg2.extensions


def get_connection(dsn: str = "", retries: int = 10, delay: float = 3.0) -> psycopg2.extensions.connection:
    """Return a psycopg2 connection to the given DSN, retrying on transient errors."""
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(dsn)
            conn.autocommit = False
            return conn
        except psycopg2.OperationalError:
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise
