import psycopg2

from src.db import get_pg


def pg() -> psycopg2.extensions.connection:
    connection = get_pg()
    try:
        yield connection
    finally:
        connection.close()
