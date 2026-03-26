import psycopg2.extensions
import pytest

from src.db.connection import get_connection


class TestGetConnection:
    def test_returns_psycopg2_connection(self, memory_connection):
        assert isinstance(memory_connection, psycopg2.extensions.connection)

    def test_connection_is_usable(self, memory_connection):
        with memory_connection.cursor() as cur:
            cur.execute("CREATE TABLE test_conn (id INTEGER)")
            cur.execute("SELECT * FROM test_conn")
            rows = cur.fetchall()
        assert rows == []

    def test_connection_is_reusable(self, memory_connection):
        with memory_connection.cursor() as cur:
            cur.execute("CREATE TABLE test_reuse (id INTEGER)")
            cur.execute("INSERT INTO test_reuse VALUES (1)")
        memory_connection.commit()

        with memory_connection.cursor() as cur:
            cur.execute("SELECT * FROM test_reuse")
            result = cur.fetchall()
        assert result == [(1,)]

    def test_retries_on_failure(self):
        with pytest.raises(Exception):
            get_connection("postgresql://invalid:5432/nonexistent", retries=1, delay=0)
