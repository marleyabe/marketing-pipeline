import os

import duckdb
import pytest

from src.db.connection import get_connection


class TestGetConnection:
    def test_returns_duckdb_connection(self):
        conn = get_connection(":memory:")
        assert isinstance(conn, duckdb.DuckDBPyConnection)
        conn.close()

    def test_in_memory_mode(self):
        conn = get_connection(":memory:")
        conn.execute("CREATE TABLE test (id INTEGER)")
        result = conn.execute("SELECT * FROM test").fetchall()
        assert result == []
        conn.close()

    def test_creates_database_file(self, tmp_path):
        db_path = str(tmp_path / "test.duckdb")
        assert not os.path.exists(db_path)
        conn = get_connection(db_path)
        assert os.path.exists(db_path)
        conn.close()

    def test_connection_is_reusable(self):
        conn = get_connection(":memory:")
        conn.execute("CREATE TABLE test (id INTEGER)")
        conn.execute("INSERT INTO test VALUES (1)")
        result = conn.execute("SELECT * FROM test").fetchall()
        assert result == [(1,)]
        conn.close()
