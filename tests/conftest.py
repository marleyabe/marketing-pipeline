import os
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("API_KEY_PEPPER", "test-pepper")


class FakeCursor:
    def __init__(self, rows_queue):
        self._rows_queue = rows_queue
        self._current: list = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, parameters=None):
        self._current = self._rows_queue.pop(0) if self._rows_queue else []

    def fetchone(self):
        return self._current[0] if self._current else None

    def fetchall(self):
        return list(self._current)


class FakeConn:
    def __init__(self, rows_queue: list[list]):
        self.rows_queue = rows_queue
        self.autocommit = True

    def cursor(self):
        return FakeCursor(self.rows_queue)

    def close(self):
        pass


@pytest.fixture
def fake_conn():
    def _make(rows_queue: list[list]) -> FakeConn:
        return FakeConn(rows_queue)

    return _make


@pytest.fixture
def mock_request():
    request = MagicMock()
    request.method = "GET"
    request.url.path = "/test"
    request.client.host = "127.0.0.1"
    return request
