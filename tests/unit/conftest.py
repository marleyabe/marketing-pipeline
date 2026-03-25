"""Mock airflow.sdk so DAG files can be imported without Airflow installed."""
import sys
from unittest.mock import MagicMock


class _MockDag:
    def __init__(self, schedule):
        self.schedule = schedule


def _dag(**kwargs):
    schedule = kwargs.get("schedule")

    def decorator(fn):
        def wrapper(*args, **kw):
            fn(*args, **kw)
            return _MockDag(schedule)

        return wrapper

    return decorator


class _MockTaskInstance:
    """Returned when a @task-decorated function is called."""

    def __rshift__(self, other):
        return other

    def expand(self, **kwargs):
        return _MockTaskInstance()


class _MockTask:
    """Wraps a function decorated with @task."""

    def __init__(self, fn):
        self._fn = fn

    def __call__(self, *args, **kwargs):
        return _MockTaskInstance()

    def expand(self, **kwargs):
        return _MockTaskInstance()


airflow_sdk = MagicMock()
airflow_sdk.dag = _dag
airflow_sdk.task = _MockTask
airflow_sdk.get_current_context = MagicMock(return_value={})

airflow_mod = MagicMock()
airflow_mod.sdk = airflow_sdk

sys.modules.setdefault("airflow", airflow_mod)
sys.modules.setdefault("airflow.sdk", airflow_sdk)
