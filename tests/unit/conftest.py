"""Mock airflow.sdk so DAG files can be imported without Airflow installed."""
import sys
from unittest.mock import MagicMock

_current_dag_tasks: dict = {}


class _MockDag:
    def __init__(self, schedule, task_dict=None):
        self.schedule = schedule
        self.task_dict = task_dict or {}


def _dag(**kwargs):
    schedule = kwargs.get("schedule")

    def decorator(fn):
        def wrapper(*args, **kw):
            global _current_dag_tasks
            _current_dag_tasks = {}
            fn(*args, **kw)
            return _MockDag(schedule, task_dict=dict(_current_dag_tasks))

        return wrapper

    return decorator


class _MockTaskInstance:
    """Returned when a @task-decorated function is called."""

    def __rshift__(self, other):
        return other

    def expand(self, **kwargs):
        return _MockTaskInstance()


class _MockTask:
    """Wraps a function decorated with @task.

    Supports both bare ``@task`` and parameterised ``@task(retries=3)`` usage.
    """

    def __init__(self, fn=None, *, retries=0, retry_delay=None, **kwargs):
        self._fn = fn
        self.retries = retries
        self.retry_delay = retry_delay
        if fn is not None:
            self.task_id = fn.__name__
            _current_dag_tasks[fn.__name__] = self

    def __call__(self, *args, **kwargs):
        # @task(retries=3)(fn) — second call wraps the function
        if self._fn is None and len(args) == 1 and callable(args[0]):
            return _MockTask(args[0], retries=self.retries, retry_delay=self.retry_delay)
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
