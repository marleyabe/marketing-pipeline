"""Retry local com backoff exponencial + jitter para erros transientes.

Seguindo recomendação oficial do Google Ads API:
https://developers.google.com/google-ads/api/docs/best-practices/understand-api-errors
Retry apenas em: UNAVAILABLE, DEADLINE_EXCEEDED, INTERNAL, UNKNOWN, ABORTED,
RESOURCE_EXHAUSTED. Erros de autenticação / quota permanente / bad request
NÃO devem ser retryed — quebram rápido.

Retry do Airflow sozinho não basta: ele reexecuta a task inteira, perdendo
parciais. Este retry atua por (conta, dia) dentro da task.
"""

import logging
import random
import time
from typing import Callable, TypeVar

from google.ads.googleads.errors import GoogleAdsException
from grpc import StatusCode

logger = logging.getLogger(__name__)

T = TypeVar("T")

TRANSIENT_GRPC_CODES = {
    StatusCode.UNAVAILABLE,
    StatusCode.DEADLINE_EXCEEDED,
    StatusCode.INTERNAL,
    StatusCode.UNKNOWN,
    StatusCode.ABORTED,
    StatusCode.RESOURCE_EXHAUSTED,
}


def _is_transient(error: Exception) -> bool:
    if isinstance(error, GoogleAdsException):
        grpc_error = getattr(error, "error", None)
        code = grpc_error.code() if grpc_error is not None else None
        return code in TRANSIENT_GRPC_CODES
    return False


def _sleep_with_jitter(attempt: int, base_seconds: float, cap_seconds: float) -> None:
    # Exponential backoff com full jitter (AWS architecture blog recipe)
    # delay aleatório em [0, min(cap, base * 2^attempt))
    upper = min(cap_seconds, base_seconds * (2 ** attempt))
    delay = random.uniform(0, upper)
    time.sleep(delay)


def call_with_retry(
    func: Callable[[], T],
    label: str,
    max_attempts: int = 5,
    base_seconds: float = 2.0,
    cap_seconds: float = 60.0,
) -> T:
    """Executa `func` com retry em erros transientes do Google Ads.

    Re-raise erros não-transientes imediatamente. Após esgotar tentativas em
    transientes, re-raise o último erro para o Airflow fazer retry da task.
    """
    last_error: Exception | None = None
    for attempt in range(max_attempts):
        try:
            return func()
        except Exception as error:  # noqa: BLE001 — classificador dedica abaixo
            if not _is_transient(error):
                raise
            last_error = error
            if attempt == max_attempts - 1:
                break
            logger.warning(
                "retry %s attempt=%d/%d transient=%s", label, attempt + 1,
                max_attempts, type(error).__name__,
            )
            _sleep_with_jitter(attempt, base_seconds, cap_seconds)
    assert last_error is not None
    raise last_error
