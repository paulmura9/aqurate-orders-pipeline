"""A small HTTP helper with retries.

Both upstreams (the orders REST endpoint and Frankfurter) are third-party
services: transient 5xx responses and timeouts are normal and must not fail the
daily run, while a persistent failure must fail it loudly.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}


class HttpError(RuntimeError):
    """Raised when a request keeps failing after all retries."""


def get_json(
    session: requests.Session,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 30,
    max_retries: int = 5,
) -> tuple[Any, requests.structures.CaseInsensitiveDict]:
    """GET a JSON document, returning ``(payload, response_headers)``."""
    delay = 1.0
    last_error: str = ""

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, params=params, headers=headers, timeout=timeout)
            if response.status_code in RETRYABLE_STATUS:
                last_error = f"HTTP {response.status_code}"
            else:
                response.raise_for_status()
                return response.json(), response.headers
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        except requests.HTTPError as exc:  # non-retryable status
            raise HttpError(f"GET {url} failed: {exc}") from exc

        if attempt < max_retries:
            log.warning("GET %s failed (%s), retry %s/%s in %.1fs", url, last_error, attempt, max_retries, delay)
            time.sleep(delay)
            delay *= 2

    raise HttpError(f"GET {url} failed after {max_retries} attempts: {last_error}")
