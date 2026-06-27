#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""GCS operation retry core with per-bucket circuit breaker.

Provides a focused async retry helper that:

* Emits a structured log line on each non-final retry attempt::

    gcs_operation_retry bucket=%s operation=%s attempt=%d/%d error=%s

  A single GCP log-based metric targeting this text covers every GCS retry
  site regardless of caller, making retry-rate-per-bucket/operation charts
  trivial to build.

* Integrates with :class:`~dynastore.modules.storage.circuit_breaker.CircuitBreaker`
  keyed by *bucket name* so that a single unhealthy bucket trips its own
  circuit independently of all other buckets.  The existing
  :class:`~dynastore.modules.storage.index_dispatcher.IndexDispatcher` breaker
  is reused without modification — only the key domain changes (bucket name
  vs. indexer_id).

This module covers GCS bucket API calls only.  Pub/Sub operations
(topic/subscription endpoints) use a different service and already have
their own bounded retry loops in :mod:`gcp_eventing_ops`; they are not
wrapped here.

Retry tunables and breaker thresholds are driven by
:class:`~dynastore.modules.gcp.gcp_config.GcpModuleConfig` fields, kept
in module-level variables that are updated at lifespan startup (hot-reload
on process restart, consistent with the catalog-visibility tunables pattern).
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Awaitable, Callable, Optional, Tuple, TypeVar

if TYPE_CHECKING:
    from dynastore.modules.storage.circuit_breaker import CircuitBreaker

logger = logging.getLogger(__name__)

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Transient-error classifier
# ---------------------------------------------------------------------------


def _is_transient_gcs_error(exc: BaseException) -> bool:
    """Return True for GCS errors that are safe to retry on a fresh attempt.

    Covers the google-api-core exception hierarchy for transient service
    degradation (503, 500, ABORTED, DEADLINE_EXCEEDED) and low-level
    connectivity failures.

    Deliberately does **not** match:

    * ``NotFound`` — bucket not yet visible is an eventual-consistency probe
      handled by the caller (e.g. ``wait_for_bucket_ready``), not a transient
      service error.
    * ``Forbidden`` / ``PermissionDenied`` — permanent; retrying is pointless.
    * ``Conflict`` / ``AlreadyExists`` — idempotent-op collisions resolved by
      the caller, not a service degradation.
    """
    try:
        from google.api_core import exceptions as gexc

        return isinstance(
            exc,
            (
                gexc.ServiceUnavailable,
                gexc.InternalServerError,
                gexc.Unknown,
                gexc.DeadlineExceeded,
                gexc.Aborted,
                ConnectionError,
                TimeoutError,
            ),
        )
    except ImportError:
        return isinstance(exc, (ConnectionError, TimeoutError))


# ---------------------------------------------------------------------------
# Tunable reader (hot-reload via module-level variables)
# ---------------------------------------------------------------------------


def _get_gcs_retry_tunables() -> Tuple[int, int, float]:
    """Return live GCS retry tunables from the gcp_module module-level variables.

    Variables are updated from ``GcpModuleConfig`` during lifespan startup,
    giving hot-reload-on-restart semantics consistent with the
    ``_CATALOG_VISIBILITY_*`` pattern in :mod:`gcp_catalog_ops`.

    Returns ``(max_attempts, breaker_failure_threshold, breaker_cooldown_seconds)``.
    """
    from dynastore.modules.gcp import gcp_module as _mod

    return (
        _mod._GCS_RETRY_MAX_ATTEMPTS,
        _mod._GCS_BREAKER_FAILURE_THRESHOLD,
        _mod._GCS_BREAKER_COOLDOWN_SECONDS,
    )


# ---------------------------------------------------------------------------
# Core retry helper
# ---------------------------------------------------------------------------


async def gcs_run_with_retry(
    call: Callable[[], Awaitable[T]],
    *,
    bucket: str,
    operation: str,
    breaker: "CircuitBreaker",
    max_attempts: Optional[int] = None,
    base_delay: float = 1.0,
) -> T:
    """Async retry wrapper for GCS operations with observability and circuit breaker.

    On each non-final retry attempt emits a structured WARNING:

        gcs_operation_retry bucket=%s operation=%s attempt=%d/%d error=%s

    A single GCP log-based metric targeting this text pattern covers all GCS
    retry sites regardless of which operation or bucket triggered the retry.

    The ``breaker`` is keyed by ``bucket``: one wedged bucket opens its own
    circuit independently of every other bucket.  Only calls that raise a
    *transient* GCS error (see :func:`_is_transient_gcs_error`) feed the
    failure counter — logical errors (``NotFound``, ``PermissionDenied``)
    are not retried and propagate immediately, though they still increment
    the failure counter so a sustained logical-error stream eventually trips
    the breaker too.

    The function never holds a database connection — it is safe to call
    between the three-phase DB splits in :mod:`bucket_service`.

    :param call: Zero-arg async thunk wrapping the GCS API call.
    :param bucket: GCS bucket name — circuit-breaker key and log field.
    :param operation: Short descriptive label for the log (e.g. ``"patch_cors"``,
        ``"list_notifications"``).
    :param breaker: Shared :class:`CircuitBreaker` held on ``GCPModule``
        and keyed per-bucket.  Pass ``GCPModule._gcs_breaker``.
    :param max_attempts: Total attempt budget (1 = no retry).  Defaults to
        the live module tunable from :func:`_get_gcs_retry_tunables`.
    :param base_delay: Base for the ``base_delay * 2**attempt`` exponential
        back-off in seconds between retry attempts.
    """
    from dynastore.modules.gcp.errors import GcpServiceUnavailableError

    if max_attempts is None:
        max_attempts = _get_gcs_retry_tunables()[0]

    # Fast-fail if the bucket's circuit is already open.
    if breaker.is_open(bucket):
        raise GcpServiceUnavailableError(
            f"GCS bucket '{bucket}' circuit is OPEN — skipping '{operation}' "
            "to avoid hammering a failing endpoint.",
            retry_after=30,
        )

    last_exc: Optional[BaseException] = None
    for attempt in range(max_attempts):
        try:
            result = await call()
            breaker.record_success(bucket)
            return result  # type: ignore[return-value]
        except BaseException as exc:
            last_exc = exc
            # Always record to the breaker, including non-transient errors.
            # A sustained permission error or misconfiguration should still
            # eventually trip the breaker to surface the problem clearly.
            breaker.record_failure(bucket)

            if not _is_transient_gcs_error(exc):
                # Non-transient: propagate immediately without burning retry budget.
                raise

            if attempt == max_attempts - 1:
                # Final attempt exhausted — surface the original error.
                raise

            delay = base_delay * (2**attempt)
            logger.warning(
                "gcs_operation_retry bucket=%s operation=%s attempt=%d/%d error=%s",
                bucket,
                operation,
                attempt + 1,
                max_attempts,
                f"{type(exc).__name__}: {exc}",
            )

            # Re-check the breaker: the record_failure above may have just tripped
            # it, in which case continuing to sleep-and-retry is wasteful.
            if breaker.is_open(bucket):
                raise GcpServiceUnavailableError(
                    f"GCS bucket '{bucket}' circuit opened during '{operation}' "
                    "after consecutive failures — aborting retry.",
                    retry_after=30,
                ) from exc

            await asyncio.sleep(delay)

    # Unreachable: the loop always raises on the final attempt.
    assert last_exc is not None
    raise last_exc
