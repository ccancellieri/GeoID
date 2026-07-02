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

"""Storage module error types."""


class ReadOnlyDriverError(Exception):
    """Write attempted on a read-only driver."""


class SoftDeleteNotSupportedError(Exception):
    """Soft delete requested but driver doesn't support it."""


class ConflictError(Exception):
    """Write refused because identity resolution matched an existing entity
    and the collection write policy rejects the write (``REFUSE_FAIL``).

    Carries the matched ``geoid`` and the ``matcher`` name that triggered the
    conflict so callers can surface actionable diagnostics (HTTP 409 body,
    batch error reports).
    """

    def __init__(
        self,
        message: str,
        *,
        geoid: object | None = None,
        matcher: str | None = None,
    ) -> None:
        super().__init__(message)
        self.geoid = geoid
        self.matcher = matcher


class RequiredFieldMissingError(Exception):
    """A ``FieldDefinition.required=True`` field was null/missing on write.

    Raised either by the service-layer fallback helper (when the primary
    driver lacks ``Capability.REQUIRED_ENFORCEMENT``) or surfaced from a
    driver-native NOT NULL violation. Maps to HTTP 400.
    """

    def __init__(self, message: str, *, field: str | None = None) -> None:
        super().__init__(message)
        self.field = field


class UnknownFieldsError(Exception):
    """A feature carried fields not declared in the collection's
    ``ItemsSchema.fields`` while ``strict_unknown_fields=True``.

    Raised by the service-layer pre-write helper to enforce the strict-mode
    contract: collections that declare a schema with strict mode reject any
    write whose properties contain a field outside the declared set.
    Always service-layer enforcement (no driver-native equivalent — drivers
    that store JSON properties accept any field by default). Maps to HTTP 422.
    """

    def __init__(
        self,
        message: str,
        *,
        unknown_fields: list[str] | None = None,
        allowed_fields: list[str] | None = None,
    ) -> None:
        super().__init__(message)
        self.unknown_fields = unknown_fields or []
        self.allowed_fields = allowed_fields or []


class SidecarRejectedError(Exception):
    """A sidecar refused a feature at ingestion time.

    Raised instead of silently returning ``None`` so the batch caller can
    aggregate a structured ``IngestionReport`` and surface the rejection to
    the client (HTTP 200/207), instead of producing a mismatch between
    submitted count and accepted count.
    """

    def __init__(
        self,
        message: str,
        *,
        geoid: str | None = None,
        external_id: str | None = None,
        sidecar_id: str | None = None,
        matcher: str | None = None,
        reason: str = "sidecar_rejected",
    ) -> None:
        super().__init__(message)
        self.geoid = geoid
        self.external_id = external_id
        self.sidecar_id = sidecar_id
        self.matcher = matcher
        self.reason = reason


class IndexMappingMismatchError(Exception):
    """Live ES index mapping is missing a field the writer just sent.

    Surfaces when an ES write fails with ``illegal_argument_exception``
    because a code-side field was added after the index was created and
    the index was never re-rolled. Maps to HTTP 503 + Retry-After:
    operator action (recreate index / reindex) fixes it; the request
    itself is well-formed.
    """

    def __init__(
        self,
        message: str,
        *,
        index: str | None = None,
        field: str | None = None,
    ) -> None:
        super().__init__(message)
        self.index = index
        self.field = field


class EsBulkWriteError(Exception):
    """One or more documents in an ES ``_bulk`` call were rejected by the
    cluster (HTTP 200 response with ``"errors": true`` and per-doc
    ``status >= 300`` or ``"error"`` key).

    Raised by every inline ES write path so the dispatcher's
    ``on_failure`` policy can apply: ``OUTBOX`` enqueues for the drain
    worker; ``FATAL`` rolls back the wrapping transaction.

    Carries the list of per-item failures so callers can log structured
    diagnostics without re-parsing the raw ES response.

    Attributes
    ----------
    failures:
        List of ``(id, reason)`` tuples for every rejected document.
        ``id`` is the document's ``_id`` as echoed by ES (or the
        submitted id when ES omits ``_id`` from the error entry).
        ``reason`` is a compact ``"{status} {err_type}: {err_reason}"``
        string built by the classifier.
    acknowledged:
        List of ids ES actually acknowledged (``2xx``, no error — including
        ids recovered by the geo_shape degradation ladder) among the ids
        submitted in the same ``_bulk`` call that raised this error. A
        rejected sub-chunk is not an all-or-nothing outcome: ES's ``_bulk``
        endpoint is per-item, so some documents in the same request as a
        rejection are still indexed. Callers crediting a batch as "written"
        MUST use this list rather than assuming every non-failed id
        succeeded (#2799).
    """

    def __init__(
        self,
        message: str,
        *,
        failures: list[tuple[str, str]] | None = None,
        acknowledged: list[str] | None = None,
    ) -> None:
        super().__init__(message)
        self.failures: list[tuple[str, str]] = failures or []
        self.acknowledged: list[str] = acknowledged or []


class UniqueConstraintViolationError(Exception):
    """A ``FieldDefinition.unique=True`` field collided with an existing value.

    Raised either by the service-layer fallback helper (when the primary
    driver lacks ``Capability.UNIQUE_ENFORCEMENT``) or surfaced from a
    driver-native UNIQUE violation. Maps to HTTP 409.
    """

    def __init__(
        self,
        message: str,
        *,
        field: str | None = None,
        value: object | None = None,
    ) -> None:
        super().__init__(message)
        self.field = field
        self.value = value
