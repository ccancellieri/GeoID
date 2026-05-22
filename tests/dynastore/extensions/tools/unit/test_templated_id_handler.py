#    Copyright 2025 FAO
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

"""#1191 — an unsubstituted ``{{...}}`` template placeholder in a resource id
must surface as a clean 400 (actionable message), not as a 404 lookup-miss
on the routing-resolver path nor as a generic 422.

Two routes into ``ValidationExceptionHandler``:

- the typed ``InvalidIdentifierError`` raised by ``validate_sql_identifier``
  at guarded boundaries (e.g. ``require_catalog_ready``); and
- a raw ``Catalog '{{m.catalog}}' not found`` ``ValueError`` from a route that
  never validated the id and let the literal token fall through to the catalog
  lookup (the original ``routed-resolve unavailable`` symptom).

Both must map to 400; legitimate (non-templated) errors keep their existing
404 / 422 mapping.
"""

from __future__ import annotations

from dynastore.extensions.tools.exception_handlers import (
    ValidationExceptionHandler,
    handle_exception,
)
from dynastore.tools.db import InvalidIdentifierError


class TestTemplatedIdMapsTo400:
    def test_typed_invalid_identifier_error_maps_to_400(self) -> None:
        h = ValidationExceptionHandler()
        result = h.handle(
            InvalidIdentifierError(
                "Identifier '{{m.catalog}}' contains an unsubstituted template "
                "placeholder ('{{...}}'); substitute it."
            )
        )
        assert result is not None
        assert result.status_code == 400

    def test_lookup_miss_carrying_placeholder_maps_to_400_not_404(self) -> None:
        # A route that never validated the id lets the literal token reach the
        # catalog lookup, which raises ``... not found``. The "not found" text
        # would normally downgrade to 404 — but an unsubstituted placeholder is
        # a malformed request, so it must win as a 400.
        h = ValidationExceptionHandler()
        result = h.handle(ValueError("Catalog '{{m.catalog}}' not found."))
        assert result is not None
        assert result.status_code == 400
        assert "{{" in str(result.detail) or "placeholder" in str(result.detail).lower()


class TestRegressionsPreserved:
    def test_plain_not_found_still_404(self) -> None:
        h = ValidationExceptionHandler()
        result = h.handle(ValueError("Catalog 'real_cat' not found."))
        assert result is not None
        assert result.status_code == 404

    def test_plain_validation_error_still_422(self) -> None:
        h = ValidationExceptionHandler()
        result = h.handle(ValueError("cloud_cover must be between 0 and 100"))
        assert result is not None
        assert result.status_code == 422


class TestRegistryDispatch:
    """The resolver path routes through the global registry; pin both ends."""

    def test_registry_routes_typed_error_to_400(self) -> None:
        result = handle_exception(
            InvalidIdentifierError("Identifier '{{m.catalog}}' is templated.")
        )
        assert result.status_code == 400

    def test_registry_routes_placeholder_lookup_miss_to_400(self) -> None:
        result = handle_exception(
            ValueError("Catalog '{{m.catalog}}' not found."),
            resource_name="Catalog",
            operation="GET /features/catalogs/{{m.catalog}}/collections",
        )
        assert result.status_code == 400

    def test_registry_plain_not_found_still_404(self) -> None:
        result = handle_exception(
            ValueError("Catalog 'real_cat' not found."),
            resource_name="Catalog",
            operation="lookup",
        )
        assert result.status_code == 404
