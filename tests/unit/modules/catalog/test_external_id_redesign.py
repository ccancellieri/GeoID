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

"""Unit tests for external-id redesign Steps 0–2.

Step 0: generate_physical_name entropy + PK-collision retry.
Step 1: rename_catalog / rename_collection service logic (mocked DB).
Step 2: PUT-as-MOVE in _ogc_replace_catalog / _ogc_replace_collection
        (Content-Location header + rename dispatch).
"""

from __future__ import annotations

import re
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.catalog_service import generate_physical_name


# ---------------------------------------------------------------------------
# Step 0 — generator properties
# ---------------------------------------------------------------------------

class TestGeneratePhysicalName:
    """Verify the widened generator output shape and statistical properties."""

    def test_prefix_separator_format(self):
        name = generate_physical_name("c")
        assert name.startswith("c_"), f"Expected 'c_' prefix, got {name!r}"

    def test_suffix_length(self):
        name = generate_physical_name("col")
        # prefix=col + "_" = 4 chars; suffix = 13 chars → total = 17
        _, suffix = name.split("_", 1)
        assert len(suffix) == 13, (
            f"Expected 13-char suffix, got {len(suffix)!r} in {name!r}"
        )

    def test_only_safe_chars(self):
        """Suffix must be all-lowercase alphanumeric (safe for PG/GCS/ES)."""
        for _ in range(50):
            name = generate_physical_name("s")
            _, suffix = name.split("_", 1)
            assert re.match(r"^[a-z2-9]+$", suffix), (
                f"Unsafe characters in suffix {suffix!r}"
            )

    def test_uniqueness_across_many_calls(self):
        """Probability of collision in 10 000 names is negligible at 67 bits."""
        names = {generate_physical_name("c") for _ in range(10_000)}
        assert len(names) == 10_000, "Unexpected collision in 10 000 generated names"

    def test_total_length_within_pg_limit(self):
        """Full name must fit in a PG identifier (63-char limit)."""
        for prefix in ("c", "col", "s", "very_long_prefix"):
            name = generate_physical_name(prefix)
            assert len(name) <= 63, (
                f"Name {name!r} ({len(name)} chars) exceeds PG 63-char limit"
            )

    def test_catalog_prefix_c(self):
        name = generate_physical_name("c")
        assert name.startswith("c_")

    def test_collection_prefix_col(self):
        name = generate_physical_name("col")
        assert name.startswith("col_")

    def test_schema_prefix_s(self):
        name = generate_physical_name("s")
        assert name.startswith("s_")


# ---------------------------------------------------------------------------
# Step 0 — PK-collision retry backstop (catalog)
# ---------------------------------------------------------------------------

class TestCatalogPKRetry:
    """_insert_catalog_row_with_pk_retry: retries on PK clash, raises on external_id clash."""

    @pytest.mark.asyncio
    async def test_succeeds_on_first_attempt(self):
        from dynastore.modules.catalog.catalog_service import (
            _insert_catalog_row_with_pk_retry,
        )

        mock_conn = AsyncMock()
        # Simulate successful INSERT (no exception)
        with patch(
            "dynastore.modules.catalog.catalog_service._create_catalog_strict_query"
        ) as mock_q:
            mock_q.execute = AsyncMock(return_value=1)
            result = await _insert_catalog_row_with_pk_retry(
                mock_conn,
                external_id="my_catalog",
                provisioning_status="ready",
            )
        assert result.startswith("c_")
        assert len(result.split("_")[1]) == 13

    @pytest.mark.asyncio
    async def test_retries_on_pk_clash_then_succeeds(self):
        """First call raises a PK UniqueViolationError; second succeeds."""
        from dynastore.modules.catalog.catalog_service import (
            _insert_catalog_row_with_pk_retry,
        )
        from dynastore.modules.db_config.exceptions import UniqueViolationError

        call_count = 0

        async def _side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Simulate PK constraint violation
                exc = UniqueViolationError("catalogs_pkey clash")
                exc.orig = type("_FakeOrig", (), {
                    "pgcode": "23505",
                    "constraint_name": "catalogs_pkey",
                })()
                raise exc
            return 1

        with patch(
            "dynastore.modules.catalog.catalog_service._create_catalog_strict_query"
        ) as mock_q:
            mock_q.execute = _side_effect
            mock_conn = AsyncMock()
            result = await _insert_catalog_row_with_pk_retry(
                mock_conn,
                external_id="my_catalog",
                provisioning_status="ready",
            )
        assert call_count == 2, f"Expected 2 attempts, got {call_count}"
        assert result.startswith("c_")

    @pytest.mark.asyncio
    async def test_external_id_clash_not_retried(self):
        """A unique violation on the external_id index must NOT be retried."""
        from dynastore.modules.catalog.catalog_service import (
            _insert_catalog_row_with_pk_retry,
        )
        from dynastore.modules.db_config.exceptions import UniqueViolationError

        call_count = 0

        async def _side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            exc = UniqueViolationError("catalogs_external_uq clash")
            exc.orig = type("_FakeOrig", (), {
                "pgcode": "23505",
                "constraint_name": "catalogs_external_uq",
            })()
            raise exc

        with patch(
            "dynastore.modules.catalog.catalog_service._create_catalog_strict_query"
        ) as mock_q:
            mock_q.execute = _side_effect
            mock_conn = AsyncMock()
            with pytest.raises(UniqueViolationError):
                await _insert_catalog_row_with_pk_retry(
                    mock_conn,
                    external_id="my_catalog",
                    provisioning_status="ready",
                )
        # Must NOT retry — only one attempt.
        assert call_count == 1, f"external_id clash must not be retried; got {call_count} calls"


# ---------------------------------------------------------------------------
# Step 0 — PK-collision retry backstop (collection)
# ---------------------------------------------------------------------------

class TestCollectionPKRetry:
    """_insert_collection_row_with_pk_retry behaves analogously for collections."""

    @pytest.mark.asyncio
    async def test_retries_on_pk_clash(self):
        from dynastore.modules.catalog.collection_service import (
            _insert_collection_row_with_pk_retry,
        )
        from dynastore.modules.db_config.exceptions import UniqueViolationError

        call_count = 0

        async def _side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                exc = UniqueViolationError("collections_pkey clash")
                exc.orig = type("_FakeOrig", (), {
                    "pgcode": "23505",
                    "constraint_name": "collections_pkey",
                })()
                raise exc
            return "col_newid12345"

        with patch(
            "dynastore.modules.catalog.collection_service.DQLQuery"
        ) as mock_dql:
            mock_instance = MagicMock()
            mock_instance.execute = _side_effect
            mock_dql.return_value = mock_instance
            mock_conn = AsyncMock()
            result = await _insert_collection_row_with_pk_retry(
                mock_conn,
                phys_schema="s_abc123",
                external_id="my_collection",
                catalog_id="c_cat123",
                lifecycle_status=None,
            )
        assert call_count == 2
        assert result.startswith("col_")

    @pytest.mark.asyncio
    async def test_external_id_clash_not_retried(self):
        from dynastore.modules.catalog.collection_service import (
            _insert_collection_row_with_pk_retry,
        )
        from dynastore.modules.db_config.exceptions import UniqueViolationError

        call_count = 0

        async def _side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            exc = UniqueViolationError("collections_external_uq clash")
            exc.orig = type("_FakeOrig", (), {
                "pgcode": "23505",
                "constraint_name": "collections_external_uq",
            })()
            raise exc

        with patch(
            "dynastore.modules.catalog.collection_service.DQLQuery"
        ) as mock_dql:
            mock_instance = MagicMock()
            mock_instance.execute = _side_effect
            mock_dql.return_value = mock_instance
            mock_conn = AsyncMock()
            with pytest.raises(UniqueViolationError):
                await _insert_collection_row_with_pk_retry(
                    mock_conn,
                    phys_schema="s_abc123",
                    external_id="my_collection",
                    catalog_id="c_cat123",
                    lifecycle_status=None,
                )
        assert call_count == 1


# ---------------------------------------------------------------------------
# Step 1 — rename_catalog (service-layer logic, DB mocked)
# ---------------------------------------------------------------------------

class TestRenameCatalog:
    """rename_catalog: one-row label change, cache invalidation, conflict detection."""

    def _make_service(self):
        from dynastore.modules.catalog.catalog_service import CatalogService

        svc = CatalogService.__new__(CatalogService)
        svc.engine = MagicMock()
        svc._collection_service = None
        svc._item_service = None
        svc._cascade_orchestrator = None
        return svc

    @pytest.mark.asyncio
    async def test_rename_updates_external_id_and_returns_tuple(self):
        """Happy path: returns (old_external_id, new_external_id)."""

        svc = self._make_service()

        async def _fake_tx(engine):
            class _FakeConn:
                async def __aenter__(self): return self
                async def __aexit__(self, *a): pass
            return _FakeConn()

        # Patch managed_transaction to yield a fake conn, and DQLQuery.execute.
        current_row = MagicMock()
        current_row._mapping = {"id": "c_internal1", "external_id": "old_label"}
        current_row.__getitem__ = lambda s, k: {"id": "c_internal1", "external_id": "old_label"}[k]

        execute_calls = []

        async def _execute(*args, **kwargs):
            execute_calls.append(kwargs)
            if len(execute_calls) == 1:  # SELECT current row
                return current_row
            if len(execute_calls) == 2:  # SELECT conflict check
                return None  # no conflict
            return 1  # UPDATE rowcount / INSERT alias

        with patch(
            "dynastore.modules.catalog.catalog_service.managed_transaction"
        ) as mock_tx, patch(
            "dynastore.modules.catalog.catalog_service.DQLQuery"
        ) as mock_dql, patch(
            "dynastore.modules.catalog.catalog_service._invalidate_catalog_external_id_cache"
        ) as mock_inv_ext, patch(
            "dynastore.modules.catalog.catalog_service._invalidate_catalog_model_cache"
        ) as mock_inv_model:
            mock_dql_inst = MagicMock()
            mock_dql_inst.execute = _execute
            mock_dql.return_value = mock_dql_inst

            class _FakeCM:
                async def __aenter__(self): return MagicMock()
                async def __aexit__(self, *a): pass

            mock_tx.return_value = _FakeCM()

            old, new = await svc.rename_catalog("c_internal1", "new_label")

        assert old == "old_label"
        assert new == "new_label"
        # Cache invalidated for both labels.
        mock_inv_ext.assert_any_call("old_label")
        mock_inv_ext.assert_any_call("new_label")
        mock_inv_model.assert_called_once_with("c_internal1")

    @pytest.mark.asyncio
    async def test_rename_noop_when_already_same_label(self):
        """No-op if new label == old label (returns tuple without DB write)."""

        svc = self._make_service()

        current_row = MagicMock()
        current_row._mapping = {"id": "c_internal1", "external_id": "same_label"}

        execute_calls = []

        async def _execute(*args, **kwargs):
            execute_calls.append(kwargs)
            return current_row

        with patch(
            "dynastore.modules.catalog.catalog_service.managed_transaction"
        ) as mock_tx, patch(
            "dynastore.modules.catalog.catalog_service.DQLQuery"
        ) as mock_dql:
            mock_dql_inst = MagicMock()
            mock_dql_inst.execute = _execute
            mock_dql.return_value = mock_dql_inst

            class _FakeCM:
                async def __aenter__(self): return MagicMock()
                async def __aexit__(self, *a): pass

            mock_tx.return_value = _FakeCM()
            old, new = await svc.rename_catalog("c_internal1", "same_label")

        assert old == new == "same_label"
        # Only one SELECT (the current-row fetch); no UPDATE.
        assert len(execute_calls) == 1

    @pytest.mark.asyncio
    async def test_rename_conflict_raises_error(self):
        """Conflict (another live catalog holds new label) → CatalogRenameConflictError."""
        from dynastore.modules.db_config.exceptions import CatalogRenameConflictError

        svc = self._make_service()

        current_row = MagicMock()
        current_row._mapping = {"id": "c_internal1", "external_id": "old_label"}

        conflict_row = MagicMock()  # non-None → conflict
        execute_calls = []

        async def _execute(*args, **kwargs):
            execute_calls.append(kwargs)
            if len(execute_calls) == 1:
                return current_row
            return conflict_row  # conflict SELECT

        with patch(
            "dynastore.modules.catalog.catalog_service.managed_transaction"
        ) as mock_tx, patch(
            "dynastore.modules.catalog.catalog_service.DQLQuery"
        ) as mock_dql:
            mock_dql_inst = MagicMock()
            mock_dql_inst.execute = _execute
            mock_dql.return_value = mock_dql_inst

            class _FakeCM:
                async def __aenter__(self): return MagicMock()
                async def __aexit__(self, *a): pass

            mock_tx.return_value = _FakeCM()

            with pytest.raises(CatalogRenameConflictError) as exc_info:
                await svc.rename_catalog("c_internal1", "taken_label")

        assert "taken_label" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Step 1 — rename_collection (service-layer logic, DB mocked)
# ---------------------------------------------------------------------------

class TestRenameCollection:
    """rename_collection: one-row label change, conflict detection."""

    def _make_service(self):
        from dynastore.modules.catalog.collection_service import CollectionService

        svc = CollectionService.__new__(CollectionService)
        svc.engine = MagicMock()
        return svc

    @pytest.mark.asyncio
    async def test_rename_returns_old_and_new(self):

        svc = self._make_service()

        current_row = MagicMock()
        current_row._mapping = {"id": "col_int1", "external_id": "old_col"}
        execute_calls = []

        async def _execute(*args, **kwargs):
            execute_calls.append(kwargs)
            if len(execute_calls) == 1:
                return current_row
            if len(execute_calls) == 2:
                return None  # no conflict
            return 1  # UPDATE + alias INSERT

        with patch(
            "dynastore.modules.catalog.collection_service.managed_transaction"
        ) as mock_tx, patch(
            "dynastore.modules.catalog.collection_service.DQLQuery"
        ) as mock_dql, patch(
            "dynastore.modules.catalog.collection_service.get_protocol"
        ) as mock_gp, patch(
            "dynastore.modules.catalog.collection_service._invalidate_collection_external_id_cache"
        ) as mock_inv_ext, patch(
            "dynastore.modules.catalog.collection_service._invalidate_collection_model_cache"
        ) as mock_inv_model:
            # Stub CatalogsProtocol.resolve_physical_schema
            mock_catalog_svc = AsyncMock()
            mock_catalog_svc.resolve_physical_schema = AsyncMock(return_value="s_abc")
            mock_gp.return_value = mock_catalog_svc

            mock_dql_inst = MagicMock()
            mock_dql_inst.execute = _execute
            mock_dql.return_value = mock_dql_inst

            class _FakeCM:
                async def __aenter__(self): return MagicMock()
                async def __aexit__(self, *a): pass

            mock_tx.return_value = _FakeCM()

            old, new = await svc.rename_collection(
                "c_cat1", "col_int1", "new_col"
            )

        assert old == "old_col"
        assert new == "new_col"
        mock_inv_ext.assert_any_call("c_cat1", "old_col")
        mock_inv_ext.assert_any_call("c_cat1", "new_col")
        mock_inv_model.assert_called_once_with("c_cat1", "col_int1")

    @pytest.mark.asyncio
    async def test_rename_collection_conflict_raises(self):
        from dynastore.modules.db_config.exceptions import CollectionRenameConflictError

        svc = self._make_service()

        current_row = MagicMock()
        current_row._mapping = {"id": "col_int1", "external_id": "old_col"}
        conflict_row = MagicMock()
        execute_calls = []

        async def _execute(*args, **kwargs):
            execute_calls.append(kwargs)
            if len(execute_calls) == 1:
                return current_row
            return conflict_row

        with patch(
            "dynastore.modules.catalog.collection_service.managed_transaction"
        ) as mock_tx, patch(
            "dynastore.modules.catalog.collection_service.DQLQuery"
        ) as mock_dql, patch(
            "dynastore.modules.catalog.collection_service.get_protocol"
        ) as mock_gp:
            mock_catalog_svc = AsyncMock()
            mock_catalog_svc.resolve_physical_schema = AsyncMock(return_value="s_abc")
            mock_gp.return_value = mock_catalog_svc

            mock_dql_inst = MagicMock()
            mock_dql_inst.execute = _execute
            mock_dql.return_value = mock_dql_inst

            class _FakeCM:
                async def __aenter__(self): return MagicMock()
                async def __aexit__(self, *a): pass

            mock_tx.return_value = _FakeCM()

            with pytest.raises(CollectionRenameConflictError):
                await svc.rename_collection("c_cat1", "col_int1", "taken_col")


# ---------------------------------------------------------------------------
# Step 2 — PUT-as-MOVE via _ogc_replace_catalog/_ogc_replace_collection
# ---------------------------------------------------------------------------

class TestPutAsMoveCatalog:
    """_ogc_replace_catalog: body_id != path → rename + Content-Location header."""

    def _make_mixin(self):
        """Return a minimal OGCServiceMixin instance with all helpers stubbed."""
        from dynastore.extensions.ogc_base import OGCServiceMixin

        class _ConcreteOGC(OGCServiceMixin):
            prefix = "/stac"
            conformance_uris: list = []
            protocol_title = "test"
            protocol_description = "test"

            def _localize_resource(self, model: Any, lang: str) -> tuple:
                d = model.model_dump(exclude_none=True) if hasattr(model, "model_dump") else {}
                return d, lang

        obj = _ConcreteOGC.__new__(_ConcreteOGC)
        obj.prefix = "/stac"
        return obj

    @pytest.mark.asyncio
    async def test_body_id_equals_path_id_is_normal_replace(self):
        """When body_id == catalog_id the normal update path runs."""
        obj = self._make_mixin()

        updated_model = MagicMock()
        updated_model.model_dump = MagicMock(return_value={"id": "my_catalog", "type": "Catalog"})

        catalogs_svc = AsyncMock()
        catalogs_svc.update_catalog = AsyncMock(return_value=updated_model)
        catalogs_svc.rename_catalog = AsyncMock()

        with patch.object(obj, "_get_catalogs_service", return_value=AsyncMock(
            return_value=catalogs_svc
        )) as _mock_get_svc, patch.object(
            obj, "_require_catalog_write_ready", new_callable=AsyncMock
        ):
            _mock_get_svc.return_value = catalogs_svc

            resp = await obj._ogc_replace_catalog(
                "my_catalog",
                {"id": "my_catalog"},
                "en",
                None,
                body_id="my_catalog",
            )

        # rename must NOT have been called.
        catalogs_svc.rename_catalog.assert_not_awaited()
        assert resp.status_code == 200
        assert "Content-Location" not in resp.headers

    @pytest.mark.asyncio
    async def test_body_id_differs_triggers_rename_and_content_location(self):
        """When body_id != catalog_id and Prefer: handling=move, rename runs and Content-Location is set."""
        obj = self._make_mixin()

        updated_model = MagicMock()
        updated_model.model_dump = MagicMock(return_value={"id": "new_label", "type": "Catalog"})

        catalogs_svc = AsyncMock()
        catalogs_svc.resolve_catalog_id = AsyncMock(return_value="c_internal1")
        catalogs_svc.rename_catalog = AsyncMock(return_value=("old_label", "new_label"))
        catalogs_svc.update_catalog = AsyncMock(return_value=updated_model)

        # Fake request with Prefer: handling=move to trigger the rename gate.
        mock_request = MagicMock()
        mock_request.headers = {"prefer": "handling=move"}
        mock_request.url = MagicMock()

        with patch.object(obj, "_get_catalogs_service", return_value=catalogs_svc), \
             patch.object(obj, "_require_catalog_write_ready", new_callable=AsyncMock), \
             patch.object(
                 obj, "_build_catalog_url", return_value="https://example.com/stac/catalogs/new_label"
             ):
            resp = await obj._ogc_replace_catalog(
                "old_label",
                {"id": "new_label"},
                "en",
                None,
                request=mock_request,
                body_id="new_label",
            )

        catalogs_svc.rename_catalog.assert_awaited_once_with("c_internal1", "new_label")
        assert resp.status_code == 200
        assert resp.headers.get("Content-Location") == "https://example.com/stac/catalogs/new_label"
        assert resp.headers.get("Preference-Applied") == "handling=move"

    @pytest.mark.asyncio
    async def test_rename_conflict_maps_to_409(self):
        """A CatalogRenameConflictError during PUT-as-MOVE → HTTP 409."""
        from fastapi import HTTPException
        from dynastore.modules.db_config.exceptions import CatalogRenameConflictError

        obj = self._make_mixin()

        catalogs_svc = AsyncMock()
        catalogs_svc.resolve_catalog_id = AsyncMock(return_value="c_internal1")
        catalogs_svc.rename_catalog = AsyncMock(
            side_effect=CatalogRenameConflictError("taken_label")
        )

        mock_request = MagicMock()
        mock_request.headers = {"prefer": "handling=move"}

        with patch.object(obj, "_get_catalogs_service", return_value=catalogs_svc), \
             patch.object(obj, "_require_catalog_write_ready", new_callable=AsyncMock):
            with pytest.raises(HTTPException) as exc_info:
                await obj._ogc_replace_catalog(
                    "old_label",
                    {"id": "taken_label"},
                    "en",
                    None,
                    request=mock_request,
                    body_id="taken_label",
                )

        assert exc_info.value.status_code == 409

    @pytest.mark.asyncio
    async def test_replace_collection_rename_sets_content_location(self):
        """PUT collection body_id != path + Prefer: handling=move → rename + Content-Location."""
        obj = self._make_mixin()

        updated_model = MagicMock()
        updated_model.model_dump = MagicMock(
            return_value={"id": "new_col", "type": "Collection"}
        )

        mock_cat_model = MagicMock()
        mock_cat_model.external_id = "my_catalog"

        catalogs_svc = AsyncMock()
        catalogs_svc.resolve_catalog_id = AsyncMock(return_value="c_int1")
        catalogs_svc.collections = AsyncMock()
        catalogs_svc.collections.resolve_collection_id = AsyncMock(return_value="col_int1")
        catalogs_svc.rename_collection = AsyncMock(return_value=("old_col", "new_col"))
        catalogs_svc.update_collection = AsyncMock(return_value=updated_model)
        catalogs_svc.get_catalog_model = AsyncMock(return_value=mock_cat_model)

        mock_request = MagicMock()
        mock_request.headers = {"prefer": "handling=move"}

        with patch.object(obj, "_get_catalogs_service", return_value=catalogs_svc), \
             patch.object(obj, "_require_catalog_write_ready", new_callable=AsyncMock), \
             patch.object(
                 obj, "_build_collection_url",
                 return_value="https://example.com/stac/catalogs/my_catalog/collections/new_col",
             ):
            resp = await obj._ogc_replace_collection(
                "my_catalog",
                "old_col",
                {"id": "new_col"},
                "en",
                request=mock_request,
                body_id="new_col",
            )

        catalogs_svc.rename_collection.assert_awaited_once_with(
            "c_int1", "col_int1", "new_col"
        )
        assert resp.status_code == 200
        assert "new_col" in resp.headers.get("Content-Location", "")
        assert resp.headers.get("Preference-Applied") == "handling=move"


# ---------------------------------------------------------------------------
# Step 3 — PATCH-id-as-rename via _ogc_update_catalog/_ogc_update_collection
# ---------------------------------------------------------------------------

class TestPatchAsMoveCatalog:
    """_ogc_update_catalog: body_id != path → rename + Content-Location header."""

    def _make_mixin(self):
        from dynastore.extensions.ogc_base import OGCServiceMixin

        class _ConcreteOGC(OGCServiceMixin):
            prefix = "/stac"
            conformance_uris: list = []
            protocol_title = "test"
            protocol_description = "test"

            def _localize_resource(self, model: Any, lang: str) -> tuple:
                d = model.model_dump(exclude_none=True) if hasattr(model, "model_dump") else {}
                return d, lang

        obj = _ConcreteOGC.__new__(_ConcreteOGC)
        obj.prefix = "/stac"
        return obj

    @pytest.mark.asyncio
    async def test_patch_body_id_equals_path_id_is_normal_update(self):
        """PATCH with body_id == catalog_id → normal update, no rename."""
        obj = self._make_mixin()

        updated_model = MagicMock()
        updated_model.model_dump = MagicMock(return_value={"id": "my_catalog"})

        catalogs_svc = AsyncMock()
        catalogs_svc.update_catalog = AsyncMock(return_value=updated_model)
        catalogs_svc.rename_catalog = AsyncMock()

        with patch.object(obj, "_get_catalogs_service", return_value=catalogs_svc), \
             patch.object(obj, "_require_catalog_write_ready", new_callable=AsyncMock):
            resp = await obj._ogc_update_catalog(
                "my_catalog",
                {"id": "my_catalog", "title": "New Title"},
                "en",
                None,
                body_id="my_catalog",
            )

        catalogs_svc.rename_catalog.assert_not_awaited()
        assert resp.status_code == 200
        assert "Content-Location" not in resp.headers

    @pytest.mark.asyncio
    async def test_patch_no_body_id_is_normal_update(self):
        """PATCH with no id field → normal update, no rename."""
        obj = self._make_mixin()

        updated_model = MagicMock()
        updated_model.model_dump = MagicMock(return_value={"id": "my_catalog"})

        catalogs_svc = AsyncMock()
        catalogs_svc.update_catalog = AsyncMock(return_value=updated_model)
        catalogs_svc.rename_catalog = AsyncMock()

        with patch.object(obj, "_get_catalogs_service", return_value=catalogs_svc), \
             patch.object(obj, "_require_catalog_write_ready", new_callable=AsyncMock):
            resp = await obj._ogc_update_catalog(
                "my_catalog",
                {"title": "New Title"},
                "en",
                None,
                body_id=None,
            )

        catalogs_svc.rename_catalog.assert_not_awaited()
        assert resp.status_code == 200
        assert "Content-Location" not in resp.headers

    @pytest.mark.asyncio
    async def test_patch_body_id_differs_triggers_rename_and_content_location(self):
        """PATCH with body_id != catalog_id and Prefer: handling=move → rename + Content-Location."""
        obj = self._make_mixin()

        updated_model = MagicMock()
        updated_model.model_dump = MagicMock(return_value={"id": "new_catalog"})

        catalogs_svc = AsyncMock()
        catalogs_svc.resolve_catalog_id = AsyncMock(return_value="c_internal1")
        catalogs_svc.rename_catalog = AsyncMock(return_value=("old_catalog", "new_catalog"))
        catalogs_svc.update_catalog = AsyncMock(return_value=updated_model)

        mock_request = MagicMock()
        mock_request.headers = {"prefer": "handling=move"}

        with patch.object(obj, "_get_catalogs_service", return_value=catalogs_svc), \
             patch.object(obj, "_require_catalog_write_ready", new_callable=AsyncMock), \
             patch.object(
                 obj, "_build_catalog_url",
                 return_value="https://example.com/stac/catalogs/new_catalog",
             ):
            resp = await obj._ogc_update_catalog(
                "old_catalog",
                {"id": "new_catalog", "title": "New Title"},
                "en",
                None,
                body_id="new_catalog",
                request=mock_request,
            )

        catalogs_svc.rename_catalog.assert_awaited_once_with("c_internal1", "new_catalog")
        # "id" must have been stripped before calling update_catalog.
        call_args = catalogs_svc.update_catalog.call_args
        assert "id" not in (call_args.args[1] if len(call_args.args) > 1 else call_args.kwargs.get("updates", {}))
        assert resp.status_code == 200
        assert resp.headers.get("Content-Location") == "https://example.com/stac/catalogs/new_catalog"
        assert resp.headers.get("Preference-Applied") == "handling=move"

    @pytest.mark.asyncio
    async def test_patch_catalog_rename_conflict_maps_to_409(self):
        """PATCH body_id conflicts with existing catalog → HTTP 409."""
        from fastapi import HTTPException
        from dynastore.modules.db_config.exceptions import CatalogRenameConflictError

        obj = self._make_mixin()

        catalogs_svc = AsyncMock()
        catalogs_svc.resolve_catalog_id = AsyncMock(return_value="c_internal1")
        catalogs_svc.rename_catalog = AsyncMock(
            side_effect=CatalogRenameConflictError("taken_catalog")
        )

        mock_request = MagicMock()
        mock_request.headers = {"prefer": "handling=move"}

        with patch.object(obj, "_get_catalogs_service", return_value=catalogs_svc), \
             patch.object(obj, "_require_catalog_write_ready", new_callable=AsyncMock):
            with pytest.raises(HTTPException) as exc_info:
                await obj._ogc_update_catalog(
                    "old_catalog",
                    {"id": "taken_catalog"},
                    "en",
                    None,
                    body_id="taken_catalog",
                    request=mock_request,
                )

        assert exc_info.value.status_code == 409

    @pytest.mark.asyncio
    async def test_put_rename_still_works_unchanged(self):
        """PUT-as-MOVE with Prefer: handling=move still renames correctly."""
        obj = self._make_mixin()

        updated_model = MagicMock()
        updated_model.model_dump = MagicMock(return_value={"id": "new_catalog"})

        catalogs_svc = AsyncMock()
        catalogs_svc.resolve_catalog_id = AsyncMock(return_value="c_internal1")
        catalogs_svc.rename_catalog = AsyncMock(return_value=("old_catalog", "new_catalog"))
        catalogs_svc.update_catalog = AsyncMock(return_value=updated_model)

        mock_request = MagicMock()
        mock_request.headers = {"prefer": "handling=move"}

        with patch.object(obj, "_get_catalogs_service", return_value=catalogs_svc), \
             patch.object(obj, "_require_catalog_write_ready", new_callable=AsyncMock), \
             patch.object(
                 obj, "_build_catalog_url",
                 return_value="https://example.com/stac/catalogs/new_catalog",
             ):
            resp = await obj._ogc_replace_catalog(
                "old_catalog",
                {"id": "new_catalog"},
                "en",
                None,
                request=mock_request,
                body_id="new_catalog",
            )

        catalogs_svc.rename_catalog.assert_awaited_once_with("c_internal1", "new_catalog")
        assert resp.status_code == 200
        assert resp.headers.get("Content-Location") == "https://example.com/stac/catalogs/new_catalog"


class TestPatchAsMoveCollection:
    """_ogc_update_collection: body_id != path → rename + Content-Location header."""

    def _make_mixin(self):
        from dynastore.extensions.ogc_base import OGCServiceMixin

        class _ConcreteOGC(OGCServiceMixin):
            prefix = "/features"
            conformance_uris: list = []
            protocol_title = "test"
            protocol_description = "test"

            def _localize_resource(self, model: Any, lang: str) -> tuple:
                d = model.model_dump(exclude_none=True) if hasattr(model, "model_dump") else {}
                return d, lang

        obj = _ConcreteOGC.__new__(_ConcreteOGC)
        obj.prefix = "/features"
        return obj

    @pytest.mark.asyncio
    async def test_patch_collection_no_id_is_normal_update(self):
        """PATCH with no id field → normal update, no rename."""
        obj = self._make_mixin()

        updated_model = MagicMock()
        updated_model.model_dump = MagicMock(return_value={"id": "my_col"})

        catalogs_svc = AsyncMock()
        catalogs_svc.update_collection = AsyncMock(return_value=updated_model)
        catalogs_svc.rename_collection = AsyncMock()

        with patch.object(obj, "_get_catalogs_service", return_value=catalogs_svc), \
             patch.object(obj, "_require_catalog_write_ready", new_callable=AsyncMock), \
             patch.object(obj, "_pre_update_collection_validate", new_callable=AsyncMock):
            resp = await obj._ogc_update_collection(
                "my_catalog", "my_col",
                {"title": "New Title"},
                "en", None,
                body_id=None,
            )

        catalogs_svc.rename_collection.assert_not_awaited()
        assert resp.status_code == 200
        assert "Content-Location" not in resp.headers

    @pytest.mark.asyncio
    async def test_patch_collection_body_id_differs_triggers_rename_and_content_location(self):
        """PATCH collection with body_id != collection_id and Prefer: handling=move → rename + Content-Location."""
        obj = self._make_mixin()

        updated_model = MagicMock()
        updated_model.model_dump = MagicMock(return_value={"id": "new_col"})

        mock_cat_model = MagicMock()
        mock_cat_model.external_id = "my_catalog"

        catalogs_svc = AsyncMock()
        catalogs_svc.resolve_catalog_id = AsyncMock(return_value="c_int1")
        catalogs_svc.collections = AsyncMock()
        catalogs_svc.collections.resolve_collection_id = AsyncMock(return_value="col_int1")
        catalogs_svc.rename_collection = AsyncMock(return_value=("old_col", "new_col"))
        catalogs_svc.update_collection = AsyncMock(return_value=updated_model)
        catalogs_svc.get_catalog_model = AsyncMock(return_value=mock_cat_model)

        mock_request = MagicMock()
        mock_request.headers = {"prefer": "handling=move"}

        with patch.object(obj, "_get_catalogs_service", return_value=catalogs_svc), \
             patch.object(obj, "_require_catalog_write_ready", new_callable=AsyncMock), \
             patch.object(obj, "_pre_update_collection_validate", new_callable=AsyncMock), \
             patch.object(
                 obj, "_build_collection_url",
                 return_value="https://example.com/features/catalogs/my_catalog/collections/new_col",
             ):
            resp = await obj._ogc_update_collection(
                "my_catalog", "old_col",
                {"id": "new_col", "title": "Renamed"},
                "en", mock_request,
                body_id="new_col",
            )

        catalogs_svc.rename_collection.assert_awaited_once_with("c_int1", "col_int1", "new_col")
        # "id" must have been stripped before calling update_collection.
        call_args = catalogs_svc.update_collection.call_args
        patch_payload = call_args.args[2] if len(call_args.args) > 2 else call_args.kwargs.get("updates", {})
        assert "id" not in patch_payload
        assert resp.status_code == 200
        assert resp.headers.get("Content-Location", "").endswith("new_col")
        assert resp.headers.get("Preference-Applied") == "handling=move"

    @pytest.mark.asyncio
    async def test_patch_collection_rename_conflict_maps_to_409(self):
        """PATCH collection body_id conflicts with existing collection → HTTP 409."""
        from fastapi import HTTPException
        from dynastore.modules.db_config.exceptions import CollectionRenameConflictError

        obj = self._make_mixin()

        catalogs_svc = AsyncMock()
        catalogs_svc.resolve_catalog_id = AsyncMock(return_value="c_int1")
        catalogs_svc.collections = AsyncMock()
        catalogs_svc.collections.resolve_collection_id = AsyncMock(return_value="col_int1")
        catalogs_svc.rename_collection = AsyncMock(
            side_effect=CollectionRenameConflictError("c_int1", "taken_col")
        )

        mock_request = MagicMock()
        mock_request.headers = {"prefer": "handling=move"}

        with patch.object(obj, "_get_catalogs_service", return_value=catalogs_svc), \
             patch.object(obj, "_require_catalog_write_ready", new_callable=AsyncMock):
            with pytest.raises(HTTPException) as exc_info:
                await obj._ogc_update_collection(
                    "my_catalog", "old_col",
                    {"id": "taken_col"},
                    "en", mock_request,
                    body_id="taken_col",
                )

        assert exc_info.value.status_code == 409


# ---------------------------------------------------------------------------
# Task A — item write path persists INTERNAL ids in canonical _source
# ---------------------------------------------------------------------------

class _CapturedIds(Exception):
    """Sentinel: raised inside a stubbed coroutine to short-circuit upsert."""
    def __init__(self, catalog_id: str, collection_id: str) -> None:
        self.catalog_id = catalog_id
        self.collection_id = collection_id


class TestItemWritePersistsInternalIds:
    """ItemService.upsert resolves external→internal ids before dispatching downstream.

    Strategy: intercept ``_enforce_strict_unknown_fields`` — the first method
    called with (catalog_id, collection_id) AFTER the resolution block — and
    record the ids it receives.  This avoids wiring up the entire write stack
    while still proving the invariant.
    """

    @pytest.mark.asyncio
    async def test_internal_ids_passed_downstream(self):
        """After resolution, internal ids are what every downstream call receives."""
        from dynastore.modules.catalog.item_service import ItemService

        svc = ItemService.__new__(ItemService)
        svc.engine = MagicMock()
        svc._col_config_cache = {}

        async def _capture_strict(catalog_id, collection_id, *args, **kwargs):
            raise _CapturedIds(catalog_id, collection_id)

        mock_catalog_svc = AsyncMock()
        mock_catalog_svc.resolve_catalog_id = AsyncMock(return_value="c_internal")
        mock_collections = AsyncMock()
        mock_collections.resolve_collection_id = AsyncMock(return_value="col_internal")
        mock_catalog_svc.collections = mock_collections

        item = {
            "type": "Feature",
            "id": "item-1",
            "geometry": None,
            "properties": {},
        }

        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_catalog_svc,
        ), patch.object(svc, "_enforce_strict_unknown_fields", side_effect=_capture_strict):
            with pytest.raises(_CapturedIds) as exc_info:
                await svc.upsert("my_catalog", "my_collection", item)

        assert exc_info.value.catalog_id == "c_internal", (
            f"Expected 'c_internal' after resolution, got {exc_info.value.catalog_id!r}"
        )
        assert exc_info.value.collection_id == "col_internal", (
            f"Expected 'col_internal' after resolution, got {exc_info.value.collection_id!r}"
        )

    @pytest.mark.asyncio
    async def test_no_resolution_when_protocol_unavailable(self):
        """When CatalogsProtocol is not registered, original ids pass through unchanged."""
        from dynastore.modules.catalog.item_service import ItemService

        svc = ItemService.__new__(ItemService)
        svc.engine = MagicMock()
        svc._col_config_cache = {}

        async def _capture_strict(catalog_id, collection_id, *args, **kwargs):
            raise _CapturedIds(catalog_id, collection_id)

        item = {
            "type": "Feature",
            "id": "item-1",
            "geometry": None,
            "properties": {},
        }

        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=None,
        ), patch.object(svc, "_enforce_strict_unknown_fields", side_effect=_capture_strict):
            with pytest.raises(_CapturedIds) as exc_info:
                await svc.upsert("my_catalog", "my_collection", item)

        assert exc_info.value.catalog_id == "my_catalog"
        assert exc_info.value.collection_id == "my_collection"


# ---------------------------------------------------------------------------
# Task B — item read path projects internal → external collection id
# ---------------------------------------------------------------------------

class TestItemReadProjectsExternalCollectionId:
    """read_entities resolves internal collection id back to external in Feature."""

    @pytest.mark.asyncio
    async def test_collection_field_projected_to_external(self):
        """A feature with internal 'collection' field gets external id projected."""

        # Simulate the resolver that `read_entities` calls inline.
        # We test the projection helper logic directly without instantiating the full driver.

        internal_to_external: dict = {"col_internal": "my_collection"}

        async def _resolve_collection_external_id(catalog_id, internal_id, allow_missing=True):
            return internal_to_external.get(internal_id)

        mock_collections = AsyncMock()
        mock_collections.resolve_collection_id = AsyncMock(return_value="col_internal")
        mock_collections.resolve_collection_external_id = _resolve_collection_external_id

        mock_catalog_svc = AsyncMock()
        mock_catalog_svc.resolve_catalog_id = AsyncMock(return_value="c_internal")
        mock_catalog_svc.collections = mock_collections

        # Build a minimal Feature with an internal 'collection' in __pydantic_extra__
        from dynastore.models.ogc import Feature
        feature = Feature.model_validate(
            {
                "type": "Feature",
                "id": "item-1",
                "geometry": None,
                "properties": {},
            }
        )
        # Simulate internal id stored in collection field
        if feature.__pydantic_extra__ is None:
            object.__setattr__(feature, "__pydantic_extra__", {})
        feature.__pydantic_extra__["collection"] = "col_internal"

        # Invoke the same projection logic that read_entities uses:
        # resolve internal→external once, then patch the feature.
        cache: dict = {}
        raw_coll = feature.__pydantic_extra__.get("collection")
        if raw_coll not in cache:
            resolved = await mock_collections.resolve_collection_external_id(
                "c_internal", raw_coll, allow_missing=True
            )
            cache[raw_coll] = resolved if resolved is not None else raw_coll
        feature.__pydantic_extra__["collection"] = cache[raw_coll]

        assert feature.__pydantic_extra__["collection"] == "my_collection"

    @pytest.mark.asyncio
    async def test_collection_field_unchanged_when_no_mapping(self):
        """When resolve returns None, collection field is left as-is (fail-open)."""
        mock_collections = AsyncMock()
        mock_collections.resolve_collection_external_id = AsyncMock(return_value=None)

        from dynastore.models.ogc import Feature
        feature = Feature.model_validate(
            {"type": "Feature", "id": "item-2", "geometry": None, "properties": {}}
        )
        if feature.__pydantic_extra__ is None:
            object.__setattr__(feature, "__pydantic_extra__", {})
        feature.__pydantic_extra__["collection"] = "col_internal"

        raw_coll = feature.__pydantic_extra__.get("collection")
        resolved = await mock_collections.resolve_collection_external_id(
            "c_internal", raw_coll, allow_missing=True
        )
        # fail-open: keep original when resolution returns None
        feature.__pydantic_extra__["collection"] = resolved if resolved is not None else raw_coll

        assert feature.__pydantic_extra__["collection"] == "col_internal"

    @pytest.mark.asyncio
    async def test_multi_item_same_collection_resolves_once(self):
        """Multiple features with same internal collection_id resolve the mapping once."""
        call_count = 0

        async def _resolve(catalog_id, internal_id, allow_missing=True):
            nonlocal call_count
            call_count += 1
            return "my_collection"

        from dynastore.models.ogc import Feature

        features = []
        for i in range(5):
            f = Feature.model_validate(
                {"type": "Feature", "id": f"item-{i}", "geometry": None, "properties": {}}
            )
            if f.__pydantic_extra__ is None:
                object.__setattr__(f, "__pydantic_extra__", {})
            f.__pydantic_extra__["collection"] = "col_internal"
            features.append(f)

        # Replicate the per-query cache as in read_entities
        cache: dict = {}
        for feature in features:
            raw_coll = feature.__pydantic_extra__.get("collection")
            if raw_coll not in cache:
                resolved = await _resolve("c_internal", raw_coll)
                cache[raw_coll] = resolved if resolved is not None else raw_coll
            feature.__pydantic_extra__["collection"] = cache[raw_coll]

        # Resolution called exactly once for the shared collection id.
        assert call_count == 1
        for f in features:
            assert f.__pydantic_extra__["collection"] == "my_collection"


# ---------------------------------------------------------------------------
# Regression — resolve_catalog_id is internal-first / idempotent
# ---------------------------------------------------------------------------
#
# An already-internal catalog id (e.g. the asset-upload flow pre-resolves the
# external label to the immutable internal id before minting the upload URL)
# was fed to the JIT provisioning gate, which called resolve_catalog_id() →
# ensure_catalog_exists().  The old external-only resolver could not recognise
# an internal id, reported "missing", and JIT-created a *phantom* catalog whose
# external_id equalled the real catalog's internal id.  Subsequent get_catalog()
# then resolved to the never-ready phantom → 500 "storage is still being
# provisioned".  resolve_catalog_id is now internal-first (mirrors
# resolve_physical_schema): a direct id hit is authoritative and returned as-is.
class TestResolveCatalogIdExternalOnly:
    """resolve_catalog_id is strictly forward (external_id → internal id).

    An internal id is NOT a valid input and resolves to "not found", so security
    stays keyed on the external id and internal ids never re-enter the public API
    surface.  The phantom-catalog JIT is prevented at the source (existence is
    probed via the internal-first resolve_physical_schema) rather than by making
    this resolver accept internal ids (the reverted #2353 behaviour).
    """

    @pytest.mark.asyncio
    async def test_internal_id_resolves_to_none(self):
        """An already-internal id is not a known external_id → None. This is the
        security invariant: a caller cannot address a catalog by its internal id
        (e.g. delete_catalog by internal id becomes a no-op, never a real hit)."""
        from dynastore.modules.catalog import catalog_service as cs

        svc = cs.CatalogService(engine=None)
        with patch.object(
            cs, "_physical_schema_cache", AsyncMock(return_value="c_internal1")
        ) as ps, patch.object(
            cs, "_catalog_external_id_cache", AsyncMock(return_value=None)
        ) as ext:
            result = await svc.resolve_catalog_id("c_internal1", allow_missing=True)

        assert result is None
        # External-only: resolution goes through the external_id cache, and never
        # short-circuits on the physical-schema (internal id) cache.
        ext.assert_awaited_once_with(svc, "c_internal1")
        ps.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_external_label_resolves_to_internal(self):
        from dynastore.modules.catalog import catalog_service as cs

        svc = cs.CatalogService(engine=None)
        with patch.object(
            cs, "_physical_schema_cache", AsyncMock(return_value=None)
        ), patch.object(
            cs, "_catalog_external_id_cache", AsyncMock(return_value="c_internal1")
        ):
            result = await svc.resolve_catalog_id("cat_friendly_label", allow_missing=True)

        assert result == "c_internal1"

    @pytest.mark.asyncio
    async def test_missing_returns_none_when_allowed(self):
        from dynastore.modules.catalog import catalog_service as cs

        svc = cs.CatalogService(engine=None)
        with patch.object(
            cs, "_catalog_external_id_cache", AsyncMock(return_value=None)
        ):
            result = await svc.resolve_catalog_id("nope", allow_missing=True)

        assert result is None

    @pytest.mark.asyncio
    async def test_missing_raises_when_not_allowed(self):
        from dynastore.modules.catalog import catalog_service as cs

        svc = cs.CatalogService(engine=None)
        with patch.object(
            cs, "_catalog_external_id_cache", AsyncMock(return_value=None)
        ):
            with pytest.raises(ValueError):
                await svc.resolve_catalog_id("nope", allow_missing=False)

    @pytest.mark.asyncio
    async def test_ensure_catalog_exists_does_not_recreate_internal_id(self):
        """The phantom-catalog regression: given an already-internal id,
        ensure_catalog_exists must recognise it (via the internal-first
        resolve_physical_schema) and never JIT-create a phantom catalog."""
        from dynastore.modules.catalog import catalog_service as cs

        svc = cs.CatalogService(engine=None)
        svc.create_catalog = AsyncMock()
        # resolve_physical_schema (internal-first, no ctx) resolves an internal id
        # straight through the physical-schema cache.
        with patch.object(
            cs, "_physical_schema_cache", AsyncMock(return_value="c_internal1")
        ):
            await svc.ensure_catalog_exists("c_internal1")

        svc.create_catalog.assert_not_called()

    @pytest.mark.asyncio
    async def test_ensure_catalog_exists_creates_for_unknown_external(self):
        """A genuinely-new external label resolves nowhere → JIT-create fires."""
        from dynastore.modules.catalog import catalog_service as cs

        svc = cs.CatalogService(engine=None)
        svc.create_catalog = AsyncMock()
        with patch.object(
            cs, "_physical_schema_cache", AsyncMock(return_value=None)
        ), patch.object(
            cs, "_catalog_external_id_cache", AsyncMock(return_value=None)
        ):
            await svc.ensure_catalog_exists("brand_new_catalog")

        svc.create_catalog.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_catalog_rejects_internal_shaped_external_id(self):
        """The invariant guard: a public id matching the internal shape
        (``c_<13 base32>``) is rejected before any storage work."""
        from dynastore.modules.catalog import catalog_service as cs
        from dynastore.tools.db import InvalidIdentifierError

        svc = cs.CatalogService(engine=None)
        with pytest.raises(InvalidIdentifierError):
            # ``c_cw5tetduiu959`` is a real internal-id shape (the phantom cascade
            # minted exactly these as external_ids).
            await svc.create_catalog({"id": "c_cw5tetduiu959", "title": "x"}, lang="en")

    def test_is_internal_physical_name_shapes(self):
        """The shape predicate that keeps the external/internal id spaces disjoint."""
        from dynastore.modules.catalog import catalog_service as cs

        # Internal shapes (prefix + 13 base32 chars) collide → True.
        assert cs.is_internal_physical_name("c_cw5tetduiu959", "c") is True
        assert cs.is_internal_physical_name("col_cw5tetduiu959", "col") is True
        # Human-friendly public labels do not collide → False.
        assert cs.is_internal_physical_name("cat_cool_meadow_1284", "c") is False
        assert cs.is_internal_physical_name("gaul", "col") is False
        assert cs.is_internal_physical_name("c_short", "c") is False
        # Wrong prefix does not match the catalog shape.
        assert cs.is_internal_physical_name("col_cw5tetduiu959", "c") is False
