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

"""Unit tests: collection-scoped IAM grants persist the internal collection id.

Regression for the external_id rename: a collection-scoped grant must be
keyed on the immutable internal id so that renaming the collection's public
external_id does not silently break authorization.

The test uses pure in-memory stubs — no DB, no Valkey.
"""
from __future__ import annotations

from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CATALOG_EXTERNAL = "my-catalog"
CATALOG_INTERNAL = "cat_internal_abc123"

COLLECTION_EXTERNAL_V1 = "my-collection"
COLLECTION_EXTERNAL_V2 = "my-collection-renamed"
COLLECTION_INTERNAL = "col_internal_xyz789"

PRINCIPAL_UUID = uuid4()
ROLE_NAME = "viewer"
CATALOG_SCHEMA = "tenant_abc"


def _make_catalogs_protocol(
    cat_ext: str = CATALOG_EXTERNAL,
    cat_int: str = CATALOG_INTERNAL,
    col_ext: str = COLLECTION_EXTERNAL_V1,
    col_int: str = COLLECTION_INTERNAL,
):
    """Return a minimal CatalogsProtocol stub."""
    catalogs = MagicMock()

    async def _resolve_catalog_id(external_id: str, allow_missing: bool = False):
        if external_id == cat_ext:
            return cat_int
        return None

    async def _resolve_collection_id(
        catalog_id: str, external_id: str, allow_missing: bool = False
    ):
        if catalog_id == cat_int and external_id == col_ext:
            return col_int
        return None

    catalogs.resolve_catalog_id = AsyncMock(side_effect=_resolve_catalog_id)
    catalogs.collections = MagicMock()
    catalogs.collections.resolve_collection_id = AsyncMock(
        side_effect=_resolve_collection_id
    )
    return catalogs


# ---------------------------------------------------------------------------
# PolicyService._resolve_collection_internal_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_collection_internal_id_returns_internal():
    """_resolve_collection_internal_id maps external → internal collection id."""
    from dynastore.modules.iam.policies import PolicyService

    svc = PolicyService.__new__(PolicyService)

    catalogs = _make_catalogs_protocol()
    with patch(
        "dynastore.modules.iam.policies.get_protocol",
        return_value=catalogs,
    ):
        result = await svc._resolve_collection_internal_id(
            CATALOG_EXTERNAL, COLLECTION_EXTERNAL_V1
        )

    assert result == COLLECTION_INTERNAL, (
        f"expected internal id {COLLECTION_INTERNAL!r}, got {result!r}"
    )


@pytest.mark.asyncio
async def test_resolve_collection_internal_id_none_collection_passthrough():
    """None collection_id passes through unchanged."""
    from dynastore.modules.iam.policies import PolicyService

    svc = PolicyService.__new__(PolicyService)
    result = await svc._resolve_collection_internal_id(CATALOG_EXTERNAL, None)
    assert result is None


@pytest.mark.asyncio
async def test_resolve_collection_internal_id_protocol_unavailable_passthrough():
    """When CatalogsProtocol is unavailable, external id is returned as-is."""
    from dynastore.modules.iam.policies import PolicyService

    svc = PolicyService.__new__(PolicyService)
    with patch(
        "dynastore.modules.iam.policies.get_protocol",
        return_value=None,
    ):
        result = await svc._resolve_collection_internal_id(
            CATALOG_EXTERNAL, COLLECTION_EXTERNAL_V1
        )
    # graceful degradation — returns the original value, never raises
    assert result == COLLECTION_EXTERNAL_V1


# ---------------------------------------------------------------------------
# Rename simulation: grant written with V1 external_id, queried with V2
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collection_grant_survives_rename():
    """Simulate a rename: grant stored with internal id, query with new external id.

    Steps:
    1. Write grant: external_id V1 → resolved to internal id → stored as resource_ref.
    2. Rename: collection external_id changes from V1 to V2.
    3. Query grant: external_id V2 → resolved to same internal id → grant still found.
    """
    from dynastore.modules.iam.policies import PolicyService

    svc = PolicyService.__new__(PolicyService)

    stored_resource_ref: Optional[str] = None

    # --- Step 1: write path (simulated admin POST) ---
    catalogs_v1 = _make_catalogs_protocol(
        col_ext=COLLECTION_EXTERNAL_V1,
        col_int=COLLECTION_INTERNAL,
    )
    with patch("dynastore.modules.iam.policies.get_protocol", return_value=catalogs_v1):
        stored_resource_ref = await svc._resolve_collection_internal_id(
            CATALOG_EXTERNAL, COLLECTION_EXTERNAL_V1
        )

    assert stored_resource_ref == COLLECTION_INTERNAL, "grant must store internal id"

    # --- Step 2: rename (external_id V1 → V2 in DB; internal id unchanged) ---

    # --- Step 3: read path (simulated grant query after rename) ---
    catalogs_v2 = _make_catalogs_protocol(
        col_ext=COLLECTION_EXTERNAL_V2,  # new public name
        col_int=COLLECTION_INTERNAL,      # same internal id
    )
    with patch("dynastore.modules.iam.policies.get_protocol", return_value=catalogs_v2):
        query_resource_ref = await svc._resolve_collection_internal_id(
            CATALOG_EXTERNAL, COLLECTION_EXTERNAL_V2
        )

    assert query_resource_ref == COLLECTION_INTERNAL, (
        "after rename, new external_id must still resolve to the same internal id"
    )
    assert stored_resource_ref == query_resource_ref, (
        "stored resource_ref == query resource_ref → grant still applies after rename"
    )


# ---------------------------------------------------------------------------
# admin _resolve_collection_internal_id helper
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admin_resolve_collection_internal_id():
    """admin_service._resolve_collection_internal_id returns internal id."""
    from dynastore.extensions.admin.admin_service import (
        _resolve_collection_internal_id as admin_resolve,
    )

    catalogs = _make_catalogs_protocol()
    with patch(
        "dynastore.extensions.admin.admin_service.get_protocol",
        return_value=catalogs,
    ):
        result = await admin_resolve(CATALOG_INTERNAL, COLLECTION_EXTERNAL_V1)

    assert result == COLLECTION_INTERNAL


@pytest.mark.asyncio
async def test_admin_resolve_collection_passthrough_when_protocol_absent():
    """admin_service helper returns external id when CatalogsProtocol absent."""
    from dynastore.extensions.admin.admin_service import (
        _resolve_collection_internal_id as admin_resolve,
    )

    with patch(
        "dynastore.extensions.admin.admin_service.get_protocol",
        return_value=None,
    ):
        result = await admin_resolve(CATALOG_INTERNAL, COLLECTION_EXTERNAL_V1)

    assert result == COLLECTION_EXTERNAL_V1
