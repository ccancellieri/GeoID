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

"""Shared building block for the ``setup_catalog`` / ``setup_collection``
fixture pair that used to be hand-rolled independently in six extension
conftest files (features, stac, wfs, moving_features, plus
``modules/catalog`` and ``modules/elasticsearch``).

Every copy followed the same create-via-HTTP-then-yield-id shape and only
differed in the REST prefix and a few behavioral knobs: which client
fixture to use, whether to delete any pre-existing catalog/collection
before creating, how strictly to check the create response, and whether to
delete on teardown. ``build_setup_fixtures`` factors the shape out; callers
supply their own knobs so each suite's existing behavior is preserved
exactly rather than silently unified.

The client fixture is selected via a static dependency declaration (not
``request.getfixturevalue``): looking up an async fixture dynamically from
inside another already-running async fixture trips pytest-asyncio's
``Runner.run() cannot be called from a running event loop`` under this
repo's fixture-loop-scope setup, so both supported client fixtures get
their own thin fixture shell that depends on it by name.
"""

from __future__ import annotations

from typing import Literal

import pytest

AssertMode = Literal["none", "loose", "strict"]
"""How strictly to check the HTTP response of the create call.

- ``"none"``: don't check the response at all.
- ``"loose"``: accept 201 (created) or 409 (already exists).
- ``"strict"``: require 201.
"""


def _check_create_response(resp, mode: AssertMode, what: str) -> None:
    if mode == "none":
        return
    if mode == "strict":
        assert resp.status_code == 201, f"Failed to create {what}: {resp.text}"
        return
    assert resp.status_code in (201, 409), (
        f"Failed to create {what}: {resp.status_code} {resp.text}"
    )


async def _setup_catalog_body(
    client, prefix, catalog_data, catalog_id, *, delete_before, assert_mode, catalog_teardown
):
    if delete_before:
        await client.delete(f"{prefix}/catalogs/{catalog_id}?force=true")

    r = await client.post(f"{prefix}/catalogs", json=catalog_data)
    _check_create_response(r, assert_mode, "setup catalog")

    yield catalog_id

    if catalog_teardown:
        await client.delete(f"{prefix}/catalogs/{catalog_id}?force=true")


async def _setup_collection_body(
    client,
    prefix,
    catalog_id,
    collection_data,
    collection_id,
    *,
    delete_before,
    assert_mode,
    collection_teardown,
):
    if delete_before:
        await client.delete(
            f"{prefix}/catalogs/{catalog_id}/collections/{collection_id}?force=true"
        )

    r = await client.post(
        f"{prefix}/catalogs/{catalog_id}/collections", json=collection_data
    )
    _check_create_response(r, assert_mode, "setup collection")

    yield collection_id

    if collection_teardown:
        await client.delete(
            f"{prefix}/catalogs/{catalog_id}/collections/{collection_id}?force=true"
        )


def build_setup_fixtures(
    prefix: str,
    *,
    client_fixture: str = "sysadmin_in_process_client",
    delete_before: bool = False,
    assert_mode: AssertMode = "none",
    catalog_teardown: bool = False,
    collection_teardown: bool = False,
):
    """Build a ``(setup_catalog, setup_collection)`` pytest fixture pair.

    ``prefix`` is the REST prefix the suite exercises (e.g. ``/features``,
    ``/stac``). ``client_fixture`` selects which HTTP client fixture
    (function-scoped ``sysadmin_in_process_client`` or module-scoped
    ``in_process_client_module``) both fixtures depend on. Both also depend
    on the catalog/collection id and data fixtures already defined in the
    calling conftest (or inherited from ``tests/dynastore/conftest.py``).
    """

    if client_fixture == "sysadmin_in_process_client":

        @pytest.fixture
        async def setup_catalog(sysadmin_in_process_client, catalog_data, catalog_id):
            async for value in _setup_catalog_body(
                sysadmin_in_process_client,
                prefix,
                catalog_data,
                catalog_id,
                delete_before=delete_before,
                assert_mode=assert_mode,
                catalog_teardown=catalog_teardown,
            ):
                yield value

        @pytest.fixture
        async def setup_collection(
            sysadmin_in_process_client, setup_catalog, collection_data, collection_id
        ):
            async for value in _setup_collection_body(
                sysadmin_in_process_client,
                prefix,
                setup_catalog,
                collection_data,
                collection_id,
                delete_before=delete_before,
                assert_mode=assert_mode,
                collection_teardown=collection_teardown,
            ):
                yield value

    elif client_fixture == "in_process_client_module":

        @pytest.fixture
        async def setup_catalog(in_process_client_module, catalog_data, catalog_id):
            async for value in _setup_catalog_body(
                in_process_client_module,
                prefix,
                catalog_data,
                catalog_id,
                delete_before=delete_before,
                assert_mode=assert_mode,
                catalog_teardown=catalog_teardown,
            ):
                yield value

        @pytest.fixture
        async def setup_collection(
            in_process_client_module, setup_catalog, collection_data, collection_id
        ):
            async for value in _setup_collection_body(
                in_process_client_module,
                prefix,
                setup_catalog,
                collection_data,
                collection_id,
                delete_before=delete_before,
                assert_mode=assert_mode,
                collection_teardown=collection_teardown,
            ):
                yield value

    else:
        raise ValueError(f"Unsupported client_fixture: {client_fixture!r}")

    return setup_catalog, setup_collection
