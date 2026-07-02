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

"""#2655: ``canonical_index_read`` threads the real ``CollectionInfo.kind``
(+ ``allow_geometry``) into ``ItemService.map_row_to_feature`` via
``_resolve_collection_type`` / ``_extract_feature_parts`` — closing the same
missing-context gap ``_resolve_sidecars_for`` was already fixed for in #2670.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.ogc import Feature


def _make_col_config():
    cfg = MagicMock()
    cfg.sidecars = []
    return cfg


@pytest.mark.asyncio
async def test_records_collection_type_reaches_map_row_to_feature():
    """A RECORDS collection's kind/allow_geometry are resolved once and
    threaded into every row's map_row_to_feature call."""
    from dynastore.modules.catalog.canonical_index_read import (
        read_canonical_index_inputs,
    )
    from dynastore.modules.catalog.catalog_config import CollectionInfo, CollectionKind

    raw_row = {"geoid": "gid-1", "attributes": "{}"}
    stub_feature = Feature(type="Feature", geometry=None, properties={}, id="gid-1")

    mock_map_row = MagicMock(return_value=stub_feature)

    with (
        patch(
            "dynastore.modules.catalog.canonical_index_read._fetch_raw_rows",
            new=AsyncMock(return_value={"gid-1": raw_row}),
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._resolve_sidecars_for",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._get_col_config",
            new=AsyncMock(return_value=_make_col_config()),
        ),
        patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=AsyncMock(
                get_config=AsyncMock(
                    return_value=CollectionInfo(kind=CollectionKind.RECORDS)
                )
            ),
        ),
        patch(
            "dynastore.modules.catalog.item_service.ItemService.map_row_to_feature",
            mock_map_row,
        ),
    ):
        result = await read_canonical_index_inputs("cat1", "col1", ["gid-1"])

    assert "gid-1" in result
    _, kwargs = mock_map_row.call_args
    assert kwargs["collection_type"] == "RECORDS"


@pytest.mark.asyncio
async def test_no_configs_protocol_resolves_vector_default():
    """No ConfigsProtocol registered → CollectionInfo() default (VECTOR),
    matching the pre-#2655 fallback behaviour for that edge case."""
    from dynastore.modules.catalog.canonical_index_read import (
        read_canonical_index_inputs,
    )

    raw_row = {"geoid": "gid-2", "attributes": "{}"}
    stub_feature = Feature(type="Feature", geometry=None, properties={}, id="gid-2")

    mock_map_row = MagicMock(return_value=stub_feature)

    with (
        patch(
            "dynastore.modules.catalog.canonical_index_read._fetch_raw_rows",
            new=AsyncMock(return_value={"gid-2": raw_row}),
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._resolve_sidecars_for",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._get_col_config",
            new=AsyncMock(return_value=_make_col_config()),
        ),
        patch("dynastore.tools.discovery.get_protocol", return_value=None),
        patch(
            "dynastore.modules.catalog.item_service.ItemService.map_row_to_feature",
            mock_map_row,
        ),
    ):
        result = await read_canonical_index_inputs("cat1", "col1", ["gid-2"])

    assert "gid-2" in result
    _, kwargs = mock_map_row.call_args
    assert kwargs["collection_type"] == "VECTOR"
    assert kwargs["allow_geometry"] is None
