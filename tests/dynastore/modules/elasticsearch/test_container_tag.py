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
"""Task 1 — Container classification in the queryables SSOT (refs #1800).

Asserts that ``classify_container`` resolves each field name to the correct
container and that ``FieldDefinition`` accepts a ``container`` attribute.
"""
from __future__ import annotations

import pytest

from dynastore.modules.storage.computed_fields import (
    SYSTEM_FIELD_KEYS,
    classify_container,
)
from dynastore.models.protocols.field_definition import FieldDefinition


# ---------------------------------------------------------------------------
# Identity fields
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name", ["external_id", "asset_id", "geoid"])
def test_identity_fields_classify_as_identity(name: str) -> None:
    """The three platform identity axes must always resolve to 'identity'."""
    fd = FieldDefinition(name=name, data_type="string")
    assert classify_container(name, fd) == "identity"


# ---------------------------------------------------------------------------
# System fields (SYSTEM_FIELD_KEYS minus identity)
# ---------------------------------------------------------------------------

_SYSTEM_ONLY = [k for k in SYSTEM_FIELD_KEYS if k not in ("external_id", "asset_id", "geoid")]


@pytest.mark.parametrize("name", _SYSTEM_ONLY)
def test_system_fields_classify_as_system(name: str) -> None:
    """Fields in SYSTEM_FIELD_KEYS (minus the identity set) resolve to 'system'."""
    fd = FieldDefinition(name=name, data_type="string")
    assert classify_container(name, fd) == "system"


# ---------------------------------------------------------------------------
# Stats fields — sidecar-produced computed names
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "name",
    [
        "area",
        "centroid",
        "s2_7",
        "s2_12",
        "h3_5",
        "h3_10",
        "geohash_6",
        "geohash_8",
    ],
)
def test_computed_sidecar_names_classify_as_stats(name: str) -> None:
    """Geometry-derived computed names that are NOT in SYSTEM_FIELD_KEYS → 'stats'."""
    fd = FieldDefinition(name=name, data_type="double" if name == "area" else "string")
    # Pass container="stats" explicitly to simulate a sidecar-tagged definition;
    # the classifier must honour the tag when provided.
    fd_tagged = FieldDefinition(name=name, data_type="double" if name == "area" else "string", container="stats")
    assert classify_container(name, fd_tagged) == "stats"

    # The classifier must also derive 'stats' purely from the name pattern when
    # the field_def carries the default container.
    assert classify_container(name, fd) == "stats"


@pytest.mark.parametrize(
    "name",
    [
        "s2_res12",
        "s2_res_12",
        "h3_res10",
        "geohash_res6",
    ],
)
def test_custom_named_spatial_cells_classify_as_stats(name: str) -> None:
    """Spatial-cell fields named with a ``res`` token (e.g. the geoid preset's
    ``s2_res12``) must still classify to 'stats' from the name pattern alone.

    Regression: the original pattern ``^(s2|h3|geohash)_\\d+$`` did not match a
    ``res`` infix, so ``s2_res12`` fell through to 'properties' and got swept
    into ``properties.extras`` by the ES projection.
    """
    fd = FieldDefinition(name=name, data_type="string")
    assert classify_container(name, fd) == "stats"


# ---------------------------------------------------------------------------
# Properties — user / STAC attributes
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "name",
    [
        "datetime",
        "eo:cloud_cover",
        "platform",
        "my_custom_field",
    ],
)
def test_user_stac_attrs_classify_as_properties(name: str) -> None:
    """STAC common metadata and arbitrary user attributes resolve to 'properties'."""
    fd = FieldDefinition(name=name, data_type="string")
    assert classify_container(name, fd) == "properties"


# ---------------------------------------------------------------------------
# Metadata fields — multilingual title / description / keywords (refs #1828)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "name",
    [
        "title",
        "description",
        "keywords",
        "item_title",
        "item_description",
        "item_keywords",
    ],
)
def test_descriptive_metadata_classifies_as_metadata(name: str) -> None:
    """Descriptive multilingual metadata routes to the 'metadata' container, not
    'properties' — so the strict mapping types it as localized text and the read
    projector resolves it per ?lang= instead of leaking into properties."""
    fd = FieldDefinition(name=name, data_type="string")
    assert classify_container(name, fd) == "metadata"


@pytest.mark.parametrize(
    "container",
    ["metadata", "extras", "stac", "assets", "access"],
)
def test_field_definition_accepts_new_open_containers(container: str) -> None:
    """FieldDefinition accepts the v2 namespace containers (refs #1828)."""
    fd = FieldDefinition(name="x", data_type="string", container=container)  # type: ignore[arg-type]
    assert fd.container == container
    # An explicit tag is honoured by classify_container (rule 3) for a plain name.
    assert classify_container("x", fd) == container


# ---------------------------------------------------------------------------
# FieldDefinition.container attribute (Task 1 additive change)
# ---------------------------------------------------------------------------

def test_field_definition_accepts_container_default() -> None:
    """FieldDefinition can be built without specifying container; default is 'properties'."""
    fd = FieldDefinition(name="my_field", data_type="string")
    assert fd.container == "properties"


@pytest.mark.parametrize("container", ["identity", "properties", "stats", "system"])
def test_field_definition_accepts_valid_containers(container: str) -> None:
    """FieldDefinition accepts all four valid container literals."""
    fd = FieldDefinition(name="x", data_type="string", container=container)
    assert fd.container == container


def test_field_definition_rejects_invalid_container() -> None:
    """An unknown container value must be rejected by Pydantic validation."""
    with pytest.raises(Exception):  # ValidationError subclass
        FieldDefinition(name="x", data_type="string", container="unknown_bucket")


# ---------------------------------------------------------------------------
# classify_container honours an explicit tag on the FieldDefinition
# ---------------------------------------------------------------------------

def test_classify_container_explicit_tag_wins_for_non_identity_non_system() -> None:
    """An explicit container='stats' on a normal property name must be honoured
    — but identity and system names must still win regardless of the tag."""
    fd_stats = FieldDefinition(name="my_stat", data_type="double", container="stats")
    assert classify_container("my_stat", fd_stats) == "stats"


def test_classify_container_identity_wins_over_explicit_tag() -> None:
    """identity classification wins even if the FieldDefinition carries a different tag."""
    fd = FieldDefinition(name="geoid", data_type="string", container="properties")
    assert classify_container("geoid", fd) == "identity"


def test_classify_container_system_wins_over_explicit_tag() -> None:
    """system classification wins even if the FieldDefinition carries a different tag."""
    fd = FieldDefinition(name="transaction_time", data_type="timestamp", container="properties")
    assert classify_container("transaction_time", fd) == "system"
