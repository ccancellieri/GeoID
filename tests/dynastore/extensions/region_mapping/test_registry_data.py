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

"""DB-free unit tests for ``registry_data`` — the region_mapping extension's
shared claim-computation + ItemsSchema kernel (dynastore#443 Phase 1).
"""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# compute_claim_set
# ---------------------------------------------------------------------------


def test_compute_claim_set_default_alias_is_column() -> None:
    from dynastore.extensions.region_mapping.registry_data import compute_claim_set

    claims = compute_claim_set(
        catalog_id="fao", collection_id="countries",
        column="adm0_code", alias=None, extra_aliases=[],
    )

    # column == canonical_alias -> {column, "fao_column"} = 2 distinct claims.
    values = {claim for claim, _role in claims.values()}
    assert values == {"adm0_code", "fao_adm0_code"}


def test_compute_claim_set_explicit_alias_marks_primary() -> None:
    from dynastore.extensions.region_mapping.registry_data import compute_claim_set

    claims = compute_claim_set(
        catalog_id="fao", collection_id="countries",
        column="adm0_code", alias="country", extra_aliases=["adm0"],
    )

    roles_by_claim = {claim: role for claim, role in claims.values()}
    assert roles_by_claim["country"] == "primary"
    assert roles_by_claim["adm0_code"] == "alias"
    assert roles_by_claim["adm0"] == "alias"
    assert roles_by_claim["fao_country"] == "alias"
    assert len(roles_by_claim) == 4


def test_compute_claim_set_casefold_dedup() -> None:
    """Two candidates differing only by case collapse to one claim record."""
    from dynastore.extensions.region_mapping.registry_data import compute_claim_set

    claims = compute_claim_set(
        catalog_id="fao", collection_id="countries",
        column="Country", alias="country", extra_aliases=[],
    )

    # column ("Country") casefolds identically to alias ("country") -> 2
    # distinct claims total ({country, fao_country}), not 3.
    assert len(claims) == 2
    assert "country" in claims  # casefolded key


def test_compute_claim_set_exactly_one_primary() -> None:
    from dynastore.extensions.region_mapping.registry_data import compute_claim_set

    claims = compute_claim_set(
        catalog_id="fao", collection_id="countries",
        column="adm0_code", alias="country", extra_aliases=["adm0", "iso3"],
    )

    primaries = [claim for claim, role in claims.values() if role == "primary"]
    assert primaries == ["country"]


def test_compute_claim_set_rejects_regex_metacharacters() -> None:
    from dynastore.extensions.region_mapping.registry_data import compute_claim_set

    with pytest.raises(ValueError, match="regex metacharacters"):
        compute_claim_set(
            catalog_id="fao", collection_id="countries",
            column="adm0.code", alias=None, extra_aliases=[],
        )


def test_compute_claim_set_rejects_regex_metacharacters_in_extra_alias() -> None:
    from dynastore.extensions.region_mapping.registry_data import compute_claim_set

    with pytest.raises(ValueError, match="regex metacharacters"):
        compute_claim_set(
            catalog_id="fao", collection_id="countries",
            column="adm0", alias="country", extra_aliases=["adm(0)"],
        )


def test_validate_claim_text_accepts_plain_literal() -> None:
    from dynastore.extensions.region_mapping.registry_data import validate_claim_text

    validate_claim_text("adm0_code")  # must not raise


# ---------------------------------------------------------------------------
# mapping_id_for / slugify / item_id_for
# ---------------------------------------------------------------------------


def test_mapping_id_for_slugifies() -> None:
    from dynastore.extensions.region_mapping.registry_data import mapping_id_for

    assert mapping_id_for("FAO Catalog", "Country Boundaries!") == "fao_catalog_country_boundaries"


def test_item_id_for_is_stable_and_scoped_to_mapping() -> None:
    from dynastore.extensions.region_mapping.registry_data import item_id_for

    id_a = item_id_for("fao_countries", "country")
    id_b = item_id_for("who_countries", "country")

    assert id_a != id_b, "same claim text under a different mapping must get a different item id"
    assert item_id_for("fao_countries", "country") == id_a, "must be deterministic"


# ---------------------------------------------------------------------------
# is_degenerate_bbox
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bbox,expected",
    [
        (None, True),
        ([], True),
        ([0.0, 0.0, 0.0], True),  # too short
        ([0.0, 0.0, 0.0, 0.0], True),  # zero-area
        ([10.0, 10.0, 10.0, 20.0], True),  # zero-width
        ([-180.0, -90.0, 180.0, 90.0], False),
        ([10.0, 20.0, 30.0, 40.0], False),
    ],
)
def test_is_degenerate_bbox(bbox, expected) -> None:
    from dynastore.extensions.region_mapping.registry_data import is_degenerate_bbox

    assert is_degenerate_bbox(bbox) is expected


# ---------------------------------------------------------------------------
# ItemsSchema — claim_ci must carry a native UNIQUE constraint (unique=True)
# ---------------------------------------------------------------------------


def test_registry_items_schema_claim_ci_is_unique() -> None:
    from dynastore.extensions.region_mapping.registry_data import build_registry_items_schema

    schema = build_registry_items_schema()

    assert schema.fields["claim_ci"].unique is True
    # Sibling fields must not carry the constraint.
    assert schema.fields["claim"].unique is False
    assert schema.fields["mapping_id"].unique is False


def test_registry_items_schema_carries_expected_fields() -> None:
    from dynastore.extensions.region_mapping.registry_data import build_registry_items_schema

    schema = build_registry_items_schema()

    expected = {
        "claim_ci", "claim", "mapping_id", "role",
        "src_catalog", "src_collection", "region_prop", "alias", "title",
    }
    assert set(schema.fields) == expected
