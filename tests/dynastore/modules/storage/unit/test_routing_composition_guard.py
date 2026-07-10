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

"""#1047 composition guard — a public-ES collection requires a public-ES
parent catalog.

The canonical ES index architecture (#1047 SSOT) keeps a single global
``{prefix}-catalogs`` index and a single global ``{prefix}-collections``
index. Membership is expressed by pinning the public envelope ES driver in
``operations[INDEX]`` (lane-model canonical shape, #2494 — the ES indexer
rides the async INDEX lane). The enforceable invariant is composition rule
1: a globally-searchable collection envelope ("public collection") may only
live under a globally-navigable catalog ("public catalog"). A public
collection under a private / PG-only catalog would leak the collection into
global search while its parent is not navigable, so it is rejected.

These tests pin the pure decision helpers; they do not exercise the
DB-backed validate handler (that resolves the parent via the configs
protocol — covered by integration tests). IAM remains the access SSOT; this
index split is defense-in-depth.
"""

from __future__ import annotations

import pytest

from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    FailurePolicy,
    Operation,
    OperationDriverEntry,
    _assert_public_collection_has_public_parent,
    _catalog_routing_is_public,
    _collection_routing_is_public,
)


# ---------------------------------------------------------------------------
# Builders — minimal routing configs at the WRITE / INDEX tiers
# ---------------------------------------------------------------------------


def _public_collection_routing() -> CollectionRoutingConfig:
    """Collection routing that pins the public collection ES driver in
    INDEX (the collection envelope is globally searchable)."""
    return CollectionRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.INDEX: [
                OperationDriverEntry(driver_ref="collection_elasticsearch_driver"),
            ],
        },
    )


def _private_collection_routing() -> CollectionRoutingConfig:
    """Collection routing with PG only — no public ES pin in INDEX, so the
    collection is not globally searchable (private)."""
    return CollectionRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
        },
    )


def _public_catalog_routing() -> CatalogRoutingConfig:
    """Catalog routing that pins the public catalog ES driver in INDEX (the
    catalog envelope is globally navigable)."""
    return CatalogRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.INDEX: [
                OperationDriverEntry(driver_ref="catalog_elasticsearch_driver"),
            ],
        },
    )


def _private_catalog_routing() -> CatalogRoutingConfig:
    """Catalog routing with PG only — no public ES pin in INDEX, so the
    catalog is not globally navigable (private / PG-only)."""
    return CatalogRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
        },
    )


# ---------------------------------------------------------------------------
# Public-detection helpers
# ---------------------------------------------------------------------------


def test_collection_routing_is_public_detects_es_index_pin():
    assert _collection_routing_is_public(_public_collection_routing()) is True


def test_collection_routing_is_public_false_when_pg_only():
    assert _collection_routing_is_public(_private_collection_routing()) is False


def test_collection_routing_is_public_false_when_es_only_in_read():
    """Public ES driver in READ but NOT in INDEX does not count as public
    for membership — INDEX is the membership key (#1047 SSOT, #2494 lane
    model)."""
    routing = CollectionRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(driver_ref="collection_elasticsearch_driver"),
            ],
        },
    )
    assert _collection_routing_is_public(routing) is False


def test_catalog_routing_is_public_detects_es_index_pin():
    assert _catalog_routing_is_public(_public_catalog_routing()) is True


def test_catalog_routing_is_public_false_when_pg_only():
    assert _catalog_routing_is_public(_private_catalog_routing()) is False


# ---------------------------------------------------------------------------
# Composition guard — the cross-tier invariant
# ---------------------------------------------------------------------------


def test_reject_public_collection_under_private_catalog():
    """Rule 1 / rule 3: a public collection under a private (PG-only) catalog
    is rejected — it would leak the collection envelope into global search
    while the parent catalog is not navigable."""
    with pytest.raises(ValueError, match="public collection"):
        _assert_public_collection_has_public_parent(
            _public_collection_routing(),
            _private_catalog_routing(),
        )


def test_reject_public_collection_when_parent_unresolved():
    """Fail-closed: if the parent catalog routing config cannot be resolved,
    a public collection is rejected rather than allowed through."""
    with pytest.raises(ValueError, match="public collection"):
        _assert_public_collection_has_public_parent(
            _public_collection_routing(),
            None,
        )


def test_accept_public_collection_under_public_catalog():
    """The happy path: public collection under a public catalog is allowed."""
    _assert_public_collection_has_public_parent(
        _public_collection_routing(),
        _public_catalog_routing(),
    )


def test_accept_private_collection_under_private_catalog():
    """Rule 2/3: a private collection (no public ES pin) is always accepted,
    regardless of parent catalog visibility — including under a private
    catalog."""
    _assert_public_collection_has_public_parent(
        _private_collection_routing(),
        _private_catalog_routing(),
    )


def test_accept_private_collection_under_public_catalog():
    """Rule 2: a public catalog may contain a mix of public and private
    collections — a private collection under a public catalog is fine."""
    _assert_public_collection_has_public_parent(
        _private_collection_routing(),
        _public_catalog_routing(),
    )


def test_accept_private_collection_when_parent_unresolved():
    """A private collection imposes no parent requirement, so an unresolved
    parent is harmless for it (the guard only constrains public children)."""
    _assert_public_collection_has_public_parent(
        _private_collection_routing(),
        None,
    )
