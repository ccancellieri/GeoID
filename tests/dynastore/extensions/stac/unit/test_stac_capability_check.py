"""Unit tests for the STAC catalog-creation capability precheck.

``_assert_stac_capable_metadata_stack`` refuses a STAC catalog create
when the ``CatalogStore`` registry has no driver implementing
``StacCatalogEntityStoreCapability`` (the sub-Protocol owned by the STAC
extension at ``extensions/stac/protocols.py``). Default PG deployment
satisfies the check once the ``modules/stac/`` module is loaded
(STAC PG drivers register at both tiers via ``StacModule.lifespan``);
custom configs pointing at STAC-blind backends must fail loudly so
the STAC envelope isn't silently dropped on write.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from dynastore.extensions.stac.protocols import (
    StacCatalogEntityStoreCapability,
    StacCollectionEntityStoreCapability,
)
from dynastore.extensions.stac.stac_service import (
    _assert_stac_capable_metadata_stack,
)


def _stac_driver(spec_cls):
    """Return a MagicMock that satisfies ``isinstance(d, spec_cls)``.

    Uses ``spec=spec_cls`` so the mock declares the same attribute /
    method surface as the Protocol — enough for ``runtime_checkable``
    ``isinstance`` to pass.
    """
    return MagicMock(spec=spec_cls)


def _core_driver():
    """Return a MagicMock with no STAC marker — ``isinstance`` against
    any STAC sub-Protocol returns False because the marker method
    ``stac_metadata_columns`` is absent.
    """
    d = MagicMock(spec=[])  # empty spec — no attributes at all
    return d


def test_raises_when_no_catalog_stac_driver_registered():
    """No CatalogStore satisfying StacCatalogEntityStoreCapability → reject."""

    def _get_protocols(proto_cls):
        # Collection-tier has a STAC driver; catalog-tier does not.
        if proto_cls.__name__ == "CatalogStore":
            return [_core_driver()]
        return [_stac_driver(StacCollectionEntityStoreCapability)]

    with patch(
        "dynastore.extensions.stac.stac_service.get_protocols",
        side_effect=_get_protocols,
    ):
        with pytest.raises(HTTPException) as exc:
            _assert_stac_capable_metadata_stack()

    assert exc.value.status_code == 422
    assert "StacCatalogEntityStoreCapability" in exc.value.detail


def test_warns_but_proceeds_when_no_collection_stac_driver_registered(caplog):
    """Missing collection-tier STAC driver is a WARNING, not a reject."""

    def _get_protocols(proto_cls):
        if proto_cls.__name__ == "CollectionStore":
            return [_core_driver()]
        return [_stac_driver(StacCatalogEntityStoreCapability)]

    with patch(
        "dynastore.extensions.stac.stac_service.get_protocols",
        side_effect=_get_protocols,
    ):
        with caplog.at_level("WARNING"):
            _assert_stac_capable_metadata_stack()  # should not raise

    assert any(
        "StacCollectionEntityStoreCapability" in r.message
        for r in caplog.records
    )


def test_passes_when_both_tiers_have_stac_driver():
    """Default PG config: STAC drivers at both tiers → no raise."""

    def _get_protocols(proto_cls):
        if proto_cls.__name__ == "CatalogStore":
            return [_core_driver(), _stac_driver(StacCatalogEntityStoreCapability)]
        return [_core_driver(), _stac_driver(StacCollectionEntityStoreCapability)]

    with patch(
        "dynastore.extensions.stac.stac_service.get_protocols",
        side_effect=_get_protocols,
    ):
        _assert_stac_capable_metadata_stack()  # should not raise


def test_raises_when_registry_empty_on_catalog_tier():
    """Empty catalog-tier registry → hard reject (catalog can't be STAC)."""

    with patch(
        "dynastore.extensions.stac.stac_service.get_protocols",
        return_value=[],
    ):
        with pytest.raises(HTTPException) as exc:
            _assert_stac_capable_metadata_stack()

    assert exc.value.status_code == 422
    assert "StacCatalogEntityStoreCapability" in exc.value.detail


def test_warns_when_collection_wrapper_satisfies_capability_but_returns_empty_columns(caplog):
    """Wrapper composition driver style (PR 1e step 3b semantics):
    a driver always exposes ``stac_metadata_columns`` as a method to keep
    structural ``isinstance`` honest, but in deployments without the stac
    extra installed the call returns ``()`` — STAC slice would still be
    silently dropped on write.  ``_has_stac`` must treat empty columns
    as "STAC unavailable" so the WARNING fires.
    """

    class _CapableButEmptyWrapper:
        """Wrapper-shaped fake: passes isinstance, returns ()."""

        def stac_metadata_columns(self):
            return ()

    def _get_protocols(proto_cls):
        if proto_cls.__name__ == "CollectionStore":
            return [_CapableButEmptyWrapper()]
        return [_stac_driver(StacCatalogEntityStoreCapability)]

    with patch(
        "dynastore.extensions.stac.stac_service.get_protocols",
        side_effect=_get_protocols,
    ):
        with caplog.at_level("WARNING"):
            _assert_stac_capable_metadata_stack()  # should not raise

    assert any(
        "StacCollectionEntityStoreCapability" in r.message
        for r in caplog.records
    ), "WARNING must fire when the wrapper exposes the marker but returns empty columns"
