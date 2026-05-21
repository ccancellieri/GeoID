"""Regression: ``physical_schema`` must resolve from the authoritative registry,
not from the cached ``Catalog`` model.

Root cause (production outage on a slim, read-only service profile): the
``Catalog`` model carried ``physical_schema`` as an ``exclude=True`` field and
``resolve_physical_schema`` read it off the distributed catalog-model cache. The
distributed (Valkey / L2) encoder serializes Pydantic models via
``model_dump_json()``, which honors ``exclude=True`` — so ``physical_schema`` was
stripped on every L2 round-trip. A process reading the model cold from L2 then
resolved ``None`` ("No physical schema found"), even though the
``catalog.catalogs`` column was correctly populated. The persistent store was
fine; the cache silently dropped the field.

Fix: ``resolve_physical_schema`` reads a plain string from the registry behind a
lossless string cache (``_physical_schema_cache``), and the field is no longer
carried on the platform model. The cache is a transient accelerator; a cold miss
always falls back to the registry SELECT.
"""

import pytest
from pydantic import BaseModel, Field

from dynastore.models.shared_models import Catalog
from dynastore.modules.catalog.catalog_service import (
    CatalogService,
    _physical_schema_cache,
)


class _ExcludedField(BaseModel):
    keep: str = "kept"
    secret: str | None = Field(default=None, exclude=True)


def test_l2_encoder_drops_exclude_true_fields():
    """Why the model cache cannot carry ``physical_schema``: the L2 (Valkey)
    encoder serializes models through ``model_dump_json()``, which omits
    ``exclude=True`` fields before they ever reach the wire."""
    from dynastore.tools.cache_valkey import _serialize

    blob = _serialize(_ExcludedField(secret="s_secret_value"))
    assert b"s_secret_value" not in blob


def test_plain_string_round_trips_losslessly_through_l2():
    """The chosen representation (a bare string) survives the L2 encoder intact."""
    from dynastore.tools.cache_valkey import _serialize, _deserialize

    assert _deserialize(_serialize("s_abc12345")) == "s_abc12345"


def test_catalog_model_does_not_expose_physical_schema():
    """The platform model no longer carries the storage detail, so it cannot be
    serialized into API output or the distributed cache."""
    assert "physical_schema" not in Catalog.model_fields


@pytest.mark.asyncio
async def test_resolve_uses_registry_not_stripped_model(monkeypatch):
    """RED→GREEN: even when the cached model lost ``physical_schema`` (as it does
    across L2), resolution returns the correct schema from the registry."""
    _physical_schema_cache.cache_clear()
    svc = CatalogService.__new__(CatalogService)

    async def _stripped_model(cid):
        # Mimics a model rehydrated from L2: no physical_schema.
        return Catalog.model_validate({"id": cid})

    async def _registry(cid):
        return "s_correct"

    monkeypatch.setattr(svc, "_get_catalog_model_db", _stripped_model)
    monkeypatch.setattr(svc, "_get_physical_schema_db", _registry)

    # No ctx → goes through the string cache → registry fallback.
    assert await svc.resolve_physical_schema("cat_redgreen_1") == "s_correct"


@pytest.mark.asyncio
async def test_resolve_missing_catalog_raises_unless_allow_missing(monkeypatch):
    _physical_schema_cache.cache_clear()
    svc = CatalogService.__new__(CatalogService)

    async def _absent(cid):
        return None

    monkeypatch.setattr(svc, "_get_physical_schema_db", _absent)

    with pytest.raises(ValueError, match="not found"):
        await svc.resolve_physical_schema("cat_absent_1")

    assert await svc.resolve_physical_schema("cat_absent_2", allow_missing=True) is None
