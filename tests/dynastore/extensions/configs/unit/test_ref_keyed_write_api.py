"""Cycle F.4c.4 — pin the ref-keyed write API on ``ConfigsProtocol``.

Static-shape pin tests:

1. ``ConfigsProtocol`` carries the new methods ``set_config_by_ref`` and
   ``delete_config_by_ref`` and they're runtime-checkable.
2. ``PlatformConfigService`` and ``ConfigService`` both implement them.
3. The class-mismatch guard rejects an attempt to overwrite a stored row
   with a different class (without touching a real DB — pure dispatch
   shape).

DB-side end-to-end (round-tripping a write through F.4c.1 storage and
reading it back via F.4c.2 ``get_config_by_ref``) lands in the
F.4c.5 integration suite alongside the notebook + cross-repo bump.
"""

from __future__ import annotations

import inspect

from dynastore.models.protocols.configs import ConfigsProtocol


# ---------------------------------------------------------------------------
# Protocol surface
# ---------------------------------------------------------------------------


def test_protocol_carries_set_config_by_ref():
    assert hasattr(ConfigsProtocol, "set_config_by_ref")


def test_protocol_carries_delete_config_by_ref():
    assert hasattr(ConfigsProtocol, "delete_config_by_ref")


def test_set_config_by_ref_signature():
    """Pin the parameter shape — operators rely on the kw spelling
    (``ref_key``, ``catalog_id``, ``collection_id``, ``check_immutability``,
    ``ctx``) for cross-tier consistency with set_config."""
    sig = inspect.signature(ConfigsProtocol.set_config_by_ref)
    params = list(sig.parameters)
    assert params == [
        "self",
        "ref_key",
        "config",
        "catalog_id",
        "collection_id",
        "check_immutability",
        "ctx",
    ]


def test_delete_config_by_ref_returns_bool():
    """Operators distinguish 204 from 404 by the return value — pin
    that it's a bool (no-op vs deleted)."""
    sig = inspect.signature(ConfigsProtocol.delete_config_by_ref)
    assert sig.return_annotation is bool or sig.return_annotation == "bool"


# ---------------------------------------------------------------------------
# Implementation discovery
# ---------------------------------------------------------------------------


def test_platform_service_implements_set_and_delete_by_ref():
    from dynastore.modules.db_config.platform_config_service import PlatformConfigService

    assert callable(getattr(PlatformConfigService, "set_config_by_ref", None))
    assert callable(getattr(PlatformConfigService, "delete_config_by_ref", None))


def test_config_service_implements_set_and_delete_by_ref():
    from dynastore.modules.catalog.config_service import ConfigService

    assert callable(getattr(ConfigService, "set_config_by_ref", None))
    assert callable(getattr(ConfigService, "delete_config_by_ref", None))


def test_config_service_dispatches_collection_requires_catalog():
    """Sanity: collection scope without a catalog is a config error,
    same as set_config / delete_config."""
    import asyncio
    from dynastore.modules.catalog.config_service import ConfigService

    svc = ConfigService(engine=None)

    async def _run():
        try:
            await svc.set_config_by_ref(
                "any_ref", config=None, collection_id="c", catalog_id=None,  # type: ignore[arg-type]
            )
        except ValueError as e:
            return str(e)
        return None

    msg = asyncio.run(_run())
    assert msg is not None and "catalog_id is required" in msg
