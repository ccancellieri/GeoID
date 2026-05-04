"""CI guard: every concrete ``_PluginDriverConfig`` subclass must be bound
to a :class:`TypedDriver` — otherwise :meth:`_PluginDriverConfig.class_key`
silently falls back to ``__qualname__`` and the operator-facing JSON wire
key drifts away from the routing-entry ``driver_id``.

This walks every concrete subclass of ``_PluginDriverConfig`` reachable
after importing the codebase's driver modules and calls
:meth:`_PluginDriverConfig.assert_bound` on each — an orphan raises a
loud ``RuntimeError`` listing the missing
``class XDriver(TypedDriver[XDriverConfig])`` declaration.

When a new driver module lands, add its import here so its
``*DriverConfig`` subclass becomes reachable via ``__subclasses__()``.
The deliberately-explicit import list is the forcing function — silent
failure (a Config registered but never imported) would defeat the whole
guard.
"""
from __future__ import annotations

import pytest

# Import every known driver module so its ``_PluginDriverConfig``
# subclasses become reachable via ``__subclasses__()``.  Order doesn't
# matter — we just need each module's class-creation side effects to fire.
import dynastore.modules.storage.driver_config  # noqa: F401
import dynastore.modules.storage.drivers.catalog_postgresql  # noqa: F401
import dynastore.modules.storage.drivers.collection_postgresql  # noqa: F401
import dynastore.modules.storage.drivers.postgresql  # noqa: F401
import dynastore.modules.storage.drivers.duckdb  # noqa: F401
import dynastore.modules.storage.drivers.iceberg  # noqa: F401
import dynastore.modules.storage.drivers.bigquery  # noqa: F401
import dynastore.modules.storage.drivers.elasticsearch  # noqa: F401
import dynastore.modules.storage.drivers.elasticsearch_private.driver  # noqa: F401
import dynastore.modules.storage.drivers.core_postgresql  # noqa: F401
import dynastore.modules.stac.drivers.postgresql  # noqa: F401
import dynastore.modules.elasticsearch.collection_es_driver  # noqa: F401
import dynastore.modules.elasticsearch.catalog_es_driver  # noqa: F401
import dynastore.modules.catalog.drivers.pg_asset_driver  # noqa: F401

from dynastore.models.protocols.typed_driver import _PluginDriverConfig


def _all_concrete_subclasses(root: type) -> list[type]:
    """Return every concrete leaf subclass of ``root`` reachable via
    recursive ``__subclasses__()`` walk, skipping known abstract bases
    AND test-local subclasses defined inside test function bodies.

    The ``<locals>`` filter is necessary because pytest-xdist may place
    this test in the same worker process as ``test_typed_driver.py``,
    which intentionally creates orphan and named-with-non-Config
    ``_PluginDriverConfig`` subclasses inside test function scope to
    exercise edge cases of the bind machinery.  Those locals stay
    referenced in ``__subclasses__()`` for the lifetime of the worker
    even after the test function returns, so without this filter the
    orphan-guard would conflate test fixtures with production code.
    """
    seen: set[type] = set()
    out: list[type] = []
    stack: list[type] = list(root.__subclasses__())
    while stack:
        cls = stack.pop()
        if cls in seen:
            continue
        seen.add(cls)
        stack.extend(cls.__subclasses__())
        if cls.__dict__.get("is_abstract_base", False):
            continue
        if "<locals>" in cls.__qualname__:
            continue  # test-local fixture, not a production driver config
        out.append(cls)
    return out


def test_every_concrete_driver_config_is_bound():
    """Walks every concrete ``_PluginDriverConfig`` and asserts each is
    bound to a :class:`TypedDriver`.  An orphan surfaces as a single
    ``RuntimeError`` naming the missing driver declaration.
    """
    concrete = _all_concrete_subclasses(_PluginDriverConfig)
    # Sanity: the imports above must surface at least the well-known
    # production driver configs, otherwise the test is a tautology.
    assert len(concrete) >= 10, (
        f"Only {len(concrete)} concrete _PluginDriverConfig subclasses "
        "found — the import block at the top of this test is stale.  "
        "Add the new driver module so its config class is reachable."
    )

    orphans: list[str] = []
    for cfg_cls in concrete:
        try:
            cfg_cls.assert_bound()
        except RuntimeError as exc:
            orphans.append(f"  - {cfg_cls.__qualname__}: {exc}")

    if orphans:
        pytest.fail(
            "Orphan _PluginDriverConfig subclass(es) — no TypedDriver "
            "class binds them, so the wire key falls back to "
            "__qualname__ instead of the driver class name:\n"
            + "\n".join(orphans),
        )


def test_class_key_matches_driver_class_name_for_every_pair():
    """Round-trip: every bound config's ``class_key()`` must equal the
    snake_case form of the bound driver class's ``__name__``.  Catches a
    registry-corruption regression where the reverse lookup returns the
    wrong class.
    """
    from dynastore.models.protocols.typed_driver import _registered_pairs
    from dynastore.tools.typed_store.base import _to_snake

    pairs = _registered_pairs()
    assert pairs, "Registry is empty — driver imports didn't fire."

    mismatches: list[str] = []
    for cfg_cls, driver_cls in pairs.items():
        wire_key = cfg_cls.class_key()
        expected = _to_snake(driver_cls.__name__)
        if wire_key != expected:
            mismatches.append(
                f"  - {cfg_cls.__qualname__}: class_key()={wire_key!r} "
                f"but driver={driver_cls.__name__!r} (expected {expected!r})"
            )
    if mismatches:
        pytest.fail(
            "class_key() / driver-class mismatch:\n" + "\n".join(mismatches),
        )


def test_class_key_drops_config_suffix_convention():
    """Convention pin: every bound ``*DriverConfig`` class's wire key
    equals the snake_case form of the class name with the trailing
    ``Config`` removed.  Operators copy ONE name between
    ``routing.WRITE[].driver_id`` and ``configs.storage.drivers.{key}``.
    """
    from dynastore.models.protocols.typed_driver import _registered_pairs
    from dynastore.tools.typed_store.base import _to_snake

    violations: list[str] = []
    for cfg_cls in _registered_pairs():
        # Skip test-local fixtures (see _all_concrete_subclasses docstring).
        if "<locals>" in cfg_cls.__qualname__:
            continue
        if not cfg_cls.__name__.endswith("Config"):
            violations.append(
                f"  - {cfg_cls.__qualname__}: name doesn't end in 'Config'"
            )
            continue
        expected = _to_snake(cfg_cls.__name__[: -len("Config")])
        if cfg_cls.class_key() != expected:
            violations.append(
                f"  - {cfg_cls.__qualname__}: class_key()={cfg_cls.class_key()!r} "
                f"but expected {expected!r} (snake_case Config-suffix-stripped)"
            )
    if violations:
        pytest.fail(
            "Wire-key convention violation — driver and config class "
            "names must differ only by a trailing 'Config':\n"
            + "\n".join(violations),
        )
