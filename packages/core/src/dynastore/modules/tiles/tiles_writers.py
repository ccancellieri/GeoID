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

"""Tile-cache writer selection.

``TilesCachingConfig.writers`` is an ordered list of typed
``TileWriterConfig`` objects (one ``PersistentModel`` subclass per
implementation — ``PgTileWriterConfig`` here, ``GcsTileWriterConfig`` in the
``gcp`` module, a local-disk equivalent in ``modules/local``), the same
polymorphic-envelope idiom every other typed-store config in this codebase
uses: ``class_key()`` (auto snake_case of the class name) is both the
``TypedModelRegistry`` lookup key and, serialized as ``writer_key``, the
discriminator that resolves a JSON list entry back to its concrete class.

Unlike the ingestion-reporters idiom this mirrors structurally, tile-cache
writers are not fanned out to — at most ONE is ever active. Each
implementation module registers a ``(config_class, factory)`` pair via
:func:`register_tile_writer_factory` at its own import time (the ``gcp``
module never needs to be imported from here — resolving a writer's config
class through ``TypedModelRegistry`` by its string ``class_key`` is exactly
the existing decoupling mechanism this codebase uses for cross-module typed
lookups, e.g. driver configs). :func:`select_tile_writer` walks the
ordered list and returns the first writer whose factory reports it usable —
this is what replaces the old, never-actually-wired
``TilesPreseedConfig.storage_priority`` list, and it fixes the field bug
where a bucket-first config on a ``gcp``-less job image silently cached
nothing: the unavailable candidate is now named in an INFO log line, and the
next candidate (typically PG) wins instead.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, FrozenSet, List, Optional, Tuple, Type, TypeVar, cast

from pydantic import computed_field

from dynastore.tools.typed_store.base import PersistentModel
from dynastore.tools.typed_store.registry import TypedModelRegistry
from dynastore.modules.storage.hints import Hint

if TYPE_CHECKING:
    from dynastore.modules.tiles.tiles_config import TilesCachingConfig
    from dynastore.modules.tiles.tiles_module import TileStorageProtocol

logger = logging.getLogger(__name__)

# class_key() of the gcp module's GcsTileWriterConfig — a plain string, not
# an import, so core never depends on the gcp module. Kept as one named
# constant (rather than a literal at each call site) so a rename of that
# class is a one-line fix here plus the class itself.
_GCS_WRITER_KEY = "gcs_tile_writer_config"


class TileWriterConfig(PersistentModel):
    """Base config every tile-writer config model extends.

    ``writer_key`` (a computed field mirroring ``class_key()``) is emitted on
    serialization and read back on load so a heterogeneous ``writers`` list
    round-trips through JSON to the right concrete subclass — see
    :func:`resolve_writer_config_entry`.
    """

    enabled: bool = True

    @computed_field  # type: ignore[prop-decorator]
    @property
    def writer_key(self) -> str:
        return self.class_key()


T_WRITER_CONFIG = TypeVar("T_WRITER_CONFIG", bound=TileWriterConfig)


def resolve_writer_config_entry(raw: Any) -> TileWriterConfig:
    """Resolve one ``writers`` list entry to its concrete typed instance.

    Accepts an already-constructed ``TileWriterConfig`` (the common case —
    ``resolve_effective_writers`` returns typed instances directly) or a raw
    dict (the wire/JSON case), read back via its ``writer_key``/``class_key``
    discriminator. Unknown or invalid entries fail loudly, naming every
    registered writer.
    """
    if isinstance(raw, TileWriterConfig):
        return raw
    if not isinstance(raw, dict):
        raise TypeError(f"Invalid tile writer config entry: {raw!r}")
    data = dict(raw)
    key = data.pop("writer_key", None) or data.pop("class_key", None)
    if not key:
        raise ValueError(
            "Tile writer config entry is missing its 'writer_key' "
            "discriminator (e.g. 'gcs_tile_writer_config', 'pg_tile_writer_config')."
        )
    cls = TypedModelRegistry.get(key)
    if cls is None or not issubclass(cls, TileWriterConfig):
        available = ", ".join(sorted(_factory_registry.keys())) or "<none>"
        raise ValueError(f"Unknown tile writer '{key}'; available: [{available}]")
    return cls.model_validate(data)


# --- Writer factories -------------------------------------------------------
#
# config_class.class_key() -> factory(config, caching_cfg, catalog_id, ensure)
# -> a TileStorageProtocol instance, or None when this writer cannot
# currently serve (e.g. no matching StorageProtocol registered, or no
# managed bucket provisioned yet on a read). ``ensure`` mirrors the
# save-vs-read distinction on StorageProtocol.ensure_storage_for_catalog /
# get_storage_identifier: True only for the write path, so a read never
# side-effects a bucket into existence.
#
# The registry itself is necessarily loosely typed (each entry's factory
# accepts a DIFFERENT config subclass) — register_tile_writer_factory below
# is the generic, precisely-typed entry point every call site actually uses;
# correctness of the config_cls <-> factory pairing is enforced there per
# call, and again at runtime by class_key()-keyed dispatch in
# select_tile_writer.
AnyWriterFactory = Callable[[Any, "TilesCachingConfig", str, bool], Awaitable[Optional["TileStorageProtocol"]]]
_factory_registry: Dict[str, Tuple[AnyWriterFactory, FrozenSet[Hint]]] = {}


def register_tile_writer_factory(
    config_cls: Type[T_WRITER_CONFIG],
    factory: Callable[[T_WRITER_CONFIG, "TilesCachingConfig", str, bool], Awaitable[Optional["TileStorageProtocol"]]],
    *, hints: FrozenSet[Hint] = frozenset(),
) -> None:
    """Bind a writer factory (+ optional hint affinity) to its config's
    ``class_key()``.

    Each implementation module (this module for PG, the ``gcp`` module for
    GCS, ``modules/local`` for local disk) calls this once at its own import
    time; core never imports the others. ``hints`` declares which routing
    hints (``dynastore.modules.storage.hints.Hint`` — the same vocabulary
    ``get_driver`` consumes) this writer is a preferred match for; see
    :func:`select_tile_writer`.
    """
    _factory_registry[config_cls.class_key()] = (factory, hints)  # type: ignore[assignment]


async def select_tile_writer(
    writers: List[TileWriterConfig],
    caching_cfg: "TilesCachingConfig",
    catalog_id: str,
    *,
    ensure: bool,
    hints: FrozenSet[Hint] = frozenset(),
) -> Tuple[str, "TileStorageProtocol"]:
    """Select the active writer among ``writers`` (list order = read/write
    priority — this is what replaces ``storage_priority``).

    1. A candidate is AVAILABLE when: a factory is registered for it (its
       module is in this image's SCOPE), it is ``enabled``, and its factory
       resolves a usable target.
    2. With no ``hints`` (the common case): the first available candidate
       wins immediately — cheaper, no need to probe every remaining one.
    3. With ``hints``: every candidate is probed for availability first, then
       the first AVAILABLE candidate whose declared affinity intersects
       ``hints`` wins; availability always gates — a hinted-but-unavailable
       writer never wins. No match among the available set falls back to
       the first available in list order.

    Logs at INFO which writer won and why any earlier/skipped candidate
    wasn't used. Raises ``RuntimeError`` naming every candidate and its
    unavailability reason when none are available.
    """
    reasons: List[str] = []
    available: List[Tuple[str, "TileStorageProtocol", FrozenSet[Hint]]] = []

    for config in writers:
        key = config.class_key()
        if not config.enabled:
            reasons.append(f"{key}: disabled")
            continue
        entry = _factory_registry.get(key)
        if entry is None:
            reasons.append(f"{key}: no writer factory registered for this image's SCOPE")
            continue
        factory, affinity = entry
        try:
            instance = await factory(config, caching_cfg, catalog_id, ensure)
        except Exception as exc:
            reasons.append(f"{key}: {exc}")
            continue
        if instance is None:
            reasons.append(f"{key}: target did not resolve")
            continue

        available.append((key, instance, affinity))
        if not hints:
            if reasons:
                logger.info(
                    "tile writer selection: '%s' active for catalog=%r; skipped: %s",
                    key, catalog_id, "; ".join(reasons),
                )
            else:
                logger.debug("tile writer selection: '%s' active for catalog=%r.", key, catalog_id)
            return key, instance

    if not available:
        detail = "; ".join(reasons) if reasons else "no writers configured"
        raise RuntimeError(f"No tile writer is available for catalog {catalog_id!r} ({detail}).")

    for key, instance, affinity in available:
        if affinity & hints:
            logger.info(
                "tile writer selection: '%s' active for catalog=%r (matched hints %s).",
                key, catalog_id, sorted(h.value for h in (affinity & hints)),
            )
            return key, instance

    key, instance, _affinity = available[0]
    logger.info(
        "tile writer selection: no available writer matched hints %s for catalog=%r; "
        "'%s' selected (first available, list order).",
        sorted(h.value for h in hints), catalog_id, key,
    )
    return key, instance


async def resolve_effective_writers(
    cfg: "TilesCachingConfig", catalog_id: str, *, storage_priority: Optional[List[str]] = None,
) -> List[TileWriterConfig]:
    """Back-compat + default-injection resolution of ``TilesCachingConfig.writers``.

    Precedence:

    1. ``cfg.writers`` set (non-empty) -> honored verbatim.
    2. Legacy ``GcpTileCacheConfig.cache_bucket``/``cache_prefix`` set for
       this catalog (only possible when the ``gcp`` module registered its
       GCS writer config) -> ``[GcsTileWriterConfig(bucket=cache_bucket,
       prefix=cache_prefix)]`` — PR #2553 configs keep working unchanged.
    3. Legacy ``TilesPreseedConfig.storage_priority`` first entry ``== 'pg'``
       -> ``[PgTileWriterConfig()]``.
    4. Default injection, reproducing the pre-existing
       ``storage_priority=['bucket', 'pg']`` default exactly: GCS first, PG
       as a fail-safe fallback, when the ``gcp`` module's writer is
       registered AND a ``StorageProtocol`` provider is present ->
       ``[GcsTileWriterConfig(), PgTileWriterConfig()]``; otherwise ->
       ``[PgTileWriterConfig()]`` (PG always works — no external dependency).
    """
    if cfg.writers:
        return cfg.writers

    # PgTileWriterConfig is core-native (defined in this module, below) — no
    # cross-module lookup needed. GcsTileWriterConfig lives in the ``gcp``
    # module; resolved by its class_key() string so core never imports gcp
    # directly (TypedModelRegistry is the existing decoupling seam for this).
    # Its concrete type — and therefore its 'bucket'/'prefix' fields — is
    # inherently unknown to static analysis; the cast + type: ignore below
    # are the seam, not a masked bug (runtime correctness is enforced by the
    # class_key()-keyed registry lookup itself).
    pg_cls = PgTileWriterConfig
    gcs_cls = cast(Optional[Type[TileWriterConfig]], TypedModelRegistry.get(_GCS_WRITER_KEY))

    if gcs_cls is not None:
        try:
            from dynastore.models.protocols.configs import ConfigsProtocol
            from dynastore.modules.gcp.gcp_config import GcpTileCacheConfig
            from dynastore.tools.discovery import get_protocol as _get_protocol

            svc = _get_protocol(ConfigsProtocol)
            if svc is not None:
                gcfg = await svc.get_config(GcpTileCacheConfig, catalog_id=catalog_id)
                if isinstance(gcfg, GcpTileCacheConfig) and gcfg.cache_bucket:
                    return [gcs_cls(bucket=gcfg.cache_bucket, prefix=gcfg.cache_prefix)]  # type: ignore[call-arg]
        except Exception as exc:
            logger.debug(
                "tile writers: GcpTileCacheConfig read failed (%s); skipping legacy bucket mapping", exc,
            )

    if storage_priority and storage_priority[0] == "pg":
        return [pg_cls()]

    if gcs_cls is not None:
        from dynastore.models.protocols import StorageProtocol
        from dynastore.modules import get_protocol

        if get_protocol(StorageProtocol) is not None:
            return [gcs_cls(), pg_cls()]
    return [pg_cls()]


# --- Built-in writer: PG-table store -----------------------------------
#
# Lives here (not tile_blob_storage.py) because it has no StorageProtocol
# dependency at all — it is the guaranteed-available fallback (no external
# credentials/extra needed), so it is registered unconditionally by core.


class PgTileWriterConfig(TileWriterConfig):
    """PG-table tile writer — no fields beyond the base ``enabled``."""


async def _pg_writer_factory(
    config: PgTileWriterConfig, cfg: "TilesCachingConfig", catalog_id: str, ensure: bool,
) -> "TileStorageProtocol":
    from dynastore.modules.tiles.tiles_module import TilePGPreseedStorage

    return TilePGPreseedStorage()


# Hint.DURABLE: a caller that wants strong read-your-write consistency (e.g.
# immediately after a preseed run) can elevate PG over a bucket-backed
# writer via select_tile_writer(..., hints=frozenset({Hint.DURABLE})).
register_tile_writer_factory(PgTileWriterConfig, _pg_writer_factory, hints=frozenset({Hint.DURABLE}))
