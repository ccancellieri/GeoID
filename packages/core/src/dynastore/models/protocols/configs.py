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

"""
Configuration management protocol definitions.

Class-as-identity is the canonical lookup ((class_key)).  Cycle F.4c.2
introduces the parallel ref-key-as-identity API (``get_config_by_ref`` +
``list_refs_at_scope``) so callers can read by operator-chosen instance
name when multiple driver/engine instances of the same class are stored
side-by-side.  For single-instance configs ``ref_key == class_key`` and
both APIs return the same row.
"""

from typing import Protocol, Optional, Any, Dict, List, Tuple, Type, TypeVar, runtime_checkable, TYPE_CHECKING

if TYPE_CHECKING:
    from dynastore.models.plugin_config import PluginConfig
    from dynastore.models.driver_context import DriverContext

_T_Config = TypeVar("_T_Config", bound="PluginConfig")


@runtime_checkable
class ConfigsProtocol(Protocol):
    """
    Protocol for configuration management operations (class-as-identity only).

    ``get_config(SomeConfigClass, ...)`` — every PluginConfig subclass is its
    own identity.  Return type narrows to that class.  No string-form legacy
    id is accepted.
    """

    async def get_config(
        self,
        config_cls: Type[_T_Config],
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional["DriverContext"] = None,
        config_snapshot: Optional[Dict[str, Any]] = None,
    ) -> _T_Config:
        """
        Retrieves configuration with 4-tier waterfall:
        1. Collection (if provided)
        2. Catalog (if provided)
        3. Platform (global)
        4. Code-level Defaults
        """
        ...

    async def get_configs_batch(
        self,
        config_cls: Type[_T_Config],
        catalog_id: str,
        collection_ids: List[str],
        ctx: Optional["DriverContext"] = None,
    ) -> Dict[str, _T_Config]:
        """Resolve ``config_cls`` for every id in ``collection_ids`` in a bounded
        number of round trips instead of one ``get_config`` call per id.

        Same base → catalog → collection waterfall as :meth:`get_config`, but
        the catalog-tier work runs once and the collection-tier deltas are
        fetched in a single batched query. Built for callers that must
        resolve the same config class across many/most collections of a
        catalog in one request (e.g. a PG-fallback search over an
        auto-expanded collection scope) — see ``ConfigService.get_configs_batch``.
        """
        ...

    async def get_persisted_config(
        self,
        config_cls: Type["PluginConfig"],
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Tier-local read — returns the raw row data at the scope implied by
        (catalog_id, collection_id), or None when absent. Never walks the
        waterfall; never fills class defaults. Callers that need a
        fully-resolved view should use ``get_config``.
        """
        ...

    async def list_collection_config_deltas(
        self,
        config_cls: Type["PluginConfig"],
        catalog_id: str,
        ctx: Optional["DriverContext"] = None,
    ) -> List[Dict[str, Any]]:
        """Tier-local read of every collection-level delta row stored for
        ``config_cls`` in ``catalog_id``, in one query (#3160).

        Returns the raw delta payloads (partial dicts, exactly as persisted)
        for the collections that explicitly stored a row for this class —
        collections without a row inherit the catalog-scope resolution and
        therefore do not appear here. Never walks the waterfall; never fills
        class defaults. Empty list when the catalog has no such rows (or no
        collection-configs table yet).
        """
        ...

    async def get_config_versioned(
        self,
        config_cls: Type[_T_Config],
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional["DriverContext"] = None,
    ) -> "Tuple[_T_Config, Optional[str]]":
        """Versioned read pairing a resolved config with its CAS token (#2707).

        ``config`` is the same fully-resolved waterfall value ``get_config``
        would return. ``version`` is the opaque token for the row at
        exactly the tier ``(catalog_id, collection_id)`` implies — the one
        a paired ``set_config(..., expected_version=version)`` call asserts
        against — or ``None`` when no row is persisted at that tier yet
        (nothing to CAS against; write unconditionally instead).

        Always reads storage directly, never through the config cache — a
        stale cached read must never mask (or falsely trigger) a CAS
        conflict.
        """
        ...

    async def set_config(
        self,
        config_cls: Type["PluginConfig"],
        config: "PluginConfig",
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        check_immutability: bool = True,
        ctx: Optional["DriverContext"] = None,
        expected_version: Optional[str] = None,
    ) -> "PluginConfig":
        """
        Sets configuration at the appropriate level based on provided parameters:
        - If collection_id is provided: sets at collection level
        - If only catalog_id is provided: sets at catalog level
        - If neither is provided: sets at platform level

        Returns the persisted config — the post-apply object, mutated in
        place by the validate phase (``_self_register_*`` augmentation).
        The configs API route returns this body; #738/#747 — a successful
        PUT now returns the effective config, not ``200 + null``.

        ``expected_version`` (#2707): ``None`` (default) writes
        unconditionally, matching prior behavior. A token obtained from
        :meth:`get_config_versioned` turns this into an atomic
        compare-and-set against the row at the tier implied by
        ``(catalog_id, collection_id)`` — raises
        ``dynastore.modules.db_config.exceptions.ConfigVersionConflictError``
        when a concurrent writer already moved the row past that version
        (or removed it). Closes the read-modify-write race described in
        #2689: an actuator that reads a config, mutates one field, and
        writes it back can now assert the write is still based on the
        value it read, instead of silently reverting a concurrent edit to
        a different field.
        """
        ...

    async def list_configs(
        self,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        ctx: Optional["DriverContext"] = None,
    ) -> Dict[str, Any]:
        """
        Lists all configurations at the specified level with pagination.
        Returns ``{"total": int, "results": list[{"class_key": str, "config": dict}]}``.
        """
        ...

    async def list_catalog_configs(
        self,
        catalog_id: str,
        ctx: Optional["DriverContext"] = None,
    ) -> Dict[str, Any]:
        """
        Returns all configurations for a catalog as a dict keyed by class_key.
        """
        ...

    async def snapshot_catalog_defaults(
        self,
        catalog_id: str,
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        """Freeze the catalog's inherited config defaults at creation (#1079 c).

        Captures the resolved platform/code defaults for stable value-configs
        into a schema-id-tagged blob used as the inheritance base, so later
        default changes do not silently re-resolve into existing collections.
        """
        ...

    async def get_catalog_defaults_snapshot(
        self,
        catalog_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Return the catalog's frozen defaults-snapshot blob (#1079 c), or ``None``.

        Raw ``{class_key: {"schema_id": ..., "data": ...}}`` blob captured by
        :meth:`snapshot_catalog_defaults` at catalog creation. Callers gate
        the per-class entry through
        ``dynastore.modules.catalog.config_snapshot.select_snapshot_base``
        (or its ``resolve_catalog_snapshot_base`` wrapper) — this is the
        shared accessor both ``get_config``'s runtime resolution and the
        composed-config view use so the two never disagree (#2830).
        """
        ...

    async def delete_config(
        self,
        config_cls: Type["PluginConfig"],
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        """
        Deletes configuration at the specified level:
        - If collection_id is provided: deletes collection-level config
        - If only catalog_id is provided: deletes catalog-level config
        - If neither is provided: deletes platform-level config (acts as reset to defaults)
        """
        ...

    async def search(
        self,
        query: Optional[str] = None,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        ctx: Optional["DriverContext"] = None,
    ) -> Dict[str, Any]:
        """
        Searches for configurations across the hierarchy.
        ``query`` is matched against class_key (ILIKE).
        """
        ...

    # -----------------------------------------------------------------
    # F.4c.2 — ref-keyed read API
    # -----------------------------------------------------------------

    async def list_refs_at_scope(
        self,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional["DriverContext"] = None,
    ) -> Dict[str, str]:
        """Return ``{ref_key: class_key}`` for every stored row at the implied scope.

        Tier-local: does NOT walk the waterfall.  Empty dict when nothing is
        stored at the scope.  For single-instance configs ``ref_key`` equals
        the row's ``class_key``; multi-instance writes via
        :meth:`set_config_by_ref` produce rows whose ref_key diverges from
        the class_key (the dispatch class) — both surface here.
        """
        ...

    async def get_config_by_ref(
        self,
        ref_key: str,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional["DriverContext"] = None,
    ) -> Optional["PluginConfig"]:
        """Return the stored ``PluginConfig`` for ``(ref_key, scope)``, else ``None``.

        Resolves the dispatch class from the row's ``class_key`` discriminator
        and validates the JSON payload through that class.  Tier-local: does
        NOT walk the waterfall — refs are explicit.  Returns ``None`` when no
        row exists for ``ref_key`` at the implied scope, OR when the row's
        ``class_key`` cannot be resolved against the live registry (warning
        logged at the call site).
        """
        ...

    # -----------------------------------------------------------------
    # F.4c.4 — ref-keyed write API
    # -----------------------------------------------------------------

    async def set_config_by_ref(
        self,
        ref_key: str,
        config: "PluginConfig",
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        check_immutability: bool = True,
        ctx: Optional["DriverContext"] = None,
    ) -> "PluginConfig":
        """Store ``config`` at ``(ref_key, scope)`` and return it.

        ``config`` carries its own dispatch class — implementations persist
        the row with ``class_key`` derived from ``type(config).class_key()``
        as the discriminator and ``ref_key`` as the primary key.  When
        ``ref_key == class_key`` the row is observable via the class-keyed
        ``set_config`` (single-instance compatibility); when they diverge
        the row is only reachable via :meth:`get_config_by_ref` /
        :meth:`list_refs_at_scope`.

        Immutability is enforced on the row at the same ref_key (an
        operator can't repurpose a ref to a different class) when
        ``check_immutability=True``.

        Returns the post-apply config (mutated in place by the validate
        phase).
        """
        ...

    async def delete_config_by_ref(
        self,
        ref_key: str,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        """Delete the stored row at ``(ref_key, scope)`` and return whether
        a row was actually removed (so callers can distinguish a no-op from
        a successful delete without an extra read).

        Symmetrical with :meth:`get_config_by_ref` — deletes are
        tier-local; the waterfall is not consulted.
        """
        ...
