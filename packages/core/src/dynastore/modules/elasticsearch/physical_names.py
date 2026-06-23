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

"""Elasticsearch PhysicalNameResolver implementations.

Two implementations are provided:

* :class:`EsPhysicalNames` — resolves names for the public items index
  (``ITEMS``) and the assets index (``ASSETS``).  Both delegate to the
  authoritative builder functions in :mod:`dynastore.modules.elasticsearch.mappings`.

* :class:`EsPrivatePhysicalNames` — resolves names for the private items
  index (``PRIVATE_ITEMS``).  Delegates to
  :func:`~dynastore.modules.storage.drivers.elasticsearch_private.mappings.get_private_index_name`.

Both classes implement :class:`~dynastore.models.protocols.physical_names.PhysicalNameResolver`
and accept ONLY resolved ``catalog_physical_id`` values — never mutable logical
catalog ids.
"""

from __future__ import annotations

from typing import ClassVar, FrozenSet, Optional

from dynastore.models.protocols.physical_names import PhysicalNameResolver, ResourceKind


class EsPhysicalNames:
    """Maps a catalog physical id to Elasticsearch index names.

    Supported kinds: :attr:`~ResourceKind.ITEMS`, :attr:`~ResourceKind.ASSETS`.

    ITEMS
        ``{prefix}-{catalog_physical_id}-items`` — the per-catalog public
        items index managed by :class:`~dynastore.modules.storage.drivers.elasticsearch.ItemsElasticsearchDriver`.

    ASSETS
        ``{prefix}-{catalog_physical_id}-assets`` — the per-catalog assets
        index managed by :class:`~dynastore.modules.storage.drivers.elasticsearch.AssetElasticsearchDriver`.

    ``prefix`` (the platform index prefix, e.g. ``"ds"``) must be supplied
    via the ``prefix`` argument to :meth:`physical_name`.
    """

    backend: ClassVar[str] = "es"
    supported_kinds: ClassVar[FrozenSet[ResourceKind]] = frozenset(
        {ResourceKind.ITEMS, ResourceKind.ASSETS}
    )

    def physical_name(
        self,
        kind: ResourceKind,
        *,
        catalog_physical_id: str,
        collection_physical_id: Optional[str] = None,
        prefix: Optional[str] = None,
    ) -> str:
        """Return the concrete Elasticsearch resource name for ``kind``.

        Parameters
        ----------
        kind:
            ``ITEMS`` or ``ASSETS``.
        catalog_physical_id:
            Immutable physical identifier of the catalog (e.g. ``"s_2ka8fbc3"``).
            Must never be a mutable logical catalog id.
        collection_physical_id:
            Not used for these index names (both are catalog-level).
        prefix:
            Platform index prefix (e.g. ``"ds"``).  When ``None``, the active
            deployment prefix is read from
            :func:`~dynastore.modules.elasticsearch.client.get_index_prefix`.
        """
        if not catalog_physical_id:
            raise ValueError(
                "EsPhysicalNames.physical_name: catalog_physical_id must not be empty."
            )

        from dynastore.modules.elasticsearch.client import get_index_prefix
        from dynastore.modules.elasticsearch.mappings import (
            get_assets_index_name,
            get_tenant_items_index,
        )

        resolved_prefix = prefix if prefix is not None else get_index_prefix()

        if kind == ResourceKind.ITEMS:
            return get_tenant_items_index(resolved_prefix, catalog_physical_id)

        if kind == ResourceKind.ASSETS:
            return get_assets_index_name(resolved_prefix, catalog_physical_id)

        raise ValueError(
            f"EsPhysicalNames does not support ResourceKind.{kind.name}. "
            f"Supported: {sorted(k.name for k in self.supported_kinds)}"
        )


# Verify structural conformance at import time (runtime_checkable protocol).
assert isinstance(EsPhysicalNames(), PhysicalNameResolver), (
    "EsPhysicalNames must satisfy the PhysicalNameResolver protocol"
)


class EsPrivatePhysicalNames:
    """Maps a catalog physical id to the Elasticsearch private items index name.

    Supported kinds: :attr:`~ResourceKind.PRIVATE_ITEMS`.

    PRIVATE_ITEMS
        ``{prefix}-{catalog_physical_id}-private-items`` — the per-catalog
        private items index managed by
        :class:`~dynastore.modules.storage.drivers.elasticsearch_private.driver.ItemsElasticsearchPrivateDriver`.

    ``prefix`` (the platform index prefix) must be supplied via the ``prefix``
    argument to :meth:`physical_name`, or is read from the active deployment
    when ``None``.
    """

    backend: ClassVar[str] = "es_private"
    supported_kinds: ClassVar[FrozenSet[ResourceKind]] = frozenset(
        {ResourceKind.PRIVATE_ITEMS}
    )

    def physical_name(
        self,
        kind: ResourceKind,
        *,
        catalog_physical_id: str,
        collection_physical_id: Optional[str] = None,
        prefix: Optional[str] = None,
    ) -> str:
        """Return the concrete private Elasticsearch index name for ``kind``.

        Parameters
        ----------
        kind:
            ``PRIVATE_ITEMS``.
        catalog_physical_id:
            Immutable physical identifier of the catalog (e.g. ``"s_2ka8fbc3"``).
            Must never be a mutable logical catalog id.
        collection_physical_id:
            Not used for the private items index (catalog-level resource).
        prefix:
            Platform index prefix.  When ``None``, read from
            :func:`~dynastore.modules.elasticsearch.client.get_index_prefix`.
        """
        if not catalog_physical_id:
            raise ValueError(
                "EsPrivatePhysicalNames.physical_name: catalog_physical_id must not be empty."
            )

        if kind == ResourceKind.PRIVATE_ITEMS:
            from dynastore.modules.elasticsearch.client import get_index_prefix
            from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
                get_private_index_name,
            )

            resolved_prefix = prefix if prefix is not None else get_index_prefix()
            return get_private_index_name(resolved_prefix, catalog_physical_id)

        raise ValueError(
            f"EsPrivatePhysicalNames does not support ResourceKind.{kind.name}. "
            f"Supported: {sorted(k.name for k in self.supported_kinds)}"
        )


# Verify structural conformance at import time.
assert isinstance(EsPrivatePhysicalNames(), PhysicalNameResolver), (
    "EsPrivatePhysicalNames must satisfy the PhysicalNameResolver protocol"
)
