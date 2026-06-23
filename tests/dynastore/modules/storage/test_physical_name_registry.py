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

"""Unit tests for the central PhysicalNameResolver registry.

Verifies:
* All five built-in resolvers (pg, gcs, pubsub, es, es_private) are registered
  after module import.
* ``get_physical_name_resolver`` returns the correct resolver per backend.
* ``physical_name`` dispatches to the correct resolver and produces correct names.
* Unknown backend raises KeyError with a helpful message.
* Unsupported (backend, kind) pair raises ValueError.
* ``register_physical_name_resolver`` rejects non-conformant objects.
* Double-registration raises ValueError; unregister clears the slot.
"""

from __future__ import annotations

from typing import ClassVar, FrozenSet, Optional

import pytest

from dynastore.models.protocols.physical_names import PhysicalNameResolver, ResourceKind

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CATALOG_ID = "s_2ka8fbc3"
COLLECTION_ID = "t_9xz12345"
PREFIX = "ds"


class _MinimalResolver:
    """A minimal resolver used to test registration mechanics."""

    backend: ClassVar[str] = "_test_backend_x_"
    supported_kinds: ClassVar[FrozenSet[ResourceKind]] = frozenset({ResourceKind.SCHEMA})

    def physical_name(
        self,
        kind: ResourceKind,
        *,
        catalog_physical_id: str,
        collection_physical_id: Optional[str] = None,
        prefix: Optional[str] = None,
    ) -> str:
        return f"test-{catalog_physical_id}"


@pytest.fixture()
def clean_registry():
    """Snapshot and restore registry state around tests that mutate it."""
    import dynastore.modules.storage.physical_name_registry as reg

    snapshot = dict(reg._registry)
    yield reg
    with reg._lock:
        reg._registry.clear()
        reg._registry.update(snapshot)


# ---------------------------------------------------------------------------
# Built-in resolver registration
# ---------------------------------------------------------------------------


class TestBuiltinResolvers:
    """All five built-in resolvers are present after module import."""

    def test_pg_resolver_registered(self):
        from dynastore.modules.storage.physical_name_registry import get_physical_name_resolver
        resolver = get_physical_name_resolver("pg")
        assert resolver is not None
        assert type(resolver).__name__ == "PgPhysicalNames"

    def test_gcs_resolver_registered(self):
        from dynastore.modules.storage.physical_name_registry import get_physical_name_resolver
        resolver = get_physical_name_resolver("gcs")
        assert type(resolver).__name__ == "GcsPhysicalNames"

    def test_pubsub_resolver_registered(self):
        from dynastore.modules.storage.physical_name_registry import get_physical_name_resolver
        resolver = get_physical_name_resolver("pubsub")
        assert type(resolver).__name__ == "PubSubPhysicalNames"

    def test_es_resolver_registered(self):
        from dynastore.modules.storage.physical_name_registry import get_physical_name_resolver
        resolver = get_physical_name_resolver("es")
        assert type(resolver).__name__ == "EsPhysicalNames"

    def test_es_private_resolver_registered(self):
        from dynastore.modules.storage.physical_name_registry import get_physical_name_resolver
        resolver = get_physical_name_resolver("es_private")
        assert type(resolver).__name__ == "EsPrivatePhysicalNames"

    def test_all_five_backends_present(self):
        import dynastore.modules.storage.physical_name_registry as reg
        registered = set(reg._registry)
        assert {"pg", "gcs", "pubsub", "es", "es_private"}.issubset(registered)


# ---------------------------------------------------------------------------
# physical_name dispatcher
# ---------------------------------------------------------------------------


class TestPhysicalNameDispatcher:
    def test_pg_schema_returns_catalog_physical_id(self):
        from dynastore.modules.storage.physical_name_registry import physical_name
        result = physical_name(
            "pg", ResourceKind.SCHEMA, catalog_physical_id=CATALOG_ID
        )
        assert result == CATALOG_ID

    def test_pg_items_returns_collection_physical_id(self):
        from dynastore.modules.storage.physical_name_registry import physical_name
        result = physical_name(
            "pg",
            ResourceKind.ITEMS,
            catalog_physical_id=CATALOG_ID,
            collection_physical_id=COLLECTION_ID,
        )
        assert result == COLLECTION_ID

    def test_es_items_index_shape(self):
        from dynastore.modules.storage.physical_name_registry import physical_name
        result = physical_name(
            "es",
            ResourceKind.ITEMS,
            catalog_physical_id=CATALOG_ID,
            prefix=PREFIX,
        )
        assert result == f"{PREFIX}-{CATALOG_ID}-items"

    def test_es_assets_index_shape(self):
        from dynastore.modules.storage.physical_name_registry import physical_name
        result = physical_name(
            "es",
            ResourceKind.ASSETS,
            catalog_physical_id=CATALOG_ID,
            prefix=PREFIX,
        )
        assert result == f"{PREFIX}-{CATALOG_ID}-assets"

    def test_es_private_index_shape(self):
        from dynastore.modules.storage.physical_name_registry import physical_name
        result = physical_name(
            "es_private",
            ResourceKind.PRIVATE_ITEMS,
            catalog_physical_id=CATALOG_ID,
            prefix=PREFIX,
        )
        assert result == f"{PREFIX}-{CATALOG_ID}-private-items"

    def test_pubsub_topic_shape(self):
        from dynastore.modules.storage.physical_name_registry import physical_name
        result = physical_name(
            "pubsub",
            ResourceKind.TOPIC,
            catalog_physical_id=CATALOG_ID,
        )
        assert result == f"ds-{CATALOG_ID}-events"

    def test_pubsub_subscription_shape(self):
        from dynastore.modules.storage.physical_name_registry import physical_name
        result = physical_name(
            "pubsub",
            ResourceKind.SUBSCRIPTION,
            catalog_physical_id=CATALOG_ID,
        )
        assert result == f"ds-{CATALOG_ID}-default-sub"


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestRegistryErrors:
    def test_unknown_backend_raises_key_error(self):
        from dynastore.modules.storage.physical_name_registry import get_physical_name_resolver
        with pytest.raises(KeyError, match="no_such_backend"):
            get_physical_name_resolver("no_such_backend")

    def test_unknown_backend_key_error_lists_registered(self):
        from dynastore.modules.storage.physical_name_registry import get_physical_name_resolver
        with pytest.raises(KeyError) as exc_info:
            get_physical_name_resolver("no_such_backend")
        assert "pg" in str(exc_info.value)

    def test_physical_name_unknown_backend_raises_key_error(self):
        from dynastore.modules.storage.physical_name_registry import physical_name
        with pytest.raises(KeyError, match="no_such_backend"):
            physical_name(
                "no_such_backend",
                ResourceKind.SCHEMA,
                catalog_physical_id=CATALOG_ID,
            )

    def test_unsupported_kind_raises_value_error(self):
        """pg backend does not support ASSETS."""
        from dynastore.modules.storage.physical_name_registry import physical_name
        with pytest.raises(ValueError, match="ASSETS"):
            physical_name(
                "pg",
                ResourceKind.ASSETS,
                catalog_physical_id=CATALOG_ID,
            )

    def test_unsupported_kind_message_names_backend(self):
        from dynastore.modules.storage.physical_name_registry import physical_name
        with pytest.raises(ValueError, match="pg"):
            physical_name(
                "pg",
                ResourceKind.BUCKET,
                catalog_physical_id=CATALOG_ID,
            )

    def test_es_does_not_support_schema(self):
        from dynastore.modules.storage.physical_name_registry import physical_name
        with pytest.raises(ValueError, match="SCHEMA"):
            physical_name(
                "es",
                ResourceKind.SCHEMA,
                catalog_physical_id=CATALOG_ID,
                prefix=PREFIX,
            )

    def test_pubsub_does_not_support_items(self):
        from dynastore.modules.storage.physical_name_registry import physical_name
        with pytest.raises(ValueError):
            physical_name(
                "pubsub",
                ResourceKind.ITEMS,
                catalog_physical_id=CATALOG_ID,
            )


# ---------------------------------------------------------------------------
# Registration mechanics
# ---------------------------------------------------------------------------


class TestRegistrationMechanics:
    def test_register_non_conformant_raises_type_error(self, clean_registry):
        reg = clean_registry

        class _BadResolver:
            backend: ClassVar[str] = "_test_bad_"
            # missing supported_kinds and physical_name

        with pytest.raises(TypeError, match="PhysicalNameResolver"):
            reg.register_physical_name_resolver(_BadResolver())  # type: ignore[arg-type]

    def test_double_registration_raises_value_error(self, clean_registry):
        reg = clean_registry
        resolver = _MinimalResolver()
        reg.register_physical_name_resolver(resolver)
        with pytest.raises(ValueError, match="_test_backend_x_"):
            reg.register_physical_name_resolver(_MinimalResolver())

    def test_unregister_clears_slot(self, clean_registry):
        reg = clean_registry
        resolver = _MinimalResolver()
        reg.register_physical_name_resolver(resolver)
        reg.unregister_physical_name_resolver("_test_backend_x_")
        with pytest.raises(KeyError):
            reg.get_physical_name_resolver("_test_backend_x_")

    def test_unregister_unknown_backend_is_noop(self, clean_registry):
        reg = clean_registry
        # Should not raise
        reg.unregister_physical_name_resolver("_never_registered_")

    def test_register_then_lookup_succeeds(self, clean_registry):
        reg = clean_registry
        resolver = _MinimalResolver()
        reg.register_physical_name_resolver(resolver)
        found = reg.get_physical_name_resolver("_test_backend_x_")
        assert found is resolver

    def test_registered_resolver_dispatches_via_physical_name(self, clean_registry):
        reg = clean_registry
        resolver = _MinimalResolver()
        reg.register_physical_name_resolver(resolver)
        result = reg.physical_name(
            "_test_backend_x_",
            ResourceKind.SCHEMA,
            catalog_physical_id=CATALOG_ID,
        )
        assert result == f"test-{CATALOG_ID}"
