"""Tests for the ``?meta=`` query mode on the composed-config endpoints.

Post #517 (2026-05-11): per-class field documentation is injected INLINE
on each in-scope plugin leaf as a ``_meta`` sibling — replacing the
retired parallel ``meta`` tree mirroring ``configs``.

  * ``?meta=none``           — no ``_meta`` key on any leaf.
  * ``?meta=field`` (default) — leaf carries ``_meta = {docs:
                                {field_name: description}}``.
  * ``?meta=schema``         — leaf carries ``_meta = {json_schema:
                                <full Pydantic schema>}``.
"""

from unittest.mock import patch

from dynastore.extensions.configs.config_api_service import ConfigApiService


def _stub_registry_with_schema(**classes):
    """Build a fake registry where each class also exposes a callable
    ``model_json_schema()`` returning a deterministic stub document.
    """
    out = {}
    for name, attrs in classes.items():
        attrs = dict(attrs)
        schema = attrs.pop("schema", {"title": name, "type": "object"})

        class _Cls:
            _address = attrs.get("_address")
            _visibility = attrs.get("_visibility")

        _Cls.__name__ = name

        @classmethod
        def _model_json_schema(cls, _s=schema):
            return _s

        _Cls.model_json_schema = _model_json_schema  # type: ignore[assignment]
        out[name] = _Cls
    return out


def test_compose_tree_meta_none_keeps_provenance():
    """Per #665 slice 3: every leaf carries ``_meta = {tier, source}``
    even under ``meta_mode="none"`` (provenance is structural).  Only
    the field-level extras (``docs`` / ``json_schema``) are suppressed.
    """
    by_class = {"web_config": {"brand_name": "X"}}
    registry = _stub_registry_with_schema(
        web_config={"_address": ("platform", "web")},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="platform", meta_mode="none",
        )
    leaf = tree["platform"]["web"]["web_config"]
    assert leaf["_meta"] == {"tier": "platform", "source": "default"}
    assert "docs" not in leaf["_meta"]
    assert "json_schema" not in leaf["_meta"]


def test_compose_tree_meta_field_attaches_docs():
    """``meta_mode="field"`` injects ``_meta = {docs: …}`` ON each
    in-scope plugin leaf (sibling of the plugin's own fields)."""
    schema = {
        "properties": {
            "brand_name": {"description": "Display name."},
            "version":    {"description": "Schema version."},
        },
    }
    by_class = {"web_config": {"brand_name": "X"}}
    registry = _stub_registry_with_schema(
        web_config={"_address": ("platform", "web"), "schema": schema},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        ConfigApiService._extract_docs.cache_clear()
        tree = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="platform", meta_mode="field",
        )
    leaf = tree["platform"]["web"]["web_config"]
    assert leaf["brand_name"] == "X"
    assert leaf["_meta"]["docs"] == {
        "brand_name": "Display name.",
        "version":    "Schema version.",
    }


def test_compose_tree_meta_schema_attaches_full_json_schema():
    """``meta_mode="schema"`` injects ``_meta = {json_schema: …}``."""
    schema = {"title": "WebConfig", "type": "object", "properties": {}}
    by_class = {"web_config": {"brand_name": "X"}}
    registry = _stub_registry_with_schema(
        web_config={"_address": ("platform", "web"), "schema": schema},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="platform", meta_mode="schema",
        )
    leaf = tree["platform"]["web"]["web_config"]
    assert leaf["_meta"]["json_schema"] == schema
    # Field-docs leaf must not appear under schema mode.
    assert "docs" not in leaf["_meta"]


def test_compose_tree_default_mode_surfaces_full_config_tree():
    """Post-#761: the default ``include=scope`` mode at catalog/collection
    scope surfaces every leaf that ``_place()`` accepts (engines, modules,
    extensions, tasks).  ``_meta.source`` on each leaf reports where the
    effective value comes from so operators see what they inherit vs
    override.
    """
    by_class = {"web_config": {"brand_name": "X"}}
    registry = _stub_registry_with_schema(
        web_config={"_address": ("platform", "web")},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree = ConfigApiService._compose_tree(
            by_class, sources={"web_config": "platform"},
            active_scope="collection", meta_mode="field",
            include_mode="scope",
        )
    # Platform-tier null-visibility config now surfaces at collection scope
    # with source="platform" so operators see what they inherit.
    leaf = tree["platform"]["web"]["web_config"]
    assert leaf["brand_name"] == "X"
    assert leaf["_meta"]["source"] == "platform"
    assert leaf["_meta"]["tier"] == "collection"


def test_compose_tree_catalog_tier_under_upstream_mode_gets_meta():
    """Catalog-tier configs at collection scope are filtered out under
    slim mode, but render inlined under ``include=upstream`` with
    ``_meta = {tier=collection, source=catalog, docs={...}}``.
    """
    schema = {"properties": {"private": {"description": "Private mode."}}}
    by_class = {"elasticsearch_catalog_config": {"private": True}}
    registry = _stub_registry_with_schema(
        elasticsearch_catalog_config={
            "_address": ("platform", "catalog", "elasticsearch"),
            "_visibility": "catalog",
            "schema": schema,
        },
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        ConfigApiService._extract_docs.cache_clear()
        tree = ConfigApiService._compose_tree(
            by_class, sources={"elasticsearch_catalog_config": "catalog"},
            active_scope="collection", meta_mode="field",
            include_mode="upstream",
        )
    leaf = tree["platform"]["catalog"]["elasticsearch"]["elasticsearch_catalog_config"]
    assert leaf["private"] is True
    assert leaf["_meta"]["tier"] == "collection"
    assert leaf["_meta"]["source"] == "catalog"
    assert leaf["_meta"]["docs"] == {"private": "Private mode."}


def test_compose_tree_meta_tier_source_always_present_across_modes():
    """#665 slice 3 lockdown: every rendered leaf carries
    ``_meta = {tier, source}`` regardless of ``meta_mode``.  Provenance
    is structural, not opt-in.  Field-level extras (``docs`` /
    ``json_schema``) gate on the mode; ``tier`` + ``source`` do not.
    """
    schema = {"properties": {"brand_name": {"description": "Brand label."}}}
    by_class = {"web_config": {"brand_name": "X"}}
    registry = _stub_registry_with_schema(
        web_config={"_address": ("platform", "web"), "schema": schema},
    )
    for mode, must_have, must_not_have in [
        ("none",   set(),               {"docs", "json_schema"}),
        ("field",  {"docs"},            {"json_schema"}),
        ("schema", {"json_schema"},     {"docs"}),
    ]:
        with patch(
            "dynastore.extensions.configs.config_api_service.list_registered_configs",
            return_value=registry,
        ):
            ConfigApiService._extract_docs.cache_clear()
            tree = ConfigApiService._compose_tree(
                {"web_config": {"brand_name": "X"}},
                sources={"web_config": "platform"},
                active_scope="catalog",
                meta_mode=mode,
                include_mode="upstream",
            )
        leaf = tree["platform"]["web"]["web_config"]
        meta = leaf["_meta"]
        assert meta["tier"] == "catalog", f"mode={mode}: tier missing/wrong"
        assert meta["source"] == "platform", f"mode={mode}: source missing/wrong"
        for k in must_have:
            assert k in meta, f"mode={mode}: missing extra {k}"
        for k in must_not_have:
            assert k not in meta, f"mode={mode}: leaked extra {k}"


def test_compose_tree_meta_mutability_map_rendered_on_leaf():
    """#665 slice 4: ``?meta=field`` adds ``_meta.mutability =
    {field_name: kind}`` covering every marked field, alongside ``docs``.
    Field-level marker contracts (Mutable / WriteOnce / Immutable /
    Computed) flow from class definition → leaf rendering with no
    duplication.
    """
    from typing import ClassVar, Optional, Tuple
    from pydantic import Field
    from dynastore.models.mutability import Immutable, Mutable, WriteOnce
    from dynastore.modules.db_config.plugin_config import PluginConfig

    class _MutabilityFixture(PluginConfig):
        _address: ClassVar[Tuple[str, ...]] = ("platform", "_fixture")
        brand_name: Mutable[str] = Field("X", description="Brand label.")
        engine_ref: WriteOnce[Optional[str]] = Field(None, description="Engine binding.")
        physical_table: Immutable[str] = Field("fixture_tbl", description="Backing table.")

    ConfigApiService._extract_docs.cache_clear()
    ConfigApiService._extract_mutability.cache_clear()
    by_class = {"_mutability_fixture": {
        "brand_name": "X",
        "engine_ref": None,
        "physical_table": "fixture_tbl",
    }}
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value={"_mutability_fixture": _MutabilityFixture},
    ):
        tree = ConfigApiService._compose_tree(
            by_class, sources={"_mutability_fixture": "platform"},
            active_scope="platform", meta_mode="field",
        )
    leaf = tree["platform"]["_fixture"]["_mutability_fixture"]
    assert leaf["_meta"]["mutability"] == {
        "brand_name":     "mutable",
        "engine_ref":     "write_once",
        "physical_table": "immutable",
    }


def test_compose_tree_meta_schema_carries_x_mutability_on_properties():
    """#665 slice 4: ``?meta=schema`` carries ``x-mutability`` (plus
    ``readOnly`` for everything except ``Mutable``) on every property —
    via the markers' own ``__get_pydantic_json_schema__`` hook.  Schema-
    driven form-builders see the contract natively.
    """
    from typing import ClassVar, Optional, Tuple
    from pydantic import Field
    from dynastore.models.mutability import Immutable, Mutable, WriteOnce
    from dynastore.modules.db_config.plugin_config import PluginConfig

    class _SchemaMutabilityFixture(PluginConfig):
        _address: ClassVar[Tuple[str, ...]] = ("platform", "_schema_fixture")
        brand_name: Mutable[str] = Field("X", description="Brand label.")
        engine_ref: WriteOnce[Optional[str]] = Field(None, description="Engine binding.")
        physical_table: Immutable[str] = Field("fixture_tbl", description="Backing table.")

    props = _SchemaMutabilityFixture.model_json_schema()["properties"]
    assert props["brand_name"]["x-mutability"] == "mutable"
    assert props["brand_name"].get("readOnly") is not True
    assert props["engine_ref"]["x-mutability"] == "write_once"
    assert props["engine_ref"]["readOnly"] is True
    assert props["physical_table"]["x-mutability"] == "immutable"
    assert props["physical_table"]["readOnly"] is True


def test_write_once_setter_guard_rejects_post_construction_assignment():
    """#665 slice 4: ``WriteOnce[T]`` is enforced at runtime by
    ``PluginConfig.__setattr__`` — initial construction sets the value,
    subsequent assignment raises ``AttributeError``.  ``Mutable[T]``
    fields stay freely reassignable on the same instance.  The framework
    guard is intentionally stricter than the legacy
    ``enforce_config_immutability`` (which allowed ``None → value``
    transitions on a diff): once Pydantic init has placed the field in
    ``__dict__``, no further write is accepted.
    """
    from typing import ClassVar, Optional, Tuple
    import pytest
    from pydantic import Field
    from dynastore.models.mutability import Mutable, WriteOnce
    from dynastore.modules.db_config.plugin_config import PluginConfig

    class _WriteOnceFixture(PluginConfig):
        _address: ClassVar[Tuple[str, ...]] = ("platform", "_writeonce_fixture")
        engine_ref: WriteOnce[Optional[str]] = Field(None, description="Engine binding.")
        brand_name: Mutable[str] = Field("X", description="Brand label.")

    inst = _WriteOnceFixture(engine_ref="postgresql_engine", brand_name="Y")
    assert inst.engine_ref == "postgresql_engine"
    assert inst.brand_name == "Y"

    # Mutable field stays reassignable.
    inst.brand_name = "Z"
    assert inst.brand_name == "Z"

    # WriteOnce field rejects any post-construction write — including a
    # no-op assignment to the same value.
    with pytest.raises(AttributeError, match="WriteOnce"):
        inst.engine_ref = "another_engine"
    with pytest.raises(AttributeError, match="WriteOnce"):
        inst.engine_ref = "postgresql_engine"
    with pytest.raises(AttributeError, match="WriteOnce"):
        inst.engine_ref = None

    assert inst.engine_ref == "postgresql_engine"

    # Setter-guard registration: classes with WriteOnce fields carry
    # ``_write_once_fields`` (frozenset); classes without don't need it.
    assert _WriteOnceFixture._write_once_fields == frozenset({"engine_ref"})


def test_write_once_setter_guard_absent_when_no_write_once_fields():
    """A PluginConfig with only ``Mutable`` fields has no
    ``_write_once_fields`` attribute installed — the enforcer skips the
    bookkeeping when there's nothing to guard.
    """
    from typing import ClassVar, Tuple
    from pydantic import Field
    from dynastore.models.mutability import Mutable
    from dynastore.modules.db_config.plugin_config import PluginConfig

    class _AllMutableFixture(PluginConfig):
        _address: ClassVar[Tuple[str, ...]] = ("platform", "_all_mutable_fixture")
        brand_name: Mutable[str] = Field("X", description="Brand label.")

    # Class-level: no inherited fallback attribute on the bare class.
    # (Inheritance from PluginConfig may surface the parent's frozenset
    # default, so check it's at least empty when present.)
    assert getattr(_AllMutableFixture, "_write_once_fields", frozenset()) == frozenset()

    # Instance: free reassignment works without any guard tripping.
    inst = _AllMutableFixture(brand_name="A")
    inst.brand_name = "B"
    inst.brand_name = "C"
    assert inst.brand_name == "C"
