"""Architectural invariants for PluginConfig subclasses.

Identity is the Pydantic class itself (via ``class_key()`` == ``__qualname__``).
Legacy string IDs, Records-domain names, and Plugin-infix names are all banned.
"""
from importlib.metadata import entry_points


def _load_all_modules() -> None:
    for ep in entry_points(group="dynastore.modules"):
        try:
            ep.load()
        except Exception:
            pass


def test_no_plugin_id_classvar():
    """No PluginConfig subclass may declare _plugin_id."""
    _load_all_modules()
    from dynastore.tools.typed_store.registry import TypedModelRegistry
    from dynastore.modules.db_config.plugin_config import PluginConfig

    offenders = [
        cls.__qualname__
        for cls in TypedModelRegistry.subclasses_of(PluginConfig)
        if "_plugin_id" in cls.__dict__
    ]
    assert not offenders, (
        "PluginConfig subclasses must not declare `_plugin_id`; identity is the class itself. "
        f"Offenders: {offenders}"
    )


def test_no_legacy_class_keys():
    """class_key must be the snake_case form auto-derived from the Python
    class name — no colon-separated IDs (legacy ``namespace:foo`` plugin
    IDs from the pre-typed-store era).

    PR #140 (snake_case identity flip) made ``class_key`` snake_case
    end-to-end, so the previous ``PascalCase __qualname__`` invariant no
    longer holds.  The ban on colon-separated IDs survives because that
    was about plugin-identity-via-string vs. plugin-identity-via-class.
    """
    _load_all_modules()
    from dynastore.tools.typed_store.registry import TypedModelRegistry
    from dynastore.modules.db_config.plugin_config import PluginConfig

    bad = [
        cls.class_key()
        for cls in TypedModelRegistry.subclasses_of(PluginConfig)
        if ":" in cls.class_key()
    ]
    assert not bad, f"class_key must not contain ':'; legacy keys found: {bad}"


def test_no_class_key_classvar_override():
    """No PluginConfig subclass may override _class_key — the class IS the identity."""
    _load_all_modules()
    from dynastore.tools.typed_store.registry import TypedModelRegistry
    from dynastore.modules.db_config.plugin_config import PluginConfig

    offenders = [
        cls.__qualname__
        for cls in TypedModelRegistry.subclasses_of(PluginConfig)
        if "_class_key" in cls.__dict__
        and not cls.__name__.startswith("_")
    ]
    assert not offenders, (
        "PluginConfig subclasses must not declare `_class_key`; "
        "class_key() defaults to __qualname__. "
        f"Offenders: {offenders}"
    )


def test_no_records_in_class_key():
    """No class_key may contain 'Records' — drivers serve features/items, not OGC Records specifically."""
    _load_all_modules()
    from dynastore.tools.typed_store.registry import TypedModelRegistry
    from dynastore.modules.db_config.plugin_config import PluginConfig

    # Extension exposure configs are legitimately named after the extension they toggle.
    _EXEMPT = {"RecordsPluginConfig"}

    bad = [
        cls.class_key()
        for cls in TypedModelRegistry.subclasses_of(PluginConfig)
        if "Records" in cls.class_key()
        and cls.__name__ not in _EXEMPT
    ]
    assert not bad, (
        "class_key must not contain 'Records'; use Collection/Asset/Metadata role prefix. "
        f"Offenders: {bad}"
    )


def test_no_plugin_infix_in_config_class_key():
    """Storage/tiles config classes must not have 'Plugin' infix.

    The classes renamed in M1 (RoutingPluginConfig → ItemsRoutingConfig,
    FeatureTypePluginConfig → ItemsSchema, TilesPluginConfig → TilesConfig,
    AssetRoutingPluginConfig → AssetRoutingConfig) must not regress.

    Other modules (catalog, tasks) may have PluginConfig subclasses with "Plugin"
    in their name that have not been renamed yet — they are in the explicit
    allow-list below until their own rename milestone lands.
    """
    _load_all_modules()
    from dynastore.tools.typed_store.registry import TypedModelRegistry
    from dynastore.modules.db_config.plugin_config import PluginConfig

    # Base classes and modules not yet renamed — excluded from this guard.
    _ALLOWED = {
        "PluginConfig",           # the base class itself
        "DriverPluginConfig",     # storage driver base
        "CollectionPluginConfig", # catalog module — not yet renamed
        "TasksPluginConfig",      # tasks module — not yet renamed
        "FeaturesPluginConfig",   # features module — not yet renamed
        # Extension service-exposure configs — named after the extension ID they toggle.
        # These are candidates for a future rename milestone (e.g. ProcessesConfig).
        "ProcessesPluginConfig",
        "SearchPluginConfig",
        "StacPluginConfig",
        "MapsPluginConfig",
        "LogsPluginConfig",
        "WFSPluginConfig",
        "RecordsPluginConfig",
        "DimensionsPluginConfig",
        "DwhPluginConfig",
        "NotebooksPluginConfig",
        "CrsPluginConfig",
        "GdalPluginConfig",
        "AssetsPluginConfig",
        "StylesPluginConfig",
        "SecurityPluginConfig",
        "JoinsPluginConfig",      # joins extension — exposure config, candidate for rename
        "IngestionPluginConfig",  # tasks/ingestion — candidate for rename
        "MovingFeaturesPluginConfig",  # OGC API Moving Features — candidate for rename
    }

    bad = [
        cls.class_key()
        for cls in TypedModelRegistry.subclasses_of(PluginConfig)
        if "Plugin" in cls.class_key()
        and cls.__name__ not in _ALLOWED
        and not cls.__name__.startswith("_")
    ]
    assert not bad, (
        "Config classes outside the allow-list must not contain 'Plugin' infix. "
        "Update the allow-list or rename the class. "
        f"Offenders: {bad}"
    )


def test_driver_config_naming_convention():
    """Concrete driver config classes must follow '<Tier><Backend>DriverConfig' pattern.

    Rules:
    - Must end in 'DriverConfig'
    - Must start with a known tier prefix: Items, Collection, Catalog, Asset, or Metadata.
      Post-rename:
        * ``Items<Backend>DriverConfig`` — item-level drivers (the former
          ``Collection<Backend>DriverConfig``); the ``Collection`` prefix
          remains reserved for the collection-metadata tier created in M2.1+.
        * ``Catalog<Backend>DriverConfig`` — catalog-metadata tier (M2.1).
        * ``Asset<Backend>DriverConfig`` — asset-level drivers.
        * ``Metadata<Backend>DriverConfig`` — legacy whole-envelope metadata
          drivers (deleted in M2.5 once the split-domain drivers ship).
    - Base classes (DriverPluginConfig, CollectionDriverConfig, AssetDriverConfig) are excluded
    """
    _load_all_modules()
    from dynastore.tools.typed_store.registry import TypedModelRegistry
    from dynastore.modules.db_config.plugin_config import PluginConfig
    from dynastore.modules.storage.driver_config import (
        DriverPluginConfig,
        CollectionDriverConfig,
        AssetDriverConfig,
    )

    _BASE_CLASSES = {DriverPluginConfig, CollectionDriverConfig, AssetDriverConfig}
    _ROLE_PREFIXES = ("Items", "Collection", "Catalog", "Asset", "Metadata")

    concrete_driver_configs = [
        cls for cls in TypedModelRegistry.subclasses_of(PluginConfig)
        if issubclass(cls, DriverPluginConfig) and cls not in _BASE_CLASSES
    ]

    bad_suffix = [
        cls.__name__ for cls in concrete_driver_configs
        if not cls.__name__.endswith("DriverConfig")
    ]
    assert not bad_suffix, (
        "Driver config classes must end in 'DriverConfig'. "
        f"Offenders: {bad_suffix}"
    )

    bad_prefix = [
        cls.__name__ for cls in concrete_driver_configs
        if not any(cls.__name__.startswith(p) for p in _ROLE_PREFIXES)
    ]
    assert not bad_prefix, (
        f"Driver config classes must start with one of {_ROLE_PREFIXES}. "
        f"Offenders: {bad_prefix}"
    )
