import importlib.metadata
import inspect
from typing import (
    Dict,
    Type,
    TypeVar,
    Any,
    List,
    cast,
    Optional,
)
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

T = TypeVar("T")

# ---------------------------------------------------------------------------
# Global plugin registry
# ---------------------------------------------------------------------------
_DYNASTORE_PLUGINS: List[Any] = []

def register_plugin(instance: Any) -> None:
    if instance not in _DYNASTORE_PLUGINS:
        _DYNASTORE_PLUGINS.append(instance)
        _get_protocol_cached.cache_clear()
        _get_protocols_cached.cache_clear()
        # Keep DriverRegistry in sync so it rebuilds on next access.
        try:
            from dynastore.modules.storage.driver_registry import DriverRegistry
            DriverRegistry.clear()
        except ImportError:
            pass

def unregister_plugin(instance: Any) -> None:
    if instance in _DYNASTORE_PLUGINS:
        _DYNASTORE_PLUGINS.remove(instance)
        _get_protocol_cached.cache_clear()
        _get_protocols_cached.cache_clear()
        try:
            from dynastore.modules.storage.driver_registry import DriverRegistry
            DriverRegistry.clear()
        except ImportError:
            pass

@lru_cache(maxsize=128)
def _get_protocol_cached(protocol: type) -> Optional[Any]:
    instances = _get_protocols_cached(protocol)
    return instances[0] if instances else None

@lru_cache(maxsize=128)
def _get_protocols_cached(protocol: type) -> List[Any]:
    discovered: List[Any] = []
    seen: set = set()

    def _accept(obj: Any) -> bool:
        obj_id = id(obj)
        if obj_id in seen or not isinstance(obj, protocol):
            return False
        if hasattr(obj, "is_available") and not inspect.iscoroutinefunction(obj.is_available):
            try:
                if not obj.is_available():
                    return False
            except Exception as exc:
                logger.warning(
                    "Driver %s.is_available() raised %s; treating as unavailable",
                    type(obj).__name__, exc,
                )
                return False
        seen.add(obj_id)
        return True

    # 1. Dynamically registered plugins
    for instance in _DYNASTORE_PLUGINS:
        if _accept(instance):
            discovered.append(instance)

    # 2. Modules
    from dynastore.modules import _DYNASTORE_MODULES
    for config in _DYNASTORE_MODULES.values():
        if config.instance and _accept(config.instance):
            discovered.append(config.instance)

    # 3. Extensions
    try:
        from dynastore.extensions.registry import _DYNASTORE_EXTENSIONS
        for config in _DYNASTORE_EXTENSIONS.values():
            if config.instance and _accept(config.instance):
                discovered.append(config.instance)
    except (ImportError, ModuleNotFoundError):
        pass

    # 4. Tasks
    from dynastore.tasks import _DYNASTORE_TASKS
    for config in _DYNASTORE_TASKS.values():
        if config.instance and _accept(config.instance):
            discovered.append(config.instance)

    discovered.sort(key=lambda x: getattr(x, "priority", 100))
    return discovered


def get_protocol(protocol: Type[T]) -> Optional[T]:
    """Return the first registered instance of the given protocol, or None."""
    return cast(Optional[T], _get_protocol_cached(protocol))


def get_protocols(protocol: Type[T]) -> List[T]:
    """Return all registered instances of the given protocol, sorted by priority."""
    return cast(List[T], _get_protocols_cached(protocol))


def get_all_protocols(protocol: Type[T]) -> List[T]:
    """Return all registered instances of the given protocol — including
    those whose ``is_available()`` currently returns False. Intended for
    introspection UIs (e.g. driver listings for a configurator)."""
    discovered: List[Any] = []
    seen: set = set()

    def _accept(obj: Any) -> bool:
        obj_id = id(obj)
        if obj_id in seen or not isinstance(obj, protocol):
            return False
        seen.add(obj_id)
        return True

    for instance in _DYNASTORE_PLUGINS:
        if _accept(instance):
            discovered.append(instance)

    from dynastore.modules import _DYNASTORE_MODULES
    for config in _DYNASTORE_MODULES.values():
        if config.instance and _accept(config.instance):
            discovered.append(config.instance)

    try:
        from dynastore.extensions.registry import _DYNASTORE_EXTENSIONS
        for config in _DYNASTORE_EXTENSIONS.values():
            if config.instance and _accept(config.instance):
                discovered.append(config.instance)
    except (ImportError, ModuleNotFoundError):
        pass

    from dynastore.tasks import _DYNASTORE_TASKS
    for config in _DYNASTORE_TASKS.values():
        if config.instance and _accept(config.instance):
            discovered.append(config.instance)

    discovered.sort(key=lambda x: getattr(x, "priority", 100))
    return cast(List[T], discovered)


def _clear_protocol_caches() -> None:
    _get_protocol_cached.cache_clear()
    _get_protocols_cached.cache_clear()


# Expose cache_clear on the wrapper functions so callers (e.g. tests) can
# invalidate the caches via `get_protocol.cache_clear()`.
get_protocol.cache_clear = _clear_protocol_caches
get_protocols.cache_clear = _clear_protocol_caches


def discover_and_load_plugins(group: str) -> Dict[str, Type[Any]]:
    """Discover plugins registered via entry points.

    Identity is package metadata: every entry-point declared by an installed
    distribution in ``group`` is loaded.  Optional deps are filtered by Python
    itself — an entry-point whose module raises ``ImportError`` (because its
    deps weren't selected at ``pip install`` time) is gracefully skipped.

    No runtime SCOPE filter, no name-based allowlists.  SCOPE exists only as a
    build-time pip extras selector in the Dockerfile.
    """
    loaded_plugins: Dict[str, Type[Any]] = {}

    for entry_point in importlib.metadata.entry_points(group=group):
        try:
            loaded_plugins[entry_point.name] = entry_point.load()
            logger.info(f"Successfully discovered installed {group}: {entry_point.name}")
        except ImportError as e:
            logger.warning(
                f"Skipping {group} plugin '{entry_point.name}': "
                f"optional dependencies not installed — {e} "
                f"(expected when SCOPE excludes the plugin's extras; "
                f"unexpected if the plugin should be active for this service)"
            )
        except Exception as e:
            logger.error(
                f"Failed to load installed plugin '{entry_point.name}' "
                f"from group '{group}': {e}",
                exc_info=True,
            )

    return loaded_plugins

def get_installed_module_metadata() -> List[Dict[str, str]]:
    """
    Returns metadata about installed modules for documentation auto-generation.
    Only modules installed via build-time extras will appear here.
    """
    metadata = []
    
    # We check the main groups
    groups = ["dynastore.modules", "dynastore.extensions", "dynastore.tasks"]
    
    for group in groups:
        eps = importlib.metadata.entry_points(group=group)

        for ep in eps:
            # Basic metadata derived from the entry point
            item = {
                "name": ep.name,
                "group": group,
                "module": ep.value.split(":")[0],
            }
            
            # Attempt to find the distribution package that provides this entry point
            try:
                if hasattr(ep, "dist") and ep.dist:
                    dist = ep.dist
                    item["package"] = dist.metadata.get("Name", "unknown")
                    item["version"] = dist.version
                    item["summary"] = dist.metadata.get("Summary", "")
            except Exception as e:
                logger.debug(f"Could not extract package metadata for {ep.name}: {e}")
                
            metadata.append(item)
            
    return metadata
