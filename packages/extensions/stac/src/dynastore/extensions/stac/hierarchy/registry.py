"""Decorator registry mapping `kind` → provider factory."""

from __future__ import annotations

from typing import Any, Callable, Dict

from .base import HierarchyProvider
from .config import HierarchyProviderConfig


ProviderFactory = Callable[[HierarchyProviderConfig, Any], HierarchyProvider]

_REGISTRY: Dict[str, ProviderFactory] = {}


def register_provider(kind: str) -> Callable[[ProviderFactory], ProviderFactory]:
    """Decorator: register a factory for `kind`.

    @register_provider("static")
    def _build_static(config, ctx): ...
    """

    def _decorate(factory: ProviderFactory) -> ProviderFactory:
        if kind in _REGISTRY:
            raise ValueError(f"provider kind already registered: {kind!r}")
        _REGISTRY[kind] = factory
        return factory

    return _decorate


def get_hierarchy_provider(
    config: HierarchyProviderConfig,
    ctx: Any,
) -> HierarchyProvider:
    """Dispatch on `config.kind` to the registered factory."""
    try:
        factory = _REGISTRY[config.kind]
    except KeyError as exc:
        raise LookupError(
            f"no provider registered for kind={config.kind!r}; "
            f"known kinds: {sorted(_REGISTRY)}"
        ) from exc
    return factory(config, ctx)
