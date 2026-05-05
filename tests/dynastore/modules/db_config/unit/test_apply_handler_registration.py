"""Pin the Phase 1.5 apply-handler registration cleanup.

Two registration paths used to coexist:

- **Pattern A (retired)** — ``_on_apply: ClassVar[Optional[Callable]] = fn``
  on the class body.  ``PluginConfig.__init_subclass__`` would pick it up
  and append to ``_APPLY_HANDLERS``.  Single-handler-only; couldn't compose.

- **Pattern B (canonical)** — ``MyConfig.register_apply_handler(fn)`` called
  imperatively at module-import time.  Multi-handler support, no
  initialisation-order surprises.

Phase 1.5 standardised on Pattern B, retired Pattern A from the base, and
added an enforcement check in ``__init_subclass__`` that raises a clear
migration error if a developer reintroduces the ClassVar pattern.

These tests pin:
- The base class no longer declares ``_on_apply``.
- Every concrete ``PluginConfig`` subclass that needs an apply handler
  uses the imperative path (handler registered, no ``_on_apply`` in
  ``cls.__dict__``).
- The ``__init_subclass__`` enforcement raises with a useful message if
  a future class reintroduces the ClassVar pattern.
"""

from __future__ import annotations

from typing import ClassVar, Optional, Tuple

import pytest

from dynastore.modules.db_config.platform_config_service import (
    PluginConfig,
    _APPLY_HANDLERS,
    list_registered_configs,
)


def test_base_does_not_declare_on_apply():
    """``PluginConfig`` no longer declares ``_on_apply: ClassVar``.

    The retired ClassVar slot would silently absorb subclass declarations
    via ``__init_subclass__`` — Phase 1.5 deletes the slot AND adds an
    enforcement raise so the migration is uniform.
    """
    assert "_on_apply" not in PluginConfig.__dict__


def test_no_concrete_subclass_declares_on_apply_classvar():
    """Audit every registered concrete subclass.

    A regression here means a new subclass slipped through the
    ``__init_subclass__`` check (or the check itself was bypassed).
    Either way, the dual-pattern footgun is back.
    """
    violations: list[str] = []
    for class_key, cls in list_registered_configs().items():
        if cls.__dict__.get("is_abstract_base", False):
            continue
        if "_on_apply" in cls.__dict__:
            violations.append(
                f"  - {cls.__module__}.{cls.__qualname__}: declares "
                f"``_on_apply`` ClassVar (Pattern A — retired). Migrate to "
                f"``{cls.__qualname__}.register_apply_handler(fn)``."
            )
    if violations:
        pytest.fail(
            "Phase 1.5 violation — apply-handler registration must use "
            "the imperative ``register_apply_handler`` path:\n"
            + "\n".join(violations),
        )


def test_init_subclass_raises_on_legacy_on_apply_pattern():
    """Reintroducing the retired ClassVar pattern must fail loudly at
    class-creation time with a clear migration message.
    """
    with pytest.raises(TypeError, match=r"retired ``_on_apply.*ClassVar`` pattern"):

        class _LegacyOnApplyConcreteConfig(PluginConfig):
            # Distinct name to avoid class_key collision with the
            # ``_BadConcreteConfig`` defined inline in the address-enforcement
            # test in ``test_config_api_service.py`` (TypedModelRegistry
            # refuses duplicate class_keys; both tests' inline classes
            # would otherwise resolve to ``_bad_concrete_config``).
            _address: ClassVar[Tuple[str, str, Optional[str]]] = (
                "platform", "misc", None,
            )

            @staticmethod
            def _legacy_handler(*_args, **_kwargs):
                pass

            _on_apply: ClassVar = _legacy_handler  # type: ignore[assignment]


def test_imperative_register_apply_handler_works():
    """The canonical Pattern B continues to work and supports multiple
    handlers per class (which Pattern A could not do — one ClassVar slot).
    """

    class _DemoConfig(PluginConfig):
        _address: ClassVar[Tuple[str, str, Optional[str]]] = (
            "platform", "misc", None,
        )

    calls: list[str] = []

    def handler_one(*_args, **_kwargs):
        calls.append("one")

    def handler_two(*_args, **_kwargs):
        calls.append("two")

    _DemoConfig.register_apply_handler(handler_one)
    _DemoConfig.register_apply_handler(handler_two)

    handlers = _APPLY_HANDLERS[_DemoConfig]
    assert handlers == [handler_one, handler_two]


def test_known_consumers_have_handlers_registered():
    """Sanity-check the classes that ship apply handlers — their handlers
    must end up in ``_APPLY_HANDLERS`` after import.

    Cycle E retired the catalog-wide private-mode apply handler
    (``ElasticsearchCatalogConfig`` deleted; ``CatalogPolicyConfig`` is
    pure data — consulted only at collection-create time as a default
    seed for the per-collection ``is_private`` flag).
    """
    from dynastore.modules.gcp.gcp_config import (
        GcpCatalogBucketConfig,
        GcpEventingConfig,
    )

    for cls in (GcpCatalogBucketConfig, GcpEventingConfig):
        handlers = _APPLY_HANDLERS.get(cls, [])
        assert handlers, (
            f"{cls.__name__} has no apply handler registered — "
            f"its module must call ``{cls.__name__}.register_apply_handler(...)`` "
            f"at module-import time."
        )
