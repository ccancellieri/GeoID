"""Per-field mutability framework for ``PluginConfig`` (#665 slice 4).

This module is the single home for every mutability concept:

- The four marker types — ``Mutable[T]`` / ``WriteOnce[T]`` /
  ``Immutable[T]`` / ``Computed[T]`` — and their underlying
  ``*Marker`` metadata classes.
- ``is_immutable_field`` / ``is_write_once_field`` — per-field marker
  predicates used by the immutability enforcer.
- ``mutability_map(cls)`` / ``missing_markers(cls)`` — introspection
  helpers used by ``PluginConfig``'s enforcer and the composed-config
  renderer.
- ``MutabilityIntrospectionProtocol`` — the contract the
  composed-config renderer depends on (per ``feedback_prefer_protocols``).

It is intentionally dependency-free (stdlib + pydantic only): the
markers used to live in ``platform_config_service`` next to
``PluginConfig``, which pulled that heavy module into a load-order
cycle whenever a Protocol-contracts file (e.g. ``models/protocols/
authorization.py``) needed a marker.  Keeping the markers in this leaf
module breaks the cycle structurally — see #686.

Authors annotate every Pydantic field with exactly one marker::

    from dynastore.models.mutability import Mutable, WriteOnce, Immutable, Computed

    class CollectionPostgresqlDriverConfig(CollectionDriverConfig):
        engine_ref:     WriteOnce[Optional[str]] = Field(default=None, ...)
        physical_table: Immutable[str]           = Field(default="collection_stac", ...)
        sidecars:       Mutable[List[str]]       = Field(default_factory=list, ...)

Each marker contributes ``readOnly`` (where applicable) +
``x-mutability`` to the field's JSON Schema via its
``__get_pydantic_json_schema__`` hook.  ``mutability_map()`` produces
the same data in a flat ``{field: kind}`` map for ``_meta.mutability``
under ``?meta=field``.
"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Dict,
    FrozenSet,
    Iterable,
    Protocol,
    Type,
    get_args,
    get_origin,
    get_type_hints,
    runtime_checkable,
)

if TYPE_CHECKING:
    from pydantic.fields import FieldInfo


# ---------------------------------------------------------------------------
#  Marker metadata classes
# ---------------------------------------------------------------------------


class _MutabilityKindMixin:
    """Shared ``__get_pydantic_json_schema__`` hook for all four markers.

    Each subclass declares ``kind: ClassVar[str]`` — one of ``"mutable"``,
    ``"write_once"``, ``"immutable"``, ``"computed"``.  The hook injects
    ``x-mutability: <kind>`` into the field's JSON Schema (#665 slice 4)
    and, for everything except ``Mutable``, ``readOnly: true`` (JSON
    Schema 2020-12 standard form-builders honour natively) + the legacy
    ``x-ui.readonly`` advisory the existing admin UI consumes.
    """

    kind: ClassVar[str] = ""

    @classmethod
    def __get_pydantic_json_schema__(cls, schema, handler):
        out = handler(schema)
        out["x-mutability"] = cls.kind
        if cls.kind != "mutable":
            out["readOnly"] = True
            from dynastore.tools.ui_hints import merge_ui
            out = merge_ui(out, readonly=True)
        return out


class MutableMarker(_MutabilityKindMixin):
    """Freely editable across the config's lifetime (the default kind)."""
    kind: ClassVar[str] = "mutable"


class ImmutableMarker(_MutabilityKindMixin):
    """Fixed at class definition; ``enforce_config_immutability`` rejects diffs."""
    kind: ClassVar[str] = "immutable"


class WriteOnceMarker(_MutabilityKindMixin):
    """Settable once from ``None`` → value, locked thereafter."""
    kind: ClassVar[str] = "write_once"


class ComputedMarker(_MutabilityKindMixin):
    """Derived value with no stored override.  Prefer ``@computed_field`` when no value is stored."""
    kind: ClassVar[str] = "computed"


# ---------------------------------------------------------------------------
#  Marker types — ``Field: Mutable[int]`` etc.
# ---------------------------------------------------------------------------


if TYPE_CHECKING:
    type Mutable[T] = T  # pyright-transparent alias
    type WriteOnce[T] = T
    type Immutable[T] = T
    type Computed[T] = T
else:
    class Mutable:
        """Marker for freely-editable fields.

        Usage::

            field: Mutable[int] = Field(default=0, description="...")

        Equivalent to ``field: Annotated[int, MutableMarker] = Field(...)``.
        """

        def __class_getitem__(cls, item: Any) -> Any:
            return Annotated[item, MutableMarker]

    class Immutable:
        """Marker for class-definition-fixed fields.

        Usage::

            field: Immutable[int] = Field(...)
        """

        def __class_getitem__(cls, item: Any) -> Any:
            return Annotated[item, ImmutableMarker]

    class WriteOnce:
        """Marker for write-once fields (``None`` → value transition allowed,
        non-``None`` → anything rejected).

        Usage::

            field: WriteOnce[Optional[str]] = Field(default=None, ...)
        """

        def __class_getitem__(cls, item: Any) -> Any:
            return Annotated[item, WriteOnceMarker]

    class Computed:
        """Marker for derived fields with no stored override.

        Prefer ``@computed_field`` when no stored value is needed; this
        marker is for fields that ARE stored but recomputed externally.
        """

        def __class_getitem__(cls, item: Any) -> Any:
            return Annotated[item, ComputedMarker]


# ---------------------------------------------------------------------------
#  Per-field marker predicates
# ---------------------------------------------------------------------------


def is_immutable_field(field_info: "FieldInfo") -> bool:
    if get_origin(field_info.annotation) is Annotated:
        args = get_args(field_info.annotation)
        if ImmutableMarker in args or Immutable in args:
            return True
    return any(
        item is ImmutableMarker
        or item is Immutable
        or (isinstance(item, type) and issubclass(item, ImmutableMarker))
        for item in field_info.metadata
    )


def is_write_once_field(field_info: "FieldInfo") -> bool:
    if get_origin(field_info.annotation) is Annotated:
        args = get_args(field_info.annotation)
        if WriteOnceMarker in args or WriteOnce in args:
            return True
    return any(
        item is WriteOnceMarker
        or item is WriteOnce
        or (isinstance(item, type) and issubclass(item, WriteOnceMarker))
        for item in field_info.metadata
    )


def is_computed_field(field_info: "FieldInfo") -> bool:
    """True when the field is marked ``Computed[...]`` — a stored value that is
    machine-assigned (generated/recomputed by the system), never editable by an
    external caller.  Such fields advertise ``readOnly`` and are stripped from
    caller-supplied config payloads on the external write path."""
    if get_origin(field_info.annotation) is Annotated:
        args = get_args(field_info.annotation)
        if ComputedMarker in args or Computed in args:
            return True
    return any(
        item is ComputedMarker
        or item is Computed
        or (isinstance(item, type) and issubclass(item, ComputedMarker))
        for item in field_info.metadata
    )


# ---------------------------------------------------------------------------
#  Introspection helpers
# ---------------------------------------------------------------------------


def _extract_kind(annotation: Any) -> str | None:
    """Return ``"mutable"`` / ``"write_once"`` / ``"immutable"`` / ``"computed"``
    if the type carries a mutability marker, else ``None``.

    Resolves both ``__class_getitem__`` markers (``Immutable[T]``
    expanding to ``Annotated[T, ImmutableMarker]`` — the existing
    pattern) and the bare instance form (``Annotated[T, MarkerInstance]``).
    """
    if get_origin(annotation) is None:
        return None
    metadata = tuple(get_args(annotation))[1:]
    for meta in metadata:
        for marker_cls, kind in (
            (MutableMarker, "mutable"),
            (WriteOnceMarker, "write_once"),
            (ImmutableMarker, "immutable"),
            (ComputedMarker, "computed"),
        ):
            if meta is marker_cls or (
                isinstance(meta, type) and issubclass(meta, marker_cls)
            ):
                return kind
    return None


def mutability_map(cls: Type[Any]) -> Dict[str, str]:
    """Build the ``{field_name: kind}`` map for a Pydantic model class.

    Only Pydantic ``model_fields`` are included — ClassVars, plain class
    attributes, and ``@computed_field`` outputs are skipped (they aren't
    stored fields and don't need a marker).
    """
    hints = get_type_hints(cls, include_extras=True)
    out: Dict[str, str] = {}
    model_fields = getattr(cls, "model_fields", {})
    for name in model_fields:
        kind = _extract_kind(hints.get(name))
        if kind is not None:
            out[name] = kind
    return out


def missing_markers(cls: Type[Any]) -> Iterable[str]:
    """Yield Pydantic field names with no mutability marker on the resolved type."""
    hints = get_type_hints(cls, include_extras=True)
    model_fields = getattr(cls, "model_fields", {})
    for name in model_fields:
        if _extract_kind(hints.get(name)) is None:
            yield name


def computed_fields(cls: Type[Any]) -> FrozenSet[str]:
    """Names of ``Computed[...]`` fields on a Pydantic model class.

    These are machine-assigned/generated stored fields. The config write path
    uses this set to discard caller-supplied values on the external (non-trusted)
    write path — system-assigned fields can never be set or changed by an API
    caller, only by the internal provisioner (which writes with
    ``check_immutability=False``)."""
    return frozenset(
        name for name, kind in mutability_map(cls).items() if kind == "computed"
    )


@runtime_checkable
class MutabilityIntrospectionProtocol(Protocol):
    """Surface a PluginConfig class exposes for mutability introspection.

    Implemented by ``PluginConfig`` itself (via plumbing in
    ``platform_config_service.py``).  The renderer depends on this
    Protocol, not on the concrete class — keeps the configs extension
    framework-free per ``feedback_prefer_protocols``.
    """

    @classmethod
    def mutability_map(cls) -> Dict[str, str]:
        """``{field_name: 'mutable'|'write_once'|'immutable'|'computed'}``."""
        ...
