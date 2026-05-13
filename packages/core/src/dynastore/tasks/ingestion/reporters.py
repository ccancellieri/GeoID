import re
from typing import Dict, Type
from dynastore.tasks.reporters import ReportingInterface

_ingestion_reporter_registry: Dict[str, Type[ReportingInterface]] = {}

_PASCAL_BOUNDARY_1 = re.compile(r"(.)([A-Z][a-z]+)")
_PASCAL_BOUNDARY_2 = re.compile(r"([a-z0-9])([A-Z])")


def _to_snake(name: str) -> str:
    leading = ""
    body = name
    while body.startswith("_"):
        leading += "_"
        body = body[1:]
    s1 = _PASCAL_BOUNDARY_1.sub(r"\1_\2", body)
    return leading + _PASCAL_BOUNDARY_2.sub(r"\1_\2", s1).lower()


def ingestion_reporter(cls: Type[ReportingInterface]) -> Type[ReportingInterface]:
    """Register a reporter under its snake_case identity.

    Mirrors ``PersistentModel.class_key()`` — ``GcsDetailedReporter`` →
    ``gcs_detailed_reporter`` — so reporter ids share the convention used by
    every other typed registration in the codebase (PluginConfig, drivers).
    The helper is inlined to avoid pulling ``typed_store.base`` (and its
    transitive ``PluginConfig`` registration) onto the leaf-tasks import path,
    which produces a circular import.
    """
    reporter_name = _to_snake(cls.__name__)
    if reporter_name in _ingestion_reporter_registry:
        raise TypeError(f"Reporter with name '{reporter_name}' is already registered.")
    _ingestion_reporter_registry[reporter_name] = cls
    return cls
