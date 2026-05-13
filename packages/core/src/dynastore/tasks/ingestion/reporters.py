from typing import Dict, Type

from dynastore.tasks.reporters import ReportingInterface
from dynastore.tools.typed_store._snake import to_snake

_ingestion_reporter_registry: Dict[str, Type[ReportingInterface]] = {}


def ingestion_reporter(cls: Type[ReportingInterface]) -> Type[ReportingInterface]:
    """Register a reporter under its snake_case identity.

    Mirrors ``PersistentModel.class_key()`` — ``GcsDetailedReporter`` →
    ``gcs_detailed_reporter`` — so reporter ids share the convention used by
    every other typed registration in the codebase (PluginConfig, drivers).
    Imports ``to_snake`` from the leaf module ``typed_store._snake`` (no
    project imports) to keep this file off the ``PluginConfig`` registration
    chain that would otherwise circular-import via ``typed_store.base``.
    """
    reporter_name = to_snake(cls.__name__)
    if reporter_name in _ingestion_reporter_registry:
        raise TypeError(f"Reporter with name '{reporter_name}' is already registered.")
    _ingestion_reporter_registry[reporter_name] = cls
    return cls
