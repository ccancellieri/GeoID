from typing import Any, Dict, Type
from dynastore.tasks.reporters import ReportingInterface

# --- Ingestion Reporter Registry ---
_ingestion_reporter_registry: Dict[str, Type[ReportingInterface]] = {}

def ingestion_reporter(cls: Type[ReportingInterface]) -> Type[ReportingInterface]:
    """A class decorator to register a class as an available ingestion reporter."""
    reporter_name = cls.__name__
    if reporter_name in _ingestion_reporter_registry:
        raise TypeError(f"Reporter with name '{reporter_name}' is already registered.")
    _ingestion_reporter_registry[reporter_name] = cls
    return cls
