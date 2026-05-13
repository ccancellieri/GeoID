import logging
from typing import Any, Dict, List, Optional, Type
from pydantic import BaseModel

from dynastore.tasks.reporters import ReportingInterface
from dynastore.tasks.ingestion.reporters_impl import DatabaseStatusReporter

logger = logging.getLogger(__name__)

def _get_config_model_from_reporter(reporter_class: Type[ReportingInterface]) -> Optional[Type[BaseModel]]:
    """
    Extracts the configuration model class from a reporter implementation's 
    generic base class, if one is specified.
    """
    from typing import get_args, get_origin
    for base in getattr(reporter_class, "__orig_bases__", []):
        if get_origin(base) is ReportingInterface:
            args = get_args(base)
            if args and issubclass(args[0], BaseModel):
                return args[0]
    return None

def initialize_reporters(
    engine: Any, 
    task_id: str, 
    task_request: Any, 
    reporting_config: Optional[dict] = None,
    registry: Optional[Dict[str, Type[ReportingInterface]]] = None,
    schema: str = "tasks",
    catalog_id: Optional[str] = None,
    collection_id: Optional[str] = None
) -> List[ReportingInterface]:
    """
    Initialize reporters for a task based on its configuration.
    
    Args:
        engine: Database engine/engine manager
        task_id: Unique task ID
        task_request: Full request object for the task
        reporting_config: Optional reporting configuration dict (from request.reporting)
        registry: Optional registry of available reporters to lookup from
        schema: Database schema to report to (default: "tasks")
        catalog_id: Optional catalog ID owner of the task
        collection_id: Optional collection ID owner of the task
        
    Returns:
        List of initialized reporter instances
    """
    reporters: List[ReportingInterface] = []
    
    # --- Base arguments for all reporters ---
    base_args = {
        "engine": engine,
        "task_id": task_id,
        "task_request": task_request,
        "schema": schema,
        "catalog_id": catalog_id,
        "collection_id": collection_id
    }

    # DatabaseStatusReporter is usually always wanted if we want to track status in DB
    reporters.append(DatabaseStatusReporter(**base_args))
    logger.debug(f"Task '{task_id}': DatabaseStatusReporter enabled (schema: {schema}).")
    
    if not reporting_config or not registry:
        return reporters

    unresolved: List[str] = []
    for reporter_name, config_dict in reporting_config.items():
        if reporter_name not in registry:
            unresolved.append(reporter_name)
            continue
        reporter_class = registry[reporter_name]
        config_model_class = _get_config_model_from_reporter(reporter_class)

        config_obj = None
        if config_model_class and config_dict:
            try:
                config_obj = config_model_class(**config_dict)
            except Exception as e:
                logger.warning(f"Task '{task_id}': Failed to validate config for reporter '{reporter_name}': {e}")

        reporters.append(reporter_class(**base_args, config=config_obj))
        logger.info(f"Task '{task_id}': Reporter '{reporter_name}' initialized.")

    if unresolved:
        # Silent skip used to mask user typos (e.g. issue #654: `gcs_detailed` vs
        # the registered `gcs_detailed_reporter`) — the task completed but no
        # report ever appeared. Fail loud so callers correct the config.
        available = ", ".join(sorted(registry.keys())) or "<none>"
        raise ValueError(
            f"Task '{task_id}': unknown reporter(s) {unresolved!r}; "
            f"available: [{available}]"
        )

    return reporters
