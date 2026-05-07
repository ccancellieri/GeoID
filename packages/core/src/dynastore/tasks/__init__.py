import logging
import inspect
import os
from dataclasses import dataclass
from contextlib import asynccontextmanager, AsyncExitStack
from typing import Dict, Type, Any, Optional, List, cast, AsyncGenerator

from .protocols import TaskProtocol
from dynastore.models.tasks import TaskPayload
from dynastore.tools.env import load_component_dotenv

logger = logging.getLogger(__name__)

@dataclass
class TaskConfig:
    cls: Type[TaskProtocol]
    module_name: str
    name: str
    type: str = "task"
    definition: Any = None
    instance: TaskProtocol | None = None

_DYNASTORE_TASKS: Dict[str, TaskConfig] = {}

# Module-level handle to the runtime app_state so ``get_task_instance`` can
# construct task classes that declare ``def __init__(self, app_state)`` even
# when called outside the ``manage_tasks`` lifespan (e.g. capability checks
# during dispatcher startup, FastAPI runners that look up tasks ad-hoc).
# Set by ``manage_tasks`` and ``tasks/bootstrap.py``.
_TASK_APP_STATE: object | None = None


def set_task_app_state(app_state: object) -> None:
    """Register the runtime app_state used to construct task instances.

    Called once during process bootstrap (Cloud Run job entrypoint or API
    server lifespan). Subsequent ``get_task_instance`` calls will pass it to
    task ``__init__`` methods that declare an ``app_state`` parameter.
    """
    global _TASK_APP_STATE
    _TASK_APP_STATE = app_state

def get_task_config(task_type: str) -> Optional[TaskConfig]:
    """Look up task config by registration name or task_type class attribute."""
    config = _DYNASTORE_TASKS.get(task_type)
    if config:
        return config
    for cfg in _DYNASTORE_TASKS.values():
        if getattr(cfg.cls, "task_type", None) == task_type:
            return cfg
    return None

def get_all_task_configs() -> Dict[str, TaskConfig]:
    return _DYNASTORE_TASKS

def get_loaded_task_types() -> List[str]:
    """Returns a list of all task_type values from discovered task classes.

    These correspond to the task_type attribute on each TaskProtocol class,
    which is the value stored in the DB 'task_type' column — NOT the entry-point
    registration name (which may differ).
    """
    types: List[str] = []
    for name, config in _DYNASTORE_TASKS.items():
        task_type = getattr(config.cls, "task_type", None)
        types.append(task_type if task_type else name)
    return types

def get_definitions_by_type(target_type: Any) -> List[Any]:
    """
    Returns a list of task definitions (e.g. Process objects) that match the target type.
    """
    results = []
    for config in _DYNASTORE_TASKS.values():
        if config.definition and isinstance(config.definition, target_type):
            results.append(config.definition)
    return results

def get_task_definitions() -> List[Any]:
    """Returns all task definitions."""
    return [config.definition for config in _DYNASTORE_TASKS.values() if config.definition]

def discover_tasks():
    """Discover every ``dynastore.tasks`` entry-point from installed packages.

    Identity is package metadata + protocol shape: every entry-point whose
    module imports successfully is registered; entry-points whose heavy deps
    are missing fall back to definition-only placeholders (Process metadata
    without a runnable class) so remote runners (Cloud Run Jobs) can still
    surface OGC Process schemas via the catalog service.
    """
    logger.info("--- [tasks] Discovering components via entry points... ---")

    from dynastore.tools.discovery import discover_and_load_plugins
    classes = discover_and_load_plugins("dynastore.tasks")

    for name, cls in classes.items():
        # Use the task_type class attribute as key when available so the
        # registry key matches the value stored in the DB task_type column.
        key = getattr(cls, "task_type", None) or name

        definition = None
        if hasattr(cls, "get_definition"):
            definition = cls.get_definition()
        elif hasattr(cls, "get_process_definition"):
            definition = cls.get_process_definition()

        if key is None:
            continue
        _DYNASTORE_TASKS[key] = TaskConfig(
            cls=cls,
            type="task",
            module_name=cls.__module__,
            name=key,
            definition=definition
        )

    _register_definition_only_placeholders(already_loaded=set(classes.keys()))

    logger.info(f"--- DISCOVERED TASKS: {list(_DYNASTORE_TASKS.keys())} ---")


def _register_definition_only_placeholders(already_loaded: set) -> None:
    """Fallback for tasks whose heavy deps are missing.

    Iterate entry points in ``dynastore.tasks`` that aren't already loaded,
    load the ``<package>.definition`` sibling module (which must only depend on
    lightweight packages), and register a placeholder TaskConfig exposing the
    Process definition. The placeholder can't ``run()`` — it exists so callers
    (e.g. ``/processes`` inventory, GcpCloudRunRunner) can see the metadata.
    """
    import importlib.metadata
    import importlib

    from dynastore.modules.processes.models import Process

    for ep in importlib.metadata.entry_points(group="dynastore.tasks"):
        if ep.name in already_loaded:
            continue
        module_path = ep.value.split(":")[0]
        pkg_path = module_path.rsplit(".", 1)[0]
        definition_mod_path = f"{pkg_path}.definition"
        try:
            def_mod = importlib.import_module(definition_mod_path)
        except ImportError as e:
            logger.debug(
                f"Task '{ep.name}' heavy deps missing and no {definition_mod_path} "
                f"fallback available: {e}"
            )
            continue
        process_def: Optional[Process] = None
        for attr in vars(def_mod).values():
            if isinstance(attr, Process):
                process_def = attr
                break
        if process_def is None:
            logger.debug(
                f"Task '{ep.name}' fallback {definition_mod_path} has no Process "
                "instance at module scope; skipping."
            )
            continue

        task_type = getattr(process_def, "id", ep.name) or ep.name

        class DefinitionOnlyTask:
            """Placeholder: heavy deps missing; execution delegated to a runner."""
            _task_type = task_type
            _process_definition = process_def
            is_placeholder = True

            @staticmethod
            def get_definition() -> Process:
                return process_def  # type: ignore[return-value]

        _DYNASTORE_TASKS[ep.name] = TaskConfig(
            cls=cast(Type[TaskProtocol], DefinitionOnlyTask),
            type="task",
            module_name=definition_mod_path,
            name=ep.name,
            definition=process_def,
        )
        logger.info(
            f"Registered definition-only placeholder for task '{ep.name}' "
            f"from {definition_mod_path} (heavy deps not installed)."
        )


def _register_task(target_cls: Type[TaskProtocol], registration_name: Optional[str] = None, type: str = "task"):
    if registration_name is None:
        # Prefer the explicit task_type class attribute (matches what gets stored
        # in the DB) so that each task class gets a unique key even when multiple
        # task classes live in the same Python package.
        task_type_attr = getattr(target_cls, "task_type", None)
        if task_type_attr:
            registration_name = task_type_attr
        else:
            try:
                parts = target_cls.__module__.split('.')
                idx = parts.index('tasks')
                registration_name = parts[idx + 1]
            except (ValueError, IndexError):
                registration_name = target_cls.__name__

    assert registration_name is not None
    if registration_name in _DYNASTORE_TASKS:
        if _DYNASTORE_TASKS[registration_name].cls == target_cls:
            return target_cls
        logger.warning(f"Task '{registration_name}' is already registered. Overwriting.")

    # Extract some metadata from the class if possible
    definition = None
    if hasattr(target_cls, "get_definition"):
        definition = target_cls.get_definition() # type: ignore
    elif hasattr(target_cls, "get_process_definition"):
        definition = target_cls.get_process_definition() # type: ignore

    _DYNASTORE_TASKS[registration_name] = TaskConfig(
        cls=target_cls,
        type=type,
        module_name=target_cls.__module__,
        name=registration_name,
        definition=definition
    )
    setattr(target_cls, "_registered_name", registration_name)
    logger.info(f"Registered task: {target_cls.__name__} (as '{registration_name}')")
    return target_cls

def get_task_instance(name: str, app_state: object | None = None) -> TaskProtocol | None:
    """Look up a task by registration name OR by its task_type class attribute.

    If the task class declares ``def __init__(self, app_state)`` it is
    constructed with the supplied ``app_state`` (or, if None, the module-level
    one set by :func:`set_task_app_state` / :func:`manage_tasks`). This mirrors
    the construction logic in :func:`manage_tasks` so single-shot lookups
    (capability checks, ad-hoc runners) work identically.
    """
    config = _DYNASTORE_TASKS.get(name)
    if not config:
        # Fallback: search by task_type attribute (DB task_type may differ from
        # the entry-point registration name).
        for cfg in _DYNASTORE_TASKS.values():
            if getattr(cfg.cls, "task_type", None) == name:
                config = cfg
                break
    if not config:
        return None

    # Skip definition-only placeholders. They exist so that `/processes`
    # discovery can surface task metadata in services that don't carry the
    # task's heavy deps — but they have no `run` method, so claiming them
    # via Background/Sync runner crashes at first dispatch with
    # `'DefinitionOnlyTask' object has no attribute 'run'`. Returning None
    # here makes `Sync/BackgroundRunner.can_handle()` return False for
    # placeholders, letting GcpJobRunner take the task and route it to the
    # actual Cloud Run Job container which DOES have the deps loaded.
    if getattr(config.cls, "is_placeholder", False):
        return None

    if config.instance:
        return config.instance

    load_component_dotenv(config.cls)
    try:
        sig = inspect.signature(config.cls.__init__)
        factory = cast(Any, config.cls)
        if "app_state" in sig.parameters:
            resolved_app_state = app_state if app_state is not None else _TASK_APP_STATE
            if resolved_app_state is None:
                logger.error(
                    "Task '%s' requires app_state but none was provided and "
                    "set_task_app_state() was never called. Ensure the process "
                    "bootstrap calls bootstrap_task_env() or manage_tasks().",
                    name,
                )
                return None
            return factory(app_state=resolved_app_state)
        return factory()
    except Exception:
        logger.exception("Failed to instantiate task '%s'", name)
        return None

def _extract_inputs_type(payload_hint):
    """Extract the InputsType T from a TaskPayload[T] type annotation.

    ``typing.get_args`` returns ``()`` for Pydantic v2 concrete generics
    because Pydantic creates real subclasses instead of ``typing`` aliases.
    Fall back to ``__pydantic_generic_metadata__['args']`` in that case.
    """
    from typing import get_args
    args = get_args(payload_hint)
    if args:
        return args[0]
    # Pydantic v2 concrete generic: TaskPayload[T] is a ModelMetaclass subclass
    meta = getattr(payload_hint, '__pydantic_generic_metadata__', None)
    if meta:
        pydantic_args = meta.get('args', ())
        if pydantic_args:
            return pydantic_args[0]
    return None


def hydrate_task_payload(task_instance: TaskProtocol, raw_payload: Dict[str, Any]) -> TaskPayload:
    """
    Hydrates a raw payload dictionary into a strongly-typed TaskPayload instance,
    using the type hints from the task's run method to determine the expected input type.
    """
    import inspect
    from typing import get_type_hints
    from pydantic import BaseModel

    # 1. Determine the expected input type from the task's run method
    input_model = None
    try:
        # Get type hints for the 'run' method
        hints = get_type_hints(task_instance.run)
        payload_hint = hints.get('payload')
        if payload_hint:
            input_model = _extract_inputs_type(payload_hint)
    except Exception as e:
        logger.debug(f"Failed to extract input model for task {task_instance.__class__.__name__}: {e}")

    # 2. Extract raw inputs
    inputs = raw_payload.get("inputs", {})
    
    # 3. Hydrate inputs if they are expected to be a Pydantic model
    if input_model and inspect.isclass(input_model) and issubclass(input_model, BaseModel) and isinstance(inputs, dict):
        try:
            inputs = input_model(**inputs)
        except Exception as e:
            logger.warning(f"Failed to hydrate inputs to {input_model.__name__} for task {task_instance.__class__.__name__}: {e}")
            # Fall back to raw inputs

    # 4. Construct the TaskPayload
    return TaskPayload(
        task_id=raw_payload["task_id"],
        caller_id=raw_payload["caller_id"],
        inputs=inputs,
        asset=raw_payload.get("asset")
    )

@asynccontextmanager
async def manage_tasks(app_state: object, include_only: Optional[List[str]] = None) -> AsyncGenerator[None, None]:
    """
    Async context manager that owns the full lifecycle of all enabled tasks.
    It instantiates each task, enters its lifespan, and yields control.
    On exit, it ensures all lifespans are exited in reverse order.
    """
    # Make app_state available to subsequent get_task_instance() lookups.
    set_task_app_state(app_state)

    # Filter tasks if requested (primarily for tests)
    tasks_to_manage = _DYNASTORE_TASKS.items()
    if include_only is not None:
        target_names = {name.lower().replace("_", "-") for name in include_only}
        tasks_to_manage = [
            (name, config) for name, config in tasks_to_manage 
            if name.lower().replace("_", "-") in target_names
        ]

    # Sort tasks by priority (highest first)
    ordered_tasks = sorted(
        tasks_to_manage,
        key=lambda item: getattr(item[1].cls, "priority", 0),
        reverse=True,
    )

    async with AsyncExitStack() as stack:
        for name, config in ordered_tasks:
            load_component_dotenv(config.cls)
            try:
                # Instantiate
                import inspect
                sig = inspect.signature(config.cls.__init__)
                factory = cast(Any, config.cls)
                if "app_state" in sig.parameters:
                    instance = factory(app_state=app_state)
                else:
                    instance = factory()
                
                config.instance = instance
                
                # Register the task plugin in the central discovery registry
                from dynastore.tools.discovery import register_plugin
                register_plugin(instance)
                
                # Enter lifespan if implemented
                if hasattr(instance, "lifespan"):
                    logger.debug(f"Entering lifespan for task: {name}")
                    await stack.enter_async_context(instance.lifespan(app_state))
                    logger.debug(f"Lifespan for task '{name}' entered successfully.")
                else:
                    logger.debug(f"Task '{name}' does not implement lifespan. Skipping.")
            except Exception as e:
                logger.error(f"Failed to initialize task '{name}': {e}", exc_info=True)
                # If a task fails to initialize, we continue with others unless it's critical?
                # For now, we just log the error.

        yield
