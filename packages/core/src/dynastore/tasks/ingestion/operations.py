#    Copyright 2025 FAO
# 
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
# 
#        http://www.apache.org/licenses/LICENSE-2.0
# 
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
# 
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar
from pydantic import BaseModel
from dynastore.modules.db_config.query_executor import DbEngine
import logging

logger = logging.getLogger(__name__)

T_CONFIG = TypeVar("T_CONFIG", bound=BaseModel)

class IngestionOperationInterface(Generic[T_CONFIG], ABC):
    """
    Abstract base class defining the interface for all ingestion operations.
    Operations can be run before or after the ingestion process.
    """
    def __init__(self, engine: DbEngine, task_id: str, task_request: Any, catalog_config: Any = None, ingestion_config: Any = None, config: Optional[T_CONFIG] = None, **kwargs):
        """
        Initializes the operation with common context.
        :param engine: The database engine for any DB operations.
        :param task_id: The unique ID of the running task.
        :param task_request: The full request model for the ingestion task (TaskIngestionRequest).
        :param catalog_config: The physical configuration for the collection (ItemsPostgresqlDriverConfig).
        :param ingestion_config: The ingestion logic configuration (IngestionPluginConfig).
        :param config: The validated Pydantic model for this operation's specific configuration.
        """
        self.engine = engine
        self.task_id = task_id
        self.task_request = task_request
        self.catalog_config = catalog_config
        self.ingestion_config = ingestion_config
        self.config = config

    @abstractmethod
    async def pre_op(self, catalog: Any, collection: Any, asset: Any) -> Any:
        """
        Executes before the ingestion process.
        Should return the updated asset if modified, or the original asset.
        """
        pass

    @abstractmethod
    async def post_op(self, catalog: Any, collection: Any, asset: Any, status: str, error_message: Optional[str] = None):
        """
        Executes after the ingestion process.
        """
        pass

# --- Ingestion Operation Registry ---
_ingestion_operation_registry: Dict[str, Type[IngestionOperationInterface]] = {}

def ingestion_operation(cls: Type[IngestionOperationInterface]) -> Type[IngestionOperationInterface]:
    """A class decorator to register a class as an available ingestion operation."""
    op_name = cls.__name__
    if op_name in _ingestion_operation_registry:
        raise TypeError(f"Operation with name '{op_name}' is already registered.")
    _ingestion_operation_registry[op_name] = cls
    return cls

def _get_config_model_from_operation(op_class: Type[IngestionOperationInterface]) -> Optional[Type[BaseModel]]:
    """
    Extracts the configuration model class from an operation implementation's 
    generic base class, if one is specified.
    """
    from typing import get_args, get_origin
    for base in getattr(op_class, "__orig_bases__", []):
        if get_origin(base) is IngestionOperationInterface:
            args = get_args(base)
            if args and issubclass(args[0], BaseModel):
                return args[0]
    return None

def initialize_operations(
    engine: DbEngine,
    task_id: str,
    task_request: Any,
    ops_config: Optional[Dict[str, Dict[str, Any]]] = None,
    catalog_config: Any = None,
    ingestion_config: Any = None
) -> List[IngestionOperationInterface]:
    """
    Initialize operations for a task based on its configuration.
    
    Args:
        engine: Database engine/engine manager
        task_id: Unique task ID
        task_request: Full request object for the task
        ops_config: Optional operations configuration dict (from request.pre_operations or post_operations)
        
    Returns:
        List of initialized operation instances
    """
    operations: List[IngestionOperationInterface] = []
    if not ops_config:
        return operations

    for op_name, config_dict in ops_config.items():
        if op_name in _ingestion_operation_registry:
            op_class = _ingestion_operation_registry[op_name]
            config_model_class = _get_config_model_from_operation(op_class)
            
            config_obj = None
            if config_model_class and config_dict:
                try:
                    config_obj = config_model_class(**config_dict)
                except Exception as e:
                    logger.warning(f"Task '{task_id}': Failed to validate config for operation '{op_name}': {e}")
            
            operations.append(op_class(
                engine=engine, 
                task_id=task_id, 
                task_request=task_request, 
                catalog_config=catalog_config,
                ingestion_config=ingestion_config,
                config=config_obj
            ))
            logger.info(f"Task '{task_id}': Operation '{op_name}' initialized.")
        else:
            logger.warning(f"Task '{task_id}': Operation '{op_name}' not found in registry.")

    return operations

async def run_pre_operations(operations: List[IngestionOperationInterface], catalog: Any, collection: Any, asset: Any) -> Any:
    """
    Run pre-operations in sequence, passing the asset to the next operation.
    """
    current_asset = asset
    for op in operations:
        op_name = op.__class__.__name__
        logger.info(f"Running pre-operation: {op_name}")
        try:
            result = await op.pre_op(catalog, collection, current_asset)
            if result is not None:
                current_asset = result
            logger.info(f"Pre-operation {op_name} completed successfully.")
        except Exception as e:
            logger.error(f"Pre-operation {op_name} failed: {e}")
            raise e
    return current_asset

async def run_post_operations(operations: List[IngestionOperationInterface], catalog: Any, collection: Any, asset: Any, status: str, error_message: Optional[str] = None):
    """
    Run post-operations.
    """
    import asyncio
    if not operations:
        return
    await asyncio.gather(*(op.post_op(catalog, collection, asset, status, error_message) for op in operations))
