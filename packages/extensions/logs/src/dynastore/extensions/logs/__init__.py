"""
Thin wrapper that re-exports the catalog module's log_manager.
This maintains backward compatibility for code that imports from extensions/logs.
The actual implementation lives in modules/catalog/log_manager.py to respect
the Three Pillars architecture (modules don't import extensions).
"""
from dynastore.modules.catalog.log_manager import (
    log_event,
    log_info,
    log_warning,
    log_error,
    LogEntryCreate,
    LOG_SERVICE
)

__all__ = ['log_event', 'log_info', 'log_warning', 'log_error', 'LogEntryCreate', 'LOG_SERVICE']

from . import config  # noqa: F401  -- service-exposure plugin registration
from . import presets as _logs_presets  # noqa: F401  -- preset registration side-effect
