from .records_service import RecordsService
from . import config  # noqa: F401  -- service-exposure plugin registration
from . import presets as _records_presets  # noqa: F401  -- preset registration side-effect

__all__ = ["RecordsService"]
