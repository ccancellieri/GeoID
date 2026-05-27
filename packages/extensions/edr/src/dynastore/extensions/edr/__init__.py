from .edr_service import EDRService
from . import config  # noqa: F401  -- service-exposure plugin registration
from . import presets as _edr_presets  # noqa: F401  -- preset registration side-effect

__all__ = ["EDRService"]
