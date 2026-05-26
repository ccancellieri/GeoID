from .dggs_service import DGGSService
from . import config  # noqa: F401  -- service-exposure plugin registration
from . import presets as _dggs_presets  # noqa: F401  -- preset registration side-effect

__all__ = ["DGGSService"]
