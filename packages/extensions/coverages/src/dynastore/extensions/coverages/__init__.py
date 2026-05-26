from .coverages_service import CoveragesService
from . import config  # noqa: F401  -- service-exposure plugin registration
from . import presets as _coverages_presets  # noqa: F401  -- preset registration side-effect

__all__ = ["CoveragesService"]
