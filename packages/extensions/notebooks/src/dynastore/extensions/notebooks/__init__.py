from .notebooks_extension import NotebooksExtension
from . import tenant_initialization  # noqa: F401  -- lifecycle registration side-effect
from . import config  # noqa: F401  -- service-exposure plugin registration
from . import presets as _notebooks_presets  # noqa: F401  -- preset registration side-effect

__all__ = ["NotebooksExtension"]
