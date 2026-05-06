from .coverages_service import CoveragesService
from . import config  # noqa: F401  -- service-exposure plugin registration

__all__ = ["CoveragesService"]
