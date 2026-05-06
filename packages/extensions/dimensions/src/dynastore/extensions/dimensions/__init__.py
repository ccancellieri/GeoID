from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook
from . import config  # noqa: F401  -- service-exposure plugin registration

_examples_dir = Path(__file__).parent / "examples"

if _examples_dir.is_dir():
    for ipynb in sorted(_examples_dir.glob("*.ipynb")):
        register_platform_notebook(
            notebook_id=ipynb.stem,
            registered_by="dimensions_extension",
            notebook_path=ipynb,
        )
