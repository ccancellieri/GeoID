"""Static-lint regression: every heavy task module MUST top-level-import
its hardest runtime dep.

Without this gate, removing ``@requires(Protocol)`` (geoid v0.5.86) means a
service whose SCOPE doesn't pull in the runtime dep would still load the task
class via the entry-point loader → CapabilityMap surfaces it → claim → runtime
crash. Hard imports cause the entry-point load to fail loud, so the task is
excluded from the dispatcher's claimable types instead.

Symmetric with the elasticsearch hard-import fix (54d1cdc) and the GCP fix
(3da0f10).
"""
from __future__ import annotations

import pathlib

import pytest

from tests._repo_paths import CORE_SRC as _CORE_SRC


# Each entry: relative path under src/ → list of acceptable top-level imports.
# At least one must appear as a top-level ``import X`` or ``from X import …``
# (i.e. not nested inside a function or try-block) for the test to pass.
HEAVY_TASK_GATES = {
    "dynastore/tasks/elasticsearch_indexer/tasks.py": ["opensearchpy"],
    "dynastore/tasks/gcp_provision/task.py": ["google.cloud.storage"],
    "dynastore/tasks/gcp/gcp_catalog_cleanup_task.py": ["google.cloud.storage"],
    "dynastore/tasks/dwh_join/dwh_join_export_task.py": ["google.cloud.bigquery"],
    "dynastore/tasks/export_features/export_features_task.py": ["shapely"],
    "dynastore/tasks/gdal/gdalinfo_task.py": ["osgeo"],
    "dynastore/tasks/ingestion/ingestion_task.py": ["geopandas", "fiona", "pyogrio"],
    "dynastore/tasks/tiles_preseed/task.py": ["morecantile", "shapely"],
}


SRC_ROOT = _CORE_SRC.parent


def _top_level_imports(path: pathlib.Path) -> list[str]:
    """Return module-or-package paths used by top-level ``import`` /
    ``from … import`` statements (anything indented is skipped — that's a
    function-local lazy import, which doesn't gate entry-point load)."""
    found: list[str] = []
    for raw in path.read_text().splitlines():
        # Indentation = nested = lazy import, doesn't gate entry-point load.
        if raw.startswith((" ", "\t")):
            continue
        # Strip trailing comments so ``import x  # noqa`` is parsed as ``x``.
        line = raw.split("#", 1)[0].strip()
        if line.startswith("import "):
            # ``import a.b.c [as d]``  →  capture ``a.b.c``
            mod = line[len("import "):].split(" as ")[0].split(",")[0].strip()
            found.append(mod)
        elif line.startswith("from "):
            # ``from a.b.c import x``  →  capture ``a.b.c``
            mod = line[len("from "):].split(" import ")[0].strip()
            found.append(mod)
    return found


@pytest.mark.parametrize(
    "rel_path,acceptable", list(HEAVY_TASK_GATES.items()),
)
def test_heavy_task_has_hard_runtime_import(rel_path: str, acceptable: list[str]):
    """Each heavy task module must top-level-import at least one acceptable
    runtime dep (so wrong-SCOPE services skip the entry-point)."""
    path = SRC_ROOT / rel_path
    assert path.exists(), f"missing source file: {path}"
    top = _top_level_imports(path)

    # Match by prefix so ``google.cloud.storage`` accepts ``google.cloud.storage``,
    # ``google.cloud.storage.client``, etc.
    def _matches(mod: str, gate: str) -> bool:
        return mod == gate or mod.startswith(gate + ".")

    matched = [g for g in acceptable if any(_matches(m, g) for m in top)]
    assert matched, (
        f"{rel_path}: no top-level import of any acceptable runtime dep "
        f"({acceptable!r}). Top-level imports found: {sorted(set(top))}. "
        f"Without one of these, a wrong-SCOPE service would claim this task "
        f"and crash at run() time. See modules/elasticsearch/module.py for "
        f"the rationale of the hard-import gate."
    )
