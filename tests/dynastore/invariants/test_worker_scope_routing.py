"""B6-style invariant — for every worker_task_* SCOPE in pyproject.toml,
every WRITE/INDEX driver in the default routing config must be either:
  (a) installable from the SCOPE's transitive extras, or
  (b) configured with on_failure=OUTBOX.

This catches drift like 'ES driver in routing but worker_task_ingestion
doesn't pull module_elasticsearch' before it ships.
"""
from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
)

PYPROJECT = Path(__file__).resolve().parents[3] / "pyproject.toml"


# Map driver_id (snake_case) to the extras alias that brings its
# runtime dep. Update when new drivers ship.
DRIVER_TO_EXTRA: dict[str, str] = {
    "items_postgresql_driver": "module_storage_postgresql",
    "items_duckdb_driver": "module_storage_duckdb",
    "items_iceberg_driver": "module_storage_iceberg",
    "items_elasticsearch_driver": "module_storage_elasticsearch",
    "items_elasticsearch_private_driver": "module_storage_elasticsearch_private",
    "items_big_query_driver": "module_storage_bigquery",
}


def _load_extras() -> dict:
    return tomllib.loads(PYPROJECT.read_text())["project"]["optional-dependencies"]


def _expand_aliases(
    extras: dict, name: str, seen: set[str] | None = None,
) -> set[str]:
    """Recursively expand an extras alias to its full set of transitively-
    referenced alias names (including itself)."""
    seen = seen or set()
    if name in seen or name not in extras:
        # If the name isn't an extras alias (e.g. a leaf package or unknown),
        # still include it so callers can match against `DRIVER_TO_EXTRA`
        # values that may be referenced as bare extras names.
        return {name}
    seen.add(name)
    out: set[str] = {name}
    for token in extras.get(name, []):
        if token.startswith("dynastore["):
            inner = token[len("dynastore["):-1]
            for sub in inner.split(","):
                out |= _expand_aliases(extras, sub.strip(), seen)
    return out


def _worker_scope_names() -> list[str]:
    extras = _load_extras()
    return [k for k in extras if k.startswith("worker_task_")]


# Parametrize over every worker_task_* SCOPE found at module import time.
@pytest.mark.parametrize("scope", _worker_scope_names())
def test_worker_scope_covers_routing_or_uses_outbox(scope: str) -> None:
    extras = _load_extras()
    aliases = _expand_aliases(extras, scope)

    cfg = ItemsRoutingConfig()
    write_entries = cfg.operations.get(Operation.WRITE, [])

    failures: list[str] = []
    for entry in write_entries:
        needed = DRIVER_TO_EXTRA.get(entry.driver_id)
        if needed is None:
            # Unknown driver_id — skip; if a new driver was added, the
            # mapping should be extended in this file.
            continue
        if needed in aliases:
            continue
        if entry.on_failure == FailurePolicy.OUTBOX:
            continue
        failures.append(
            f"  {entry.driver_id} requires '{needed}' which is NOT in "
            f"{scope}'s transitive extras AND on_failure="
            f"{entry.on_failure.name} (not OUTBOX)"
        )

    assert not failures, (
        f"\nSCOPE '{scope}' has unmet routing deps:\n" + "\n".join(failures)
    )
