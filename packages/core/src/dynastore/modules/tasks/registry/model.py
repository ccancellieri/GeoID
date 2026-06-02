"""Row shape, qualified table name, and the build-keyed publish digest."""
from __future__ import annotations

import hashlib
from typing import List, Optional

from pydantic import BaseModel

# The registry is a platform-wide observed-fact table; it lives in the same
# platform schema as the typed-store configs (created alongside the platform
# schema DDL). Keep this name in lockstep with the DDL constant.
TASK_CAPABILITY_REGISTRY_TABLE = "configs.task_capability_registry"


class CapabilityRow(BaseModel):
    """One (service, task_key) fact — what a deployed pod can run."""

    service: str
    task_key: str
    kind: str  # "process" | "task"
    required_capability: Optional[str] = None
    mandatory: bool = False
    affinity_tier: Optional[str] = None
    service_version: str
    service_commit: str
    version: str  # generic task version — build commit/version for now
    description: str = ""
    payload_schema: Optional[dict] = None


def compute_publish_digest(
    service_commit: str,
    service_version: str,
    rows: List[CapabilityRow],
) -> str:
    """Stable digest keyed on the build + the set of task keys this pod ships.

    Order-insensitive (task keys sorted). Structural PG writes happen only when
    this digest changes — i.e. ~once per deploy when the build commit rolls.
    """
    task_keys = ",".join(sorted(r.task_key for r in rows))
    payload = f"{service_commit}|{service_version}|{task_keys}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
