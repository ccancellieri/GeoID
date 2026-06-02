"""Durable task-capability registry: the cross-pod record of what each
deployed service can run, self-published via version-gated autodiscovery.

Pure data + IO; no routing policy (that lives in the routing config)
and no claim-path coupling (the resolver is wired into CapabilityMap elsewhere).
"""
from dynastore.modules.tasks.registry.model import (
    CapabilityRow,
    TASK_CAPABILITY_REGISTRY_TABLE,
    compute_publish_digest,
)

__all__ = [
    "CapabilityRow",
    "TASK_CAPABILITY_REGISTRY_TABLE",
    "compute_publish_digest",
]
