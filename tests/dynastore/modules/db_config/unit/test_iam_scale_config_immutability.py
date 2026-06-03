"""#1346 — the structural ``IamScaleConfig.usage_counter_hash_partitions`` knob
must be guarded against post-provision mutation in the Configuration Hub.

The field shapes the ``iam.usage_counters`` table at provisioning time, so it
is declared ``Immutable[int]`` and the config governs a platform-tier resource
(``_freeze_at="platform"``). The immutability gate therefore locks the field
once any catalog is provisioned (materialized), while leaving it editable on a
fresh platform (pre-provision tuning) and never constraining the runtime-tunable
``Mutable`` knobs.

These are DB-free: ``is_materialized`` is patched, exercising
``enforce_config_immutability`` directly — the same gate the
``PUT /configs/...`` write path runs.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.models.protocols.authorization import IamScaleConfig
from dynastore.modules.db_config import platform_config_service as svc
from dynastore.modules.db_config.exceptions import ImmutableConfigError


def _conn() -> MagicMock:
    return MagicMock(spec=AsyncConnection)


def test_freeze_at_is_platform_tier():
    """The structural intent is declared explicitly, not left to the default."""
    assert IamScaleConfig._freeze_at == "platform"


@pytest.mark.asyncio
async def test_usage_counter_hash_partitions_locked_post_provision():
    """Once the platform is materialized, changing the structural partition
    count is rejected — operator cannot silently repartition a live table."""
    current = IamScaleConfig(usage_counter_hash_partitions=1)
    incoming = IamScaleConfig(usage_counter_hash_partitions=4)
    with patch.object(svc, "is_materialized", new=AsyncMock(return_value=True)):
        with pytest.raises(ImmutableConfigError) as ei:
            await svc.enforce_config_immutability(current, incoming, conn=_conn())
    assert "usage_counter_hash_partitions" in str(ei.value)


@pytest.mark.asyncio
async def test_usage_counter_hash_partitions_editable_pre_provision():
    """On a fresh platform (nothing materialized) the structural field is still
    tunable, matching the operator mental model for pre-provision setup."""
    current = IamScaleConfig(usage_counter_hash_partitions=1)
    incoming = IamScaleConfig(usage_counter_hash_partitions=4)
    with patch.object(svc, "is_materialized", new=AsyncMock(return_value=False)):
        await svc.enforce_config_immutability(current, incoming, conn=_conn())  # no raise


@pytest.mark.asyncio
async def test_mutable_iam_scale_knobs_change_freely_when_materialized():
    """The runtime-tunable knobs are never frozen — a materialized platform can
    still retune TTLs/flags without touching the structural field."""
    current = IamScaleConfig(denylist_ttl_seconds=300, compiled_rule_cache_maxsize=1024)
    incoming = IamScaleConfig(denylist_ttl_seconds=600, compiled_rule_cache_maxsize=4096)
    with patch.object(svc, "is_materialized", new=AsyncMock(return_value=True)):
        await svc.enforce_config_immutability(current, incoming, conn=_conn())  # no raise


@pytest.mark.asyncio
async def test_first_write_imposes_no_constraint():
    """A first write (no stored row yet) is unconstrained even for the
    structural field — there is nothing to diverge from."""
    incoming = IamScaleConfig(usage_counter_hash_partitions=8)
    with patch.object(svc, "is_materialized", new=AsyncMock(return_value=True)):
        await svc.enforce_config_immutability(None, incoming, conn=_conn())  # no raise
