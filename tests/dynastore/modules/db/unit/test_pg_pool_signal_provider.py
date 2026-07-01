#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""``PgPoolSignalProvider`` — the PostgreSQL-pool half of the #2333 autoscaling
blind spot fix. Before this provider, the only instance-scope signal feeding
the scaling control loop was ``DuckDbPoolSignalProvider``, which never
reflects PG pool contention (the actual bottleneck the dev load test hit).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from dynastore.modules.db.db_service import PgPoolSignalProvider


def _fake_engine(checkedout: int):
    pool = MagicMock()
    pool.checkedout.return_value = checkedout
    return SimpleNamespace(pool=pool)


def _fake_db_config(pool_min_size: int, pool_max_overflow: int):
    return SimpleNamespace(
        pool_min_size=pool_min_size, pool_max_overflow=pool_max_overflow,
    )


class TestPgPoolSignalProvider:
    def test_reports_instance_scope_pg_pool_saturation(self):
        engine = _fake_engine(checkedout=5)
        db_config = _fake_db_config(pool_min_size=5, pool_max_overflow=5)  # capacity 10

        provider = PgPoolSignalProvider(engine, db_config)
        signals = provider.scaling_signals()

        assert len(signals) == 1
        signal = signals[0]
        assert signal.source == "pg_pool"
        assert signal.metric == "pool_saturation"
        assert signal.scope == "instance"
        assert signal.value == 0.5

    def test_saturation_clamped_to_one_when_overflow_exceeds_capacity(self):
        """A transient overshoot (checkedout briefly above capacity during a
        pool resize race) must never produce an out-of-range signal value —
        ``ScalingSignal.value`` is constrained to [0, 1]."""
        engine = _fake_engine(checkedout=25)
        db_config = _fake_db_config(pool_min_size=5, pool_max_overflow=5)

        provider = PgPoolSignalProvider(engine, db_config)
        signals = provider.scaling_signals()

        assert signals[0].value == 1.0

    def test_empty_when_capacity_is_zero(self):
        engine = _fake_engine(checkedout=0)
        db_config = _fake_db_config(pool_min_size=0, pool_max_overflow=0)

        provider = PgPoolSignalProvider(engine, db_config)
        assert provider.scaling_signals() == []

    def test_empty_when_pool_has_no_checkedout_method(self):
        """Defensive: an unexpected pool implementation must degrade to no
        signal rather than raise (mirrors ``DuckDbPoolSignalProvider``'s
        never-raise contract from ``ScalingSignalProtocol``)."""
        engine = SimpleNamespace(pool=object())
        db_config = _fake_db_config(pool_min_size=5, pool_max_overflow=5)

        provider = PgPoolSignalProvider(engine, db_config)
        assert provider.scaling_signals() == []

    def test_empty_when_checkedout_raises(self):
        pool = MagicMock()
        pool.checkedout.side_effect = RuntimeError("boom")
        engine = SimpleNamespace(pool=pool)
        db_config = _fake_db_config(pool_min_size=5, pool_max_overflow=5)

        provider = PgPoolSignalProvider(engine, db_config)
        assert provider.scaling_signals() == []
