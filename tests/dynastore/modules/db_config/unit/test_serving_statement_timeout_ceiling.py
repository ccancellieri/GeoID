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

"""Unit tests for the shared serving engine's statement_timeout ceiling (#2898).

``DBConfig.statement_timeout`` resolves to ``"0"`` (disabled, dev default) or
a configured value like ``"90s"`` (prod) -- both above the 60s load
balancer/Cloud Run deadline, so a stuck query on the shared serving engine
holds its connection past the client's timeout instead of being cancelled and
reclaimed server-side. ``clamp_serving_statement_timeout`` clamps the
effective value below a configurable ceiling without touching the resolved
``DB_STATEMENT_TIMEOUT`` itself.
"""

from __future__ import annotations

import pytest

from dynastore.modules.db_config.db_config import DBConfig
from dynastore.modules.db_config.db_timeout_config import (
    clamp_serving_statement_timeout,
)


class TestClampServingStatementTimeout:
    def test_disabled_clamps_to_ceiling(self):
        assert clamp_serving_statement_timeout("0", 55) == "55s"

    def test_above_ceiling_clamps_to_ceiling(self):
        assert clamp_serving_statement_timeout("90s", 55) == "55s"

    def test_at_or_below_ceiling_unchanged(self):
        assert clamp_serving_statement_timeout("30s", 55) == "30s"

    def test_minutes_suffix_above_ceiling_clamps(self):
        assert clamp_serving_statement_timeout("2min", 55) == "55s"

    def test_bare_integer_below_ceiling_unchanged(self):
        assert clamp_serving_statement_timeout("45", 55) == "45s"

    def test_unparseable_clamps_to_ceiling(self):
        assert clamp_serving_statement_timeout("garbage", 55) == "55s"

    @pytest.mark.parametrize("bad_value", ["", "-30s", "-5", "0s", "0min"])
    def test_non_positive_or_empty_clamps_to_ceiling(self, bad_value):
        assert clamp_serving_statement_timeout(bad_value, 55) == "55s"


class TestServingStatementTimeoutCeilingDefault:
    def test_default_is_55(self):
        assert DBConfig().serving_statement_timeout_ceiling_seconds == 55

    def test_default_is_below_the_60s_lb_timeout(self):
        assert DBConfig().serving_statement_timeout_ceiling_seconds < 60
