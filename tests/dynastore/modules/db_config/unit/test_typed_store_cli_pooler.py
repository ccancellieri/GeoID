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

from __future__ import annotations

from unittest.mock import patch

from dynastore.modules.db_config.typed_store import cli


def test_schema_audit_engine_uses_pooler_safe_connect_args():
    with patch(
        "dynastore.modules.db_config.typed_store.cli.create_async_engine"
    ) as mock_create_async_engine:
        cli._engine("postgresql://user:pass@db:5432/gis")

    _args, kwargs = mock_create_async_engine.call_args
    connect_args = kwargs["connect_args"]
    assert connect_args["prepared_statement_cache_size"] == 0
    assert connect_args["statement_cache_size"] == 0
    assert callable(connect_args["prepared_statement_name_func"])
