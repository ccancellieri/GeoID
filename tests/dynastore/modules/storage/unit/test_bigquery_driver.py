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

import pytest


class TestDriverMeta:
    """Driver class name / priority / capabilities / read-flavour hints
    are pinned once for all drivers in ``test_driver_meta_contract.py``
    (see that table's ``ItemsBigQueryDriver`` row for the WRITE/reporter_mode
    note)."""

    def test_preferred_for_features(self):
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
        d = ItemsBigQueryDriver()
        assert "features" in d.preferred_for

    def test_is_available_false_when_no_bq_service(self, monkeypatch):
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
        import dynastore.modules.storage.drivers.bigquery as mod
        monkeypatch.setattr(mod, "_get_bq_service", lambda: None)
        d = ItemsBigQueryDriver()
        assert d.is_available() is False


class TestReadEntities:
    @pytest.mark.asyncio
    async def test_read_entities_requires_fully_qualified_target(self, monkeypatch):
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, ItemsBigQueryDriverConfig,
        )
        from unittest.mock import AsyncMock

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=ItemsBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p"),
            )),
        )
        with pytest.raises(ValueError):
            async for _ in d.read_entities("cat", "col"):
                pass

    @pytest.mark.asyncio
    async def test_read_entities_streams_via_bq_service(self, monkeypatch):
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, ItemsBigQueryDriverConfig,
        )
        from unittest.mock import AsyncMock
        import dynastore.modules.storage.drivers.bigquery as mod

        fake_service = type("S", (), {})()
        fake_service.execute_query = AsyncMock(side_effect=[
            [{"id": "a"}, {"id": "b"}],
            [],
        ])
        monkeypatch.setattr(mod, "_get_bq_service", lambda: fake_service)

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=ItemsBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
                page_size=10,
            )),
        )

        feats = [f async for f in d.read_entities("cat", "col", limit=10)]
        assert [f.id for f in feats] == ["a", "b"]
        assert fake_service.execute_query.called


class TestCountAndAggregate:
    @pytest.mark.asyncio
    async def test_count_entities_runs_count_star(self, monkeypatch):
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, ItemsBigQueryDriverConfig,
        )
        from unittest.mock import AsyncMock
        import dynastore.modules.storage.drivers.bigquery as mod

        fake = type("S", (), {})()
        fake.execute_query = AsyncMock(return_value=[{"f0_": 42}])
        monkeypatch.setattr(mod, "_get_bq_service", lambda: fake)

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=ItemsBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
            )),
        )
        assert await d.count_entities("cat", "col") == 42
        called_sql = fake.execute_query.call_args[0][0]
        assert "COUNT(*)" in called_sql.upper()

    @pytest.mark.asyncio
    async def test_aggregate_sum(self, monkeypatch):
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, ItemsBigQueryDriverConfig,
        )
        from unittest.mock import AsyncMock
        import dynastore.modules.storage.drivers.bigquery as mod

        fake = type("S", (), {})()
        fake.execute_query = AsyncMock(return_value=[{"f0_": 100.0}])
        monkeypatch.setattr(mod, "_get_bq_service", lambda: fake)

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=ItemsBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
            )),
        )
        result = await d.aggregate("cat", "col", aggregation_type="sum", field="value")
        assert result == 100.0

    @pytest.mark.asyncio
    async def test_aggregate_rejects_unsafe_field(self, monkeypatch):
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, ItemsBigQueryDriverConfig,
        )
        from unittest.mock import AsyncMock
        import dynastore.modules.storage.drivers.bigquery as mod

        fake = type("S", (), {})()
        fake.execute_query = AsyncMock(return_value=[{"f0_": 0}])
        monkeypatch.setattr(mod, "_get_bq_service", lambda: fake)

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=ItemsBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
            )),
        )
        with pytest.raises(ValueError):
            await d.aggregate("cat", "col", aggregation_type="sum", field="x; DROP TABLE")


class TestIntrospect:
    @pytest.mark.asyncio
    async def test_introspect_schema_translates_bq_fields_to_field_definitions(
        self, monkeypatch,
    ):
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, ItemsBigQueryDriverConfig,
        )
        from unittest.mock import AsyncMock, MagicMock
        import dynastore.modules.storage.drivers.bigquery as mod

        fake_schema = []
        for name, ftype, mode in [
            ("id",    "STRING",    "REQUIRED"),
            ("value", "INTEGER",   "NULLABLE"),
            ("geom",  "GEOGRAPHY", "NULLABLE"),
        ]:
            f = MagicMock()
            f.name = name
            f.field_type = ftype
            f.mode = mode
            fake_schema.append(f)
        fake_table = MagicMock()
        fake_table.schema = fake_schema

        def fake_client_factory(project_id, **kwargs):
            c = MagicMock()
            c.get_table = MagicMock(return_value=fake_table)
            return c
        monkeypatch.setattr(mod, "_make_bq_client", fake_client_factory)

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=ItemsBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
            )),
        )
        fields = await d.introspect_schema("cat", "col")
        names = [f.name for f in fields]
        assert names == ["id", "value", "geom"]


class TestCredentialResolution:
    def test_make_bq_client_uses_cloud_identity_when_creds_empty(self, monkeypatch):
        """Back-compat: no credentials -> CloudIdentityProtocol path."""
        from unittest.mock import MagicMock

        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryCredentials,
        )

        fake_identity = MagicMock()
        fake_identity.get_credentials_object.return_value = "cloud-creds-obj"
        fake_bq_client = MagicMock()

        monkeypatch.setattr(mod, "get_protocol", lambda p: fake_identity)
        monkeypatch.setattr(
            "google.cloud.bigquery.Client", fake_bq_client, raising=False,
        )

        mod._make_bq_client("proj", credentials=BigQueryCredentials())
        fake_bq_client.assert_called_once()
        _, kwargs = fake_bq_client.call_args
        assert kwargs["credentials"] == "cloud-creds-obj"

    def test_make_bq_client_uses_service_account_when_supplied(self, monkeypatch):
        """Secret-wrapped SA JSON -> service_account.Credentials.from_service_account_info."""
        import json
        from unittest.mock import MagicMock

        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryCredentials,
        )
        from dynastore.tools.secrets import Secret

        fake_sa_creds = MagicMock()
        fake_from_info = MagicMock(return_value=fake_sa_creds)

        # Avoid hitting CloudIdentity in this branch.
        monkeypatch.setattr(mod, "get_protocol", lambda p: None)

        import google.oauth2.service_account as sa
        monkeypatch.setattr(
            sa.Credentials, "from_service_account_info", fake_from_info,
        )

        fake_bq_client = MagicMock()
        monkeypatch.setattr("google.cloud.bigquery.Client", fake_bq_client)

        creds = BigQueryCredentials(
            service_account_json=Secret(json.dumps({"type": "service_account", "x": 1})),
        )
        mod._make_bq_client("proj", credentials=creds)

        fake_from_info.assert_called_once()
        info_arg = fake_from_info.call_args[0][0]
        assert info_arg == {"type": "service_account", "x": 1}
        _, client_kwargs = fake_bq_client.call_args
        assert client_kwargs["credentials"] is fake_sa_creds

    def test_make_bq_client_api_key_falls_back_to_cloud_identity(
        self, monkeypatch, caplog,
    ):
        """API key auth not wired to bigquery.Client -- warn and fall back."""
        import logging
        from unittest.mock import MagicMock

        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryCredentials,
        )
        from dynastore.tools.secrets import Secret

        fake_identity = MagicMock()
        fake_identity.get_credentials_object.return_value = "fallback-creds"
        monkeypatch.setattr(mod, "get_protocol", lambda p: fake_identity)

        fake_bq_client = MagicMock()
        monkeypatch.setattr("google.cloud.bigquery.Client", fake_bq_client)

        creds = BigQueryCredentials(api_key=Secret("key-123"))
        with caplog.at_level(logging.WARNING, logger=mod.__name__):
            mod._make_bq_client("proj", credentials=creds)
        assert any("api_key" in r.message for r in caplog.records)
        _, kwargs = fake_bq_client.call_args
        assert kwargs["credentials"] == "fallback-creds"


def test_driver_discovered_via_entry_point():
    from importlib.metadata import entry_points
    eps = [ep for ep in entry_points(group="dynastore.modules") if ep.name == "storage_bigquery"]
    assert len(eps) == 1
    assert "ItemsBigQueryDriver" in eps[0].value


# ---------------------------------------------------------------------------
# Phase 3 — reporter-mode WRITE path
# ---------------------------------------------------------------------------


class TestReporterShape:
    """Row-shape helpers are pure — no BQ client needed."""

    def _features(self):
        from dynastore.models.ogc import Feature
        return [
            Feature.model_validate({
                "type": "Feature",
                "id": "a",
                "geometry": None,
                "properties": {"name": "Alice", "ssn": "123-45-6789", "tier": 1},
            }),
            Feature.model_validate({
                "type": "Feature",
                "id": "b",
                "geometry": None,
                "properties": {"name": "Bob", "tier": 2},
            }),
        ]

    def test_rows_flat_default_excludes_payload(self):
        from dynastore.modules.storage.drivers.bigquery import _rows_flat

        rows = _rows_flat(
            self._features(),
            catalog_id="cat", collection_id="col",
            include_payload=False, exclude_fields=[],
        )
        assert len(rows) == 2
        assert {r["entity_id"] for r in rows} == {"a", "b"}
        for r in rows:
            assert r["catalog_id"] == "cat"
            assert r["collection_id"] == "col"
            assert "payload" not in r
            assert "ingested_at" in r

    def test_rows_flat_include_payload_strips_excluded(self):
        import json
        from dynastore.modules.storage.drivers.bigquery import _rows_flat

        rows = _rows_flat(
            self._features(),
            catalog_id="cat", collection_id="col",
            include_payload=True, exclude_fields=["ssn"],
        )
        assert len(rows) == 2
        # Alice's payload must not contain 'ssn' but must retain 'name'/'tier'
        alice = next(r for r in rows if r["entity_id"] == "a")
        payload = json.loads(alice["payload"])
        assert "ssn" not in payload
        assert payload == {"name": "Alice", "tier": 1}

    def test_rows_batch_summary_counts_and_bounds(self):
        from dynastore.modules.storage.drivers.bigquery import _rows_batch_summary

        rows = _rows_batch_summary(
            self._features(),
            catalog_id="cat", collection_id="col",
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["row_count"] == 2
        assert row["first_entity"] == "a"
        assert row["last_entity"] == "b"
        assert row["catalog_id"] == "cat"

    def test_rows_batch_summary_handles_empty_input(self):
        from dynastore.modules.storage.drivers.bigquery import _rows_batch_summary

        rows = _rows_batch_summary(
            [], catalog_id="cat", collection_id="col",
        )
        assert len(rows) == 1
        assert rows[0]["row_count"] == 0
        assert rows[0]["first_entity"] is None
        assert rows[0]["last_entity"] is None


class TestReporterWriteEntities:
    """Integration of reporter-mode + fake BigQueryService."""

    @pytest.mark.asyncio
    async def test_reporter_off_is_noop(self, monkeypatch):
        """Default reporter_mode='off' must NOT call BigQueryService."""
        from unittest.mock import AsyncMock
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, ItemsBigQueryDriverConfig,
        )
        from dynastore.models.ogc import Feature

        fake_service = AsyncMock()
        fake_service.insert_rows_json = AsyncMock(return_value=[])
        monkeypatch.setattr(mod, "_get_bq_service", lambda: fake_service)

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=ItemsBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
                # reporter_mode defaults to "off"
            )),
        )
        feat = Feature.model_validate({
            "type": "Feature", "id": "x", "geometry": None, "properties": {},
        })
        result = await d.write_entities("cat", "col", [feat])
        assert result == [feat]
        fake_service.insert_rows_json.assert_not_called()

    @pytest.mark.asyncio
    async def test_reporter_flat_calls_insert_rows_json(self, monkeypatch):
        from unittest.mock import AsyncMock
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, ItemsBigQueryDriverConfig,
        )
        from dynastore.models.ogc import Feature

        fake_service = AsyncMock()
        fake_service.insert_rows_json = AsyncMock(return_value=[])
        monkeypatch.setattr(mod, "_get_bq_service", lambda: fake_service)

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=ItemsBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
                reporter_mode="flat",
            )),
        )
        features = [
            Feature.model_validate({
                "type": "Feature", "id": str(i), "geometry": None, "properties": {"k": i},
            })
            for i in range(3)
        ]
        result = await d.write_entities("cat", "col", features)
        assert result == features

        fake_service.insert_rows_json.assert_called_once()
        args, kwargs = fake_service.insert_rows_json.call_args
        assert args[0] == "p.d.t"  # report_target.fqn()
        rows = args[1]
        assert len(rows) == 3
        assert {r["entity_id"] for r in rows} == {"0", "1", "2"}
        assert kwargs["project_id"] == "p"

    @pytest.mark.asyncio
    async def test_reporter_batch_summary_one_row(self, monkeypatch):
        from unittest.mock import AsyncMock
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, ItemsBigQueryDriverConfig,
        )
        from dynastore.models.ogc import Feature

        fake_service = AsyncMock()
        fake_service.insert_rows_json = AsyncMock(return_value=[])
        monkeypatch.setattr(mod, "_get_bq_service", lambda: fake_service)

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=ItemsBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
                reporter_mode="batch_summary",
            )),
        )
        features = [
            Feature.model_validate({
                "type": "Feature", "id": f"item-{i}", "geometry": None, "properties": {},
            })
            for i in range(5)
        ]
        await d.write_entities("cat", "col", features)

        fake_service.insert_rows_json.assert_called_once()
        rows = fake_service.insert_rows_json.call_args.args[1]
        assert len(rows) == 1
        assert rows[0]["row_count"] == 5
        assert rows[0]["first_entity"] == "item-0"
        assert rows[0]["last_entity"] == "item-4"

    @pytest.mark.asyncio
    async def test_reporter_bq_exception_is_warning_not_fatal(self, monkeypatch, caplog):
        """BQ streaming failures must surface as warnings and not raise."""
        import logging
        from unittest.mock import AsyncMock
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, ItemsBigQueryDriverConfig,
        )
        from dynastore.models.ogc import Feature

        fake_service = AsyncMock()
        fake_service.insert_rows_json = AsyncMock(
            side_effect=RuntimeError("BQ quota exceeded"),
        )
        monkeypatch.setattr(mod, "_get_bq_service", lambda: fake_service)

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=ItemsBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
                reporter_mode="flat",
            )),
        )
        feat = Feature.model_validate({
            "type": "Feature", "id": "x", "geometry": None, "properties": {},
        })
        with caplog.at_level(logging.WARNING, logger=mod.__name__):
            result = await d.write_entities("cat", "col", [feat])
        # Input features returned unchanged (contract preservation)
        assert result == [feat]
        assert any("BQ reporter WRITE failed" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_reporter_requires_report_target_fqn(self, monkeypatch, caplog):
        """Reporter mode without fully-qualified report_target logs a warning and skips."""
        import logging
        from unittest.mock import AsyncMock
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, ItemsBigQueryDriverConfig,
        )
        from dynastore.models.ogc import Feature

        fake_service = AsyncMock()
        fake_service.insert_rows_json = AsyncMock(return_value=[])
        monkeypatch.setattr(mod, "_get_bq_service", lambda: fake_service)

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=ItemsBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p"),  # only project, not FQN
                reporter_mode="flat",
            )),
        )
        feat = Feature.model_validate({
            "type": "Feature", "id": "x", "geometry": None, "properties": {},
        })
        with caplog.at_level(logging.WARNING, logger=mod.__name__):
            await d.write_entities("cat", "col", [feat])
        fake_service.insert_rows_json.assert_not_called()
        assert any("not fully qualified" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_reporter_partial_failure_logged(self, monkeypatch, caplog):
        """BQ partial-failure list is logged as a warning, not raised."""
        import logging
        from unittest.mock import AsyncMock
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryTarget, ItemsBigQueryDriverConfig,
        )
        from dynastore.models.ogc import Feature

        fake_service = AsyncMock()
        fake_service.insert_rows_json = AsyncMock(
            return_value=[{"index": 1, "errors": [{"reason": "invalid"}]}],
        )
        monkeypatch.setattr(mod, "_get_bq_service", lambda: fake_service)

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(
            d, "get_driver_config",
            AsyncMock(return_value=ItemsBigQueryDriverConfig(
                target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
                reporter_mode="flat",
            )),
        )
        features = [
            Feature.model_validate({
                "type": "Feature", "id": str(i), "geometry": None, "properties": {},
            })
            for i in range(2)
        ]
        with caplog.at_level(logging.WARNING, logger=mod.__name__):
            await d.write_entities("cat", "col", features)
        assert any("partial failure" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Phase 4e step 2 — credentials kwarg forwarding
# ---------------------------------------------------------------------------


def _cfg_with_creds(secret_value: str = "{}"):
    """Build a fully-qualified config carrying a Secret-wrapped SA JSON."""
    from dynastore.modules.storage.drivers.bigquery_models import (
        BigQueryCredentials,
        BigQueryTarget,
        ItemsBigQueryDriverConfig,
    )
    from dynastore.tools.secrets import Secret

    return ItemsBigQueryDriverConfig(
        target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        credentials=BigQueryCredentials(service_account_json=Secret(secret_value)),
    )


class TestDriverForwardsCredentials:
    """Driver call sites must forward cfg.credentials to BigQueryProtocol."""

    @pytest.mark.asyncio
    async def test_count_entities_forwards_credentials(self, monkeypatch):
        from unittest.mock import AsyncMock

        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver

        cfg = _cfg_with_creds()
        fake = type("S", (), {})()
        fake.execute_query = AsyncMock(return_value=[{"f0_": 7}])
        monkeypatch.setattr(mod, "_get_bq_service", lambda: fake)

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(d, "get_driver_config", AsyncMock(return_value=cfg))
        await d.count_entities("cat", "col")

        kwargs = fake.execute_query.call_args.kwargs
        assert kwargs["credentials"] is cfg.credentials

    @pytest.mark.asyncio
    async def test_aggregate_forwards_credentials(self, monkeypatch):
        from unittest.mock import AsyncMock

        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver

        cfg = _cfg_with_creds()
        fake = type("S", (), {})()
        fake.execute_query = AsyncMock(return_value=[{"f0_": 1.0}])
        monkeypatch.setattr(mod, "_get_bq_service", lambda: fake)

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(d, "get_driver_config", AsyncMock(return_value=cfg))
        await d.aggregate("cat", "col", aggregation_type="sum", field="value")

        kwargs = fake.execute_query.call_args.kwargs
        assert kwargs["credentials"] is cfg.credentials

    @pytest.mark.asyncio
    async def test_read_entities_forwards_credentials(self, monkeypatch):
        from unittest.mock import AsyncMock

        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver

        cfg = _cfg_with_creds()
        fake = type("S", (), {})()
        fake.execute_query = AsyncMock(side_effect=[[{"id": "x"}], []])
        monkeypatch.setattr(mod, "_get_bq_service", lambda: fake)

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(d, "get_driver_config", AsyncMock(return_value=cfg))
        async for _ in d.read_entities("cat", "col", limit=5):
            pass

        kwargs = fake.execute_query.call_args.kwargs
        assert kwargs["credentials"] is cfg.credentials

    @pytest.mark.asyncio
    async def test_reporter_write_forwards_credentials(self, monkeypatch):
        from unittest.mock import AsyncMock

        import dynastore.modules.storage.drivers.bigquery as mod
        from dynastore.models.ogc import Feature
        from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryCredentials,
            BigQueryTarget,
            ItemsBigQueryDriverConfig,
        )
        from dynastore.tools.secrets import Secret

        cfg = ItemsBigQueryDriverConfig(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
            credentials=BigQueryCredentials(service_account_json=Secret("{}")),
            reporter_mode="batch_summary",
        )
        fake = type("S", (), {})()
        fake.insert_rows_json = AsyncMock(return_value=[])
        monkeypatch.setattr(mod, "_get_bq_service", lambda: fake)

        d = ItemsBigQueryDriver()
        monkeypatch.setattr(d, "get_driver_config", AsyncMock(return_value=cfg))
        feat = Feature.model_validate({
            "type": "Feature", "id": "a", "geometry": None, "properties": {},
        })
        await d.write_entities("cat", "col", [feat])

        kwargs = fake.insert_rows_json.call_args.kwargs
        assert kwargs["credentials"] is cfg.credentials


class TestBigQueryServiceCredentialResolution:
    """BigQueryService.execute_query / insert_rows_json must route credential
    resolution through ``_make_bq_client`` (the single SA-reveal site)."""

    @pytest.mark.asyncio
    async def test_execute_query_uses_make_bq_client_with_credentials(
        self, monkeypatch,
    ):
        from unittest.mock import MagicMock

        import dynastore.modules.gcp.bigquery_service as svc_mod
        from dynastore.modules.gcp.bigquery_service import BigQueryService
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryCredentials,
        )
        from dynastore.tools.secrets import Secret

        creds = BigQueryCredentials(service_account_json=Secret("{}"))

        fake_query_job = MagicMock()
        fake_df = MagicMock()
        fake_df.to_dict.return_value = [{"a": 1}]
        fake_query_job.to_dataframe.return_value = fake_df
        fake_client = MagicMock()
        fake_client.query.return_value = fake_query_job

        captured = {}

        def fake_build(project_id, credentials):
            captured["project_id"] = project_id
            captured["credentials"] = credentials
            return fake_client

        monkeypatch.setattr(svc_mod, "_build_client", fake_build)

        rows = await BigQueryService().execute_query(
            "SELECT 1", "proj", credentials=creds,
        )
        assert rows == [{"a": 1}]
        assert captured == {"project_id": "proj", "credentials": creds}
        fake_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_query_without_credentials_passes_none(self, monkeypatch):
        from unittest.mock import MagicMock

        import dynastore.modules.gcp.bigquery_service as svc_mod
        from dynastore.modules.gcp.bigquery_service import BigQueryService

        fake_df = MagicMock()
        fake_df.to_dict.return_value = []
        fake_client = MagicMock()
        fake_client.query.return_value.to_dataframe.return_value = fake_df

        captured = {}

        def fake_build(project_id, credentials):
            captured["credentials"] = credentials
            return fake_client

        monkeypatch.setattr(svc_mod, "_build_client", fake_build)

        await BigQueryService().execute_query("SELECT 1", "proj")
        assert captured["credentials"] is None

    @pytest.mark.asyncio
    async def test_insert_rows_json_uses_make_bq_client_with_credentials(
        self, monkeypatch,
    ):
        from unittest.mock import MagicMock

        import dynastore.modules.gcp.bigquery_service as svc_mod
        from dynastore.modules.gcp.bigquery_service import BigQueryService
        from dynastore.modules.storage.drivers.bigquery_models import (
            BigQueryCredentials,
        )
        from dynastore.tools.secrets import Secret

        creds = BigQueryCredentials(service_account_json=Secret("{}"))

        fake_client = MagicMock()
        fake_client.insert_rows_json.return_value = []

        captured = {}

        def fake_build(project_id, credentials):
            captured["credentials"] = credentials
            return fake_client

        monkeypatch.setattr(svc_mod, "_build_client", fake_build)

        result = await BigQueryService().insert_rows_json(
            "p.d.t", [{"x": 1}], project_id="proj", credentials=creds,
        )
        assert result == []
        assert captured["credentials"] is creds
        fake_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_insert_rows_json_empty_input_skips_client_build(
        self, monkeypatch,
    ):
        """Empty input is a fast no-op — no client built, no creds resolved."""
        import dynastore.modules.gcp.bigquery_service as svc_mod
        from dynastore.modules.gcp.bigquery_service import BigQueryService

        called = {"n": 0}

        def fake_build(project_id, credentials):
            called["n"] += 1
            raise AssertionError("should not build a client for empty rows")

        monkeypatch.setattr(svc_mod, "_build_client", fake_build)

        result = await BigQueryService().insert_rows_json(
            "p.d.t", [], project_id="proj",
        )
        assert result == []
        assert called["n"] == 0


class TestBigQueryServiceThreadOffload:
    """execute_query / insert_rows_json must not block the event loop — the
    synchronous google-cloud-bigquery client work (build + query/insert +
    close) runs via ``asyncio.to_thread`` (#2960)."""

    @pytest.mark.asyncio
    async def test_execute_query_runs_via_to_thread(self, monkeypatch):
        import asyncio
        from unittest.mock import MagicMock

        import dynastore.modules.gcp.bigquery_service as svc_mod
        from dynastore.modules.gcp.bigquery_service import BigQueryService

        fake_df = MagicMock()
        fake_df.to_dict.return_value = [{"a": 1}]
        fake_client = MagicMock()
        fake_client.query.return_value.to_dataframe.return_value = fake_df
        monkeypatch.setattr(
            svc_mod, "_build_client",
            lambda project_id, credentials: fake_client,
        )

        real_to_thread = asyncio.to_thread
        calls = []

        async def spy_to_thread(func, *args, **kwargs):
            calls.append(func)
            return await real_to_thread(func, *args, **kwargs)

        monkeypatch.setattr(svc_mod.asyncio, "to_thread", spy_to_thread)

        rows = await BigQueryService().execute_query("SELECT 1", "proj")

        assert rows == [{"a": 1}]
        assert len(calls) == 1
        fake_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_insert_rows_json_runs_via_to_thread(self, monkeypatch):
        import asyncio
        from unittest.mock import MagicMock

        import dynastore.modules.gcp.bigquery_service as svc_mod
        from dynastore.modules.gcp.bigquery_service import BigQueryService

        fake_client = MagicMock()
        fake_client.insert_rows_json.return_value = []
        monkeypatch.setattr(
            svc_mod, "_build_client",
            lambda project_id, credentials: fake_client,
        )

        real_to_thread = asyncio.to_thread
        calls = []

        async def spy_to_thread(func, *args, **kwargs):
            calls.append(func)
            return await real_to_thread(func, *args, **kwargs)

        monkeypatch.setattr(svc_mod.asyncio, "to_thread", spy_to_thread)

        result = await BigQueryService().insert_rows_json(
            "p.d.t", [{"x": 1}], project_id="proj",
        )

        assert result == []
        assert len(calls) == 1
        fake_client.close.assert_called_once()
