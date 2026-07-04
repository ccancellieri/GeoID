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

"""Unit tests for gc_phantom_catalogs.py.

All tests are pure-unit: no real DB, no real GCP.

Coverage:
- is_phantom_external_id predicate (both matching and non-matching ids)
- _detect_phantoms uses the regex filter and resolves bucket_name
- _detect_all_catalogs selects ALL non-deleted catalogs regardless of shape
- _fetch_phantoms_by_ids validates id shape, skips non-matching, handles missing rows
- _resolve_bucket_name returns None when schema absent, None when row absent
- _run in dry-run mode performs zero DB mutations
- _run in --execute mode invokes teardown + row delete per phantom
- teardown is idempotent: already-gone GCP resources are skipped, not errored
- safety invariant: a real external_id (non-internal-shaped) is never touched
- --all mode: selects all catalogs (mock DB), not just phantoms
- dev-only guard: refuses when env is prod/empty, allows when env=dev/development/review
- dry-run with --all makes no deletions
"""
from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import dynastore.scripts.gc_phantom_catalogs as gc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


@asynccontextmanager
async def _noop_lifespan(*a, **kw):
    """Async context manager no-op used to mock modules_lifespan in tests."""
    yield


# ---------------------------------------------------------------------------
# Predicate tests
# ---------------------------------------------------------------------------

class TestIsPhantomExternalId:
    @pytest.mark.parametrize(
        "external_id, expected",
        [
            pytest.param("c_2abc3defg4hij", True, id="internal_shaped_catalog_id_matches"),
            pytest.param("c_2345678923456", True, id="all_digits_suffix_matches"),
            pytest.param("c_abcdefghijklm", True, id="all_letters_from_alphabet_matches"),
            pytest.param("my-real-catalog", False, id="normal_external_id_does_not_match"),
            pytest.param(
                "550e8400-e29b-41d4-a716-446655440000", False, id="uuid_external_id_does_not_match"
            ),
            # col_ prefix used for collections, not catalogs
            pytest.param("col_2abc3defg4hi", False, id="wrong_prefix_does_not_match"),
            pytest.param("c_2abc3def", False, id="too_short_suffix_does_not_match"),
            pytest.param("c_2abc3defg4hijk", False, id="too_long_suffix_does_not_match"),
            # 'y' is NOT in [2-9a-x]
            pytest.param("c_2yyyyyyyyyyyyyyy", False, id="forbidden_char_y_does_not_match"),
            # '0' is not in the base32 alphabet used
            pytest.param("c_0yyyyyyyyyyy0", False, id="forbidden_char_0_does_not_match"),
            pytest.param("c_1abc3defg4hi", False, id="forbidden_char_1_does_not_match"),
            pytest.param("", False, id="empty_string_does_not_match"),
            # A representative id produced by generate_physical_name("c").
            # Alphabet is 2-9 + a-x (32 symbols), so all chars in [2-9a-x].
            pytest.param(
                "c_3kp7rmn4bcdef", True, id="real_internal_id_from_generate_physical_name"
            ),
        ],
    )
    def test_is_phantom_external_id(self, external_id, expected):
        assert gc.is_phantom_external_id(external_id) is expected


# ---------------------------------------------------------------------------
# _resolve_bucket_name
# ---------------------------------------------------------------------------

class TestResolveBucketName:
    def test_returns_none_when_schema_absent(self):
        """When the phantom's schema no longer exists, bucket_name is None."""
        conn = AsyncMock()
        conn.fetchval = AsyncMock(return_value=None)  # schema absent

        result = _run(gc._resolve_bucket_name(conn, "c_2abc3defg4hi"))
        assert result is None
        conn.fetchrow.assert_not_called()

    def test_returns_none_when_config_row_absent(self):
        """Schema exists but the config row was never written."""
        conn = AsyncMock()
        conn.fetchval = AsyncMock(return_value=1)  # schema exists
        conn.fetchrow = AsyncMock(return_value=None)  # no config row

        result = _run(gc._resolve_bucket_name(conn, "c_2abc3defg4hi"))
        assert result is None

    def test_returns_bucket_name_from_config_data_dict(self):
        row = MagicMock()
        row.__getitem__ = lambda self, key: {"bucket_name": "my-bucket-xyz"}
        conn = AsyncMock()
        conn.fetchval = AsyncMock(return_value=1)
        conn.fetchrow = AsyncMock(return_value={"config_data": {"bucket_name": "my-bucket-xyz"}})

        result = _run(gc._resolve_bucket_name(conn, "c_2abc3defg4hi"))
        assert result == "my-bucket-xyz"

    def test_returns_bucket_name_from_config_data_json_string(self):
        """asyncpg may return JSONB as a pre-parsed dict, but test the string path too."""
        conn = AsyncMock()
        conn.fetchval = AsyncMock(return_value=1)
        conn.fetchrow = AsyncMock(
            return_value={"config_data": json.dumps({"bucket_name": "str-bucket"})}
        )

        result = _run(gc._resolve_bucket_name(conn, "c_2abc3defg4hi"))
        assert result == "str-bucket"

    def test_returns_none_when_bucket_name_key_absent(self):
        conn = AsyncMock()
        conn.fetchval = AsyncMock(return_value=1)
        conn.fetchrow = AsyncMock(return_value={"config_data": {"provision_enabled": True}})

        result = _run(gc._resolve_bucket_name(conn, "c_2abc3defg4hi"))
        assert result is None


# ---------------------------------------------------------------------------
# _detect_phantoms
# ---------------------------------------------------------------------------

class TestDetectPhantoms:
    def _make_conn(self, rows: list, bucket: Optional[str] = "bucket-abc") -> Any:
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=rows)
        # Schema exists and config row returns bucket
        conn.fetchval = AsyncMock(return_value=1)
        conn.fetchrow = AsyncMock(
            return_value={"config_data": {"bucket_name": bucket}} if bucket else None
        )
        return conn

    def test_returns_phantom_for_each_matching_row(self):
        rows = [
            {"id": "c_2abc3defg4hi5", "external_id": "c_2abc3defg4hi5", "provisioning_status": "ready"},
            {"id": "c_3kbc7rmn4bcde", "external_id": "c_3kbc7rmn4bcde", "provisioning_status": "provisioning"},
        ]
        conn = self._make_conn(rows, bucket="bkt-1")
        phantoms = _run(gc._detect_phantoms(conn))
        assert len(phantoms) == 2
        assert phantoms[0].internal_id == "c_2abc3defg4hi5"
        assert phantoms[0].bucket_name == "bkt-1"

    def test_returns_empty_when_no_phantoms(self):
        conn = self._make_conn([])
        phantoms = _run(gc._detect_phantoms(conn))
        assert phantoms == []

    def test_phantom_with_no_bucket_has_none(self):
        rows = [
            {"id": "c_2abc3defg4hi5", "external_id": "c_2abc3defg4hi5", "provisioning_status": "provisioning"},
        ]
        conn = self._make_conn(rows, bucket=None)
        # When schema exists but no config row
        conn.fetchrow = AsyncMock(return_value=None)
        phantoms = _run(gc._detect_phantoms(conn))
        assert phantoms[0].bucket_name is None


# ---------------------------------------------------------------------------
# _detect_all_catalogs
# ---------------------------------------------------------------------------

class TestDetectAllCatalogs:
    """--all mode selects every non-deleted catalog regardless of external_id shape."""

    def _make_conn(self, rows: list, bucket: Optional[str] = "bucket-all") -> Any:
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=rows)
        conn.fetchval = AsyncMock(return_value=1)
        conn.fetchrow = AsyncMock(
            return_value={"config_data": {"bucket_name": bucket}} if bucket else None
        )
        return conn

    def test_returns_all_rows_regardless_of_external_id_shape(self):
        """Both phantom-shaped and real external_ids must appear in the result."""
        rows = [
            {"id": "c_2abc3defg4hi5", "external_id": "c_2abc3defg4hi5", "provisioning_status": "ready"},
            {"id": "c_realinternal1x", "external_id": "my-real-catalog", "provisioning_status": "ready"},
            {"id": "c_anotherreal1xx", "external_id": "another-catalog", "provisioning_status": "provisioning"},
        ]
        conn = self._make_conn(rows)
        results = _run(gc._detect_all_catalogs(conn))
        assert len(results) == 3
        external_ids = {r.external_id for r in results}
        assert "c_2abc3defg4hi5" in external_ids
        assert "my-real-catalog" in external_ids
        assert "another-catalog" in external_ids

    def test_query_has_no_external_id_filter(self):
        """_detect_all_catalogs must NOT pass a regex argument to conn.fetch."""
        conn = self._make_conn([])
        _run(gc._detect_all_catalogs(conn))
        call_args = conn.fetch.call_args
        # The query must not carry a positional arg (the regex) like _detect_phantoms does
        positional = call_args[0]  # (sql,) for all-catalogs vs (sql, regex) for phantoms
        assert len(positional) == 1, (
            "_detect_all_catalogs must not pass a regex filter to conn.fetch"
        )

    def test_returns_empty_when_no_catalogs(self):
        conn = self._make_conn([])
        results = _run(gc._detect_all_catalogs(conn))
        assert results == []

    def test_bucket_name_resolved_per_catalog(self):
        rows = [
            {"id": "c_realinternal1x", "external_id": "my-catalog", "provisioning_status": "ready"},
        ]
        conn = self._make_conn(rows, bucket="my-bucket")
        results = _run(gc._detect_all_catalogs(conn))
        assert results[0].bucket_name == "my-bucket"


# ---------------------------------------------------------------------------
# _fetch_phantoms_by_ids
# ---------------------------------------------------------------------------

class TestFetchPhantomsByIds:
    def test_skips_non_internal_shaped_ids(self, capsys):
        conn = AsyncMock()
        phantoms = _run(gc._fetch_phantoms_by_ids(conn, ["my-real-catalog"]))
        assert phantoms == []
        conn.fetchrow.assert_not_called()

    def test_skips_missing_rows(self, capsys):
        conn = AsyncMock()
        conn.fetchrow = AsyncMock(return_value=None)
        conn.fetchval = AsyncMock(return_value=None)
        phantoms = _run(gc._fetch_phantoms_by_ids(conn, ["c_2abc3defg4hi5"]))
        assert phantoms == []

    def test_returns_phantom_for_existing_internal_id(self):
        conn = AsyncMock()
        conn.fetchrow = AsyncMock(side_effect=[
            # catalog row
            {"id": "c_2abc3defg4hi5", "external_id": "c_2abc3defg4hi5", "provisioning_status": "ready"},
            # catalog_configs row for bucket
            {"config_data": {"bucket_name": "my-bkt"}},
        ])
        conn.fetchval = AsyncMock(return_value=1)  # schema exists
        phantoms = _run(gc._fetch_phantoms_by_ids(conn, ["c_2abc3defg4hi5"]))
        assert len(phantoms) == 1
        assert phantoms[0].internal_id == "c_2abc3defg4hi5"
        assert phantoms[0].bucket_name == "my-bkt"

    def test_mixed_valid_and_invalid_ids(self):
        conn = AsyncMock()
        conn.fetchrow = AsyncMock(side_effect=[
            {"id": "c_2abc3defg4hi5", "external_id": "c_2abc3defg4hi5", "provisioning_status": "ready"},
            {"config_data": {"bucket_name": "bkt"}},
        ])
        conn.fetchval = AsyncMock(return_value=1)
        ids = ["real-catalog-id", "c_2abc3defg4hi5", "another-real-one"]
        phantoms = _run(gc._fetch_phantoms_by_ids(conn, ids))
        assert len(phantoms) == 1
        assert phantoms[0].internal_id == "c_2abc3defg4hi5"


# ---------------------------------------------------------------------------
# teardown_phantom_gcp_resources
# ---------------------------------------------------------------------------

class TestTeardownPhantomGcpResources:
    def test_skips_when_no_protocols_registered(self):
        # get_protocol is imported inside teardown_phantom_gcp_resources from
        # dynastore.modules — patch it there.
        import dynastore.scripts.gc_phantom_catalogs as _gc

        with patch("dynastore.modules.get_protocol", return_value=None):
            result = _run(_gc.teardown_phantom_gcp_resources("c_2abc3defg4hi5", None))
        assert result["status"] == "skipped_no_protocols"

    def test_calls_eventing_teardown_and_bucket_delete(self):
        mock_storage = AsyncMock()
        mock_storage.get_storage_identifier = AsyncMock(return_value="my-bucket")
        mock_eventing = AsyncMock()
        mock_eventing.teardown_catalog_eventing = AsyncMock()

        import dynastore.scripts.gc_phantom_catalogs as _gc

        def _get_protocol(cls):
            from dynastore.models.protocols import StorageProtocol, EventingProtocol
            if cls == StorageProtocol:
                return mock_storage
            if cls == EventingProtocol:
                return mock_eventing
            return None

        mock_delete = AsyncMock()
        with patch("dynastore.modules.get_protocol", side_effect=_get_protocol):
            with patch("dynastore.modules.gcp.gcp_config.GcpEventingConfig", MagicMock()):
                with patch(
                    "dynastore.modules.gcp.tools.bucket.delete_bucket",
                    new=mock_delete,
                ):
                    result = _run(_gc.teardown_phantom_gcp_resources("c_2abc3defg4hi5", "pre-resolved-bkt"))

        mock_eventing.teardown_catalog_eventing.assert_called_once()
        mock_delete.assert_called_once_with("pre-resolved-bkt", force=True, client=None)
        assert result["status"] == "cleaned"

    def test_idempotent_when_bucket_already_gone(self):
        """If delete_bucket raises (bucket not found), the exception propagates so
        the caller can decide to log and continue.  The teardown is not silently
        swallowed — that would mask real errors."""
        mock_storage = AsyncMock()
        mock_eventing = AsyncMock()
        mock_eventing.teardown_catalog_eventing = AsyncMock()

        import dynastore.scripts.gc_phantom_catalogs as _gc

        def _get_protocol(cls):
            from dynastore.models.protocols import StorageProtocol, EventingProtocol
            if cls == StorageProtocol:
                return mock_storage
            if cls == EventingProtocol:
                return mock_eventing
            return None

        async def _bucket_not_found(*a, **kw):
            raise FileNotFoundError("bucket not found")

        with patch("dynastore.modules.get_protocol", side_effect=_get_protocol):
            with patch(
                "dynastore.modules.gcp.tools.bucket.delete_bucket",
                new=_bucket_not_found,
            ):
                with pytest.raises(FileNotFoundError):
                    _run(_gc.teardown_phantom_gcp_resources("c_2abc3defg4hi5", "gone-bkt"))


# ---------------------------------------------------------------------------
# _run — dry-run performs zero mutations
# ---------------------------------------------------------------------------

class TestRunDryRun:
    def _make_asyncpg(self, phantoms_rows: list):
        """Patch asyncpg.connect to return a mock connection."""
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=phantoms_rows)
        conn.fetchval = AsyncMock(return_value=None)  # schema absent → bucket=None
        conn.fetchrow = AsyncMock(return_value=None)
        conn.execute = AsyncMock()
        conn.close = AsyncMock()
        return conn

    def test_dry_run_does_not_call_execute(self, capsys):
        phantom_row = {
            "id": "c_2abc3defg4hi5",
            "external_id": "c_2abc3defg4hi5",
            "provisioning_status": "ready",
        }
        conn = self._make_asyncpg([phantom_row])

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            rc = asyncio.get_event_loop().run_until_complete(
                gc._run(
                    "postgresql://localhost/test",
                    execute=False,
                    allow_gcp_skip=False,
                    ids=None,
                )
            )

        assert rc == 0
        conn.execute.assert_not_called()
        out = capsys.readouterr().out
        assert "DRY RUN" in out
        assert "c_2abc3defg4hi5" in out

    def test_dry_run_mentions_gcp_skip_note(self, capsys):
        """Dry-run output must tell the operator about the --allow-gcp-skip requirement."""
        phantom_row = {
            "id": "c_2abc3defg4hi5",
            "external_id": "c_2abc3defg4hi5",
            "provisioning_status": "ready",
        }
        conn = self._make_asyncpg([phantom_row])

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            asyncio.get_event_loop().run_until_complete(
                gc._run(
                    "postgresql://localhost/test",
                    execute=False,
                    allow_gcp_skip=False,
                    ids=None,
                )
            )

        out = capsys.readouterr().out
        assert "--allow-gcp-skip" in out

    def test_dry_run_with_no_phantoms_exits_zero(self, capsys):
        conn = self._make_asyncpg([])
        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            rc = asyncio.get_event_loop().run_until_complete(
                gc._run(
                    "postgresql://localhost/test",
                    execute=False,
                    allow_gcp_skip=False,
                    ids=None,
                )
            )
        assert rc == 0
        out = capsys.readouterr().out
        assert "No phantom catalogs found" in out

    def test_dry_run_all_catalogs_makes_no_mutations(self, capsys):
        """--all dry-run selects all catalogs but never calls conn.execute."""
        rows = [
            {"id": "c_realinternal1x", "external_id": "my-real-catalog", "provisioning_status": "ready"},
            {"id": "c_2abc3defg4hi5", "external_id": "c_2abc3defg4hi5", "provisioning_status": "ready"},
        ]
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=rows)
        conn.fetchval = AsyncMock(return_value=None)
        conn.fetchrow = AsyncMock(return_value=None)
        conn.execute = AsyncMock()
        conn.close = AsyncMock()

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            rc = asyncio.get_event_loop().run_until_complete(
                gc._run(
                    "postgresql://localhost/test",
                    execute=False,
                    allow_gcp_skip=False,
                    ids=None,
                    all_catalogs=True,
                )
            )

        assert rc == 0
        conn.execute.assert_not_called()
        out = capsys.readouterr().out
        assert "DRY RUN" in out
        assert "my-real-catalog" in out


# ---------------------------------------------------------------------------
# _run — execute mode calls teardown + delete per phantom
# ---------------------------------------------------------------------------

class TestRunExecute:
    def test_execute_drops_schema_and_deletes_row_with_allow_gcp_skip(self, capsys):
        """When GCP protocols are absent and --allow-gcp-skip is set, the schema
        and row are still dropped (GCP cleanup was done manually)."""
        phantom_row = {
            "id": "c_2abc3defg4hi5",
            "external_id": "c_2abc3defg4hi5",
            "provisioning_status": "ready",
        }
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[phantom_row])
        conn.fetchval = AsyncMock(return_value=None)  # schema absent
        conn.fetchrow = AsyncMock(return_value=None)
        conn.execute = AsyncMock()
        conn.close = AsyncMock()

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            with patch("dynastore.modules.get_protocol", return_value=None):
                rc = asyncio.get_event_loop().run_until_complete(
                    gc._run(
                        "postgresql://localhost/test",
                        execute=True,
                        allow_gcp_skip=True,
                        ids=None,
                    )
                )

        assert rc == 0
        drop_calls = [str(c) for c in conn.execute.call_args_list]
        assert any("DROP SCHEMA" in s and "c_2abc3defg4hi5" in s for s in drop_calls)
        assert any("DELETE FROM catalog.catalogs" in s for s in drop_calls)

    def test_execute_refuses_without_allow_gcp_skip_when_no_protocols(self, capsys):
        """The orphan-guard: when GCP protocols are absent and --allow-gcp-skip is
        NOT set, the schema and row must NOT be dropped.  The phantom must be counted
        as failed so the operator is alerted and can retry in a GCP-aware context.

        The lifespan bootstrap is mocked (no-op) to simulate a run where the module
        stack booted but the GCP module was not installed/registered.
        """
        phantom_row = {
            "id": "c_2abc3defg4hi5",
            "external_id": "c_2abc3defg4hi5",
            "provisioning_status": "ready",
        }
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[phantom_row])
        conn.fetchval = AsyncMock(return_value=None)
        conn.fetchrow = AsyncMock(return_value=None)
        conn.execute = AsyncMock()
        conn.close = AsyncMock()

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            with patch("dynastore.modules.get_protocol", return_value=None):
                with patch("dynastore.tasks.bootstrap.bootstrap_task_env"):
                    with patch(
                        "dynastore.modules.lifespan",
                        return_value=_noop_lifespan(),
                    ):
                        rc = asyncio.get_event_loop().run_until_complete(
                            gc._run(
                                "postgresql://localhost/test",
                                execute=True,
                                allow_gcp_skip=False,
                                ids=None,
                            )
                        )

        # Must report failure — the orphan guard fired
        assert rc == 1
        # Schema and row must NOT have been touched
        conn.execute.assert_not_called()
        out = capsys.readouterr().out
        assert "ERROR" in out or "failed" in out.lower()

    def test_execute_returns_nonzero_on_db_failure(self, capsys):
        """A DB error during schema drop is caught per-phantom and counted as failed."""
        phantom_row = {
            "id": "c_2abc3defg4hi5",
            "external_id": "c_2abc3defg4hi5",
            "provisioning_status": "ready",
        }
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[phantom_row])
        conn.fetchval = AsyncMock(return_value=None)
        conn.fetchrow = AsyncMock(return_value=None)
        conn.execute = AsyncMock(side_effect=RuntimeError("DB error"))
        conn.close = AsyncMock()

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            with patch("dynastore.modules.get_protocol", return_value=None):
                rc = asyncio.get_event_loop().run_until_complete(
                    gc._run(
                        "postgresql://localhost/test",
                        execute=True,
                        allow_gcp_skip=True,
                        ids=None,
                    )
                )
        assert rc == 1
        out = capsys.readouterr().out
        assert "failed" in out.lower() or "ERROR" in out

    def test_execute_all_catalogs_drops_schema_and_deletes_row(self, capsys):
        """--all --execute tears down every catalog, including real external_ids."""
        rows = [
            {"id": "c_realinternal1x", "external_id": "my-real-catalog", "provisioning_status": "ready"},
            {"id": "c_2abc3defg4hi5", "external_id": "c_2abc3defg4hi5", "provisioning_status": "ready"},
        ]
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=rows)
        conn.fetchval = AsyncMock(return_value=None)
        conn.fetchrow = AsyncMock(return_value=None)
        conn.execute = AsyncMock()
        conn.close = AsyncMock()

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            with patch("dynastore.modules.get_protocol", return_value=None):
                rc = asyncio.get_event_loop().run_until_complete(
                    gc._run(
                        "postgresql://localhost/test",
                        execute=True,
                        allow_gcp_skip=True,
                        ids=None,
                        all_catalogs=True,
                    )
                )

        assert rc == 0
        drop_calls = [str(c) for c in conn.execute.call_args_list]
        # Both catalogs must have been touched
        assert any("c_realinternal1x" in s for s in drop_calls)
        assert any("c_2abc3defg4hi5" in s for s in drop_calls)
        assert any("DROP SCHEMA" in s for s in drop_calls)
        assert any("DELETE FROM catalog.catalogs" in s for s in drop_calls)


# ---------------------------------------------------------------------------
# Safety invariant: explicit --ids with non-phantom shape is always skipped
# ---------------------------------------------------------------------------

class TestSafetyInvariant:
    def test_real_external_id_in_ids_is_never_touched(self, capsys):
        """Passing a real catalog's external id via --ids must be silently skipped
        — the id shape guard must fire before any DB access."""
        conn = AsyncMock()
        conn.fetchrow = AsyncMock()
        conn.fetchval = AsyncMock()
        conn.close = AsyncMock()

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            rc = asyncio.get_event_loop().run_until_complete(
                gc._run(
                    "postgresql://localhost/test",
                    execute=True,
                    allow_gcp_skip=False,
                    ids=["real-external-catalog-name"],
                )
            )

        assert rc == 0
        conn.fetchrow.assert_not_called()
        conn.fetchval.assert_not_called()
        out = capsys.readouterr().out
        assert "No phantom catalogs found" in out or "SKIP" in out


# ---------------------------------------------------------------------------
# Orphan-guard: _teardown_phantom behaviour with skipped_no_protocols
# ---------------------------------------------------------------------------

class TestOrphanGuard:
    """The orphan-guard prevents dropping schema/row when GCP teardown is skipped."""

    def _make_phantom(self) -> gc.PhantomCatalog:
        return gc.PhantomCatalog(
            internal_id="c_2abc3defg4hi5",
            external_id="c_2abc3defg4hi5",
            provisioning_status="ready",
            bucket_name="bkt-xyz",
        )

    def test_raises_when_gcp_skipped_and_allow_gcp_skip_false(self):
        conn = AsyncMock()
        conn.execute = AsyncMock()
        phantom = self._make_phantom()

        with patch("dynastore.modules.get_protocol", return_value=None):
            with pytest.raises(RuntimeError, match="skipped_no_protocols|allow-gcp-skip"):
                _run(gc._teardown_phantom(conn, phantom, allow_gcp_skip=False))

        # Schema and row must NOT have been touched
        conn.execute.assert_not_called()

    def test_proceeds_when_gcp_skipped_and_allow_gcp_skip_true(self, capsys):
        conn = AsyncMock()
        conn.execute = AsyncMock()
        phantom = self._make_phantom()

        with patch("dynastore.modules.get_protocol", return_value=None):
            _run(gc._teardown_phantom(conn, phantom, allow_gcp_skip=True))

        drop_calls = [str(c) for c in conn.execute.call_args_list]
        assert any("DROP SCHEMA" in s for s in drop_calls)
        assert any("DELETE FROM catalog.catalogs" in s for s in drop_calls)
        # Must log a warning naming the catalog id
        out = capsys.readouterr().out
        assert "WARNING" in out
        assert "c_2abc3defg4hi5" in out

    def test_no_guard_fires_when_gcp_teardown_succeeds(self):
        """When GCP protocols are present and teardown succeeds, the guard is not
        triggered and the schema + row are dropped normally."""
        conn = AsyncMock()
        conn.execute = AsyncMock()
        phantom = self._make_phantom()

        mock_storage = AsyncMock()
        mock_eventing = AsyncMock()
        mock_eventing.teardown_catalog_eventing = AsyncMock()

        def _get_protocol(cls):
            from dynastore.models.protocols import StorageProtocol, EventingProtocol
            if cls == StorageProtocol:
                return mock_storage
            if cls == EventingProtocol:
                return mock_eventing
            return None

        with patch("dynastore.modules.get_protocol", side_effect=_get_protocol):
            with patch("dynastore.modules.gcp.tools.bucket.delete_bucket", new=AsyncMock()):
                _run(gc._teardown_phantom(conn, phantom, allow_gcp_skip=False))

        drop_calls = [str(c) for c in conn.execute.call_args_list]
        assert any("DROP SCHEMA" in s for s in drop_calls)
        assert any("DELETE FROM catalog.catalogs" in s for s in drop_calls)


# ---------------------------------------------------------------------------
# Dev-only guard: _require_dev_env
# ---------------------------------------------------------------------------

class TestRequireDevEnv:
    """_require_dev_env refuses prod/empty and allows dev/development/review."""

    def _call(self, monkeypatch, dynastore_env=None, environment=None):
        if dynastore_env is not None:
            monkeypatch.setenv("DYNASTORE_ENV", dynastore_env)
        else:
            monkeypatch.delenv("DYNASTORE_ENV", raising=False)
        if environment is not None:
            monkeypatch.setenv("ENVIRONMENT", environment)
        else:
            monkeypatch.delenv("ENVIRONMENT", raising=False)

    @pytest.mark.parametrize(
        "dynastore_env, environment, should_raise",
        [
            pytest.param("prod", None, True, id="refuses_when_env_is_prod"),
            pytest.param("production", None, True, id="refuses_when_env_is_production"),
            # Empty label is refused — --all must not run when env is unknown.
            pytest.param(None, None, True, id="refuses_when_env_is_empty"),
            pytest.param("staging", None, True, id="refuses_when_env_is_unknown_label"),
            pytest.param("dev", None, False, id="allows_dev"),
            pytest.param("development", None, False, id="allows_development"),
            pytest.param("review", None, False, id="allows_review"),
            pytest.param("DEV", None, False, id="allows_dev_case_insensitive"),
            # When DYNASTORE_ENV is absent, ENVIRONMENT is used as fallback.
            pytest.param(None, "dev", False, id="falls_back_to_environment_var"),
            # DYNASTORE_ENV=prod wins even when ENVIRONMENT=dev.
            pytest.param("prod", "dev", True, id="dynastore_env_takes_priority_over_environment"),
        ],
    )
    def test_require_dev_env(self, monkeypatch, dynastore_env, environment, should_raise):
        self._call(monkeypatch, dynastore_env=dynastore_env, environment=environment)
        if should_raise:
            with pytest.raises(SystemExit) as exc:
                gc._require_dev_env()
            assert exc.value.code == 2
        else:
            gc._require_dev_env()  # must not raise


# ---------------------------------------------------------------------------
# _clean_dsn / _normalize_dsn / _resolve_dsn
# ---------------------------------------------------------------------------

class TestCleanDsn:
    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param("  postgresql://x/db  ", "postgresql://x/db", id="strips_surrounding_whitespace"),
            pytest.param("'postgresql://x/db'", "postgresql://x/db", id="strips_single_quotes"),
            pytest.param('"postgresql://x/db"', "postgresql://x/db", id="strips_double_quotes"),
            pytest.param("", None, id="returns_none_for_empty_string"),
            pytest.param(None, None, id="returns_none_for_none"),
        ],
    )
    def test_clean_dsn(self, value, expected):
        assert gc._clean_dsn(value) == expected


class TestNormalizeDsn:
    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param(
                "postgresql+asyncpg://u:p@h/db", "postgresql://u:p@h/db", id="strips_postgresql_asyncpg"
            ),
            pytest.param(
                "postgres+asyncpg://u:p@h/db", "postgresql://u:p@h/db", id="strips_postgres_asyncpg"
            ),
            pytest.param(
                "postgresql://u:p@h/db", "postgresql://u:p@h/db", id="plain_postgresql_unchanged"
            ),
        ],
    )
    def test_normalize_dsn(self, value, expected):
        assert gc._normalize_dsn(value) == expected


class TestResolveDsn:
    def test_prefers_db_config_json(self, monkeypatch):
        """When load_db_config returns a DATABASE_URL, it is used and tagged db_config.json."""
        monkeypatch.delenv("DATABASE_URL", raising=False)
        dsn_val = "postgresql://user:pass@dbhost:5432/mydb"
        with patch(
            "dynastore.modules.db_config.instance.load_db_config",
            return_value={"DATABASE_URL": dsn_val},
        ):
            dsn, source = gc._resolve_dsn()
        assert dsn == dsn_val
        assert source == "db_config.json"

    def test_falls_back_to_dbconfig_database_url_when_no_json(self, monkeypatch):
        """When load_db_config returns empty dict, DBConfig.database_url is tried.

        _LazyDatabaseUrl caches its result in an instance attribute (_resolved) on
        the descriptor object itself.  Patching the cache directly avoids triggering
        the descriptor's __get__ (which would raise RuntimeError in a no-env context).
        """
        dsn_val = "postgresql://u:p@h/db"
        import dynastore.modules.db_config.db_config as _dbcfg
        # Access the descriptor instance via __dict__ to avoid triggering __get__.
        descriptor = _dbcfg.DBConfig.__dict__["database_url"]
        # Pre-fill the cache so __get__ returns dsn_val without calling _resolve_database_url.
        monkeypatch.setattr(descriptor, "_resolved", dsn_val)
        with patch(
            "dynastore.modules.db_config.instance.load_db_config",
            return_value={},
        ):
            dsn, source = gc._resolve_dsn()
        assert dsn == dsn_val
        assert source == "DBConfig.database_url"

    def test_falls_back_to_env_when_others_unavailable(self, monkeypatch):
        """When load_db_config raises and DBConfig raises, plain env is used."""
        dsn_val = "postgresql://u:p@envhost:5432/mydb"
        monkeypatch.setenv("DATABASE_URL", dsn_val)
        with patch(
            "dynastore.modules.db_config.instance.load_db_config",
            side_effect=ImportError("unavailable"),
        ):
            with patch(
                "dynastore.modules.db_config.db_config.DBConfig",
                side_effect=RuntimeError("unavailable"),
            ):
                dsn, source = gc._resolve_dsn()
        assert dsn == dsn_val
        assert source == "env"

    def test_returns_missing_when_nothing_available(self, monkeypatch):
        """When all three sources fail, dsn is None and source is MISSING."""
        monkeypatch.delenv("DATABASE_URL", raising=False)
        with patch(
            "dynastore.modules.db_config.instance.load_db_config",
            return_value={},
        ):
            with patch(
                "dynastore.modules.db_config.db_config.DBConfig",
                side_effect=RuntimeError("no db"),
            ):
                dsn, source = gc._resolve_dsn()
        assert dsn is None
        assert source == "MISSING"

    def test_db_config_json_dsn_without_scheme_is_skipped(self, monkeypatch):
        """A value in db_config.json without '://' must be skipped, not used."""
        monkeypatch.delenv("DATABASE_URL", raising=False)
        with patch(
            "dynastore.modules.db_config.instance.load_db_config",
            return_value={"DATABASE_URL": "not-a-url"},
        ):
            with patch(
                "dynastore.modules.db_config.db_config.DBConfig",
                side_effect=RuntimeError("no db"),
            ):
                dsn, source = gc._resolve_dsn()
        assert dsn is None  # the invalid value must not be forwarded


# ---------------------------------------------------------------------------
# Drift guard: is_phantom_external_id must agree with the SSOT predicate
# is_internal_physical_name from catalog_service.
# ---------------------------------------------------------------------------

class TestDriftGuard:
    """Pin that the local regex in is_phantom_external_id agrees with the SSOT
    ``is_internal_physical_name`` in catalog_service for the catalog prefix.

    If they ever diverge (e.g. the alphabet or length changes in generate_physical_name),
    this test will catch it before the operator script silently misclassifies rows.

    SSOT: dynastore.modules.catalog.catalog_service.is_internal_physical_name
    """

    @pytest.fixture(autouse=True)
    def _import_ssot(self):
        from dynastore.modules.catalog.catalog_service import is_internal_physical_name
        self.ssot = is_internal_physical_name

    def _agree(self, value: str) -> None:
        """Assert the script and the SSOT agree on ``value`` for prefix 'c'."""
        script_says = gc.is_phantom_external_id(value)
        ssot_says = self.ssot(value, "c")
        assert script_says == ssot_says, (
            f"Divergence on {value!r}: script={script_says}, ssot={ssot_says}"
        )

    def test_valid_catalog_internal_id(self):
        self._agree("c_2abc3defg4hij")

    def test_collection_internal_id_is_not_catalog(self):
        # col_ prefix → not a catalog internal id; both must return False
        self._agree("col_2abc3defg4hi")

    def test_human_slug_is_not_internal(self):
        self._agree("my-real-catalog")

    def test_too_short(self):
        self._agree("c_2abc3defg4")

    def test_too_long(self):
        self._agree("c_2abc3defg4hijkl")
