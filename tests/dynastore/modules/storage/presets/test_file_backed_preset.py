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

"""The file_backed preset (#378): driver + routing + discoverability composition."""
from __future__ import annotations


def _bundle(**params):
    from dynastore.modules.storage.presets.file_backed import (
        FileBackedPreset, FileBackedPresetParams,
    )
    p = FileBackedPresetParams(**params)
    return FileBackedPreset()._build_bundle(p, {"catalog_id": "cat1", "collection_id": "col1"})


def _by_slot(bundle):
    return {e.slot: e for e in bundle.entries}


def test_preset_is_registered():
    from dynastore.modules.storage.presets import find_preset
    preset = find_preset("file_backed")
    assert preset is not None
    assert preset.name == "file_backed"


def test_bundle_writes_duckdb_config_with_asset_binding():
    slots = _by_slot(_bundle(asset_id="asset-7", format="gpkg", id_column="fid"))
    cfg = slots["duckdb_driver_config"].instance
    assert cfg.asset_id == "asset-7"
    assert cfg.format == "gpkg"
    assert cfg.id_column == "fid"


def test_discoverable_routes_file_read_and_es_index():
    slots = _by_slot(_bundle(asset_id="a", discoverable=True))
    routing = slots["items_routing"].instance
    ops = routing.operations

    # READ resolves the file driver via GEOMETRY_EXACT (file = exact source).
    read_refs = [e.driver_ref for e in ops["READ"]]
    assert read_refs == ["items_duckdb_driver"]
    from dynastore.modules.storage.hints import Hint
    assert Hint.GEOMETRY_EXACT in ops["READ"][0].hints

    # No WRITE lane at all — items are never client-writable (#2494).
    assert "WRITE" not in ops

    # INDEX carries the ES materialization target so the file->ES reindex
    # can run; derived search prefers it, falling back to the file driver
    # (READ lane) when INDEX is empty.
    index_refs = [e.driver_ref for e in ops["INDEX"]]
    assert index_refs == ["items_elasticsearch_driver"]

    # Discoverable bundle requests geometry simplification for the global index.
    es_cfg = _by_slot(_bundle(asset_id="a", discoverable=True))["es_driver_config"].instance
    assert es_cfg.simplify_geometry is True


def test_non_discoverable_has_no_es_and_no_index():
    bundle = _bundle(asset_id="a", discoverable=False)
    slots = _by_slot(bundle)
    assert "es_driver_config" not in slots
    ops = slots["items_routing"].instance.operations
    assert "INDEX" not in ops or not ops["INDEX"]
    # No configured SEARCH operation — with INDEX empty, derived search
    # falls back to the READ lane (the file driver) only.
    assert [e.driver_ref for e in ops["READ"]] == ["items_duckdb_driver"]


def test_simplify_geometry_param_is_respected():
    slots = _by_slot(_bundle(asset_id="a", discoverable=True, simplify_geometry=False))
    assert slots["es_driver_config"].instance.simplify_geometry is False


# ---------------------------------------------------------------------------
# Examples / meta tests
# ---------------------------------------------------------------------------

def test_three_examples_are_declared():
    from dynastore.modules.storage.presets.file_backed import FileBackedPreset
    preset = FileBackedPreset()
    examples = getattr(preset, "examples", ())
    assert len(examples) == 3
    names = [ex.name for ex in examples]
    assert "remote-geoparquet-https" in names
    assert "cloud-geoparquet-folder-discoverable" in names
    assert "geopackage-asset" in names


def test_example_names_and_summaries():
    from dynastore.modules.storage.presets.file_backed import FileBackedPreset
    preset = FileBackedPreset()
    by_name = {ex.name: ex for ex in preset.examples}

    https_ex = by_name["remote-geoparquet-https"]
    assert https_ex.params["format"] == "geoparquet"
    assert https_ex.params["discoverable"] is False
    assert "https://" in https_ex.params["path"]

    cloud_ex = by_name["cloud-geoparquet-folder-discoverable"]
    assert cloud_ex.params["format"] == "geoparquet"
    assert cloud_ex.params["discoverable"] is True
    assert cloud_ex.params["id_column"] == "id"
    assert "s3://" in cloud_ex.params["path"]

    gpkg_ex = by_name["geopackage-asset"]
    assert gpkg_ex.params["format"] == "gpkg"
    assert gpkg_ex.params["asset_id"] == "admin-boundaries-gpkg"
    assert gpkg_ex.params["discoverable"] is True


def test_describe_preset_examples_have_resulting_config():
    """describe_preset builds a live resulting_config for all three examples."""
    from dynastore.modules.storage.presets.file_backed import FileBackedPreset
    from dynastore.modules.storage.presets.describe import describe_preset

    preset = FileBackedPreset()
    desc = describe_preset(preset)

    assert len(desc["examples"]) == 3

    for ex in desc["examples"]:
        assert "resulting_config" in ex, f"Missing resulting_config in {ex['name']}"
        if ex.get("error"):
            # If there is an error, print it for debugging but do not fail here
            # (invalid params in an example are an authoring error, tested separately)
            pass
        else:
            rc = ex["resulting_config"]
            assert rc is not None, f"resulting_config is None for {ex['name']}"
            assert isinstance(rc, list), f"resulting_config not a list for {ex['name']}"


def test_remote_geoparquet_example_resulting_config():
    """Example #1 (remote-geoparquet-https): non-discoverable → no ES slot."""
    from dynastore.modules.storage.presets.file_backed import FileBackedPreset
    from dynastore.modules.storage.presets.describe import describe_preset

    preset = FileBackedPreset()
    desc = describe_preset(preset)
    ex = next(e for e in desc["examples"] if e["name"] == "remote-geoparquet-https")

    assert ex.get("error") is None, f"Example raised error: {ex.get('error')}"
    rc = ex["resulting_config"]
    assert rc is not None

    slots = {entry["slot"]: entry for entry in rc}
    assert "duckdb_driver_config" in slots
    assert "items_routing" in slots
    # Non-discoverable: no ES driver config
    assert "es_driver_config" not in slots

    # Check DuckDB config carries the correct path and format
    duckdb_cfg = slots["duckdb_driver_config"]["config"]
    assert duckdb_cfg["format"] == "geoparquet"
    assert "opengeospatial" in duckdb_cfg.get("path", "")


def test_cloud_folder_example_resulting_config():
    """Example #2 (cloud-geoparquet-folder-discoverable): discoverable → ES slot present."""
    from dynastore.modules.storage.presets.file_backed import FileBackedPreset
    from dynastore.modules.storage.presets.describe import describe_preset

    preset = FileBackedPreset()
    desc = describe_preset(preset)
    ex = next(e for e in desc["examples"] if e["name"] == "cloud-geoparquet-folder-discoverable")

    assert ex.get("error") is None, f"Example raised error: {ex.get('error')}"
    rc = ex["resulting_config"]
    assert rc is not None

    slots = {entry["slot"]: entry for entry in rc}
    assert "duckdb_driver_config" in slots
    assert "items_routing" in slots
    assert "es_driver_config" in slots  # discoverable=True

    duckdb_cfg = slots["duckdb_driver_config"]["config"]
    assert duckdb_cfg["format"] == "geoparquet"
    assert duckdb_cfg.get("id_column") == "id"
    assert "s3://" in duckdb_cfg.get("path", "")

    es_cfg = slots["es_driver_config"]["config"]
    assert es_cfg.get("simplify_geometry") is True


def test_field_descriptions_present_in_meta():
    """describe_preset _meta.docs carries non-empty descriptions for key fields."""
    from dynastore.modules.storage.presets.file_backed import FileBackedPreset
    from dynastore.modules.storage.presets.describe import describe_preset

    desc = describe_preset(FileBackedPreset(), mode="field")
    meta = desc.get("_meta", {})
    docs = meta.get("docs", {})

    # All declared params must have a non-empty description
    for field_name in ("asset_id", "path", "format", "id_column", "discoverable", "simplify_geometry"):
        assert field_name in docs, f"Missing docs entry for field '{field_name}'"
        assert docs[field_name], f"Empty description for field '{field_name}'"


def test_geometry_column_threads_to_duckdb_config():
    """geometry_column preset param is forwarded to the DuckDB driver config."""
    slots = _by_slot(_bundle(
        path="s3://bucket/data/*.parquet",
        format="geoparquet",
        geometry_column="geom",
    ))
    duckdb_cfg = slots["duckdb_driver_config"].instance
    assert duckdb_cfg.geometry_column == "geom"


def test_geometry_column_default_is_none_in_config():
    """When geometry_column is not set, the DuckDB config carries None (default)."""
    slots = _by_slot(_bundle(path="/data/x.parquet", format="geoparquet"))
    duckdb_cfg = slots["duckdb_driver_config"].instance
    assert duckdb_cfg.geometry_column is None
