"""Local / on-prem ``file://`` URIs must resolve to a plain GDAL path.

GDAL/OGR cannot open a ``file://`` URI directly (``ogr.Open`` returns ``None``),
so every URI→GDAL-path translator must strip the scheme to a filesystem path.
Regression guard for local-disk asset ingestion (gdalinfo + ingestion readers).
"""
from dynastore.modules.gcp.tools.bucket import get_gdal_path
from dynastore.modules.gdal.service import _href_to_vsi_path
from dynastore.tasks.ingestion.readers.base import _to_vsigs

_URI = "file:///tmp/dynastore/assets/cat/col/ITAL1_01.geojson"
_PATH = "/tmp/dynastore/assets/cat/col/ITAL1_01.geojson"


def test_get_gdal_path_strips_file_scheme():
    assert get_gdal_path(_URI) == _PATH


def test_to_vsigs_strips_file_scheme():
    assert _to_vsigs(_URI) == _PATH


def test_href_to_vsi_path_strips_file_scheme():
    assert _href_to_vsi_path(_URI) == _PATH


def test_remote_schemes_unchanged():
    # The fix must not regress the cloud/HTTP translations.
    assert get_gdal_path("gs://b/k.geojson") == "/vsigs/b/k.geojson"
    assert _to_vsigs("gs://b/k.geojson") == "/vsigs/b/k.geojson"
    assert _href_to_vsi_path("gs://b/k.geojson") == "/vsigs/b/k.geojson"
    assert _href_to_vsi_path("s3://b/k.geojson") == "/vsis3/b/k.geojson"


def test_plain_local_path_unchanged():
    assert get_gdal_path(_PATH) == _PATH
    assert _to_vsigs(_PATH) == _PATH
    assert _href_to_vsi_path(_PATH) == _PATH
