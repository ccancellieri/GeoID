#    Copyright 2025 FAO
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

"""Shared helpers for the DuckDB storage driver."""

from typing import Any, Dict, List, Union

from dynastore.models.ogc import Feature, FeatureCollection


def normalize_to_dicts(
    entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """Normalize input entities to a list of dicts for DuckDB ingestion."""
    if isinstance(entities, Feature):
        return [_feature_to_dict(entities)]
    if isinstance(entities, FeatureCollection):
        return [_feature_to_dict(f) for f in (entities.features or [])]
    if isinstance(entities, list):
        return [
            _feature_to_dict(e) if isinstance(e, Feature) else e
            for e in entities
        ]
    if isinstance(entities, dict):
        return [entities]
    if hasattr(entities, "model_dump"):
        return [entities.model_dump()]
    return []


def _feature_to_dict(feature: Feature) -> Dict[str, Any]:
    """Convert a Feature to a flat dict for tabular storage."""
    d: Dict[str, Any] = {}
    if feature.id is not None:
        d["id"] = str(feature.id)
    if feature.geometry is not None:
        d["geometry"] = (
            feature.geometry.model_dump()
            if hasattr(feature.geometry, "model_dump")
            else feature.geometry
        )
    if feature.properties:
        d.update(feature.properties)
    return d


def dicts_to_features(rows: List[Dict[str, Any]]) -> List[Feature]:
    """Convert flat dicts back to Feature models."""
    features = []
    for row in rows:
        row = dict(row)
        fid = row.pop("id", None)
        geom = row.pop("geometry", None)
        features.append(
            Feature(type="Feature", id=fid, geometry=geom, properties=row)
        )
    return features
