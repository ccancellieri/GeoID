from datetime import datetime, timezone

# shapely is imported lazily inside `Feature.to_geojson_feature` (the only
# consumer) so this module stays import-safe in SCOPE-trimmed Cloud Run
# Job containers that don't carry shapely. Module-level import here was
# the root cause of the dwh_join container's entry-point load failure
# (`Skipping plugin 'dwh_join': No module named 'shapely'`) — file_io
# imports Feature transitively, and dwh_join imports file_io.

from pydantic import BaseModel, ConfigDict
from typing import Optional
import itertools
import orjson
import uuid
import inspect
from typing import Generator, List, Tuple

from dynastore.tools.json import orjson_default


class FeatureProperties(BaseModel):
    """
    Defines the set of user-facing attributes for a WFS feature.
    This model is used to control which fields are exposed in GetFeature
    and described in DescribeFeatureType, ensuring they are always in sync.
    """

    geoid: uuid.UUID
    # id: int
    external_id: Optional[str] = None
    asset_code: Optional[str] = None
    transaction_time: Optional[datetime] = None
    geometry_hash: Optional[str] = None

    valid_from: Optional[datetime] = None
    valid_to: Optional[datetime] = None
    attributes: Optional[dict] = None

    geom_gml: Optional[str] = None
    geom: Optional[str | bytes] = None
    bbox: Optional[Tuple[float]] = None

    model_config = ConfigDict(
        # This model is used as a field definition guide, not for data validation.
        from_attributes=True  # Allow creating from ORM-like objects (e.g. dicts)
    )


class Feature(FeatureProperties):
    """Represents a single feature, inheriting core properties."""

    def get_id(self) -> uuid.UUID:
        return self.geoid

    def to_geojson_feature(
        self, layer_id: str, property_names: Optional[List[str]] = None
    ) -> dict:
        """Converts the WFSFeature into a GeoJSON Feature dictionary."""

        geometry = None
        bbox = self.bbox
        if self.geom:
            try:
                from shapely import wkb as shapely_wkb
                shapely_geom = shapely_wkb.loads(self.geom)
                geometry = shapely_geom.__geo_interface__
                if not bbox:
                    bbox = shapely_geom.bounds
            except Exception:
                # In case of invalid geometry, we'll just have a null geometry feature
                pass

        if property_names:
            properties = {}
            # Get all available top-level properties first, excluding the attributes dict itself
            all_props = self.model_dump(
                exclude={"geom", "geom_gml", "bbox", "attributes"}
            )

            for prop_name in property_names:
                # Case 1: Promote all attributes to top-level properties
                if prop_name == "attributes.*":
                    if self.attributes:
                        properties.update(self.attributes)
                    continue

                # Case 2: Promote a specific attribute to a top-level property
                if prop_name.startswith("attributes."):
                    attr_key = prop_name.split(".", 1)[1]
                    if self.attributes and attr_key in self.attributes:
                        properties[attr_key] = self.attributes[attr_key]
                    continue

                # Case 3: Select a regular top-level property
                if prop_name in all_props:
                    properties[prop_name] = all_props[prop_name]
        else:
            # Default behavior: dump all properties, with 'attributes' as a nested object
            properties = self.model_dump(
                exclude={"geom", "geom_gml", "bbox", "deleted_at"}
            )

        feature = {
            "type": "Feature",
            "id": f"{layer_id}.{self.get_id()}",
            "geometry": geometry,
            "properties": properties,
        }
        if bbox:
            feature["bbox"] = bbox
        return feature


class FeatureCollection:
    """
    A proxy collection that provides a view over raw database results.
    Supports both synchronous list/cursor and asynchronous stream cursor.
    """

    def __init__(
        self, raw_result_cursor, total_count: int = 0, number_returned: int = 0
    ):
        self._cursor = raw_result_cursor
        self.total_count = total_count
        self.number_returned = number_returned

        # Legacy support: if cursor has 'total_count' in first row (sync only)
        # and total_count wasn't provided, try to peek.
        self._first_row = None
        if self.total_count == 0 and self._cursor and not self._is_async_cursor():
            try:
                # Fetch the first row to get the total_count without consuming the cursor
                self._first_row = next(self._cursor.mappings(), None)
                if self._first_row:
                    self.total_count = self._first_row.get("total_count", 0)
            except (StopIteration, AttributeError):
                self._first_row = None

    def _is_async_cursor(self):
        return (
            inspect.isasyncgen(self._cursor)
            or hasattr(self._cursor, "__aiter__")
            or inspect.iscoroutine(self._cursor)
        )

    def __iter__(self) -> Generator[Feature, None, None]:
        """Yields WFSFeature objects one by one (Synchronous)."""
        if not self._cursor:
            return

        if self._is_async_cursor():
            raise NotImplementedError(
                f"Cannot synchronously iterate over an async cursor {type(self._cursor)}. Use async for."
            )

        # Chain the already-fetched first row with the rest of the cursor
        effective_iterator = itertools.chain(
            [self._first_row] if self._first_row else [], self._cursor.mappings()
        )
        for item in effective_iterator:
            self.number_returned += 1
            yield Feature.model_validate(dict(item))

    async def __aiter__(self):
        """Yields WFSFeature objects one by one (Asynchronous)."""
        if not self._cursor:
            return

        # If _cursor is a coroutine (not yet awaited), await it first
        # This typically happens if the cursor provider forgot to await the execute/stream call
        # or if the result is lazy. But here we assume we are passed an iterator or async iterator.
        # If it is a coroutine, it likely returns an iterator?
        if inspect.iscoroutine(self._cursor):
            self._cursor = await self._cursor

        if not self._is_async_cursor():
            # Fallback for sync cursor in async context
            # However, we must call __iter__ explicitly if it's not async
            # But __iter__ implementation above calls .mappings() which might be what we want
            for item in self:
                yield item
            return

        async for item in self._cursor:
            self.number_returned += 1
            yield Feature.model_validate(dict(item))

    async def to_streaming_geojson(
        self, layer_id: str, property_names: Optional[List[str]] = None
    ):
        """
        Yields the collection as a streaming GeoJSON FeatureCollection bytestring.
        This is suitable for use with FastAPI's StreamingResponse.
        """
        # Yield the start of the JSON object and the features array key
        yield b'{"type":"FeatureCollection","features":['

        is_first = True

        # Use aiter to handle both sync and async cursors transparently
        async for feature in self:
            if not is_first:
                yield b","

            # Convert feature to GeoJSON dict and then to a JSON bytestring
            feature_dict = feature.to_geojson_feature(
                layer_id, property_names=property_names
            )
            yield orjson.dumps(feature_dict, default=orjson_default)
            is_first = False

        # Close the features array and add the summary fields at the end.
        yield b'],"numberMatched":'
        # Handle None total_count gracefully by emitting 'null' (though 0 is preferred)
        if self.total_count is None:
            yield b"0"  # WFS standard usually implies 0 or unknown. 0 is safer if empty.
        else:
            yield str(self.total_count).encode("utf-8")
        yield b',"numberReturned":'
        # Note: number_returned is updated during iteration
        yield str(self.number_returned).encode("utf-8")
        yield (
            b',"timeStamp":"'
            + datetime.now(timezone.utc).isoformat().encode("utf-8")
            + b'Z"}'
        )

    def get_total_count(self) -> int:
        """Returns the total number of features matching the query."""
        return self.total_count

    def get_number_returned(self) -> int:
        """Returns the number of features returned by the query."""
        return self.number_returned
