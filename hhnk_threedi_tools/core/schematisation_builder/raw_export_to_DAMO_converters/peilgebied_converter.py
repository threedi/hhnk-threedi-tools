import uuid

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.features import rasterize
from scipy import ndimage
from shapely.geometry import LineString, MultiLineString, Point

from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converter import RawExportToDAMOConverter

# Input layers for peilgebiedpraktijk
PEILGEBIED_SOURCE_LAYERS = ["combinatiepeilgebied", "hydrodeelgebied", "peilgebiedpraktijk", "waterbergingsgebied"]

# Input layers for waterkering
WATERKERING_SOURCE_LAYERS = [
    "combinatiepeilgebied",
    "hydrodeelgebied",
    "peilgebiedpraktijk",
    "waterbergingsgebied",
    "levees",
    "verhoogde lijnen",
    "wegen",
]

# Columns for peilgebiedpraktijk according to DAMO schema
PEILGEBIEDPRAKTIJK_COLUMNS = [
    "objectid",
    "statusPeilgebied",
    "voertAfOp",
    "bevat",
]

# Columns for waterkering according to DAMO schema
WATERKERING_COLUMNS = [
    "objectid",
    "categorie",
    "typeWaterkering",
    "soortReferentielijn",
    "waterstaatswerkWaterkeringID",
]

# Configuration for height extraction
DEFAULT_SEGMENT_LENGTH = 50.0  # meters
DEFAULT_BUFFER_WIDTH = 10.0  # meters


class PeilgebiedConverter(RawExportToDAMOConverter):
    """
    Convert raw export peilgebied data to DAMO schema format.

    This converter creates two DAMO-compliant layers:
    1. peilgebiedpraktijk (polygon) - built from layers defined in PEILGEBIED_SOURCE_LAYERS
    2. waterkering (linestring) - built from layers defined in WATERKERING_SOURCE_LAYERS

    Combines multiple source layers into unified DAMO-compliant output layers with proper schema mapping.
    """

    def __init__(self, raw_export_converter: RawExportToDAMOConverter):
        self.data = raw_export_converter.data
        self.logger = raw_export_converter.logger

    def run(self):
        """Execute the converter to create peilgebiedpraktijk and waterkering layers."""
        self.logger.info("Running PeilgebiedConverter...")
        self.update_peilgebied_layers()

    def update_peilgebied_layers(self):
        """Update peilgebiedpraktijk and waterkering layers."""
        self.logger.info("Updating peilgebied layers...")
        self._create_peilgebiedpraktijk_layer()
        self._create_waterkering_layer()

    def _create_peilgebiedpraktijk_layer(self) -> None:
        """Create DAMO-compliant peilgebiedpraktijk layer from PEILGEBIED_SOURCE_LAYERS."""
        self.logger.info("Creating peilgebiedpraktijk layer...")

        available_layers = self._load_source_layers(PEILGEBIED_SOURCE_LAYERS)
        if not available_layers:
            self.logger.warning("No source layers found for peilgebiedpraktijk")
            return

        combined_gdf = self._combine_layers(available_layers)
        if combined_gdf is None or combined_gdf.empty:
            self.logger.warning("Combined peilgebiedpraktijk layer is empty")
            return

        combined_gdf = self._ensure_polygon_geometry(combined_gdf)
        damo_gdf = self._map_to_damo_peilgebiedpraktijk(combined_gdf)

        self.data.peilgebiedpraktijk = damo_gdf
        self.logger.info(f"Created peilgebiedpraktijk layer with {len(damo_gdf)} features")

    def _create_waterkering_layer(self) -> None:
        """Create DAMO-compliant waterkering layer from WATERKERING_SOURCE_LAYERS."""
        self.logger.info("Creating waterkering layer...")

        available_layers = self._load_source_layers(WATERKERING_SOURCE_LAYERS)
        if not available_layers:
            self.logger.warning("No source layers found for waterkering")
            return

        combined_gdf = self._combine_layers(available_layers)
        if combined_gdf is None or combined_gdf.empty:
            self.logger.warning("Combined waterkering layer is empty")
            return

        combined_gdf = self._convert_to_linestring(combined_gdf)

        # Extract heights from DEM if available
        if self.data.dem_dataset is not None:
            combined_gdf = self._extract_heights_from_dem(
                combined_gdf,
                segment_length=DEFAULT_SEGMENT_LENGTH,
                search_distance=DEFAULT_BUFFER_WIDTH,
            )
        else:
            self.logger.warning("DEM not available, skipping height extraction")

        damo_gdf = self._map_to_damo_waterkering(combined_gdf)

        # Remove features with null geometry
        initial_count = len(damo_gdf)
        damo_gdf = damo_gdf[damo_gdf.geometry.notna()].copy()
        removed_count = initial_count - len(damo_gdf)
        if removed_count > 0:
            self.logger.warning(f"Removed {removed_count} features with null geometry")

        self.data.waterkering = damo_gdf
        self.logger.info(f"Created waterkering layer with {len(damo_gdf)} features")

    def _load_source_layers(self, layer_names: list[str]) -> dict[str, gpd.GeoDataFrame]:
        """Load source layers that are available in the data.

        Args:
            layer_names: List of layer names to load

        Returns:
            Dictionary with layer name as key and GeoDataFrame as value
        """
        available_layers = {}

        for layer_name in layer_names:
            # Check if attribute exists on data object
            gdf = getattr(self.data, layer_name.lower(), None)

            if gdf is None:
                self.logger.debug(f"Source layer '{layer_name}' not found in data")
                continue

            if not gdf.empty:
                available_layers[layer_name] = gdf
                self.logger.info(f"Loaded source layer '{layer_name}' with {len(gdf)} features")
            else:
                self.logger.debug(f"Source layer '{layer_name}' is empty")

        return available_layers

    def _combine_layers(self, layers_dict: dict[str, gpd.GeoDataFrame]) -> gpd.GeoDataFrame | None:
        """Combine multiple GeoDataFrames into one.

        Args:
            layers_dict: Dictionary with layer name as key and GeoDataFrame as value

        Returns:
            Combined GeoDataFrame or None if input is empty
        """
        if not layers_dict:
            return None

        # Add source layer information to each GeoDataFrame
        gdfs_with_source = []
        for layer_name, gdf in layers_dict.items():
            gdf_copy = gdf.copy()
            gdf_copy["source_layer"] = layer_name
            gdfs_with_source.append(gdf_copy)

        combined = gpd.GeoDataFrame(pd.concat(gdfs_with_source, ignore_index=True))
        self.logger.info(f"Combined {len(layers_dict)} layers into {len(combined)} features")
        return combined

    def _ensure_polygon_geometry(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Ensure all geometries are polygons.

        Args:
            gdf: Input GeoDataFrame

        Returns:
            GeoDataFrame with polygon geometries only
        """
        polygon_mask = gdf.geometry.type.isin(["Polygon", "MultiPolygon"])

        if not polygon_mask.all():
            non_polygon_count = (~polygon_mask).sum()
            self.logger.warning(f"Removing {non_polygon_count} non-polygon geometries")
            gdf = gdf[polygon_mask].copy()

        return gdf

    def _convert_to_linestring(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Convert polygon geometries to linestring by extracting boundaries.

        Extracts exterior boundaries from polygons and removes duplicate lines
        that occur where polygons share boundaries.

        Args:
            gdf: Input GeoDataFrame with polygons

        Returns:
            GeoDataFrame with unique linestring geometries
        """

        def extract_boundary(geom):
            """Extract boundary from polygon or multipolygon."""
            if geom is None:
                return None

            geom_type = geom.geom_type
            if geom_type == "Polygon":
                return LineString(geom.exterior.coords)
            if geom_type == "MultiPolygon":
                return MultiLineString([LineString(polygon.exterior.coords) for polygon in geom.geoms])
            if geom_type in ("LineString", "MultiLineString"):
                return geom

            self.logger.warning(f"Unexpected geometry type: {geom_type}")
            return None

        result = gdf.copy()
        result.geometry = result.geometry.apply(extract_boundary)
        result = result[result.geometry.notna()].copy()

        # Remove duplicate lines by converting to WKT and back
        initial_count = len(result)
        result = self._remove_duplicate_linestrings(result)
        removed_count = initial_count - len(result)

        if removed_count > 0:
            self.logger.info(f"Removed {removed_count} duplicate linestrings from waterkering layer")

        return result

    def _remove_duplicate_linestrings(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Remove duplicate linestrings based on geometry.

        Compares linestrings in both directions to catch reversed duplicates
        (e.g., line A->B is the same as B->A for a boundary).
        Uses coordinate tuple hashing for fast comparison.

        Args:
            gdf: Input GeoDataFrame with linestring geometries

        Returns:
            GeoDataFrame with unique linestrings only
        """
        if gdf.empty:
            return gdf

        def get_coord_hash(geom):
            """Create a hashable representation of coordinates, handling both directions."""
            if geom is None or geom.is_empty:
                return None

            if geom.geom_type == "LineString":
                coords = tuple(geom.coords)
                # Create canonical form: always use the lexicographically smaller direction
                reversed_coords = tuple(reversed(coords))
                return min(coords, reversed_coords)
            elif geom.geom_type == "MultiLineString":
                # For MultiLineString, use WKB as it's faster than WKT
                return geom.wkb
            else:
                return None

        # Vectorized approach: create hash column
        gdf = gdf.copy()
        gdf["_coord_hash"] = gdf.geometry.apply(get_coord_hash)

        # Remove None values
        gdf = gdf[gdf["_coord_hash"].notna()].copy()

        # Drop duplicates based on coordinate hash (much faster than iterrows)
        gdf = gdf.drop_duplicates(subset=["_coord_hash"], keep="first")

        # Remove helper column
        gdf = gdf.drop(columns=["_coord_hash"])

        return gdf

    def _map_to_damo_peilgebiedpraktijk(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Map attributes to DAMO peilgebiedpraktijk schema.

        Args:
            gdf: Input GeoDataFrame

        Returns:
            GeoDataFrame with DAMO schema
        """
        result = gpd.GeoDataFrame()

        # OBJECTID - will be auto-generated by the database (EsriFieldTypeOID)
        # Not included in result

        # statusPeilgebied - PeilgebiedStatus
        result["statusPeilgebied"] = self._get_column_value(
            gdf, ["statusPeilgebied", "status_peilgebied", "status"], "statusPeilgebied"
        )

        # voertAfOp - GUID (GlobalID waar dit peilgebied op afvoert)
        result["voertAfOp"] = self._get_column_value(gdf, ["voertAfOp", "voert_af_op"], "voertAfOp")

        # bevat - GUID (GlobalID dat afvoert op dit peilgebied)
        result["bevat"] = self._get_column_value(gdf, ["bevat"], "bevat")

        # Set geometry
        result.set_geometry(gdf.geometry, crs=gdf.crs, inplace=True)

        self.logger.info(f"Mapped {len(result)} features to DAMO peilgebiedpraktijk schema")
        return result

    def _map_to_damo_waterkering(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Map attributes to DAMO waterkering schema.

        Args:
            gdf: Input GeoDataFrame

        Returns:
            GeoDataFrame with DAMO schema
        """
        result = gpd.GeoDataFrame()

        # OBJECTID - will be auto-generated by the database (esriFieldTypeOID)
        # Not included in result

        # categorie - CategorieWaterkering
        result["categorie"] = self._get_column_value(gdf, ["categorie"], "categorie")

        # typeWaterkering - TypeWaterkering
        result["typeWaterkering"] = self._get_column_value(
            gdf, ["typeWaterkering", "type_waterkering"], "typeWaterkering"
        )

        # soortReferentielijn - TypeReferentielijn
        result["soortReferentielijn"] = self._get_column_value(
            gdf, ["soortReferentielijn", "soort_referentielijn"], "soortReferentielijn"
        )

        # waterstaatswerkWaterkeringID - GUID
        waterkering_id = self._get_column_value(
            gdf, ["waterstaatswerkWaterkeringID", "waterstaatswerk_waterkering_id"], "waterstaatswerkWaterkeringID"
        )
        # Generate new GUIDs if not present
        if waterkering_id is None or waterkering_id.isna().all():
            result["waterstaatswerkWaterkeringID"] = [str(uuid.uuid4()) for _ in range(len(gdf))]
            self.logger.debug("Generated new GUIDs for waterstaatswerkWaterkeringID")
        else:
            result["waterstaatswerkWaterkeringID"] = waterkering_id

        # Preserve min_height if it was extracted from DEM
        if "min_height" in gdf.columns:
            result["min_height"] = gdf["min_height"]

        # Preserve length_m if it was calculated
        if "length_m" in gdf.columns:
            result["length_m"] = gdf["length_m"]

        # Set geometry
        result.set_geometry(gdf.geometry, crs=gdf.crs, inplace=True)

        self.logger.info(f"Mapped {len(result)} features to DAMO waterkering schema")
        return result

    def _get_column_value(self, gdf: gpd.GeoDataFrame, column_names: list[str], target_name: str):
        """Get column value from GeoDataFrame, trying multiple possible column names.

        Args:
            gdf: Input GeoDataFrame
            column_names: List of possible column names to try
            target_name: Target column name for logging

        Returns:
            Series with values or None if no column found
        """
        for col_name in column_names:
            if col_name in gdf.columns:
                return gdf[col_name]

        self.logger.debug(f"Column '{target_name}' not found in source data")
        return None

    def _extract_heights_from_dem(
        self, gdf: gpd.GeoDataFrame, segment_length: float = 50.0, search_distance: float = 10.0
    ) -> gpd.GeoDataFrame:
        """Extract crest heights from DEM for linestrings using perpendicular sampling.

        Uses sophisticated perpendicular sampling to detect the crest (maximum elevation)
        on both sides of the line, similar to the threedi_beta_processing algorithm.
        Splits lines into segments and creates a separate feature for each segment with its own height.

        Args:
            gdf: GeoDataFrame with linestring geometries
            segment_length: Maximum length of line segments in meters (default: 50m)
            search_distance: Distance to search perpendicular to line in meters (default: 10m)

        Returns:
            GeoDataFrame with split segments as separate features, each with 'min_height' column
        """
        if self.data.dem_dataset is None:
            self.logger.warning("No DEM available for height extraction")
            return gdf

        self.logger.info(
            f"Splitting lines and extracting crest heights (segment_length={segment_length}m, search_distance={search_distance}m)"
        )

        dem = self.data.dem_dataset

        # Collect all segments as separate features
        segments_data = []

        for idx, row in gdf.iterrows():
            geom = row.geometry

            if geom is None or geom.is_empty:
                continue

            # Split the line into individual segments
            segments = self._split_line_into_segments(geom, segment_length)

            # Extract height for each segment using perpendicular sampling
            for segment_geom in segments:
                # Extract crest height using perpendicular sampling
                crest_height = self._sample_crest_height_perpendicular(segment_geom, dem, search_distance)

                # Round height to 2 decimals to reduce data size
                if crest_height is not None:
                    crest_height = round(crest_height, 2)

                # Calculate segment length and round to 2 decimals
                segment_length_m = round(segment_geom.length, 2)

                # Create a new row for this segment with all original attributes
                segment_row = row.copy()
                segment_row["geometry"] = segment_geom
                segment_row["min_height"] = crest_height
                segment_row["length_m"] = segment_length_m
                segments_data.append(segment_row)

        if not segments_data:
            self.logger.warning("No segments created")
            return gpd.GeoDataFrame(columns=gdf.columns.tolist() + ["min_height", "length_m"], crs=gdf.crs)

        # Create new GeoDataFrame with all segments
        result = gpd.GeoDataFrame(segments_data, crs=gdf.crs)

        # Reset index to avoid duplicate index issues (multiple segments from same source line)
        result = result.reset_index(drop=True)

        # Log statistics
        valid_heights = result["min_height"].dropna()
        self.logger.info(
            f"Split {len(gdf)} lines into {len(result)} segments (avg {len(result) / len(gdf):.1f} segments per line)"
        )

        if len(valid_heights) > 0:
            self.logger.info(
                f"Extracted crest heights for {len(valid_heights)} segments "
                f"(min: {valid_heights.min():.2f}m, max: {valid_heights.max():.2f}m)"
            )
        else:
            self.logger.warning("No valid heights extracted from DEM")

        return result

    def _split_line_into_segments(self, geom, max_length: float) -> list[LineString]:
        """Split a line geometry into separate LineString segments.

        Args:
            geom: LineString or MultiLineString
            max_length: Maximum segment length

        Returns:
            List of LineString segments, each with length <= max_length
        """
        segments = []

        if geom.geom_type == "LineString":
            segments.extend(self._split_single_linestring(geom, max_length))
        elif geom.geom_type == "MultiLineString":
            for line in geom.geoms:
                segments.extend(self._split_single_linestring(line, max_length))

        return segments

    def _split_single_linestring(self, line: LineString, max_length: float) -> list[LineString]:
        """Split a single LineString into segments of approximately max_length.

        Args:
            line: LineString to split
            max_length: Target maximum length for segments

        Returns:
            List of LineString segments
        """
        if line.length <= max_length:
            return [line]

        # Calculate number of segments needed
        n_segments = int(np.ceil(line.length / max_length))
        segment_length = line.length / n_segments

        segments = []
        distance_along = 0.0

        while distance_along < line.length:
            # Get point at current distance
            start_point = line.interpolate(distance_along)

            # Get point at next distance (or end of line)
            end_distance = min(distance_along + segment_length, line.length)
            end_point = line.interpolate(end_distance)

            # Create segment
            # We need to extract the actual linestring between these two points
            # Use line.project to get distances, then substring
            start_dist = distance_along
            end_dist = end_distance

            # Extract substring of original line
            if end_dist >= line.length:
                # Last segment - take from start to end
                segment = LineString(
                    [
                        line.interpolate(d)
                        for d in np.linspace(start_dist, line.length, max(2, int((line.length - start_dist) / 10) + 1))
                    ]
                )
            else:
                # Create segment by interpolating points
                segment = LineString(
                    [
                        line.interpolate(d)
                        for d in np.linspace(start_dist, end_dist, max(2, int(segment_length / 10) + 1))
                    ]
                )

            if segment.length > 0:
                segments.append(segment)

            distance_along += segment_length

        return segments

    def _segmentize_line(self, geom, max_length: float):
        """Segmentize a line geometry into smaller segments.

        DEPRECATED: Use _split_line_into_segments instead for creating separate features.
        This method only adds vertices but doesn't split into separate LineStrings.

        Args:
            geom: LineString or MultiLineString
            max_length: Maximum segment length

        Returns:
            Segmentized geometry
        """
        if geom.geom_type == "LineString":
            # Shapely's segmentize for LineString
            return geom.segmentize(max_length)
        elif geom.geom_type == "MultiLineString":
            # Process each line in MultiLineString
            segmented_lines = [line.segmentize(max_length) for line in geom.geoms]
            return MultiLineString(segmented_lines)
        else:
            return geom

    def _sample_crest_height_perpendicular(
        self, line: LineString, dem_dataset, search_distance: float, step_distance: float = 2.0
    ) -> float | None:
        """Extract crest height using perpendicular sampling.

        Samples the DEM perpendicular to the line at regular intervals,
        finds the maximum elevation on each side (the crest), and returns
        a weighted average along the line.

        This is based on the threedi_beta_processing crest level algorithm.

        Args:
            line: LineString geometry
            dem_dataset: Open rasterio dataset
            search_distance: Distance to search perpendicular to line in meters
            step_distance: Distance between perpendicular sample lines in meters

        Returns:
            Weighted average crest height or None if no valid data
        """
        try:
            # Get DEM pixel size
            pixel_size = abs(dem_dataset.transform[0])

            # Calculate number of points along the line
            line_length = line.length
            if line_length < 0.1:  # Too short
                return None

            n_samples = max(2, int(line_length / step_distance) + 1)

            # Sample points along the line
            distances = np.linspace(0, line_length, n_samples)
            sample_points = [line.interpolate(d) for d in distances]

            # Get perpendicular vectors at each point
            heights = []
            weights = []

            for i, point in enumerate(sample_points):
                # Get line direction at this point
                if i == 0:
                    # Use direction to next point
                    next_point = sample_points[i + 1]
                    dx = next_point.x - point.x
                    dy = next_point.y - point.y
                elif i == len(sample_points) - 1:
                    # Use direction from previous point
                    prev_point = sample_points[i - 1]
                    dx = point.x - prev_point.x
                    dy = point.y - prev_point.y
                else:
                    # Use average direction
                    prev_point = sample_points[i - 1]
                    next_point = sample_points[i + 1]
                    dx = next_point.x - prev_point.x
                    dy = next_point.y - prev_point.y

                # Normalize direction vector
                length = np.sqrt(dx**2 + dy**2)
                if length < 0.001:
                    continue
                dx /= length
                dy /= length

                # Get perpendicular vector (rotate 90 degrees)
                perp_dx = -dy
                perp_dy = dx

                # Sample along perpendicular line
                n_perp_samples = max(3, int(search_distance / pixel_size) + 1)
                perp_distances = np.linspace(-search_distance, search_distance, n_perp_samples)

                perp_heights = []
                for d in perp_distances:
                    sample_x = point.x + perp_dx * d
                    sample_y = point.y + perp_dy * d

                    # Sample DEM at this point
                    row, col = ~dem_dataset.transform * (sample_x, sample_y)
                    row, col = int(row), int(col)

                    if 0 <= row < dem_dataset.height and 0 <= col < dem_dataset.width:
                        try:
                            height = dem_dataset.read(1, window=((row, row + 1), (col, col + 1)))[0, 0]
                            if not np.isnan(height) and height != dem_dataset.nodata:
                                perp_heights.append(height)
                        except Exception:
                            pass

                if perp_heights:
                    # Take maximum (crest) from perpendicular samples
                    crest = np.max(perp_heights)
                    heights.append(crest)
                    # Weight by segment length (distance to next sample point)
                    if i < len(sample_points) - 1:
                        segment_length = step_distance
                    else:
                        segment_length = line_length - distances[i - 1] if i > 0 else line_length
                    weights.append(segment_length)

            if not heights:
                # Fallback to buffer method if perpendicular sampling found nothing
                self.logger.debug("Perpendicular sampling found no heights, trying buffer fallback")
                return self._get_height_from_buffer_fallback(line, dem_dataset, search_distance)

            # Return weighted average
            heights = np.array(heights)
            weights = np.array(weights)
            weighted_avg = np.average(heights, weights=weights)

            return float(weighted_avg)

        except Exception as e:
            self.logger.debug(f"Error in perpendicular sampling: {e}, trying fallback")
            # Fallback to buffer method
            return self._get_height_from_buffer_fallback(line, dem_dataset, search_distance)

    def _get_height_from_buffer_fallback(self, line: LineString, dem_dataset, buffer_distance: float) -> float | None:
        """Fallback method: extract maximum height from buffered line.

        Args:
            line: LineString geometry
            dem_dataset: Open rasterio dataset
            buffer_distance: Buffer distance in meters

        Returns:
            Maximum height or None if no valid data
        """
        try:
            buffered = line.buffer(buffer_distance)
            minx, miny, maxx, maxy = buffered.bounds

            # Convert bounds to pixel coordinates
            row_start, col_start = dem_dataset.index(minx, maxy)
            row_stop, col_stop = dem_dataset.index(maxx, miny)

            # Ensure valid ranges
            row_start = max(0, row_start)
            col_start = max(0, col_start)
            row_stop = min(dem_dataset.height, row_stop + 1)
            col_stop = min(dem_dataset.width, col_stop + 1)

            if row_start >= row_stop or col_start >= col_stop:
                return None

            # Read window
            window = rasterio.windows.Window(col_start, row_start, col_stop - col_start, row_stop - row_start)
            data = dem_dataset.read(1, window=window, masked=True)

            if data.size == 0 or data.mask.all():
                return None

            # For crest detection, we want the maximum (not minimum)
            return float(np.max(data.compressed()))

        except Exception as e:
            self.logger.debug(f"Error in buffer fallback: {e}")
            return None

    def _get_min_height_from_buffered_geometry(self, buffered_geom, dem):
        """Extract minimum height from DEM for a buffered geometry using windowed reading.

        Args:
            buffered_geom: Buffered polygon geometry
            dem: Rasterio dataset

        Returns:
            Minimum height value or None if no valid data
        """
        # Get bounding box
        minx, miny, maxx, maxy = buffered_geom.bounds

        # Convert bounds to pixel coordinates
        try:
            row_start, col_start = dem.index(minx, maxy)
            row_stop, col_stop = dem.index(maxx, miny)

            # Ensure valid ranges
            row_start = max(0, row_start)
            col_start = max(0, col_start)
            row_stop = min(dem.height, row_stop + 1)
            col_stop = min(dem.width, col_stop + 1)

            # Read window
            window = rasterio.windows.Window(col_start, row_start, col_stop - col_start, row_stop - row_start)
            data = dem.read(1, window=window, masked=True)

            if data.size == 0:
                return None

            # Get window transform
            window_transform = dem.window_transform(window)

            # Create mask for the buffered geometry within the window
            # This is the fastest approach: rasterize the geometry
            mask = rasterize(
                [(buffered_geom, 1)], out_shape=data.shape, transform=window_transform, fill=0, dtype=np.uint8
            ).astype(bool)

            # Extract values within the geometry
            masked_data = np.ma.masked_array(data, mask=~mask)

            # Get minimum value (ignoring nodata)
            if masked_data.count() > 0:
                return float(np.min(masked_data.compressed()))
            else:
                return None

        except Exception as e:
            self.logger.debug(f"Error extracting height: {e}")
            return None
