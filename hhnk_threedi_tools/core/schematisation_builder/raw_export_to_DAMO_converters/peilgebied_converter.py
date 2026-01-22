import uuid
from typing import Optional, TypedDict, Union

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.features import rasterize
from shapely.geometry import LineString, MultiLineString, Point
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converter import RawExportToDAMOConverter


class PerpendicularSample(TypedDict):
    """Type definition for perpendicular sample data structure."""

    geometry: LineString
    height_min: float
    height_p10: float
    height_avg: float
    height_p90: float
    height_max: float
    sample_index: int
    n_samples: int
    n_total: int
    center_x: float
    center_y: float


PEILGEBIED_SOURCE_LAYER = "combinatiepeilgebied"

WATERKERING_LINE_LAYERS = [
    "levees",
    "verhoogde lijnen",
    "wegen",
]

PEILGEBIEDPRAKTIJK_COLUMNS = [
    "objectid",
    "statusPeilgebied",
    "voertAfOp",
    "bevat",
]

WATERKERING_COLUMNS = [
    "objectid",
    "categorie",
    "typeWaterkering",
    "soortReferentielijn",
    "waterstaatswerkWaterkeringID",
]

DEFAULT_SEGMENT_LENGTH = 50.0
DEFAULT_BUFFER_WIDTH = 10.0
DEFAULT_PERPENDICULAR_SAMPLE_DISTANCE = 5.0
EXPORT_PERPENDICULAR_SAMPLES = False
SNAP_TOLERANCE = 0.01


class PeilgebiedConverter(RawExportToDAMOConverter):
    """Convert raw peilgebied exports to DAMO schema (peilgebiedpraktijk and waterkering layers)."""

    def __init__(self, raw_export_converter: RawExportToDAMOConverter):
        self.data = raw_export_converter.data
        self.logger = raw_export_converter.logger
        self.perpendicular_samples: list[PerpendicularSample] = []

    def run(self) -> None:
        """Create peilgebiedpraktijk and waterkering layers."""
        self.logger.info("Running PeilgebiedConverter...")
        self._create_peilgebiedpraktijk_layer()
        self._create_waterkering_layer()

    def _create_peilgebiedpraktijk_layer(self) -> None:
        """Create DAMO-compliant peilgebiedpraktijk layer from combinatiepeilgebied."""
        self.logger.info("Creating peilgebiedpraktijk layer...")

        gdf = getattr(self.data, PEILGEBIED_SOURCE_LAYER.lower(), None)

        if gdf is None or gdf.empty:
            self.logger.warning(f"Source layer '{PEILGEBIED_SOURCE_LAYER}' not found or empty")
            return

        self.logger.info(f"Loaded '{PEILGEBIED_SOURCE_LAYER}' with {len(gdf)} features")

        damo_gdf = self._map_to_damo_peilgebiedpraktijk(gdf)

        self.data.peilgebiedpraktijk = damo_gdf
        self.logger.info(f"Created peilgebiedpraktijk layer with {len(damo_gdf)} features")

    def _create_waterkering_layer(self) -> None:
        """Create waterkering layer from peilgebiedpraktijk boundaries and line layers."""
        self.logger.info("Creating waterkering layer...")

        all_geometries = []

        # Extract boundaries from processed peilgebiedpraktijk
        if self.data.peilgebiedpraktijk is not None and not self.data.peilgebiedpraktijk.empty:
            self.logger.info(
                f"Extracting boundaries from {len(self.data.peilgebiedpraktijk)} peilgebiedpraktijk polygons"
            )
            for geom in self.data.peilgebiedpraktijk.geometry:
                if geom is not None and not geom.is_empty:
                    if geom.geom_type == "Polygon":
                        all_geometries.append(LineString(geom.exterior.coords))
                    elif geom.geom_type == "MultiPolygon":
                        for polygon in geom.geoms:
                            all_geometries.append(LineString(polygon.exterior.coords))
            self.logger.info(f"  Extracted {len(all_geometries)} boundary lines")
        else:
            self.logger.warning("No peilgebiedpraktijk layer available for boundary extraction")

        # Add dedicated line layers
        available_line_layers = self._load_source_layers(WATERKERING_LINE_LAYERS)
        if available_line_layers:
            self.logger.info(f"Loading {len(available_line_layers)} dedicated line layers")
            for layer_name, gdf in available_line_layers.items():
                for geom in gdf.geometry:
                    if geom is not None and not geom.is_empty:
                        if geom.geom_type == "LineString":
                            all_geometries.append(geom)
                        elif geom.geom_type == "MultiLineString":
                            all_geometries.extend(geom.geoms)
                self.logger.info(f"  Added {len(gdf)} features from '{layer_name}'")

        if not all_geometries:
            self.logger.warning("No line sources found for waterkering")
            return

        self.logger.info(f"Total input: {len(all_geometries)} line geometries")

        # Create topologically clean network: snap, union, explode, deduplicate
        self.logger.info("Creating topologically clean network...")

        snap_tolerance = 0.001
        snapped_lines = []
        for geom in all_geometries:
            if geom.geom_type == "LineString":
                snapped_coords = [
                    (round(x / snap_tolerance) * snap_tolerance, round(y / snap_tolerance) * snap_tolerance)
                    for x, y in geom.coords
                ]
                if len(snapped_coords) >= 2:  # LineString needs at least 2 points
                    snapped_lines.append(LineString(snapped_coords))
            elif geom.geom_type == "MultiLineString":
                for line in geom.geoms:
                    snapped_coords = [
                        (round(x / snap_tolerance) * snap_tolerance, round(y / snap_tolerance) * snap_tolerance)
                        for x, y in line.coords
                    ]
                    if len(snapped_coords) >= 2:
                        snapped_lines.append(LineString(snapped_coords))

        self.logger.info(f"Snapped {len(snapped_lines)} lines to {snap_tolerance}m grid")

        network = unary_union(snapped_lines)

        # Explode network into segments
        raw_segments = []
        if network.geom_type == "LineString":
            raw_segments.append(network)
        elif network.geom_type == "MultiLineString":
            raw_segments.extend(network.geoms)
        elif network.geom_type == "GeometryCollection":
            for geom in network.geoms:
                if geom.geom_type == "LineString":
                    raw_segments.append(geom)
                elif geom.geom_type == "MultiLineString":
                    raw_segments.extend(geom.geoms)

        self.logger.info(f"Network exploded into {len(raw_segments)} raw segments")

        # Remove duplicate/near-duplicate segments
        unique_segments = []

        for i, segment in enumerate(raw_segments):
            is_duplicate = False
            seg_buffer = segment.buffer(0.05)  # 5cm buffer for near-duplicate detection

            for existing in unique_segments:
                # Check if segments are nearly identical (>90% overlap in both directions)
                intersection = seg_buffer.intersection(existing)
                if not intersection.is_empty:
                    overlap_with_existing = intersection.length / existing.length
                    overlap_with_current = intersection.length / segment.length

                    # If both segments overlap >90% with each other, they're duplicates
                    if overlap_with_existing > 0.90 and overlap_with_current > 0.90:
                        is_duplicate = True
                        break

            if not is_duplicate:
                unique_segments.append(segment)

        duplicates_removed = len(raw_segments) - len(unique_segments)
        if duplicates_removed > 0:
            self.logger.info(f"Removed {duplicates_removed} duplicate/near-duplicate segments")

        clean_lines = unique_segments

        if not clean_lines:
            self.logger.warning("Network creation produced no valid lines")
            return

        self.logger.info(f"Network contains {len(clean_lines)} unique line segments")

        lines_gdf = gpd.GeoDataFrame(
            {"geometry": clean_lines, "source_layer": "peilgebiedpraktijk_boundary"},
            crs=self.data.peilgebiedpraktijk.crs if self.data.peilgebiedpraktijk is not None else "EPSG:28992",
        )

        lines_gdf = self._split_lines_into_segments(lines_gdf, max_segment_length=DEFAULT_SEGMENT_LENGTH)

        self.logger.info(f"Removing overlapping segments (initial: {len(lines_gdf)})...")
        lines_gdf = self._remove_overlapping_segments_robust(lines_gdf)
        self.logger.info(f"After deduplication: {len(lines_gdf)} segments")

        initial_count = len(lines_gdf)
        lines_gdf = lines_gdf[lines_gdf.geometry.length >= 0.1].copy()
        removed_short = initial_count - len(lines_gdf)
        if removed_short > 0:
            self.logger.info(f"Removed {removed_short} segments shorter than 0.1m")

        if self.data.dem_dataset is not None:
            lines_gdf = self._extract_heights_for_segments(lines_gdf, search_distance=DEFAULT_BUFFER_WIDTH)
        else:
            self.logger.warning("DEM not available, skipping height extraction")
            lines_gdf["min_height"] = None

        damo_gdf = self._map_to_damo_waterkering(lines_gdf)

        initial_count = len(damo_gdf)
        damo_gdf = damo_gdf[damo_gdf.geometry.notna()].copy()
        removed_count = initial_count - len(damo_gdf)
        if removed_count > 0:
            self.logger.warning(f"Removed {removed_count} features with null geometry")

        self.data.waterkering = damo_gdf
        self.logger.info(f"Created waterkering layer with {len(damo_gdf)} features")

        if EXPORT_PERPENDICULAR_SAMPLES and self.perpendicular_samples:
            self._export_perpendicular_samples()

    @staticmethod
    def _snap_coords(geom: Optional[BaseGeometry], tolerance: float = SNAP_TOLERANCE) -> Optional[BaseGeometry]:
        """Snap geometry coordinates to grid for consistent topology."""
        if geom is None or geom.is_empty:
            return None

        def snap_coords_list(coords):
            return [(round(x / tolerance) * tolerance, round(y / tolerance) * tolerance) for x, y in coords]

        if geom.geom_type == "LineString":
            return LineString(snap_coords_list(geom.coords))
        elif geom.geom_type == "MultiLineString":
            return MultiLineString([LineString(snap_coords_list(line.coords)) for line in geom.geoms])
        return geom

    def _load_source_layers(self, layer_names: list[str]) -> dict[str, gpd.GeoDataFrame]:
        """Load source layers that are available in the data.

        Args:
            layer_names: List of layer names to load

        Returns:
            Dictionary with layer name as key and GeoDataFrame as value
        """
        available_layers = {}

        for layer_name in layer_names:
            gdf = getattr(self.data, layer_name.lower(), None)
            if gdf is not None and not gdf.empty:
                available_layers[layer_name] = gdf
                self.logger.info(f"Loaded source layer '{layer_name}' with {len(gdf)} features")

        return available_layers

    def _merge_connected_lines(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Merge connected linestrings into continuous features.

        Uses shapely's linemerge operation to connect touching lines into
        longer continuous linestrings. This reduces the number of features
        and prevents overlapping segments when lines are later split for height extraction.

        Args:
            gdf: GeoDataFrame with linestring geometries

        Returns:
            GeoDataFrame with merged linestrings
        """
        from shapely.ops import linemerge

        if gdf.empty:
            return gdf

        self.logger.info(f"Starting linemerge with {len(gdf)} input lines...")

        # Collect all geometries and snap them
        all_lines = []
        for geom in gdf.geometry:
            if geom is not None and not geom.is_empty:
                snapped = self._snap_coords(geom)
                if snapped is not None:
                    if snapped.geom_type == "MultiLineString":
                        all_lines.extend(snapped.geoms)
                    elif snapped.geom_type == "LineString":
                        all_lines.append(snapped)

        if not all_lines:
            self.logger.warning("No valid lines after snapping")
            return gpd.GeoDataFrame(columns=gdf.columns, crs=gdf.crs)

        self.logger.info(f"  Collected {len(all_lines)} line segments (after exploding MultiLineStrings)")

        # Merge connected lines
        merged = linemerge(all_lines)

        # Handle result (can be LineString, MultiLineString, or GeometryCollection)
        merged_lines = []
        if merged.geom_type == "LineString":
            merged_lines.append(merged)
        elif merged.geom_type == "MultiLineString":
            merged_lines.extend(merged.geoms)
        elif merged.geom_type == "GeometryCollection":
            for geom in merged.geoms:
                if geom.geom_type == "LineString":
                    merged_lines.append(geom)
                elif geom.geom_type == "MultiLineString":
                    merged_lines.extend(geom.geoms)

        if not merged_lines:
            self.logger.warning("Linemerge produced no valid linestrings")
            return gdf

        self.logger.info(f"  Linemerge produced {len(merged_lines)} continuous lines")

        # Create new GeoDataFrame with merged lines
        # Note: We lose individual line attributes since lines are merged
        # Use a generic source_layer if it exists in the original
        result_rows = []
        source_layer_value = gdf["source_layer"].iloc[0] if "source_layer" in gdf.columns and not gdf.empty else None

        for line in merged_lines:
            row_data = {"geometry": line}
            if source_layer_value is not None:
                row_data["source_layer"] = source_layer_value
            result_rows.append(row_data)

        result = gpd.GeoDataFrame(result_rows, crs=gdf.crs)

        return result

    def _remove_duplicate_linestrings(
        self, gdf: gpd.GeoDataFrame, snap_tolerance: float = SNAP_TOLERANCE
    ) -> gpd.GeoDataFrame:
        """Remove duplicate linestrings based on geometry.

        Snaps coordinates to a grid before comparison to handle lines that are
        nearly identical but not exactly matching due to small coordinate differences.
        Compares linestrings in both directions to catch reversed duplicates.

        Args:
            gdf: Input GeoDataFrame with linestring geometries
            snap_tolerance: Tolerance in meters for snapping coordinates

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
                reversed_coords = tuple(reversed(coords))
                return min(coords, reversed_coords)  # Canonical form
            elif geom.geom_type == "MultiLineString":
                return geom.wkb
            return None

        # Snap geometries to grid
        gdf = gdf.copy()
        gdf["_snapped_geom"] = gdf.geometry.apply(lambda g: self._snap_coords(g, snap_tolerance))

        # Create hash from snapped geometry
        gdf["_coord_hash"] = gdf["_snapped_geom"].apply(get_coord_hash)

        # Remove None values
        gdf = gdf[gdf["_coord_hash"].notna()].copy()

        # Drop duplicates based on coordinate hash (much faster than iterrows)
        gdf = gdf.drop_duplicates(subset=["_coord_hash"], keep="first")

        # Remove helper columns
        gdf = gdf.drop(columns=["_coord_hash", "_snapped_geom"])

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

        # Preserve source_layer to track origin of data
        if "source_layer" in gdf.columns:
            result["source_layer"] = gdf["source_layer"]

        # Set geometry
        result.set_geometry(gdf.geometry, crs=gdf.crs, inplace=True)

        self.logger.info(f"Mapped {len(result)} features to DAMO waterkering schema")
        return result

    def _get_column_value(
        self, gdf: gpd.GeoDataFrame, column_names: list[str], target_name: str
    ) -> Optional[gpd.GeoSeries]:
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

    def _split_lines_into_segments(self, gdf: gpd.GeoDataFrame, max_segment_length: float = 50.0) -> gpd.GeoDataFrame:
        """Split lines into segments using vertex-based approach.

        Args:
            gdf: GeoDataFrame with linestring geometries
            max_segment_length: Maximum segment length in meters

        Returns:
            GeoDataFrame with split segments including length_m
        """
        self.logger.info(f"Splitting lines into segments (max_segment: {max_segment_length}m)...")

        segments_data = []

        for idx, row in gdf.iterrows():
            geom = row.geometry

            if geom is None or geom.is_empty:
                continue

            segments = self._split_line_by_vertices(geom, max_segment_length)

            for segment_geom in segments:
                segment_row = row.copy()
                segment_row["geometry"] = segment_geom
                segment_row["length_m"] = round(segment_geom.length, 2)
                segments_data.append(segment_row)

        if not segments_data:
            self.logger.warning("No segments created")
            return gpd.GeoDataFrame(columns=gdf.columns.tolist() + ["length_m"], crs=gdf.crs)

        result = gpd.GeoDataFrame(segments_data, crs=gdf.crs).reset_index(drop=True)
        self.logger.info(
            f"Created {len(result)} segments from {len(gdf)} lines (avg {len(result) / len(gdf):.1f} per line)"
        )

        return result

    def _extract_heights_for_segments(self, gdf: gpd.GeoDataFrame, search_distance: float = 10.0) -> gpd.GeoDataFrame:
        """Extract heights for line segments from DEM.

        Args:
            gdf: GeoDataFrame with linestring segments
            search_distance: Distance to search perpendicular to line for crest

        Returns:
            GeoDataFrame with min_height column added
        """
        self.logger.info(f"Extracting heights for {len(gdf)} segments...")
        dem = self.data.dem_dataset

        heights = []
        total = len(gdf)
        log_interval = max(100, total // 10)  # Log every 10% or every 100 segments

        for idx, geom in enumerate(gdf.geometry):
            height = self._sample_crest_height_perpendicular(geom, dem, search_distance)
            heights.append(round(height, 2) if height is not None else None)

            # Progress logging
            if (idx + 1) % log_interval == 0 or (idx + 1) == total:
                progress_pct = ((idx + 1) / total) * 100
                self.logger.info(f"  Progress: {idx + 1}/{total} segments ({progress_pct:.1f}%)")

        gdf["min_height"] = heights

        valid_heights = gdf["min_height"].dropna()
        if len(valid_heights) > 0:
            self.logger.info(
                f"Extracted heights for {len(valid_heights)}/{len(gdf)} segments "
                f"(min: {valid_heights.min():.2f}m, max: {valid_heights.max():.2f}m)"
            )
        else:
            self.logger.warning("No valid heights extracted")

        return gdf

    def _split_line_by_vertices(self, geom: Union[LineString, MultiLineString], max_length: float) -> list[LineString]:
        """Split line by examining segments between consecutive vertices.

        Only subdivides individual segments that exceed max_length.
        This preserves the original vertex structure and prevents overlaps.

        Args:
            geom: LineString or MultiLineString
            max_length: Maximum segment length

        Returns:
            List of LineString segments
        """
        all_segments = []

        if geom.geom_type == "LineString":
            all_segments.extend(self._split_linestring_by_vertices(geom, max_length))
        elif geom.geom_type == "MultiLineString":
            for line in geom.geoms:
                all_segments.extend(self._split_linestring_by_vertices(line, max_length))

        return all_segments

    def _split_linestring_by_vertices(self, line: LineString, max_length: float) -> list[LineString]:
        """Split a single LineString by examining vertex-to-vertex segments.

        Args:
            line: LineString to split
            max_length: Maximum segment length

        Returns:
            List of LineString segments
        """
        coords = list(line.coords)
        segments = []

        for i in range(len(coords) - 1):
            start = Point(coords[i])
            end = Point(coords[i + 1])
            segment_length = start.distance(end)

            if segment_length <= max_length:
                # Segment is acceptable, keep as-is
                segments.append(LineString([start, end]))
            else:
                # Segment too long, subdivide it
                num_subsegments = int(np.ceil(segment_length / max_length))
                x_coords = np.linspace(start.x, end.x, num_subsegments + 1)
                y_coords = np.linspace(start.y, end.y, num_subsegments + 1)

                subsegment_points = [Point(x, y) for x, y in zip(x_coords, y_coords)]
                for j in range(len(subsegment_points) - 1):
                    segments.append(LineString([subsegment_points[j], subsegment_points[j + 1]]))

        return segments

    def _remove_overlapping_segments_robust(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Remove segments that heavily overlap with each other.

        Args:
            gdf: GeoDataFrame with segments

        Returns:
            GeoDataFrame with overlapping segments removed
        """
        if len(gdf) < 2:
            return gdf

        kept_indices = []

        for i in range(len(gdf)):
            geom_i = gdf.iloc[i].geometry
            if geom_i is None or geom_i.is_empty:
                continue

            is_duplicate = False
            buffer_i = geom_i.buffer(0.05)  # 5cm buffer

            # Check against all previously kept segments
            for j in kept_indices:
                geom_j = gdf.iloc[j].geometry
                if geom_j is None or geom_j.is_empty:
                    continue

                # Check if they heavily overlap (>90% in both directions)
                intersection = buffer_i.intersection(geom_j)
                if not intersection.is_empty:
                    overlap_i = intersection.length / geom_i.length if geom_i.length > 0 else 0
                    overlap_j = intersection.length / geom_j.length if geom_j.length > 0 else 0

                    if overlap_i > 0.90 and overlap_j > 0.90:
                        is_duplicate = True
                        break

            if not is_duplicate:
                kept_indices.append(i)

        result = gdf.iloc[kept_indices].copy().reset_index(drop=True)
        removed = len(gdf) - len(result)
        if removed > 0:
            self.logger.info(f"  Removed {removed} overlapping segments")

        return result

    def _sample_crest_height_perpendicular(
        self,
        line: LineString,
        dem_dataset: rasterio.DatasetReader,
        search_distance: float,
        step_distance: float = DEFAULT_PERPENDICULAR_SAMPLE_DISTANCE,
    ) -> Optional[float]:
        """Extract crest height using perpendicular sampling.

        Samples the DEM perpendicular to the line at regular intervals,
        finds the maximum elevation on each side (the crest), and returns
        the 10th percentile along the line to capture weak points for flood modeling.

        This is based on the threedi_beta_processing crest level algorithm.

        Args:
            line: LineString geometry
            dem_dataset: Open rasterio dataset
            search_distance: Distance to search perpendicular to line in meters
            step_distance: Distance between perpendicular sample lines in meters (default: DEFAULT_PERPENDICULAR_SAMPLE_DISTANCE)

        Returns:
            10th percentile crest height or None if no valid data
        """
        try:
            # Get DEM pixel size
            pixel_size = abs(dem_dataset.transform[0])

            # Log DEM info on first call (use a class attribute to track)
            if not hasattr(self, "_dem_info_logged"):
                self.logger.info(
                    f"DEM info: size=({dem_dataset.width}, {dem_dataset.height}), "
                    f"pixel_size={pixel_size:.3f}m, transform={dem_dataset.transform}, "
                    f"bounds=({dem_dataset.bounds.left:.1f}, {dem_dataset.bounds.bottom:.1f}, "
                    f"{dem_dataset.bounds.right:.1f}, {dem_dataset.bounds.top:.1f}), "
                    f"nodata={dem_dataset.nodata}"
                )
                self._dem_info_logged = True

            # Calculate number of points along the line
            line_length = line.length
            if line_length < 0.1:  # Too short
                self.logger.warning(f"Line too short ({line_length:.3f}m) for perpendicular sampling, skipping")
                return None

            n_samples = max(2, int(line_length / step_distance) + 1)

            # Sample points along the line
            distances = np.linspace(0, line_length, n_samples)
            sample_points = [line.interpolate(d) for d in distances]

            # Get perpendicular vectors at each point
            heights = []
            failed_samples = 0
            outside_bounds_count = 0
            nodata_count = 0

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
                    self.logger.debug(f"Direction vector too small at sample {i}, skipping")
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
                samples_outside = 0
                samples_nodata = 0
                samples_error = 0

                for d in perp_distances:
                    sample_x = point.x + perp_dx * d
                    sample_y = point.y + perp_dy * d

                    # Sample DEM at this point - use rasterio's index method for correct row,col
                    row, col = dem_dataset.index(sample_x, sample_y)

                    if 0 <= row < dem_dataset.height and 0 <= col < dem_dataset.width:
                        try:
                            height = dem_dataset.read(1, window=((row, row + 1), (col, col + 1)))[0, 0]
                            if not np.isnan(height) and height != dem_dataset.nodata:
                                perp_heights.append(height)
                            else:
                                samples_nodata += 1
                        except Exception as e:
                            samples_error += 1
                            if samples_error == 1:  # Log first error only
                                self.logger.debug(f"DEM read error at ({sample_x:.1f}, {sample_y:.1f}): {e}")
                    else:
                        samples_outside += 1

                if perp_heights:
                    # Take maximum (crest) from perpendicular samples
                    crest = np.max(perp_heights)
                    heights.append(crest)

                    # Log sample statistics for debugging
                    if i < 3 or i == len(sample_points) - 1:  # Log first 3 and last sample
                        self.logger.debug(
                            f"Sample {i} at ({point.x:.1f}, {point.y:.1f}): "
                            f"{len(perp_heights)}/{n_perp_samples} valid heights, "
                            f"range: {np.min(perp_heights):.3f} - {np.max(perp_heights):.3f}m, "
                            f"crest: {crest:.3f}m"
                        )

                    # Store perpendicular sample line if debug output enabled
                    if EXPORT_PERPENDICULAR_SAMPLES:
                        perp_start = Point(point.x - perp_dx * search_distance, point.y - perp_dy * search_distance)
                        perp_end = Point(point.x + perp_dx * search_distance, point.y + perp_dy * search_distance)
                        perp_line = LineString([perp_start, perp_end])

                        # Store all sampled coordinates and values for debugging
                        sample_coords = []
                        for d in perp_distances:
                            sample_x = point.x + perp_dx * d
                            sample_y = point.y + perp_dy * d
                            sample_coords.append(f"({sample_x:.1f},{sample_y:.1f})")

                        self.perpendicular_samples.append(
                            {
                                "geometry": perp_line,
                                "height_min": round(np.min(perp_heights), 3),
                                "height_p10": round(np.percentile(perp_heights, 10), 3),
                                "height_avg": round(np.mean(perp_heights), 3),
                                "height_p90": round(np.percentile(perp_heights, 90), 3),
                                "height_max": round(np.max(perp_heights), 3),
                                "sample_index": i,
                                "n_samples": len(perp_heights),
                                "n_total": n_perp_samples,
                                "center_x": round(point.x, 1),
                                "center_y": round(point.y, 1),
                            }
                        )
                else:
                    failed_samples += 1
                    if samples_outside > 0:
                        outside_bounds_count += samples_outside
                    if samples_nodata > 0:
                        nodata_count += samples_nodata
                    self.logger.debug(
                        f"No valid heights at sample {i} (point: {point.x:.1f}, {point.y:.1f}): "
                        f"{samples_outside}/{n_perp_samples} outside DEM, {samples_nodata}/{n_perp_samples} nodata, "
                        f"{samples_error}/{n_perp_samples} errors"
                    )

            if not heights:
                self.logger.warning(
                    f"No heights extracted from perpendicular sampling (line length: {line_length:.2f}m, "
                    f"samples: {n_samples}, search distance: {search_distance:.2f}m). "
                    f"Failed: {failed_samples}/{n_samples} sample points. "
                    f"Total: {outside_bounds_count} samples outside DEM bounds, {nodata_count} samples hit nodata. "
                    f"Line coords: ({line.coords[0][0]:.1f}, {line.coords[0][1]:.1f}) to ({line.coords[-1][0]:.1f}, {line.coords[-1][1]:.1f})"
                )
                return None

            # Return 10th percentile to capture low points for flood modeling
            heights = np.array(heights)
            percentile_10 = np.percentile(heights, 10)

            return float(percentile_10)

        except Exception as e:
            self.logger.error(f"Error in perpendicular sampling: {e}")
            import traceback

            self.logger.debug(traceback.format_exc())
            return None

    def _export_perpendicular_samples(self) -> None:
        """Export perpendicular sample lines to geopackage for debugging.

        Creates a 'perpendicular_samples' layer in the output with sample lines and their extracted heights.
        """
        if not self.perpendicular_samples:
            self.logger.warning("No perpendicular samples to export")
            return

        self.logger.info(f"Exporting {len(self.perpendicular_samples)} perpendicular sample lines...")

        perp_gdf = gpd.GeoDataFrame(
            self.perpendicular_samples,
            crs=self.data.peilgebiedpraktijk.crs if self.data.peilgebiedpraktijk is not None else "EPSG:28992",
        )

        self.data.perpendicular_samples = perp_gdf
        self.logger.info(f"✓ Created perpendicular_samples layer with {len(perp_gdf)} features")

    def _get_min_height_from_buffered_geometry(
        self, buffered_geom: BaseGeometry, dem: rasterio.DatasetReader
    ) -> Optional[float]:
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
