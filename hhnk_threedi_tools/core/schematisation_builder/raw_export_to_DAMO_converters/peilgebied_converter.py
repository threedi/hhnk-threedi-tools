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

# Priority order for peilgebied layers (highest to lowest)
# Higher priority layers will "cut out" areas from lower priority layers to prevent overlaps
PEILGEBIED_PRIORITY_ORDER = [
    "peilgebiedpraktijk",  # Highest priority - actual operational areas
    "waterbergingsgebied",  # Water storage specific areas
    "hydrodeelgebied",  # Hydrological units
    "combinatiepeilgebied",  # Lowest priority - aggregated areas
]

# Input layers for waterkering
# Note: Polygon boundaries will be extracted from the final processed peilgebiedpraktijk
# These are additional dedicated line layers to include
WATERKERING_LINE_LAYERS = [
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

# Configuration for duplicate line removal
SNAP_TOLERANCE = 0.01  # meters - lines within this tolerance are considered duplicates


class PeilgebiedConverter(RawExportToDAMOConverter):
    """Convert raw peilgebied exports to DAMO schema (peilgebiedpraktijk and waterkering layers)."""

    def __init__(self, raw_export_converter: RawExportToDAMOConverter):
        self.data = raw_export_converter.data
        self.logger = raw_export_converter.logger

    def run(self):
        """Create peilgebiedpraktijk and waterkering layers."""
        self.logger.info("Running PeilgebiedConverter...")
        self._create_peilgebiedpraktijk_layer()
        self._create_waterkering_layer()

    def _create_peilgebiedpraktijk_layer(self) -> None:
        """Create DAMO-compliant peilgebiedpraktijk layer from PEILGEBIED_SOURCE_LAYERS."""
        self.logger.info("Creating peilgebiedpraktijk layer...")

        available_layers = self._load_source_layers(PEILGEBIED_SOURCE_LAYERS)
        if not available_layers:
            self.logger.warning("No source layers found for peilgebiedpraktijk")
            return

        # Combine layers with priority-based overlap handling
        combined_gdf = self._combine_layers_without_overlaps(available_layers)
        if combined_gdf is None or combined_gdf.empty:
            self.logger.warning("Combined peilgebiedpraktijk layer is empty")
            return

        combined_gdf = self._ensure_polygon_geometry(combined_gdf)
        damo_gdf = self._map_to_damo_peilgebiedpraktijk(combined_gdf)

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
        from shapely.ops import unary_union

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
            seg_buffer = segment.buffer(0.01)  # 1cm buffer for near-duplicate detection

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

        # Split segments and extract heights
        if self.data.dem_dataset is not None:
            lines_gdf = self._split_lines_and_extract_heights(
                lines_gdf,
                max_segment_length=DEFAULT_SEGMENT_LENGTH,
                search_distance=DEFAULT_BUFFER_WIDTH,
            )
        else:
            self.logger.warning("DEM not available, skipping height extraction")
            lines_gdf["min_height"] = None
            lines_gdf["length_m"] = lines_gdf.geometry.length.apply(lambda l: round(l, 2))

        # 8. Map to DAMO schema
        damo_gdf = self._map_to_damo_waterkering(lines_gdf)

        # 8. Remove features with null geometry
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

    def _combine_layers_without_overlaps(self, layers_dict: dict[str, gpd.GeoDataFrame]) -> gpd.GeoDataFrame | None:
        """Combine multiple GeoDataFrames with priority-based overlap removal.

        Higher priority layers will "cut out" their areas from lower priority layers
        to ensure no overlapping polygons in the final result.

        Args:
            layers_dict: Dictionary with layer name as key and GeoDataFrame as value

        Returns:
            Combined GeoDataFrame without overlaps or None if input is empty
        """
        if not layers_dict:
            return None

        # Get layers in priority order (only those that are available)
        ordered_layers = []
        for layer_name in PEILGEBIED_PRIORITY_ORDER:
            if layer_name in layers_dict:
                gdf = layers_dict[layer_name].copy()
                gdf["source_layer"] = layer_name
                ordered_layers.append((layer_name, gdf))

        # Add any layers not in priority order at the end
        for layer_name, gdf in layers_dict.items():
            if layer_name not in PEILGEBIED_PRIORITY_ORDER:
                gdf_copy = gdf.copy()
                gdf_copy["source_layer"] = layer_name
                ordered_layers.append((layer_name, gdf_copy))
                self.logger.warning(
                    f"Layer '{layer_name}' not in PEILGEBIED_PRIORITY_ORDER, adding with lowest priority"
                )

        if not ordered_layers:
            return None

        self.logger.info(f"Processing {len(ordered_layers)} layers with priority-based overlap removal")

        # Start with highest priority layer
        result_gdfs = []
        accumulated_mask = None  # Union of all higher priority geometries

        for i, (layer_name, gdf) in enumerate(ordered_layers):
            if gdf.empty:
                self.logger.debug(f"Layer '{layer_name}' is empty, skipping")
                continue

            self.logger.info(f"Processing layer {i + 1}/{len(ordered_layers)}: '{layer_name}' ({len(gdf)} features)")

            if accumulated_mask is None:
                # First layer - keep as is
                result_gdfs.append(gdf)
                accumulated_mask = gdf.geometry.union_all()
                self.logger.info(f"  Added {len(gdf)} features from highest priority layer")
            else:
                # Subsequent layers - subtract higher priority areas
                processed_features = []
                original_count = len(gdf)
                removed_count = 0
                modified_count = 0

                for idx, row in gdf.iterrows():
                    geom = row.geometry
                    if geom is None or geom.is_empty:
                        removed_count += 1
                        continue

                    # Subtract all higher priority areas
                    try:
                        new_geom = geom.difference(accumulated_mask)

                        if new_geom.is_empty:
                            # Completely overlapped by higher priority
                            removed_count += 1
                            continue

                        if not new_geom.equals(geom):
                            modified_count += 1

                        # Keep the modified geometry
                        row_copy = row.copy()
                        row_copy.geometry = new_geom
                        processed_features.append(row_copy)

                    except Exception as e:
                        self.logger.warning(f"  Error processing geometry in '{layer_name}': {e}")
                        removed_count += 1
                        continue

                if processed_features:
                    layer_result = gpd.GeoDataFrame(processed_features, crs=gdf.crs)
                    result_gdfs.append(layer_result)

                    # Update accumulated mask
                    accumulated_mask = accumulated_mask.union(layer_result.geometry.union_all())

                    kept_count = len(processed_features)
                    self.logger.info(
                        f"  Kept {kept_count}/{original_count} features "
                        f"({removed_count} removed, {modified_count} modified by higher priority layers)"
                    )
                else:
                    self.logger.info(
                        f"  All {original_count} features removed due to overlap with higher priority layers"
                    )

        if not result_gdfs:
            self.logger.warning("No features remaining after overlap removal")
            return None

        # Combine all processed layers
        combined = gpd.GeoDataFrame(pd.concat(result_gdfs, ignore_index=True))
        self.logger.info(f"Combined {len(ordered_layers)} layers into {len(combined)} non-overlapping features")

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

    def _extract_boundaries_from_polygons(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame | None:
        """Extract boundary lines from polygon GeoDataFrame.

        Args:
            gdf: GeoDataFrame with polygon geometries

        Returns:
            GeoDataFrame with boundary linestrings, or None if input is empty
        """
        if gdf is None or gdf.empty:
            return None

        boundaries = []
        for idx, row in gdf.iterrows():
            geom = row.geometry
            if geom is None or geom.is_empty:
                continue

            # Extract boundary
            if geom.geom_type == "Polygon":
                boundary = LineString(geom.exterior.coords)
            elif geom.geom_type == "MultiPolygon":
                boundary = MultiLineString([LineString(polygon.exterior.coords) for polygon in geom.geoms])
            else:
                self.logger.warning(f"Unexpected geometry type for boundary extraction: {geom.geom_type}")
                continue

            # Create new row with boundary geometry
            row_copy = row.copy()
            row_copy.geometry = boundary
            boundaries.append(row_copy)

        if not boundaries:
            return None

        return gpd.GeoDataFrame(boundaries, crs=gdf.crs)

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

        # Snap coordinates to ensure lines connect properly
        # Use the same snap tolerance as duplicate removal
        snap_tolerance = SNAP_TOLERANCE

        def snap_line_coords(geom):
            """Snap line coordinates to grid."""
            if geom is None or geom.is_empty:
                return None
            if geom.geom_type == "LineString":
                snapped_coords = [
                    (round(x / snap_tolerance) * snap_tolerance, round(y / snap_tolerance) * snap_tolerance)
                    for x, y in geom.coords
                ]
                return LineString(snapped_coords)
            elif geom.geom_type == "MultiLineString":
                snapped_lines = []
                for line in geom.geoms:
                    snapped_coords = [
                        (round(x / snap_tolerance) * snap_tolerance, round(y / snap_tolerance) * snap_tolerance)
                        for x, y in line.coords
                    ]
                    snapped_lines.append(LineString(snapped_coords))
                return MultiLineString(snapped_lines)
            return geom

        # Collect all geometries and snap them
        all_lines = []
        for geom in gdf.geometry:
            if geom is not None and not geom.is_empty:
                snapped = snap_line_coords(geom)
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

    def _remove_duplicate_linestrings(
        self, gdf: gpd.GeoDataFrame, snap_tolerance: float = SNAP_TOLERANCE
    ) -> gpd.GeoDataFrame:
        """Remove duplicate linestrings based on geometry.

        Snaps coordinates to a grid before comparison to handle lines that are
        nearly identical but not exactly matching due to small coordinate differences.
        Compares linestrings in both directions to catch reversed duplicates
        (e.g., line A->B is the same as B->A for a boundary).

        Args:
            gdf: Input GeoDataFrame with linestring geometries
            snap_tolerance: Tolerance in meters for snapping coordinates (default: SNAP_TOLERANCE)

        Returns:
            GeoDataFrame with unique linestrings only
        """
        if gdf.empty:
            return gdf

        def snap_coords(geom, tolerance):
            """Snap geometry coordinates to a grid."""
            if geom is None or geom.is_empty:
                return None

            if geom.geom_type == "LineString":
                # Round coordinates to tolerance
                snapped_coords = [
                    (round(x / tolerance) * tolerance, round(y / tolerance) * tolerance) for x, y in geom.coords
                ]
                return LineString(snapped_coords)
            elif geom.geom_type == "MultiLineString":
                snapped_lines = []
                for line in geom.geoms:
                    snapped_coords = [
                        (round(x / tolerance) * tolerance, round(y / tolerance) * tolerance) for x, y in line.coords
                    ]
                    snapped_lines.append(LineString(snapped_coords))
                return MultiLineString(snapped_lines)
            else:
                return geom

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

        # Snap geometries to grid
        gdf = gdf.copy()
        gdf["_snapped_geom"] = gdf.geometry.apply(lambda g: snap_coords(g, snap_tolerance))

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

        # Preserve source_layer to track origin of data
        if "source_layer" in gdf.columns:
            result["source_layer"] = gdf["source_layer"]

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

    def _split_lines_and_extract_heights(
        self, gdf: gpd.GeoDataFrame, max_segment_length: float = 50.0, search_distance: float = 10.0
    ) -> gpd.GeoDataFrame:
        """Split lines using vertex-based approach and extract heights for each segment.

        This method:
        1. Splits each line by examining segments between consecutive vertices
        2. Only subdivides segments longer than max_segment_length
        3. Extracts crest height for each resulting segment

        Args:
            gdf: GeoDataFrame with linestring geometries
            max_segment_length: Maximum segment length in meters
            search_distance: Distance to search perpendicular to line for crest

        Returns:
            GeoDataFrame with split segments, each with height and length
        """
        if self.data.dem_dataset is None:
            self.logger.warning("No DEM available for height extraction")
            return gdf

        self.logger.info(f"Splitting lines and extracting heights (max_segment: {max_segment_length}m)...")

        dem = self.data.dem_dataset
        segments_data = []

        for idx, row in gdf.iterrows():
            geom = row.geometry

            if geom is None or geom.is_empty:
                continue

            # Split using vertex-based approach
            segments = self._split_line_by_vertices(geom, max_segment_length)

            # Extract height for each segment
            for segment_geom in segments:
                crest_height = self._sample_crest_height_perpendicular(segment_geom, dem, search_distance)

                # Round to 2 decimals
                if crest_height is not None:
                    crest_height = round(crest_height, 2)

                segment_length_m = round(segment_geom.length, 2)

                # Create row for this segment
                segment_row = row.copy()
                segment_row["geometry"] = segment_geom
                segment_row["min_height"] = crest_height
                segment_row["length_m"] = segment_length_m
                segments_data.append(segment_row)

        if not segments_data:
            self.logger.warning("No segments created")
            return gpd.GeoDataFrame(columns=gdf.columns.tolist() + ["min_height", "length_m"], crs=gdf.crs)

        result = gpd.GeoDataFrame(segments_data, crs=gdf.crs).reset_index(drop=True)

        # CRITICAL: Remove duplicate/overlapping segments created during splitting
        self.logger.info(f"Removing duplicate segments after splitting (initial: {len(result)})...")
        result = self._remove_overlapping_segments_robust(result)
        self.logger.info(f"After deduplication: {len(result)} segments")

        # Log statistics
        valid_heights = result["min_height"].dropna()
        self.logger.info(
            f"Created {len(result)} segments from {len(gdf)} lines "
            f"(avg {len(result) / len(gdf):.1f} segments per line)"
        )

        if len(valid_heights) > 0:
            self.logger.info(
                f"Extracted heights for {len(valid_heights)}/{len(result)} segments "
                f"(min: {valid_heights.min():.2f}m, max: {valid_heights.max():.2f}m)"
            )
        else:
            self.logger.warning("No valid heights extracted")

        return result

    def _split_line_by_vertices(self, geom, max_length: float) -> list[LineString]:
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
            buffer_i = geom_i.buffer(0.01)  # 1cm buffer

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
        """Split a single LineString into segments respecting vertex structure.

        Splits lines by examining each segment between consecutive vertices.
        Only subdivides segments that are longer than max_length, preserving
        the original vertex structure and preventing overlapping segments.

        This approach is based on splitting between existing vertices rather than
        extracting substrings at arbitrary distances, which prevents overlap issues.

        Args:
            line: LineString to split
            max_length: Target maximum length for segments

        Returns:
            List of LineString segments
        """
        if line.length <= max_length:
            return [line]

        coords = list(line.coords)
        segments = []

        # Go through each pair of consecutive vertices
        for i in range(len(coords) - 1):
            start = Point(coords[i])
            end = Point(coords[i + 1])
            segment_length = start.distance(end)

            if segment_length <= max_length:
                # Segment is short enough, keep as-is
                segments.append(LineString([start, end]))
            else:
                # Segment is too long, subdivide it
                num_subsegments = int(np.ceil(segment_length / max_length))
                x_coords = np.linspace(start.x, end.x, num_subsegments + 1)
                y_coords = np.linspace(start.y, end.y, num_subsegments + 1)

                subsegment_points = [Point(x, y) for x, y in zip(x_coords, y_coords)]
                for j in range(len(subsegment_points) - 1):
                    segments.append(LineString([subsegment_points[j], subsegment_points[j + 1]]))

        return segments

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
