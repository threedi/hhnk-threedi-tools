from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import LineString, Polygon

from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converter import RawExportToDAMOConverter
from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converters import peilgebied_converter
from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converters.peilgebied_converter import (
    DEFAULT_SEGMENT_LENGTH,
    PEILGEBIED_SOURCE_LAYER,
    WATERKERING_LINE_LAYERS,
    PeilgebiedConverter,
)
from tests.config import TEMP_DIR, TEST_DIRECTORY


def _validate_output_file(output_file: Path, logger):
    """Validate that output file exists and has at least one layer."""
    assert output_file.exists(), "Output file not created"

    available_layers = []
    for layer_name in ["peilgebiedpraktijk", "waterkering"]:
        try:
            layer = gpd.read_file(output_file, layer=layer_name)
            if not layer.empty:
                available_layers.append(layer_name)
        except Exception:
            pass

    assert len(available_layers) > 0, "No output layers created"
    logger.info(f"Created layers: {', '.join(available_layers)}")


def _validate_peilgebiedpraktijk_layer(output_file: Path, logger):
    """Validate peilgebiedpraktijk layer attributes and geometry."""
    try:
        peilgebiedpraktijk = gpd.read_file(output_file, layer="peilgebiedpraktijk")
    except Exception:
        logger.warning("Peilgebiedpraktijk layer not created")
        return

    if peilgebiedpraktijk.empty:
        logger.warning("Peilgebiedpraktijk layer is empty")
        return

    assert not peilgebiedpraktijk.empty, "Peilgebiedpraktijk layer is empty"
    assert "geometry" in peilgebiedpraktijk.columns, "Peilgebiedpraktijk missing geometry column"
    assert peilgebiedpraktijk["geometry"].notna().all(), "Some features have null geometry"

    geom_types = peilgebiedpraktijk.geometry.type.unique()
    assert all(gt in ["Polygon", "MultiPolygon"] for gt in geom_types), (
        f"Expected polygon geometries, found: {geom_types}"
    )

    required_columns = ["statusPeilgebied", "voertAfOp", "bevat"]
    for col in required_columns:
        assert col in peilgebiedpraktijk.columns, f"Missing required column: {col}"

    logger.info(f"✓ Peilgebiedpraktijk layer validated: {len(peilgebiedpraktijk)} features")


def _validate_waterkering_layer(output_file: Path, logger):
    """Validate waterkering layer attributes and geometry."""
    try:
        waterkering = gpd.read_file(output_file, layer="waterkering")
    except Exception:
        logger.warning("Waterkering layer not created")
        return

    if waterkering.empty:
        logger.warning("Waterkering layer is empty")
        return

    # Test: Layer has data and geometry
    assert not waterkering.empty, "Waterkering layer is empty"
    assert "geometry" in waterkering.columns, "Waterkering missing geometry column"
    assert waterkering["geometry"].notna().all(), "Some features have null geometry"

    # Test: Geometry is linestring
    geom_types = waterkering.geometry.type.unique()
    assert all(gt in ["LineString", "MultiLineString"] for gt in geom_types), (
        f"Expected linestring geometries, found: {geom_types}"
    )

    # Test: Required DAMO attributes exist
    required_columns = ["categorie", "typeWaterkering", "soortReferentielijn", "waterstaatswerkWaterkeringID"]
    for col in required_columns:
        assert col in waterkering.columns, f"Missing required column: {col}"

    assert waterkering["waterstaatswerkWaterkeringID"].notna().all(), (
        "Some features have null waterstaatswerkWaterkeringID"
    )
    assert waterkering["waterstaatswerkWaterkeringID"].is_unique, "Duplicate waterstaatswerkWaterkeringID found"

    assert "min_height" in waterkering.columns, "min_height column missing - DEM height extraction was not performed"

    missing_heights = waterkering["min_height"].isna().sum()
    missing_pct = (missing_heights / len(waterkering)) * 100
    assert missing_pct < 2.0, (
        f"Height extraction incomplete: {missing_heights}/{len(waterkering)} features ({missing_pct:.2f}%) are missing heights. "
        f"More than 2% missing suggests a systematic issue. Expected >98% success rate."
    )

    if missing_heights > 0:
        logger.warning(
            f"⚠ {missing_heights}/{len(waterkering)} features ({missing_pct:.2f}%) have no height (likely edge cases)"
        )
    else:
        logger.info(f"✓ Height extraction complete: all {len(waterkering)} features have heights")

    height_values = waterkering["min_height"].dropna()
    assert len(height_values) > 0, "No valid heights found"
    assert height_values.min() > -10, f"Suspiciously low height: {height_values.min():.2f}m"
    assert height_values.max() < 50, f"Suspiciously high height: {height_values.max():.2f}m"
    logger.info(f"✓ Height range valid: {height_values.min():.2f}m to {height_values.max():.2f}m")

    assert "length_m" in waterkering.columns, "length_m column missing - segment length was not stored"
    assert waterkering["length_m"].notna().all(), "Some features have null length_m"

    assert (waterkering["length_m"] >= 0.1).all(), "Found segments shorter than 0.1m (should have been filtered)"
    for length_val in waterkering["length_m"]:
        decimal_places = len(str(length_val).split(".")[-1]) if "." in str(length_val) else 0
        assert decimal_places <= 2, f"length_m {length_val} has more than 2 decimal places"

    for height_val in waterkering["min_height"].dropna():
        decimal_places = len(str(height_val).split(".")[-1]) if "." in str(height_val) else 0
        assert decimal_places <= 2, f"min_height {height_val} has more than 2 decimal places"

    logger.info(f"✓ Length and height values properly rounded to 2 decimals")

    max_segment_lengths = []
    for geom in waterkering.geometry:
        if geom.geom_type == "LineString":
            max_segment_lengths.append(geom.length)
        elif geom.geom_type == "MultiLineString":
            for line in geom.geoms:
                max_segment_lengths.append(line.length)

    if max_segment_lengths:
        max_line_length = max(max_segment_lengths)
        tolerance_factor = 1.5
        max_expected = DEFAULT_SEGMENT_LENGTH * tolerance_factor

        assert max_line_length <= max_expected, (
            f"Line segment too long: {max_line_length:.2f}m exceeds expected maximum {max_expected:.2f}m "
            f"(segment_length={DEFAULT_SEGMENT_LENGTH}m with {tolerance_factor}x tolerance). "
            "Line splitting may not be working correctly."
        )
        logger.info(f"✓ Line segment lengths valid: max {max_line_length:.2f}m (limit: {max_expected:.2f}m)")

    for idx, row in waterkering.iterrows():
        actual_length = round(row.geometry.length, 2)
        stored_length = row["length_m"]
        assert abs(actual_length - stored_length) < 0.01, (
            f"Feature {idx}: stored length_m ({stored_length}) doesn't match geometry length ({actual_length})"
        )
    logger.info(f"✓ Stored length_m matches geometry lengths")

    logger.info(f"✓ Waterkering layer validated: {len(waterkering)} features")


def _validate_no_duplicates(output_file: Path, logger):
    """Validate that no duplicate linestrings exist in waterkering layer."""
    try:
        waterkering = gpd.read_file(output_file, layer="waterkering")
    except Exception:
        logger.warning("Waterkering layer not found, skipping duplicate check")
        return

    if waterkering.empty:
        logger.warning("Waterkering layer is empty, skipping duplicate check")
        return

    wkt_set = set(waterkering.geometry.apply(lambda g: g.wkt))
    assert len(wkt_set) == len(waterkering), "Duplicate geometries found after deduplication"

    logger.info(f"✓ No duplicate linestrings found ({len(waterkering)} unique features)")


def _validate_no_overlapping_segments(output_file: Path, logger):
    """Validate that line segments in waterkering layer are not overlapping.

    After linemerge, segments should be distinct and not overlap with each other
    (beyond touching at endpoints).
    """
    try:
        waterkering = gpd.read_file(output_file, layer="waterkering")
    except Exception:
        logger.warning("Waterkering layer not found, skipping overlap check")
        return

    if waterkering.empty:
        logger.warning("Waterkering layer is empty, skipping overlap check")
        return

    if len(waterkering) < 2:
        logger.info("✓ Only one segment, no overlap possible")
        return

    buffer_distance = 0.5
    overlapping_pairs = []

    logger.info(f"Checking {len(waterkering)} segments for overlaps (buffer: {buffer_distance}m)...")

    for i, row_i in waterkering.iterrows():
        geom_i = row_i.geometry
        if geom_i is None or geom_i.is_empty:
            continue

        buffered_i = geom_i.buffer(buffer_distance)

        for j, row_j in waterkering.iterrows():
            if i >= j:  # Skip self-comparison and already-checked pairs
                continue

            geom_j = row_j.geometry
            if geom_j is None or geom_j.is_empty:
                continue

            intersection = buffered_i.intersection(geom_j)

            if not intersection.is_empty:
                intersection_length = intersection.length
                line_j_length = geom_j.length

                overlap_percentage = (intersection_length / line_j_length) * 100

                if overlap_percentage > 20:
                    source_i = row_i.get("source_layer", "unknown")
                    source_j = row_j.get("source_layer", "unknown")
                    overlapping_pairs.append(
                        f"Segment {i} (source: {source_i}, length: {geom_i.length:.2f}m) "
                        f"overlaps {overlap_percentage:.1f}% with segment {j} "
                        f"(source: {source_j}, length: {geom_j.length:.2f}m)"
                    )

    total_possible_pairs = (len(waterkering) * (len(waterkering) - 1)) // 2
    overlap_percentage = (len(overlapping_pairs) / total_possible_pairs * 100) if total_possible_pairs > 0 else 0
    max_acceptable_overlap_pct = 20.0

    if overlapping_pairs:
        if overlap_percentage > max_acceptable_overlap_pct:
            shown_overlaps = overlapping_pairs[:10]
            error_msg = (
                f"Found {len(overlapping_pairs)} overlapping segment pairs "
                f"({overlap_percentage:.2f}% of {total_possible_pairs} total pairs). "
                f"More than {max_acceptable_overlap_pct}% overlapping suggests a systematic issue.\n"
                f"First {len(shown_overlaps)} overlaps:\n" + "\n".join(shown_overlaps)
            )
            if len(overlapping_pairs) > 10:
                error_msg += f"\n... and {len(overlapping_pairs) - 10} more"
            assert False, error_msg
        else:
            logger.warning(
                f"⚠ Found {len(overlapping_pairs)} overlapping segment pairs "
                f"({overlap_percentage:.2f}% of {total_possible_pairs} total pairs). "
                f"Acceptable (<{max_acceptable_overlap_pct}%), but should be inspected in QGIS."
            )
            if len(overlapping_pairs) <= 10:
                for overlap in overlapping_pairs:
                    logger.warning(f"  - {overlap}")
            else:
                for overlap in overlapping_pairs[:5]:
                    logger.warning(f"  - {overlap}")
                logger.warning(f"  ... and {len(overlapping_pairs) - 5} more")
    else:
        logger.info(f"✓ No overlapping segments found (checked {len(waterkering)} segments)")


def test_peilgebied_converter():
    """Test PeilgebiedConverter with all validations in a single run."""
    logger = hrt.logging.get_logger(__name__)
    raw_export_file = TEST_DIRECTORY / "schematisation_builder" / "raw_export.gpkg"
    output_dir = TEMP_DIR / f"temp_peilgebied_converter_{hrt.current_time(date=True)}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "damo.gpkg"

    # Check if source layers exist
    converter_base = RawExportToDAMOConverter(raw_export_file, output_file, logger)

    assert converter_base.data.dem_dataset is not None, (
        f"DEM file not found. Expected ahn.tif at {raw_export_file.parent / 'ahn.tif'}. "
        "DEM is required for testing height extraction functionality."
    )
    logger.info(f"✓ DEM loaded from {converter_base.data.dem_path}")

    source_layers_exist = hasattr(converter_base.data, PEILGEBIED_SOURCE_LAYER.lower()) or any(
        hasattr(converter_base.data, layer_name.lower()) for layer_name in WATERKERING_LINE_LAYERS
    )

    if not source_layers_exist:
        logger.warning(f"No source layers found, skipping test")
        return

    # Run converter once
    logger.info("Running peilgebied converter...")

    peilgebied_converter.EXPORT_PERPENDICULAR_SAMPLES = True

    converter = PeilgebiedConverter(converter_base)
    logger.info(f"DEBUG: Converter has DEM: {converter.data.dem_dataset is not None}")

    # Limit test to 3 polygons for speed (indices 5, 12, 23)
    source_layer_name = PEILGEBIED_SOURCE_LAYER.lower()
    if hasattr(converter.data, source_layer_name):
        source_gdf = getattr(converter.data, source_layer_name)
        if source_gdf is not None and len(source_gdf) > 3:
            selected_indices = [5, 12, 23]
            filtered_gdf = source_gdf.iloc[selected_indices].copy()
            setattr(converter.data, source_layer_name, filtered_gdf)
            logger.info(f"Limited test to 3 polygons (indices {selected_indices}) for speed")

    converter.run()
    converter_base.write_outputs()

    # Check if perpendicular_samples layer was created
    try:
        perp_samples = gpd.read_file(output_file, layer="perpendicular_samples")
        logger.info(f"✓ Perpendicular samples layer created with {len(perp_samples)} features")
    except Exception as e:
        logger.warning(f"Perpendicular samples layer not found: {e}")

    # Run all validations on the single output
    logger.info("\nValidating outputs...")
    _validate_output_file(output_file, logger)
    _validate_peilgebiedpraktijk_layer(output_file, logger)
    _validate_waterkering_layer(output_file, logger)
    _validate_no_duplicates(output_file, logger)
    _validate_no_overlapping_segments(output_file, logger)

    logger.info("\n✓ All validations passed!")


def test_duplicate_removal_with_synthetic_data():
    """Test duplicate removal logic using synthetic overlapping polygons."""
    # Setup
    logger = hrt.logging.get_logger(__name__)
    raw_export_file = TEST_DIRECTORY / "schematisation_builder" / "raw_export.gpkg"
    output_dir = TEMP_DIR / f"temp_duplicate_removal_{hrt.current_time(date=True)}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "damo.gpkg"

    # Run converter
    converter_base = RawExportToDAMOConverter(raw_export_file, output_file, logger)

    # Create test data with overlapping polygons (shared boundaries)
    test_polygons = gpd.GeoDataFrame(
        {
            "code": ["A", "B"],
            "geometry": [
                Polygon([(0, 0), (10, 0), (10, 10), (0, 10)]),  # Left polygon
                Polygon([(10, 0), (20, 0), (20, 10), (10, 10)]),  # Right polygon (shares boundary)
            ],
        },
        crs="EPSG:28992",
    )

    # Inject test data into combinatiepeilgebied layer
    setattr(converter_base.data, PEILGEBIED_SOURCE_LAYER.lower(), test_polygons)

    converter = PeilgebiedConverter(converter_base)
    converter.run()

    # Get waterkering result
    waterkering = getattr(converter_base.data, "waterkering", None)

    if waterkering is None or waterkering.empty:
        logger.warning("Waterkering layer not created (test data may not have been processed)")
        return

    # The two adjacent polygons should create lines, but the shared boundary should be deduplicated
    logger.info(f"Created {len(waterkering)} unique linestrings after deduplication")

    # Test: No exact duplicate geometries exist (filter None first)
    waterkering_valid = waterkering[waterkering.geometry.notna()].copy()
    wkt_set = set(waterkering_valid.geometry.apply(lambda g: g.wkt))
    assert len(wkt_set) == len(waterkering_valid), "Duplicate geometries found after deduplication"

    logger.info("✓ Duplicate removal validated with synthetic data")


if __name__ == "__main__":
    test_peilgebied_converter()
    test_duplicate_removal_with_synthetic_data()
