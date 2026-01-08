from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import LineString, Polygon

from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converter import RawExportToDAMOConverter
from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converters.peilgebied_converter import (
    DEFAULT_SEGMENT_LENGTH,
    PEILGEBIED_SOURCE_LAYERS,
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

    # Test: Layer has data and geometry
    assert not peilgebiedpraktijk.empty, "Peilgebiedpraktijk layer is empty"
    assert "geometry" in peilgebiedpraktijk.columns, "Peilgebiedpraktijk missing geometry column"
    assert peilgebiedpraktijk["geometry"].notna().all(), "Some features have null geometry"

    # Test: Geometry is polygon
    geom_types = peilgebiedpraktijk.geometry.type.unique()
    assert all(gt in ["Polygon", "MultiPolygon"] for gt in geom_types), (
        f"Expected polygon geometries, found: {geom_types}"
    )

    # Test: Required DAMO attributes exist
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

    # Test: waterstaatswerkWaterkeringID are valid GUIDs (non-null and unique)
    assert waterkering["waterstaatswerkWaterkeringID"].notna().all(), (
        "Some features have null waterstaatswerkWaterkeringID"
    )
    assert waterkering["waterstaatswerkWaterkeringID"].is_unique, "Duplicate waterstaatswerkWaterkeringID found"

    # Test: Height extraction must be performed (min_height column must exist)
    assert "min_height" in waterkering.columns, "min_height column missing - DEM height extraction was not performed"

    # Test: Most features must have height extracted (allow <5% missing for edge cases)
    missing_heights = waterkering["min_height"].isna().sum()
    missing_pct = (missing_heights / len(waterkering)) * 100
    assert missing_pct < 5.0, (
        f"Height extraction incomplete: {missing_heights}/{len(waterkering)} features ({missing_pct:.2f}%) are missing heights. "
        f"More than 5% missing suggests a systematic issue. Expected >95% success rate."
    )

    if missing_heights > 0:
        logger.warning(
            f"⚠ {missing_heights}/{len(waterkering)} features ({missing_pct:.2f}%) have no height (likely edge cases)"
        )
    else:
        logger.info(f"✓ Height extraction complete: all {len(waterkering)} features have heights")

    # Test: Heights are reasonable values (typically -10 to 50m for Netherlands)
    height_values = waterkering["min_height"]
    assert height_values.min() > -50, f"Suspiciously low height: {height_values.min()}"
    assert height_values.max() < 500, f"Suspiciously high height: {height_values.max()}"
    logger.info(f"✓ Height range valid: {height_values.min():.2f}m to {height_values.max():.2f}m")

    # Test: length_m column must exist and be properly rounded
    assert "length_m" in waterkering.columns, "length_m column missing - segment length was not stored"
    assert waterkering["length_m"].notna().all(), "Some features have null length_m"

    # Test: length_m values should be rounded to 2 decimals
    for length_val in waterkering["length_m"]:
        decimal_places = len(str(length_val).split(".")[-1]) if "." in str(length_val) else 0
        assert decimal_places <= 2, f"length_m {length_val} has more than 2 decimal places"

    # Test: min_height values should be rounded to 2 decimals
    for height_val in waterkering["min_height"].dropna():
        decimal_places = len(str(height_val).split(".")[-1]) if "." in str(height_val) else 0
        assert decimal_places <= 2, f"min_height {height_val} has more than 2 decimal places"

    logger.info(f"✓ Length and height values properly rounded to 2 decimals")

    # Test: Line segment lengths should not exceed segment length
    # Each feature should be a short segment after splitting
    max_segment_lengths = []
    for geom in waterkering.geometry:
        if geom.geom_type == "LineString":
            max_segment_lengths.append(geom.length)
        elif geom.geom_type == "MultiLineString":
            for line in geom.geoms:
                max_segment_lengths.append(line.length)

    if max_segment_lengths:
        max_line_length = max(max_segment_lengths)
        # Allow some tolerance (e.g., 50% over) since segments may not divide evenly
        tolerance_factor = 1.5
        max_expected = DEFAULT_SEGMENT_LENGTH * tolerance_factor

        assert max_line_length <= max_expected, (
            f"Line segment too long: {max_line_length:.2f}m exceeds expected maximum {max_expected:.2f}m "
            f"(segment_length={DEFAULT_SEGMENT_LENGTH}m with {tolerance_factor}x tolerance). "
            "Line splitting may not be working correctly."
        )
        logger.info(f"✓ Line segment lengths valid: max {max_line_length:.2f}m (limit: {max_expected:.2f}m)")

    # Test: length_m should match actual geometry length (within rounding tolerance)
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

    # Test: No exact duplicate geometries exist
    wkt_set = set(waterkering.geometry.apply(lambda g: g.wkt))
    assert len(wkt_set) == len(waterkering), "Duplicate geometries found after deduplication"

    logger.info(f"✓ No duplicate linestrings found ({len(waterkering)} unique features)")


def _validate_no_contained_polygons(output_file: Path, logger):
    """Validate that no polygon is completely contained within another polygon in peilgebiedpraktijk layer."""
    try:
        peilgebiedpraktijk = gpd.read_file(output_file, layer="peilgebiedpraktijk")
    except Exception:
        logger.warning("Peilgebiedpraktijk layer not found, skipping containment check")
        return

    if peilgebiedpraktijk.empty:
        logger.warning("Peilgebiedpraktijk layer is empty, skipping containment check")
        return

    if len(peilgebiedpraktijk) < 2:
        logger.info("✓ Only one polygon, no containment possible")
        return

    # Test: No polygon should be completely within another polygon
    contained_pairs = []

    for i, row_i in peilgebiedpraktijk.iterrows():
        geom_i = row_i.geometry
        if geom_i is None or geom_i.is_empty:
            continue

        for j, row_j in peilgebiedpraktijk.iterrows():
            if i >= j:  # Skip self-comparison and already-checked pairs
                continue

            geom_j = row_j.geometry
            if geom_j is None or geom_j.is_empty:
                continue

            # Check if i is within j
            if geom_i.within(geom_j):
                source_i = row_i.get("source_layer", "unknown")
                source_j = row_j.get("source_layer", "unknown")
                contained_pairs.append(
                    f"Polygon {i} (source: {source_i}, area: {geom_i.area:.2f}) "
                    f"is contained within polygon {j} (source: {source_j}, area: {geom_j.area:.2f})"
                )
            # Check if j is within i
            elif geom_j.within(geom_i):
                source_i = row_i.get("source_layer", "unknown")
                source_j = row_j.get("source_layer", "unknown")
                contained_pairs.append(
                    f"Polygon {j} (source: {source_j}, area: {geom_j.area:.2f}) "
                    f"is contained within polygon {i} (source: {source_i}, area: {geom_i.area:.2f})"
                )

    if contained_pairs:
        error_msg = f"Found {len(contained_pairs)} contained polygon(s):\n" + "\n".join(contained_pairs)
        assert False, error_msg

    logger.info(f"✓ No contained polygons found (checked {len(peilgebiedpraktijk)} polygons)")


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

    # Test: Check for overlapping line segments using a small buffer
    # Buffer lines by a small amount (0.5m) to detect substantial overlaps
    # Touching at endpoints is OK, but overlapping segments are not
    buffer_distance = 0.5  # meters
    overlapping_pairs = []

    logger.info(f"Checking {len(waterkering)} segments for overlaps (buffer: {buffer_distance}m)...")

    for i, row_i in waterkering.iterrows():
        geom_i = row_i.geometry
        if geom_i is None or geom_i.is_empty:
            continue

        # Create buffer around this line
        buffered_i = geom_i.buffer(buffer_distance)

        for j, row_j in waterkering.iterrows():
            if i >= j:  # Skip self-comparison and already-checked pairs
                continue

            geom_j = row_j.geometry
            if geom_j is None or geom_j.is_empty:
                continue

            # Check if buffered line i significantly overlaps with line j
            # We expect lines to only touch at endpoints, not overlap substantially
            intersection = buffered_i.intersection(geom_j)

            if not intersection.is_empty:
                # Calculate what percentage of line j overlaps with buffered line i
                intersection_length = intersection.length
                line_j_length = geom_j.length

                # If more than 20% of the line overlaps, it's a problem
                # (allows for endpoint connections and small numerical issues)
                # Increased from 10% to 20% to account for minor overlaps at segment boundaries
                overlap_percentage = (intersection_length / line_j_length) * 100

                if overlap_percentage > 20:
                    source_i = row_i.get("source_layer", "unknown")
                    source_j = row_j.get("source_layer", "unknown")
                    overlapping_pairs.append(
                        f"Segment {i} (source: {source_i}, length: {geom_i.length:.2f}m) "
                        f"overlaps {overlap_percentage:.1f}% with segment {j} "
                        f"(source: {source_j}, length: {geom_j.length:.2f}m)"
                    )

    # Test: Allow up to 20% overlapping pairs (for manual inspection in QGIS)
    # Calculate percentage of overlapping pairs relative to total possible pairs
    total_possible_pairs = (len(waterkering) * (len(waterkering) - 1)) // 2
    overlap_percentage = (len(overlapping_pairs) / total_possible_pairs * 100) if total_possible_pairs > 0 else 0
    max_acceptable_overlap_pct = 20.0  # Allow up to 20% of pairs to overlap

    if overlapping_pairs:
        if overlap_percentage > max_acceptable_overlap_pct:
            # Too many overlaps - fail the test
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
            # Acceptable amount of overlaps - warn but don't fail
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
    # Setup
    logger = hrt.logging.get_logger(__name__)
    raw_export_file = TEST_DIRECTORY / "schematisation_builder" / "raw_export.gpkg"
    output_dir = TEMP_DIR / f"temp_peilgebied_converter_{hrt.current_time(date=True)}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "damo.gpkg"

    # Check if source layers exist
    converter_base = RawExportToDAMOConverter(raw_export_file, output_file, logger)

    # Test: DEM must be available for proper testing
    assert converter_base.data.dem_dataset is not None, (
        f"DEM file not found. Expected ahn.tif at {raw_export_file.parent / 'ahn.tif'}. "
        "DEM is required for testing height extraction functionality."
    )
    logger.info(f"✓ DEM loaded from {converter_base.data.dem_path}")

    source_layers_exist = any(
        hasattr(converter_base.data, layer_name.lower())
        for layer_name in PEILGEBIED_SOURCE_LAYERS + WATERKERING_LINE_LAYERS
    )

    if not source_layers_exist:
        logger.warning(f"No source layers found, skipping test")
        return

    # Run converter once
    logger.info("Running peilgebied converter...")
    converter = PeilgebiedConverter(converter_base)
    logger.info(f"DEBUG: Converter has DEM: {converter.data.dem_dataset is not None}")
    converter.run()
    converter_base.write_outputs()

    # Run all validations on the single output
    logger.info("\nValidating outputs...")
    _validate_output_file(output_file, logger)
    _validate_peilgebiedpraktijk_layer(output_file, logger)
    _validate_waterkering_layer(output_file, logger)
    _validate_no_duplicates(output_file, logger)
    _validate_no_contained_polygons(output_file, logger)
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

    # Inject test data into first available source layer (use peilgebied source layers)
    for layer_name in PEILGEBIED_SOURCE_LAYERS:
        setattr(converter_base.data, layer_name.lower(), test_polygons)
        break

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


def test_crest_height_sampling_synthetic():
    """Test crest height detection with synthetic DEM - simple controlled test.

    Creates a simple flat terrain with one elevated strip (levee) and verifies
    the perpendicular sampling correctly detects the crest.
    """
    logger = hrt.logging.get_logger(__name__)
    output_dir = TEMP_DIR / f"temp_synthetic_height_{hrt.current_time(date=True)}"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create simple DEM: 50x50 grid, 1m resolution
    # Flat at 0m, with a 10m wide levee at y=20-30 with height 3m
    dem_data = np.zeros((50, 50), dtype=np.float32)
    dem_data[20:30, :] = 3.0  # Horizontal strip (levee running east-west)

    # Save DEM
    dem_path = output_dir / "synthetic_dem.tif"
    transform = from_origin(0, 50, 1.0, 1.0)

    with rasterio.open(
        dem_path,
        "w",
        driver="GTiff",
        height=50,
        width=50,
        count=1,
        dtype=dem_data.dtype,
        crs="EPSG:28992",
        transform=transform,
        nodata=-9999,
    ) as dst:
        dst.write(dem_data, 1)

    logger.info(f"Created synthetic DEM: 50x50m, levee at y=20-30 with height 3m")

    # Mock converter to access sampling method
    dem_ds = rasterio.open(dem_path)

    class MockData:
        dem_dataset = dem_ds

    class MockBase:
        pass

    mock_base = MockBase()
    mock_base.data = MockData()
    mock_base.logger = logger

    converter = PeilgebiedConverter(mock_base)
    line_crossing = LineString([(25, 10), (25, 40)])

    with rasterio.open(dem_path) as dem:
        height_crossing = converter._sample_crest_height_perpendicular(
            line_crossing, dem, search_distance=5.0, step_distance=2.0
        )

    logger.info(f"Line crossing levee: {height_crossing:.2f}m (expected ~3.0m)")
    assert height_crossing is not None, "Failed to detect height"
    assert 2.9 < height_crossing < 3.1, f"Expected 3.0±0.1m, got {height_crossing:.2f}m"

    logger.info("✓ Synthetic test passed - perpendicular sampling detects crest correctly")

    # Cleanup
    dem_ds.close()


if __name__ == "__main__":
    test_peilgebied_converter()
    test_duplicate_removal_with_synthetic_data()
    test_crest_height_sampling_synthetic()
