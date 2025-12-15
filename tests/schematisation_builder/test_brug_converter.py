from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
from core.schematisation_builder.raw_export_to_DAMO_converter import RawExportToDAMOConverter

from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converters.brug_converter import (
    BrugConverter,
)
from tests.config import TEMP_DIR, TEST_DIRECTORY


def test_brug_converter():
    """Test BrugConverter creates proper brug→doorstroomopening relationships."""
    # Setup
    logger = hrt.logging.get_logger(__name__)
    raw_export_file = TEST_DIRECTORY / "schematisation_builder" / "raw_export.gpkg"
    output_dir = TEMP_DIR / f"temp_brug_converter_{hrt.current_time(date=True)}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "damo.gpkg"

    # Run converter
    converter_base = RawExportToDAMOConverter(raw_export_file, output_file, logger)
    converter = BrugConverter(converter_base)
    converter.run()
    converter_base.write_outputs()

    # Load output layers
    assert output_file.exists(), "Output file not created"
    brug = gpd.read_file(output_file, layer="brug")
    doorstroomopening = gpd.read_file(output_file, layer="doorstroomopening")

    # Test 1: Both layers exist and have data
    assert not brug.empty, "Brug layer is empty"
    assert not doorstroomopening.empty, "Doorstroomopening layer is empty"
    assert len(doorstroomopening) == len(brug), (
        f"Expected {len(brug)} doorstroomopeningen, got {len(doorstroomopening)}"
    )

    # Test 2: GlobalID fields are valid (non-null and unique)
    for layer_name, layer in [("brug", brug), ("doorstroomopening", doorstroomopening)]:
        assert "globalid" in layer.columns, f"{layer_name} missing globalid"
        assert layer["globalid"].notna().all(), f"{layer_name} has null globalid values"
        assert layer["globalid"].is_unique, f"{layer_name} has duplicate globalid values"

    # Test 3: Referential integrity - all brugid values exist in brug.globalid
    assert "brugid" in doorstroomopening.columns, "Doorstroomopening layer missing brugid"
    invalid_refs = doorstroomopening[~doorstroomopening["brugid"].isin(brug["globalid"])]
    assert invalid_refs.empty, f"Found {len(invalid_refs)} doorstroomopening(en) with invalid brugid"

    # Test 4: Each doorstroomopening is linked to exactly one brug
    assert doorstroomopening["brugid"].notna().all(), "Some doorstroomopeningen have null brugid"

    # Test 5: Geometry validation - doorstroomopening should have no geometry
    assert "geometry" not in doorstroomopening.columns or doorstroomopening["geometry"].isna().all(), (
        "Doorstroomopening should not have geometry"
    )


def _test_brug_converter_with_existing_layer():
    """Test BrugConverter updates existing doorstroomopening layer."""
    import uuid

    import pandas as pd

    # Setup
    logger = hrt.logging.get_logger(__name__)
    raw_export_file = TEST_DIRECTORY / "schematisation_builder" / "raw_export.gpkg"
    output_dir = TEMP_DIR / f"temp_brug_converter_existing_{hrt.current_time(date=True)}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "damo_with_existing.gpkg"

    # First run: create base layers
    converter_base = RawExportToDAMOConverter(raw_export_file, output_file, logger)
    converter_base.load_layers()

    # Get brug data and create mock existing layer
    brug_data = converter_base.data.brug

    # Create existing doorstroomopening with matching codes
    existing_doorstroomopening = pd.DataFrame(
        [{"globalid": str(uuid.uuid4()), "code": row["code"], "brugid": None} for _, row in brug_data.iterrows()]
    )

    # Inject existing layer
    converter_base.data.doorstroomopening = existing_doorstroomopening

    # Run converter (should update, not create)
    converter = BrugConverter(converter_base)
    converter.run()
    converter_base.write_outputs()

    # Load output layers
    assert output_file.exists(), "Output file not created"
    brug = gpd.read_file(output_file, layer="brug")
    doorstroomopening = gpd.read_file(output_file, layer="doorstroomopening")

    # Test: Existing layer was updated, not replaced
    assert len(doorstroomopening) == len(brug), "Doorstroomopening count changed"

    # Test: Foreign keys were populated
    assert doorstroomopening["brugid"].notna().all(), "Some doorstroomopeningen still have null brugid"

    # Test: Referential integrity
    invalid_refs = doorstroomopening[~doorstroomopening["brugid"].isin(brug["globalid"])]
    assert invalid_refs.empty, f"Found {len(invalid_refs)} doorstroomopeningen with invalid brugid"

    # Test: Geometry validation
    assert "geometry" not in doorstroomopening.columns or doorstroomopening["geometry"].isna().all(), (
        "Doorstroomopening should not have geometry"
    )


if __name__ == "__main__":
    test_brug_converter()
    test_brug_converter_with_existing_layer()
