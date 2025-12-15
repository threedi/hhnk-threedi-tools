from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
from core.schematisation_builder.raw_export_to_DAMO_converter import RawExportToDAMOConverter

from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converters.gemaal_converter import (
    GemaalConverter,
)
from tests.config import TEMP_DIR, TEST_DIRECTORY


def test_gemaal_converter():
    """Test GemaalConverter creates proper gemaal-pomp relationships."""
    # Setup
    logger = hrt.logging.get_logger(__name__)
    raw_export_file = TEST_DIRECTORY / "schematisation_builder" / "raw_export.gpkg"
    output_dir = TEMP_DIR / f"temp_gemaal_converter_{hrt.current_time(date=True)}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "damo.gpkg"

    # Run converter
    converter_base = RawExportToDAMOConverter(raw_export_file, output_file, logger)
    converter = GemaalConverter(converter_base)
    converter.run()
    converter_base.write_outputs()

    # Load output layers
    assert output_file.exists(), "Output file not created"
    gemaal = gpd.read_file(output_file, layer="gemaal")
    pomp = gpd.read_file(output_file, layer="pomp")

    # Test 1: Both layers exist and have data
    assert not gemaal.empty, "Gemaal layer is empty"
    assert not pomp.empty, "Pomp layer is empty"
    assert len(pomp) == len(gemaal), f"Expected {len(gemaal)} pompen, got {len(pomp)}"

    # Test 2: GlobalID fields are valid (non-null and unique)
    for layer_name, layer in [("gemaal", gemaal), ("pomp", pomp)]:
        assert "globalid" in layer.columns, f"{layer_name} missing globalid"
        assert layer["globalid"].notna().all(), f"{layer_name} has null globalid values"
        assert layer["globalid"].is_unique, f"{layer_name} has duplicate globalid values"

    # Test 3: All gemaalid values exist in gemaal.globalid
    assert "gemaalid" in pomp.columns, "Pomp layer missing gemaalid"
    invalid_refs = pomp[~pomp["gemaalid"].isin(gemaal["globalid"])]
    assert invalid_refs.empty, f"Found {len(invalid_refs)} pomp(en) with invalid gemaalid"

    # Test 4: Each pomp is linked to exactly one gemaal
    assert pomp["gemaalid"].notna().all(), "Some pompen have null gemaalid"


if __name__ == "__main__":
    test_gemaal_converter()
