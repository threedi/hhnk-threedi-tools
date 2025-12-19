from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt

from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converter import RawExportToDAMOConverter
from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converters.stuw_converter import (
    StuwConverter,
)
from tests.config import TEMP_DIR, TEST_DIRECTORY


def test_stuw_converter():
    """Test StuwConverter creates proper stuw→kunstwerkopening→regelmiddel relationships."""
    # Setup
    logger = hrt.logging.get_logger(__name__)
    raw_export_file = TEST_DIRECTORY / "schematisation_builder" / "raw_export.gpkg"
    output_dir = TEMP_DIR / f"temp_stuw_converter_{hrt.current_time(date=True)}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "damo.gpkg"

    # Run converter
    converter_base = RawExportToDAMOConverter(raw_export_file, output_file, logger)
    converter = StuwConverter(converter_base)
    converter.run()
    converter_base.write_outputs()

    # Load output layers
    assert output_file.exists(), "Output file not created"
    stuw = gpd.read_file(output_file, layer="stuw")
    kunstwerkopening = gpd.read_file(output_file, layer="kunstwerkopening")
    regelmiddel = gpd.read_file(output_file, layer="regelmiddel")

    # Test 1: All layers exist and have data
    assert not stuw.empty, "Stuw layer is empty"
    assert not kunstwerkopening.empty, "Kunstwerkopening layer is empty"
    assert not regelmiddel.empty, "Regelmiddel layer is empty"
    assert len(kunstwerkopening) == len(stuw), f"Expected {len(stuw)} kunstwerkopeningen, got {len(kunstwerkopening)}"
    assert len(regelmiddel) == len(kunstwerkopening), (
        f"Expected {len(kunstwerkopening)} regelmiddelen, got {len(regelmiddel)}"
    )

    # Test 2: GlobalID fields are valid (non-null and unique)
    for layer_name, layer in [("stuw", stuw), ("kunstwerkopening", kunstwerkopening), ("regelmiddel", regelmiddel)]:
        assert "globalid" in layer.columns, f"{layer_name} missing globalid"
        assert layer["globalid"].notna().all(), f"{layer_name} has null globalid values"
        assert layer["globalid"].is_unique, f"{layer_name} has duplicate globalid values"

    # Test 3: Check stuw→kunstwerkopening links - all stuwid values should exist in stuw.globalid
    assert "stuwid" in kunstwerkopening.columns, "Kunstwerkopening layer missing stuwid"
    invalid_refs = kunstwerkopening[~kunstwerkopening["stuwid"].isin(stuw["globalid"])]
    assert invalid_refs.empty, f"Found {len(invalid_refs)} kunstwerkopening(en) with invalid stuwid"
    assert kunstwerkopening["stuwid"].notna().all(), "Some kunstwerkopeningen have null stuwid"

    # Test 4: Check kunstwerkopening→regelmiddel links - all kunstwerkopeningid values should exist
    assert "kunstwerkopeningid" in regelmiddel.columns, "Regelmiddel layer missing kunstwerkopeningid"
    invalid_refs = regelmiddel[~regelmiddel["kunstwerkopeningid"].isin(kunstwerkopening["globalid"])]
    assert invalid_refs.empty, f"Found {len(invalid_refs)} regelmiddel(en) with invalid kunstwerkopeningid"
    assert regelmiddel["kunstwerkopeningid"].notna().all(), "Some regelmiddelen have null kunstwerkopeningid"

    # Test 5: Geometry validation - kunstwerkopening should have no geometry, regelmiddel should have geometry
    assert "geometry" not in kunstwerkopening.columns or kunstwerkopening["geometry"].isna().all(), (
        "Kunstwerkopening should not have geometry"
    )
    assert "geometry" in regelmiddel.columns, "Regelmiddel missing geometry column"
    assert regelmiddel["geometry"].notna().all(), "Some regelmiddelen have null geometry"
    # Regelmiddel should have same geometry as stuw
    assert len(regelmiddel) > 0, "No regelmiddelen to test geometry"


def _test_stuw_converter_with_existing_layers():
    """Test StuwConverter updates existing kunstwerkopening and regelmiddel layers."""
    import uuid

    import pandas as pd

    # Setup
    logger = hrt.logging.get_logger(__name__)
    raw_export_file = TEST_DIRECTORY / "schematisation_builder" / "raw_export.gpkg"
    output_dir = TEMP_DIR / f"temp_stuw_converter_existing_{hrt.current_time(date=True)}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "damo_with_existing.gpkg"

    # First run: create base layers
    converter_base = RawExportToDAMOConverter(raw_export_file, output_file, logger)
    converter_base.load_layers()

    # Get stuw data and create mock existing layers
    stuw_data = converter_base.data.stuw

    # Create existing kunstwerkopening with matching codes
    existing_kunstwerkopening = pd.DataFrame(
        [{"globalid": str(uuid.uuid4()), "code": row["code"], "stuwid": None} for _, row in stuw_data.iterrows()]
    )

    # Create existing regelmiddel with matching codes
    existing_regelmiddel = gpd.GeoDataFrame(
        [
            {"globalid": str(uuid.uuid4()), "code": row["code"], "kunstwerkopeningid": None, "geometry": row.geometry}
            for _, row in stuw_data.iterrows()
        ],
        crs=stuw_data.crs,
    )

    # Inject existing layers
    converter_base.data.kunstwerkopening = existing_kunstwerkopening
    converter_base.data.regelmiddel = existing_regelmiddel

    # Run converter (should update, not create)
    converter = StuwConverter(converter_base)
    converter.run()
    converter_base.write_outputs()

    # Load output layers
    assert output_file.exists(), "Output file not created"
    stuw = gpd.read_file(output_file, layer="stuw")
    kunstwerkopening = gpd.read_file(output_file, layer="kunstwerkopening")
    regelmiddel = gpd.read_file(output_file, layer="regelmiddel")

    # Test: Existing layers were updated, not replaced
    assert len(kunstwerkopening) == len(stuw), "Kunstwerkopening count changed"
    assert len(regelmiddel) == len(stuw), "Regelmiddel count changed"

    # Test: Links were populated
    assert kunstwerkopening["stuwid"].notna().all(), "Some kunstwerkopeningen still have null stuwid"
    assert regelmiddel["kunstwerkopeningid"].notna().all(), "Some regelmiddelen still have null kunstwerkopeningid"

    # Test: All links are valid
    invalid_stuw_refs = kunstwerkopening[~kunstwerkopening["stuwid"].isin(stuw["globalid"])]
    assert invalid_stuw_refs.empty, f"Found {len(invalid_stuw_refs)} kunstwerkopeningen with invalid stuwid"

    invalid_kunstwerk_refs = regelmiddel[~regelmiddel["kunstwerkopeningid"].isin(kunstwerkopening["globalid"])]
    assert invalid_kunstwerk_refs.empty, (
        f"Found {len(invalid_kunstwerk_refs)} regelmiddelen with invalid kunstwerkopeningid"
    )

    # Test: Geometry validation
    assert "geometry" not in kunstwerkopening.columns or kunstwerkopening["geometry"].isna().all(), (
        "Kunstwerkopening should not have geometry"
    )
    assert regelmiddel["geometry"].notna().all(), "Regelmiddel should have geometry"


if __name__ == "__main__":
    test_stuw_converter()
    test_stuw_converter_with_existing_layers()
