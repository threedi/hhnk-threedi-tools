# %%
import sys
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import pytest

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources
from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converters.gemaal_converter import (
    GemaalConverter,
)
from tests.config import TEMP_DIR, TEST_DIRECTORY

always_skip_for_now = True  # TODO remove later when test works


# %%
@pytest.mark.skipif(always_skip_for_now, reason="Remove later")
def test_gemaal_converter():
    logger = hrt.logging.get_logger(__name__)

    raw_export_file_path = TEST_DIRECTORY / "schematisation_builder" / "raw_export.gpkg"
    temp_dir_out = TEMP_DIR / f"temp_profile_intermediate_converter_{hrt.current_time(date=True)}"
    Path(temp_dir_out).mkdir(parents=True, exist_ok=True)
    output_file_path = temp_dir_out / "damo.gpkg"

    converter = GemaalConverter(
        raw_export_file_path=raw_export_file_path, output_file_path=output_file_path, logger=logger
    )
    converter.run()

    # Assert output exists
    assert output_file_path.exists(), "Output file does not exist."

    # Load gemaal layer
    gemaal_layer = gpd.read_file(converter.output_file_path, layer="gemaal")
    assert not gemaal_layer.empty, "Gemaal layer is empty."

    # Load pomp layer
    pomp_layer = gpd.read_file(converter.output_file_path, layer="pomp")
    assert not pomp_layer.empty, "Pomp layer is empty."

    # Check gemaal has globalid column (non empty, unique values)
    assert "globalid" in gemaal_layer.columns, "Gemaal layer is missing 'globalid' column."
    assert gemaal_layer["globalid"].notna().all(), "Gemaal layer 'globalid' column has null values."
    assert gemaal_layer["globalid"].is_unique, "Gemaal layer 'globalid' column has duplicate values."

    # Same for pomp layer
    assert "globalid" in pomp_layer.columns, "Pomp layer is missing 'globalid' column."
    assert pomp_layer["globalid"].notna().all(), "Pomp layer 'globalid' column has null values."
    assert pomp_layer["globalid"].is_unique, "Pomp layer 'globalid' column has duplicate values."

    # For each pomp check the gemaalid
    # Join pomp_layer with gemaal_layer on 'gemaalid' and 'globalid'
    joined = pomp_layer.merge(
        gemaal_layer[["globalid"]],
        left_on="gemaalid",
        right_on="globalid",
        how="left",
        indicator=True,
    )
    # Assert all gemaalid values in pomp_layer exist in gemaal_layer globalid
    invalid_rows = joined[joined["_merge"] == "left_only"]
    assert invalid_rows.empty, f"Pomp layer has rows with invalid 'gemaalid': {invalid_rows['gemaalid'].tolist()}"


# %%
if __name__ == "__main__":
    test_gemaal_converter()

# %%
