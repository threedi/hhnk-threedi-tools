# %%
import sys
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import pytest

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources
from hhnk_threedi_tools.core.schematisation_builder.intermediate_converter import GemaalIntermediateConverter
from tests.config import TEMP_DIR, TEST_DIRECTORY


# %%
def test_gemaal_intermediate_converter():
    logger = hrt.logging.get_logger(__name__)

    raw_export_file_path = TEST_DIRECTORY / "schematisation_builder" / "raw_export.gpkg"
    temp_dir_out = TEMP_DIR / f"temp_profile_intermediate_converter_{hrt.current_time(date=True)}"
    Path(temp_dir_out).mkdir(parents=True, exist_ok=True)
    output_file_path = temp_dir_out / "damo.gpkg"

    converter = GemaalIntermediateConverter(
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
    for idx, row in pomp_layer.iterrows():
        gemaal_id = row.get("gemaalid")
        if gemaal_id is not None:
            assert gemaal_id in gemaal_layer["globalid"].values, (
                f"Pomp layer row {idx} has invalid 'gemaalid': {gemaal_id}"
            )


# %%
if __name__ == "__main__":
    test_gemaal_intermediate_converter()

# %%
