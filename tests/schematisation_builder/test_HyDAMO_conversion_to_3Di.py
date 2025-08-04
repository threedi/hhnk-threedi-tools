# %%
import geopandas as gpd
import hhnk_research_tools as hrt

from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_conversion_to_3Di import convert_to_3Di
from tests.config import TEMP_DIR, TEST_DIRECTORY


def test_HyDAMO_conversion_to_3Di():
    output_schematisation_directory = TEMP_DIR / f"temp_HyDAMO_converter_to_3Di_{hrt.current_time(date=True)}"
    hydamo_file_path = TEST_DIRECTORY / "schematisation_builder" / "HyDAMO.gpkg"
    hydamo_layers = ["HYDROOBJECT"]
    empty_schematisation_file_path = None  # Use default from htt.resources

    convert_to_3Di(
        hydamo_file_path=hydamo_file_path,
        hydamo_layers=hydamo_layers,
        empty_schematisation_file_path=empty_schematisation_file_path,
        output_schematisation_directory=output_schematisation_directory,
    )

    # Check if the output schematisation file exists
    output_schematisation_file = output_schematisation_directory / "3Di_schematisation.gpkg"
    assert output_schematisation_file.exists()

    # Get all layers from the output schematisation file
    output_schematisation_layers = {
        layer: gpd.read_file(
            output_schematisation_file,
            layer=layer,
            engine="pyogrio",
        )
        for layer in ["connection_node", "channel"]
    }

    # Check if feature with code OAF-Q-121848 is present in the channel layer
    channel_layer = output_schematisation_layers["channel"]
    assert channel_layer[channel_layer["code"] == "OAF-Q-121848"].shape[0] == 1


# %%
if __name__ == "__main__":
    test_HyDAMO_conversion_to_3Di()
