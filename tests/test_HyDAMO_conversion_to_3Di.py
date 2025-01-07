import geopandas as gpd

from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_conversion_to_3Di import convert_to_3Di
from tests.config import TEST_DIRECTORY


def test_HyDAMO_conversion_to_3Di():
    TEST_DIRECTORY_HyDAMO_validator = TEST_DIRECTORY / "test_HyDAMO_converter_to_3Di"
    HyDAMO_path = TEST_DIRECTORY_HyDAMO_validator / "HyDAMO.gpkg"
    empty_schematisation_file_path = TEST_DIRECTORY_HyDAMO_validator / "empty.gpkg"

    if not HyDAMO_path.exists():
        raise FileNotFoundError(f"File {HyDAMO_path} does not exist")
    if not empty_schematisation_file_path.exists():
        raise FileNotFoundError(f"File {empty_schematisation_file_path} does not exist")

    convert_to_3Di(
        hydamo_file_path=HyDAMO_path,
        empty_schematisation_file_path=empty_schematisation_file_path,
        output_schematisation_directory=TEST_DIRECTORY_HyDAMO_validator,
    )

    # Check if the output schematisation file exists
    output_schematisation_file = TEST_DIRECTORY_HyDAMO_validator / "3Di_schematisation.gpkg"
    assert output_schematisation_file.exists()

    # Get all layers from the output schematisation file
    output_schematisation_layers = {
        layer: gpd.read_file(output_schematisation_file, layer=layer) for layer in ["connection_node", "channel"]
    }

    # Check if feature with code OAF-Q-121848 is present in the channel layer
    channel_layer = output_schematisation_layers["channel"]
    assert channel_layer[channel_layer["code"] == "OAF-Q-121848"].shape[0] == 1


# %%
if __name__ == "__main__":
    test_HyDAMO_conversion_to_3Di()

# %%
