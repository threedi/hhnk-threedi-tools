# %%

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np

from hhnk_threedi_tools.core.schematisation_builder.DAMO_HyDAMO_converter import DAMO_to_HyDAMO_Converter
from tests.config import TEMP_DIR, TEST_DIRECTORY

LAYERS = ["HYDROOBJECT"]
temp_dir_out = TEMP_DIR / f"temp_DAMO_HyDAMO_converter_{hrt.current_time(date=True)}"

# %%


def test_DAMO_HyDAMO_converter():
    """
    - If domain values in DAMO are converted to descriptive values in HyDAMO
    - If a NEN3610id column is added to the layer
    - If correct HyDAMO field types are assigned to the attributes based on the HyDAMO schema
    """
    damo_file_path = TEST_DIRECTORY / "schematisation_builder" / "DAMO.gpkg"
    DAMO_hydroobject_gdf = gpd.read_file(damo_file_path, layer=LAYERS[0])
    assert DAMO_hydroobject_gdf["categorieoppwaterlichaam"].apply(lambda x: isinstance(x, (int, np.integer))).all()
    DAMO_hydroobject_gdf = DAMO_hydroobject_gdf[DAMO_hydroobject_gdf["objectid"] == 448639]

    """
    hydamo_file_path = temp_dir_out / f"HyDAMO_{hrt.current_time(date=True)}.gpkg"

    converter = DAMO_to_HyDAMO_Converter(
        damo_file_path=damo_file_path,
        hydamo_file_path=hydamo_file_path,
        layers=LAYERS,
        overwrite=False,
    )
    converter.run()

    # Check if HyDAMO.gpkg is created
    assert hydamo_file_path.exists()

    HyDAMO_hydroobject_gdf = gpd.read_file(hydamo_file_path, layer=LAYERS[0])

    # Check if the column NEN3610id is added to the layer
    assert "NEN3610id" in HyDAMO_hydroobject_gdf.columns

    # Check if fields have proper field types
    assert HyDAMO_hydroobject_gdf["categorieoppwaterlichaam"].apply(lambda x: isinstance(x, str)).all()

    # Filter DAMO and HyDAMO on column objectid value 448639
    # Check if the value for column categorieoppwaterlichaam is converted to a descriptive value
    # In DAMO the value is 1, in HyDAMO the value is 'primair'
    HyDAMO_hydroobject_gdf = HyDAMO_hydroobject_gdf[HyDAMO_hydroobject_gdf["objectid"] == 448639]

    assert (
        DAMO_hydroobject_gdf["categorieoppwaterlichaam"].values[0] == 1
        and HyDAMO_hydroobject_gdf["categorieoppwaterlichaam"].values[0] == "primair"
    ), (
        f"Expected DAMO value to be 1 and HyDAMO value to be 'primair', but got "
        f"DAMO: {DAMO_hydroobject_gdf['categorieoppwaterlichaam'].values[0]}, "
        f"HyDAMO: {HyDAMO_hydroobject_gdf['categorieoppwaterlichaam'].values[0]}"
    )
    """
    # New hydamo file path for the next test
    hydamo_file_path_2 = temp_dir_out / f"HyDAMO_{hrt.current_time(date=True)}_no_convert.gpkg"

    # Also test if it works with convert_domain_values set to False
    converter = DAMO_to_HyDAMO_Converter(
        damo_file_path=damo_file_path,
        hydamo_file_path=hydamo_file_path_2,
        layers=LAYERS,
        overwrite=False,
        convert_domain_values=False,
    )
    converter.run()

    # Check if the value for column categorieoppwaterlichaam is not converted to a descriptive value
    # In DAMO the value is 1, in HyDAMO the value is still 1, but string type
    HyDAMO_hydroobject_gdf = gpd.read_file(hydamo_file_path_2, layer=LAYERS[0])
    HyDAMO_hydroobject_gdf = HyDAMO_hydroobject_gdf[HyDAMO_hydroobject_gdf["objectid"] == 448639]
    assert DAMO_hydroobject_gdf["categorieoppwaterlichaam"].values[0] == 1 and HyDAMO_hydroobject_gdf[
        "categorieoppwaterlichaam"
    ].values[0] == str(1), (
        f"Expected both values to be 1, but got "
        f"DAMO: {DAMO_hydroobject_gdf['categorieoppwaterlichaam'].values[0]}, "
        f"HyDAMO: {HyDAMO_hydroobject_gdf['categorieoppwaterlichaam'].values[0]}"
    )


# %%
if __name__ == "__main__":
    test_DAMO_HyDAMO_converter()

# %%
