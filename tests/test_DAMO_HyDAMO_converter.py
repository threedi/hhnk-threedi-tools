# %%

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np

from hhnk_threedi_tools.core.schematisation_builder.DAMO_HyDAMO_converter import DAMO_to_HyDAMO_Converter
from tests.config import TEMP_DIR, TEST_DIRECTORY

LAYERS = ["HYDROOBJECT", "DUIKERSIFONHEVEL", "GEMAAL", "POMP"]
temp_dir_out = TEMP_DIR / f"temp_DAMO_HyDAMO_converter_{hrt.current_time(date=True)}"

# %%


def test_DAMO_HyDAMO_converter():
    """
    - If domain values in DAMO are converted to descriptive values in HyDAMO
    - If a NEN3610id column is added to the layer
    - If correct HyDAMO field types are assigned to the attributes based on the HyDAMO schema
    """
    damo_file_path = TEST_DIRECTORY / "schema_builder" / "DAMO.gpkg"
    hydamo_file_path = temp_dir_out / "HyDAMO.gpkg"

    converter = DAMO_to_HyDAMO_Converter(
        damo_file_path=damo_file_path,
        hydamo_file_path=hydamo_file_path,
        layers=LAYERS,
        overwrite=False,
    )
    converter.run()

    # Check if HyDAMO.gpkg is created
    assert hydamo_file_path.exists()

    # Check if fields have proper field types
    for layer in LAYERS:
        hydamo_gdf = gpd.read_file(hydamo_file_path, layer=layer)
        # Check if NEN3610id column is added in each layer
        assert "NEN3610id" in hydamo_gdf.columns

        if layer == "HYDROOBJECT":
            DAMO_hydroobject_gdf = gpd.read_file(damo_file_path, layer="HYDROOBJECT")
            assert DAMO_hydroobject_gdf["categorieoppwaterlichaam"].apply(lambda x: isinstance(x, (int, np.integer))).all()
            assert hydamo_gdf["categorieoppwaterlichaam"].apply(lambda x: isinstance(x, str)).all()

            # Filter DAMO and HyDAMO on column objectid value 448639
            DAMO_hydroobject_gdf = DAMO_hydroobject_gdf[DAMO_hydroobject_gdf["objectid"] == 448639]
            hydamo_gdf = hydamo_gdf[hydamo_gdf["objectid"] == 448639]

        if layer == "POMP":
            # Check if the field type of the column 'pompcapaciteit' is float
            assert hydamo_gdf["maximalecapaciteit"].dtype == "float64"
        if layer == "GEMAAL":
            # Check if the field type of the column 'gemaalcapiciteit' is float
            assert hydamo_gdf["maximalecapaciteit"].dtype == "float64"


# %%
if __name__ == "__main__":
    test_DAMO_HyDAMO_converter()

# %%
