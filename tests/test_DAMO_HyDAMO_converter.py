from pathlib import Path

import geopandas as gpd
import numpy as np

from hhnk_threedi_tools.core.schematisation_builder.DAMO_HyDAMO_converter import Converter
from tests.config import FOLDER_TEST, TEMP_DIR, TEST_DIRECTORY

LAYERS = ["HYDROOBJECT"]


def test_DAMO_HyDAMO_converter():
    """
    Test
    - If domain values in DAMO are converted to descriptive values in HyDAMO
    - If a NEN3610id column is added to the layer
    - If correct HyDAMO field types are assigned to the attributes based on the HyDAMO schema
    """
    TEST_DIRECTORY_DAMO_HyDAMO_converter = TEST_DIRECTORY / "test_DAMO_HyDAMO_converter"
    DAMO_path = TEST_DIRECTORY_DAMO_HyDAMO_converter / "DAMO.gpkg"
    HyDAMO_path = TEST_DIRECTORY_DAMO_HyDAMO_converter / "HyDAMO.gpkg"

    if not DAMO_path.exists():
        raise FileNotFoundError(f"File {DAMO_path} does not exist")

    # remove HyDAMO.gpkg if exists
    if HyDAMO_path.exists():
        HyDAMO_path.unlink()

    converter = Converter(DAMO_path=DAMO_path, HyDAMO_path=HyDAMO_path, layers=LAYERS)
    converter.run()

    # Check if HyDAMO.gpkg is created
    assert HyDAMO_path.exists()

    DAMO_hydroobject_gdf = gpd.read_file(DAMO_path, layer=LAYERS[0])
    HyDAMO_hydroobject_gdf = gpd.read_file(HyDAMO_path, layer=LAYERS[0])

    # Check if the column NEN3610id is added to the layer
    assert "NEN3610id" in HyDAMO_hydroobject_gdf.columns

    # Check if fields have proper field types
    assert DAMO_hydroobject_gdf["categorieoppwaterlichaam"].apply(lambda x: isinstance(x, (int, np.integer))).all()
    assert HyDAMO_hydroobject_gdf["categorieoppwaterlichaam"].apply(lambda x: isinstance(x, str)).all()

    # Filter DAMO and HyDAMO on column objectid value 448639
    # Check if the value for column categorieoppwaterlichaam is converted to a descriptive value
    # In DAMO the value is 1, in HyDAMO the value is 'primair'
    DAMO_hydroobject_gdf = DAMO_hydroobject_gdf[DAMO_hydroobject_gdf["objectid"] == 448639]
    HyDAMO_hydroobject_gdf = HyDAMO_hydroobject_gdf[HyDAMO_hydroobject_gdf["objectid"] == 448639]

    assert (
        DAMO_hydroobject_gdf["categorieoppwaterlichaam"].values[0] == 1
        and HyDAMO_hydroobject_gdf["categorieoppwaterlichaam"].values[0] == "primair"
    )


# %%
if __name__ == "__main__":
    test_DAMO_HyDAMO_converter()

# %%
