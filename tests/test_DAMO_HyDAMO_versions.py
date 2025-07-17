import json
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources
from hhnk_threedi_tools.core.schematisation_builder.DAMO_HyDAMO_converter import DAMO_to_HyDAMO_Converter
from tests.config import TEMP_DIR, TEST_DIRECTORY

LAYERS = ["HYDROOBJECT"]  # , "DUIKERSIFONHEVEL", "GEMAAL"]
temp_dir_out = TEMP_DIR / f"temp_DAMO_HyDAMO_converter_{hrt.current_time(date=True)}"


def test_DAMO_HyDAMO_versions():
    from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_validator import validate_hydamo

    damo_file_path = TEST_DIRECTORY / "schema_builder" / "DAMO.gpkg"
    hydamo_file_path = temp_dir_out / "HyDAMO.gpkg"

    versions = [("2.3", "2.3"), ("2.4.1", "2.4")]  # , ("2.5", "2.5")]

    columns_2_3 = []
    domains = []

    # LAYERS = ["PROFIELEN"]

    for damo_version, hydamo_version in versions:
        print("VERSIONS: ", (damo_version, hydamo_version))
        converter = DAMO_to_HyDAMO_Converter(
            damo_file_path=damo_file_path,
            hydamo_file_path=hydamo_file_path,
            damo_version=damo_version,
            hydamo_version=hydamo_version,
            layers=LAYERS,
            overwrite=False,
        )
        converter.run()
        domains.append(converter.domains)

    domains_23 = domains[0]
    domains_241 = domains[1]

    ## for version 2.3 and layer PROFIEL: check if typeprofiel is in domain names
    # Check if mapping is correct: in objects: soortmeetpunt might be integer, but hydamo file wants strings for soortmeetpunt
    ## for version 2.4 and layer PROFIEL: check if profieltype is in domain names
    # Check if mapping is correct: in objects: soortmeetpunt might be integer, but hydamo file wants strings for soortmeetpunt
    ## use other attributes from pdf to check if stuff works damo to hydamo

    missing_keys = [key for key in domains_241 if key not in domains_23]
    print(missing_keys)
    assert missing_keys == []
    # with open(Path(__file__).parent.parent.parent / f"domains_{damo_version}.json", "w") as f:
    #     json.dump(converter.domains, f, indent=4)

    # Check if HyDAMO.gpkg is created
    # assert hydamo_file_path.exists()

    # DAMO_hydroobject_gdf = gpd.read_file(damo_file_path, layer=LAYERS[0])
    # HyDAMO_hydroobject_gdf = gpd.read_file(hydamo_file_path, layer=LAYERS[0])
    # print(HyDAMO_hydroobject_gdf.columns)

    ## just check if the mapping is correct using some examples

    # if hydamo_version == "2.3":
    #     print("2.3")
    #     ## todo: check if column in original gpkg are in hydroobject
    #     assert "typeprofiel" in HyDAMO_hydroobject_gdf.columns
    #     assert HyDAMO_hydroobject_gdf["typeprofiel"].apply(lambda x: isinstance(x, (str))).all()
    # if hydamo_version == "2.4":
    #     print("2.4")
    #     assert "profieltype" in HyDAMO_hydroobject_gdf.columns
    #     assert HyDAMO_hydroobject_gdf["profieltype"].apply(lambda x: isinstance(x, (str))).all()

    # assert DAMO_hydroobject_gdf["categorieoppwaterlichaam"].apply(lambda x: isinstance(x, (int, np.integer))).all()

    # if columns_2_3 == []:
    #     columns_2_3 = DAMO_hydroobject_gdf.columns.to_list()
    # else:
    #     assert DAMO_hydroobject_gdf.columns.to_list() == columns_2_3

    # # Check if the column NEN3610id is added to the layer
    # assert "NEN3610id" in HyDAMO_hydroobject_gdf.columns

    # # Check if fields have proper field types
    # assert DAMO_hydroobject_gdf["categorieoppwaterlichaam"].apply(lambda x: isinstance(x, (int, np.integer))).all()
    # assert HyDAMO_hydroobject_gdf["categorieoppwaterlichaam"].apply(lambda x: isinstance(x, str)).all()

    # # Filter DAMO and HyDAMO on column objectid value 448639
    # # Check if the value for column categorieoppwaterlichaam is converted to a descriptive value
    # # In DAMO the value is 1, in HyDAMO the value is 'primair'
    # DAMO_hydroobject_gdf = DAMO_hydroobject_gdf[DAMO_hydroobject_gdf["objectid"] == 430639]
    # HyDAMO_hydroobject_gdf = HyDAMO_hydroobject_gdf[HyDAMO_hydroobject_gdf["objectid"] == 430639]

    # print("final assert")
    # print(DAMO_hydroobject_gdf)
    # assert (
    #     DAMO_hydroobject_gdf["categorieoppwaterlichaam"].values[0] == 1
    #     and HyDAMO_hydroobject_gdf["categorieoppwaterlichaam"].values[0] == "primair"
    # )

    # assert False


# %%
if __name__ == "__main__":
    damo_versions = ["2.3", "2.4.1", "2.5"]
    hydamo_versions = ["2.3", "2.4"]

    test_DAMO_HyDAMO_versions()

# %%
