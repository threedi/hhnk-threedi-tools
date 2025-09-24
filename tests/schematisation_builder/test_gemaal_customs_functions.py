# %%
import sys
import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pytest
import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources
from tests.config import TEMP_DIR, TEST_DIRECTORY

# %%
# TODO remove skip when py312 implemented.
@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_gemaal_custom_fucntions():
    from hydamo_validation.datamodel import HyDAMO
    from hydamo_validation.functions.custom import gemaal_streefpeil_value, intersected_pump_peilgebieden

    """
    This function will test the customs function that where done for gemaal converter. Specifically it test to
    functions: intersected_pump_peilgebieden, gemaal_streefpeil_value
    """
    logger = hrt.logging.get_logger(__name__)

    # load data
    # TEST_DIRECTORY = Path(r"D:\github\jacosta\hhnk-threedi-tools\tests\data")
    # TEMP_DIR = TEST_DIRECTORY / 'temp'
    hydamo_file_path = TEST_DIRECTORY / "schematisation_builder" / "HyDAMO.gpkg"
    temp_dir_out = TEMP_DIR / f"HyDAMO_gemaal_test_subset_{hrt.current_time(date=True)}.gpkg"

    # select layer to do the test
    gemaal = gpd.read_file(hydamo_file_path, layer="gemaal")
    polder = gpd.read_file(hydamo_file_path, layer="polder")
    combinatiepeilgebied_gdf = gpd.read_file(hydamo_file_path, layer="combinatiepeilgebied")

    # save layer in the temp folder
    gemaal.to_file(temp_dir_out, layer="gemaal")
    combinatiepeilgebied_gdf.to_file(temp_dir_out, layer="combinatiepeilgebied")
    polder.to_file(temp_dir_out, layer="polder")

    # make a hydamo object out f the temp file
    hydamo = HyDAMO.from_geopackage(temp_dir_out, check_columns=False)

    # run functions
    results_intersected_pump_peilgebieden = intersected_pump_peilgebieden(gpd.GeoDataFrame, hydamo)
    results_gemaal_streefpeil_value = gemaal_streefpeil_value(gpd.GeoDataFrame, hydamo)

    # # assert intersected code peilgebied KMG-Q-25259
    # val_25259 = result_streefpeil_value.loc[result_streefpeil_value['code']=='KGM-Q-25259', 'pgd_codes'].values[0]
    # assert val_25259 == "CMB_2020-6, CMB_GPG-Q-140709"

    # # assert intersected code peilgebied KGM-Q-25263
    # val_25263 = result_streefpeil_value.loc[result_streefpeil_value["code"] == "KGM-Q-25263", "pgd_codes"].values[0]
    # assert val_25263 == "CMB_GPG-W-32, CMB_GPG-Q-140717"

    # #gemaal code KGM-Q-25259 in intersected with peilgebiedes CMB_2020-6, CMB_GPG-Q-140709
    # val_25263 = result_pump_peilgebieden.loc[result_pump_peilgebieden["code"] == "KGM-Q-25263", "pgd_codes"].values[0]
    # assert val_25263 == "CMB_GPG-W-32, CMB_GPG-Q-140717"

    # #gemaal code KGM-Q-25259 in intersected with peilgebiedes CMB_2020-6, CMB_GPG-Q-140709
    # val_25263 = result_pump_peilgebieden.loc[result_pump_peilgebieden["code"] == "KGM-Q-25265", "pgd_codes"].values[0]
    # assert val_25263 == "CMB_GPG-Q-140716, CMB_1000-01"

    # asset functions
    assert np.sum(results_intersected_pump_peilgebieden["distance_to_peilgebied"] == 0.0)
    assert np.average(results_gemaal_streefpeil_value["aantal_peilgebieden"] == 2.0)

# %%
if __name__ == "__main__":
    test_gemaal_custom_fucntions()

# %%
