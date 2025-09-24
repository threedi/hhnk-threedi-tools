# %%
import sys
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pytest

from tests.config import TEMP_DIR, TEST_DIRECTORY


# %%
# TODO remove skip when py312 implemented.
@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_peilgebiede_custom_fucntions():
    from hydamo_validation.datamodel import HyDAMO
    from hydamo_validation.functions.custom import intersected_pump_peilgebieden, peil_basic_properties

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
    hydroobject = gpd.read_file(hydamo_file_path, layer="hydroobject")
    combinatiepeilgebied_gdf = gpd.read_file(hydamo_file_path, layer="combinatiepeilgebied")

    # save layer in the temp folder
    gemaal.to_file(temp_dir_out, layer="gemaal")
    combinatiepeilgebied_gdf.to_file(temp_dir_out, layer="combinatiepeilgebied")
    polder.to_file(temp_dir_out, layer="polder")
    hydroobject.to_file(temp_dir_out, layer="hydroobject")
    # make a hydamo object out f the temp file
    hydamo = HyDAMO.from_geopackage(temp_dir_out, check_columns=False)

    # run functions
    results_peil_basic_properties = peil_basic_properties(gpd.GeoDataFrame, hydamo)

    no_holes_peilen = [
        "CMB_GPG-W-32",
        "CMB_GPG-Q-140723",
        "CMB_2030 W",
        "CMB_GPG-JF-42",
        "CMB_GPG-Q-140712",
        "CMB_GPG-JF-41",
        "CMB_GPG-N-320",
        "CMB_GPG-Q-140834",
        "CMB_2020-5",
        "CMB_GPG-A-19468",
        "CMB_GPG-Q-140871",
        "CMB_GPG-Q-140710",
        "CMB_GPG-Q-140709",
        "CMB_GPG-Q-140876",
        "CMB_GPG-Q-140853",
        "CMB_GPG-A-19469",
    ]

    no_holes_peils = results_peil_basic_properties.loc[results_peil_basic_properties["code_holes"].isna()]

    code_peilgen_geen_hydroobject = [
        "CMB_2040-2060",
        "CMB_2030 W",
        "CMB_GPG-E-1091",
        "CMB_1000-01",
        "CMB_2040-2030-01",
    ]
    peil_no_hydroobject = results_peil_basic_properties.loc[results_peil_basic_properties["hydroobject_count"] == 0]

    # asset functions
    assert np.min(results_peil_basic_properties["area"] > 100.0)
    assert no_holes_peils["code"].isin(no_holes_peilen).all()
    assert peil_no_hydroobject["code"].isin(code_peilgen_geen_hydroobject).all()


#
# %%
if __name__ == "__main__":
    test_peilgebiede_custom_fucntions()

# %%
