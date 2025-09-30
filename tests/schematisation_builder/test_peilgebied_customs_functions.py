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
    from hydamo_validation.functions.custom import (
        kruising_met_waterloop,
        peil_basic_properties,
        peil_verbonde,
        peil_versus_AHN,
        peilgebieded_waterstand_dm,
    )

    """
    This function will test the customs function that where done for the peilgebied converter. Specifically it test to
    functions: peil_basic_properties, kruising_met_waterloop
    """
    logger = hrt.logging.get_logger(__name__)

    # load data
    # TEST_DIRECTORY = Path(r"D:\github\jacosta\hhnk-threedi-tools\tests\data")
    # TEMP_DIR = TEST_DIRECTORY / "temp"
    hydamo_file_path = TEST_DIRECTORY / "schematisation_builder" / "HyDAMO.gpkg"
    temp_dir_out = TEMP_DIR / f"HyDAMO_gemaal_test_subset_{hrt.current_time(date=True)}.gpkg"

    # select layer to do the test
    polder = gpd.read_file(hydamo_file_path, layer="polder")
    hydroobject = gpd.read_file(hydamo_file_path, layer="hydroobject")
    combinatiepeilgebied_gdf = gpd.read_file(hydamo_file_path, layer="combinatiepeilgebied")
    stuw_gdf = gpd.read_file(hydamo_file_path, layer="stuw")

    # save layer in the temp folder
    combinatiepeilgebied_gdf.to_file(temp_dir_out, layer="combinatiepeilgebied")
    polder.to_file(temp_dir_out, layer="polder")
    hydroobject.to_file(temp_dir_out, layer="hydroobject")
    stuw_gdf.to_file(temp_dir_out, layer="stuw")

    # make a hydamo object out f the temp file
    hydamo = HyDAMO.from_geopackage(temp_dir_out, check_columns=False)

    # run functions to be tested
    results_peil_basic_properties = peil_basic_properties(gpd.GeoDataFrame, hydamo)
    results_kruising_met_waterloop = kruising_met_waterloop(gpd.GeoDataFrame, hydamo)
    results_peil_verbonde = peil_verbonde(gpd.GeoDataFrame, hydamo)
    results_peil_versus_AHN = peil_versus_AHN(gpd.GeoDataFrame, hydamo)
    results_peilgebieded_waterstand_dm = peilgebieded_waterstand_dm(gpd.GeoDataFrame, hydamo)

    # make some asserts
    holes_in_peils_code = ["CMB_2030 W", "CMB_1000-01"]
    holes_in_peils = results_peil_basic_properties.loc[results_peil_basic_properties["holes_in_polygon"] >= 1]

    # peilgebieden that have no hydroobject linked to them
    code_peilgen_geen_hydroobject = [
        "CMB_2040-2060",
        "CMB_2030 W",
        "CMB_GPG-E-1091",
        "CMB_1000-01",
        "CMB_2040-2030-01",
    ]
    peil_no_hydroobject = results_peil_basic_properties.loc[results_peil_basic_properties["hydroobject_count"] == 0]

    # peilgen that has aan stuw with a lower hoogstedoorstroomhoogte than streefpeil_zomer_bovengrens
    code_peilen_met_stuw_fout = results_kruising_met_waterloop.loc[
        results_kruising_met_waterloop["stuw_fout_count"] > 1, "code"
    ]
    # codes from peilegen with wrong stuw crest level to be asserted
    peilen_with_stuw_fout = [
        "CMB_2040-2060",
        "CMB_2040-2020",
        "CMB_GPG-N-323",
        "CMB_GPG-Q-140705",
        "CMB_GPG-Q-140718",
        "CMB_GPG-Q-140717",
        "CMB_GPG-W-32",
        "CMB_GPG-Q-140716",
        "CMB_GPG-Q-140723",
        "CMB_2020-7",
        "CMB_GPG-N-43",
        "CMB_2020-6",
        "CMB_GPG-Q-140710",
        "CMB_GPG-Q-140709",
        "CMB_GPG-A-19469",
        "CMB_GPG-E-1091",
        "CMB_2030 W",
        "CMB_GPG-Q-140712",
        "CMB_2040-2030-01",
    ]

    # waterstand median values to be asserted from peilgebieded_waterstand_dm function
    CMB_GP_JF_41 = results_peilgebieded_waterstand_dm.loc[
        results_peilgebieded_waterstand_dm["code"] == "CMB_GPG-JF-41", "waterstand_median"
    ].values[0]
    CMB_GPG_N_323 = results_peilgebieded_waterstand_dm.loc[
        results_peilgebieded_waterstand_dm["code"] == "CMB_GPG-N-323", "waterstand_median"
    ].values[0]
    CMB_GPG_Q_140705 = results_peilgebieded_waterstand_dm.loc[
        results_peilgebieded_waterstand_dm["code"] == "CMB_GPG-Q-140705", "waterstand_median"
    ].values[0]

    # asset functions
    assert np.min(results_peil_basic_properties["area"] > 100.0)
    assert holes_in_peils["code"].isin(holes_in_peils_code).all()
    assert peil_no_hydroobject["code"].isin(code_peilgen_geen_hydroobject).all()
    assert code_peilen_met_stuw_fout.isin(peilen_with_stuw_fout).all()
    assert np.sum(results_peil_verbonde["num_vertices"]) == 1890
    assert np.average([results_peil_versus_AHN["percentile_90_min"]]) == 0.21595743498519848
    assert CMB_GP_JF_41 == -0.875
    assert np.round(CMB_GPG_N_323, 3) == -0.88
    assert np.round(CMB_GPG_Q_140705, 3) == -0.77


#
# %%
if __name__ == "__main__":
    test_peilgebiede_custom_functions()

# %%
