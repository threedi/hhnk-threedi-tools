# %%
"""Functional testing creating a grid.gpkg from a .nc"""

import hhnk_research_tools as hrt
import pytest

from hhnk_threedi_tools.core.result_rasters import netcdf_to_gridgpkg
from tests.config import FOLDER_TEST, TEMP_DIR

if __name__ == "__main__":
    import importlib

    importlib.reload(netcdf_to_gridgpkg)


def test_netcdf_to_gridgpkg():
    # input
    timesteps_seconds = ["max", 3600, 5400]

    netcdf_gpkg = netcdf_to_gridgpkg.NetcdfToGPKG.from_folder(
        folder=FOLDER_TEST,
        threedi_result=FOLDER_TEST.threedi_results.batch["batch_test"].downloads.piek_glg_T10.netcdf,
    )

    grid_gdf = netcdf_gpkg.create_base_gdf()

    grid_gdf = netcdf_gpkg.add_correction_parameters(
        grid_gdf=grid_gdf,
        replace_dem_below_perc=50,
        replace_water_above_perc=95,
        replace_pand_above_perc=95,
    )

    with pytest.raises(ValueError):
        grid_gdf = netcdf_gpkg.get_waterlevels(grid_gdf=grid_gdf, timesteps_seconds=[7250])

    # get and correct waterlevels
    grid_gdf = netcdf_gpkg.get_waterlevels(grid_gdf=grid_gdf, timesteps_seconds=timesteps_seconds)
    grid_gdf = netcdf_gpkg.correct_waterlevels(grid_gdf=grid_gdf, timesteps_seconds=timesteps_seconds)

    assert int(grid_gdf.loc[1, "wlvl_1h30min"] * 1e5) == 160608
    assert int(grid_gdf.loc[1, "wlvl_corr_1h30min"] * 1e5) == 66456

    # Test run statement
    output_file = TEMP_DIR.joinpath(f"grid_wlvl_{hrt.current_time('%H%M%S')}.gpkg")
    netcdf_gpkg.run(output_file=output_file)

    assert output_file.exists()


if __name__ == "__main__":
    test_netcdf_to_gridgpkg()
