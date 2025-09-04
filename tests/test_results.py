# %%
"""Functional testing creating a grid.gpkg from a .nc"""

import hhnk_research_tools as hrt
import pytest

from hhnk_threedi_tools.core.results import netcdf_essentials, netcdf_timeseries
from tests.config import FOLDER_TEST, TEMP_DIR

if __name__ == "__main__":
    import importlib

    importlib.reload(netcdf_essentials)
    importlib.reload(netcdf_timeseries)


def test_netcdf_timeseries():
    netcdf_ts = netcdf_timeseries.NetcdfTimeSeries.from_folder(
        folder=FOLDER_TEST,
        threedi_result=FOLDER_TEST.threedi_results.batch["batch_test"].downloads.piek_glg_T10.netcdf,
    )

    assert netcdf_ts.get_ts_index(time_seconds=3600) == 12

    attribute = "s1"
    element = "nodes"
    subset = "2D_OPEN_WATER"
    ts = netcdf_ts.get_ts(attribute=attribute, element=element, subset=subset)

    assert ts.shape == (422, 577)


def test_netcdf_essentials():
    # input
    user_defined_timesteps = ["max", 3600, 5400]
    output_file = TEMP_DIR.joinpath(f"grid_wlvl_{hrt.current_time('%H%M%S')}.gpkg")
    wlvl_correction = True
    overwrite = True

    netcdf_gpkg = netcdf_essentials.NetcdfEssentials.from_folder(
        folder=FOLDER_TEST,
        threedi_result=FOLDER_TEST.threedi_results.batch["batch_test"].downloads.piek_glg_T10.netcdf,
    )

    # Load timeseries data from netcdf into ness dataframe
    ness = netcdf_gpkg.load_ness()
    ness = netcdf_gpkg.process_ness(ness=ness)

    # Check data is loaded correctly
    assert ness is not None
    assert "data" in ness.columns
    assert ness["amount"][0] == 157  # 157 1d nodes
    assert ness.data[0].shape == (157, 577)

    # Create base GeoDataFrames containing geometries end metadata
    grid_gdf, node_gdf, line_gdf, meta_gdf = netcdf_gpkg.create_base_gdf()

    # Check shapes and sizes of geometries
    assert grid_gdf.shape[0] == 422  # 422 2d nodes
    assert node_gdf.shape[0] == 157  # 157 1d nodes
    assert line_gdf.shape[0] == 291

    # Test that error is raised when trying to get waterlevels with invalid timesteps
    with pytest.raises(ValueError):
        grid_gdf = netcdf_gpkg.append_data(ness=ness, gdf=grid_gdf, timesteps_seconds_output=[7250])

    # get and correct waterlevels
    timesteps_seconds_output = netcdf_gpkg.get_output_timesteps(user_defined_timesteps)
    grid_gdf = netcdf_gpkg.append_data(ness=ness, gdf=grid_gdf, timesteps_seconds_output=user_defined_timesteps)

    grid_gdf = netcdf_gpkg.add_correction_parameters(
        grid_gdf=grid_gdf,
        replace_dem_below_perc=50,
        replace_water_above_perc=95,
        replace_pand_above_perc=95,
    )
    grid_gdf = netcdf_gpkg.correct_waterlevels(grid_gdf=grid_gdf, timesteps_seconds_output=timesteps_seconds_output)

    assert int(grid_gdf.loc[1, "wlvl_1h30min"] * 1e5) == 160608
    assert int(grid_gdf.loc[1, "wlvl_corr_1h30min"] * 1e5) == 66456

    # Test run statement
    output_file = TEMP_DIR.joinpath(f"nc_ess_{hrt.current_time('%H%M%S')}.gpkg")
    netcdf_gpkg.run(output_file=output_file)

    assert output_file.exists()


# %%
if __name__ == "__main__":
    test_netcdf_timeseries()
    test_netcdf_essentials()
