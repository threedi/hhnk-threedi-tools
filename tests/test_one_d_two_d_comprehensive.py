from unittest.mock import MagicMock, patch

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import LineString

from hhnk_threedi_tools.core.checks.one_d_two_d import OneDTwoDCheck


class MockableOneDTwoDCheck(OneDTwoDCheck):
    def __init__(self):
        pass


@patch.object(MockableOneDTwoDCheck, "_read_pumpline_results")
@patch.object(MockableOneDTwoDCheck, "_read_flowline_results")
def test_happy_case_run_flowline_stats(mock_read_flowline, mock_read_pumpline):
    # Arrange
    mock_flowlines_gdf = gpd.GeoDataFrame(
        {
            "id": [1, 2],
            "content_type": ["1d2d", "2d"],
            "geometry": [LineString([[0, 0], [1, 1]]), LineString([[1, 1], [2, 2]])],
        }
    )
    mock_pumplines_gdf = gpd.GeoDataFrame(
        {
            "id": [100, 101],
            "content_type": ["pump_line", "pump_line"],
            "geometry": [LineString([[0, 0], [0.5, 0.5]]), LineString([[2, 2], [3, 3]])],
        }
    )
    mock_read_flowline.return_value = mock_flowlines_gdf
    mock_read_pumpline.return_value = mock_pumplines_gdf
    check = MockableOneDTwoDCheck()

    # Act
    result = check.run_flowline_stats()

    # Assert
    mock_read_flowline.assert_called_once()
    mock_read_pumpline.assert_called_once()
    assert isinstance(result, gpd.GeoDataFrame)
    assert len(result) == 4
    assert result["content_type"].tolist() == ["1d2d", "2d", "pump_line", "pump_line"]


@patch.object(MockableOneDTwoDCheck, "_read_pumpline_results")
@patch.object(MockableOneDTwoDCheck, "_read_flowline_results")
def test_worst_case_run_flowline_stats_empty_data(mock_read_flowline, mock_read_pumpline):
    # Arrange
    empty_flowlines_gdf = gpd.GeoDataFrame({"geometry": []})
    empty_pumplines_gdf = gpd.GeoDataFrame({"geometry": []})
    mock_read_flowline.return_value = empty_flowlines_gdf
    mock_read_pumpline.return_value = empty_pumplines_gdf
    check = MockableOneDTwoDCheck()

    # Act
    result = check.run_flowline_stats()

    # Assert
    mock_read_flowline.assert_called_once()
    mock_read_pumpline.assert_called_once()
    assert isinstance(result, gpd.GeoDataFrame)
    assert len(result) == 0


@patch.object(MockableOneDTwoDCheck, "_read_pumpline_results")
@patch.object(MockableOneDTwoDCheck, "_read_flowline_results")
def test_worst_case_run_flowline_stats_zero_length_geometries(mock_read_flowline, mock_read_pumpline):
    # Arrange
    flowlines_with_zero_length = gpd.GeoDataFrame(
        {
            "id": [1, 2],
            "content_type": ["1d2d", "2d"],
            "geometry": [LineString([[0, 0], [0, 0]]), LineString([[1, 1], [2, 2]])],
        }
    )
    empty_pumplines_gdf = gpd.GeoDataFrame({"geometry": []})
    mock_read_flowline.return_value = flowlines_with_zero_length
    mock_read_pumpline.return_value = empty_pumplines_gdf
    check = MockableOneDTwoDCheck()

    # Act
    result = check.run_flowline_stats()

    # Assert
    mock_read_flowline.assert_called_once()
    mock_read_pumpline.assert_called_once()

    assert len(result) == 1
    assert result.geometry.length.iloc[0] > 0
    assert result["id"].iloc[0] == 2


@patch("hhnk_threedi_tools.core.checks.one_d_two_d.NetcdfToGPKG")
@patch("hhnk_threedi_tools.core.checks.one_d_two_d.GridToWaterLevel")
@patch("hhnk_threedi_tools.core.checks.one_d_two_d.GridToWaterDepth")
@patch("hhnk_threedi_tools.core.checks.one_d_two_d.gpd.read_file")
def test_happy_case_run_wlvl_depth_at_timesteps(mock_read_file, mock_depth, mock_level, mock_netcdf):
    # Arrange
    mock_netcdf_instance = MagicMock()
    mock_netcdf.from_folder.return_value = mock_netcdf_instance
    mock_grid_gdf = MagicMock()
    mock_read_file.return_value = mock_grid_gdf
    mock_level_instance = MagicMock()
    mock_level_instance.run.return_value = MagicMock()
    mock_level.return_value.__enter__.return_value = mock_level_instance
    mock_depth_instance = MagicMock()
    mock_depth_instance.run.return_value = MagicMock()
    mock_depth.return_value.__enter__.return_value = mock_depth_instance
    check = MockableOneDTwoDCheck()
    check.folder = MagicMock()
    check.output_fd = MagicMock()
    check.result_fd = MagicMock()

    check.TIMESTEPS = [1, 3, 15]

    # Act
    check.run_wlvl_depth_at_timesteps(chunksize=1024, overwrite=True)

    # Assert
    mock_netcdf.from_folder.assert_called_once()
    mock_netcdf_instance.run.assert_called_once()
    assert mock_level.call_count == 3
    assert mock_depth.call_count == 3


@patch("hhnk_threedi_tools.core.checks.one_d_two_d.NetcdfToGPKG")
@patch("hhnk_threedi_tools.core.checks.one_d_two_d.GridToWaterLevel")
@patch("hhnk_threedi_tools.core.checks.one_d_two_d.GridToWaterDepth")
@patch("hhnk_threedi_tools.core.checks.one_d_two_d.gpd.read_file")
def test_worst_case_run_wlvl_depth_at_timesteps_depth_calculation_fails(
    mock_read_file, mock_depth, mock_level, mock_netcdf
):
    # Arrange
    mock_netcdf_instance = MagicMock()
    mock_netcdf.from_folder.return_value = mock_netcdf_instance
    mock_grid_gdf = MagicMock()
    mock_read_file.return_value = mock_grid_gdf
    mock_level_instance = MagicMock()
    mock_level_instance.run.return_value = MagicMock()
    mock_level.return_value.__enter__.return_value = mock_level_instance
    mock_depth.return_value.__enter__.side_effect = RuntimeError("DEM file not found")
    check = MockableOneDTwoDCheck()
    check.folder = MagicMock()
    check.output_fd = MagicMock()
    check.result_fd = MagicMock()

    check.TIMESTEPS = [1, 3, 15]

    # Act & Assert
    with pytest.raises(RuntimeError, match="DEM file not found"):
        check.run_wlvl_depth_at_timesteps(chunksize=1024, overwrite=True)

    mock_level.assert_called()
    assert mock_level_instance.run.call_count == 1
    mock_depth.assert_called_once()
    mock_depth.assert_called_with(
        dem_path=check.folder.model.schema_base.rasters.dem,
        wlvl_path=mock_level_instance.run.return_value,
    )


@patch("hhnk_threedi_tools.core.checks.one_d_two_d.hrt.threedi.line_geometries_to_coords")
def test_worst_case_read_flowline_results_extreme_values(mock_coords):
    # Arrange
    mock_coords.return_value = [LineString([[0, 0], [1, 1]])]
    grid_result = MagicMock()
    grid_result.lines.line_geometries = np.array([[[0, 0], [1, 1]]])
    grid_result.lines.id = np.array([1])
    grid_result.lines.content_pk = np.array([10])
    grid_result.lines.content_type = np.array(["channel"])
    grid_result.lines.kcu = np.array([100])
    mock_timeseries = MagicMock()
    mock_timeseries.q = np.array([[1e6], [-1e6], [1e6]])
    mock_timeseries.u1 = np.array([[100.0], [-100.0], [100.0]])
    grid_result.lines.timeseries.return_value = mock_timeseries
    # Create DataFrame and manually add .value attribute to each Series column
    df = pd.DataFrame({"t_start_rain": [0], "t_end_rain": [1], "t_end_sum": [2]})
    # Add .value property to series for backward compatibility with old pandas
    for col in df.columns:
        setattr(df[col], "value", df[col].iloc[0])

    check = MockableOneDTwoDCheck()
    check.grid_result = grid_result
    check.timestep_df = df

    # Act
    result = check._read_flowline_results()

    # Assert
    assert isinstance(result, gpd.GeoDataFrame)
    assert len(result) == 1
