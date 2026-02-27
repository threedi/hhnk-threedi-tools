from pathlib import Path
from typing import Union

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd

from hhnk_threedi_tools.core.results import column_index

logger = hrt.logging.get_logger(__name__)


class CorrectWaterLevels:
    """
    Parameters
    ----------
    grid_gdf : gpd.GeoDataFrame
        GeodataFrame with 2D grid cells containing waterlevels
    waterdeel_path : str, optional
        Path to waterdeel layer. If None, not used for cell selection.
    waterdeel_layer : str, optional
        Layer name for waterdeel if part of a GeoPackage.
    panden_path : str, optional
        Path to panden layer. If None, not used for cell selection.
    panden_layer : str, optional
        Layer name for panden if part of a GeoPackage.
    user_defined_timesteps : list[int], optional
        List of output timesteps (seconds or "max").

    Methods
    -------
    _calculate_layer_area_per_cell(...)
        Calculate area and percentage of a layer per grid cell.
    _add_correction_parameters(...)
        Add columns to grid_gdf for waterlevel correction logic.
    correct_water_levels(...)
        Apply water level correction for selected cells.
    """

    def _calculate_layer_area_per_cell(  # TODO move to correct waterlevels?
        self,
        grid_gdf: gpd.GeoDataFrame = None,
        layer_path: Union[Path, hrt.File] = None,
        layer_name: str = None,
    ) -> gpd.GeoDataFrame:
        """Calculate for each grid cell the area and percentage of total area of the
        input layer. Returns the area and percentage columns.

        ----------
        grid_gdf : gpd.GeoDataFrame
            gdf with grid cells. Created inside main class
        layer_path : Path or hrt.File
            Path to layer to calculate area and percentage from
        layer_name : str, optional, by default None
            Name of layer if the layer is part of a gpkg
        """
        gdf = None
        # Load layer as gdf
        if grid_gdf is not None and layer_path is not None:
            logger.warning("Both grid_gdf and layer_path provided, using grid_gdf")
        elif layer_path is not None and layer_path.exists():
            grid_gdf = gpd.read_file(str(layer_path), layer=layer_name)
        else:
            logger.warning(f"Couldn't load {layer_path}. Ignoring it in correction.")

        if gdf is not None:
            area_col = "area"  # area in m2
            perc_col = "perc"  # percentage of total area

            if area_col in grid_gdf:
                raise ValueError(f"Column {area_col} was already found in grid_gdf.")

            gdf["value"] = 1
            # Overlay grid with input shape.
            overlay_df = gpd.overlay(grid_gdf[["id", "geometry"]], gdf[["value", "geometry"]], how="intersection")

            # Calculate sum of area per cell
            overlay_df[area_col] = overlay_df.area

            # Group by ids so we get the total area per cell
            overlay_df_grouped = overlay_df.groupby("id")[[area_col]].agg("sum")

            # Put in area in grid gdf and calculate percentage.
            grid_gdf_merged = grid_gdf.merge(overlay_df_grouped[area_col], left_on="id", right_on="id", how="left")
            grid_gdf_merged[perc_col] = grid_gdf_merged[area_col] / grid_gdf_merged.area * 100
            return grid_gdf_merged[[area_col, perc_col]]
        return np.nan

    def _add_correction_parameters(
        self,
        grid_gdf: gpd.GeoDataFrame,
        replace_dem_below_perc: float = 50,
        replace_water_above_perc: float = 95,
        replace_pand_above_perc: float = 99,
    ) -> gpd.GeoDataFrame:
        """Determine which cells should have their waterlevel replaced by their neighbours.

        Parameters
        ----------
        grid_gdf : gpd.GeoDataFrame
            GeoDataFrame with grid cells.
        replace_dem_below_perc : float, optional
            Percentage of minimal DEM area in waterlevel, if less replaced, by default 50.
        replace_water_above_perc : float, optional
            Percentage of water area above which the waterlevel should be replaced, by default 95.
        replace_pand_above_perc : float, optional
            Percentage of panden area above which the waterlevel should be replaced, by default 99.

        Returns
        -------
        gpd.GeoDataFrame
            extened grid_gdf with correction parameters columns.
        """
        grid_gdf["dem_minimal_m"] = self.grid.cells.subset("2D_open_water").z_coordinate  # FIXME bottom level?
        # Percentage of dem in a calculation cell
        # so we can make a selection of cells on model edge that need to be ignored
        grid_gdf["dem_area"] = self.grid.cells.subset("2D_open_water").sumax
        # Percentage dem in calculation cell
        grid_gdf["dem_perc"] = grid_gdf["dem_area"] / grid_gdf.area * 100

        grid_gdf[["water_area", "water_perc"]] = self._calculate_layer_area_per_cell(
            grid_gdf=grid_gdf, layer_path=self.waterdeel_path, layer_name=self.waterdeel_layer
        )
        grid_gdf[["pand_area", "pand_perc"]] = self._calculate_layer_area_per_cell(
            grid_gdf=grid_gdf, layer_path=self.panden_path, layer_name=self.panden_layer
        )

        # Select cells that need replacing of wlvl
        grid_gdf["replace_dem"] = grid_gdf["dem_perc"] < replace_dem_below_perc
        grid_gdf["replace_water"] = grid_gdf["water_perc"] > replace_water_above_perc
        grid_gdf["replace_pand"] = grid_gdf["pand_perc"] > replace_pand_above_perc

        # Write reason of replacing
        grid_gdf["replace_all"] = False
        grid_gdf.loc[grid_gdf["replace_dem"], "replace_all"] = "Dem percentage below threshold"
        grid_gdf.loc[grid_gdf["replace_water"], "replace_all"] = "Water percentage above threshold"
        grid_gdf.loc[grid_gdf["replace_pand"], "replace_all"] = "Pand percentage above threshold"

        # Find neighbour cells and add their id's to a new column
        neighbours = []
        for row in grid_gdf.itertuples():
            # find all indices that touch the cell
            neighbours_ids = grid_gdf[grid_gdf.geometry.touches(row.geometry)].id.tolist()
            # find the id of those indices
            neighbours.append(str(neighbours_ids))
        grid_gdf["neighbour_ids"] = neighbours

        return grid_gdf


def correct_water_levels(self, grid_gdf: gpd.GeoDataFrame, timesteps_seconds_output: list):
    """Correct the water level for the given time steps. Results are only corrected
    for cells where the 'replace_all' value is not False.
    """
    # Create copy and set_index the id field so we can use the neighbours_ids column easily
    grid_gdf_local = grid_gdf.copy()
    grid_gdf_local.set_index("id", inplace=True)

    for timestep in timesteps_seconds_output:
        base_col = self._create_column_base(time_seconds=timestep)
        wlvl_col = f"wlvl_{base_col}"
        wlvl_corr_col = f"wlvl_corr_{base_col}"
        diff_col = f"diff_{base_col}"
        col_idx = column_index.ColumnIdx(gdf=grid_gdf_local)

        # Make copy of original wlvls and set to None when they need to be replaced
        grid_gdf_local.insert(col_idx.wlvl_corr, wlvl_corr_col, grid_gdf_local[wlvl_col])
        replace_idx = grid_gdf_local["replace_all"] != False  # noqa: E712
        grid_gdf_local.loc[replace_idx, wlvl_corr_col] = None

        # Loop cells that need replacing.
        for row in grid_gdf_local.loc[replace_idx].itertuples():
            # Dont replace nan values
            if pd.isna(grid_gdf_local.loc[row.Index, wlvl_col]):
                continue

            # Calculate avg wlvl of neighbours and update in table
            neighbour_ids = [int(i) for i in row.neighbour_ids[1:-1].split(",")]  # str list to list
            neighbour_avg_wlvl = np.round(grid_gdf_local.loc[neighbour_ids][wlvl_corr_col].mean(), 5)
            grid_gdf_local.loc[row.Index, wlvl_corr_col] = neighbour_avg_wlvl

        # Add diff col between corrected and original wlvl
        grid_gdf_local.insert(
            col_idx.diff, diff_col, np.round(grid_gdf_local[wlvl_corr_col] - grid_gdf_local[wlvl_col], 5)
        )
    return grid_gdf_local


def run(  # TODO from essentials, create custom run here
    self,
    ness_fp: Path = None,
    output_file: Path = None,
    user_defined_timesteps: list[int] = None,
    simulation_type: str = None,
    replace_dem_below_perc: float = 50,
    replace_water_above_perc: float = 95,
    replace_pand_above_perc: float = 99,
    wlvl_correction: bool = True,
    overwrite: bool = False,
):
    """Transform netcdf into a grid gpkg.

    Parameters
    ----------
    ness_fp : str, optional
        Path to netcdf_essentials.csv file with relevant attributes.
        If None, the default file will be used.
        default is: hhnk_threedi_tools.data.netcdf_essentials.csv
    output_file : str or hrt.File, optional
        Path to output file where the grid will be saved as a GeoPackage.
        When None is passed the output will be placed in the same directory as the netcdf.
        default name is: grid_wlvl.gpkg
    user_defined_timesteps : list[int], optional
        List of output timesteps in seconds or "max" to include maximum waterlevel over calculation.
        If None, only uses max.
    simulation_type : str, optional
        Calculation type to specify retrieval of which results,
        options are 0d1d_test, 1d2d_test, klimaattoets, breach.
        Pass None to retrieve default set specified in resources/netcdf_essentials
        and user specified timesteps.
    replace_dem_below_perc : float, optional, by default 50
        if cell area has no dem (isna) above this value waterlevels will be replaced
    replace_water_above_perc : float, optional, by default 95
        if cell has water surface area above this value waterlevels will be replaced
    replace_pand_above_perc : float, optional, by default 99
        if cell has pand surface area above this value waterlevels will be replaced
    wlvl_correction : bool, optional, by default True
        applies waterlevel correction when true.
    overwrite : bool, optional, by default False
        overwrite output if it exists
    """

    if output_file is None:
        output_file = self.output_default
    output_file = hrt.SpatialDatabase(output_file)

    # initialize logger
    logger = hrt.logging.get_logger(
        __name__, filepath=Path(output_file.path).parent.absolute() / "NetCDFEssentials.log"
    )
    logger.setLevel(logging.INFO)
    logger.info(f"Starting NetcdfEssentials with result: {self.threedi_result}")

    create = hrt.check_create_new_file(output_file=output_file, overwrite=overwrite)
    if create:
        timesteps_seconds_output = self.get_output_timesteps(simulation_type, user_defined_timesteps)

        ness = self.load_ness(ness_fp=ness_fp)
        ness = self.process_nc_from_ness(ness=ness)

        grid_gdf, node_gdf, line_gdf, meta_gdf = self.create_base_gdf()

        grid_gdf = self.append_data(ness=ness, gdf=grid_gdf, timesteps_seconds_output=timesteps_seconds_output)
        node_gdf = self.append_data(ness=ness, gdf=node_gdf, timesteps_seconds_output=timesteps_seconds_output)
        line_gdf = self.append_data(ness=ness, gdf=line_gdf, timesteps_seconds_output=timesteps_seconds_output)

        if wlvl_correction:
            logger.info("Correcting 2D waterlevels")
            grid_gdf = self.add_correction_parameters(
                grid_gdf=grid_gdf,
                replace_dem_below_perc=replace_dem_below_perc,
                replace_water_above_perc=replace_water_above_perc,
                replace_pand_above_perc=replace_pand_above_perc,
            )
            grid_gdf = self.correct_waterlevels(grid_gdf=grid_gdf, timesteps_seconds_output=timesteps_seconds_output)

        # Save to file
        grid_gdf.to_file(output_file.path, layer="grid_2d", engine="pyogrio", overwrite=overwrite)
        node_gdf.to_file(output_file.path, layer="node_1d", engine="pyogrio", overwrite=overwrite)
        line_gdf.to_file(output_file.path, layer="line_1d", engine="pyogrio", overwrite=overwrite)
        meta_gdf.to_file(
            output_file.path,
            layer="metadata",
            driver="GPKG",
            overwrite=overwrite,
        )
        logger.info(f"Saved NetCDF essentials to {output_file.path}")
    else:
        logger.warning(f"Output file {output_file.path} already exists. Set overwrite to True to overwrite.")
