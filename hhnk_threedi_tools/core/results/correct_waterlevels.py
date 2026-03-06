# %%
from dataclasses import dataclass
from pathlib import Path
from typing import Union

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd

from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.results import column_index

logger = hrt.logging.get_logger(__name__)


class CorrectWaterLevels:
    """
    Correct water levels in 2D grid cells based on the percentage of DEM, water, and panden area in
    the cell. Cells that have a high percentage of DEM area or a low percentage of water or panden
    area are likely to have incorrect water levels and will be corrected by averaging the water
    levels of neighboring cells.

    Parameters
    ----------
    result_gpkg : Path
        Path to GeoPackage with 2D grid cells containing water levels in layer grid_2d and
        optionally grid_2d_agg.
    wlvl_prefix : str, optional
        Prefix of the water level columns in the grid_gdf, by default "wlvl".
    waterdeel_path : str, optional
        Path to waterdeel layer. If None, not used for cell selection.
    waterdeel_layer : str, optional
        Layer name for waterdeel if part of a GeoPackage.
    panden_path : str, optional
        Path to panden layer. If None, not used for cell selection.
    panden_layer : str, optional
        Layer name for panden if part of a GeoPackage.
    """

    def __init__(
        self,
        result_gpkg: Path = None,
        wlvl_prefix: str = "wlvl",
        waterdeel_path: str = None,
        waterdeel_layer: str = None,
        panden_path: str = None,
        panden_layer: str = None,
    ):
        self.result_gpkg = result_gpkg
        self.wlvl_prefix = wlvl_prefix
        self.waterdeel_path = waterdeel_path
        self.waterdeel_layer = waterdeel_layer
        self.panden_path = panden_path
        self.panden_layer = panden_layer

    @classmethod
    def from_folder(cls, folder: Folders, result_gpkg: Path):
        """Initialize from folder structure."""

        return cls(result_gpkg=result_gpkg)

    def _calculate_layer_area_per_cell(
        self,
        grid_gdf: gpd.GeoDataFrame = None,
        layer_path: Union[Path, hrt.File] = None,  # TODO add default location result gpkg
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
        if layer_path is not None and layer_path.exists():
            gdf = gpd.read_file(str(layer_path), layer=layer_name)
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
        """Determine which cells should have their water level replaced by their neighbors.

        Parameters
        ----------
        grid_gdf : gpd.GeoDataFrame
            GeoDataFrame with grid cells.
        replace_dem_below_perc : float, optional
            Percentage of minimal DEM area in water level, if less replaced, by default 50.
        replace_water_above_perc : float, optional
            Percentage of water area above which the water level should be replaced, by default 95.
        replace_pand_above_perc : float, optional
            Percentage of panden area above which the water level should be replaced, by default 99.

        Returns
        -------
        gpd.GeoDataFrame
            Extended grid_gdf with correction parameters columns.
        """
        # Percentage of dem in a calculation cell
        # so we can make a selection of cells on model edge that need to be ignored
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

        # Find neighbor cells and add their id's to a new column
        neighbors = []
        for row in grid_gdf.itertuples():
            # find all indices that touch the cell
            neighbors_ids = grid_gdf[grid_gdf.geometry.touches(row.geometry)].id.tolist()
            # find the id of those indices
            neighbors.append(str(neighbors_ids))
        grid_gdf["neighbor_ids"] = neighbors

        return grid_gdf

    def correct_water_levels(self, grid_gdf: gpd.GeoDataFrame, wlvl_prefix: str = "wlvl") -> gpd.GeoDataFrame:
        """Correct the water level for the given time steps. Results are only corrected
        for cells where the 'replace_all' value is not False.

        Parameters
        ----------
        grid_gdf : gpd.GeoDataFrame
            GeoDataFrame with grid cells.
        wlvl_prefix : str
            Time step prefix to correct water levels for.

        Returns
        -------
        gpd.GeoDataFrame
            Extended grid_gdf with corrected water levels.
        """

        # Create copy and set_index the id field so we can use the neighbors_ids column easily
        grid_gdf_local = grid_gdf.copy()
        grid_gdf_local.set_index("id", inplace=True)

        # List of columns with water levels to correct
        wlvl_cols = [col for col in grid_gdf_local.columns if col.startswith(wlvl_prefix)]

        for wlvl_col in wlvl_cols:
            label = str(wlvl_col).split(f"{wlvl_prefix}_")[-1]
            wlvl_corr_col = f"{wlvl_prefix}_corr_{label}"
            diff_col = f"diff_{label}"
            col_idx = column_index.ColumnIdx(gdf=grid_gdf_local)

            # Make copy of original wlvls and set to None when they need to be replaced
            grid_gdf_local.insert(col_idx.wlvl_corr, wlvl_corr_col, grid_gdf_local[wlvl_col])
            replace_idx = grid_gdf_local["replace_all"] != False  # noqa: E712
            grid_gdf_local.loc[replace_idx, wlvl_corr_col] = None

            # Loop cells that need replacing.
            for row in grid_gdf_local.loc[replace_idx].itertuples():
                # Don't replace nan values
                if pd.isna(grid_gdf_local.loc[row.Index, wlvl_col]):
                    continue

                # Calculate avg wlvl of neighbors and update in table
                neighbor_ids = [int(i) for i in row.neighbor_ids[1:-1].split(",")]  # str list to list
                neighbor_avg_wlvl = np.round(grid_gdf_local.loc[neighbor_ids][wlvl_corr_col].mean(), 5)
                grid_gdf_local.loc[row.Index, wlvl_corr_col] = neighbor_avg_wlvl

            # Add diff col between corrected and original wlvl
            grid_gdf_local.insert(
                col_idx.diff, diff_col, np.round(grid_gdf_local[wlvl_corr_col] - grid_gdf_local[wlvl_col], 5)
            )
        return grid_gdf_local

    def run(
        self,
        result_gpkg: Path = None,
        wlvl_prefix: str = "wlvl",
        replace_dem_below_perc: float = 50,
        replace_water_above_perc: float = 95,
        replace_pand_above_perc: float = 99,
    ):
        """
        Run the waterlevel correction process. This includes calculating the area percentages,
        determining which cells need correction, and applying the correction by averaging the water
        levels of neighboring cells.

        Parameters
        ----------
        result_gpkg : Path, optional
            Path to the result GeoPackage to read the grid_gdf from and save the corrected grid_gdf
            to. If None, uses default output path.
        wlvl_prefix : str, optional
            Prefix of the water level columns in the grid_gdf, by default "wlvl".
        replace_dem_below_perc : float, optional
            Percentage of minimal DEM area in water level, if less replaced, by default 50.
        replace_water_above_perc : float, optional
            Percentage of water area above which the water level should be replaced, by default 95.
        replace_pand_above_perc : float, optional
            Percentage of panden area above which the water level should be replaced, by default 99.

        """

        if result_gpkg is None:
            # TODO add default location result gpkg
            result_gpkg = None
        result_gpkg = hrt.SpatialDatabase(result_gpkg)
        layers = [layer for layer in result_gpkg.available_layers() if "grid" in layer]

        # initialize logger
        # logger = hrt.logging.get_logger(
        #     __name__, filepath=Path(result_gpkg.path).parent.absolute() / "NetCDFEssentials.log"
        # )

        for layer in layers:
            # Load grid gdf
            grid_gdf = gpd.read_file(result_gpkg.path, layer=layer)

            # Check if there is a wlvl column in grid_gdf
            wlvl_cols = [col for col in grid_gdf.columns if col.startswith(wlvl_prefix)]
            if len(wlvl_cols) > 0:
                logger.info(f"Correcting 2D water levels for layer {layer}")
                grid_gdf = self._add_correction_parameters(
                    grid_gdf=grid_gdf,
                    replace_dem_below_perc=replace_dem_below_perc,
                    replace_water_above_perc=replace_water_above_perc,
                    replace_pand_above_perc=replace_pand_above_perc,
                )
                grid_gdf = self.correct_water_levels(grid_gdf=grid_gdf, wlvl_prefix=wlvl_prefix)
            else:
                logger.info(f"No wlvl columns found in layer {layer}, skipping correction.")

            # Save to file
            grid_gdf.to_file(result_gpkg.path, layer=layer, engine="pyogrio", overwrite=True)
            logger.info(f"Saved corrected water levels to {result_gpkg.path}")


# %%
if __name__ == "__main__":
    from hhnk_threedi_tools import Folders

    folder_path = r"tests\data\model_test"
    folder = Folders(folder_path)
    result_gpkg = folder.threedi_results.batch["batch_test"].downloads.piek_glg_T10.netcdf.full_path(
        "threedi_result.gpkg"
    )
    self = CorrectWaterLevels.from_folder(folder=folder, result_gpkg=result_gpkg)

    self.wlvl_prefix = "wlvl"
    self.waterdeel_path = folder.source_data.damo.path
    self.waterdeel_layer = folder.source_data.damo.layers.waterdeel.name
    panden_path = None
    panden_layer = None

    self.run(result_gpkg=result_gpkg, wlvl_prefix=self.wlvl_prefix)
    # TODO add corr columns into correct index column
    # TODO figure out wehre to define parameters

# %%
