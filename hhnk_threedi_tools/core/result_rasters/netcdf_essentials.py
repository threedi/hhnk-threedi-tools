# %%
from dataclasses import dataclass
from pathlib import Path
from typing import Union

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
from shapely.geometry import LineString, Point, box

from hhnk_threedi_tools.core.folders import Folders


@dataclass
class ColumnIdx:
    """Find index of columns in dataframe so we can insert new column
    at the correct location. This way we can group column types.
    e.g.
    wlvl_1h wlvl_3h wlvl_1h_corr wlvl_3h_corr diff_1h diff_15h
    TODO uitbreiden met extra elementen
    """

    gdf: gpd.GeoDataFrame

    def _get_idx(self, search_str) -> int:
        """Get idx based on search pattern, if not found return last index"""
        idxs = self.gdf.columns.get_indexer(
            self.gdf.columns[self.gdf.columns.str.contains(search_str, na=False)]
        ).tolist()
        return (idxs or [len(self.gdf.columns) - 1])[-1] + 1

    @property
    def wlvl(self):
        return self._get_idx(search_str="^wlvl_(?!.*corr).*")

    @property
    def wlvl_corr(self):
        return self._get_idx(search_str="^wlvl_corr_.*")

    @property
    def diff(self):
        return self._get_idx(search_str="^diff_.*")

    @property
    def vol(self):
        return self._get_idx(search_str="^vol_.*")

    @property
    def infilt(self):
        return self._get_idx(search_str="^infilt_.*")

    @property
    def incept(self):
        return self._get_idx(search_str="^incept_.*")

    @property
    def rain(self):
        return self._get_idx(search_str="^rain_.*")

    @property
    def q(self):
        return self._get_idx(search_str="^discharge_.*")

    @property
    def u1(self):
        return self._get_idx(search_str="^vel_.*")

    @property
    def storage(self):
        return self._get_idx(search_str="^storage_mm_.*")


@dataclass
class NetcdfEssentials:
    """
    Class te retrieve essential data from NetCDf and store into Geopackage

    Replaces classes NedcdfToGPKG and NetcdfTimeSeries.

    Transform netcdf into a gpkg. Can also correct waterlevels
    based on conditions. These are passed when running the function
    .run, to turn this behaviour off, use wlvl_correction=False there.

    Input layers can be passed directly, or when using the htt.Folders
    structure, use the .from_folder classmethod.

    Parameters
    ----------
    threedi_result : hrt.ThreediResult
        path to folder with netcdf and h5 result.
    waterdeel_path : str, optional
        path to waterdeel. If None is passed it wont be used in the selection of cells
    waterdeel_layer : str, optional
        layername if waterdeel is part of a gpkg
    panden_path : str, optional
        path to panden. If None is passed it wont be used in the selection of cells
    panden_layer : str, optional
        layername if panden is part of a gpkg
    """

    threedi_result: hrt.ThreediResult
    waterdeel_path: str = None
    waterdeel_layer: str = None
    panden_path: str = None
    panden_layer: str = None
    user_defined_timesteps: list[int] = (
        None  # TODO ik ga max altijd opslaan bij q en u moet nog iets met abs en een richting
    )
    ness_fp: str = None
    use_aggregate: bool = False  # NOTE dus ik moet als gebruiker aangeven of ik aggregate result gebruik

    def __post_init__(self):
        self._ts = None

        # Check if result is aggregate # TODO add somehow check that it contains attributes
        self.aggregate: bool = self.typecheck_aggregate

        # Check if result has breaches
        self.breaches: bool = self.grid.has_breaches

    @classmethod
    def from_folder(cls, folder: Folders, threedi_result: hrt.ThreediResult, use_aggregate: bool = False, **kwargs):
        """Initialize from folder structure."""
        waterdeel_path = folder.source_data.damo
        waterdeel_layer = "Waterdeel"

        panden_path = folder.source_data.panden
        panden_layer = "panden"

        return cls(
            threedi_result=threedi_result,
            waterdeel_path=waterdeel_path,
            waterdeel_layer=waterdeel_layer,
            panden_path=panden_path,
            panden_layer=panden_layer,
            use_aggregate=use_aggregate,
        )

    @property
    def ness(self):
        """Relevant data for HHNK models"""
        return self.ness

    @property
    def grid(self):
        """Instance of threedigrid.admin.gridresultadmin.GridH5ResultAdmin or GridH5AggregateResultAdmin"""
        if self.use_aggregate is False:
            return self.threedi_result.grid
        return self.threedi_result.aggregate_grid

    @property
    def timestamps(self):
        """Retrieve timestamps for timeseries"""
        return self.grid.nodes.timestamps

    @property
    def output_default(self):
        """Default output if no path is specified."""
        return self.threedi_result.full_path("grid_wlvl.gpkg")  # TODO

    @property
    def typecheck_aggregate(self) -> bool:
        """Check if we have a normal or aggregated netcdf"""
        return str(type(self.grid)) == "<class 'threedigrid.admin.gridresultadmin.GridH5AggregateResultAdmin'>"

    def load_default_ness(self, ness_fp=None):
        """Relevant data for HHNK models"""
        if ness_fp is None:
            # ness = pd.read_csv("./netcdf_essentials.csv")
            ness = pd.read_csv(
                r"E:\github\wvanesse\hhnk-threedi-tools\hhnk_threedi_tools\core\result_rasters\netcdf_essentials.csv"
            )  # TODO hoe ga ik dit meegeven? Bij input? Of default ergens in de module?
        else:
            ness = pd.read_csv(ness_fp)
        return ness

    def _create_column_base(self, time_seconds):
        """Return a base column name with hours and minutes."""
        # time_seconds = self.timestamps[np.argmin(np.abs(self.timestamps - time_seconds))]
        if time_seconds == "max":
            col_base = time_seconds
        else:
            timestep_h = time_seconds / 3600

            if timestep_h % 1 == 0:  # round hours
                col_base = f"{int(timestep_h)}h"
            else:
                if timestep_h < 1:
                    col_base = f"{int(timestep_h * 60)}min"
                else:
                    col_base = f"{int(np.floor(timestep_h))}h{int((timestep_h % 1) * 60)}min"
        return col_base

    def _calculate_layer_area_per_cell(
        self,
        grid_gdf: gpd.GeoDataFrame,
        layer_path: Union[Path, hrt.File],
        layer_name: str = None,
    ) -> gpd.GeoDataFrame:
        """Calculate for each gridcel the area and percentage of total area of the
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
        if (layer_path is not None) and (layer_path.exists()):
            gdf = gpd.read_file(str(layer_path), layer=layer_name)
        else:
            print(f"Couldn't load {layer_path.name}. Ignoring it in correction.")

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

    def _set_active_attributes(self, ness):
        """Set active attributes in ness dataframe"""
        ness["active"] = True
        if not self.grid.has_1d:
            ness.loc[(ness["subset"] == "1D_All"), "active"] = False
        if not self.grid.has_2d:
            ness.loc[(ness["subset"] == "2D_OPEN_WATER"), "active"] = False
        if not self.grid.has_interception:
            ness.loc[(ness["attribute"] == "intercepted_volume"), "active"] = False
        if not self.grid.has_max_infiltration_capacity:
            ness.loc[(ness["attribute"] == "infiltration_rate_simple"), "active"] = False
        return ness

    def _get_ts(self, ness):
        """Retrieve timeseries for rows in ness dataframe"""
        # initialise data column
        ness["count"] = None
        ness["data"] = None

        for i, row in ness.iterrows():
            if row["active"]:
                data = getattr(
                    getattr(self.grid, row["element"])
                    .subset(row["subset"])
                    .timeseries(indexes=slice(0, len(self.timestamps))),
                    row["attribute"],
                ).T
                # Replace -9999 with nan values to prevent -9999 being used in replacing values.
                data[data == -9999.0] = np.nan
                # add data as nested array to dataframe
                ness.at[i, "data"] = data  # noqa: PD008 .loc werkt niet met nested array
                ness.loc[i, "count"] = data.shape[0]

        return ness

    def _get_ts_index(self, time_seconds: int):
        """Retrieve indices of requested output time_seconds"""
        abs_diff = np.abs(self.timestamps - time_seconds)
        # geeft 1 index voor de gevraagde timestep gelijk voor alle elementen
        ts_indx = np.argmin(abs_diff)
        if np.min(abs_diff) > 30:  # seconds diff. # TODO gebruik hier de helft van de opgegeven output time_seconds
            raise ValueError(
                f"""Provided time_seconds {time_seconds} not found in netcdf timeseries.
                    Closest timestep is {self.timestamps[ts_indx]} seconds at index {ts_indx}. \
                    Debug by checking available timeseries through the (.timestamps) timeseries attributes"""
            )

        return ts_indx

    def _attronly_schema(self, df):
        """
        Needed to save attributes without geometry
        see: https://gis.stackexchange.com/questions/396752
        """

        def remap(dtype):
            correction = {
                "int64": "int",
                "int32": "int",
                "float32": "float",
                "float64": "float",
                "object": "str",
            }
            return correction[dtype] if dtype in correction else dtype

        return {
            "geometry": "None",
            "properties": {
                column: remap(str(dtype)) for column, dtype in zip(df.columns, df.dtypes) if column != "geometry"
            },
        }

    def process_ness(self, ness):
        """Process ness dataframe to retrieve timeseries and indices"""
        ness = self._set_active_attributes(ness)
        ness = self._get_ts(ness)
        return ness

    def create_base_gdf(self):  # NOTE WE layername wordt belangrijk voor raster creatie
        """Create base grid from netcdf"""

        grid_gdf = gpd.GeoDataFrame()
        node_gdf = gpd.GeoDataFrame()
        line_gdf = gpd.GeoDataFrame()

        # =========================
        # GRID
        # =========================
        if self.grid.has_2d:
            # * inputs every element from row as a new function argument, creating a (square) box.
            grid_gdf.set_geometry(
                [box(*row) for row in self.grid.nodes.subset("2D_OPEN_WATER").cell_coords.T],
                crs=f"EPSG:{self.grid.epsg_code}",
                inplace=True,
            )

            # Add relevant metadata
            grid_gdf["id"] = self.grid.cells.subset("2D_OPEN_WATER").id
            grid_gdf["calculation_type"] = self.grid.cells.subset("2D_OPEN_WATER").calculation_type
            grid_gdf["dmax_bottom_level"] = self.grid.cells.subset("2D_OPEN_WATER").dmax

            grid_gdf["dem_minimal_m"] = self.grid.cells.subset(
                "2D_OPEN_WATER"
            ).z_coordinate  # TODO in de testdata is dit alleen nan, misschien dmax? Dit zou bottom level zijn?

            grid_gdf["dem_area"] = self.grid.cells.subset("2D_OPEN_WATER").sumax

        if self.grid.has_1d:
            # =========================
            # NODES
            # =========================
            # Read 1d node coordinates
            coords_1d = self.grid.nodes.subset("1D_All").coordinates

            # Create a list of Shapely Point objects
            xy = []
            for i in range(0, len(coords_1d[0])):
                xy.append([coords_1d[0, i], coords_1d[1, i]])
            points = [Point(j) for j in xy]

            # Voeg geometry toe aan node gdf
            node_gdf.set_geometry(
                points,
                crs=f"EPSG:{self.grid.epsg_code}",
                inplace=True,
            )

            # Add relevant metadata
            node_gdf["id"] = self.grid.nodes.subset("1D_All").id
            node_gdf["connection_node_id"] = self.grid.nodes.subset("1D_All").content_pk
            node_gdf["initial_waterlevel"] = self.grid.nodes.subset("1D_All").initial_waterlevel
            node_gdf["storage_area"] = self.grid.nodes.subset("1D_All").storage_area
            node_gdf["drain_level"] = self.grid.nodes.subset("1D_All").drain_level
            node_gdf["zoom_category"] = self.grid.nodes.subset("1D_All").zoom_category
            node_gdf["calculation_type"] = self.grid.nodes.subset("1D_All").calculation_type

            # =========================
            # LINES 1D_All
            # =========================

            # Voeg geometry toe aan line gdf
            xys = self.grid.lines.subset("1D_All").line_geometries  # format [x1, x2, ..., y1, y2, ...]

            # Shapely requires [[x1,y1],[x2,y2],...]
            lines = []
            for xxyy in xys:
                n = int(len(xxyy) / 2)
                xy = []
                for i in range(0, n):
                    xy.append([xxyy[i], xxyy[i + n]])
                lines.append(LineString(xy))

            # Add line geometry to gdf
            line_gdf.set_geometry(
                lines,
                crs=f"EPSG:{self.grid.epsg_code}",
                inplace=True,
            )

            # Add relevant metadata
            line_gdf["id"] = self.grid.lines.subset("1D_All").id
            line_gdf["channel_id"] = self.grid.lines.subset("1D_All").content_pk
            line_gdf["exchange_level"] = self.grid.lines.subset("1D_All").dpumax
            line_gdf["line_type_kcu"] = self.grid.lines.subset("1D_All").kcu
            line_gdf["start_node"] = self.grid.lines.subset("1D_All").line[0]
            line_gdf["end_node"] = self.grid.lines.subset("1D_All").line[1]  # TODO check start and end node
            line_gdf["zoom_category"] = self.grid.lines.subset("1D_All").zoom_category

        # =========================
        # METADATA
        # =========================
        meta_dict = {
            "model_slug": self.grid.model_slug,
            "model_name": self.grid.model_name,
            "revision_hash": self.grid.revision_hash,
            "revision_nr": self.grid.revision_nr,
            "has_0d": self.grid.has_0d,
            "has_1d": self.grid.has_1d,
            "has_2d": self.grid.has_2d,
            "has_breaches": self.grid.has_interception,
            "has_max_infiltration_capacity": self.grid.has_max_infiltration_capacity,
            "has_simple_infiltration": self.grid.has_simple_infiltration,
            "threedicore_result_version": str(self.grid.threedicore_result_version),
            "epsg_code": self.grid.nodes.epsg_code,
        }
        meta_gdf = gpd.GeoDataFrame(meta_dict, index=[0])  # zodat ik hem weg kan schrijven naar geopackage
        meta_gdf["geometry"] = None
        meta_gdf.set_geometry("geometry", inplace=True)

        # TODO breaches?
        return grid_gdf, node_gdf, line_gdf, meta_gdf

    def add_correction_parameters(
        self,
        grid_gdf: gpd.GeoDataFrame,
        replace_dem_below_perc: float = 50,
        replace_water_above_perc: float = 95,
        replace_pand_above_perc: float = 99,
    ) -> gpd.GeoDataFrame:
        """Determine which cells should have their waterlevel replaced by their neighbours.

        Returns
        -------
        gpd.GeoDataFrame
            extened grid_gdf with correction parameters columns.
        """
        # Percentage of dem in a calculation cell
        # so we can make a selection of cells on model edge that need to be ignored
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

    def get_output_timesteps(self, user_defined_timesteps: list[int]):
        """
        Stel de output timesteps goed in

        In ieder geval:
        * Opgegeven timesteps door gebruiker, incl. max
        * Timesteps nodig voor tests 0d1d en 1d2d
        * Misschien gewoon alle? Maar dan wel iets uniforms?

        TODO
        NOTE hoe kom ik aan output timestep uit de settings?
        NOTE misschien optie maken voor alle tijstappen?
        NOTE de ness als input?
        """
        timesteps_seconds_output = user_defined_timesteps  # voorlopig even zo

        return timesteps_seconds_output

    def append_data(self, ness, gdf, timesteps_seconds_output: list):
        """Insert data at given timesteps to geodataframe."""

        col_idx = ColumnIdx(gdf=gdf)

        for i, row in ness.iterrows():
            # break
            if row["active"] and row["geom_type"] == gdf.geometry.geom_type.unique()[0]:
                for key in timesteps_seconds_output:
                    # break
                    # TODO min negatieve max etc
                    if key == "max":  # isinstance(time_seconds, str):
                        gdf.insert(
                            getattr(col_idx, row["attribute_name"]),
                            f"{row['attribute_name']}_{key}",
                            np.max(abs(row["data"]), axis=1),
                        )
                    elif isinstance(key, int):
                        # Make pretty column names
                        col_sub = self._create_column_base(time_seconds=key)
                        # Find index of timestep
                        data_timestep = row["data"][:, self._get_ts_index(time_seconds=key)]
                        gdf.insert(
                            getattr(col_idx, row["attribute_name"]),
                            f"{row['attribute_name']}_{col_sub}",
                            data_timestep,
                        )

        return gdf

    def correct_waterlevels(self, grid_gdf, timesteps_seconds_output: list):
        """Correct the waterlevel for the given timesteps. Results are only corrected
        for cells where the 'replace_all' value is not False.
        """
        # Create copy and set_index the id field so we can use the neighbours_ids column easily
        grid_gdf_local = grid_gdf.copy()
        grid_gdf_local.set_index("id", inplace=True)

        # Also correct max
        timesteps_seconds_output.append("max")

        for timestep in timesteps_seconds_output:
            base_col = self._create_column_base(time_seconds=timestep)
            wlvl_col = f"wlvl_{base_col}"
            wlvl_corr_col = f"wlvl_corr_{base_col}"
            diff_col = f"diff_{base_col}"
            col_idx = ColumnIdx(gdf=grid_gdf_local)

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

    def run(
        self,
        ness_fp=None,
        output_file=None,
        user_defined_timesteps: list[int] = None,
        replace_dem_below_perc: float = 50,
        replace_water_above_perc: float = 95,
        replace_pand_above_perc: float = 99,
        wlvl_correction: bool = True,
        overwrite: bool = False,
    ):
        """Transform netcdf into a grid gpkg.

        Parameters
        ----------
        output_file, by default None
            When None is passed the output will be placed in the same directory as the netcdf.
            default name is: grid_wlvl.gpkg
        timesteps_seconds, by default ["max"]
            time in seconds since start of calculation. Will create cols for each item in list.
            options:
                int value - seconds since start
                "max" - maximum wlvl over calculation
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
        # TODO bijwerken
        """

        if output_file is None:
            output_file = self.output_default
        output_file = hrt.FileGDB(output_file)  # TODO FileGDB??

        create = hrt.check_create_new_file(output_file=output_file, overwrite=overwrite)
        if create:
            timesteps_seconds_output = self.get_output_timesteps(user_defined_timesteps)

            ness = self.load_default_ness(ness_fp=ness_fp)
            ness = self.process_ness(ness=ness)

            grid_gdf, node_gdf, line_gdf, meta_gdf = self.create_base_gdf()

            if wlvl_correction:
                grid_gdf = self.add_correction_parameters(
                    grid_gdf=grid_gdf,
                    replace_dem_below_perc=replace_dem_below_perc,
                    replace_water_above_perc=replace_water_above_perc,
                    replace_pand_above_perc=replace_pand_above_perc,
                )

            grid_gdf = self.append_data(ness=ness, gdf=grid_gdf, timesteps_seconds_output=timesteps_seconds_output)
            node_gdf = self.append_data(ness=ness, gdf=node_gdf, timesteps_seconds_output=timesteps_seconds_output)
            node_gdf = self.append_data(ness=ness, gdf=node_gdf, timesteps_seconds_output=timesteps_seconds_output)

            # Save to file
            grid_gdf.to_file(output_file.path, layer="grid_2d", engine="pyogrio", overwrite=overwrite)
            node_gdf.to_file(output_file.path, layer="node_1d", engine="pyogrio", overwrite=overwrite)
            line_gdf.to_file(output_file.path, layer="line_1d", engine="pyogrio", overwrite=overwrite)
            meta_gdf.to_file(
                output_file.path,
                layer="metadata",
                driver="GPKG",
                schema=self._attronly_schema(meta_gdf),
                overwrite=overwrite,
            )
        else:
            print("Output file already exists. Set overwrite to True to overwrite.")


# %% Working code example small model
if __name__ == "__main__":
    from hhnk_threedi_tools import Folders

    folder_path = r"../tests\data\model_test"
    folder = Folders(folder_path)

    user_defined_timesteps = [3600, 5400]
    output_file = None
    wlvl_correction = True
    overwrite = True
    self = NetcdfEssentials.from_folder(
        folder=folder, threedi_result=folder.threedi_results.batch["batch_test"].downloads.piek_glg_T10.netcdf
    )

    self.run(
        output_file=output_file,
        user_defined_timesteps=user_defined_timesteps,
        wlvl_correction=wlvl_correction,
        overwrite=overwrite,
    )

# %% Performance test (large model including sewerage)
if __name__ == "__main__":
    from hhnk_threedi_tools import Folders

    folder_path = r"E:\02.modellen\BWN_Castricum_Integraal_10m"
    folder = Folders(folder_path)
    threedi_result = folder.threedi_results.batch["rev1"].downloads.piek_ghg_T1000

    user_defined_timesteps = [3600, 5400]
    output_file = r"E:\02.modellen\BWN_Castricum_Integraal_10m\test_netcdfessentials_piek_ghg_T1000.gpkg"
    wlvl_correction = False
    overwrite = True
    self = NetcdfEssentials(threedi_result=threedi_result.netcdf, use_aggregate=False)
    self.run(
        output_file=output_file,
        user_defined_timesteps=user_defined_timesteps,
        wlvl_correction=wlvl_correction,
        overwrite=overwrite,
    )
# NOTE op volle server geeft dit al memory issues, duurt nu 20 s

# %% Working code example with aggregate result TODO
if __name__ == "__main__":
    from hhnk_threedi_tools import Folders

    folder_path = r"E:\02.modellen\HKC23010_Eijerland_WP"
    folder = Folders(folder_path)

    threedi_result = folder.threedi_results.batch["bwn_gxg"].downloads.piek_ghg_T10

    # # get and correct waterlevels
    #  timesteps_seconds = ["max", 3600, 5400]
    # grid_gdf = netcdf_gpkg.get_waterlevels(grid_gdf=grid_gdf, timesteps_seconds=timesteps_seconds)
    # grid_gdf = netcdf_gpkg.correct_waterlevels(grid_gdf=grid_gdf, timesteps_seconds=timesteps_seconds)

    output_file = None
    wlvl_correction = False
    overwrite = True
    self = NetcdfEssentials(threedi_result=threedi_result.netcdf, use_aggregate=True)
    timesteps_seconds = ["max"]
    self.run(wlvl_correction=wlvl_correction)

# %% TODO test model zonder maaiveld

# %% TODO test model met bressen
