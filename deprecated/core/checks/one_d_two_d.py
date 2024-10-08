# %%
import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
from hhnk_research_tools.variables import all_2d, t_end_rain_col, t_end_sum_col, t_start_rain_col
from shapely.geometry import LineString, box

# Local imports
import hhnk_threedi_tools.core.checks.grid_result_metadata as grid_result_metadata
from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.variables.database_aliases import df_geo_col
from hhnk_threedi_tools.variables.default_variables import DEF_TRGT_CRS
from hhnk_threedi_tools.variables.one_d_two_d import (
    content_type_col,
    end_rain_sfx,
    id_col,
    kcu_col,
    max_area_col,
    max_sfx,
    minimal_dem_col,
    node_type_col,
    one_d,
    one_d_boundary_col,
    one_d_two_d,
    pump_capacity_m3_s_col,
    q_m3_s_col,
    spatialite_id_col,
    start_rain_sfx,
    storage_mm_col,
    suffixes_list,
    twelve_hr_after_rain_sfx,
    two_d,
    vel_m_s_col,
    volume_m3_col,
    wet_area_m2_col,
    wtrlvl_col,
    wtrlvl_m_col,
)


# TODO functies weer in class onderbrengen, class nu buiten gebruik.
class OneDTwoDTest:
    def __init__(self, folder: Folders, revision=0, dem_path=None):
        self.fenv = folder
        self.revision = revision

        self.result_fd = self.fenv.threedi_results.one_d_two_d[self.revision]
        self.output_fd = self.fenv.output.one_d_two_d[self.revision]

        self.grid_result = self.result_fd.grid

        (
            rain,
            detected_rain,
            timestep,
            days_dry_start,
            days_dry_end,
            self.timestep_df,
        ) = grid_result_metadata.construct_scenario(self.grid_result)

        # if output_path:
        #     self.output_path = output_path
        #     self.layer_path = output_path + "/Layers"
        #     self.log_path = output_path + "/Logs"
        # else:
        #     self.layer_path = str(folder.output.one_d_two_d[self.revision].layers)
        #     self.log_path = str(folder.output.one_d_two_d[self.revision].logs)

        if dem_path:
            self.dem = hrt.Raster(dem_path)
        else:
            self.dem = self.fenv.model.schema_base.rasters.dem

        self.iresults = {}

    @classmethod
    def from_path(cls, path_to_polder, **kwargs):
        return cls(Folders(path_to_polder), **kwargs)

    @property
    def results(self):
        return self.iresults

    def run_wlvl_depth_at_timesteps(self, overwrite=False):
        """
        Deze functie bepaalt de waterstanden op de gegeven tijdstappen op basis van het 3di resultaat.
        Vervolgens wordt op basis van de DEM en de waterstand per tijdstap de waterdiepte bepaald.
        """

        def _create_depth_raster(self, windows, band_out, **kwargs):
            """hrt.Raster_calculator custom_run_window_function"""
            self.dem = self.raster1
            self.wlvl = self.raster2

            block_dem = self.dem._read_array(window=windows["raster1"])
            block_wlvl = self.wlvl._read_array(window=windows["raster2"])

            # Calculate output
            block_depth = np.subtract(block_wlvl, block_dem)

            # Mask output
            nodatamask = (block_dem == self.dem.nodata) | (block_dem == 10) | (block_depth < 0)
            block_depth[nodatamask] = self.raster_out.nodata

            # Get the window of the small raster
            window_small = windows[[k for k, v in self.raster_mapping.items() if v == "small"][0]]

            # Write to file
            band_out.WriteArray(block_depth, xoff=window_small[0], yoff=window_small[1])

        try:
            timesteps_arr = [
                self.timestep_df["t_start_rain"].value,
                self.timestep_df["t_end_rain"].value,
                self.timestep_df["t_end_sum"].value,
            ]
            # hours since start of calculation
            timestrings = [int(round(self.grid_result.nodes.timestamps[t] / 60 / 60, 0)) for t in timesteps_arr]

            assert timestrings == [1, 3, 15]

            # For each timestring calculate wlvl and depth raster.
            for timestep, timestr in zip(timesteps_arr, timestrings):
                wlvl_raster = getattr(self.output_fd, f"waterstand_T{timestr}")
                depth_raster = getattr(self.output_fd, f"waterdiepte_T{timestr}")

                # Calculate wlvl raster
                nodes_2d_wlvl = self._read_2node_wlvl_at_timestep(timestep)
                hrt.gdf_to_raster(
                    gdf=nodes_2d_wlvl,
                    value_field=wtrlvl_col,
                    raster_out=wlvl_raster,
                    nodata=self.dem.nodata,
                    metadata=self.dem.metadata,
                    read_array=False,
                    overwrite=overwrite,
                )

                # Calculate depth raster
                depth_calculator = hrt.RasterCalculator(
                    raster1=self.dem,
                    raster2=wlvl_raster,
                    raster_out=depth_raster,
                    custom_run_window_function=_create_depth_raster,
                    output_nodata=0,
                    verbose=False,
                )

                depth_calculator.run(overwrite=overwrite)
        except Exception as e:
            raise e from None

    def _read_2node_wlvl_at_timestep(self, timestep):
        """Timesteps is the index of the time in the timeseries you want to use
        to calculate the wlvl and depth raster
        """
        nodes_2d = gpd.GeoDataFrame()
        # * inputs every element from row as a new function argument.
        nodes_2d[df_geo_col] = [box(*row) for row in self.grid_result.cells.subset(all_2d).cell_coords.T]
        # waterstand
        nodes_2d[wtrlvl_col] = self.grid_result.nodes.subset(all_2d).timeseries(indexes=[timestep]).s1[0]
        return nodes_2d

    def run_node_stats(self):
        """
        Deze functie leest alle 2d nodes uit het 3di resultaat en berekent de volgende waarden:
            * de minimale DEM waarde binnen het gebied van de betreffende node (geometrie is omgezet naar een vierkant)
            * het totale oppervlak dat de node beslaat
        Vervolgens wordt op drie tijdstappen (het begin van de regen het einde van de regen en het einde van de som
        de volgende informatie berekend:
            * de waterstand op de genoemde tijdstappen
            * de hoeveelheid water (volume in m3) per tijdstap
            * het natte oppervlak per tijdstap (in m2)
            * opslag van regen in het gebied van de node (hoeveelheid water / totale oppervlak gebied)
        """
        try:
            nodes_wlvl = hrt.threedi.grid_nodes_to_gdf(self.grid_result)
            # We need to keep a reference to the crs to restore it later
            crs_orig = nodes_wlvl.crs
            nodes_wlvl[id_col] = self.grid_result.nodes.id
            nodes_wlvl[spatialite_id_col] = self.grid_result.nodes.content_pk
            nodes_wlvl[node_type_col] = self.grid_result.nodes.node_type
            # Replace numbers with human readable values
            nodes_wlvl[node_type_col].replace([1, 3, 7], [two_d, one_d, one_d_boundary_col], inplace=True)

            # totaal oppervlak
            nodes_wlvl[max_area_col] = self.grid_result.nodes.sumax

            # Load grid_result
            # waterstand
            wlvl = self.grid_result.nodes.timeseries(
                indexes=[
                    self.timestep_df[t_start_rain_col].value,
                    self.timestep_df[t_end_rain_col].value,
                    self.timestep_df[t_end_sum_col].value,
                ]
            ).s1
            volume = self.grid_result.nodes.timeseries(
                indexes=[
                    self.timestep_df[t_start_rain_col].value,
                    self.timestep_df[t_end_rain_col].value,
                    self.timestep_df[t_end_sum_col].value,
                ]
            ).vol
            # actueel nat oppervlak
            wet_area = self.grid_result.nodes.timeseries(
                indexes=[
                    self.timestep_df[t_start_rain_col].value,
                    self.timestep_df[t_end_rain_col].value,
                    self.timestep_df[t_end_sum_col].value,
                ]
            ).su

            # Add grid_result to dataframe
            args_lst = [start_rain_sfx, end_rain_sfx, twelve_hr_after_rain_sfx]
            for index, time_str in enumerate(args_lst):
                nodes_wlvl[wtrlvl_m_col + time_str] = np.round(wlvl[index], 2)
            for index, time_str in enumerate(args_lst):
                nodes_wlvl[wet_area_m2_col + time_str] = np.round(wet_area[index], 2)
            for index, time_str in enumerate(args_lst):
                nodes_wlvl[volume_m3_col + time_str] = np.round(volume[index], 2)
            for index, time_str in enumerate(args_lst):
                nodes_wlvl[storage_mm_col + time_str] = np.round(
                    nodes_wlvl[volume_m3_col + time_str] / nodes_wlvl[max_area_col], 2
                )

            # select 2d nodes and create polygons for plotting.
            nodes_2d = self._2d_nodes_to_grid(nodes_wlvl=nodes_wlvl)
            orig_geom = nodes_2d[df_geo_col]
            nodes_2d_gdf = gpd.GeoDataFrame(nodes_2d, geometry=orig_geom, crs=crs_orig)

            # self.iresults["node_stats"] = nodes_2d_gdf
            return nodes_2d_gdf
        except Exception as e:
            raise e from None

    # TODO staat dit niet al in hhnk_research_tools
    def _2d_nodes_to_grid(self, nodes_wlvl):
        """Transfer the nodes into polygons of the grid."""
        try:
            nodes_2d = nodes_wlvl[nodes_wlvl[node_type_col] == two_d].copy()
            # replace geometry with polygons of the cells
            nodes_2d.loc[:, df_geo_col] = [box(*row) for row in self.grid_result.cells.subset(all_2d).cell_coords.T]
            nodes_2d.loc[:, minimal_dem_col] = self.grid_result.cells.subset(all_2d).z_coordinate
            return nodes_2d
        except Exception as e:
            raise e from None

    def run_flowline_stats(self):
        """
        Deze functie leest alle stroom lijnen in uit het 3di resultaat. Vervolgens wordt gekeken naar het type van de lijn
        (1D2D of 2D). Vervolgens wordt op drie tijdstappen (het begin van de regen het einde van de regen en het einde van de
        som) het volgende bepaald:
            * De waterstand per tijdstap
            * Het debiet (q) in m3/s per tijdstap
            * De stroomsnelheid in m/s per tijdstap
            * De stroomrichting per tijdstap
        """
        # Load individual line results
        flowlines_gdf = self._read_flowline_results()
        pumplines_gdf = self._read_pumpline_results()

        # combine to one table
        lines_gdf = pd.concat([flowlines_gdf, pumplines_gdf], ignore_index=True, sort=False)
        lines_gdf = lines_gdf[lines_gdf.geometry.length != 0]  # Drop weird values with -9999 geometries

        return lines_gdf

    def _read_flowline_results(self):
        try:
            coords = hrt.threedi.line_geometries_to_coords(
                self.grid_result.lines.line_geometries
            )  # create gdf from node coords

            flowlines_gdf = gpd.GeoDataFrame(geometry=coords, crs=f"EPSG:{DEF_TRGT_CRS}")
            flowlines_gdf[id_col] = self.grid_result.lines.id
            flowlines_gdf[spatialite_id_col] = self.grid_result.lines.content_pk

            content_type_list = self.grid_result.lines.content_type.astype("U13")
            flowlines_gdf[content_type_col] = content_type_list

            flowlines_gdf[kcu_col] = self.grid_result.lines.kcu
            flowlines_gdf.loc[flowlines_gdf[kcu_col].isin([51, 52]), content_type_col] = one_d_two_d
            flowlines_gdf.loc[flowlines_gdf[kcu_col].isin([100, 101]), content_type_col] = two_d

            q = self.grid_result.lines.timeseries(
                indexes=[
                    self.timestep_df[t_start_rain_col].value,
                    self.timestep_df[t_end_rain_col].value,
                    self.timestep_df[t_end_sum_col].value,
                ]
            ).q  # waterstand
            vel = self.grid_result.lines.timeseries(
                indexes=[
                    self.timestep_df[t_start_rain_col].value,
                    self.timestep_df[t_end_rain_col].value,
                    self.timestep_df[t_end_sum_col].value,
                ]
            ).u1
            q_all = self.grid_result.lines.timeseries(indexes=slice(0, -1)).q
            vel_all = self.grid_result.lines.timeseries(indexes=slice(0, -1)).u1

            # Write discharge and velocity to columns in dataframe
            for index, time_str in enumerate(suffixes_list):
                if time_str == max_sfx:
                    q_max_ind = abs(q_all).argmax(axis=0)
                    flowlines_gdf[q_m3_s_col + time_str] = np.round(
                        [row[q_max_ind[enum]] for enum, row in enumerate(q_all.T)], 5
                    )
                else:
                    flowlines_gdf[q_m3_s_col + time_str] = np.round(q[index], 5)

            for index, time_str in enumerate(suffixes_list):
                if time_str == max_sfx:
                    vel_max_ind = abs(vel_all).argmax(axis=0)
                    flowlines_gdf[vel_m_s_col + time_str] = np.round(
                        [row[vel_max_ind[enum]] for enum, row in enumerate(vel_all.T)],
                        5,
                    )
                else:
                    flowlines_gdf[vel_m_s_col + time_str] = np.round(vel[index], 3)

            # Flowlines of 1d2d lines weirdly have flow in different direction.
            # Therefore we invert this here so arrows are plotted correctly
            for index, time_str in enumerate(suffixes_list):
                flowlines_gdf.loc[
                    flowlines_gdf[content_type_col] == one_d_two_d,
                    q_m3_s_col + time_str,
                ] = flowlines_gdf.loc[
                    flowlines_gdf[content_type_col] == one_d_two_d,
                    q_m3_s_col + time_str,
                ].apply(lambda x: x * -1)

            for index, time_str in enumerate(suffixes_list):
                filt = (
                    flowlines_gdf[content_type_col] == one_d_two_d,
                    vel_m_s_col + time_str,
                )

                flowlines_gdf.loc[filt] = flowlines_gdf.loc[filt].apply(lambda x: x * -1)

            return flowlines_gdf
        except Exception as e:
            raise e from None

    def _read_pumpline_results(self):
        try:
            coords = [LineString([x[[0, 1]], x[[2, 3]]]) for x in self.grid_result.pumps.node_coordinates.T]
            pump_gdf = gpd.GeoDataFrame(geometry=coords, crs=f"EPSG:{DEF_TRGT_CRS}")

            pump_gdf[id_col] = self.grid_result.pumps.id
            pump_gdf[content_type_col] = "pump_line"
            pump_gdf[pump_capacity_m3_s_col] = self.grid_result.pumps.capacity

            q_m3 = self.grid_result.pumps.timeseries(
                indexes=[
                    self.timestep_df[t_start_rain_col].value,
                    self.timestep_df[t_end_rain_col].value,
                    self.timestep_df[t_end_sum_col].value,
                ]
            ).q_pump  # waterstand
            q_all_pump = self.grid_result.pumps.timeseries(indexes=slice(0, -1)).q_pump

            for index, time_str in enumerate(suffixes_list):
                if time_str == max_sfx:
                    q_max_ind = abs(q_all_pump).argmax(axis=0)
                    pump_gdf[q_m3_s_col + time_str] = np.round(
                        [row[q_max_ind[enum]] for enum, row in enumerate(q_all_pump.T)],
                        5,
                    )
                else:
                    pump_gdf[q_m3_s_col + time_str] = np.round(q_m3[index], 5)
            return pump_gdf
        except Exception as e:
            raise e from None


# %%
if __name__ == "__main__":
    from pathlib import Path

    from hhnk_threedi_tools import Folders

    TEST_MODEL = Path(__file__).parent.parent.parent.parent.full_path(r"tests/data/model_test/")
    folder = Folders(TEST_MODEL)
    # %%
    self = OneDTwoDTest.from_path(TEST_MODEL)

    # def test_run_depth_at_timesteps_test(self):
    """test of de 0d1d test werkt"""
    output = self.run_levels_depths_at_timesteps()

    assert len(output) > 0
    assert output[0] == 1
    assert "waterdiepte_T15.tif" in self.test_1d2d.fenv.output.one_d_two_d[0].content

    # %%
    folder.threedi_results.one_d_two_d.revisions
