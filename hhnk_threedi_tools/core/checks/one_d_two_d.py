# -*- coding: utf-8 -*-
"""
Created on Mon Aug 23 11:27:51 2021

@author: chris.kerklaan
"""
# Third-party imports
import os
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import box
from shapely.geometry import LineString

# research tools
import hhnk_research_tools as hrt
# from hhnk_research_tools.threedi.geometry_functions import coordinates_to_points
# from hhnk_research_tools.variables import file_types_dict, TIF
# from hhnk_research_tools.threedi.construct_rain_scenario import threedi_timesteps
# from hhnk_research_tools.threedi.construct_rain_scenario_dataframe import (
#     create_results_dataframe,
# )
from hhnk_research_tools.variables import (
    t_start_rain_col,
    t_end_rain_col,
    t_end_sum_col,
    all_2d,
)

# Local imports
import hhnk_threedi_tools.core.checks.grid_result_metadata as grid_result_metadata
from hhnk_threedi_tools.core.folders import Folders, create_tif_path
from hhnk_threedi_tools.variables.default_variables import DEF_TRGT_CRS
from hhnk_threedi_tools.variables.database_aliases import df_geo_col
from hhnk_threedi_tools.variables.one_d_two_d import (
    wtrlvl_col,
    one_d_two_d,
    max_sfx,
    suffixes_list,
    pump_line,
    id_col,
    spatialite_id_col,
    content_type_col,
    kcu_col,
    q_m3_s_col,
    vel_m_s_col,
    pump_capacity_m3_s_col,
    start_rain_sfx,
    end_rain_sfx,
    twelve_hr_after_rain_sfx,
    node_type_col,
    one_d,
    two_d,
    one_d_boundary_col,
    max_area_col,
    wtrlvl_m_col,
    wet_area_m2_col,
    minimal_dem_col,
    volume_m3_col,
    storage_mm_col,
)





#TODO functies weer in class onderbrengen, class nu buiten gebruik.
class OneDTwoDTest:
    def __init__(self, folder: Folders, revision=0, dem_path=None):
        self.fenv = folder
        self.revision=revision

        self.grid_result = folder.threedi_results.one_d_two_d[self.revision].grid
        rain, detected_rain, timestep, days_dry_start, days_dry_end, self.timestep_df = grid_result_metadata.construct_scenario(self.grid_result)

        # if output_path:
        #     self.output_path = output_path
        #     self.layer_path = output_path + "/Layers"
        #     self.log_path = output_path + "/Logs"
        # else:
        #     self.layer_path = str(folder.output.one_d_two_d[self.revision].layers)
        #     self.log_path = str(folder.output.one_d_two_d[self.revision].logs)

        if dem_path:
            self.dem_path = dem_path
        else:
            self.dem_path = self.fenv.model.schema_base.rasters.dem.path

        self.iresults = {}

    @classmethod
    def from_path(cls, path_to_polder, **kwargs):
        return cls(Folders(path_to_polder), **kwargs)

    @property
    def results(self):
        return self.iresults

    def read_flowline_results(self):
        try:
            threedi_result = self.threedi_results
            timesteps_df = self.timestep_df

            coords = hrt.threedi.line_geometries_to_coords(
                threedi_result.lines.line_geometries
            )  # create gdf from node coords

            flowlines_gdf = gpd.GeoDataFrame(
                geometry=coords, crs=f"EPSG:{DEF_TRGT_CRS}"
            )
            flowlines_gdf[id_col] = threedi_result.lines.id
            flowlines_gdf[spatialite_id_col] = threedi_result.lines.content_pk

            content_type_list = threedi_result.lines.content_type.astype("U13")
            flowlines_gdf[content_type_col] = content_type_list

            flowlines_gdf[kcu_col] = threedi_result.lines.kcu
            flowlines_gdf.loc[
                flowlines_gdf[kcu_col].isin([51, 52]), content_type_col
            ] = one_d_two_d
            flowlines_gdf.loc[
                flowlines_gdf[kcu_col].isin([100, 101]), content_type_col
            ] = two_d

            q = threedi_result.lines.timeseries(
                indexes=[
                    timesteps_df[t_start_rain_col].value,
                    timesteps_df[t_end_rain_col].value,
                    timesteps_df[t_end_sum_col].value,
                ]
            ).q  # waterstand
            vel = threedi_result.lines.timeseries(
                indexes=[
                    timesteps_df[t_start_rain_col].value,
                    timesteps_df[t_end_rain_col].value,
                    timesteps_df[t_end_sum_col].value,
                ]
            ).u1
            q_all = threedi_result.lines.timeseries(indexes=slice(0, -1)).q
            vel_all = threedi_result.lines.timeseries(indexes=slice(0, -1)).u1

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
                ].apply(
                    lambda x: x * -1
                )

            for index, time_str in enumerate(suffixes_list):
                filt = (
                    flowlines_gdf[content_type_col] == one_d_two_d,
                    vel_m_s_col + time_str,
                )

                flowlines_gdf.loc[filt] = flowlines_gdf.loc[filt].apply(
                    lambda x: x * -1
                )

            return flowlines_gdf
        except Exception as e:
            raise e from None

    def run_levels_depths_at_timesteps(self):
        """
        Deze functie bepaalt de waterstanden op de gegeven tijdstappen op basis van het 3di resultaat.
        Vervolgens wordt op basis van de DEM en de waterstand per tijdstap de waterdiepte bepaald.
        """
        try:
            timesteps_arr = [
                self.timestep_df["t_start_rain"].value,
                self.timestep_df["t_end_rain"].value,
                self.timestep_df["t_end_sum"].value,
            ]
            # hours since start of calculation
            timestrings = [int(round(self.grid_result.nodes.timestamps[t] / 60 / 60, 0)) for t in timesteps_arr]

            dem_list, dem_nodata, dem_meta = hrt.load_gdal_raster(self.dem_path)

            for timestep, timestr in zip(timesteps_arr, timestrings):
                # output files
                wlvl_output_path = getattr(self.fenv.output.one_d_two_d[self.revision], f'waterstand_T{timestr}').path
                depth_output_path = getattr(self.fenv.output.one_d_two_d[self.revision], f'waterdiepte_T{timestr}').path
                print(wlvl_output_path)
                print(depth_output_path)
                # calculate waterlevel at selected timestep in nodes gdf
                nodes_2d_wlvl = self._read_2node_wlvl_at_timestep(timestep)
                wlvl_list = hrt.gdf_to_raster(
                    gdf=nodes_2d_wlvl,
                    value_field=wtrlvl_col,
                    raster_out=wlvl_output_path,
                    nodata=dem_nodata,
                    metadata=dem_meta,
                )
                # calculate water depth at time steps at nodes
                _ = self._create_depth_raster(wlvl_list, dem_list, dem_nodata, dem_meta, depth_output_path)
            return timestrings
        except Exception as e:
            raise e from None


    def _read_2node_wlvl_at_timestep(self, timestep):
        """timesteps is the index of the time in the timeseries you want to use 
        to calculate the wlvl and depth raster"""
        nodes_2d = gpd.GeoDataFrame()
        # * inputs every element from row as a new function argument.
        nodes_2d[df_geo_col] = [
            box(*row) for row in self.grid_result.cells.subset(all_2d).cell_coords.T
        ]
        # waterstand
        nodes_2d[wtrlvl_col] = (
            self.grid_result.nodes.subset(all_2d).timeseries(indexes=[timestep]).s1[0]
        )
        return nodes_2d


    def _create_depth_raster(self, wlvl_list, dem_list, dem_nodata, dem_meta, raster_output_path):
        """Calculate the depth raster by subtracting the dem from the wlvl raster."""
        # difference between surface and initial water level
        try:
            depth_list = np.subtract(wlvl_list, dem_list)

            # restore nodata pixels using a mask, also filter waterways (height=10) and negative depths
            nodatamask = (dem_list == dem_nodata) | (dem_list == 10) | (depth_list < 0)
            depth_list[nodatamask] = dem_nodata

            # write array to tiff
            hrt.save_raster_array_to_tiff(
                output_file=raster_output_path,
                raster_array=depth_list,
                nodata=dem_nodata,
                metadata=dem_meta,
            )
            return depth_list
        except Exception as e:
            raise e from None

            content_type_list = threedi_result.lines.content_type.astype("U13")
            flowlines_gdf[content_type_col] = content_type_list

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
            nodes_wlvl[node_type_col].replace(
                [1, 3, 7], [two_d, one_d, one_d_boundary_col], inplace=True
            )

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

        # def write(self, filename, result, csv_path, gpkg_path):
        #     hrt.gdf_write_to_csv(result, csv_path, filename)
        #     hrt.gdf_write_to_geopackage(result, gpkg_path, filename)


    #TODO staat dit niet al in hhnk_research_tools
    def _2d_nodes_to_grid(self, nodes_wlvl):
        """Transfer the nodes into polygons of the grid."""
        try:
            nodes_2d = nodes_wlvl[nodes_wlvl[node_type_col] == two_d].copy()
            # replace geometry with polygons of the cells
            nodes_2d.loc[:, df_geo_col] = [
                box(*row) for row in self.grid_result.cells.subset(all_2d).cell_coords.T
            ]
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
            flowlines_gdf.loc[
                flowlines_gdf[kcu_col].isin([51, 52]), content_type_col
            ] = one_d_two_d
            flowlines_gdf.loc[
                flowlines_gdf[kcu_col].isin([100, 101]), content_type_col
            ] = two_d

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
                        [row[vel_max_ind[enum]] for enum, row in enumerate(vel_all.T)], 5
                    )
                else:
                    flowlines_gdf[vel_m_s_col + time_str] = np.round(vel[index], 3)

            # Flowlines of 1d2d lines weirdly have flow in different direction.
            # Therefore we invert this here so arrows are plotted correctly
            for index, time_str in enumerate(suffixes_list):
                flowlines_gdf.loc[
                    flowlines_gdf[content_type_col] == one_d_two_d, q_m3_s_col + time_str
                ] = flowlines_gdf.loc[
                    flowlines_gdf[content_type_col] == one_d_two_d, q_m3_s_col + time_str
                ].apply(lambda x: x * -1)

            for index, time_str in enumerate(suffixes_list):
                filt= flowlines_gdf[content_type_col] == one_d_two_d, vel_m_s_col + time_str

                flowlines_gdf.loc[filt] = flowlines_gdf.loc[filt].apply(lambda x: x * -1)

            return flowlines_gdf
        except Exception as e:
            raise e from None


    def _read_pumpline_results(self):
        try:
            coords = [
                LineString([x[[0, 1]], x[[2, 3]]])
                for x in self.grid_result.pumps.node_coordinates.T
            ]
            pump_gdf = gpd.GeoDataFrame(geometry=coords, crs=f"EPSG:{DEF_TRGT_CRS}")

            pump_gdf[id_col] = self.grid_result.pumps.id
            pump_gdf[content_type_col] = pump_line
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
                        [row[q_max_ind[enum]] for enum, row in enumerate(q_all_pump.T)], 5
                    )
                else:
                    pump_gdf[q_m3_s_col + time_str] = np.round(q_m3[index], 5)
            return pump_gdf
        except Exception as e:
            raise e from None



