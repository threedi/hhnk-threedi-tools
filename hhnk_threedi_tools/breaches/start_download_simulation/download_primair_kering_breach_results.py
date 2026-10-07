# %%
# def collect_ipo_restults(input_base_folder,base_scenario_prefix):
# SRIPT for downloading results and creating
"""
Downloaden en opslaan 3Di resultaat met één breslocatie.
Maakt een grafiek met belangrijkste info van de bres voor in de Storymap Overstromingen.

Werkt met API v3 en threedigrid 1.1.9.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
from threedi_api_client.api import ThreediApi
from threedi_api_client.openapi import ApiException
from threedi_api_client.versions import V3Api
from threedi_scenario_downloader import downloader as dl
from threedigrid.admin.gridresultadmin import GridH5AggregateResultAdmin, GridH5ResultAdmin

from hhnk_threedi_tools.breaches.breaches import Breaches
from hhnk_threedi_tools.breaches.ldo.metadata_class import COLUMNS_NAMES, metadata_template, metadata_type
from hhnk_threedi_tools.breaches.maps_graphs.create_breach_graph import create_breach_graph
from hhnk_threedi_tools.breaches.start_download_simulation.download_results_from_3di import download_results_from_3di

# %%


def download_breach_scenario(base_folder, simulation_metadata_df):
    api_keys_path = (
        rf"{os.getenv('APPDATA')}\3Di\QGIS3\profiles\default\python\plugins\hhnk_threedi_plugin\api_key.txt"
    )

    api_keys = hrt.read_api_file(api_keys_path)
    # Loggin code.
    config = {
        "THREEDI_API_HOST": "https://api.3di.live",
        "THREEDI_API_PERSONAL_API_TOKEN": api_keys["threedi"],
    }
    api_client: V3Api = ThreediApi(config=config, version="v3-beta")

    # Loggin Confirmation Message
    try:
        user = api_client.auth_profile_list()
    except ApiException as e:
        print("Oops, something went wrong. Maybe you made a typo?")
    else:
        print(f"Successfully logged in as {user.username}!")

    # SET API KEY
    dl.set_api_key(api_keys["threedi"])

    # Create list of available scenarios
    filter_names = simulation_metadata_df["scenario_name"].tolist()
    id_simulations = simulation_metadata_df["scenario_id"].tolist()
        

    # Set up lists
    x_name_list = []
    x_list = []
    y_list = []
    breach_id_list = []
    max_breach_q_list = []
    max_breach_u_list = []
    max_breach_wlev_upstream_list = []
    min_breach_wlev_upstream_list = []
    max_breach_wlev_downstream_list = []
    min_breach_wlev_downstream_list = []
    max_breach_depth_list = []
    max_breach_width_list = []

    for x_name in filter_names:

        print(x_name)
        scenario_id = api_client.usage_list(simulation__name=x_name).results[0].simulation.id
        
        if float(scenario_id) in id_simulations:
            
            print(f"downloading scenario: {x_name}")

            model_name = simulation_metadata_df.loc[simulation_metadata_df["scenario_name"] == x_name, "schematitation_name"].iloc[0]
            # set main output folder for the scenario with name x_name
            breach_folder = Path(os.path.join(base_folder, model_name, x_name))
            if not breach_folder.exists():
                os.makedirs(breach_folder)
            breach_folder = Breaches(breach_folder)
            output_folder = breach_folder.path

            # set netcdf location folder
            netcdf_path = breach_folder.netcdf.path

            # Set netcdf files locations
            resultnc = os.path.join(netcdf_path, "results_3di.nc")
            resulth5 = os.path.join(netcdf_path, "gridadmin.h5")
            aggregated_result = os.path.join(netcdf_path, "aggregate_results_3di.nc")
            if not os.path.exists(aggregated_result):
                # os.makedirs(netcdf_path)
                # zoek in de api een lijst met scenarios
                simulation = download_results_from_3di(netcdf_path, x_name, api_client)
            else:
                simulation = api_client.usage_list(simulation__name=x_name).results[0]

            # Set ssm folder location (This one is use when the raster can be downloaded from lizard)
            raster_folder = breach_folder.ssm.path
            if not raster_folder.exists():
                os.makedirs(raster_folder)

            # Set jpeg folder
            jpeg_path = breach_folder.jpeg.path
            if not jpeg_path.exists():
                os.makedirs(jpeg_path)

            # set figures names and locations
            fig_path_name = os.path.join(jpeg_path, x_name + ".png")
            fig_path_name_agg = os.path.join(jpeg_path, x_name + "_agg.png")

            # reduce the file name in case is need, otherwise we will get an error.
            if len(fig_path_name_agg) > 256:
                fig_path_name = os.path.join(jpeg_path, "disagg.png")
                fig_path_name_agg = os.path.join(jpeg_path, "agg.png")

            if os.path.exists(fig_path_name_agg):
                print(f"the graph for the scenario {x_name} already exists")
            else:
                # set csv location
                csv_result_simulation_data = os.path.join(output_folder, "simulation_data.csv")
                csv_result = os.path.join(output_folder, "breach_data.csv")
                csv_result_agg = os.path.join(output_folder, "breach_data_agg.csv")

                # open grid a netcdf administration files
                gr = GridH5ResultAdmin(resulth5, resultnc)
                ga = GridH5AggregateResultAdmin(resulth5, aggregated_result)

                # find active breach
                breach_mask = gr.lines.breach_width[-1, :] > 0  # op dit tijdstip moet de bres open zijn
                breach_line = gr.lines.id[breach_mask]

                # locate breach id
                breach_id = gr.lines.content_pk[breach_line]

                breach_width = gr.lines.timeseries(start_time=0, end_time=gr.lines.timestamps[-1]).breach_width[
                    :, breach_mask
                ][:, 0]
                breach_width[breach_width <= -999] = np.nan
                max_breach_width = np.nanmax(breach_width)

                breach_depth = gr.lines.timeseries(start_time=0, end_time=gr.lines.timestamps[-1]).breach_depth[
                    :, breach_mask
                ][:, 0]
                max_breach_depth = np.amax(breach_depth)

                breach_q = (
                    gr.lines.filter(id__eq=breach_line).timeseries(start_time=0, end_time=gr.lines.timestamps[-1]).q[:, 0]
                )

                breach_q_agg = (
                    ga.lines.filter(id__eq=breach_line)
                    .timeseries(start_time=0, end_time=gr.lines.timestamps[-1])
                    .q_avg[:, 0]
                )
                breach_q_agg[breach_q_agg < 0] = 0
                max_breach_q_agg = np.amax(breach_q_agg)

                breach_u = (
                    gr.lines.filter(id__eq=breach_line).timeseries(start_time=0, end_time=gr.lines.timestamps[-1]).u1[:, 0]
                )

                breach_u_agg = (
                    ga.lines.filter(id__eq=breach_line)
                    .timeseries(start_time=0, end_time=gr.lines.timestamps[-1])
                    .u1_avg[:, 0]
                )
                breach_u_agg[breach_u_agg <= -999] = 0
                breach_u_agg = np.abs(breach_u_agg)
                max_breach_u_agg = np.amax(breach_u_agg)

                start_time = datetime.strptime(gr.lines.dt_timestamps[0].split(".")[0], "%Y-%m-%dT%H:%M:%S")
                end_time = datetime.strptime(gr.lines.dt_timestamps[-1].split(".")[0], "%Y-%m-%dT%H:%M:%S")
                timestamps = gr.lines.dt_timestamps
                time_sec = gr.lines.timestamps
                time_sec_agg = ga.lines.timestamps["q_avg"]

                coef_dif = len(breach_width) / len(time_sec_agg)
                index = np.arange(0, len(breach_width), coef_dif)
                width_interp_agg = np.interp(index, np.arange(len(breach_width)), breach_width)
                breach_depth_agg = np.interp(index, np.arange(len(breach_depth)), breach_depth)

                # cumulative data
                q_cuml = (
                    ga.lines.filter(id__eq=breach_line)
                    .timeseries(start_time=0, end_time=gr.lines.timestamps[-1])
                    .q_max[:, 0]
                )
                vol_max = q_cuml[-1]

                # model_name = gr.model_slug
                model_revision = gr.revision_nr

                # get breach waterlevels upstream and downastream
                breach_node_upstream = gr.lines.filter(id__eq=breach_line).line[1]
                breach_node_downstream = gr.lines.filter(id__eq=breach_line).line[0]

                breach_wlev_upstream = (
                    gr.nodes.filter(id__eq=breach_node_upstream)
                    .timeseries(start_time=0, end_time=gr.lines.timestamps[-1])
                    .s1[:, 0]
                )
                breach_wlev_upstream[breach_wlev_upstream <= -999] = np.nan
                max_breach_wlev_upstream = np.amax(breach_wlev_upstream)
                min_breach_wlev_upstream = np.amin(breach_wlev_upstream)

                breach_wlev_downstream = (
                    gr.nodes.filter(id__eq=breach_node_downstream)
                    .timeseries(start_time=0, end_time=gr.lines.timestamps[-1])
                    .s1[:, 0]
                )
                breach_wlev_downstream[breach_wlev_downstream <= -999] = np.nan
                max_breach_wlev_downstream = np.nanmax(breach_wlev_downstream)
                min_breach_wlev_downstream = np.nanmin(breach_wlev_downstream)

                # get aggergated breach waterlevels
                breach_wlev_upstream_agg = (
                    ga.nodes.filter(id__eq=breach_node_upstream)
                    .timeseries(start_time=0, end_time=gr.lines.timestamps[-1])
                    .s1_avg[:, 0]
                )
                breach_wlev_upstream_agg[breach_wlev_upstream_agg <= -999] = np.nan
                max_breach_wlev_upstream_agg = np.amax(breach_wlev_upstream_agg)
                min_breach_wlev_upstream_agg = np.amin(breach_wlev_upstream_agg)

                breach_wlev_downstream_agg = (
                    ga.nodes.filter(id__eq=breach_node_downstream)
                    .timeseries(start_time=0, end_time=gr.lines.timestamps[-1])
                    .s1_avg[:, 0]
                )
                breach_wlev_downstream_agg[breach_wlev_downstream_agg <= -999] = np.nan
                max_breach_wlev_downstream_agg = np.nanmax(breach_wlev_downstream_agg)
                min_breach_wlev_downstream_agg = np.nanmin(breach_wlev_downstream_agg)

                # coordianten van het bovenstrooms punt als brescoordinaat (het lukt me niet dit uit gr.breaches te halen)
                x = gr.nodes.filter(id__eq=breach_node_upstream).coordinates[0][0]
                y = gr.nodes.filter(id__eq=breach_node_upstream).coordinates[1][0]

                # Re order data into dataframe for breach data
                df = pd.DataFrame(
                    {
                        "name": x_name,
                        "x": x,
                        "y": y,
                        "breach_line": breach_line[0],
                        "breach_id": breach_id[0],
                        "timestamps": timestamps,
                        "time_sec": time_sec,
                        "breach_width": breach_width,
                        "breach_depth": breach_depth,
                        "breach_q": breach_q,
                        "breach_u": breach_u,
                        "breach_wlev_upstream": breach_wlev_upstream,
                        "breach_wlev_downstream": breach_wlev_downstream,
                    }
                )

                # save to csv file
                df.to_csv(csv_result, sep=";", decimal=",")

                # Re order data into dataframe for breach data
                df_agg = pd.DataFrame(
                    {
                        "name": x_name,
                        "x": x,
                        "y": y,
                        "breach_line": breach_line[0],
                        "breach_id": breach_id[0],
                        # 'timestamps':timestamps,
                        "time_sec": time_sec_agg,
                        "breach_width": width_interp_agg,
                        "breach_depth": breach_depth_agg,
                        "breach_q": breach_q_agg,
                        "breach_u": breach_u_agg,
                        "breach_wlev_upstream": breach_wlev_upstream_agg,
                        "breach_wlev_downstream": breach_wlev_downstream_agg,
                    }
                )

                # save to csv file
                df_agg.to_csv(csv_result_agg, sep=";", decimal=",")

                # APPEND new values to list
                x_name_list.append(x_name)
                x_list.append(x)
                y_list.append(y)
                breach_id_list.append(breach_id)
                max_breach_q_list.append(max_breach_q_agg)
                max_breach_u_list.append(max_breach_u_agg)
                max_breach_wlev_upstream_list.append(max_breach_wlev_upstream)
                min_breach_wlev_upstream_list.append(min_breach_wlev_upstream)
                max_breach_wlev_downstream_list.append(max_breach_wlev_downstream)
                min_breach_wlev_downstream_list.append(min_breach_wlev_downstream)
                max_breach_depth_list.append(max_breach_depth)
                max_breach_width_list.append(max_breach_width)

                # Select maximum and mimimum data per breach.
                df_simulation_data = pd.DataFrame(
                    {
                        "name": x_name_list,
                        "x": x_list,
                        "y": y_list,
                        "breach_id": breach_id_list,
                        "Maximum Breach Discharge": max_breach_q_list,
                        "Maximum Breach Width": max_breach_width_list,
                        "Maximum Breach Flow Velocity": max_breach_u_list,
                        "Maximum Upstream Water Level": max_breach_wlev_upstream_list,
                        "Minimum Upstream Water Level": min_breach_wlev_upstream_list,
                        "Maximum Downstream Water Lev": max_breach_wlev_downstream_list,
                        "Minimum Downstream Water Level": min_breach_wlev_downstream_list,
                        "Maximum Breach Depth": max_breach_depth_list,
                    }
                )

                # save to csv file
                df_simulation_data.to_csv(csv_result_simulation_data, sep=";", decimal=",")

                # # figuur maken
                # create_breach_graph(
                #     x_name,
                #     time_sec,
                #     model_name,
                #     model_revision,
                #     breach_id_list,
                #     breach_depth,
                #     breach_wlev_upstream,
                #     breach_wlev_downstream,
                #     breach_q,
                #     breach_u,
                #     breach_width,
                #     fig_path_name,
                # )

                # # figuur aggregated maken
                # create_breach_graph(
                #     x_name,
                #     time_sec_agg,
                #     model_name,
                #     model_revision,
                #     breach_id_list,
                #     breach_depth,
                #     breach_wlev_upstream_agg,
                #     breach_wlev_downstream_agg,
                #     breach_q_agg,
                #     breach_u_agg,
                #     breach_width,
                #     fig_path_name_agg,
                # )

                # relative_path = (os.path.relpath(resultnc))[3:]

        else:
            print(f"{x_name} is still running")
        


# %%
if __name__ == "__main__":
    base_folder = Path(r"H:\03.resultaten\RWS_Test")
    simulation_metadata = base_folder / "simulations_id.xlsx"
    simulation_metadata_df = pd.read_excel(simulation_metadata, sheet_name="Sheet1")
    # new_metadata_path = r"y:\03.resultaten\IPO_Overstromingsberekeningen_compartimentering\metadata\metadata.gpkg"
    # filter_id_path = r"Y:\03.resultaten\Normering Regionale Keringen\output\scenarios_output\N&S\breach_SBMN_redo.gpkg"
    # filter_id_gdf = gpd.read_file(filter_id_path)
    # filter_names = filter_id_gdf["display_name"].tolist()
    filter_names = simulation_metadata_df["scenario_name"].tolist()
    download_breach_scenario(base_folder, simulation_metadata_df)
# %%
