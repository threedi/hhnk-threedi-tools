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
from shapely.geometry import Point
from threedi_api_client.api import ThreediApi
from threedi_api_client.openapi import ApiException
from threedi_api_client.versions import V3Api
from threedi_scenario_downloader import downloader as dl
from threedigrid.admin.gridresultadmin import GridH5AggregateResultAdmin, GridH5ResultAdmin

from hhnk_threedi_tools.breaches.breaches import Breaches
from hhnk_threedi_tools.breaches.create_breach_graph import create_breach_graph
from hhnk_threedi_tools.breaches.download_results_from_3di import download_results_from_3di


def download_breach_scenario(base_folder, model_name, metadata_path, filter_names):
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
    dl.set_api_key(api_keys["lizard"])

    # Create list of available scenarios
    name = []

    if not filter_names:
        # look up tabel
        # metadata_gdf_v6 = gpd.read_file(metadata_path, driver="Shapefile")

        # Filter Scenarios
        # name_ids = metadata_gdf_v6.loc[(metadata_gdf_v6["model"] == model_name), "SC_IDENT"].values
        # remove nan values
        id_simulations = [name_id for name_id in name_ids if not np.isnan(name_id)]

        for id_simulation in id_simulations:
            # scenario_name = metadata_gdf_v6.loc[(metadata_gdf_v6["SC_IDENT"] == id_simulation), "SC_NAAM"].values[0]
            scenario_count = api_client.usage_list(simulation__name=scenario_name).count

            if scenario_count == 0:
                print((scenario_name))
                continue

            else:
                now = datetime.now()
                scenario_finish = api_client.usage_list(simulation__name=scenario_name).results[0].finished
                scenario_finish_convert = scenario_finish.replace(tzinfo=None)
                delta_finish = now - scenario_finish_convert
                limit = timedelta(days=8)
                if delta_finish > limit:
                    continue
                else:
                    scenario_id = api_client.usage_list(simulation__name=scenario_name).results[0].simulation.id
                    if float(scenario_id) in id_simulations:
                        name.append(scenario_name)
                    else:
                        print(f"{scenario_name} is still running")
    else:
        print(f"the scenarios selected to download are: {filter_names}")
        name = filter_names

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

    crs_epsg = 28992
    # look up tabel
    if os.path.exists(metadata_path):
        metadata_gdf = gpd.read_file(metadata_path)
    else:
        cols = [
            "SC_NAAM",
            "DBR_BR_MAX",
            "DBR_BRESDI",
            "DBR_QMAX",
            "MOD_DATE",
            "MOD_START",
            "MOD_EIND",
            "MOD_REKEND",
            "MOD_SIM_ST",
            "MOD_SIM_EI",
            "MOD_SIM_DU",
            "DBR_INI_CR",
            "DBR_LOW_CR",
            "BUW_HMAX",
            "DBR_VTOT",
            "SC_IDENT",
            "BREACH_ID",
            "SC_DATE",
            "BES_3Di_REgeometry",
        ]

        metadata_gdf = gpd.GeoDataFrame(columns=cols + ["geometry"], geometry="geometry", crs=f"EPSG:{crs_epsg}")

    for x_name in name:
        print(x_name)
        # set main output folder for the scenario with name x_name
        breach_folder = Path(os.path.join(base_folder, model_name, x_name))
        if not breach_folder.exists():
            os.makedirs(breach_folder)
        breach_folder = Breaches(breach_folder)
        output_folder = breach_folder.path

        # set netcdf location folder
        netcdf_path = breach_folder.netcdf.path

        # Set netcdf files locations
        resultnc = netcdf_path / "results_3di.nc"
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
                .u1_max[:, 0]
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

            model_name = gr.model_slug
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

            # # Re order data into dataframe for breach data
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

            # # save to csv file
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

            # figuur maken
            create_breach_graph(
                x_name,
                time_sec,
                model_name,
                model_revision,
                breach_id_list,
                breach_depth,
                breach_wlev_upstream,
                breach_wlev_downstream,
                breach_q,
                breach_u,
                breach_width,
                fig_path_name,
            )

            # # figuur aggregated maken
            create_breach_graph(
                x_name,
                time_sec_agg,
                model_name,
                model_revision,
                breach_id_list,
                breach_depth,
                breach_wlev_upstream_agg,
                breach_wlev_downstream_agg,
                breach_q_agg,
                breach_u_agg,
                breach_width,
                fig_path_name_agg,
            )

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

            # Add information to metadata file
            scenario_name = x_name
            # scenario_name = simulation_name
            print(f"Adding metada for scearnio {scenario_name}")
            # scenario_name = scenario1['name']

            simulation_started_raw = simulation.started
            simuatlion_started = simulation_started_raw.strftime("%d-%m-%y")

            simulation_start_raw = simulation.simulation.start_datetime
            simulation_start = simulation_start_raw.strftime("%d-%m-%y %H:%M %S")

            mod_date_raw = simulation.started
            mod_date = mod_date_raw.strftime("%d-%m-%y %H:%M %S")

            simulation_end_raw = simulation.simulation.end_datetime  #'simulation_end': '2000-01-21T00:00:00Z'
            simulation_end = simulation_end_raw.strftime("%d-%m-%y %H:%M %S")

            simulation_duur = simulation_end_raw - simulation_start_raw
            sim_duur = f"{simulation_duur.days} 00:00:00"

            # Max Flow
            breach_qmax = max(df_simulation_data["Maximum Breach Discharge"])
            # Max With
            breach_width_max = max_breach_width
            # Simulation Status
            log_status = simulation.status
            # Day when the simulation started (human datum)
            log_start_datum = simulation.started.strftime("%d-%m-%Y %H:%M:%S")
            log_total_time_raw = timedelta(seconds=int((simulation.total_time)))
            log_total_time_format = datetime(1, 1, 1) + log_total_time_raw
            log_total_time = "0 " + log_total_time_format.strftime("%H:%M:%S")
            log_end_datum = simulation.finished.strftime("%d-%m-%Y %H:%M:%S")
            simulation_id = simulation.simulation.id
            relative_path = os.path.relpath(resultnc, start=netcdf_path)

            mask = metadata_gdf["SC_NAAM"] == scenario_name

            if mask.any():
                idx = metadata_gdf.index[mask][0]
            else:
                idx = len(metadata_gdf)
                metadata_gdf.loc[idx, "SC_NAAM"] = scenario_name
                metadata_gdf.loc[idx, "geometry"] = Point(x, y)

            # DBR_INI_CR default = 0 si está vacío
            ini_crest = metadata_gdf.loc[idx, "DBR_INI_CR"]
            ini_crest = 0 if pd.isna(ini_crest) else ini_crest
            low_crest_level = ini_crest - max_breach_depth

            # update
            metadata_gdf.loc[idx, "DBR_BR_MAX"] = breach_width_max
            metadata_gdf.loc[idx, "DBR_BRESDI"] = max_breach_depth
            metadata_gdf.loc[idx, "DBR_QMAX"] = breach_qmax
            metadata_gdf.loc[idx, "MOD_DATE"] = mod_date
            metadata_gdf.loc[idx, "MOD_START"] = log_start_datum
            metadata_gdf.loc[idx, "MOD_EIND"] = log_end_datum
            metadata_gdf.loc[idx, "MOD_REKEND"] = log_total_time
            metadata_gdf.loc[idx, "MOD_SIM_ST"] = simulation_start
            metadata_gdf.loc[idx, "MOD_SIM_EI"] = simulation_end
            metadata_gdf.loc[idx, "MOD_SIM_DU"] = sim_duur
            metadata_gdf.loc[idx, "DBR_LOW_CR"] = low_crest_level
            metadata_gdf.loc[idx, "BUW_HMAX"] = max_breach_wlev_upstream_list[0]
            metadata_gdf.loc[idx, "DBR_VTOT"] = vol_max
            metadata_gdf.loc[idx, "SC_IDENT"] = simulation_id
            metadata_gdf.loc[idx, "BREACH_ID"] = breach_id[0] if hasattr(breach_id, "__len__") else breach_id
            metadata_gdf.loc[idx, "SC_DATE"] = simuatlion_started
            metadata_gdf.loc[idx, "BES_3Di_RE"] = relative_path

            # Save metadata in the last vesion file.
            metadata_gdf.to_file(metadata_path, driver="GPKG")


# %%
if __name__ == "__main__":
    base_folder = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output\test_waterbalance"
    model_name = "test_water_balance"
    # metadata_path = r"E:\03.resultaten\IPO_Overstromingsberekeningen_compartimentering\metadata\metadata.gpkg"
    metadata_path = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output\test_waterbalance\metadata.gpkg"

    filter_names = [
        # "IPO_DWK-R32-06_DID454_SBLN_EQ_573",
        "test_breach_storage",
        # "IPO_DWK-R32-10_DID459_VRNK_O_EQ_401",
        # "IPO_DWK-R30-06_DID424_VRNK_W_EQ_1292",
        # "IPO_DWK-R55-10_DID768_SBLZ_EQ_1097",
        # "IPO_DWK-R44-39_DID928_SBHZ_EQ_765",
        # "IPO_DWK-R23-45_DID333_SBMZ_EQ_914",
        # "IPO_DWK-R06-08_DID124_SBMN_EQ_2024",
        # "IPO_DWK-R02-01_DID80_SBHN_EQ_862",
        # "IPO_D96_DID999_AB_EQ_995",
        "ROR-PRI-test_T100000",
        # "IPO_DWK-R24-04_DID360_SKB_EQ_3828",
        # "IPO_DWK-I\/-003863_DID52_WL1D_EQ_27881",
        # "IPO_DWK-R24-04_DID360_SKB_EQ_3828",
    ]

    # download_breach_scenario(base_folder, model_name, filter_names)
    download_breach_scenario(base_folder, model_name, metadata_path, filter_names)

# %%
