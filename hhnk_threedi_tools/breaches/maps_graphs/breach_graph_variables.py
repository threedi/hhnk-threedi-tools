
#%%
from pathlib import Path

import numpy as np
import pandas as pd
from create_breach_graph import create_breach_graph
from threedigrid.admin.gridresultadmin import GridH5AggregateResultAdmin, GridH5ResultAdmin

def graph_breach_variables(
    x_name: str, #Breach name or Scenario name
    resulth5:Path,
    resultnc:Path,
    aggregated_result:Path,
    csv_result:Path,
    csv_result_agg:Path,
    fig_path_name:Path,
    fig_path_name_agg:Path,
    csv_result_simulation_data:Path,
):
    """
    Generate breach-variable CSV summaries and plots from 3Di grid results.

    This function reads time series and aggregate time series from the provided
    3Di result files (resulth5/resultnc and aggregated_result), extracts the
    active breach data, computes some aggregated statistics, writes per-timestep
    and aggregated CSV outputs, produces breach plots via create_breach_graph,
    and writes a CSV with selected simulation summary values.

    Parameters
    ----------
    x_name : str
        Name for the breach or scenario (used in output CSVs and plots).
    resulth5 : pathlib.Path
        Path to the grid H5 result file.
    resultnc : pathlib.Path
        Path to the corresponding NetCDF result file.
    aggregated_result : pathlib.Path
        Path to the aggregated result (used for averaged/aggregate time series).
    csv_result : pathlib.Path
        Output path for the per-timestep CSV file.
    csv_result_agg : pathlib.Path
        Output path for the aggregated (interpolated/averaged) CSV file.
    fig_path_name : pathlib.Path
        Output path (or base name) for the breach figures for raw time series.
    fig_path_name_agg : pathlib.Path
        Output path (or base name) for the breach figures for aggregated data.
    csv_result_simulation_data : pathlib.Path
        Output path for the CSV file containing selected maximum/minimum metrics.

    Returns
    -------
    None

    Side effects
    ------------
    - Reads 3Di result files via GridH5ResultAdmin and GridH5AggregateResultAdmin.
    - Writes CSV files (csv_result, csv_result_agg, csv_result_simulation_data).
    - Calls create_breach_graph to write figure files.
    - Replaces sentinel values (<= -999) with NaN or zeros where appropriate.

    Notes
    -----
    The function assumes exactly one active breach is present at the inspected
    timestep and will index the first matching breach. Time series length
    differences between raw and aggregated results are handled by simple
    interpolation.
    """

    # open grid a netcdf administration files
    gr = GridH5ResultAdmin(resulth5, resultnc)
    ga = GridH5AggregateResultAdmin(resulth5, aggregated_result)

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

    timestamps = gr.lines.dt_timestamps
    time_sec = gr.lines.timestamps
    time_sec_agg = ga.lines.timestamps["q_avg"]

    coef_dif = len(breach_width) / len(time_sec_agg)
    index = np.arange(0, len(breach_width), coef_dif)
    width_interp_agg = np.interp(index, np.arange(len(breach_width)), breach_width)
    breach_depth_agg = np.interp(index, np.arange(len(breach_depth)), breach_depth)

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

    breach_wlev_downstream_agg = (
        ga.nodes.filter(id__eq=breach_node_downstream)
        .timeseries(start_time=0, end_time=gr.lines.timestamps[-1])
        .s1_avg[:, 0]
    )
    breach_wlev_downstream_agg[breach_wlev_downstream_agg <= -999] = np.nan

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
            # "timestamps": timestamps,
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
            'timestamps':timestamps,
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

    # figuur aggregated maken
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
#%%
if __name__ == "__main__":
    from pathlib import Path
    import os
    breach_folder = (r'E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output\texel_overstroming_bressen\ROR-PRI-DUINEN_TEXEL_40-T100000')
    breach_netcdf = os.path.join(breach_folder, '01_NetCDF')
    breach_jpeg = os.path.join(breach_folder, '04_JPEG')
    breach_name = Path(breach_folder).name + '_v2'
    # Example usage
    graph_breach_variables(
        x_name= breach_name,
        resulth5= os.path.join(breach_netcdf,'gridadmin.h5'),
        resultnc= os.path.join(breach_netcdf, 'results_3di.nc'),
        aggregated_result= os.path.join(breach_netcdf, 'aggregate_results_3di.nc'),
        csv_result= os.path.join(breach_folder, f'{breach_name}_breach_data.csv'),
        csv_result_agg=  os.path.join(breach_folder, f'{breach_name}_breach_data_agg.csv'),
        fig_path_name= os.path.join(breach_jpeg, f'{breach_name}.png'),
        fig_path_name_agg= os.path.join(breach_jpeg, f'{breach_name}_agg.png'),
        csv_result_simulation_data= os.path.join(breach_folder,f'{breach_name}_simulation_data.csv'),
    )
# %%
