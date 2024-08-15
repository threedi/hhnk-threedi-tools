from threedi_scenario_downloader import downloader as dl
import os
import geopandas as gpd
import pandas as pd
import numpy as np
from threedigrid.admin.gridresultadmin import GridH5ResultAdmin, GridH5AggregateResultAdmin
from datetime import datetime, timedelta
from threedi_api_client.api import ThreediApi
from threedi_api_client.versions import V3Api
from threedi_api_client.openapi import ApiException
from pathlib import Path
#%%

def complete_missing_metadata(metadata_path, output_folder, skip_region):
    #Loggin code. 
    API_KEY='5ba9BrZJ.yNxkRWDwFXH8w4U0ceyGt2RaYkjv8uWu'
    config = {
        "THREEDI_API_HOST": "https://api.3di.live",
        "THREEDI_API_PERSONAL_API_TOKEN":API_KEY
        }
    api_client: V3Api = ThreediApi(config=config, version='v3-beta')

    #Loggin Confirmation Message
    try:
        user = api_client.auth_profile_list()
    except ApiException as e:
        print("Oops, something went wrong. Maybe you made a typo?")
    else:
        print(f"Successfully logged in as {user.username}!")

    # SET location and name
    organisation = 'Hoogheemraadschap Hollands Noorderkwartier'

    # SET YOUT OWN API KEY
    dl.set_api_key('Ssb7LXCk.sImsUmQLjXKHsNlaDs3tU0HHzPGQD8HN')   

    #look up tabel 
    metadata_gdf = gpd.read_file(metadata_path, driver = 'Shapefile', engine="pyogrio")
    regions =  os.listdir(output_folder)

    #%%
    scenario_paths = []
    for region in regions:
        region_path =  os.path.join(output_folder, region)
        scenarios = os.listdir(region_path)
        for scenario in scenarios:
            scenario_path = os.path.join(region_path, scenario)
            if os.path.exists(scenario_path):
                scenario_paths.append(Path(scenario_path))
                
    scenario_done = []       
    no_scenario = []    
    # 

    filter_metadata = metadata_gdf[metadata_gdf['SC_DATE'].isnull()]
    names = filter_metadata['SC_NAAM'].values
    #%%
    for path in scenario_paths:
        print(f'Adding Metadata for Scenario {path.name}')
        name = path.name
        if path in scenario_done or Path(path).parent.name in skip_region or name not in names:
            continue
        else:
            # Set up lists
            x_name_list = []
            x_list = []
            y_list = []
            breach_id_list = []
            max_breach_q_list =[]
            max_breach_u_list = []
            max_breach_wlev_upstream_list = []
            min_breach_wlev_upstream_list = []
            max_breach_wlev_downstream_list = []
            min_breach_wlev_downstream_list = []
            max_breach_depth_list = []
            max_breach_width_list = []
            x_name =  name

            model_name = Path(path).parent.name
            model_list = api_client.threedimodels_list(name__startswith=model_name)
            
            model_results = model_list.results
            for model in model_results:
                if model.schematisation_name != model_name:
                    model_results.remove(model)

            netcdf = os.path.join(path,'NetCDF')
            simulation_name_check = api_client.usage_list(simulation__name = x_name)
            if simulation_name_check.count == 0:
                no_scenario.append(x_name)
                print(f'the scenario {name} does not have simulation, check name')
                continue
            else:

                simulation = simulation_name_check.results[0] 
                simulation_name = simulation.simulation.name
                simulation_threedimodel_id = simulation.simulation.threedimodel_id

                for model_result in model_results:
                    print(model_result.id)
                    print(simulation_threedimodel_id)
                    if model_result.id == simulation_threedimodel_id:
                        print('yes')
                        schematisation_id = model_result.schematisation_id
                        revision_number = model_result.revision_number
                    else: 
                        schematisation_id = model_results[0].schematisation_id
                        revision_number = model_results[0].revision_number

                model_versie = "Schematisation id " + str(schematisation_id) + ' - Revision #' + str(revision_number)
                print(model_versie)
                    
                resultnc = os.path.join(netcdf, 'results_3di.nc')
                resulth5 = os.path.join(path,'NetCDF', 'gridadmin.h5')
                aggregated_result = os.path.join(path,'NetCDF', 'aggregate_results_3di.nc')

                gr = GridH5ResultAdmin(resulth5, resultnc)
                ga = GridH5AggregateResultAdmin(resulth5, aggregated_result)

                #find active breach
                breach_mask = gr.lines.breach_width[-1,:]>0 # op dit tijdstip moet de bres open zijn
                breach_line = gr.lines.id[breach_mask]
                
                breach_id = gr.lines.content_pk[breach_line]
                fig_path_name = os.path.join(path,'JPEG', x_name +'.png')
                fig_path_name_agg = os.path.join(path,'JPEG', x_name +'_agg.png')
                breach_width = gr.lines.timeseries(start_time=0,end_time=gr.lines.timestamps[-1]).breach_width[:,breach_mask][:,0]
                breach_width[breach_width<= -999]=np.nan
                max_breach_width = np.nanmax(breach_width)

                breach_depth = gr.lines.timeseries(start_time=0,end_time=gr.lines.timestamps[-1]).breach_depth[:,breach_mask][:,0]
                max_breach_depth = np.amax(breach_depth)

                breach_q = gr.lines.filter(id__eq=breach_line).timeseries(start_time=0,end_time=gr.lines.timestamps[-1]).q[:,0]

                breach_q_agg = ga.lines.filter(id__eq=breach_line).timeseries(start_time=0,end_time=gr.lines.timestamps[-1]).q_avg[:,0]
                breach_q_agg[breach_q_agg < 0] = 0
                max_breach_q_agg = np.amax(breach_q_agg)

                breach_u = gr.lines.filter(id__eq=breach_line).timeseries(start_time=0,end_time=gr.lines.timestamps[-1]).u1[:,0]
                    
                breach_u_agg = ga.lines.filter(id__eq=breach_line).timeseries(start_time=0,end_time=gr.lines.timestamps[-1]).u1_avg[:,0]
                breach_u_agg[breach_u_agg <= -999] = 0
                breach_u_agg = np.abs(breach_u_agg)
                max_breach_u_agg = np.amax(breach_u_agg)

                start_time = datetime.strptime(gr.lines.dt_timestamps[0].split('.')[0], '%Y-%m-%dT%H:%M:%S')
                end_time = datetime.strptime(gr.lines.dt_timestamps[-1].split('.')[0], '%Y-%m-%dT%H:%M:%S')
                timestamps = gr.lines.dt_timestamps
                time_sec = gr.lines.timestamps
                time_sec_agg = (ga.lines.timestamps['q_avg'])

                coef_dif = len(breach_width)/len(time_sec_agg)
                index = np.arange(0, len(breach_width), coef_dif)
                width_interp_agg = np.interp(index, np.arange(len(breach_width)), breach_width)
                breach_depth_agg = np.interp(index, np.arange(len(breach_depth)), breach_depth)
                
                #cumulative data
                try:
                    q_cuml = ga.lines.filter(id__eq=breach_line).timeseries(start_time=0,end_time=gr.lines.timestamps[-1]).q_cum[:,0]
                except AttributeError:
                    q_avg = (ga.lines.filter(id__eq=breach_line).timeseries(start_time=0,end_time=gr.lines.timestamps[-1]).q_avg[:,0])
                    q_per_timestep = q_avg *900
                    q_cuml = np.cumsum(q_per_timestep)
                    
                vol_max = q_cuml[-1]

                # get breach waterlevels
                breach_node_upstream = gr.lines.filter(id__eq=breach_line).line[1]
                breach_node_downstream = gr.lines.filter(id__eq=breach_line).line[0]
                
                breach_wlev_upstream = gr.nodes.filter(id__eq=breach_node_upstream).timeseries(
                    start_time=0,end_time=gr.lines.timestamps[-1]).s1[:,0]
                breach_wlev_upstream[breach_wlev_upstream <= -999]=np.nan
                max_breach_wlev_upstream = np.amax(breach_wlev_upstream)
                min_breach_wlev_upstream = np.amin(breach_wlev_upstream)

                breach_wlev_downstream = gr.nodes.filter(id__eq=breach_node_downstream).timeseries(
                    start_time=0,end_time=gr.lines.timestamps[-1]).s1[:,0]
                breach_wlev_downstream[breach_wlev_downstream <= -999]=np.nan
                max_breach_wlev_downstream = np.nanmax(breach_wlev_downstream)
                min_breach_wlev_downstream = np.nanmin(breach_wlev_downstream)
                    
                # get aggergated breach waterlevels
                breach_wlev_upstream_agg = ga.nodes.filter(id__eq=breach_node_upstream).timeseries(
                    start_time=0,end_time=gr.lines.timestamps[-1]).s1_avg[:,0]
                breach_wlev_upstream_agg[breach_wlev_upstream_agg <= -999]=np.nan
                max_breach_wlev_upstream_agg = np.amax(breach_wlev_upstream_agg)
                min_breach_wlev_upstream_agg = np.amin(breach_wlev_upstream_agg)

                breach_wlev_downstream_agg = ga.nodes.filter(id__eq=breach_node_downstream).timeseries(
                    start_time=0,end_time=gr.lines.timestamps[-1]).s1_avg[:,0]
                breach_wlev_downstream_agg[breach_wlev_downstream_agg <= -999]=np.nan
                max_breach_wlev_downstream_agg = np.nanmax(breach_wlev_downstream_agg)
                min_breach_wlev_downstream_agg = np.nanmin(breach_wlev_downstream_agg)
                
                # coordianten van het bovenstrooms punt als brescoordinaat (het lukt me niet dit uit gr.breaches te halen)
                x = gr.nodes.filter(id__eq=breach_node_upstream).coordinates[0][0]
                y = gr.nodes.filter(id__eq=breach_node_upstream).coordinates[1][0]    

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
                #Select maximum and mimimum data per breach.
                df_simulation_data = pd.DataFrame({'name':x_name_list,'x': x_list, 'y': y_list, 
                                                'breach_id' : breach_id_list,
                                                'Maximum Breach Discharge':max_breach_q_list,
                                                'Maximum Breach Width':max_breach_width_list,
                                                'Maximum Breach Flow Velocity':max_breach_u_list,
                                                'Maximum Upstream Water Level':max_breach_wlev_upstream_list,
                                                'Minimum Upstream Water Level':min_breach_wlev_upstream_list,
                                                'Maximum Downstream Water Lev':max_breach_wlev_downstream_list,
                                                'Minimum Downstream Water Level':min_breach_wlev_downstream_list,
                                                'Maximum Breach Depth': max_breach_depth_list
                                                })

                #Add information to metadata file
                print(f'Adding metada for scearnio {x_name}')
                # scenario_name = scenario1['name']
                
                simulation_started_raw= simulation.started 
                simuatlion_started = simulation_started_raw.strftime('%d-%m-%y')
                
                simulation_start_raw = simulation.simulation.start_datetime
                simulation_start = simulation_start_raw.strftime('%d-%m-%y %H:%M %S')
                
                mod_date_raw = simulation.started
                mod_date = mod_date_raw.strftime('%d-%m-%y %H:%M %S')

                simulation_end_raw  = simulation.simulation.end_datetime #'simulation_end': '2000-01-21T00:00:00Z'
                simulation_end = simulation_end_raw.strftime('%d-%m-%y %H:%M %S')

                simulation_duur = simulation_end_raw -  simulation_start_raw
                sim_duur = f'{simulation_duur.days} 00:00:00'

                #Max Flow
                breach_qmax = max(df_simulation_data['Maximum Breach Discharge'])
                #Max With
                breach_width_max = max_breach_width
                #Simulation Status
                log_status  = simulation.status
                #Day when the simulation started (human datum)
                log_start_datum  = simulation.started.strftime("%d-%m-%Y %H:%M:%S")
                log_total_time_raw = timedelta(seconds=int((simulation.total_time)))
                log_total_time_format = datetime(1,1,1) + log_total_time_raw
                log_total_time = '0 '+ log_total_time_format.strftime("%H:%M:%S")
                log_end_datum = simulation.finished.strftime("%d-%m-%Y %H:%M:%S")
                low_crest_level =  metadata_gdf.loc[metadata_gdf['SC_NAAM'] == x_name, 'DBR_INI_CR'].values[0]  - max_breach_depth
                simulation_id = simulation.simulation.id
                
                relative_path = (os.path.relpath(resultnc))[3:]
                
                metadata_gdf.loc[metadata_gdf['SC_NAAM'] == x_name, 'DBR_BR_MAX'] = (breach_width_max)
                metadata_gdf.loc[metadata_gdf['SC_NAAM'] == x_name, 'DBR_BRESDI'] = (max_breach_depth)
                metadata_gdf.loc[metadata_gdf['SC_NAAM'] == x_name, 'DBR_QMAX'] = (breach_qmax)
                metadata_gdf.loc[metadata_gdf['SC_NAAM'] == x_name, 'MOD_DATE'] = (mod_date)       
                metadata_gdf.loc[metadata_gdf['SC_NAAM'] == x_name, 'MOD_START'] = (log_start_datum)
                metadata_gdf.loc[metadata_gdf['SC_NAAM'] == x_name, 'MOD_EIND'] = (log_end_datum)
                metadata_gdf.loc[metadata_gdf['SC_NAAM'] == x_name, 'MOD_REKEND'] = (log_total_time)
                metadata_gdf.loc[metadata_gdf['SC_NAAM'] == x_name, 'MOD_SIM_ST'] = (simulation_start)
                metadata_gdf.loc[metadata_gdf['SC_NAAM'] == x_name, 'MOD_SIM_EI'] = (simulation_end)
                metadata_gdf.loc[metadata_gdf['SC_NAAM'] == x_name, 'MOD_SIM_DU'] = (sim_duur)
                metadata_gdf.loc[metadata_gdf['SC_NAAM'] == x_name, 'DBR_LOW_CR'] = (low_crest_level)
                metadata_gdf.loc[metadata_gdf['SC_NAAM'] == x_name, 'BUW_HMAX'] = (max_breach_wlev_upstream_list[0])
                metadata_gdf.loc[metadata_gdf['SC_NAAM'] == x_name, 'BES_3Di_RE'] = (relative_path)
                metadata_gdf.loc[metadata_gdf['SC_NAAM'] == x_name, 'DBR_VTOT'] = (vol_max)
                metadata_gdf.loc[metadata_gdf['SC_NAAM'] == x_name, 'SC_IDENT'] = (simulation_id)
                metadata_gdf.loc[metadata_gdf['SC_NAAM'] == x_name, 'ID'] = (breach_id)
                metadata_gdf.loc[metadata_gdf['SC_NAAM'] == x_name, 'SC_DATE'] = (simuatlion_started)
                metadata_gdf.loc[metadata_gdf['SC_NAAM'] == x_name, 'MOD_VERSIE'] = model_versie

                metadata_gdf.to_file(metadata_path, driver = 'Shapefile')

                scenario_done.append(path)
                print(f'metadta fill for scenario {x_name}')

#%%
if __name__ == "__main__":
    
    metadata_path = r"E:\03.resultaten\Overstromingsberekeningen primaire doorbraken 2024\metadata\v7\metadata_shapefile.shp"
    output_folder =  r'E:\03.resultaten\Overstromingsberekeningen primaire doorbraken 2024\output'
    
    #if there is a region to skip, type the name of thre region/modell within the list.
    skip_region = ['ROR PRI - dijktraject 13-5 - special']

    complete_missing_metadata(metadata_path, output_folder,skip_region)

    # %%
