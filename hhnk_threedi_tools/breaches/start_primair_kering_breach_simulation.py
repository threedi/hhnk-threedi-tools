
"""
Python script to run multiple breaches separeted. It requieres a metadata shapefile/geopackge to save the
simulations key values in it. if it requiere to set the scenario_name diferently you will need to change it 
within the function.
"""
#%%

import os
import geopandas as gpd
import numpy as np
import time as timesleep
import time as time2
from datetime import datetime, time
from asyncio.log import logger
from getpass import getpass
import os
from threedi_api_client.openapi import ApiException
from threedi_api_client.api import ThreediApi
from threedi_api_client.versions import V3Api
from pathlib import Path
from hhnk_threedi_tools.core.api.calculation import Simulation


def start_simulation_breaches(model_name, organisation_name, scenarios, filter_id, metadata_path, wait_time ):  
  #Loggin code. #TODO
  API_KEY = None
  #Loggin code. 
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
  # modeller_initial = '_JA'
  sim_duration = 20 # days
  start_datetime = datetime(2000, 1, 1, 0, 0)
  modellen_folder = r'E:\02.modellen'
  base_folder = os.path.join(modellen_folder, model_name)
  output_timestep = 900 #s

  # general settings setup
  numerical_setting = {
    "pump_implicit_ratio": 1,
    "cfl_strictness_factor_1d": 1,  
    "cfl_strictness_factor_2d": 1,
    "convergence_eps": 1e-5,
    "convergence_cg": 1e-9,
    "flow_direction_threshold": 1e-6,
    "friction_shallow_water_depth_correction": 0,
    "general_numerical_threshold": 1e-8,
    "time_integration_method": 0,
    "limiter_waterlevel_gradient_1d": 1,
    "limiter_waterlevel_gradient_2d": 1,
    "limiter_slope_crossectional_area_2d": 0,
    "limiter_slope_friction_2d": 0,
    "max_non_linear_newton_iterations": 20,
    "max_degree_gauss_seidel": 7,
    "min_friction_velocity": 1e-5,
    "min_surface_area": 1e-8,
    "use_preconditioner_cg": 1,
    "preissmann_slot": 0,
    "limiter_slope_thin_water_layer": 0.01,
    "use_of_cg": 20,
    "use_nested_newton": True,
    "flooding_threshold": 1e-5
  }

  physical_settings = {
    "use_advection_1d": 1,
    "use_advection_2d": 1
  }

  time_step_settings = {
    "time_step": 5,
    "min_time_step": 0.01,
    "max_time_step": 10,
    "use_time_step_stretch": True,
    "output_time_step": output_timestep
  }

  aggregation_settings= [
      {"name": "",
        "flow_variable": "discharge",
        "method": "avg",
        "interval": output_timestep
      },
      {"name": "",
        "flow_variable": "water_level",
        "method": "avg",
        "interval": output_timestep
      },
      {"name": "",
        "flow_variable": "water_level",
        "method": "max",
        "interval": output_timestep
      },
      {"name": "",
        "flow_variable": "discharge",
        "method": "max",
        "interval": output_timestep
      },
      {"name": "",
        "flow_variable": "flow_velocity",
        "method": "avg",
        "interval": output_timestep
      },
      {"name": "",
        "flow_variable": "flow_velocity",
        "method": "max",
        "interval": output_timestep
      }]
  
  # Find organisation uuid
  organisations = api_client.organisations_list(name__istartswith=organisation_name)
  org_uuid = organisations.results[0].unique_id

  #Search for the model we want to work with.
  model_list = api_client.threedimodels_list(name__contains=model_name)
  results = model_list.results
  first_value = True
  
  for result in results:
      if result.schematisation_name == model_name and first_value:
          first_value = False
          #get the id of the model 
          my_model_id = result.id
          my_model_revision = result.revision_number
          my_model_schema_id = result.schematisation_id
          model_versie = "Schematisation id " + str(my_model_schema_id) + ' - Revision #' + str(my_model_revision)
          print('model name:', result.name, 'model id:',my_model_id,' rev: ',my_model_revision)


  # Find the breaches in the model
  potential_breaches = api_client.threedimodels_potentialbreaches_list(my_model_id, limit=9999)
  path_model = os.path.join(base_folder, 'work in progress', 'schematisation', 'ROR PRI - dijktraject 13-5.gpkg')
  potential_breach_gpd = gpd.read_file(path_model, layer = 'potential_breach')

  display_names = potential_breach_gpd.display_name.values
  breach_id_gpd = []
  for display_name in display_names:
    for scenario in scenarios:
      if display_name.split('-')[-1] == str(scenario):
        active_breach = potential_breach_gpd[potential_breach_gpd['display_name']== display_name]
        breach_id_gpd.append(active_breach.id.values[0])

  number_breaches = potential_breaches.count

  #locate the breach result that corresponds to the connected point id
  specific_breaches = []
  breach_id = []
  id_filter = []

  if not filter_id:
    id_filter = breach_id_gpd
  else:
     id_filter = filter_id
  # id_filter = [102, 103, 104]
  for pnt_id in range(number_breaches):
    id = potential_breaches.results[pnt_id].connected_pnt_id  
    if id in id_filter:
    # if (1<= id <=3) or (5<= id <=74):
        breach_id.append(id)
        specific_breaches.append(pnt_id)

  # Set up simulations
  sleeptime = 2
  breach = {}
  # Start simulations in a loop
  number_breaches = specific_breaches
  #set up metadata
  metadata_gdf = gpd.read_file(metadata_path, driver="Shapefile")

  for x in (number_breaches):
    #if x >= 32: # DEBUGGING
      breach = potential_breaches.results[x]
      #print (x)
      print(breach.connected_pnt_id)
      #set simulatio time and fix format. 
      datum = datetime.now().date()
      datum_str = datum.strftime("%d-%m-%y")

      #select active breach according to connected point id from API
      active_breach = potential_breach_gpd[potential_breach_gpd['id']== breach.connected_pnt_id]
      
      #Set scearnio name aacoridng to active breach
      scenario_code = active_breach.code.values[0]
      scenario_split = scenario_code.split('-')
      if scenario_split[0][-2:] == '_1':
        scenario_split[0] = scenario_split[0][:-2]
        scenario_name ='ROR-PRI-'+ scenario_split[0] + '-T' + scenario_split[1]

      
      # Components of the simulation created. IS NOT RUNNING YET. See the status below
      simulation_template = api_client.simulation_templates_list(simulation__threedimodel__id=my_model_id).results[0]
      simulation = api_client.simulations_from_template(
          data={
            "template": simulation_template.id,
              "name": scenario_name ,
              "tags": ['ROR'],
              "threedimodel": my_model_id,
              "organisation": org_uuid,
              "start_datetime": start_datetime,
              "duration": sim_duration * 3600 *24  # in seconds
          }
      )
      timesleep.sleep(sleeptime)

      api_client.simulations_settings_time_step_delete(simulation_pk=simulation.id)
      api_client.simulations_settings_time_step_create(simulation_pk=simulation.id,
              data=time_step_settings)
      timesleep.sleep(sleeptime)

      api_client.simulations_settings_numerical_delete(simulation_pk=simulation.id)    
      api_client.simulations_settings_numerical_create(simulation_pk=simulation.id,
              data=numerical_setting)
      timesleep.sleep(sleeptime)

      api_client.simulations_settings_physical_delete(simulation_pk=simulation.id)
      api_client.simulations_settings_physical_create(simulation_pk=simulation.id,
              data=physical_settings)
      timesleep.sleep(sleeptime)

      for a in aggregation_settings:
          api_client.simulations_settings_aggregation_create(simulation_pk=simulation.id,
              data=a)
          timesleep.sleep(sleeptime)

      # Set breach event open
      breach_event = api_client.simulations_events_breaches_create(
          simulation.id, data={
              "potential_breach": breach.id,
              "duration_till_max_depth": 600,
              "maximum_breach_depth": 100,
              "initial_width": 50,
              "offset": 0
          }
    )
        
      timesleep.sleep(sleeptime)

      
      # ADD postprocessing
      # basic_processing_data = {
      #             "scenario_name": scenario_name ,
      #             "process_basic_results": True,
      #         }
      
      # api_client.simulations_results_post_processing_lizard_basic_create(
      #                     simulation.id, data=basic_processing_data)
      # timesleep.sleep(sleeptime)   

      # api_client.simulations_results_post_processing_lizard_arrival_create(
      #                     simulation_pk=simulation.id, data={})
      # timesleep.sleep(sleeptime)

      #update metadata
      metadata_gdf.loc[metadata_gdf['SC_NAAM'] == scenario_name, 'SC_IDENT'] = (simulation.id)
      metadata_gdf.loc[metadata_gdf['SC_NAAM'] == scenario_name, 'ID'] = (breach.line_id)
      metadata_gdf.loc[metadata_gdf['SC_NAAM']== scenario_name, 'SC_DATE'] = datum_str
      metadata_gdf.loc[metadata_gdf['SC_NAAM'] == scenario_name, 'MOD_VERSIE'] = model_versie

      #Save metadata as Shapefile
      path_gdf = Path(metadata_path)
      metadata_path = os.path.join(path_gdf.parent, path_gdf.name)
      if not os.path.exists(path_gdf): 
        os.mkdir(path_gdf)    
      metadata_gdf.to_file(metadata_path, driver = 'Shapefile')
              
      # Start simulation
      max_simulation = 3
      
      sim = Simulation(api_key=API_KEY)
      queue_jam_bwn = True
      
      while queue_jam_bwn:
          queue_length_bwn = sim.threedi_api.statuses_list(name__startswith='queued',
                                                        simulation__organisation__name__istartswith='BWN').count
          if queue_length_bwn < 2:
              if queue_length_bwn < max_simulation - 1:
                queue_jam_bwn = False
                free_org_uuid = "48dac75bef8a42ebbb52e8f89bbdb9f2"
                api_client.simulations_actions_create(simulation.id, data={"name": "queue"})
                print(str(x) + ': ' + scenario_name  + ' is queued with id ' + str(simulation.id))
          else:
              print('wait',wait_time,'s')
              time2.sleep(wait_time)   

#%%
if __name__ == "__main__":
  
  #Use organisation_name 'BWN HHNK' for standard simulation. Use the other one for very specific cases
  
  organisation_name = 'BWN HHNK'
  # organisation_name = 'Hoogheemraadschap Hollands Noorderkwartier'

  #Set the model name as it is either in 3di or in the local folder. 
  model_name = 'ROR PRI - dijktraject 13-5'

  #Select the return periods you want to start with. If you want to use all of them keep it. 
  scenarios = [10]

  # id_filter corresponds to the column 'id' of the potential breach table of the model we are working with.
  # In case of willing to run all the potential breach, leave the list empty  --> filter_id = []
  filter_id = [102]

  #location of the metadata file. Important to have at least 2 version: One for uploading and run model and the other one for downloading.
  metadata_path = Path(r"E:\03.resultaten\Overstromingsberekeningen primaire doorbraken 2024\metadata\v6\metadata_shapefile.shp")  

  #Time (in seconds) to wait until the script tries again to upload a model. We use it to not overload the API. 
  wait_time = 3600 # 1  hour

  start_simulation_breaches(model_name, organisation_name, scenarios, filter_id, metadata_path, wait_time)
  # %%
