# -*- coding: utf-8 -*-
"""
Created on Tue Apr 12 14:40:28 2022

@author: chris.kerklaan
"""

# First-party imports
from getpass import getpass
from datetime import datetime
from datetime import timedelta

# Third-party imports
import pytz
import threedi_api_client as tac
from cached_property import cached_property
from threedi_api_client import ThreediApi
from threedi_api_client.versions import V3Api, V3BetaApi

# Local

from hhnk_threedi_tools.variables.api_settings import API_SETTINGS

# Globals
TIMEZONE = "Europe/Amsterdam"



class Simulation:
    """
    Usage:

        sim = Simulation(CONFIG)
        sim.model =  "BWN Schermer interflow referentie #2"
        sim.template = "Referentie"
        sim.create()

    """

    def __init__(
        self,
        username: str,
        password: str,
        start_time: datetime = datetime(2000, 1, 1, 0, 0),
        end_time: datetime = datetime(2000, 1, 2, 0, 0),
        host="https://api.3di.live",
    ):

        self.start = start_time
        self.end = end_time

        # not defined variables
        self._model = None
        self._template = None
        self._organisation = None
        self._start_rain = None
        self._end_rain = None

        config=  {
                "THREEDI_API_HOST": host,
                "THREEDI_API_USERNAME": username,
                "THREEDI_API_PASSWORD": password,
            }
        
        print(config)
        self.threedi_api = ThreediApi(config=config)
        self.threedi_api_beta = ThreediApi(config=config, version="v3-beta")

    @property
    def id(self):
        return self.simulation.id

    @property
    def model(self):
        return self._model

    @model.setter
    def model_name(self, model_name: str):
        call = self.threedi_api.threedimodels_list(
            name=model_name, inp_success=True, disabled=False
        )
        self._model = api_result(call, "Model does not exist.")
    
    @model.setter
    def model_id(self, model_id:int):
        self._model = self.threedi_api.threedimodels_read(model_id)
    
    @property
    def template(self):
        return self._template

    @template.setter
    def template(self, template_name: str):
        if not self.model:
            raise ValueError("Please set model first")

        call = self.threedi_api_beta.simulation_templates_list(
            name=template_name, simulation__threedimodel__id=self.model.id
        )
        self._template = api_result(call, "Template does not exist.")

    @property
    def organisation(self):
        return self._organisation

    @organisation.setter
    def organisation_name(self, organisation_name):
        call = self.threedi_api.organisations_list(name=organisation_name)
        self._organisation = api_result(call, "Organisation does not exist.")

    @organisation.setter
    def organisation_id(self, organisation_id):
        call = self.threedi_api.organisations_read(organisation_id)
        self._organisation = api_result(call, "Organisation does not exist.")
        
    @property
    def start_rain(self):
        return self._start_rain

    @start_rain.setter
    def start_rain(self, date: datetime):
        self._start_rain = date
        assert self.start <= date <= self.end

    @property
    def end_rain(self):
        return self._end_rain

    @end_rain.setter
    def end_rain(self, date: datetime):
        self._end_rain = date
        assert self.start <= date <= self.end

    @property
    def duration(self):
        return int((self.end - self.start).total_seconds())

    def create(self, simulation_name):
        data = {
            "template": self.template.id,
            "name": simulation_name,
            "threedimodel": self.model.id,
            "organisation": self.organisation._unique_id,
            "start_datetime": self.start,
            "end_datetime": self.end,
        }
        call = self.threedi_api_beta.simulations_from_template(data)
        self.simulation = api_result(call, "Simulation is not yet processed")

    def add_constant_rain(
        self, data={"offset": None, "duration": None, "value": None, "units": "m/s"}
    ):
        self.threedi_api.simulations_events_rain_constant_create(self.id, data)

    def add_basic_post_processing(self, name):
        basic_processing_data = {
            "scenario_name": name,
            "process_basic_results": True,
        }

        self.threedi_api.simulations_results_post_processing_lizard_basic_create(
        self.id, data=basic_processing_data
            )
    def add_damage_post_processing(self, data):
        self.threedi_api.simulations_results_post_processing_lizard_damage_create(
                            self.id, data=data
                        )
    def add_arrival_post_processing(self, data):
        self.threedi_api.simulations_results_post_processing_lizard_arrival_create(
                    self.id, data=data
                )
        
class HHNK(Simulation):
    def __init__(
        self,
        username, 
        password,
        sqlite_file,
        scenario_name,
        model_id,
        organisation_uuid,
        days_dry_start,
        hours_dry_start,
        days_rain,
        hours_rain,
        days_dry_end,
        hours_dry_end,
        rain_intensity,
        basic_processing,
        damage_processing,
        arrival_processing,
    ):

        start_datetime, end_datetime, rain_data = self.date_and_rain_magic(
            days_dry_start,
            hours_dry_start,
            days_rain,
            hours_rain,
            days_dry_end,
            hours_dry_end,
            rain_intensity,
        )
        super().__init__(
            username,
            password,
            start_datetime,
            end_datetime,
        )

        self.create(scenario_name)
        self.add_constant_rain(rain_data)
        
        if basic_processing:
            self.add_basic_post_processing(scenario_name)
            
        if damage_processing:
            self.add_damage_post_processing(API_SETTINGS["damage_processing"])
        
        if arrival_processing:
            self.add_arrival_post_processing({"basic_post_processing": True})
            
    def date_and_rain_magic(
        self,
        days_dry_start,
        hours_dry_start,
        days_rain,
        hours_rain,
        days_dry_end,
        hours_dry_end,
        rain_intensity,
    ):
        if hours_dry_start + hours_rain >= 24:
            extra_days_rain = 1
            hours_end_rain = hours_dry_start + hours_rain - 24
        else:
            extra_days_rain = 0
            hours_end_rain = hours_dry_start + hours_rain

        if hours_dry_start + hours_rain + hours_dry_end >= 24:
            if (
                hours_dry_start + hours_rain + hours_dry_end >= 48
            ):  # two days are added in hours rain and dry
                extra_days = 2
                hours_end = hours_dry_start + hours_rain + hours_dry_end - 48
            else:  # one day is added in hours rain and dry
                extra_days = 1
                hours_end = hours_dry_start + hours_rain + hours_dry_end - 24
        else:  # Hours rain and dry do not add up to one day
            extra_days = 0
            hours_end = hours_dry_start + hours_rain + hours_dry_end

        # model_id = find model id based on slug (or pass model_id to this function)
        start_datetime = datetime(2000, 1, 1, 0, 0)
        end_datetime = datetime(2000, 1, 1, 0, 0) + timedelta(
            days=(days_dry_start + days_rain + days_dry_end + extra_days),
            hours=hours_end,
        )

        # add rainfall event
        rain_intensity_mmph = float(rain_intensity)  # mm/hour
        rain_intensity_mps = rain_intensity_mmph / (1000 * 3600)
        rain_start_dt = start_datetime + timedelta(
            days=days_dry_start, hours=hours_dry_start
        )
        rain_end_dt = start_datetime + timedelta(
            days=(days_dry_start + days_rain + extra_days_rain), hours=hours_end_rain
        )
        duration = (rain_end_dt - rain_start_dt).total_seconds()
        offset = (rain_start_dt - start_datetime).total_seconds()

        rain_data = {
            "offset": offset,
            "duration": duration,
            "value": rain_intensity_mps,
            "units": "m/s",
        }
        return start_datetime, end_datetime, rain_data



#     def add_constant_rain(self):
#         #adds a constant rain
#         data={"offset": 0,
#             "duration": self.duration_rain,
#             "value": RAIN_VALUE,
#             "units":"m/s"}
#         start_time=time.time()
#         while True:
#             elapsed_time=time.time()-start_time
#             if 'add_constant_rain' in locals():
#                 break
#             else:
#                 try:
#                     add_constant_rain=self.threedi_api.simulations_events_rain_constant_create(self.id_sim,data)
#                 except:
#                     time.sleep(2)
#                     continue
#         logging.info('  o   Constant rain added, value: 'f"{RAIN_VALUE:.2f}"' mm/hour for 'f"{self.duration_rain/3600:.0f}"' hours')

#     def add_nrr_rain(self):
#         #add nrr rain
#         data={
#             "offset": 0,
#             "duration": self.duration_rain,
#             "units": "m/s",
#             "reference_uuid": "d6c2347d-7bd1-4d9d-a1f6-b342c865516f",
#             "start_datetime": self.start_rain.strftime("%Y-%m-%dT%H:%M:%SZ"),
#             "multiplier": 1
#         }
#         start_time=time.time()
#         while True:
#             elapsed_time=time.time()-start_time
#             if 'add_nrr_rain' in locals():
#                 break
#             else:
#                 try:
#                     add_nrr_rain=self.threedi_api.simulations_events_rain_rasters_lizard_create(self.id_sim,data)
#                 except:
#                     time.sleep(2)
#                     continue
#         logging.info('  o   NRR rain added')

#     def add_emergency_pump(self):
#         #An emergency pump was added in order to get the water levels back to normal as fast as possible, added to the simulation using a negative lateral.
#         #Information below is received by the waterboard
#         #start noodpomp
#         #Pomp is gestart op 13-10-2019     1:30 uur
#         #Pomp is gestopt op 14-10-2019     19:10 uur
#         #Pomp had een debiet van 60 m3/minuut

#         amsterdam = pytz.timezone('Europe/Amsterdam')
#         utc=pytz.utc
#         emergency_pump_start = amsterdam.localize(datetime.datetime(2019,10,13,1,30)).astimezone(utc)
#         emergency_pump_end = amsterdam.localize(datetime.datetime(2019,10,14,19,10)).astimezone(utc)
#         emergency_pump_discharge = -1 #pumping away 1m3/s = 60m3/minute
#         emergency_pump_start_seconds = (emergency_pump_start - self.start_simulation).total_seconds()
#         emergency_pump_end_seconds = (emergency_pump_end - self.start_simulation).total_seconds()
#         emergency_pump_duration = emergency_pump_end_seconds-emergency_pump_start_seconds
#         emergency_pump_lateral_timeserie = [[0.0, 0.0], [emergency_pump_start_seconds-1, 0], [emergency_pump_start_seconds, emergency_pump_discharge], [emergency_pump_end_seconds, emergency_pump_discharge],[emergency_pump_end_seconds+1, 0]]

#         lat_data={
#                                     "values": emergency_pump_lateral_timeserie,
#                                     "units": "m3/s",
#                                     "connection_node": 9582,
#                                     "offset": 0,
#                                 }
#         start_time=time.time()
#         while True:
#             elapsed_time=time.time()-start_time
#             if 'add_emergency_pump' in locals():
#                 break
#             else:
#                 try:
#                     add_emergency_pump=self.threedi_api.simulations_events_lateral_timeseries_create(simulation_pk=self.id_sim,data=lat_data,async_req=False)
#                 except:
#                     time.sleep(2)
#                     continue
#         logging.info('  o   Emergency pump added')


#     def filter_weir_data(self,df):
#         """Process weir data
#         - Drop N/A values
#         - Dutch decimal text to float
#         - Set time to UTC (taking summer/winter time switch into account)
#         - Round to centimeters
#         - Remove concurrent identical values (only changes in the weir-setting remain)
#         - Calculate offset from simulation start
#         """
#         df = df.dropna() #drop rows containing NaN values
#         convert_columns = ['stuwstand','geschatdebiet'] #which columns to convert from text with decimal seperator to float
#         df[convert_columns] = df[convert_columns].apply(lambda x: pd.to_numeric(x.astype(str).str.replace(',','.'), errors='coerce')) #convert text to float
#         df = df.set_index('tijd') # set index to time
#         df.dropna(axis = 0, how = 'all', inplace = True)
#         df = df.tz_localize('CET',ambiguous='infer').tz_convert(pytz.utc) #convert CET time to UTC taking summer/winter switch into account
#         df = df.round(2) #round numbers to centimeters
#         df["double"] = df['stuwstand'] == df['stuwstand'].shift(1) # check if weir_crest level is the same as the previous row
#         df = df.reset_index()
#         df = df.loc[df['double'] == False] #delete row is previous crest level is the same
#         df["offset"] = df["tijd"]-self.start_simulation #calculate offset
#         df["offset"] = df["offset"].apply(lambda x: pd.to_numeric(int(x.total_seconds()), errors='coerce')) #transfer offset to seconds
#         duration_simulation=int((self.end_simulation-self.start_simulation).total_seconds())
#         df = df.loc[df['offset'] > False] #subset weir settings durings simulation
#         df = df.loc[df['offset'] < duration_simulation] #subset weir settings durings simulation
#         df["duration"] = (df["offset"] - df['offset'].shift(-1))*-1 #calculate the duration of a weir setting
#         df["duration"]
#         return df

#     def add_crest_control(self,df,weir_id):
#         ex = None
#         counter = 1
#         my_control_dict=dict.fromkeys(["timed"])
#         my_control_dict["timed"]=[]
#         controls={'filename':'my-laterals.json','offset':0}
#         for index, row in df.iterrows():
#             logging.info('Adding control nr: 'f"{counter}"' to dict')
#             offset = row['offset']
#             weir_crest_level = row['stuwstand']
#             duration = row['duration']
#             if math.isnan(duration):
#                 duration = self.duration_simulation-offset

#             my_control_dict["timed"].append(
#             {
#               "offset": offset,
#               "duration": duration,
#               "value": [weir_crest_level],
#               "type": "set_crest_level",
#               "structure_id": weir_id,
#               "structure_type": "v2_weir"
#              }
#             )
#             counter=counter+1

#         logging.info('dict ready')

#         response=self.threedi_api.simulations_events_structure_control_file_create(simulation_pk=self.id_sim,data=controls)
#         requests.put(response.put_url,data=json.dumps(my_control_dict))
#         logging.info('  o   Controls added for structure with id = 'f"{weir_id}")

#         return my_control_dict

#     def delete_2D_water_level_raster(self):
#         id_raster=self.threedi_api.simulations_initial2d_water_level_raster_list(self.id_sim).results[0]._id
#         logging.info(id_raster)
#         delete_raster=self.threedi_api.simulations_initial2d_water_level_raster_delete(id_raster,self.id_sim)
#         logging.info('  o   2D water level raster from simulation template deleted')

#     def delete_2D_water_level_global(self):
#         id_global=self.threedi_api.simulations_initial2d_water_level_constant_list(self.id_sim).results[0]._id
#         logging.info(id_global)
#         delete_global=self.threedi_api.simulations_initial2d_water_level_constant_delete(id_global,self.id_sim)
#         logging.info('  o   2D global water level from simulation template deleted')

#     def add_2D_water_level_global(self):
#         data={
#           "value": GLOBAL_2D_WL
#         }
#         start_time=time.time()
#         while True:
#             elapsed_time=time.time()-start_time
#             if 'add_water_level_2d_const' in locals():
#                 break
#             else:
#                 try:
#                     add_water_level_2d_const=self.threedi_api.simulations_initial2d_water_level_constant_create(self.id_sim,data)
#                 except:
#                     time.sleep(2)
#                     continue
#         logging.info('  o   2D global water level added of 'f"{GLOBAL_2D_WL}")

#     def save_state(self):
#         #saves the state of the system at the end of the simulation to be added in other model runs

#         data={
#             'name':'state_at_the_end_of_'f"{EVENT}",
#             'time':self.duration_simulation
#             }
#         start_time=time.time()
#         while True:
#             elapsed_time=time.time()-start_time
#             if 'save_state' in locals():
#                 break
#             else:
#                 try:
#                     save_state=self.threedi_api.simulations_create_saved_states_timed_create(self.id_sim,data)
#                 except:
#                     time.sleep(2)
#                     continue
#         self.saved_state_id=self.threedi_api.simulations_create_saved_states_timed_list(self.id_sim)._results[0]._id
#         logging.info('  o   Saving state is set with id='f"{self.saved_state_id}")

#     def add_saved_state(self):
#         #adds a saved state to the start of the simulation

#         data={'saved_state':SAVED_STATE_ID}
#         start_time=time.time()
#         while True:
#             elapsed_time=time.time()-start_time
#             if 'add_saved_state' in locals():
#                 break
#             else:
#                 try:
#                     add_saved_state=self.threedi_api.simulations_initial_saved_state_create(self.id_sim,data)
#                 except:
#                     time.sleep(2)
#                     continue
#         logging.info('  o   Saved state with id='f"{SAVED_STATE_ID}"' added')

#     def basic_lizard_post_processing(self):
#         #set lizard basic post processing

#         data={"scenario_name":self.sim['name'],"process_basic_results":True}
#         start_time=time.time()
#         while True:
#             elapsed_time=time.time()-start_time
#             if 'add_post_process' in locals():
#                 break
#             else:
#                 try:
#                     add_post_process=self.threedi_api.simulations_results_post_processing_lizard_basic_create(self.id_sim,data)
#                 except:
#                     time.sleep(2)
#                     continue
#         logging.info('  o   Basic Lizard post processing added')

#     def queue_simulation(self):
#         #queues simulation

#         start_time=time.time()
#         n=0
#         while True:
#             elapsed_time=time.time()-start_time
#             if 'start_sim' in locals():
#                 break
#             else:
#                 try:
#                     start_sim=self.threedi_api.simulations_actions_create(self.id_sim,{'name':'queue'})
#                 except:
#                     if elapsed_time >= n:
#                         logging.info('Simulation not queued. 3Di is still processing the forcings, else it means that something went wrong. Time elapsed: 'f"{elapsed_time/60:.0f}"' min...')
#                         n=n+60
#                     time.sleep(2)
#                     if n>WAIT_TIME:
#                         raise RuntimeError('Waited long enough. Something is not working')
#                     else:
#                         continue
#         logging.info('Simulation '+str(int(self.id_sim))+' started...')

#     def run(self):
#         #runs all functions
#         if INITIALISATION:
#             self.process_time_settings()
#             self.set_simulation_from_template()
#             self.add_nrr_rain()
#             self.delete_2D_water_level_raster()
#             self.add_2D_water_level_global()
#             df_BW=self.filter_weir_data(df_BW_raw)
#             df_BDW=self.filter_weir_data(df_BDW_raw)
#             df_SD=self.filter_weir_data(df_SD_raw)
#             my_control_dict=self.add_crest_control(df_BW,BW_id)
#             my_control_dict2=self.add_crest_control(df_BDW,BDW_id)
#             my_control_dict3=self.add_crest_control(df_SD,SD_id)
#             self.save_state()
#             #self.add_saved_state()
#             self.basic_lizard_post_processing()
#             self.queue_simulation()
#         elif ACTUAL_SCENARIO:
#             self.process_time_settings()
#             self.set_simulation_from_template()
#             self.add_nrr_rain()
#             #self.delete_2D_water_level_global()
#             #self.add_2D_water_level_global()
#             #self.add_emergency_pump()
#             df_BW=self.filter_weir_data(df_BW_raw)
#             df_BDW=self.filter_weir_data(df_BDW_raw)
#             df_SD=self.filter_weir_data(df_SD_raw)
#             my_control_dict=self.add_crest_control(df_BW,BW_id)
#             my_control_dict2=self.add_crest_control(df_BDW,BDW_id)
#             my_control_dict3=self.add_crest_control(df_SD,SD_id)
#             #self.add_saved_state()
#             self.basic_lizard_post_processing()
#             self.queue_simulation()

#         return my_control_dict,my_control_dict2,my_control_dict3

# def create_threedi_simulation(
#     threedi_api_client,
#     sqlite_file,
#     scenario_name,
#     model_id,
#     organisation_uuid,
#     days_dry_start,
#     hours_dry_start,
#     days_rain,
#     hours_rain,
#     days_dry_end,
#     hours_dry_end,
#     rain_intensity,
#     basic_processing,
#     damage_processing,
#     arrival_processing,
# ):  # , days_dry_start, hours_dry_start, days_rain, hours_rain, days_dry_end, hours_dry_end, rain_intensity, organisation_uuid, model_slug, scenario_name, store_results):
#     """
#     Creates and returns a Simulation (doesn't start yet) and initializes it with
#     - initial water levels
#     - boundary conditions
#     - control structures
#     - rainfall events
#     - laterals
#     """
#     if hours_dry_start + hours_rain >= 24:
#         extra_days_rain = 1
#         hours_end_rain = hours_dry_start + hours_rain - 24
#     else:
#         extra_days_rain = 0
#         hours_end_rain = hours_dry_start + hours_rain

#     if hours_dry_start + hours_rain + hours_dry_end >= 24:
#         if (
#             hours_dry_start + hours_rain + hours_dry_end >= 48
#         ):  # two days are added in hours rain and dry
#             extra_days = 2
#             hours_end = hours_dry_start + hours_rain + hours_dry_end - 48
#         else:  # one day is added in hours rain and dry
#             extra_days = 1
#             hours_end = hours_dry_start + hours_rain + hours_dry_end - 24
#     else:  # Hours rain and dry do not add up to one day
#         extra_days = 0
#         hours_end = hours_dry_start + hours_rain + hours_dry_end

#     # model_id = find model id based on slug (or pass model_id to this function)
#     start_datetime = datetime(2000, 1, 1, 0, 0)
#     end_datetime = datetime(2000, 1, 1, 0, 0) + timedelta(
#         days=(days_dry_start + days_rain + days_dry_end + extra_days), hours=hours_end
#     )  # TODO

#     # create simulation api
#     threedi_sim_api = openapi_client.api.SimulationsApi(threedi_api_client)

#     # set up simulation
#     simulation = threedi_sim_api.simulations_create(
#         openapi_client.models.simulation.Simulation(
#             name=scenario_name,
#             threedimodel=model_id,
#             organisation=organisation_uuid,
#             start_datetime=start_datetime,
#             end_datetime=end_datetime,
#         )
#     )

#     # add initial water (1d)

#     while True:
#         try:
#             threedi_sim_api.simulations_initial1d_water_level_predefined_create(
#                 simulation_pk=simulation.id, data={}, async_req=False
#             )
#         except ApiException:
#             time.sleep(10)
#             continue
#         break

#     if sqlite_file is not None:
#         # add control structures (from sqlite)
#         add_control_from_sqlite(threedi_sim_api, sqlite_file, simulation)
#         add_laterals_from_sqlite(threedi_sim_api, sqlite_file, simulation)

#     # add rainfall event
#     rain_intensity_mmph = float(rain_intensity)  # mm/hour
#     rain_intensity_mps = rain_intensity_mmph / (1000 * 3600)
#     rain_start_dt = start_datetime + timedelta(
#         days=days_dry_start, hours=hours_dry_start
#     )
#     rain_end_dt = start_datetime + timedelta(
#         days=(days_dry_start + days_rain + extra_days_rain), hours=hours_end_rain
#     )
#     duration = (rain_end_dt - rain_start_dt).total_seconds()
#     offset = (rain_start_dt - start_datetime).total_seconds()

#     rain_data = {
#         "offset": offset,
#         "duration": duration,
#         "value": rain_intensity_mps,
#         "units": "m/s",
#     }

#     while True:
#         try:
#             threedi_sim_api.simulations_events_rain_constant_create(
#                 simulation.id, rain_data
#             )
#         except ApiException:
#             time.sleep(10)
#             continue
#         break

#     # Add postprocessing
#     if basic_processing:
#         basic_processing_data = {
#             "scenario_name": scenario_name,
#             "process_basic_results": True,
#         }
#         while True:
#             try:
#                 threedi_sim_api.simulations_results_post_processing_lizard_basic_create(
#                     simulation.id, data=basic_processing_data
#                 )
#             except ApiException:
#                 time.sleep(10)
#                 continue
#             break

#     # Damage posprocessing
#     if damage_processing:
#         damage_processing_data = API_SETTINGS["damage_processing"]
#         while True:
#             try:
#                 threedi_sim_api.simulations_results_post_processing_lizard_damage_create(
#                     simulation.id, data=damage_processing_data
#                 )
#             except ApiException:
#                 time.sleep(10)
#                 continue
#             break

#     if arrival_processing:
#         arrival_processing_data = {"basic_post_processing": True}
#         # Arrival time
#         while True:
#             try:
#                 threedi_sim_api.simulations_results_post_processing_lizard_arrival_create(
#                     simulation.id, data=arrival_processing_data
#                 )
#             except ApiException:
#                 time.sleep(10)
#                 continue
#             break

#     # return simulation object
#     return simulation


def api_result(
    result: tac.openapi.models.inline_response20062.InlineResponse20062, message: str
) -> tac.openapi.models.threedi_model.ThreediModel:
    """Raises an error if no results"""
    if len(result.results) == 0:
        raise ValueError(message)
    return result.results[0]


if __name__ == "__main__":
    sim = Simulation("chris.kerklaan", "YumYum01!")
    sim.model = "BWN Schermer interflow referentie #2"
    sim.template = "Referentie"
