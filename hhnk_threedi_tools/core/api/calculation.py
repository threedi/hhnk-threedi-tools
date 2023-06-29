# -*- coding: utf-8 -*-
# %%
"""
Created on Tue Apr 12 14:40:28 2022

@author: chris.kerklaan

note, this was not needed
"""

# First-party imports
import datetime
import time
import numpy as np
import os
import requests
from pathlib import Path
import zipfile

# Third-party imports
import threedi_api_client as tac
from threedi_api_client import ThreediApi
from threedi_api_client.openapi import ApiException
from IPython.core.display import HTML


# Local
import hhnk_research_tools as hrt
from hhnk_threedi_tools.variables.api_settings import API_SETTINGS
from hhnk_threedi_tools.core.api.upload_model.threedi_calls import ThreediCalls

# Globals
TIMEZONE = "Europe/Amsterdam"


#TODO move to hrt
def update_dict_keys(mydict, translate_dict={}, remove_keys=[]) -> dict:
    """Rename dict keys and/or remove.
    trannslate_dict is of the format -> old:new"""
    for key_old in translate_dict:
        key_new = translate_dict[key_old]
        if key_old in mydict:
            mydict[key_new] = mydict.pop(key_old)

    for key in remove_keys:
        if key in mydict:
            mydict.pop(key)

    return mydict


class NumericalSettings:
    def __init__(self, database_path, settings_id):
        """
        database_path: path to sqlite.
        settings id: as defined in the global_settings (global_setting["numerical_settings_id"])
        """

        self.translate_dict_numerical = {
            "frict_shallow_water_correction": "friction_shallow_water_depth_correction",
            "integration_method": "time_integration_method",
            "limiter_grad_1d": "limiter_waterlevel_gradient_1d",
            "limiter_grad_2d": "limiter_waterlevel_gradient_2d",
            "max_nonlin_iterations": "max_non_linear_newton_iterations",
            "max_degree": "max_degree_gauss_seidel",
            "minimum_friction_velocity": "min_friction_velocity",
            "minimum_surface_area": "min_surface_area",
            "precon_cg": "use_preconditioner_cg",
            "thin_water_layer_definition": "limiter_slope_thin_water_layer",
            "use_of_nested_newton": "use_nested_newton",
        }

        # Minimum numerical settings. If in globalsettings the value is None, 
        # these values will be used.
        self.NUM_MIN = {
            # General
            "limiter_waterlevel_gradient_1d": 1,
            "limiter_waterlevel_gradient_2d": 1,
            "limiter_slope_crossectional_area_2d": 0,
            "limiter_slope_friction_2d": 0,
            "limiter_slope_thin_water_layer": 0,
            # Matrix
            "convergence_cg": 1e-12,
            "convergence_eps": 0.00005,
            "use_of_cg": 20,
            "use_nested_newton": 1,
            "max_degree_gauss_seidel": 0,
            "max_non_linear_newton_iterations": 20,
            "precon_cg": 1,
            "integration_method": 0,
            # Thresholds
            "flow_direction_threshold": 1e-13,
            "general_numerical_threshold": 1e-13,
            "thin_water_layer_definition": None,
            "minimum_friction_velocity": None,
            "minimum_surface_area": None,
            "min_friction_velocity": 0.01,
            "min_surface_area": 1.0e-8,
            "flooding_threshold": 0.000001,
            # Miscellaneous
            "cfl_strictness_factor_1d": 1,
            "cfl_strictness_factor_2d": 1,
            "friction_shallow_water_depth_correction": 0,
            "pump_implicit_ratio": 1,
            "preissmann_slot": 0,
            "time_integration_method": 0,
        }

        self.settings_df = hrt.sqlite_table_to_df(
                database_path=database_path, table_name="v2_numerical_settings"
            )
        self.settings_df.set_index("id", inplace=True)

        self.settings = self.settings_df.loc[settings_id].to_dict() 

        self.settings = update_dict_keys(mydict=self.settings, 
                                            translate_dict=self.translate_dict_numerical, 
                                            remove_keys=["id"])

        self.settings["flooding_threshold"] = 1e-5  #FIXME Not in sqlite?

        #Make sure minimum values are passed
        for key, value in self.settings.items():
            if value == None and key in self.NUM_MIN:
                self.settings[key] = self.NUM_MIN[key]
        
        #Clean (?)
        for key, value in self.settings.items():
            if type(value).__module__ == np.__name__:
                self.settings[key] = value.item()  # np to python types


class SimulationData:
    
    def __init__(self, sqlite_path, 
                        sim_name, 
                        sim_duration, 
                        rain_data=[{}],):
        
        self.sqlite_path = sqlite_path
        self.sim_name = sim_name

        self.global_setting_df = hrt.sqlite_table_to_df(
            database_path=self.sqlite_path, table_name="v2_global_settings"
        )
        self.global_setting = self.global_setting_df.iloc[0].to_dict() #only take first row
        self.boundary_1d_settings_df = hrt.sqlite_table_to_df(
            database_path=self.sqlite_path, table_name="v2_1d_boundary_conditions"
        )

        self.rain = rain_data
        if type(self.rain)==dict:
            self.rain = [self.rain]

        self.structure_control = self._get_control_from_sqlite(sim_duration=sim_duration)
        self.laterals = self._get_laterals_from_sqlite(sim_duration=sim_duration)
        self.boundaries = self._get_boundary_data()

        

    @property
    def _numerical_settings_raw(self):
        return NumericalSettings(database_path=self.sqlite_path, 
                                settings_id=self.global_setting["numerical_settings_id"])

    @property
    def numerical_settings(self):
        return self._numerical_settings_raw.settings

    @property
    def physical_settings(self):
        return {
                "use_advection_1d": int(self.global_setting["advection_1d"]),
                "use_advection_2d": int(self.global_setting["advection_2d"]),
            }

    @property
    def time_step_settings(self):
        return {
            "time_step": self.global_setting["sim_time_step"],
            "min_time_step": self.global_setting["minimum_sim_time_step"],
            "max_time_step": self.global_setting["maximum_sim_time_step"],
            "use_time_step_stretch": bool(self.global_setting["timestep_plus"]),
            "output_time_step": self.global_setting["output_time_step"],
            }


    def _get_control_from_sqlite(self, sim_duration):
        """
        Read table control structures from the Sqlite and use them in the initialisation of the simulation
        """
        # db_file = r"E:\02.modellen\23_Katvoed\02_schematisation\00_basis\bwn_katvoed.sqlite"
        try:
            # Assuming control_group_id = 1
            control_group_id = self.global_setting["control_group_id"]

            if control_group_id is None:
                return []
            else: 
                control_group_id=int(control_group_id)


            v2_control = []
            v2_control_df = hrt.execute_sql_selection(
                    query="SELECT * FROM v2_control WHERE control_group_id = {}".format(
                            control_group_id
                        ),
                    database_path=self.sqlite_path,
                )
            for index, row in v2_control_df.iterrows():
                data = {
                    "id": int(row["id"]),
                    "control_type": row["control_type"],
                    "control_id": int(row["control_id"]),
                    "control_group_id": int(row["control_group_id"]),
                    "measure_group_id": int(row["measure_group_id"]),
                }
                v2_control.append(data)

            v2_control_measure_map = []
            for index, row in hrt.sqlite_table_to_df(
                database_path=self.sqlite_path, table_name="v2_control_measure_map"
            ).iterrows():
                data = {
                    "id": int(row["id"]),
                    "measure_group_id": int(row["measure_group_id"]),
                    "object_type": row["object_type"],
                    "object_id": int(row["object_id"]),
                    "weight": int(row["weight"]),
                }
                v2_control_measure_map.append(data)

            v2_control_table = []
            for index, row in hrt.sqlite_table_to_df(
                database_path=self.sqlite_path, table_name="v2_control_table"
            ).iterrows():
                action_table_string = row["action_table"]
                action_table = []
                action_type = row["action_type"]
                for entry in action_table_string.split("#"):
                    try:
                        measurement = [float(entry.split(";")[0])]
                    except ValueError as e:
                        #Problem with action table 
                        print(f"""Problem with '{entry}' at index {action_table_string.index(entry)} of the action_table_string for
{row}
""")
                        raise e

                    if action_type in ["set_crest_level", "set_pump_capacity"]:
                        action = [float(entry.split(";")[1])]
                    elif action_type == "set_discharge_coefficients":
                        action = [
                            float(entry.split(";")[1].split(" ")[0]),
                            float(entry.split(";")[1].split(" ")[0]),
                        ]
                    else:
                        print("ACTION TYPE NOT SUPPORTED")

                    # TODO after bugfix control structures
                    measure_operator = ">"  # remove this hardcoded work-around after bugfix
                    # measure_operator = row[1]  # Uncomment this line after bugfix

                    if measure_operator in ["<", "<="]:
                        action_table.insert(0, measurement + action)
                    elif measure_operator in [">", ">="]:
                        action_table.append(measurement + action)
                data = {
                    "id": row["id"],
                    "action_table": action_table,
                    "measure_operator": measure_operator,
                    "target_id": row["target_id"],
                    "target_type": row["target_type"],
                    "measure_variable": row["measure_variable"],
                    "action_type": action_type,
                }
                v2_control_table.append(data)

        except:
            print("Unable to read control from sqlite (or there are 0)")
            raise

        control_data = []
        for control in v2_control:
            connection_node = None
            structure_type = None
            structure_id = None
            measure_variable = None
            operator = None
            action_type = None
            values = None

            for control_measure_map in v2_control_measure_map:
                if control_measure_map["measure_group_id"] == control["measure_group_id"]:
                    if control_measure_map["object_type"] == "v2_connection_nodes":
                        connection_node = control_measure_map["object_id"]

            if control["control_type"] == "table":
                for control_table in v2_control_table:
                    if control_table["id"] == control["control_id"]:
                        structure_type = control_table["target_type"]
                        structure_id = control_table["target_id"]

                        if control_table["measure_variable"] == "waterlevel":
                            measure_variable = "s1"

                        operator = control_table["measure_operator"]
                        action_type = control_table["action_type"]
                        values = control_table["action_table"]

            else:
                print("Only table control is supported")
                raise RuntimeError("Only table control is supported")

            data = {
                "offset": 0,
                "duration": sim_duration,
                "measure_specification": {
                    "name": "Measurement location",
                    "locations": [
                        {
                            "weight": 1,
                            "content_type": "v2_connection_node",
                            "content_pk": connection_node,
                        }
                    ],
                    "variable": measure_variable,
                    "operator": operator,
                },
                "structure_type": structure_type,
                "structure_id": structure_id,
                "type": action_type,
                "values": values,
            }
            control_data.append(data)

        return control_data
    


    def _get_laterals_from_sqlite(self, sim_duration):
        """
        Read 1D laterals from the Sqlite and use them in the initialisation of the simulation
        """
        laterals_data = []
        for index, row in hrt.sqlite_table_to_df(
            database_path=self.sqlite_path, table_name="v2_1d_lateral"
        ).iterrows():
            data = None

            connection_node = int(row["connection_node_id"])

            values = []

            for entry in row["timeseries"].splitlines():
                t = int(entry.split(",")[0]) * 60
                q = float(entry.split(",")[1])
                values.append([min(t, sim_duration), q])

                if t > sim_duration:
                    break

            data = {
                "values": values,
                "units": "m3/s",
                "connection_node": connection_node,
                "offset": 0,
            }
            laterals_data.append(data)
        return laterals_data
                    

    def _get_boundary_data(self):
        boundary_types =   {1: 'water_level', 
                        2: 'velocity',
                        3: 'discharge', 
                        5: 'sommerfeldt'
        }

        #1d boundary conditions
        data = []
        for index, row in self.boundary_1d_settings_df.iterrows():
            values=[]
            rows=[i for i in row['timeseries'].split('\n')]
            for i in rows:
                values.append([float(j) for j in i.split(',')])
            data_singleboundary= {
                "id": row["id"],
                "type": "1D",
                "interpolate": True,
                "values":values
            }
            data.append(data_singleboundary)
        return data

    @property
    def basic_processing(self):
        return {
            "scenario_name": self.sim_name,
            "process_basic_results": True,
        }
    
    @property
    def damage_processing(self):
        return API_SETTINGS["damage_processing"]

    @property
    def arrival_processing(self):
        return {"arrival_time": True} #TODO check if works? was: {"basic_post_processing": True}
    

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
        api_key: str,
        start_time: datetime.datetime = datetime.datetime(2022, 1, 1, 0, 0),
        host="https://api.3di.live",
    ):

        self.start_time = start_time
        # self._end_time = "set self.sim_duration first"
        self._sqlite_path = "call self.download_sqlite first"


        # not defined variables
        self._model = None
        self._template = None
        self._organisation = None
        self._output_folder = None

        config = {
            "THREEDI_API_HOST": host,
            "THREEDI_API_PERSONAL_API_TOKEN": api_key,
        }

        self.threedi_api = ThreediApi(config=config)
        self.threedi_api_beta = ThreediApi(config=config, version="v3-beta")

        self.data=None #set by calling .set_data()
        self.simulation=None
        self.simulation_created = False

    # @property
    # def end_time(self):
    #     return self._end_time

    # @end_time.setter
    # def end_time(self, sim_duration):
    #     self._end_time = self.start_time + datetime.timedelta(seconds=sim_duration)


    @property
    def end_time(self):
        return self.start_time + datetime.timedelta(seconds=self.sim_duration)


    @property
    def logged_in(self):
        call = self.threedi_api.auth_users_list()
        result = self._api_result(call, "Cannot login")
        return result

    @property
    def id(self):
        return self.simulation.id

    @property
    def model(self):
        return self._model

    def set_model(self, model_id):
        self._model=self.threedi_api.threedimodels_read(model_id)
        self.model_id = model_id

    @property
    def revision_id(self):
        try:
            return self.model.revision_id
        except:
            raise Exception("set model first (self.set_model) first (call .create)")

    @property
    def schema_id(self):
        try:
            return self.model.schematisation_id
        except:
            raise Exception("set model first (self.set_model) first (call .create)")

    @property 
    def output_folder(self):
        return self._output_folder

    @output_folder.setter
    def output_folder(self, value):
        self._output_folder=value

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
        self._template = self._api_result(call, "Template does not exist.")

    @property
    def sqlite_path(self):
        return self._sqlite_path

    @sqlite_path.setter
    def sqlite_path(self, sqlite_path):
        self._sqlite_path = sqlite_path
    # @property
    # def organisation(self):
    #     return self._organisation

    # @organisation.setter
    # def organisation_name(self, organisation_name):
    #     call = self.threedi_api.organisations_list(name=organisation_name)
    #     self._organisation = self._api_result(call, "Organisation does not exist.")

    # @organisation.setter
    # def organisation_id(self, organisation_id):
    #     call = self.threedi_api.organisations_read(organisation_id)
    #     self._organisation = self._api_result(call, "Organisation does not exist.")


    def add_default_settings(self):
        # add initial water (1d)
        self._add_to_simulation(
            self.threedi_api.simulations_initial1d_water_level_predefined_create,
            simulation_pk=self.id,
            data={},
            async_req=False,
        )

        self._add_to_simulation(
            self.threedi_api.simulations_settings_time_step_create,
            simulation_pk=self.id,
            data=self.data.time_step_settings,
        )

        self._add_to_simulation(
            self.threedi_api.simulations_settings_numerical_create,
            simulation_pk=self.id,
            data=self.data.numerical_settings,
        )

        self._add_to_simulation(
            self.threedi_api.simulations_settings_physical_create,
            simulation_pk=self.id,
            data=self.data.physical_settings,
        )


    def add_constant_rain(self):
        for rain_data in self.data.rain:
            self._add_to_simulation(
                self.threedi_api.simulations_events_rain_constant_create,
                simulation_pk=self.id,
                data=rain_data,
            )
    def check_structure_control(self, max_retries=10):
        
        i=0
        valid = False

        while (valid is False) or (i<max_retries):
            i+=1
            structure_control_list = self.threedi_api.simulations_events_structure_control_table_list(simulation_pk=self.id)

            valid = True
            for r in structure_control_list.results:
                for l in r.measure_specification.locations:
                    if l.state != "valid":
                        valid=False
            
            if not valid:
                print(f"waiting for structure control to become valid ({i}/{max_retries})")
                time.sleep(10)
                    
            
        if valid:
            print("structure control is valid")
            return True
        elif i > max_retries:
                print("max_retries exceeded (100 sec) for structure control to become valid, simulation did not start")
                return False
        else:
            print("this shouldnt happen..")
            return False    
           
            



    def add_structure_control(self):
        for control_data in self.data.structure_control:
            self._add_to_simulation(self.threedi_api.simulations_events_structure_control_table_create,
                        simulation_pk=self.id, data=control_data
                    )

    def add_laterals(self):
        """If  empty wont add."""
        for lateral in self.data.laterals:
            self._add_to_simulation(self.threedi_api.simulations_events_lateral_timeseries_create,
                            simulation_pk=self.id, data=lateral, async_req=False
                        )


    def add_boundaries(self):
        # Deels overgenomen van threedi_models_and_simulations
        def save_json(filepath, data):
            import json
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

        def upload_json(upload_result, filepath):
            """upload_result: instance returned by creating the file on the API"""
            with open(filepath, "rb") as file:
                response = requests.put(upload_result.put_url, data=file)
                return response
       
        filename = f"boundaryconditions_{self.model_id}.json"

        output_path = Path(os.path.join(self.output_folder, "tempfiles", filename))
        output_path.parent.parent.mkdir(exist_ok=True)
        output_path.parent.mkdir(exist_ok=True) #Create parent folder

        if not output_path.exists():
            save_json(output_path, self.data.boundaries)
        
        if self.data.boundaries == []:
            print("Info: Boundary file is empty, file not uploaded")
            return "Info: Boundary file is empty, file not uploaded"

        tc = ThreediCalls(threedi_api=self.threedi_api)

        UPLOAD_TIMEOUT = 45
        valid_states = ["processed", "valid"]


        if self.data.boundaries != []:

            bc_upload = self._add_to_simulation(self.threedi_api.simulations_events_boundaryconditions_file_create,
                        simulation_pk=self.id, data={'filename':filename}
            )
            upload_json(bc_upload, output_path)
            print(f"create: {output_path}")
            for ti in range(int(UPLOAD_TIMEOUT // 2)):
                uploaded_bc = tc.fetch_boundarycondition_files(self.id)[0]
                if uploaded_bc.state in valid_states:
                    print('\nUpload success')
                    break
                else:
                    print(f'Uploading {filename} ({ti}/{int(UPLOAD_TIMEOUT // 2)})', end='\r')
                    time.sleep(2)


    def add_basic_post_processing(self):
        self._add_to_simulation(self.threedi_api.simulations_results_post_processing_lizard_basic_create,
                simulation_pk=self.id, data=self.data.basic_processing
            )

    def add_damage_post_processing(self):
        self._add_to_simulation(self.threedi_api.simulations_results_post_processing_lizard_damage_create,
                simulation_pk=self.id, data=self.data.damage_processing
            )

    def add_arrival_post_processing(self):
        self._add_to_simulation(self.threedi_api.simulations_results_post_processing_lizard_arrival_create,
                simulation_pk=self.id, data=self.data.arrival_processing
            )

    def _add_to_simulation(self, func, **kwargs):
        """add something to simulation, if apiexcetion is raised sleep on it and try again."""
        while True:
            try:
                #check if data is dict
                if "data" in kwargs.keys():
                    if type(kwargs["data"]) != dict:
                        print("OJEE... data is geen dict {func}")
                        break

                r=func(**kwargs)
                return r
            except ApiException as e:
                self.error=e
                print(e)
                if self.error.status==400:
                    print(f"ERROR in {func}")
                    print(self.error.body)
                    break
                else: #TODO add error code of API overload
                    time.sleep(10)
                    continue
            break

    def _api_result(self,
        result: tac.openapi.models.inline_response20062.InlineResponse20062, message: str
    ) -> tac.openapi.models.threedi_model.ThreediModel:
        """Raises an error if no results"""
        if len(result.results) == 0:
            raise ValueError(message)
        return result.results[0]


    def create(self, output_folder, simulation_name, model_id, organisation_uuid, sim_duration):
        # data = {
        #     "template": self.template.id,
        #     "name": simulation_name,
        #     "threedimodel": self.model.id,
        #     "organisation": self.organisation._unique_id,
        #     "start_datetime": self.start,
        #     "end_datetime": self.end,
        # }
        # call = self.threedi_api_beta.simulations_from_template(data)
        # self.simulation = self._api_result(call, "Simulation is not yet processed")

        self.output_folder = output_folder
        self.sim_duration = sim_duration

        self.set_model(model_id=model_id)

        #Download the sqlite so we can retrieve some settings
        self.download_sqlite()

        #Create simulation on API
        data={  "name":simulation_name,
            	"threedimodel":self.model_id,
                "organisation":organisation_uuid,
                "start_datetime":self.start_time,
                "end_datetime":self.end_time,
                "store_results": True
                }

        self.simulation = self._add_to_simulation(self.threedi_api.simulations_create, data=data)
        self.simulation_created = True


    def download_sqlite(self, output_folder=None):
        """Download sqlite of selected model to temporary folder"""
        if output_folder is None:
            output_folder = self.output_folder
        else:
            self.output_folder = output_folder

        if output_folder is None:
            return "define self.output_folder first"

        if self.model is None:
            return "define self.model_id first"

        output_path = Path(os.path.join(output_folder, "tempfiles", f"model_{self.model_id}.zip"))
        output_path.parent.parent.mkdir(exist_ok=True)
        output_path.parent.mkdir(exist_ok=True) #Create parent folder
        if not output_path.with_suffix('').exists():
            if not output_path.exists():
                sqlite_dnwld = self._add_to_simulation(self.threedi_api.schematisations_revisions_sqlite_download,
                        id=int(self.revision_id), schematisation_pk=int(self.schema_id)
                )
                r = requests.get(sqlite_dnwld.get_url)
                with open(output_path, 'wb') as f:
                    f.write(r.content)

            #unpack zip
            zip_ref = zipfile.ZipFile(output_path, "r")
            zip_ref.extractall(output_path.with_suffix(''))
            zip_ref.close()

        self.sqlite_path = [i for i in output_path.with_suffix('').glob('*.sqlite')][0]


    def get_data(self, rain_data):
        """Load all data that should be added to the simulation"""
        self.data=SimulationData(sqlite_path=self.sqlite_path, #set  by calling .create (which calls .download_sqlite)
                sim_name=self.simulation.name, #set by calling .create
                sim_duration=self.sim_duration, #set by calling .create
                rain_data=rain_data
            )


    def start(self, extra_name=""):
        self.structure_control_valid = self.check_structure_control()
        if self.structure_control_valid:
            self.start_feedback = self._add_to_simulation(self.threedi_api.simulations_actions_create,
                                        simulation_pk=self.id, data={"name": "queue"}
            )

            # Create APIcall.txt file
            apicall_txt = os.path.join(self.output_folder, f"apicall{extra_name}_simid{self.simulation.id}_date{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
            with open(apicall_txt, "a") as t:
                t.write(self.simulation_info(str_type="text"))
        else:
            self.start_feedback="simulation_did not start"            


    def shutdown(self, simulation_pk):
        """stop simulation"""
        self.threedi_api.simulations_actions_create(simulation_pk,data={"name":"shutdown"})


    def simulation_info(self, str_type="text"):
        sim=self.simulation
        if str_type=="text":
            newline = "\n"
            return f"Simulation: {sim.url}{newline}Scenario name: {sim.name}\
            {newline}Organisation name: {sim.organisation_name}{newline}Duration: {sim.duration}s ({sim.duration/3600}h)\
            {newline}Rain events: {self.data.rain}{newline}Control structures count: {len(self.data.structure_control)}"

        if str_type=="html":
            newline = "<br>"
            return HTML(f"Simulation id: <a href={sim.url}>{sim.id}</a>{newline}Scenario name: {sim.name}\
                    {newline}Organisation name: {sim.organisation_name}{newline}Duration: {sim.duration}s ({sim.duration/3600}h)\
                    {newline}Rain events: {self.data.rain}{newline}Control structures count: {len(self.data.structure_control)} {newline}")


    def example_use(self, basic_processing=False, damage_processing=False, arrival_processing=False):
        
        if self.data is not None:
            self.add_default_settings()
            self.add_constant_rain()
            self.add_structure_control()
            self.add_laterals()
            self.add_boundaries()
            if basic_processing:
                self.add_basic_post_processing()
            if damage_processing:
                self.add_damage_post_processing()
            if arrival_processing:
                self.add_arrival_post_processing()
                #TODO aggregation

        else:
            print("Tried to add data to simulation that doesnt have data loaded yet.")


    #TODO reload created simulation and continue with that? 
    # Maybe remove the data that was added (should also do that with change simulation)
    # def load_simulation_data(self, ):
    #     SimulationData(sqlite_path=self.sqlite_path, 
    #                     sim_name, sim_duration, rain_data)



if __name__ == "__main__":
    self=Simulation(api_key="")
    # sim.model = "BWN Schermer interflow referentie #2"
    # self.set_model(49484)

    rain_data = [{'offset': 3600,
        'duration': 7200,
        'value': 4.930555555555556e-06,
        'units': 'm/s'}]

    # self.download_sqlite(output_folder='E:\\02.modellen\\model_test_v2\\03_3di_results\\0d1d_results\\hub_0d1d #6 1d2d_test')
    # self.create(output_folder='E:\\02.modellen\\model_test_v2\\03_3di_results\\0d1d_results\\hub_0d1d #6 1d2d_test',
    #                     simulation_name="hub test nr4", 
    #                     model_id=49484, 
    #                     organisation_uuid="48dac75bef8a42ebbb52e8f89bbdb9f2", 
    #                     sim_duration=10800)
    # # sim.template = "Referentie"

    # self.get_data(rain_data=rain_data)

    # self.example_use()
    # test = SimulationData(sqlite_path=self.sqlite_path, #set  by calling .create (which calls .download_sqlite)
    #             sim_name='hu', #set by calling .create
    #             sim_duration=self.sim_duration, #set by calling .create
    #             rain_data=rain_data
    #         )