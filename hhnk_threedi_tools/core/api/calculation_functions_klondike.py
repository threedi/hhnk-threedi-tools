#%%# First-party imports
import os
import time
import requests
from datetime import datetime, timedelta
from hhnk_threedi_tools.core.api.upload_model.threedi_calls import ThreediCalls


# Third-party imports
import numpy as np

import logging
import sqlite3
from IPython.core.display import display, HTML
from apscheduler.schedulers.blocking import BlockingScheduler
import threedi_api_client
from threedi_api_client.openapi import ApiException

from hhnk_threedi_tools.variables.api_settings import API_SETTINGS
import hhnk_research_tools as hrt

# local imports
from hhnk_threedi_tools.core.api.download_functions import (
    create_download_url,
    start_download,
)

# TODO remove
NUM_MIN = {
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


def secure_numerical(numerical_settings):
    for key, value in numerical_settings.items():
        if value == None and key in NUM_MIN:
            numerical_settings[key] = NUM_MIN[key]

    return numerical_settings


def clean_numerical(numerical_settings):

    for key, value in numerical_settings.items():
        if type(value).__module__ == np.__name__:
            numerical_settings[key] = value.item()  # np to python types
    return numerical_settings


def add_to_simulation(func, **kwargs):
    """add something to simulation, if apiexcetion is raised sleep on it and try again."""
    while True:
        try:
            func(**kwargs)
        except ApiException as e:
            print(e)
            time.sleep(10)
            continue
        break


def add_laterals_from_sqlite(sim, db_file, simulation):
    """
    Read 1D laterals from the Sqlite and use them in the initialisation of the simulation
    """
    try:
        for index, row in hrt.sqlite_table_to_df(
            database_path=db_file, table_name="v2_1d_lateral"
        ).iterrows():
            data = None

            connection_node = int(row["connection_node_id"])

            values = []

            for entry in row["timeseries"].splitlines():
                t = int(entry.split(",")[0]) * 60
                q = float(entry.split(",")[1])
                values.append([min(t, simulation.duration), q])

                if t > simulation.duration:
                    break

            data = {
                "values": values,
                "units": "m3/s",
                "connection_node": connection_node,
                "offset": 0,
            }

            while True:
                try:
                    if data is not None:
                        sim.threedi_api.simulations_events_lateral_timeseries_create(
                            simulation_pk=simulation.id, data=data, async_req=False
                        )
                except ApiException:
                    time.sleep(10)
                    continue
                break

    except Exception as e:
        print("Unable to read laterals from sqlite (or there are 0)")
        print(e)
        pass


def add_control_from_sqlite(sim, db_file, simulation):
    """
    Read table control structures from the Sqlite and use them in the initialisation of the simulation
    """
    # db_file = r"E:\02.modellen\23_Katvoed\02_schematisation\00_basis\bwn_katvoed.sqlite"
    try:
        # Assuming control_group_id = 1
        cntrl_group = hrt.execute_sql_selection(
            query="SELECT control_group_id FROM v2_global_settings",
            database_path=db_file,
        )
        # TODO gaat dit goed? ook bij meerdere global settings.
        for index, row in cntrl_group.iterrows():
            if row["control_group_id"] is None:
                return
            else:
                control_group_id = int(row["control_group_id"])

        v2_control = []
        v2_control_df = hrt.execute_sql_selection(
            query="SELECT * FROM v2_control WHERE control_group_id = {}".format(
                control_group_id
            ),
            database_path=db_file,
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
            database_path=db_file, table_name="v2_control_measure_map"
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
            database_path=db_file, table_name="v2_control_table"
        ).iterrows():
            action_table_string = row["action_table"]
            action_table = []
            action_type = row["action_type"]
            for entry in action_table_string.split("#"):
                measurement = [float(entry.split(";")[0])]
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
            "duration": simulation.duration,
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

        while True:
            try:
                sim.threedi_api.simulations_events_structure_control_table_create(
                    simulation_pk=simulation.id, data=data
                )
            except ApiException as e:
                if e.status == 429:
                    print("Api overload! Sleeping 1 minute")
                    time.sleep(60)
                    continue
                else:
                    raise
            break


def create_threedi_simulation(
    sim,
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
    use_structure_control,
    use_laterals,
    output_folder,
):  # , days_dry_start, hours_dry_start, days_rain, hours_rain, days_dry_end, hours_dry_end, rain_intensity, organisation_uuid, model_slug, scenario_name, store_results):
    """
    Creates and returns a Simulation (doesn't start yet) and initializes it with
    - initial water levels
    - boundary conditions
    - control structures
    - rainfall events
    - laterals
    """
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
        days=(days_dry_start + days_rain + days_dry_end + extra_days), hours=hours_end
    )  # TODO

    # create simulation api
    # threedi_sim_api = openapi_client.api.SimulationsApi(threedi_api_client)

    # TODO melden bij NenS namen in sqlite komen niet overeen met API
    # old:new
    translate_dict_numerical = {
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

    def update_dict_keys(mydict, translate_dict={}, remove_keys=[]) -> dict:
        """Rename dict keys and/or remove."""
        for key_old in translate_dict:
            key_new = translate_dict[key_old]
            if key_old in mydict:
                mydict[key_new] = mydict.pop(key_old)

        for key in remove_keys:
            if key in mydict:
                mydict.pop(key)

        return mydict

    global_setting_df = hrt.sqlite_table_to_df(
        database_path=sqlite_file, table_name="v2_global_settings"
    )
    global_setting = global_setting_df.iloc[0].to_dict()

    boundary_1d_settings_df = hrt.sqlite_table_to_df(
        database_path=sqlite_file, table_name="v2_1d_boundary_conditions"
    )

    numerical_setting_df = hrt.sqlite_table_to_df(
        database_path=sqlite_file, table_name="v2_numerical_settings"
    )
    numerical_setting_df.set_index("id", inplace=True)
    numerical_settings = numerical_setting_df.loc[
        global_setting["numerical_settings_id"]
    ].to_dict()

    numerical_settings = update_dict_keys(
        mydict=numerical_settings,
        translate_dict=translate_dict_numerical,
        remove_keys=["id"],
    )
    numerical_settings["flooding_threshold"] = 1e-5  # Not in sqlite?

    numerical_settings = secure_numerical(numerical_settings)

    numerical_settings = clean_numerical(numerical_settings)

    physical_settings = {
        "use_advection_1d": int(global_setting["advection_1d"]),
        "use_advection_2d": int(global_setting["advection_2d"]),
    }

    time_step_settings = {
        "time_step": global_setting["sim_time_step"],
        "min_time_step": global_setting["minimum_sim_time_step"],
        "max_time_step": global_setting["maximum_sim_time_step"],
        "use_time_step_stretch": bool(global_setting["timestep_plus"]),
        "output_time_step": global_setting["output_time_step"],
    }



    # set up simulation
    simulation = sim.threedi_api.simulations_create(
        threedi_api_client.openapi.models.simulation.Simulation(
            name=scenario_name,
            threedimodel=model_id,
            organisation=organisation_uuid,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )
    )

    # add initial water (1d)
    add_to_simulation(
        sim.threedi_api.simulations_initial1d_water_level_predefined_create,
        simulation_pk=simulation.id,
        data={},
        async_req=False,
    )

    if sqlite_file is not None:
        if use_structure_control:
            add_control_from_sqlite(sim, sqlite_file, simulation)
        if use_laterals:
            add_laterals_from_sqlite(sim, sqlite_file, simulation)


    add_to_simulation(
        sim.threedi_api.simulations_settings_time_step_create,
        simulation_pk=simulation.id,
        data=time_step_settings,
    )

    add_to_simulation(
        sim.threedi_api.simulations_settings_numerical_create,
        simulation_pk=simulation.id,
        data=numerical_settings,
    )

    add_to_simulation(
        sim.threedi_api.simulations_settings_physical_create,
        simulation_pk=simulation.id,
        data=physical_settings,
    )



    boundary_types =   {1: 'water_level', 
                        2: 'velocity',
                        3: 'discharge', 
                        5: 'sommerfeldt'
    }

    #1d boundary conditions
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
        

    data = []
    for index, row in boundary_1d_settings_df.iterrows():

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

    filename='boundaryconditions.json'
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
    filepath = os.path.join(output_folder,  filename)
    save_json(filepath, data)


    tc = ThreediCalls(threedi_api=sim.threedi_api)

    UPLOAD_TIMEOUT = 45
    valid_states = ["processed", "valid"]
    
    bc_upload = sim.threedi_api.simulations_events_boundaryconditions_file_create(simulation_pk=simulation.id,
                data={'filename':filename})
    upload_json(bc_upload, filepath)
    print(f"create: {filepath}")
    for ti in range(int(UPLOAD_TIMEOUT // 2)):
        uploaded_bc = tc.fetch_boundarycondition_files(simulation.id)[0]
        if uploaded_bc.state in valid_states:
            print('\nUpload success')
            break
        else:
            print(f'Uploading {filename} ({ti}/{int(UPLOAD_TIMEOUT // 2)})', end='\r')
            time.sleep(2)


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

    add_to_simulation(
        sim.threedi_api.simulations_events_rain_constant_create,
        simulation_pk=simulation.id,
        data=rain_data,
    )

    # Add postprocessing
    if basic_processing:
        basic_processing_data = {
            "scenario_name": scenario_name,
            "process_basic_results": True,
        }

        add_to_simulation(
            sim.threedi_api.simulations_results_post_processing_lizard_basic_create,
            simulation_pk=simulation.id,
            data=basic_processing_data,
        )

    # Damage posprocessing
    if damage_processing:
        damage_processing_data = API_SETTINGS["damage_processing"]

        add_to_simulation(
            sim.threedi_api.simulations_results_post_processing_lizard_damage_create,
            simulation_pk=simulation.id,
            data=damage_processing_data,
        )

    # Arrival time
    if arrival_processing:
        arrival_processing_data = {"arrival_time": True} #TODO check if works? was: {"basic_post_processing": True}
        add_to_simulation(
            sim.threedi_api.simulations_results_post_processing_lizard_arrival_create,
            simulation_pk=simulation.id,
            data=arrival_processing_data,
        )

    # return simulation object
    return simulation


def create_3Di_start_API_call_data(
    days_dry_start,
    hours_dry_start,
    days_rain,
    hours_rain,
    days_dry_end,
    hours_dry_end,
    rain_intensity,
    organisation_uuid,
    model_slug,
    scenario_name,
    store_results,
):
    """Creates the rain_events dict for the API call, puts this in the datajson
    example params:

    days_dry_start = rain_event_widget.children[0].value
    hours_dry_start = int(rain_event_widget.children[1].value)
    days_rain = rain_event_widget.children[2].value
    hours_rain = int(rain_event_widget.children[3].value)
    days_dry_end = rain_event_widget.children[4].value
    hours_dry_end = int(rain_event_widget.children[5].value)
    rain_intensity = rain_event_widget.children[6].value
    organisation_uuid = API_SETTINGS['org_uuid'][organisation_box.value]
    model_slug = model_slug_widget.value
    scenario_name = scenario_name_widget.value
    store_results = API_SETTINGS['store_results'])

    """

    days_dry_start, hours_dry_start, days_rain, hours_rain, days_dry_end, hours_dry_end, rain_intensity
    # add extra day if hours rain are over a day.
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

    # calculate rain event
    start_rain_days = str(int(days_dry_start) + 1).zfill(2)
    end_rain_days = str(
        int(days_dry_start) + int(days_rain) + extra_days_rain + 1
    ).zfill(2)
    end_som_days = str(
        int(days_dry_start) + int(days_rain) + int(days_dry_end) + extra_days + 1
    ).zfill(2)

    # Define rain event for API call
    rain_events = [
        {
            "type": "constant",
            "intensity": round(float(rain_intensity), 3),  # mm/hour
            "active_from": "2000-01-"
            + start_rain_days
            + "T"
            + str(hours_dry_start).zfill(2)
            + ":00",
            "active_till": "2000-01-"
            + end_rain_days
            + "T"
            + str(hours_end_rain).zfill(2)
            + ":00",
        }
    ]

    # Put all variables in one dictionary that can be passed to the API
    data = {
        "organisation_uuid": organisation_uuid,
        "model_slug": model_slug,
        "start": "2000-01-01T00:00",
        "end": "2000-01-" + end_som_days + "T" + str(hours_end).zfill(2) + ":00",
        "scenario_name": scenario_name,
        "rain_events": rain_events,
        "store_results": store_results,
    }
    return data


def start_3di_calculation(
    data, data_json, username, passw, output_folder, apicall_txt, batch=0
):
    """Call the 3Di API to start a calculation."""

    r = requests.post(
        "https://3di.lizard.net/api/v1/calculation/start/",
        data=data_json,
        auth=(username, passw),
        headers={"Content-Type": "application/json"},
    )

    if batch == 0:
        print("\nCalculation has (hopefully) started running")
        display(
            HTML(
                "<a href={} target='_blank'>Check progress on lizard</a>".format(
                    "https://3di.lizard.net/"
                )
            )
        )

        # Create APIcall.txt and folder for model and revision_nr.
        # Create outputfolder for model with revision_nr
        if not os.path.exists(output_folder) and output_folder != "":
            os.mkdir(output_folder)
            print("Created folder: " + output_folder.rsplit("/")[-1])

        # Create APIcall.txt file
        if not os.path.exists(apicall_txt):
            with open(apicall_txt, "a") as t:
                t.write("{")
                for key in [
                    "organisation_uuid",
                    "model_slug",
                    "start",
                    "end",
                    "scenario_name",
                    "rain_events",
                ]:
                    #                 for key in data.keys():
                    t.write("\n" + key + ":" + str(data[key]))
                t.write("\n}")
        else:
            print(
                "APIcall.txt was already in the folder. Check if everything was run according to plan"
            )

    # Display results of the api call to user
    try:
        display(r.content)
    except:
        pass


def wait_to_download_results(
    dl, scenario_name, polder_name, revision_nr, output_folder, logger_path, wait_time=5
):
    """This function checks the API every x minutes for new results. Once the results appear, the download will start."""

    def check_API_for_update():
        print(
            "{} - Checking API for update".format(
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        )

        # Check the api by searching for results.
        results = dl.find_scenarios(
            name=scenario_name,
            model_name=polder_name,
            model_revision=int(revision_nr),
            limit=1000,
        )

        # See all names that are returned
        scenario_names = [a["name"] for a in results]

        # write scenario names to logger
        logger.error("")
        logger.warning("polder: " + str(polder_name) + "revision: " + str(revision_nr))
        try:
            logger.warning("results: " + str(results))
        except:
            pass
        logger.warning("scenario_names: " + str(scenario_names))

        # Execute the job till the count of 5
        if scenario_name in scenario_names:
            # Find the id of the download url
            scenario_ids = [
                scenario_names.index(scenario_name)
            ]  # id's of selected models to download
            download_dict = {}
            download_dict["download_url"] = create_download_url(
                results, scenario_ids
            )  # url to all download links.

            print("Results were found! Proceeding to download.")

            # Call download script.
            name = scenario_name

            print("\n\033[1m\033[31mDownloading files for " + name + ":\033[0m")
            for index, url in enumerate(download_dict["download_url"][name]):
                print("{}: {}".format(index + 1, url))

            # Print destination folder
            print("\nThey will be placed in:\n" + output_folder)

            # Create destination folder
            if not os.path.exists(output_folder) and output_folder != "":
                os.mkdir(output_folder)

            # Start downloading of the files
            start_download(
                download_dict["download_url"][name],
                output_folder,
                dl.get_headers(),
                automatic_download=1,
            )

            # Stop the scheduler
            scheduler.shutdown(wait=False)

    scheduler = BlockingScheduler(timezone="Europe/Amsterdam")
    scheduler.add_job(check_API_for_update, "interval", minutes=wait_time)

    # create logger
    logger = logging.getLogger()
    handler = logging.FileHandler(logger_path)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    print(
        "{} - Checking API for update started".format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
    )
    scheduler.start()  # Start the scheduled job


if __name__ == "__main__":
    #Test
    from hhnk_threedi_tools.core.api.calculation import Simulation
    sim = Simulation("")
    sqlite_file = r"E:\02.modellen\Heemkerkerdui_Recalculation_v3\02_schematisation\00_basis\Heemkerkerduin_Recalculatio22.sqlite"
    scenario_name = "test"
    model_id = 3946
    organisation_uuid  = "48dac75b-ef8a-42eb-bb52-e8f89bbdb9f2"

    # create_threedi_simulation(
    #     sim,
    #     sqlite_file,
    #     scenario_name,
    #     model_id,
    #     organisation_uuid,
    # )

#     translate_dict_numerical = {
#         'frict_shallow_water_correction':'friction_shallow_water_depth_correction',
#         'integration_method':'time_integration_method',
#         'limiter_grad_1d':'limiter_waterlevel_gradient_1d',
#         'limiter_grad_2d':'limiter_waterlevel_gradient_2d',
#         'max_nonlin_iterations':'max_non_linear_newton_iterations',
#         'max_degree':'max_degree_gauss_seidel',
#         'minimum_friction_velocity':'min_friction_velocity',
#         'minimum_surface_area':'min_surface_area',
#         'precon_cg':'use_preconditioner_cg',
#         'thin_water_layer_definition':'limiter_slope_thin_water_layer',
#         'use_of_nested_newton':'use_nested_newton',
#         }


#     def update_dict_keys(mydict, translate_dict={}, remove_keys=[]) -> dict:
#         """Rename dict keys and/or remove."""
#         for key_old in translate_dict:
#             key_new=translate_dict[key_old]
#             if key_old in mydict:
#                 mydict[key_new] = mydict.pop(key_old)

#         for key in remove_keys:
#             if key in mydict:
#                 mydict.pop(key)

#         return mydict


#     global_setting_df = hrt.sqlite_table_to_df(database_path=sqlite_file,
#                         table_name='v2_global_settings')
#     global_setting = global_setting_df.iloc[0].to_dict()

#     numerical_setting_df = hrt.sqlite_table_to_df(database_path=sqlite_file,
#                         table_name='v2_numerical_settings')
#     numerical_setting_df.set_index('id', inplace=True)
#     numerical_settings = numerical_setting_df.loc[global_setting['numerical_settings_id']].to_dict()

#     numerical_settings = update_dict_keys(mydict=numerical_settings,
#                         translate_dict = translate_dict_numerical,
#                         remove_keys=['id'])
#     numerical_settings["flooding_threshold"] = 1e-5 #Not in sqlite?
#     numerical_settings["use_nested_newton"] = bool(numerical_settings["use_nested_newton"])
#     numerical_settings = secure_numerical(numerical_settings)

#     physical_settings = {
#     "use_advection_1d": global_setting['advection_1d'],
#     "use_advection_2d": global_setting['advection_2d']
#     }

#     time_step_settings = {
#     "time_step": global_setting['sim_time_step'],
#     "min_time_step": global_setting['minimum_sim_time_step'],
#     "max_time_step": global_setting['maximum_sim_time_step'],
#     "use_time_step_stretch": bool(global_setting['timestep_plus']),
#     "output_time_step": global_setting['output_time_step']
#     }

#     # set up simulation
#     simulation = sim.threedi_api.simulations_create(
#         threedi_api_client.openapi.models.simulation.Simulation(
#             name=scenario_name,
#             threedimodel=model_id,
#             organisation="48dac75bef8a42ebbb52e8f89bbdb9f2",
#             start_datetime=datetime(2000,1,1),
#             end_datetime=datetime(2000,1,10),
#         )
#     )
#     # add initial water (1d)
#     add_to_simulation(sim.threedi_api.simulations_initial1d_water_level_predefined_create,
#             simulation_pk=simulation.id,
#             data={}, async_req=False)

#     if sqlite_file is not None:
#         # add control structures (from sqlite)
#         add_control_from_sqlite(sim, sqlite_file, simulation)
#         add_laterals_from_sqlite(sim, sqlite_file, simulation)


#     add_to_simulation(sim.threedi_api.simulations_settings_time_step_create,
#             simulation_pk=simulation.id,
#             data=time_step_settings)

#     add_to_simulation(sim.threedi_api.simulations_settings_numerical_create,
#             simulation_pk=simulation.id,
#             data=numerical_settings)

#     add_to_simulation(sim.threedi_api.simulations_settings_physical_create,
#             simulation_pk=simulation.id,
# data=physical_settings)

# %%
