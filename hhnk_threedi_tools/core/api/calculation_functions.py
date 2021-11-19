# First-party imports
import os
import time
import requests
from datetime import datetime, timedelta


# Third-party imports
import logging
import sqlite3
from IPython.core.display import display, HTML
from apscheduler.schedulers.blocking import BlockingScheduler
from threedi_api_client import ThreediApiClient
import openapi_client
from openapi_client import ApiException

from hhnk_threedi_tools.variables.api_settings import API_SETTINGS

# local imports
from .download_functions import create_download_url, start_download


def add_laterals_from_sqlite(threedi_sim_api, db_file, simulation):
    """
    Read 1D laterals from the Sqlite and use them in the initialisation of the simulation
    """
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    try:
        for row in c.execute("SELECT * FROM v2_1d_lateral"):
            data = None

            connection_node = int(row[1])

            values = []

            for entry in row[2].splitlines():
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
                        threedi_sim_api.simulations_events_lateral_timeseries_create(
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
    conn.close()


def add_control_from_sqlite(threedi_sim_api, db_file, simulation):
    """
    Read table control structures from the Sqlite and use them in the initialisation of the simulation
    """
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    try:
        # Assuming control_group_id = 1
        for row in c.execute("SELECT control_group_id FROM v2_global_settings"):
            if row[0] is None:
                return
            else:
                control_group_id = int(row[0])

        v2_control = []
        for row in c.execute(
            "SELECT * FROM v2_control WHERE control_group_id = {}".format(
                control_group_id
            )
        ):
            data = {
                "id": int(row[7]),
                "control_type": row[2],
                "control_id": int(row[3]),
                "control_group_id": int(row[4]),
                "measure_group_id": int(row[6]),
            }
            v2_control.append(data)

        v2_control_measure_map = []
        for row in c.execute("SELECT * FROM v2_control_measure_map"):
            data = {
                "id": int(row[0]),
                "measure_group_id": int(row[1]),
                "object_type": row[2],
                "object_id": int(row[3]),
                "weight": int(row[4]),
            }
            v2_control_measure_map.append(data)

        v2_control_table = []
        for row in c.execute("SELECT * FROM v2_control_table"):
            action_table_string = row[0]
            action_table = []
            action_type = row[5]
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
                "id": row[6],
                "action_table": action_table,
                "measure_operator": measure_operator,
                "target_id": row[2],
                "target_type": row[3],
                "measure_variable": row[4],
                "action_type": action_type,
            }
            v2_control_table.append(data)

    except:
        print("Unable to read control from sqlite (or there are 0)")
        raise

    conn.close()
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
                threedi_sim_api.simulations_events_structure_control_table_create(
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
    threedi_api_client,
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
    arrival_processing
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
    threedi_sim_api = openapi_client.api.SimulationsApi(threedi_api_client)

    # set up simulation
    simulation = threedi_sim_api.simulations_create(
        openapi_client.models.simulation.Simulation(
            name=scenario_name,
            threedimodel=model_id,
            organisation=organisation_uuid,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )
    )

    # add initial water (1d)

    while True:
        try:
            threedi_sim_api.simulations_initial1d_water_level_predefined_create(
                simulation_pk=simulation.id, data={}, async_req=False
            )
        except ApiException:
            time.sleep(10)
            continue
        break

    if sqlite_file is not None:
        # add control structures (from sqlite)
        add_control_from_sqlite(threedi_sim_api, sqlite_file, simulation)
        add_laterals_from_sqlite(threedi_sim_api, sqlite_file, simulation)


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

    while True:
        try:
            threedi_sim_api.simulations_events_rain_constant_create(
                simulation.id, rain_data
            )
        except ApiException:
            time.sleep(10)
            continue
        break

    # Add postprocessing
    if basic_processing:
        basic_processing_data = {
            "scenario_name": scenario_name,
            "process_basic_results": True,
        }
        while True:
            try:
                threedi_sim_api.simulations_results_post_processing_lizard_basic_create(
                    simulation.id, data=basic_processing_data
                )
            except ApiException:
                time.sleep(10)
                continue
            break

    #Damage posprocessing
    if damage_processing:
        damage_processing_data = API_SETTINGS["damage_processing"]
        while True:
            try:
                threedi_sim_api.simulations_results_post_processing_lizard_damage_create(
                    simulation.id, data=damage_processing_data
                )
            except ApiException:
                time.sleep(10)
                continue
            break

    if arrival_processing:
        arrival_processing_data = {"basic_post_processing": True}
        #Arrival time 
        while True:
            try:
                threedi_sim_api.simulations_results_post_processing_lizard_arrival_create(
                    simulation.id, data=arrival_processing_data
                )
            except ApiException:
                time.sleep(10)
                continue
            break

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
