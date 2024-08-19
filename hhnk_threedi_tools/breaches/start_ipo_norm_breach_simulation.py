# %% Script voor het aanzetten van bresdoorbraken vanuit de Amstelmeerboezem

# Importing
import datetime
import os
import geopandas as gpd
import hhnk_research_tools as hrt
from threedi_api_client.openapi import ApiException
from threedi_api_client.api import ThreediApi
from threedi_api_client.versions import V3Api
import time
import find_upstream_links_to_breach_short as fulb_short
from pathlib import Path


# Loggin code.
class ModelFolder(hrt.Folder):
    def __init__(self, base):
        super().__init__(base)

        self.add_file("schema", rf"work in progress/schematisation/{self.name}.gpkg")


def start_ipo_norm_breach_simulation(
    testresult_folder,
    organisation_name,
    simulation_name,
    modeller_initial,
    filter_id,
    wait_time,
):
    api_keys_path = rf"{os.getenv('APPDATA')}\3Di\QGIS3\profiles\default\python\plugins\hhnk_threedi_plugin\api_key.txt"

    api_keys = hrt.read_api_file(api_keys_path)
    # Loggin code.
    config = {
        "THREEDI_API_HOST": "https://api.3di.live",
        # "THREEDI_API_USERNAME": "j.acostabarragan",
        # "THREEDI_API_PASSWORD": getpass(),
        "THREEDI_API_PERSONAL_API_TOKEN": api_keys["threedi"],
    }
    # api_client = ThreediApiClient(config=config)
    api_client: V3Api = ThreediApi(config=config, version="v3-beta")

    # Loggin Confirmation Message
    try:
        user = api_client.auth_profile_list()
    except ApiException:
        print("Oops, something went wrong. Maybe you made a typo?")
    else:
        print(f"Successfully logged in as {user.username}!")

    # organisation_name = 'BWN HHNK'

    sim_duration = 4  # days
    breach_duration = 2  # days
    start_datetime = datetime.datetime(2000, 1, 1, 0, 0)
    output_timestep = 900  # s

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
        "flooding_threshold": 1e-5,
    }

    physical_settings = {"use_advection_1d": 1, "use_advection_2d": 1}

    time_step_settings = {
        "time_step": 5,
        "min_time_step": 0.01,
        "max_time_step": 5,
        "use_time_step_stretch": False,
        "output_time_step": output_timestep,
    }

    aggregation_settings = [
        {
            "name": "",
            "flow_variable": "discharge",
            "method": "avg",
            "interval": output_timestep,
        },
        {
            "name": "",
            "flow_variable": "water_level",
            "method": "avg",
            "interval": output_timestep,
        },
        {
            "name": "",
            "flow_variable": "water_level",
            "method": "max",
            "interval": output_timestep,
        },
        {
            "name": "",
            "flow_variable": "discharge",
            "method": "max",
            "interval": output_timestep,
        },
        {
            "name": "",
            "flow_variable": "flow_velocity",
            "method": "avg",
            "interval": output_timestep,
        },
        {
            "name": "",
            "flow_variable": "flow_velocity",
            "method": "max",
            "interval": output_timestep,
        },
    ]

    # Find organisation uuid
    organisations = api_client.organisations_list(name__istartswith=organisation_name)
    org_uuid = organisations.results[0].unique_id

    # Search for the model we want to work with.
    model_list = api_client.threedimodels_list(name__icontains=model_name)
    # 0 will select the newest one
    my_model = model_list.results[0]
    # get the id of the model
    my_model_id = my_model.id
    my_model_revision = my_model.revision_number

    my_model_schema_id = my_model.schematisation_id
    model_versie = (
        f"Schematisation id {my_model_schema_id} - Revision #{my_model_revision}"
    )

    print("model id:", my_model_id, " rev: ", my_model_revision)

    # Find the breaches in the model
    potential_breaches = api_client.threedimodels_potentialbreaches_list(
        my_model.id, limit=9999
    )
    number_breaches = potential_breaches.count

    # locate the breach result that corresponds to the connected point id
    specific_breaches = []
    breach_id = []

    for pnt_id in range(number_breaches):
        id = potential_breaches.results[pnt_id].connected_pnt_id
        if not filter_id:
            breach_id.append(id)
            specific_breaches.append(pnt_id)
        elif id in filter_id:
            # if (1<= id <=3) or (5<= id <=74):
            breach_id.append(id)
            specific_breaches.append(pnt_id)

    # Find breached based on given scenario
    upstream = fulb_short.find_upstream_links_to_breach(testresult_folder, breach_id)

    # Set up simulations
    sleeptime = 2
    breach = {}
    upstream_data = {}

    metadata_gdf = gpd.read_file(metadata_path, driver="Shapefile")
    # Start simulations in a loop
    for x in specific_breaches:
        # Select breaches
        breach = potential_breaches.results[x]
        print(f"Starting simulation for breach {breach}")

        # find upstream link data (can this be done without a loop?)
        for b in upstream["data"]:
            if breach.connected_pnt_id == b["breach_id"]:
                upstream_data = b

        # set up name of simulation
        simulation_name_new = (
            simulation_name + "_" + str(breach.connected_pnt_id) + modeller_initial
        )

        # Components of the simulation created. IS NOT RUNNING YET. See the status below

        simulation_template = api_client.simulation_templates_list(
            simulation__threedimodel__id=my_model_id
        ).results[0]
        simulation = api_client.simulations_create(
            data={
                "template": simulation_template.id,
                "name": simulation_name_new,
                "tags": "ipo",
                "threedimodel": my_model.id,
                "organisation": org_uuid,
                "start_datetime": start_datetime,
                "duration": sim_duration * 3600 * 24,  # in seconds
            }
        )
        time.sleep(sleeptime)

        api_client.simulations_initial1d_water_level_predefined_create(
            simulation_pk=simulation.id, data={}, async_req=False
        )
        time.sleep(sleeptime)

        # Set up numerical setting
        api_client.simulations_settings_time_step_create(
            simulation_pk=simulation.id, data=time_step_settings
        )
        time.sleep(sleeptime)
        api_client.simulations_settings_numerical_create(
            simulation_pk=simulation.id, data=numerical_setting
        )
        time.sleep(sleeptime)
        api_client.simulations_settings_physical_create(
            simulation_pk=simulation.id, data=physical_settings
        )
        time.sleep(sleeptime)
        for a in aggregation_settings:
            api_client.simulations_settings_aggregation_create(
                simulation_pk=simulation.id, data=a
            )
            time.sleep(sleeptime)

        # Set breach event open
        breach_event = api_client.simulations_events_breaches_create(
            simulation.id,
            data={
                "potential_breach": breach.id,
                "duration_till_max_depth": 600,
                "maximum_breach_depth": 100,
                "initial_width": 10,
                "offset": 600,
            },
        )
        time.sleep(sleeptime)

        # Set event closing upstream chanels and structures
        for u in range(len(upstream_data["upstream_links"])):
            # print(u)
            # print(upstream_data['upstream_types'][u])
            if (
                upstream_data["upstream_types"][u] == "v2_added_c"
                or upstream_data["upstream_types"][u] == "v2_breach"
                or upstream_data["upstream_types"][u] == ""
            ):
                continue
            elif upstream_data["upstream_types"][u] == "v2_channel":
                api_client.simulations_events_structure_control_timed_create(
                    simulation.id,
                    data={
                        "offset": breach_duration * 3600 * 24,
                        "duration": sim_duration * 3600 * 24,
                        "value": [0.0, 0.0],
                        "type": "set_discharge_coefficients",
                        "grid_id": int(upstream_data["upstream_links"][u]),
                        "structure_type": upstream_data["upstream_types"][u],
                    },
                )
            else:
                api_client.simulations_events_structure_control_timed_create(
                    simulation.id,
                    data={
                        "offset": breach_duration * 3600 * 24,
                        "duration": sim_duration * 3600 * 24,
                        "value": [0.0, 0.0],
                        "type": "set_discharge_coefficients",
                        "structure_id": int(upstream_data["spatialite_ids"][u]),
                        "structure_type": upstream_data["upstream_types"][u],
                    },
                )
            time.sleep(sleeptime)

        # add postprocessing
        basic_processing_data = {
            "scenario_name": simulation_name_new,
            "process_basic_results": True,
        }
        api_client.simulations_results_post_processing_lizard_basic_create(
            simulation.id, data=basic_processing_data
        )
        time.sleep(sleeptime)
        api_client.simulations_results_post_processing_lizard_arrival_create(
            simulation_pk=simulation.id, data={}
        )

        time.sleep(sleeptime)

        api_client.simulations_results_post_processing_lizard_damage_create(
            simulation_pk=simulation.id,
            data={
                "basic_post_processing": basic_processing_data,
                "cost_type": "avg",  # 2,
                "flood_month": "sep",  # 9,
                "inundation_period": 48,
                "repair_time_infrastructure": 120,
                "repair_time_buildings": 240,
            },
        )

        time.sleep(sleeptime)

        datum_str = datetime.datetime.now().date().strftime("%d-%m-%y")

        # update metadata
        metadata_gdf.loc[metadata_gdf["SC_NAAM"] == simulation_name_new, "SC_IDENT"] = (
            simulation.id
        )
        metadata_gdf.loc[metadata_gdf["SC_NAAM"] == simulation_name_new, "ID"] = (
            breach.line_id
        )
        metadata_gdf.loc[metadata_gdf["SC_NAAM"] == simulation_name_new, "SC_DATE"] = (
            datum_str
        )
        metadata_gdf.loc[
            metadata_gdf["SC_NAAM"] == simulation_name_new, "MOD_VERSIE"
        ] = model_versie

        metadata_gdf.to_file(metadata_path, driver="Shapefile")

        # Start simulation
        queue_jam_bwn = True

        while queue_jam_bwn:
            queue_length_bwn = api_client.statuses_list(
                # TODO org_uuid hier gebruiken. want daar zet je de berekening op
                name__startswith="queued",
                simulation__organisation__unique_id=org_uuid,
            ).count
            if queue_length_bwn < 2:
                queue_jam_bwn = False
                api_client.simulations_actions_create(
                    simulation.id, data={"name": "queue"}
                )
                print(f"{simulation_name_new} is queued with id {simulation.id}")
            else:
                print("wait", wait_time, "s")
                time.sleep(wait_time)


# %%
if __name__ == "__main__":
    # Use organisation_name 'BWN HHNK' for standard simulation. Use the other one for very specific cases

    # Set your initial names
    modeller_initial = "_JA"

    # Define a simulation name prefix for all simulation
    simulation_name = "IPO_OPEN_SBLN"

    # Use organisation_name 'BWN HHNK' for standard simulation. Use the other one for very specific cases
    organisation_name = "BWN HHNK"

    # Set the model name as it is either in 3di or in the local folder.
    base_folder = r"E:\02.modellen"

    # Define model name to set up its location
    model_name = "RegionalFloodModel - deelmodel Schermer Laag Noord"
    model_folder = ModelFolder(rf"{base_folder}\{model_name}")

    # Set up the test location - This is a simulation that need to be done before starting the normal simulation.
    # The test, is needed to close the breaches. The test could be a simple 1 hour 1d model run. Once it is done,
    # the test must be stored within the model location.

    # name from this revision that has some breaches for closing upstream channels
    name = "TEST_OPEN_IPO_SBLN_V4"
    testresult_folder = os.path.join(model_folder.path, name)

    # id_filter corresponds to the column 'id' of the potential breach table of the model we are working with.
    # In case of willing to run all the potential breach, leave the list empty  --> filter_id = []
    filter_id = [50, 80]

    # location of the metadata file. Important to have at least 2 version: One for uploading and run model and the other one for downloading.
    metadata_path = Path(
        r"E:\03.resultaten\Normering Regionale Keringen\metadata\test\metadata_shapefile.shp"
    )

    # Time (in seconds) to wait until the script tries again to upload a model. We use it to not overload the API.
    wait_time = 3600  # 1  hour

    start_ipo_norm_breach_simulation(
        testresult_folder,
        organisation_name,
        simulation_name,
        modeller_initial,
        filter_id,
        wait_time,
    )

# %%
