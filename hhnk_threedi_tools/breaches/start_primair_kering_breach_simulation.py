"""
Python script to run multiple breaches separeted. It requieres a metadata shapefile/geopackge to save the
simulations key values in it. if it requiere to set the scenario_name diferently you will need to change it
within the function.
"""
# %%

import datetime
import os
import time
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
from threedi_api_client.api import ThreediApi
from threedi_api_client.openapi import ApiException
from threedi_api_client.versions import V3Api

from hhnk_threedi_tools.core.api.calculation import Simulation


class ModelFolder(hrt.Folder):
    def __init__(self, base):
        super().__init__(base)

        self.add_file("schema", rf"work in progress/schematisation/{self.name}.gpkg")


def start_simulation_breaches(model_folder, organisation_name, scenarios, filter_id, metadata_path, wait_time):
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
    # modeller_initial = '_JA'
    sim_duration = 20  # days
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
        "max_time_step": 10,
        "use_time_step_stretch": True,
        "output_time_step": output_timestep,
    }

    aggregation_settings = [
        {"name": "", "flow_variable": "discharge", "method": "avg", "interval": output_timestep},
        {"name": "", "flow_variable": "water_level", "method": "avg", "interval": output_timestep},
        {"name": "", "flow_variable": "water_level", "method": "max", "interval": output_timestep},
        {"name": "", "flow_variable": "discharge", "method": "max", "interval": output_timestep},
        {"name": "", "flow_variable": "flow_velocity", "method": "avg", "interval": output_timestep},
        {"name": "", "flow_variable": "flow_velocity", "method": "max", "interval": output_timestep},
    ]

    # Find organisation uuid
    organisations = api_client.organisations_list(name__istartswith=organisation_name)
    org_uuid = organisations.results[0].unique_id

    # Search for the model we want to work with.
    model_list = api_client.threedimodels_list(name__contains=model_folder.name)
    results = model_list.results
    first_value = True

    for result in results:
        if result.schematisation_name == model_folder.name and first_value:
            first_value = False
            # get the id of the model
            my_model_id = result.id
            my_model_revision = result.revision_number
            my_model_schema_id = result.schematisation_id
            model_versie = f"Schematisation id {my_model_schema_id} - Revision #{my_model_revision}"
            print("model name:", result.name, "model id:", my_model_id, " rev: ", my_model_revision)

    # Find the breaches in the model
    potential_breaches = api_client.threedimodels_potentialbreaches_list(my_model_id, limit=9999)
    # potential_breach_gdf = gpd.read_file(model_folder.schema.path, layer="potential_breach")
    gpkg = model_folder.path / "work in progress" / "schematisation" / "midden_noord_isolated.gpkg"
    potential_breach_gdf = gpd.read_file(gpkg, layer="potential_breach")

    display_names = potential_breach_gdf.display_name.values
    breach_ids_scenario = []
    # for display_name in display_names:
    #     for scenario in scenarios:
    #         # if display_name.split("-")[-1] == str(scenario):
    #         # active_breach = potential_breach_gdf[potential_breach_gdf["display_name"] == display_name]
    #         # breach_ids_scenario.append(active_breach.id.values[0])
    #         breach_ids_scenario.append(potential_breach_gdf.id)

    # Select the breach id from de geopackge to be use later to select the connected_point
    id_filter = []
    if not filter_id:
        id_filter = breach_ids_scenario
    else:
        id_filter = filter_id

    breach_selected_idxs = []

    # get conntected_point_id from the API using breach_id as identifier.
    for idx in range(potential_breaches.count):
        connected_pnt_id = potential_breaches.results[idx].connected_pnt_id
        if connected_pnt_id in id_filter:
            # if (1<= id <=3) or (5<= id <=74):
            breach_selected_idxs.append(idx)

    # Set up simulations
    sleeptime = 2
    breach = {}
    # Start simulations in a loop
    # set up metadata

    if os.path.exists(metadata_path):
        metadata_gdf = gpd.read_file(metadata_path, driver="Shapefile")
    else:
        metadata_columns = ["SC_NAAM", "SC_IDENT", "ID", "SC_DATE", "MOD_VERSIE", "geometry"]

        metadata_gdf = gpd.GeoDataFrame(
            columns=metadata_columns,
            geometry="geometry",
            crs=potential_breach_gdf.crs,  # asumimos que existe
        )

    for x in breach_selected_idxs:
        # if x >= 32: # DEBUGGING
        breach = potential_breaches.results[x]
        # print (x)
        print(breach.connected_pnt_id)
        # set simulation time and fix format.

        # FIXME dit is de nieuwe standaard. Voor implementeren, eerste alle data uit metadata.SC_DATE goed zetten.
        # Volgen LDO we moeten de format van de datum al d-m-y blijven anders gaan we en bericht fouten krijgen

        datum_str = datetime.datetime.now().date().strftime("%d-%m-%y")
        # datum_str = datetime.datetime.now().date().strftime("%y-%m-%d")

        # select active breach according to connected point id from API
        breach_row = potential_breach_gdf[potential_breach_gdf["id"] == breach.connected_pnt_id].iloc[0]

        # Set scenario name according to active breach
        breach_code_split = breach_row.code.split("-")
        if breach_code_split[0][-2:] == "_1":
            # All codes have _1 at the end of name. We dont know why this is here, but remove it.
            scenario_name = f"ROR-PRI-{breach_code_split[0][:-2]}-T{breach_code_split[1]}"
        else:
            raise ValueError("A scenario_name may have been used for the wrong breach in the past. Please check this.")

        # Components of the simulation created. IS NOT RUNNING YET. See the status below
        simulation_template = api_client.simulation_templates_list(simulation__threedimodel__id=my_model_id).results[0]
        simulation = api_client.simulations_from_template(
            data={
                "template": simulation_template.id,
                "name": scenario_name,
                "tags": ["ROR"],
                "threedimodel": my_model_id,
                "organisation": org_uuid,
                "start_datetime": start_datetime,
                "duration": sim_duration * 3600 * 24,  # in seconds
            }
        )
        time.sleep(sleeptime)

        api_client.simulations_settings_time_step_delete(simulation_pk=simulation.id)
        api_client.simulations_settings_time_step_create(simulation_pk=simulation.id, data=time_step_settings)
        time.sleep(sleeptime)

        api_client.simulations_settings_numerical_delete(simulation_pk=simulation.id)
        api_client.simulations_settings_numerical_create(simulation_pk=simulation.id, data=numerical_setting)
        time.sleep(sleeptime)

        api_client.simulations_settings_physical_delete(simulation_pk=simulation.id)
        api_client.simulations_settings_physical_create(simulation_pk=simulation.id, data=physical_settings)
        time.sleep(sleeptime)

        for a in aggregation_settings:
            api_client.simulations_settings_aggregation_create(simulation_pk=simulation.id, data=a)
            time.sleep(sleeptime)

        # Set breach event open
        breach_event = api_client.simulations_events_breaches_create(
            simulation.id,
            data={
                "potential_breach": breach.id,
                "duration_till_max_depth": 600,
                "maximum_breach_depth": 100,
                "initial_width": 50,
                "offset": 0,
            },
        )

        time.sleep(sleeptime)

        # ADD postprocessing
        # basic_processing_data = {
        #             "scenario_name": scenario_name ,
        #             "process_basic_results": True,
        #         }

        # api_client.simulations_results_post_processing_lizard_basic_create(
        #                     simulation.id, data=basic_processing_data)
        # time.sleep(sleeptime)

        # api_client.simulations_results_post_processing_lizard_arrival_create(
        #                     simulation_pk=simulation.id, data={})
        # time.sleep(sleeptime)

        # update metadata
        metadata_gdf.loc[metadata_gdf["SC_NAAM"] == scenario_name, "SC_IDENT"] = simulation.id
        metadata_gdf.loc[metadata_gdf["SC_NAAM"] == scenario_name, "ID"] = breach.line_id
        metadata_gdf.loc[metadata_gdf["SC_NAAM"] == scenario_name, "SC_DATE"] = datum_str
        metadata_gdf.loc[metadata_gdf["SC_NAAM"] == scenario_name, "MOD_VERSIE"] = model_versie

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
                api_client.simulations_actions_create(simulation.id, data={"name": "queue"})
                print(f"{x}: {scenario_name} is queued with id {simulation.id}")
            else:
                print("wait", wait_time, "s")
                time.sleep(wait_time)


# %%
if __name__ == "__main__":
    # Use organisation_name 'BWN HHNK' for standard simulation. Use the other one for very specific cases

    organisation_name = "BWN HHNK"
    # organisation_name = 'Hoogheemraadschap Hollands Noorderkwartier'

    # Set the model name as it is either in 3di or in the local folder.
    base_folder = r"E:\02.modellen"
    model_name = "RegionalFloodModel - deelmodel Schermer Midden Noord"
    model_folder = ModelFolder(rf"{base_folder}\{model_name}")
    model_folder.schema.base
    # Select the return periods you want to start with. If you want to use all of them keep it.
    scenarios = [1000]

    # id_filter corresponds to the column 'id' of the potential breach table of the model we are working with.
    # In case of willing to run all the potential breach, leave the list empty  --> filter_id = []
    filter_id = [126]
    # location of the metadata file. Important to have at least 2 version: One for uploading and run model and the other one for downloading.
    metadata_path = Path(
        r"\\corp.hhnk.nl\data\geo_info\02_Werkplaatsen\04_GIS\Niek Postma\Langere instroom NS\juan\metadata_shapefile.gpkg"
    )

    # Time (in seconds) to wait until the script tries again to upload a model. We use it to not overload the API.
    wait_time = 3600  # 1  hour

    start_simulation_breaches(model_folder, organisation_name, scenarios, filter_id, metadata_path, wait_time)
    # %%
