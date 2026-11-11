# %%
import os
import time
from pathlib import Path
from typing import Any

import fiona
import geopandas as gpd
import hhnk_research_tools as hrt
from threedi_api_client.api import ThreediApi
from threedi_api_client.versions import V3Api

from hhnk_threedi_tools.breaches.start_download_simulation import find_upstream_links_to_breach_short as fulb_short
from hhnk_threedi_tools.breaches.start_download_simulation.project_config import ProjectConfig
from hhnk_threedi_tools.breaches.start_download_simulation.register_simulation_data import SimulationRegistry
from hhnk_threedi_tools.breaches.start_download_simulation.simulation_settings import (
    AGGREGATION_SETTINGS,
    NUMERICAL_SETTINGS,
    PHYSICAL_SETTINGS,
    START_DATETIME,
    TIME_STEP_SETTINGS,
)


class ModelFolder(hrt.Folder):
    """Manage the local model folder and its schematisation GeoPackage."""

    def __init__(self, base: str | Path) -> None:
        super().__init__(str(base))

        schema_path = self.find_schema_path()
        self.add_file("schema", str(schema_path.relative_to(Path(self.base))))

    def find_schema_path(self) -> Path:
        schematisation_folder = Path(self.base) / "work in progress" / "schematisation"
        schema_files = list(schematisation_folder.glob("*.gpkg"))

        if not schema_files:
            raise FileNotFoundError(f"No .gpkg file found in {schematisation_folder}.")
        if len(schema_files) > 1:
            raise ValueError(f"More than one .gpkg file found in {schematisation_folder}.")

        return schema_files[0]


def prepare_simulations(
    api_client: V3Api,
    project: ProjectConfig,
) -> tuple[Any, Any, list[dict[str, Any]]]:
    """
    Select the latest 3Di model, its simulation template and potential breaches.

    Breach IDs, display names and materials are read from the local GeoPackage.
    The API potential breach is matched using connected_pnt_id.

    Returns
    -------
    model : Any
        Selected 3Di model.
    simulation_template : Any
        Simulation template belonging to the selected model.
    selected_breaches : list[dict]
        Selected breaches with scenario name and initial width.
    """

    model_folder = ModelFolder(project.model_folder)

    # Get latest 3Di model
    model = api_client.threedimodels_list(name__icontains=model_folder.name).results[0]

    print(f"Model: {model.name}")
    print(f"Model ID: {model.id}, Revision: {model.revision_number}")

    # Get simulation template
    simulation_template = api_client.simulation_templates_list(simulation__threedimodel__id=model.id).results[0]

    # Get potential breaches from Rana
    potential_breaches = api_client.threedimodels_potentialbreaches_list(model.id, limit=9999)

    # Read potential breaches from GeoPackage
    with fiona.open(model_folder.schema.path, layer="potential_breach") as src:
        records = list(src)
        crs = src.crs

    breach_gdf = gpd.GeoDataFrame.from_features(records, crs=crs)
    breach_gdf["id"] = [int(feature["id"]) for feature in records]

    # Select potential breaches
    selected_breaches = []

    for breach in potential_breaches.results:
        if breach.connected_pnt_id not in project.filter_id:
            continue

        breach_row = breach_gdf.loc[breach_gdf["id"] == breach.connected_pnt_id].iloc[0]

        scenario_name = breach_row["display_name"]
        material = str(breach_row["levee_material"]).strip().lower()

        # Determine initial breach width
        if project.kering_type == "primary" and material == "sand":
            initial_width = 50
        else:
            initial_width = 10

        selected_breaches.append(
            {
                "breach": breach,
                "scenario_name": scenario_name,
                "initial_width": initial_width,
            }
        )

        print(f"{scenario_name}: initial width {initial_width} m")

    return model, simulation_template, selected_breaches


def set_initial_waterlevels(
    api_client: V3Api,
    model_id: int,
    simulation_id: int,
) -> None:
    """
    Set initial water levels using files belonging to the selected 3Di model.

    The 1D initial water level file is required.
    The 2D initial water level raster is optional.

    Existing file/raster settings inherited from the template are replaced.
    """

    # Get initial water levels 1D from model
    model_waterlevels = api_client.threedimodels_initial_waterlevels_list(threedimodel_pk=model_id).results

    initial_1d = None
    initial_2d = None

    for waterlevel in model_waterlevels:
        if waterlevel.dimension == "one_d":
            initial_1d = waterlevel
        if waterlevel.dimension == "two_d":
            initial_2d = waterlevel
            break

    if initial_1d is None:
        raise ValueError(f"No initial water level 1D found for model {model_id}.")

    # Remove inherited 1D file
    current_1d = api_client.simulations_initial1d_water_level_file_list(simulation_pk=simulation_id).results

    for waterlevel in current_1d:
        api_client.simulations_initial1d_water_level_file_delete(simulation_pk=simulation_id, id=waterlevel.id)

    # Set initial water level 1D
    api_client.simulations_initial1d_water_level_file_create(
        simulation_pk=simulation_id, data={"initial_waterlevel": initial_1d.id}
    )

    print(f"Initial water level 1D: {initial_1d.id}")

    # Remove inherited 2D raster
    current_2d = api_client.simulations_initial2d_water_level_raster_list(simulation_pk=simulation_id).results

    for waterlevel in current_2d:
        api_client.simulations_initial2d_water_level_raster_delete(simulation_pk=simulation_id, id=waterlevel.id)

    # Set initial water level 2D when available
    if initial_2d is not None:
        api_client.simulations_initial2d_water_level_raster_create(
            simulation_pk=simulation_id,
            data={
                "initial_waterlevel": initial_2d.id,
                "aggregation_method": "mean",
            },
        )

        print(f"Initial water level 2D: {initial_2d.id}")

    else:
        print("No initial water level 2D available.")


def apply_simulation_settings(
    api_client: V3Api,
    simulation_id: int,
) -> None:
    """
    Apply standard HHNK simulation settings.

    Replaces inherited numerical, physical, time step and aggregation
    settings with those defined in simulation_settings.py.
    """

    # Time step settings
    api_client.simulations_settings_time_step_delete(simulation_pk=simulation_id)

    api_client.simulations_settings_time_step_create(simulation_pk=simulation_id, data=TIME_STEP_SETTINGS)

    # Numerical settings
    api_client.simulations_settings_numerical_delete(simulation_pk=simulation_id)

    api_client.simulations_settings_numerical_create(simulation_pk=simulation_id, data=NUMERICAL_SETTINGS)

    # Physical settings
    api_client.simulations_settings_physical_delete(simulation_pk=simulation_id)

    api_client.simulations_settings_physical_create(simulation_pk=simulation_id, data=PHYSICAL_SETTINGS)

    # Remove existing aggregation settings
    current_aggregations = api_client.simulations_settings_aggregation_list(
        simulation_pk=simulation_id, limit=99
    ).results

    for aggregation in current_aggregations:
        aggregation_id = Path(aggregation.url).name

        api_client.simulations_settings_aggregation_delete(simulation_pk=simulation_id, id=aggregation_id)

    # Apply HHNK aggregation settings
    for aggregation in AGGREGATION_SETTINGS:
        api_client.simulations_settings_aggregation_create(simulation_pk=simulation_id, data=aggregation)

        time.sleep(2)

    print(f"Simulation settings configured: {simulation_id}")


def configure_closure(
    api_client: V3Api,
    project: ProjectConfig,
    simulation_id: int,
    breach: Any,
) -> None:
    """
    Configure closure events for regional kering simulations.

    Supported methods:
        none: No closure events.
        compartmentalization: Close manually selected structures.
        upstream: Close upstream channels and structures.

    Closure starts at the project-defined time and remains active
    until the end of the simulation.
    """

    if project.closure_method == "none":
        return

    closure_duration = project.simulation_duration_seconds - project.closure_time_seconds

    if project.closure_method == "compartmentalization":
        # Close manually selected structures
        for structure_id, structure_type in project.structures_info.items():
            api_client.simulations_events_structure_control_timed_create(
                simulation_id,
                data={
                    "offset": project.closure_time_seconds,
                    "duration": closure_duration,
                    "value": [0.0, 0.0],
                    "type": "set_discharge_coefficients",
                    "structure_id": structure_id,
                    "structure_type": structure_type,
                },
            )

            time.sleep(2)

    elif project.closure_method == "upstream":
        # Find upstream links connected to the breach

        upstream = fulb_short.find_upstream_links_to_breach(project.testresult_folder, [breach.connected_pnt_id])

        upstream_data = None

        for data in upstream["data"]:
            print(data)
            if data["breach_id"] == breach.connected_pnt_id:
                upstream_data = data
                break

        if upstream_data is None:
            raise ValueError(f"No upstream data for breach {breach.connected_pnt_id}.")

        # Close upstream channels and structures
        for i in range(len(upstream_data["upstream_links"])):
            link_type = upstream_data["upstream_types"][i]

            if link_type in ["v2_added_c", "v2_breach", ""]:
                continue

            if link_type == "v2_channel":
                event = {
                    "offset": project.closure_time_seconds,
                    "duration": closure_duration,
                    "value": [0.0, 0.0],
                    "type": "set_discharge_coefficients",
                    "grid_id": int(upstream_data["upstream_links"][i]),
                    "structure_type": link_type,
                }

            else:
                event = {
                    "offset": project.closure_time_seconds,
                    "duration": closure_duration,
                    "value": [0.0, 0.0],
                    "type": "set_discharge_coefficients",
                    "structure_id": int(upstream_data["upstream_links"][i]),
                    "structure_type": link_type,
                }

            api_client.simulations_events_structure_control_timed_create(
                simulation_id,
                data=event,
            )

            time.sleep(5)

    print(f"Closure configured: {simulation_id}")


def start_simulation_breaches(
    api_client: V3Api,
    project: ProjectConfig,
    wait_time: int = 3600,
) -> None:
    """
    Create and queue breach simulations for a 3Di project.

    Simulations are created using a simulation template, while initial
    water levels are explicitly selected from the 3Di model.

    Each simulation is registered in simulations_id.xlsx only after
    the API accepts the queue request.

    Parameters
    ----------
    api_client : V3Api
        Authenticated 3Di API client.
    project : ProjectConfig
        Configuration of the simulation project.
    wait_time : int
        Time in seconds between queue availability checks.
    """

    # Prepare model, template and breaches
    model, simulation_template, selected_breaches = prepare_simulations(api_client, project)

    # Get organisation UUID
    organisation = api_client.organisations_list(name__istartswith=project.organisation_name).results[0]

    # Initialize simulation registry
    registry = SimulationRegistry(project.excel_path)

    for item in selected_breaches:
        breach = item["breach"]
        scenario_name = item["scenario_name"]
        initial_width = item["initial_width"]

        print(f"\nPreparing simulation: {scenario_name}")

        # Create simulation from template
        simulation = api_client.simulations_from_template(
            data={
                "template": simulation_template.id,
                "name": scenario_name,
                "tags": ["ROR"],
                "threedimodel": model.id,
                "organisation": organisation.unique_id,
                "start_datetime": START_DATETIME,
                "duration": project.simulation_duration_seconds,
            }
        )

        print(f"Simulation created: {simulation.id}")

        # Set initial water levels from model
        set_initial_waterlevels(api_client, model.id, simulation.id)

        # Apply HHNK settings
        apply_simulation_settings(api_client, simulation.id)

        # Create breach event
        api_client.simulations_events_breaches_create(
            simulation.id,
            data={
                "potential_breach": breach.id,
                "duration_till_max_depth": 600,
                "maximum_breach_depth": 100,
                "initial_width": initial_width,
                "offset": 0,
            },
        )

        # Configure regional closure events
        configure_closure(api_client, project, simulation.id, breach)
        # Wait for available queue capacity
        while True:
            queue_length = api_client.statuses_list(
                name__startswith="queued",
                simulation__organisation__unique_id=organisation.unique_id,
            ).count

            if queue_length < 2:
                break

            print(f"Queue full. Waiting {wait_time} seconds.")
            time.sleep(wait_time)

        # Queue simulation
        api_client.simulations_actions_create(simulation.id, data={"name": "queue"})

        print(f"Simulation queued: {scenario_name} ({simulation.id})")

        # Register simulation after queue request succeeds
        registry.add_simulation(
            scenario_name=scenario_name,
            schematitation_name=project.model_folder.name,
            schematisation_id=model.schematisation_id,
            revision_number=model.revision_number,
            model_id=model.id,
            scenario_id_rana=simulation.id,
            initial_width=initial_width,
        )

        print(f"Simulation registered: {simulation.id}")


# %%
if __name__ == "__main__":
    # Project configuration
    project = ProjectConfig(
        project_folder=r"H:\03.resultaten\ROR_PRI_Stroom_NO",
        model_folder=r"H:\02.modellen\RegionalFloodModel - deelmodel Schermer Laag Zuid",
        organisation_name="BWN HHNK",
        filter_id=[48],
        kering_type="regionale",  # "regionale" of "primaire"
        simulation_duration=0.2,  # days
        closure_method="compartmentalization",  # none of upstream of compartmentalization
        closure_time=4,  # None, time in hours
        structures_info={406: "v2_orifice", 407: "v2_orifice", 404: "v2_orifice"},  # structures_info = {406: "v2_orifice", 407: "v2_orifice", 404: "v2_orifice"}
        testresult_folder=r"H:\02.modellen\RegionalFloodModel - deelmodel Schermer Laag Zuid\test_ipo_sblz_class",
    )

    # API authentication
    api_keys_path = (
        rf"{os.getenv('APPDATA')}\3Di\QGIS3\profiles\default"
        rf"\python\plugins\hhnk_threedi_plugin\api_key.txt"
    )

    api_keys = hrt.read_api_file(api_keys_path)

    config = {
        "THREEDI_API_HOST": "https://api.3di.live",
        "THREEDI_API_PERSONAL_API_TOKEN": api_keys["threedi"],
    }

    api_client = ThreediApi(config=config, version="v3-beta")

    # Verify authentication
    user = api_client.auth_profile_list()
    print(f"Successfully logged in as {user.username}!")

    # Start breach simulations
    start_simulation_breaches(
        api_client=api_client,
        project=project,
        wait_time=3600,
    )

# %%
