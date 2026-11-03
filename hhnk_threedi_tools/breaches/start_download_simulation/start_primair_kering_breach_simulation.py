import time
from pathlib import Path

import fiona
import geopandas as gpd
from register_simulation_data import SimulationRegistry
from simulation_settings import START_DATETIME


def prepare_simulations(api_client, project):

    # Get latest 3Di model
    model = api_client.threedimodels_list(name=project.model_folder.name).results[0]

    # Get simulation template
    simulation_template = api_client.simulation_templates_list(simulation__threedimodel__id=model.id).results[0]

    # Get potential breaches from Rana
    potential_breaches = api_client.threedimodels_potentialbreaches_list(model.id, limit=9999)

    # Read potential breaches from GeoPackage
    gpkg_path = project.model_folder / "work in progress" / "schematisation" / f"{project.model_folder.name}.gpkg"

    with fiona.open(gpkg_path, layer="potential_breach") as src:
        records = list(src)
        crs = src.crs

    gdf = gpd.GeoDataFrame.from_features(records, crs=crs)
    gdf["id"] = [int(feature["id"]) for feature in records]

    # Select breaches
    selected_breaches = []

    for breach in potential_breaches.results:
        if breach.connected_pnt_id not in project.filter_id:
            continue

        breach_row = gdf.loc[gdf["id"] == breach.connected_pnt_id].iloc[0]

        scenario_name = breach_row["display_name"]
        material = breach_row["material"].lower()

        # Set initial breach width
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

    return model, simulation_template, selected_breaches


def create_simulations(api_client, project, model, simulation_template, selected_breaches):

    # Get organisation UUID
    organisation = api_client.organisations_list(name__istartswith=project.organisation_name).results[0]

    # Initialize Excel registry
    registry = SimulationRegistry(project.excel_path)

    # Create simulations
    for item in selected_breaches:
        breach = item["breach"]
        scenario_name = item["scenario_name"]
        initial_width = item["initial_width"]

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

        print(f"Simulation created: {scenario_name}, ID: {simulation.id}")

        # Register simulation in Excel
        registry.add_simulation(
            model_id=model.id,
            scenario_name=scenario_name,
            schematitation_name=project.model_folder.name,
            schematisation_id=model.schematisation_id,
            revision_number=model.revision_number,
            scenario_id_rana=simulation.id,
            initial_width=initial_width,
        )

        time.sleep(2)

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

        print(f"Breach configured: {breach.connected_pnt_id}, width: {initial_width} m")


from simulation_settings import (
    AGGREGATION_SETTINGS,
    NUMERICAL_SETTINGS,
    PHYSICAL_SETTINGS,
    TIME_STEP_SETTINGS,
)


def apply_simulation_settings(api_client, simulation_id):

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
        aggregation_id = aggregation.url.split("/")[-2]

        api_client.simulations_settings_aggregation_delete(simulation_pk=simulation_id, id=aggregation_id)

    # Create HHNK aggregation settings
    for aggregation in AGGREGATION_SETTINGS:
        api_client.simulations_settings_aggregation_create(simulation_pk=simulation_id, data=aggregation)

    print(f"Simulation {simulation_id}: settings configured.")


model, simulation_template, selected_breaches = prepare_simulations(
    api_client,
    project,
)

create_simulations(
    api_client,
    project,
    model,
    simulation_template,
    selected_breaches,
)

# Register simulation in Excel
SimulationRegistry(
    scenario_name=scenario_name,
    schematitation_name=project.model_folder.name,
    schematisation_id=model.schematisation_id,
    revision_number=model.revision_number,
    threedimodel_id=model.id,
    scenario_id_rana=simulation.id,
)

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
