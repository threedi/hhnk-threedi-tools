# %%
from asyncio.windows_events import NULL
import datetime
from time import sleep

import pytz

from threedi_api_client.openapi import ApiException
from threedi_api_client.api import ThreediApi

from deprecated.core.api.upload_model.constants import (
    THREEDI_API_HOST,
    ORGANISATION_UUID,
    BUIEN,
    RADAR_ID,
    SCHEMATISATIONS,
)
from deprecated.core.api.upload_model.login import get_login_details

CONFIG = {
    "THREEDI_API_HOST": THREEDI_API_HOST,
    "THREEDI_API_USERNAME": get_login_details(option="username"),
    "THREEDI_API_PASSWORD": get_login_details(option="password"),
}  #
THREEDI_API = ThreediApi(config=CONFIG, version="v3-beta")

# Define timezones
AMSTERDAM = pytz.timezone("Europe/Amsterdam")
UTC = datetime.timezone.utc

# Define start/end date
SIMULATION_START = datetime.datetime(2021, 6, 29, 17, 0).astimezone(AMSTERDAM)


def get_model_and_simulation_name(schematisation_name: str, bui: str = "T100"):
    # find model
    model = THREEDI_API.threedimodels_list(
        limit=1, revision__schematisation__name=schematisation_name
    ).results[
        0
    ]  # index 0 is always latest revision

    # construct simulation name
    return model, f"{schematisation_name} rev {model.revision_number} {bui}"


def start_simulatie(schematisation_name: str, bui: str = "T100", duration=3 * 60 * 60):
    model, simulation_name = get_model_and_simulation_name(
        schematisation_name=schematisation_name, bui=bui
    )

    # find simulation template
    simulation_template = THREEDI_API.simulation_templates_list(
        simulation__threedimodel__id=model.id
    ).results[0]
    simulation = THREEDI_API.simulations_from_template(
        data={
            "template": simulation_template.id,
            "name": simulation_name,
            "tags": ["X0012"],
            "organisation": ORGANISATION_UUID,
            "start_datetime": SIMULATION_START.astimezone(datetime.timezone.utc),
            "duration": duration,
        }
    )

    # add rain
    if bui == "T100":
        THREEDI_API.simulations_events_rain_timeseries_create(
            simulation_pk=simulation.id,
            data={
                "offset": 0,
                "interpolate": False,
                "values": BUIEN[bui],
                "units": "m/s",
            },
        )

    elif bui.upper() == "RADAR":
        THREEDI_API.simulations_events_rain_rasters_lizard_create(
            simulation.id,
            data={
                "offset": 0,
                "duration": duration,
                "reference_uuid": RADAR_ID,
                "start_datetime": SIMULATION_START.astimezone(datetime.timezone.utc),
                "units": "m/s",
                "multiplier": 1,
            },
        )
    else:
        raise ValueError("Kies 'T100' of 'Radar' als bui")

    # add lizard postprocessing
    THREEDI_API.simulations_results_post_processing_lizard_basic_create(
        simulation_pk=simulation.id,
        data={
            "scenario_name": simulation_name,
            "process_basic_results": True,
        },
    )

    # start simulatie
    started = False
    while not started:
        try:
            THREEDI_API.simulations_actions_create(
                simulation.id, data={"name": "queue"}
            )
            print(f"Started simulation {simulation_name} with model {model.name}")
            started = True
        except ApiException:
            sleep(60)


if __name__ == "__main__":
    for bui in ["T100", "Radar"]:
        for schem in SCHEMATISATIONS:
            start_simulatie(schematisation_name=schem, bui=bui)
