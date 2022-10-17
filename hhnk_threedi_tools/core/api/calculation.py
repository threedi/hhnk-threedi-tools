# -*- coding: utf-8 -*-
"""
Created on Tue Apr 12 14:40:28 2022

@author: chris.kerklaan

note, this was not needed.
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
        api_key: str,
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

        config = {
            "THREEDI_API_HOST": host,
            # "THREEDI_API_USERNAME": username,
            # "THREEDI_API_PASSWORD": password,
            "THREEDI_API_PERSONAL_API_TOKEN": api_key,
        }

        self.threedi_api = ThreediApi(config=config)
        self.threedi_api_beta = ThreediApi(config=config, version="v3-beta")

    @property
    def logged_in(self):
        call = self.threedi_api.auth_users_list()
        result = api_result(call, "Cannot login")
        return result

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
    def model_id(self, model_id: int):
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


def api_result(
    result: tac.openapi.models.inline_response20062.InlineResponse20062, message: str
) -> tac.openapi.models.threedi_model.ThreediModel:
    """Raises an error if no results"""
    if len(result.results) == 0:
        raise ValueError(message)
    return result.results[0]


if __name__ == "__main__":
    sim = Simulation("6fh7l3HU.hefCEcK7sNk9QTbWmUefU24671Q1NAB2")
    sim.model = "BWN Schermer interflow referentie #2"
    sim.template = "Referentie"
