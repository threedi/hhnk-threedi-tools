# %% TODO UNDER CONSTRUCTION
# system imports
import os
import shutil
import pprint
import datetime


import requests
from pathlib import Path
import zipfile

# Third-party imports
import json
import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import ipywidgets as widgets
from IPython.core.display import display, HTML
from traitlets import Unicode
from apscheduler.schedulers.blocking import BlockingScheduler
from ipyfilechooser import FileChooser

# threedi
from threedi_scenario_downloader import downloader as dl
from hhnk_threedi_tools.core.api.read_api_file import read_api_file
import openapi_client

# local imports
from hhnk_threedi_tools import Folders

from hhnk_threedi_tools.core.api.calculation_functions_klondike import (
    create_3Di_start_API_call_data,
    start_3di_calculation,
    wait_to_download_results,
    create_threedi_simulation,
)

from hhnk_threedi_tools.core.api.download_functions import (
    create_download_url,
    start_download,
)

from hhnk_threedi_tools.core.api.calculation import Simulation

# Globals
from hhnk_threedi_tools.variables.api_settings import (
    RAIN_SETTINGS,
    RAIN_TYPES,
    RAIN_INTENSITY,
    GROUNDWATER,
    RAIN_SCENARIOS,
    API_SETTINGS,
    MODEL_TYPES,
)

simulation = None
batch_started = False


# #  %% TESTNNG %% ##
# main_folder = "C:/Users/wvangerwen/Github/hhnk-threedi-tools/hhnk_threedi_tools/tests/data/multiple_polders"
# # main_folder=None
# base_scenario_name=None
# lizard_api_key=LIZARD_API_KEY
# if True:
# #  %% TESTNNG %% ##


dl.LIZARD_URL = "https://hhnk.lizard.net/api/v3/"
THREEDI_API_HOST = "https://api.3di.live/v3"
RESULT_LIMIT = 20


def item_layout(width="95%", grid_area="", **kwargs):
    return widgets.Layout(
        width=width, grid_area=grid_area, **kwargs
    )  # override the default width of the button to 'auto' to let the button grow




class StartCalculationWidgets:
    def __init__(self):
        # --------------------------------------------------------------------------------------------------
        # 1. Login with API keys
        # --------------------------------------------------------------------------------------------------
        self.login = self.LoginWidgets()
        self.model = self.ModelWidgets()
        self.rain = self.RainWidgets(self)
        self.output = self.OutputWidgets()
        self.calc_settings = self.CalcSettingsWidgets()
        self.feedback = self.FeedbackWidgets()
        self.start = self.StartWidgets()


    class LoginWidgets:
        def __init__(self):
            class ApikeyWidget(widgets.Text):
                _view_name = Unicode("PasswordView").tag(sync=True)

            #Label
            self.label = widgets.HTML(
                "<b>1. Login with API keys</b>",
                layout=item_layout(grid_area="login_label"),
            )

            # Api key widgets
            self.lizard_apikey_widget = ApikeyWidget(
                description="Lizard key:",
                layout=item_layout(grid_area="lizard_apikey_widget"),
            )

            self.threedi_apikey_widget = ApikeyWidget(
                description="Threedi key:",
                layout=item_layout(grid_area="threedi_apikey_widget"),
            )

            # Login button, after login create threedi api client
            self.button = widgets.Button(
                description="Login",
                layout=item_layout(height="30px", grid_area="login_button"),
            )

    class ModelWidgets:
        def __init__(self):

            #Searching for the schema on 3Di servers.
            self.label = widgets.HTML(
                    "<b>2. Search for schematisation on 3Di</b>",
                    layout=item_layout(grid_area="model_label"),
                )
            self.schema_name_widget = widgets.Text(
                    layout=item_layout(grid_area="schema_name_widget")
                )
            self.search_button = widgets.Button(
                    description="Search",
                    layout=item_layout(height="30px", grid_area="model_name_search_button"),
                )



            #Selecting the schematisation, rev and sqlite
            self.select_label = widgets.HTML(
                    "<b>3. Select schematisation and model </b>",
                    layout=item_layout(grid_area="schema_select_label"),
                )

            self.schema_label = widgets.Label(
                    "Schematisation: ", layout=item_layout(grid_area="schema_label")
                )
            self.schema_dropdown = widgets.Dropdown(
                    layout=item_layout(grid_area="schema_dropdown")
                )
            self.revision_label = widgets.Label(
                    "Revision: ", layout=item_layout(grid_area="revision_label")
                )
            self.revision_dropdown = widgets.Dropdown(
                    layout=item_layout(grid_area="revision_dropdown")
                )
            self.threedimodel_label = widgets.Label(
                    "3Di model: ", layout=item_layout(grid_area="threedimodel_label")
                )
            self.threedimodel_dropdown = widgets.Dropdown(
                    disabled=True, layout=item_layout(grid_area="threedimodel_dropdown")
                )
            self.organisation_label = widgets.Label(
                    "Organisation:", layout=item_layout(grid_area="organisation_label")
                )
            self.organisation_box = widgets.Select(
                options=[],
                rows=2,
                disabled=False,
                layout=item_layout(height="60px", grid_area="organisation_box"),
            )


            # self.sqlite_label = widgets.Label(
            #         "Select sqlite containing structure control rules",
            #         layout=item_layout(grid_area="sqlite_label"),
            #     )
            # self.sqlite_dropdown = widgets.Dropdown(
            #         layout=item_layout(grid_area="sqlite_dropdown")
            #     )
            # self.sqlite_chooser = FileChooser(filter_pattern='*sqlite',
            #             layout=item_layout(grid_area="sqlite_chooser"))

    class RainWidgets():
        def __init__(self, caller):
            self.caller=caller
            self.label = widgets.HTML(
                    "<b>4. Select rain event</b>", layout=item_layout(grid_area="rain_label")
                )

            # hydraulic test; 1 dry, 5 days rain, 2 dry
            self.test_0d1d_button = widgets.Button(
                description="Hyd test (0d1d)", layout=item_layout(grid_area="test_0d1d_button")
            )

            # 1d2d test: T10 rain peak. 35.5mm total
            self.test_1d2d_button = widgets.Button(
                description="1d2d test",
                layout=item_layout(grid_area="test_1d2d_button", justify_self="end"),
            )

            #One hour test
            self.test_hour_button = widgets.Button(
                description="1 hour test",
                layout=item_layout(grid_area="hour_test_button", justify_self="end"),
            )

            for rtype in RAIN_TYPES: #["piek", "blok"]
                for rscenario in RAIN_SCENARIOS: #["T10", "T100", "T1000"]
                    setattr(self, f"{rscenario}_{rtype}_button", widgets.Button(
                        description=f"{rscenario}_{rtype}", layout=item_layout(grid_area=f"{rscenario}_{rtype}_button")
                    ))

            #custom rain
            self.custom_rain_button = widgets.Button(
                    description=f"custom rain", layout=item_layout(grid_area=f"custom_rain_button")
                )

            self.custom_rain_label = widgets.Label("Rain string format [offset[h], duration[h], value [mm/hour]; offset, dur...]", 
                                                    layout=item_layout(grid_area="custom_rain_label"))
            self.custom_rain_text = widgets.Text(layout=item_layout(grid_area="custom_rain_text"), continuous_update=False)


            self.widget_class = self.caller.RainEventWidget()
            self.rain_event_widget = self.widget_class.rain_event_widget



    class RainEventWidget():
        def __init__(self):
            """Create interactive plot with sliders for various input parameters"""


            style = {"description_width": "100px"}

            self.days_dry_start_slider = self._create_float_slider(
                    1, 0, 5, 1, style, '',
                )
            self.hours_dry_start_slider = self._create_float_slider(
                    0, 0, 23, 1, style, '', 
                )
            self.days_rain_slider = self._create_float_slider(
                    5, 0, 10, 1, style, '', 
                )
            self.hours_rain_slider= self._create_float_slider(
                    0, 0, 23, 1, style, '',
                )
            self.days_dry_end_slider=self._create_float_slider(
                    2, 0, 5.0, 1, style, '',
                )
            self.hours_dry_end_slider=self._create_float_slider(
                    0, 0, 23, 1, style, '',
                )
            self.rain_intensity_slider=self._create_float_slider(
                    4.167, 0, 100, 0.01, style, "rain [mm/hour]",
                )

            # Comebine plot and sliders
            self.rain_event_widget = widgets.interactive(
                self.plot_rain_event,
                days_dry_start=self.days_dry_start_slider,
                hours_dry_start=self.hours_dry_start_slider,
                days_rain=self.days_rain_slider,
                hours_rain=self.hours_rain_slider,
                days_dry_end=self.days_dry_end_slider,
                hours_dry_end=self.hours_dry_end_slider,
                rain_intensity=self.rain_intensity_slider,
            )
            # Give this widget a name so it can be placed in the grid
            self.rain_event_widget.layout = widgets.Layout(grid_area="rain_event_widget")


        def _create_float_slider(self, value, minval, maxval,step, style, description):
            return widgets.FloatSlider(
                    value=value,
                    min=minval,
                    max=maxval,
                    step=step,
                    style=style,
                    description=description,
                    layout=item_layout(),
                    continuous_update=False,
                )

        def plot_rain_event(self,
            days_dry_start,
            hours_dry_start,
            days_rain,
            hours_rain,
            days_dry_end,
            hours_dry_end,
            rain_intensity,
        ):
            def create_plot(x, y, title, xlabel, ylabel):
                fig, ax = plt.subplots(figsize=[10, 3])
                plt.plot(x, y)
                plt.title(title)
                plt.xlabel(xlabel)
                plt.ylabel(ylabel)
                ax.grid()
                return fig, ax


            # Create timeseries
            dt = 1 / 24

            part1 = [
                round(x, 2)
                for x in np.arange(
                    0, days_dry_start + hours_dry_start / 24, dt
                ).tolist()
            ]
            part2 = [
                round(x, 2)
                for x in np.arange(
                    days_dry_start + hours_dry_start / 24,
                    days_dry_start + hours_dry_start / 24 + days_rain + hours_rain / 24,
                    dt,
                ).tolist()
            ]
            part3 = [
                round(x, 2)
                for x in np.arange(
                    days_dry_start + hours_dry_start / 24 + days_rain + hours_rain / 24,
                    days_dry_start
                    + hours_dry_start / 24
                    + days_rain
                    + hours_rain / 24
                    + days_dry_end
                    + hours_dry_end / 24
                    + dt,
                    dt,
                ).tolist()
            ]

            part1 = np.arange(0, days_dry_start + hours_dry_start / 24, dt).tolist()
            part2 = np.arange(
                days_dry_start + hours_dry_start / 24,
                days_dry_start + hours_dry_start / 24 + days_rain + hours_rain / 24,
                dt,
            ).tolist()
            part3 = np.arange(
                days_dry_start + hours_dry_start / 24 + days_rain + hours_rain / 24,
                days_dry_start
                + hours_dry_start / 24
                + days_rain
                + hours_rain / 24
                + days_dry_end
                + hours_dry_end / 24
                + dt,
                dt,
            ).tolist()

            if part2:  # part2 is the rain part
                time = part1 + [part2[0]] + part2 + [part3[0]] + part3
            else:
                time = part1 + part2 + [part3[0]] + part3

            # Create rain intensity timeseries
            rain = np.zeros(np.size(time))

            # Add some indices for nicer plotting
            if days_dry_start == 0 and hours_dry_start == 0:
                value_start = 0
            else:
                value_start = 1
            if days_dry_end == 0 and hours_dry_end == 0:
                value_end = 3
            else:
                value_end = 1

            if days_rain != 0 or hours_rain != 0:
                rain[
                    time.index(days_dry_start + hours_dry_start / 24)
                    + value_start : time.index(
                        days_dry_start
                        + hours_dry_start / 24
                        + days_rain
                        + hours_rain / 24
                    )
                    + value_end
                ] = rain_intensity

            fig, ax = create_plot(
                time, rain, "Rain event", "Time [days]", "Rain intensity [mm/hour]"
            )

            # fig.show()
        


        def update_sliders(self,
            days_dry_start,
            hours_dry_start,
            days_rain,
            hours_rain,
            days_dry_end,
            hours_dry_end,
            rain_intensity,
        ):
            """Update sliders with input values, used for buttons."""
            self.days_dry_start_slider.value = days_dry_start
            self.hours_dry_start_slider.value = hours_dry_start
            self.days_rain_slider.value = days_rain
            self.hours_rain_slider.value = hours_rain
            self.days_dry_end_slider.value = days_dry_end
            self.hours_dry_end_slider.value = hours_dry_end
            self.rain_intensity_slider.value = rain_intensity

        def update_sliders_rain_event(self, rain_type, rain_scenario):
            self.update_sliders(
                days_dry_start=RAIN_SETTINGS[rain_type]["days_dry_start"],
                hours_dry_start=RAIN_SETTINGS[rain_type]["hours_dry_start"],
                days_rain=RAIN_SETTINGS[rain_type]["days_rain"],
                hours_rain=RAIN_SETTINGS[rain_type]["hours_rain"],
                days_dry_end=RAIN_SETTINGS[rain_type]["days_dry_end"],
                hours_dry_end=RAIN_SETTINGS[rain_type]["hours_dry_end"],
                rain_intensity=RAIN_INTENSITY[rain_type][rain_scenario],
            )




    class OutputWidgets():
        def __init__(self):
            self.label = widgets.HTML(
                    "<b>5. Select output folder/name</b>",
                    layout=item_layout(grid_area="output_label"),
                )

            # dropdown box with polders (output folder)
            self.folder_label = widgets.Label(
                    "Output folder:", layout=item_layout(grid_area="output_folder_label")
                )
            self.folder_value = widgets.Label(
                    '', layout=item_layout(grid_area="output_folder_value"),
                )

            # Selection box of the folder the output should be put in. (Hyd toets or Extreme)
            self.folder_options = ["1d2d_results", "0d1d_results", "batch_results"]
            self.subfolder_label = widgets.Label(
                    "Sub folder:", layout=item_layout(grid_area="output_subfolder_label")
                )
            self.subfolder_box = widgets.Select(
                    options=self.folder_options,
                    rows=3,
                    disabled=False,
                    layout=item_layout(grid_area="output_subfolder_box"),
                )


    class CalcSettingsWidgets():
        def __init__(self):
            self.label = widgets.HTML(
                    "<b>6. Select settings to include</b>", layout=item_layout(grid_area="calc_settings_label")
                )

            self.basic_processing = widgets.ToggleButton(
                    value=False, description="basic processing", layout=item_layout(grid_area="calc_settings_basic_processing"), icon="plus"
                )
            self.damage_processing = widgets.ToggleButton(
                    value=False, description="damage processing", layout=item_layout(grid_area="calc_settings_damage_processing"), icon="plus"
                )
            self.arrival_processing = widgets.ToggleButton(
                    value=False, description="arrival processing", layout=item_layout(grid_area="calc_settings_arrival_processing"), icon="plus"
                )
            self.structure_control = widgets.ToggleButton(
                    value=True, description="structure control", layout=item_layout(grid_area="calc_settings_structure_control"), icon="check"
                )
            self.laterals = widgets.ToggleButton(
                    value=True, description="laterals", layout=item_layout(grid_area="calc_settings_laterals"), icon="check"
                )

            self.children = [self.basic_processing, 
                            self.damage_processing, 
                            self.arrival_processing, 
                            self.structure_control, 
                            self.laterals]


    class FeedbackWidgets():
        def __init__(self):
            self.label = widgets.HTML(
                    "<b>6. API call</b>", layout=item_layout(grid_area="feedback_label")
                )

            # API call HTML widget that shows the API call
            self.widget = widgets.HTML(layout=item_layout(grid_area="feedback_widget"))



    class StartWidgets():
        def __init__(self):
            self.label = widgets.HTML(
                    "<b>7. Start calculation</b>",
                    layout=item_layout(grid_area="start_label"),
                )

            self.create_simulation_button = widgets.Button(
                    description="Create simulation",
                    layout=item_layout(height="90%", grid_area="create_simulation_button"),
                )

            self.start_button = widgets.Button(
                    description="Start calculation",
                    layout=item_layout(height="90%", grid_area="start_button"),
                )




class StartCalculationWidgetsInteraction(StartCalculationWidgets):
    def __init__(self, caller):
        super().__init__()
        self.caller = caller

        #Login with API key
        @self.login.button.on_click
        def login(action):
            self.vars.sim = Simulation(
                api_key=self.vars.api_keys["threedi"]
            ) 
            dl.set_api_key(self.vars.api_keys['lizard'])
            try:
                self.sim.logged_in
                # Login success
                self.login.button.style.button_color = "lightgreen"
                self.login.button.description = "Logged in"
            except:
                # Login failed
                self.login.button.style.button_color = "red"
                self.login.button.description = "Invalid API key"


        #Search schematisations
        @self.model.search_button.on_click
        def search_models(action):
            schematisations = self.sim.threedi_api.schematisations_list(
                slug__icontains=self.model.schema_name_widget.value, limit=RESULT_LIMIT
            ).results

            self.vars.schematisations={}
            for result in schematisations:  
                self.vars.schematisations[result.id] = result


            organisations = self.sim.threedi_api.organisations_list().results
            self.vars.organisations = {}
            for org in organisations:
                self.vars.organisations[org.name] = org


            #TODO add check of available calc cores. self.vars.sim.threedi_api.statuses_statistics(simulation__organisation__unique_id='48dac75bef8a42ebbb52e8f89bbdb9f2', simulation__type__live=True)

            self.update_dropdowns(schema=True)
            self.update_organisations()


        #Search revisions
        def on_select_schematisation(selected_schematisation):
            """Update revisions options when repository/schematisation is selected"""
            revisions = self.sim.threedi_api.schematisations_revisions_list(
                    schematisation_pk=self.selected_schema_id, limit=RESULT_LIMIT #selected_schematisation['new'].split(' -')[0]
                ).results

            self.vars.revisions = {}
            for revision in revisions:  
                self.vars.revisions[revision.id] = revision #vars.schema_results is empty dict

            threedimodels = self.sim.threedi_api.threedimodels_list(
                    revision__schematisation__id=self.selected_schema_id, limit=RESULT_LIMIT
                ).results

            self.vars.threedimodels = {}
            for threedimodel in threedimodels:  
                self.vars.threedimodels[threedimodel.revision_id] = threedimodel 

            self.update_dropdowns(revision=True, threedimodel=True)

        self.model.schema_dropdown.observe(on_select_schematisation, names="value")


        #Search model with revision
        def on_select_revision(selected_revision):
            """Update revisions options when repository/schematisation is selected"""
            self.model.threedimodel_dropdown.value = self.vars.threedimodel_dropdown_viewlist[self.vars.revision_dropdown_viewlist.index(selected_revision['new'])]

        self.model.revision_dropdown.observe(on_select_revision, names="value")


        #Select sqlite - deprecated, sqlite is downloaded from api.
        # def on_select_sqlite(selected_sqlite):
        #     self.update_folder(schema_viewname=selected_sqlite["new"])
        
        # self.model.sqlite_dropdown.observe(on_select_sqlite, names="value")



        @self.rain.test_0d1d_button.on_click
        def change_rain(action):
            self.rain.widget_class.update_sliders(
                days_dry_start=1,
                hours_dry_start=0,
                days_rain=5,
                hours_rain=0,
                days_dry_end=2,
                hours_dry_end=0,
                rain_intensity=100
                / 24,  # 100mm/day, using impervious surface mapping makes 14.4mm/day and 11.5mm/day
            )
            self._activate_button_color(self.rain.test_0d1d_button)
            self.output.subfolder_box.value =self.output.subfolder_box.options[1]
            self._update_calc_settings_buttons(structure_control=False, laterals=True)
            # update_scenario_name_widget()


        @self.rain.test_1d2d_button.on_click
        def change_rain(action):
            self.rain.widget_class.update_sliders(
                days_dry_start=0,
                hours_dry_start=1,
                days_rain=0,
                hours_rain=2,
                days_dry_end=0,
                hours_dry_end=12,
                rain_intensity=17.75,
            )
            self._activate_button_color(self.rain.test_1d2d_button)
            self.output.subfolder_box.value =self.output.subfolder_box.options[0]
            self._update_calc_settings_buttons(structure_control=True, laterals=True)

            # update_scenario_name_widget()


        @self.rain.test_hour_button.on_click
        def change_rain(action):
            self.rain.widget_class.update_sliders(
                days_dry_start=0,
                hours_dry_start=0,
                days_rain=0,
                hours_rain=1,
                days_dry_end=0,
                hours_dry_end=0,
                rain_intensity=100,
            )
            self._activate_button_color(self.rain.test_hour_button)
            self.output.subfolder_box.value =self.output.subfolder_box.options[0]
            self._update_calc_settings_buttons(structure_control=True, laterals=True)

            # update_scenario_name_widget(" 1hour")

        @self.rain.T10_blok_button.on_click
        def change_rain(action):
            self.rain.widget_class.update_sliders_rain_event(rain_type='blok', 
                                                                rain_scenario='T10')
            self._activate_button_color(self.rain.T10_blok_button)
            self.output.subfolder_box.value =self.output.subfolder_box.options[0]
            self._update_calc_settings_buttons(structure_control=True, laterals=True)

        @self.rain.T100_blok_button.on_click
        def change_rain(action):
            self.rain.widget_class.update_sliders_rain_event(rain_type='blok', 
                                                                rain_scenario='T100')
            self._activate_button_color(self.rain.T100_blok_button)
            self.output.subfolder_box.value =self.output.subfolder_box.options[0]
            self._update_calc_settings_buttons(structure_control=True, laterals=True)

        @self.rain.T1000_blok_button.on_click
        def change_rain(action):
            self.rain.widget_class.update_sliders_rain_event(rain_type='blok', 
                                                                rain_scenario='T1000')
            self._activate_button_color(self.rain.T1000_blok_button)
            self.output.subfolder_box.value =self.output.subfolder_box.options[0]
            self._update_calc_settings_buttons(structure_control=True, laterals=True)

        @self.rain.T10_piek_button.on_click
        def change_rain(action):
            self.rain.widget_class.update_sliders_rain_event(rain_type='piek', 
                                                                rain_scenario='T10')
            self._activate_button_color(self.rain.T10_piek_button)
            self.output.subfolder_box.value =self.output.subfolder_box.options[0]
            self._update_calc_settings_buttons(structure_control=True, laterals=True)

        @self.rain.T100_piek_button.on_click
        def change_rain(action):
            self.rain.widget_class.update_sliders_rain_event(rain_type='piek', 
                                                                rain_scenario='T100')
            self._activate_button_color(self.rain.T100_piek_button)
            self.output.subfolder_box.value =self.output.subfolder_box.options[0]
            self._update_calc_settings_buttons(structure_control=True, laterals=True)

        @self.rain.T1000_piek_button.on_click
        def change_rain(action):
            self.rain.widget_class.update_sliders_rain_event(rain_type='piek', 
                                                                rain_scenario='T1000')
            self._activate_button_color(self.rain.T1000_piek_button)
            self.output.subfolder_box.value =self.output.subfolder_box.options[0]
            self._update_calc_settings_buttons(structure_control=True, laterals=True)

        @self.rain.custom_rain_button.on_click
        def update_custom_rain_text(action):
            rain_intensity_mmph = float(self.rain.widget_class.rain_intensity_slider.value)  # mm/hour
            start_datetime = datetime.datetime(2000, 1, 1, 0, 0)

            rain_start_dt = start_datetime + datetime.timedelta(
                    days=self.rain.widget_class.days_dry_start_slider.value, hours=self.rain.widget_class.hours_dry_start_slider.value
                )
            rain_end_dt = rain_start_dt + datetime.timedelta(
                    days=self.rain.widget_class.days_rain_slider.value, hours=self.rain.widget_class.hours_rain_slider.value
                )
            duration = (rain_end_dt - rain_start_dt).total_seconds()/3600
            offset = (rain_start_dt - start_datetime).total_seconds()/3600

            self.rain.custom_rain_text.value = f"{offset},{duration},{rain_intensity_mmph}"
            self._activate_button_color(self.rain.custom_rain_button)


        def on_custom_rain_change(rain_text):
            self.vars.rain_settings_view = rain_text['new']
        self.rain.custom_rain_text.observe(on_custom_rain_change, names="value")

        # Observe all calculation settings buttons
        for button in self.calc_settings.children:
            button.observe(self._update_button_icon, "value")

    #-- end of __init__ --#
    def update_api_keys(self, api_keys_path):
        self.vars.api_keys = read_api_file(api_keys_path)
        self.login.lizard_apikey_widget.value=self.vars.api_keys["lizard"]
        self.login.threedi_apikey_widget.value=self.vars.api_keys["threedi"]
        self.login.button.click()

    def update_organisations(self):
        self.model.organisation_box.options = self.vars.organisations_viewlist 


    def update_dropdowns(self, **kwargs):
        if 'schema' in kwargs:
            self.model.schema_dropdown.options = self.vars.schema_dropdown_viewlist
        if 'threedimodel' in kwargs: #Needs to be defined before revision because we observe that box.
            self.model.threedimodel_dropdown.options = self.vars.threedimodel_dropdown_viewlist
        if 'revision' in kwargs:
            self.model.revision_dropdown.options = self.vars.revision_dropdown_viewlist
        # if 'sqlite' in kwargs:
        #     self.model.sqlite_dropdown.options = self.vars.sqlite_dropdown_viewlist


    def update_folder(self):
        """when main folder changes, we update some values"""

        # if schema_viewname is None:
        #     db = self.vars.folder.model.schema_base.database
        # else:
        #     db = self.vars.sqlite_dropdown_options[schema_viewname].database

        # if db.exists:
        #     self.model.sqlite_chooser.reset(path=str(db.pl.parent), filename=db.pl.name)
        #     self.model.sqlite_chooser._select.click() #Dont know how else to update the window. 
        #     self.model.sqlite_chooser._select.click()

        #Output folder string
        self.output.folder_value.value = self.vars.folder.threedi_results.path
        
    def download_sqlite(self):
        """Download sqlite of selected revision to temporary folder"""
        schema_id = self.selected_schema_id
        rev_id = self.selected_revision_id

        if schema_id is not None and rev_id is not None:
            output_path = Path(f"tempfiles/schema{schema_id}_rev{rev_id}.zip")
            output_path.parent.mkdir(exist_ok=True) #Create parent folder
            if not output_path.with_suffix('').exists():
                if not output_path.exists():
                    sqlite_dnwld = self.vars.sim.threedi_api.schematisations_revisions_sqlite_download(id=int(rev_id), schematisation_pk=int(schema_id))
                    r = requests.get(sqlite_dnwld.get_url)
                    with open(output_path, 'wb') as f:
                        f.write(r.content)

                #unpack zip
                zip_ref = zipfile.ZipFile(output_path, "r")
                zip_ref.extractall(output_path.with_suffix(''))
                zip_ref.close()

            self.vars.sqlite_path = [i for i in output_path.with_suffix('').glob('*sqlite')][0]

    def _activate_button_color(self, button):
        """Make active button green and rest of rain buttons grey"""
        for button_grey in [
                self.rain.test_0d1d_button,
                self.rain.test_1d2d_button,
                self.rain.test_hour_button,
                self.rain.T10_blok_button,
                self.rain.T100_blok_button,
                self.rain.T1000_blok_button,
                self.rain.T10_piek_button,
                self.rain.T100_piek_button,
                self.rain.T1000_piek_button,
                self.rain.custom_rain_button,
            ]:
            button_grey.style.button_color = None
        button.style.button_color = "lightgreen"


    def _update_calc_settings_buttons(self, **kwargs):
        """set buttons for basic_processsing, damage_processing, etc.."""
        for kwarg in kwargs:
            getattr(self.calc_settings,kwarg).value=kwargs[kwarg]


    def _update_button_icon(self, value):
        """Add icons to buttons based on their state"""
        try:
            # change the icon of the same button that was observed.
            button = value["owner"]  
        except:
            button = value  # if function is not called with observe
        if button.disabled == True:
            button.icon = "minus"  # https://fontawesome.com/icons?d=gallery
        else:
            # When button is not selected but available remove icon.
            button.icon = "plus"
            if button.value == True:
                button.icon = "check"  # https://fontawesome.com/icons?d=gallery

    @property
    def selected_schema_id(self):
        try:
            return self.model.schema_dropdown.value.split(' -')[0]
        except:
            return None

    @property
    def selected_revision_id(self):
        try:
            return self.model.revision_dropdown.value.split(' -')[0]
        except:
            return None

    @property
    def sim(self):
        return self.caller.vars.sim

    @property
    def vars(self):
        return self.caller.vars


class GuiVariables:
    def __init__(self):
        self._folder = None

        self.sim = None #Is filled when pressing login
        self.schematisations = {} #Is filled when searching for models by name
        self.revisions = {}
        self.threedimodels = {}
        self.sqlite_dropdown_options = {}
        self.sqlite_path = None #Sqlite is downloaded and placed here.
        self.organisations = {}
        self.api_keys = {"lizard":"", "threedi":""}
        self.rain_settings_view = None #offset[s], duration[s], value[mm/h]

        
    @property
    def folder(self):
        return self._folder

    @folder.setter
    def main_folder(self, main_folder):
        self._folder = Folders(main_folder, create=False)

    @property
    def schema_dropdown_viewlist(self):
        schema_list = [f"{idx} - {self.schematisations[idx].name}" for idx in self.schematisations]
        schema_list.reverse()
        return schema_list

    @property
    def revision_dropdown_viewlist(self):
        revision_list = []
        for idx in self.revisions:
            rev = self.revisions[idx]
            try:
                commit_date = rev.commit_date.strftime('%y/%m/%d-%H:%M:%S') or None
            except:
                commit_date=None
            # if rev.is_valid:
            revision_list.append(f"{idx} - #{rev.number} - {commit_date} - valid:{rev.is_valid} - {rev.commit_user}")
        return revision_list

    @property
    def threedimodel_dropdown_viewlist(self):
        threedimodel_list = []
        for rev in self.revisions.keys():
            if rev in self.threedimodels.keys():
                threedimodel_list.append(f"{self.threedimodels[rev].id}")
            else:
                threedimodel_list.append(f"Revison has no model")
        return threedimodel_list
        
    @property
    def sqlite_dropdown_viewlist(self):
        self.folder.model.set_modelsplitter_paths() #set all paths in model_settings.xlsx
        for schema in self.folder.model.schema_list:
            if getattr(self.folder.model, schema).database.exists:
                schemafolder=  getattr(self.folder.model, schema)
                viewname = f"{schemafolder.pl.name}/{schemafolder.database.pl.name}" 
                self.sqlite_dropdown_options[viewname] =schemafolder

        return self.sqlite_dropdown_options.keys()

    @property
    def organisations_viewlist(self):
        return self.organisations.keys()

    @property
    def rain_settings(self):
        if self.rain_settings_view is None:
            return None

        r_settings = []
        for r in self.rain_settings_view.split(';'):
            rainsplit = r.split(',')
            if len(rainsplit) ==3:
                r_settings.append({
                    "offset": int(eval(rainsplit[0])*3600), #hours -> seconds
                    "duration": int(eval(rainsplit[1])*3600), #hours -> seconds
                    "value": eval(rainsplit[2])/(1000*3600), #mm/hour -> m/s
                    "units": "m/s",
                })
            else:
                raise Exception(f"Custom rain settings not valid")
        return r_settings


class StartCalculationGui:
    def __init__(
        self, data=None, base_scenario_name=None, 
        lizard_api_key=None, threedi_api_key=None, main_folder=None, 
    ):

        self.vars = GuiVariables()
        self.widgets = StartCalculationWidgetsInteraction(self)


        if data:
            self.widgets.update_api_keys(api_keys_path=data["api_keys_path"])
            self.vars.main_folder = data["polder_folder"]
        else:
            self.vars.api_keys["lizard"] = lizard_api_key
            self.vars.api_keys["threedi"] = threedi_api_key
            self.vars.main_folder = main_folder

        self.widgets.update_folder()
        # self.base_scenario_name = base_scenario_name
           

        # if base_scenario_name is None:
        #     self.base_scenario_name_str = ""
        # else:
        #     self.base_scenario_name_str = f"{self.base_scenario_name} "

        if not self.vars.main_folder:
            self.vars.main_folder = os.getcwd()

        self.scheduler = BlockingScheduler(timezone="Europe/Amsterdam")

        # self.scenarios = self._init_scenarios()




        self.start_calculation_tab = widgets.GridBox(
            children=[
                self.w.login.label,
                self.w.login.button,
                self.w.login.lizard_apikey_widget,
                self.w.login.threedi_apikey_widget,  # 1 login
                self.w.model.label,
                self.w.model.schema_name_widget,
                self.w.model.search_button,
                self.w.model.schema_label,
                self.w.model.select_label,
                self.w.model.schema_dropdown,
                self.w.model.revision_label,
                self.w.model.revision_dropdown,
                self.w.model.threedimodel_label,
                self.w.model.threedimodel_dropdown,
                self.w.model.organisation_label,
                self.w.model.organisation_box,
                # self.w.model.sqlite_label,
                # self.w.model.sqlite_dropdown,
                # self.w.model.sqlite_chooser,
                self.w.rain.label,
                self.w.rain.test_0d1d_button,
                self.w.rain.test_1d2d_button,
                self.w.rain.test_hour_button,
                self.w.rain.T10_blok_button,
                self.w.rain.T100_blok_button,
                self.w.rain.T1000_blok_button,
                self.w.rain.T10_piek_button,
                self.w.rain.T100_piek_button,
                self.w.rain.T1000_piek_button,
                self.w.rain.rain_event_widget,
            
                self.w.rain.custom_rain_button,
                self.w.rain.custom_rain_label,
                self.w.rain.custom_rain_text,
                
                self.w.output.label,
                self.w.output.folder_label,
                self.w.output.folder_value,
                self.w.output.subfolder_label,
                self.w.output.subfolder_box,
                self.w.calc_settings.label,
                self.w.calc_settings.basic_processing,
                self.w.calc_settings.damage_processing,
                self.w.calc_settings.arrival_processing,
                self.w.calc_settings.structure_control,
                self.w.calc_settings.laterals,
                self.w.feedback.label,
                self.w.feedback.widget,
                self.w.start.label,
                self.w.start.create_simulation_button,
                self.w.start.start_button,
                ],  # 8
            layout=widgets.Layout(
                width="100%",
                grid_row_gap="200px 200px 200px 200px",
                #             grid_template_rows='auto auto auto 50px auto 40px auto 20px 40px',
                grid_template_rows="auto auto auto",
                grid_template_columns="1% 12% 8% 10% 10% 14% 15% 15% 15%",
                grid_template_areas="""
                '. login_label login_label model_label model_label schema_select_label schema_select_label schema_select_label schema_select_label' 
                '. lizard_apikey_widget lizard_apikey_widget . . schema_label schema_dropdown schema_dropdown schema_dropdown' 
                '. threedi_apikey_widget threedi_apikey_widget schema_name_widget schema_name_widget revision_label revision_dropdown revision_dropdown revision_dropdown' 
                '. login_button login_button model_name_search_button model_name_search_button threedimodel_label threedimodel_dropdown threedimodel_dropdown threedimodel_dropdown' 
                '. . . . . organisation_label organisation_box organisation_box organisation_box' 
                '. rain_label . . . output_label output_label output_label output_label' 
                '. test_0d1d_button rain_event_widget rain_event_widget rain_event_widget output_folder_label output_folder_value output_folder_value output_folder_value'
                '. test_1d2d_button rain_event_widget rain_event_widget rain_event_widget output_subfolder_label output_subfolder_box output_subfolder_box output_subfolder_box'
                '. hour_test_button rain_event_widget rain_event_widget rain_event_widget . . . .'
                '. T10_blok_button rain_event_widget rain_event_widget rain_event_widget . . . .'
                '. T100_blok_button rain_event_widget rain_event_widget rain_event_widget calc_settings_label calc_settings_label calc_settings_label calc_settings_label'
                '. T1000_blok_button rain_event_widget rain_event_widget rain_event_widget . calc_settings_basic_processing calc_settings_damage_processing calc_settings_arrival_processing' 
                '. T10_piek_button rain_event_widget rain_event_widget rain_event_widget . calc_settings_structure_control calc_settings_laterals .' 
                '. T100_piek_button rain_event_widget rain_event_widget rain_event_widget . . . .' 
                '. T1000_piek_button rain_event_widget rain_event_widget rain_event_widget . . . .' 
                '. . custom_rain_label custom_rain_label custom_rain_label . . . .' 
                '. custom_rain_button custom_rain_text custom_rain_text custom_rain_text . . . .'
                '. feedback_label . . . . . start_label start_label'
                '. feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget create_simulation_button create_simulation_button'
                '. feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget start_button start_button'
                '. . . . . . . . .' 
                """,
            ))





    @property
    def w(self):
        return self.widgets

    ###################################################################################################
    # Layout of the GUI
    ###################################################################################################
    # initialize functions and dictorionaries that can be called
    def _init_scenarios(self):
        scenarios = {}  # This dict is filled certain buttons are pressed
        scenarios["names"] = []  # Names of models that can be downloaded
        scenarios["selected_folder"] = ""  # 03 hyd toets or 05extreme data
        scenarios["results"] = ""
        scenarios["model_type"] = ""
        scenarios["api_data"] = {}  # API call
        scenarios["api_data_json"] = ""

        # Fetch the first folder
        scenarios["folder"] = Folders(self.main_folder, create=False)
        return scenarios



if __name__ == '__main__':
    data = {'polder_folder': 'E:\\02.modellen\\model_test_v2',
 'api_keys_path': 'C:\\Users\\wvangerwen\\AppData\\Roaming\\3Di\\QGIS3\\profiles\\default\\python\\plugins\\hhnk_threedi_plugin\\api_key.txt'}
    self = StartCalculationGui(data=data); 
    display(self.start_calculation_tab)

    self.widgets.model.schema_name_widget.value='hub_0d1d'
    # self.widgets.model.search_button.click()

## TODO
# Vind de juiste modellen?
# GUI wat simplificeren.
# %%

# # Globals
# from hhnk_threedi_tools.variables.api_settings import (
#     RAIN_SETTINGS,
#     RAIN_TYPES,
#     RAIN_INTENSITY,
#     GROUNDWATER,
#     RAIN_SCENARIOS,
#     API_SETTINGS,
#     MODEL_TYPES,
# )

# simulation = None
# batch_started = False


# # #  %% TESTNNG %% ##
# # main_folder = "C:/Users/wvangerwen/Github/hhnk-threedi-tools/hhnk_threedi_tools/tests/data/multiple_polders"
# # # main_folder=None
# # base_scenario_name=None
# # lizard_api_key=LIZARD_API_KEY
# # if True:
# # #  %% TESTNNG %% ##
# def start_calculation_gui(
#     main_folder=None, base_scenario_name=None, lizard_api_key=None, data=None
# ):

#     if data:
#         api_keys = read_api_file(data["api_keys_path"])
#         main_folder = data["polder_folder"]
#     else:
#         api_keys = {}
#         api_keys["lizard"] = lizard_api_key
#         api_keys["threedi"] = ""

#     if not api_keys["lizard"]:
#         raise ValueError(
#             """Please fill in the lizard api key.\n
#                          Log in and create your own at: https://hhnk.lizard.net/management/personal_api_keys
#                          """
#         )
#     dl.LIZARD_URL = "https://hhnk.lizard.net/api/v3/"
#     THREEDI_API_HOST = "https://api.3di.live/v3"
#     RESULT_LIMIT = 100
#     dl.set_api_key(api_keys["lizard"])

#     if base_scenario_name is None:
#         base_scenario_name_str = ""
#     else:
#         base_scenario_name_str = f"{base_scenario_name} "

#     scheduler = BlockingScheduler(timezone="Europe/Amsterdam")

#     # TODO remove, replaced by API_KEY
#     # # Change threediscenario downloader header
#     # def new_get_headers():
#     #     """Setting the headers in the original toolbox is not easy when using this GUI.
#     #     Therefore we change this function in the toolbox so everything else works."""
#     #     headers_results = {
#     #         "username": "{}".format(lizard_apikey_widget.value),
#     #         "password": "{}".format(threedi_apikey_widget.value),
#     #         "Content-Type": "application/json",
#     #     }
#     #     return headers_results

#     # setattr(dl, "get_headers", new_get_headers)

#     def item_layout(width="95%", grid_area="", **kwargs):
#         return widgets.Layout(
#             width=width, grid_area=grid_area, **kwargs
#         )  # override the default width of the button to 'auto' to let the button grow

#     ###################################################################################################
#     # Layout of the GUI
#     ###################################################################################################
#     # initialize functions and dictorionaries that can be called
#     scenarios = {}  # This dict is filled certain buttons are pressed
#     scenarios["names"] = []  # Names of models that can be downloaded
#     scenarios["selected_folder"] = ""  # 03 hyd toets or 05extreme data
#     scenarios["results"] = ""
#     scenarios["model_type"] = ""
#     scenarios["api_data"] = {}  # API call
#     scenarios["api_data_json"] = ""

#     if not main_folder:
#         main_folder = os.getcwd()

#     # Fetch the first folder
#     scenarios["folder"] = Folders(main_folder, create=False)

#     def update_folders(polder_name):
#         # folder = Folders(os.path.join(main_folder, polder_name), create=False)
#         output_polder_dropdown.value = polder_name
#         # scenarios["folder"] = folder

#     # # --------------------------------------------------------------------------------------------------
#     # # 0. Select polder folder
#     # # --------------------------------------------------------------------------------------------------

#     # polders_folder_selection = FileChooser(os.getcwd())
#     # polders_folder_selection.layout.grid_area = "polders_folder"

#     # polders_folder_label = widgets.Label(
#     #     "Select the folder where multiple polders are situated",
#     #     layout=item_layout(grid_area="polders_folder_label"),
#     # )

#     # --------------------------------------------------------------------------------------------------
#     # 1. Login with 3Di account
#     # --------------------------------------------------------------------------------------------------
#     login_label = widgets.HTML(
#         "<b>1. Login with API keys</b>", layout=item_layout(grid_area="login_label")
#     )

#     class ApikeyWidget(widgets.Text):
#         _view_name = Unicode("PasswordView").tag(sync=True)

#     # Api key widgets
#     lizard_apikey_widget = ApikeyWidget(
#         description="Lizard key:",
#         layout=item_layout(width="261px", grid_area="lizard_apikey"),
#     )

#     threedi_apikey_widget = ApikeyWidget(
#         description="Threedi Key:",
#         layout=item_layout(width="261px", grid_area="threedi_apikey"),
#     )

#     # Login button, after login create threedi api client
#     login_button = widgets.Button(
#         description="Login", layout=item_layout(height="30px", grid_area="login_button")
#     )

#     @login_button.on_click
#     def login(action):
#         global sim

#         sim = Simulation(
#             api_key=api_keys["threedi"]
#         )  # username=lizard_apikey_widget.value, password=threedi_apikey_widget.value,

#         try:
#             sim.logged_in
#             # Login success
#             login_button.style.button_color = "lightgreen"
#             login_button.description = "Logged in"
#             logout_button.disabled = False
#         except:
#             # Login failed
#             login_button.style.button_color = "red"
#             login_button.description = "Try again"
#             logout_button.disabled = True

#     logout_button = widgets.Button(
#         description="Logout",
#         layout=item_layout(height="30px", grid_area="logout_button"),
#         disabled=True,
#     )

#     @logout_button.on_click
#     def logout(action):
#         global sim

#         # lizard_apikey_widget.value = ""
#         # threedi_apikey_widget.value = ""
#         sim = None

#         login_button.style.button_color = None
#         login_button.description = "Login"
#         logout_button.disabled = True

#     # --------------------------------------------------------------------------------------------------
#     # 2. Select polder (and show revision)
#     # --------------------------------------------------------------------------------------------------
#     select_polder_label = widgets.HTML(
#         "<b>2. Search model</b>",
#         layout=item_layout(grid_area="select_polder_label"),
#     )

#     # Polder name widget
#     polder_name_label = widgets.Label(
#         "Model name:", layout=item_layout(grid_area="polder_name_label")
#     )
#     polder_name_widget = widgets.Text(
#         layout=item_layout(grid_area="polder_name_widget")
#     )
#     polder_name_search_button = widgets.Button(
#         description="Search models",
#         layout=item_layout(height="30px", grid_area="polder_name_search_button"),
#     )

#     @polder_name_search_button.on_click
#     def search_models(action):
#         global sim
#         repo_list = sim.threedi_api.schematisations_list(
#             slug__icontains=polder_name_widget.value, limit=RESULT_LIMIT
#         ).results

#         slug_list = []
#         for result in repo_list:
#             slug_list.append(result.slug)

#         repository_dropdown.options = slug_list

#     # Polder revision widget
#     # model_revision_label = widgets.Label(
#     #     "Model revision:", layout=item_layout(grid_area="model_rev_label")
#     # )
#     # model_revision_widget = widgets.Text(
#     #     layout=item_layout(grid_area="model_rev_widget"), disabled=True
#     # )

#     # --------------------------------------------------------------------------------------------------
#     # 3. Go to model repository and make model visible
#     # --------------------------------------------------------------------------------------------------
#     get_slug_label = widgets.HTML(
#         "<b>3. Go to model repository and make model visible</b>",
#         layout=item_layout(grid_area="get_slug_label"),
#     )

#     repository_label = widgets.Label(
#         "Schematisation: ", layout=item_layout(grid_area="repository_label")
#     )
#     repository_dropdown = widgets.Dropdown(
#         layout=item_layout(grid_area="repository_dropdown")
#     )

#     # Link to model repository (batch only for now)
#     # link_to_model_repository = widgets.HTML(
#     #     layout=item_layout(grid_area="link_to_model_repository")
#     # )

#     revision_label = widgets.Label(
#         "3Di basis model: ", layout=item_layout(grid_area="revision_label")
#     )
#     revision_dropdown = widgets.Dropdown(
#         layout=item_layout(grid_area="revision_dropdown")
#     )

#     # Model name widget
#     model_name_label = widgets.Label(
#         "Model name:", layout=item_layout(grid_area="model_name_label")
#     )
#     model_slug_widget = widgets.Text(
#         layout=item_layout(grid_area="model_slug_widget")
#     )  # TODO deprecated
#     model_name_dropdown = widgets.Dropdown(
#         layout=item_layout(grid_area="model_name_dropdown")
#     )

#     # Model slug widget
#     model_name_glg_label = widgets.Label(
#         "Model name glg:", layout=item_layout(grid_area="model_name_glg_label")
#     )
#     model_name_glg_dropdown = widgets.Dropdown(
#         layout=item_layout(grid_area="model_name_glg_dropdown")
#     )
#     # Model slug widget
#     model_name_ggg_label = widgets.Label(
#         "Model name ggg:", layout=item_layout(grid_area="model_name_ggg_label")
#     )
#     model_name_ggg_dropdown = widgets.Dropdown(
#         layout=item_layout(grid_area="model_name_ggg_dropdown")
#     )
#     # Model slug widget
#     model_name_ghg_label = widgets.Label(
#         "Model name ghg:", layout=item_layout(grid_area="model_name_ghg_label")
#     )
#     model_name_ghg_dropdown = widgets.Dropdown(
#         layout=item_layout(grid_area="model_name_ghg_dropdown")
#     )

#     # --------------------------------------------------------------------------------------------------
#     # 4. Select rain event
#     # --------------------------------------------------------------------------------------------------
#     rain_event_label = widgets.HTML(
#         "<b>4. Select rain event</b>", layout=item_layout(grid_area="rain_event_label")
#     )

#     def create_rain_event_widget():
#         """Create interactive plot with sliders for various input parameters"""

#         def plot_rain_event(
#             days_dry_start,
#             hours_dry_start,
#             days_rain,
#             hours_rain,
#             days_dry_end,
#             hours_dry_end,
#             rain_intensity,
#         ):
#             def create_plot(x, y, title, xlabel, ylabel):
#                 fig, ax = plt.subplots(figsize=[10, 3])
#                 plt.plot(x, y)
#                 plt.title(title)
#                 plt.xlabel(xlabel)
#                 plt.ylabel(ylabel)
#                 ax.grid()
#                 return fig, ax

#             # Create timeseries
#             dt = 1 / 24

#             part1 = [
#                 round(x, 2)
#                 for x in np.arange(
#                     0, days_dry_start + hours_dry_start / 24, dt
#                 ).tolist()
#             ]
#             part2 = [
#                 round(x, 2)
#                 for x in np.arange(
#                     days_dry_start + hours_dry_start / 24,
#                     days_dry_start + hours_dry_start / 24 + days_rain + hours_rain / 24,
#                     dt,
#                 ).tolist()
#             ]
#             part3 = [
#                 round(x, 2)
#                 for x in np.arange(
#                     days_dry_start + hours_dry_start / 24 + days_rain + hours_rain / 24,
#                     days_dry_start
#                     + hours_dry_start / 24
#                     + days_rain
#                     + hours_rain / 24
#                     + days_dry_end
#                     + hours_dry_end / 24
#                     + dt,
#                     dt,
#                 ).tolist()
#             ]

#             part1 = np.arange(0, days_dry_start + hours_dry_start / 24, dt).tolist()
#             part2 = np.arange(
#                 days_dry_start + hours_dry_start / 24,
#                 days_dry_start + hours_dry_start / 24 + days_rain + hours_rain / 24,
#                 dt,
#             ).tolist()
#             part3 = np.arange(
#                 days_dry_start + hours_dry_start / 24 + days_rain + hours_rain / 24,
#                 days_dry_start
#                 + hours_dry_start / 24
#                 + days_rain
#                 + hours_rain / 24
#                 + days_dry_end
#                 + hours_dry_end / 24
#                 + dt,
#                 dt,
#             ).tolist()

#             if part2:  # part2 is the rain part
#                 time = part1 + [part2[0]] + part2 + [part3[0]] + part3
#             else:
#                 time = part1 + part2 + [part3[0]] + part3

#             # Create rain intensity timeseries
#             rain = np.zeros(np.size(time))

#             # Add some indices for nicer plotting
#             if days_dry_start == 0 and hours_dry_start == 0:
#                 value_start = 0
#             else:
#                 value_start = 1
#             if days_dry_end == 0 and hours_dry_end == 0:
#                 value_end = 3
#             else:
#                 value_end = 1

#             if days_rain != 0 or hours_rain != 0:
#                 rain[
#                     time.index(days_dry_start + hours_dry_start / 24)
#                     + value_start : time.index(
#                         days_dry_start
#                         + hours_dry_start / 24
#                         + days_rain
#                         + hours_rain / 24
#                     )
#                     + value_end
#                 ] = rain_intensity

#             fig, ax = create_plot(
#                 time, rain, "Rain event", "Time [days]", "Rain intensity [mm/hour]"
#             )
#             # fig.show()

#         # Comebine plot and sliders
#         style = {"description_width": "100px"}
#         rain_event_widget = widgets.interactive(
#             plot_rain_event,
#             days_dry_start=widgets.FloatSlider(
#                 value=1,
#                 min=0,
#                 max=5,
#                 step=1,
#                 style=style,
#                 layout=item_layout(),
#                 continuous_update=False,
#             ),
#             hours_dry_start=widgets.FloatSlider(
#                 value=0,
#                 min=0,
#                 max=23,
#                 step=1,
#                 style=style,
#                 layout=item_layout(),
#                 continuous_update=False,
#             ),
#             days_rain=widgets.FloatSlider(
#                 value=5,
#                 min=0,
#                 max=10,
#                 step=1,
#                 style=style,
#                 layout=item_layout(),
#                 continuous_update=False,
#             ),
#             hours_rain=widgets.FloatSlider(
#                 value=0,
#                 min=0,
#                 max=23,
#                 step=1,
#                 style=style,
#                 layout=item_layout(),
#                 continuous_update=False,
#             ),
#             days_dry_end=widgets.FloatSlider(
#                 value=2,
#                 min=0,
#                 max=5.0,
#                 step=1,
#                 style=style,
#                 layout=item_layout(),
#                 continuous_update=False,
#             ),
#             hours_dry_end=widgets.FloatSlider(
#                 value=0,
#                 min=0,
#                 max=23,
#                 step=1,
#                 style=style,
#                 layout=item_layout(),
#                 continuous_update=False,
#             ),
#             rain_intensity=widgets.FloatSlider(
#                 value=4.167,
#                 min=0,
#                 max=100,
#                 step=0.01,
#                 style=style,
#                 description="rain [mm/hour]",
#                 layout=item_layout(),
#                 continuous_update=False,
#             ),
#         )

#         # Give this widget a name so it can be placed in the grid
#         rain_event_widget.layout = widgets.Layout(grid_area="rain_event_widget")
#         return rain_event_widget

#     rain_event_widget = create_rain_event_widget()

#     # hydraulic test; 5 days rain
#     hyd_test_button = widgets.Button(
#         description="Hyd test (0d1d)", layout=item_layout(grid_area="hyd_test_button")
#     )

#     @hyd_test_button.on_click
#     def change_rain(action):
#         update_rain_event_widget(
#             days_dry_start=1,
#             hours_dry_start=0,
#             days_rain=5,
#             hours_rain=0,
#             days_dry_end=2,
#             hours_dry_end=0,
#             rain_intensity=100
#             / 24,  # 100mm/day, using impervious surface mapping makes 14.4mm/day and 11.5mm/day
#         )
#         activate_button_color(hyd_test_button)
#         output_folder_box.value = output_folder_box.options[0]  # hyd test folder
#         update_scenario_name_widget()

#     # Change rain event api call.
#     hour_test_button = widgets.Button(
#         description="1 hour test",
#         layout=item_layout(grid_area="hour_test_button", justify_self="end"),
#     )

#     @hour_test_button.on_click
#     def change_rain(action):
#         update_rain_event_widget(
#             days_dry_start=0,
#             hours_dry_start=0,
#             days_rain=0,
#             hours_rain=1,
#             days_dry_end=0,
#             hours_dry_end=0,
#             rain_intensity=2400 / 24,
#         )
#         activate_button_color(hour_test_button)
#         output_folder_box.value = output_folder_box.options[0]  # hyd test folder
#         update_scenario_name_widget(" 1hour")

#     # 1d2d test: T10 rain peak. 35.5mm total
#     test_1d2d_button = widgets.Button(
#         description="1d2d test",
#         layout=item_layout(grid_area="test_1d2d_button", justify_self="end"),
#     )

#     @test_1d2d_button.on_click
#     def change_rain(action):
#         update_rain_event_widget(
#             days_dry_start=0,
#             hours_dry_start=1,
#             days_rain=0,
#             hours_rain=2,
#             days_dry_end=0,
#             hours_dry_end=12,
#             rain_intensity=426 / 24,
#         )
#         activate_button_color(test_1d2d_button)
#         output_folder_box.value = output_folder_box.options[0]  # hyd test folder
#         update_scenario_name_widget()

#     T10_blok_button = widgets.Button(
#         description="T10_blok", layout=item_layout(grid_area="T10_blok_button")
#     )

#     @T10_blok_button.on_click
#     def change_rain(action):
#         rain_type = "blok"
#         update_rain_event_widget(
#             days_dry_start=RAIN_SETTINGS[rain_type]["days_dry_start"],
#             hours_dry_start=RAIN_SETTINGS[rain_type]["hours_dry_start"],
#             days_rain=RAIN_SETTINGS[rain_type]["days_rain"],
#             hours_rain=RAIN_SETTINGS[rain_type]["hours_rain"],
#             days_dry_end=RAIN_SETTINGS[rain_type]["days_dry_end"],
#             hours_dry_end=RAIN_SETTINGS[rain_type]["hours_dry_end"],
#             rain_intensity=RAIN_INTENSITY[rain_type]["T10"],
#         )
#         activate_button_color(T10_blok_button)
#         output_folder_box.value = output_folder_box.options[1]  # extreme test folder
#         update_scenario_name_widget(" T10_blok")

#     T100_blok_button = widgets.Button(
#         description="T100_blok", layout=item_layout(grid_area="T100_blok_button")
#     )

#     @T100_blok_button.on_click
#     def change_rain(action):
#         rain_type = "blok"
#         update_rain_event_widget(
#             days_dry_start=RAIN_SETTINGS[rain_type]["days_dry_start"],
#             hours_dry_start=RAIN_SETTINGS[rain_type]["hours_dry_start"],
#             days_rain=RAIN_SETTINGS[rain_type]["days_rain"],
#             hours_rain=RAIN_SETTINGS[rain_type]["hours_rain"],
#             days_dry_end=RAIN_SETTINGS[rain_type]["days_dry_end"],
#             hours_dry_end=RAIN_SETTINGS[rain_type]["hours_dry_end"],
#             rain_intensity=RAIN_INTENSITY[rain_type]["T100"],
#         )
#         activate_button_color(T100_blok_button)
#         output_folder_box.value = output_folder_box.options[1]  # extreme test folder
#         update_scenario_name_widget(" T100_blok")

#     T1000_blok_button = widgets.Button(
#         description="T1000_blok", layout=item_layout(grid_area="T1000_blok_button")
#     )

#     @T1000_blok_button.on_click
#     def change_rain(action):
#         rain_type = "blok"
#         update_rain_event_widget(
#             days_dry_start=RAIN_SETTINGS[rain_type]["days_dry_start"],
#             hours_dry_start=RAIN_SETTINGS[rain_type]["hours_dry_start"],
#             days_rain=RAIN_SETTINGS[rain_type]["days_rain"],
#             hours_rain=RAIN_SETTINGS[rain_type]["hours_rain"],
#             days_dry_end=RAIN_SETTINGS[rain_type]["days_dry_end"],
#             hours_dry_end=RAIN_SETTINGS[rain_type]["hours_dry_end"],
#             rain_intensity=RAIN_INTENSITY[rain_type]["T1000"],
#         )
#         activate_button_color(T1000_blok_button)
#         output_folder_box.value = output_folder_box.options[1]  # extreme test folder
#         update_scenario_name_widget(" T1000_blok")

#     T10_piek_button = widgets.Button(
#         description="T10_piek", layout=item_layout(grid_area="T10_piek_button")
#     )

#     @T10_piek_button.on_click
#     def change_rain(action):
#         rain_type = "piek"
#         update_rain_event_widget(
#             days_dry_start=RAIN_SETTINGS[rain_type]["days_dry_start"],
#             hours_dry_start=RAIN_SETTINGS[rain_type]["hours_dry_start"],
#             days_rain=RAIN_SETTINGS[rain_type]["days_rain"],
#             hours_rain=RAIN_SETTINGS[rain_type]["hours_rain"],
#             days_dry_end=RAIN_SETTINGS[rain_type]["days_dry_end"],
#             hours_dry_end=RAIN_SETTINGS[rain_type]["hours_dry_end"],
#             rain_intensity=RAIN_INTENSITY[rain_type]["T10"],
#         )
#         activate_button_color(T10_piek_button)
#         output_folder_box.value = output_folder_box.options[1]  # extreme test folder
#         update_scenario_name_widget(" T10_piek")

#     T100_piek_button = widgets.Button(
#         description="T100_piek", layout=item_layout(grid_area="T100_piek_button")
#     )

#     @T100_piek_button.on_click
#     def change_rain(action):
#         rain_type = "piek"
#         update_rain_event_widget(
#             days_dry_start=RAIN_SETTINGS[rain_type]["days_dry_start"],
#             hours_dry_start=RAIN_SETTINGS[rain_type]["hours_dry_start"],
#             days_rain=RAIN_SETTINGS[rain_type]["days_rain"],
#             hours_rain=RAIN_SETTINGS[rain_type]["hours_rain"],
#             days_dry_end=RAIN_SETTINGS[rain_type]["days_dry_end"],
#             hours_dry_end=RAIN_SETTINGS[rain_type]["hours_dry_end"],
#             rain_intensity=RAIN_INTENSITY[rain_type]["T100"],
#         )
#         activate_button_color(T100_piek_button)
#         output_folder_box.value = output_folder_box.options[1]  # extreme test folder
#         update_scenario_name_widget(" T100_piek")

#     T1000_piek_button = widgets.Button(
#         description="T1000_piek", layout=item_layout(grid_area="T1000_piek_button")
#     )

#     @T1000_piek_button.on_click
#     def change_rain(action):
#         rain_type = "piek"
#         update_rain_event_widget(
#             days_dry_start=RAIN_SETTINGS[rain_type]["days_dry_start"],
#             hours_dry_start=RAIN_SETTINGS[rain_type]["hours_dry_start"],
#             days_rain=RAIN_SETTINGS[rain_type]["days_rain"],
#             hours_rain=RAIN_SETTINGS[rain_type]["hours_rain"],
#             days_dry_end=RAIN_SETTINGS[rain_type]["days_dry_end"],
#             hours_dry_end=RAIN_SETTINGS[rain_type]["hours_dry_end"],
#             rain_intensity=RAIN_INTENSITY[rain_type]["T1000"],
#         )
#         activate_button_color(T1000_piek_button)
#         output_folder_box.value = output_folder_box.options[1]  # extreme test folder
#         update_scenario_name_widget(" T1000_piek")

#     # --------------------------------------------------------------------------------------------------
#     # 4. BATCH: Select scenarios to be run
#     # --------------------------------------------------------------------------------------------------
#     scenario_label = widgets.HTML(
#         "<b>4. Select scenarios to be run</b>",
#         layout=item_layout(grid_area="scenario_label"),
#     )

#     def create_scenario_box(GROUNDWATER, RAIN_SCENARIOS):
#         """Create scenario box to select which results are to be started.
#         groundwater = condition of grounderwater (determines which model is run)
#         rain_scenarios = rain event to be put on model

#         output;
#         scenario_box is for viewing in the gui
#         calc_scenarios is the HBox of every row. This can be used to get the values.
#         """
#         row_widget = {}
#         calc_scenarios = {}
#         for row in [
#             ""
#         ] + GROUNDWATER:  # loop over groundwater conditions (first entry is for headers)
#             for (
#                 col
#             ) in (
#                 RAIN_SCENARIOS
#             ):  # add togglebutton for every scenario (T10, T100, T1000)

#                 if row == "":  # headers
#                     row_widget[col] = widgets.HTML(
#                         "<b><center>{}</center></b>".format(col),
#                         layout=item_layout(width="100%"),
#                     )
#                 else:
#                     row_widget[col] = widgets.ToggleButton(
#                         value=True, layout=item_layout(), icon="check"
#                     )

#                 calc_scenarios[row] = widgets.HBox(
#                     [row_widget[col] for col in row_widget], layout=item_layout()
#                 )  # combine these in a row per calc type
#                 calc_scenarios[row].children += (
#                     widgets.HTML("<b>{}</b>".format(row), layout=item_layout()),
#                 )

#         scenario_box = widgets.VBox(
#             [calc_scenarios[row] for row in calc_scenarios],
#             layout=item_layout(grid_area="scenario_box"),
#         )
#         return calc_scenarios, scenario_box

#     calc_scenarios, scenario_box = create_scenario_box(GROUNDWATER, RAIN_SCENARIOS)

#     # buttons for Piek and Blok
#     rain_type_widgets = {}
#     for rain_type in RAIN_TYPES:
#         rain_type_widgets[rain_type] = widgets.ToggleButton(
#             value=True, description=rain_type, layout=item_layout(), icon="check"
#         )
#     rain_type_box = widgets.VBox(
#         [rain_type_widgets[t] for t in rain_type_widgets],
#         layout=item_layout(grid_area="rain_type_box"),
#     )  # combine these in a row per calc type

#     batch_scenario_name_label = widgets.Label(
#         "Batch scenario name:",
#         layout=item_layout(grid_area="batch_scenario_name_label"),
#     )
#     batch_scenario_name_widget_extra = widgets.Text(
#         layout=item_layout(grid_area="batch_scenario_name_widget_extra")
#     )
#     batch_scenario_name_widget = widgets.Text(
#         disabled=True, layout=item_layout(grid_area="batch_scenario_name_widget")
#     )

#     # --------------------------------------------------------------------------------------------------
#     # 5. Select output folder/name
#     # --------------------------------------------------------------------------------------------------
#     output_folder_label = widgets.HTML(
#         "<b>5. Select output folder/name</b>",
#         layout=item_layout(grid_area="output_folder_label"),
#     )

#     # dropdown box with polders (output folder)
#     output_polder_label = widgets.Label(
#         "Output folder:", layout=item_layout(grid_area="output_polder_label")
#     )

#     # sorted([naam for naam in next(os.walk(main_folder))[1] if naam[0:3] not in ['00.','999','.ip']])
#     output_polder_dropdown = widgets.Dropdown(
#         options=["03_3di_resultaten"],
#         layout=item_layout(grid_area="output_polder_dropdown"),
#     )

#     # Selection box of the folder the output should be put in. (Hyd toets or Extreme)
#     output_folder_options = ["1d2d_results", "0d1d_results", "batch_results"]
#     output_subfolder_label = widgets.Label(
#         "Sub folder:", layout=item_layout(grid_area="output_subfolder_label")
#     )
#     output_folder_box = widgets.Select(
#         options=output_folder_options,
#         rows=3,
#         disabled=False,
#         layout=item_layout(grid_area="output_folder_box"),
#     )

#     sqlite_selection = FileChooser(main_folder)
#     sqlite_selection.layout.grid_area = "sqlite_selection"

#     sqlite_selection_label = widgets.Label(
#         "Select sqlite containing structure control rules (are not yet automatically applied)",
#         layout=item_layout(grid_area="sqlite_selection_label"),
#     )

#     def change_sqlite_selection():
#         update_create_simulation_button()
#         update_start_batch_button()

#     sqlite_selection.register_callback(change_sqlite_selection)

#     # Scenario name the model result will be saved to.
#     scenario_name_label = widgets.Label(
#         "Scenario name:", layout=item_layout(grid_area="scenario_name_label")
#     )
#     scenario_name_widget = widgets.Text(
#         layout=item_layout(grid_area="scenario_name_widget")
#     )

#     organisation_options = API_SETTINGS["org_uuid"].keys()
#     organisation_label = widgets.Label(
#         "Organisation:", layout=item_layout(grid_area="organisation_label")
#     )
#     organisation_box = widgets.Select(
#         options=organisation_options,
#         rows=len(organisation_options),
#         disabled=False,
#         layout=item_layout(height="60px", grid_area="organisation_box"),
#     )

#     # --------------------------------------------------------------------------------------------------
#     # 6. API call
#     # --------------------------------------------------------------------------------------------------
#     API_call_label = widgets.HTML(
#         "<b>6. API call</b>", layout=item_layout(grid_area="API_call_label")
#     )

#     # API call HTML widget that shows the API call
#     API_call_widget = widgets.HTML(layout=item_layout(grid_area="API_call_widget"))

#     # --------------------------------------------------------------------------------------------------
#     # 7. Start calculation
#     # --------------------------------------------------------------------------------------------------
#     start_calculation_label = widgets.HTML(
#         "<b>7. Start calculation</b>",
#         layout=item_layout(grid_area="start_calculation_label"),
#     )

#     start_button = widgets.Button(
#         description="Start calculation",
#         layout=item_layout(height="90%", grid_area="start_button"),
#     )

#     @start_button.on_click
#     def start(action):
#         start_simulation()

#     create_simulation_button = widgets.Button(
#         description="Create simulation",
#         layout=item_layout(height="90%", grid_area="create_simulation_button"),
#     )

#     @create_simulation_button.on_click
#     def create_simulation_click(action):
#         global simulation

#         if simulation is None:
#             # Create simulation
#             create_simulation()
#         else:
#             # Erase existing simulation
#             simulation = None
#             disable_input(False)
#             API_call_widget.value = ""
#             create_simulation_button.description = "Create simulation"

#         update_start_simulation_button()

#     start_and_download_button = widgets.Button(
#         description="Start and download",
#         layout=item_layout(height="90%", grid_area="start_and_download_button"),
#     )
#     # @start_and_download_button.on_click
#     # def start_and_download(action):
#     #    start_calculation(wait_to_download=1)

#     start_batch_button = widgets.Button(
#         description="Start batch calculation",
#         layout=item_layout(height="50px", grid_area="start_batch_button"),
#     )

#     @start_batch_button.on_click
#     def start(action):
#         start_batch_simulation()

#         global batch_started
#         batch_started = True

#         update_start_batch_button()

#     # --------------------------------------------------------------------------------------------------
#     # 8. Available results
#     # --------------------------------------------------------------------------------------------------
#     available_results_label = widgets.HTML(
#         "<b>8. Available results</b>",
#         layout=item_layout(grid_area="available_results_label"),
#     )

#     # API call HTML widget that shows the API call
#     available_results_widget = widgets.HTML(
#         layout=item_layout(grid_area="available_results_widget")
#     )

#     ###################################################################################################
#     # Updating and interaction of the widgets
#     ###################################################################################################

#     # --------------------------------------------------------------------------------------------------
#     # 1. Login with 3Di account
#     # --------------------------------------------------------------------------------------------------

#     # --------------------------------------------------------------------------------------------------
#     # 2. Select polder (and show revision)
#     # --------------------------------------------------------------------------------------------------
#     # scenario name bijwerken = ook api call updaten
#     # Link bijwerken

#     def on_polder_change(polder):
#         update_model_repo_link()  # Update link to the model repo with the polder
#         update_scenario_name_widget()  # Update scenario name and API call
#         update_batch_scenario_name_widget()

#         # Update the output folder to match the polder name
#         try:
#             update_folders(polder_name_widget.value)
#             output_polder_dropdown.value = os.listdir(main_folder)
#         except:
#             pass

#     polder_name_widget.observe(on_polder_change, names="value")

#     # --------------------------------------------------------------------------------------------------
#     # 3. Go to model repository and make model visible
#     # --------------------------------------------------------------------------------------------------

#     # Observe dropdown box, when the value changes, update other stuff as well
#     def on_select_schematisation(selected_repository):
#         # global threedi_rev_api
#         global sim

#         # Find all models in the specified repository which are not disabled (due to maximum available revisions)
#         model_list = sim.threedi_api.threedimodels_list(
#             slug__icontains=selected_repository["new"], disabled=False, limit=1000
#         ).results
#         revision_numbers = []
#         revisions = []

#         # Add revision numbers of available models to the revision number list
#         for model in model_list:
#             if model.revision_number not in revision_numbers:
#                 revision_string = f"{model.revision_number} - {model.revision_commit_date[:19]} - {model.user}".format()
#                 revisions.insert(0, revision_string)

#         # TODO sorting by revision number removed, may need to be checked
#         # revision_numbers.sort()

#         revision_dropdown.options = revisions
#         update_create_simulation_button()
#         update_start_batch_button()

#     repository_dropdown.observe(on_select_schematisation, names="value")

#     def on_select_revision(selected_revision):
#         global sim
#         try:
#             selected_revision = selected_revision["new"]
#         except:
#             pass

#         # revision_number = str.split(selected_revision)[0]

#         # Search for models within selected revision
#         model_list = sim.threedi_api.threedimodels_list(
#             name__startswith=polder_name_widget.value,
#             # revision__number=revision_number,
#             limit=100,
#         ).results
#         model_list_rev = [
#             f"{model.revision_number}@{model.name}" for model in model_list
#         ]
#         model_list = [model.name for model in model_list]

#         models = {v: [] for v in ["glg", "ggg", "ghg"]}
#         for model in model_list_rev:

#             if "glg" in model:
#                 models["glg"].append(model)
#             if "ggg" in model:
#                 models["ggg"].append(model)
#             if "ghg" in model:
#                 models["ghg"].append(model)

#         model_name_dropdown.options = model_list
#         model_name_glg_dropdown.options = models["glg"]
#         model_name_ggg_dropdown.options = models["ggg"]
#         model_name_ghg_dropdown.options = models["ghg"]

#         # # Select glg, ggg, ghg for batch download. Select None if multiple found.
#         # def analyze_options(options, search_str):
#         #     """if more than one option, return None"""
#         #     options = [a for a in options if search_str in a]
#         #     if len(options) != 1:
#         #         return None
#         #     else:
#         #         return options[0]

#         # model_name_glg_dropdown.value = analyze_options(
#         #     options=model_name_glg_dropdown.options, search_str="glg"
#         # )
#         # model_name_ggg_dropdown.value = analyze_options(
#         #     options=model_name_ggg_dropdown.options, search_str="ggg"
#         # )
#         # model_name_ghg_dropdown.value = analyze_options(
#         #     options=model_name_ghg_dropdown.options, search_str="ghg"
#         # )

#         # update the scenario name
#         # update_scenario_name_widget()
#         # update_batch_scenario_name_widget()
#         update_create_simulation_button()
#         update_start_batch_button()

#     revision_dropdown.observe(on_select_revision, names="value")

#     def on_select_model(selected_model):
#         # update the scenario name
#         update_scenario_name_widget()
#         update_create_simulation_button()
#         update_start_batch_button()

#     model_name_dropdown.observe(on_select_model, names="value")
#     model_name_glg_dropdown.observe(on_select_model, names="value")
#     model_name_ggg_dropdown.observe(on_select_model, names="value")
#     model_name_ghg_dropdown.observe(on_select_model, names="value")

#     def get_models():
#         """
#         Get a model (or list of model) based on the repository, revision and model name dropdowns. Not used for batch.
#         """
#         global sim
#         revision_number = str.split(revision_dropdown.value)[0]
#         return sim.threedi_api.threedimodels_list(
#             slug__startswith=repository_dropdown.value,
#             revision__number=revision_number,
#             name=model_name_dropdown.value,
#             limit=100,
#         ).results

#     # TODO deprecated?
#     def update_model_repo_link():
#         model_url = "https://3di.lizard.net/models/repos/?search={}".format(
#             polder_name_widget.value
#         )  # Url to polder specific repo
#         # link_to_model_repository.value = '<u><a href="'+model_url+'" target="_blank">Click here to go to model repository</a></u>'

#     # model revision updaten
#     # scenario name updaten

#     def on_slug_change(slug):
#         update_scenario_name_widget()
#         update_batch_scenario_name_widget()

#     model_slug_widget.observe(on_slug_change, names="value")

#     # --------------------------------------------------------------------------------------------------
#     # 4. Select rain event
#     # --------------------------------------------------------------------------------------------------

#     def activate_button_color(button):
#         """Make active button green and rest grey"""
#         for button_grey in [
#             hour_test_button,
#             hyd_test_button,
#             test_1d2d_button,
#             T10_blok_button,
#             T100_blok_button,
#             T1000_blok_button,
#             T10_piek_button,
#             T100_piek_button,
#             T1000_piek_button,
#         ]:

#             button_grey.style.button_color = None
#         button.style.button_color = "lightgreen"

#     def update_rain_event_widget(
#         days_dry_start,
#         hours_dry_start,
#         days_rain,
#         hours_rain,
#         days_dry_end,
#         hours_dry_end,
#         rain_intensity,
#     ):
#         """Update sliders with input values, used for buttons."""
#         rain_event_widget.children[0].value = days_dry_start
#         rain_event_widget.children[1].value = hours_dry_start
#         rain_event_widget.children[2].value = days_rain
#         rain_event_widget.children[3].value = hours_rain
#         rain_event_widget.children[4].value = days_dry_end
#         rain_event_widget.children[5].value = hours_dry_end
#         rain_event_widget.children[6].value = rain_intensity

#     # def on_rain_change(scenario_name):
#     #    update_API_call_widget()
#     # for i in range(0,len(rain_event_widget.children)):
#     #    rain_event_widget.children[i].observe(on_rain_change, names='value') #observe the input parameters

#     # --------------------------------------------------------------------------------------------------
#     # 4. BATCH Select scenarios to be run
#     # --------------------------------------------------------------------------------------------------
#     # Observe buttons that select which scenario to start
#     def update_button_icon(value):
#         """Add icons to buttons based on their state"""
#         try:
#             button = value[
#                 "owner"
#             ]  # change the icon of the smae button that was observed.
#         except:
#             button = value  # if function is not called with observe
#         if button.disabled == True:
#             button.icon = "minus"  # https://fontawesome.com/icons?d=gallery
#         else:
#             button.icon = (
#                 "plus"  # When button is not selected but available remove icon.
#             )
#             if button.value == True:
#                 button.icon = "check"  # https://fontawesome.com/icons?d=gallery

#     # Observe all scenario_box buttons
#     for button in [
#         button for columnbox in scenario_box.children for button in columnbox.children
#     ]:
#         button.observe(update_button_icon, "value")
#     # Observe all rain_type buttons
#     for button in [button for button in rain_type_box.children]:
#         button.observe(update_button_icon, "value")

#     # --------------------------------------------------------------------------------------------------
#     # 5. Select output folder/name
#     # --------------------------------------------------------------------------------------------------

#     # Observe dropdown box, when the value changes, update other stuff as well
#     def on_select_change(selected_polder):
#         try:
#             selected_polder = selected_polder["new"]
#         except:
#             pass
#         update_folders(selected_polder)  # [3:]) #TODO iets doen met nummering polders.
#         sqlite_selection.default_path = scenarios["folder"].model.path

#     output_polder_dropdown.observe(on_select_change, names="value")

#     def on_folder_change(selected_folder=""):
#         """Observe if hyd test or extreme calculation folder is selected"""
#         try:
#             selected_folder = selected_folder["new"]
#         except:
#             pass
#         if (selected_folder in ["1d2d_results"]) and selected_folder != "":
#             folder = "1d2d_results"
#         elif selected_folder == "batch_results":
#             folder = "batch_results"
#         else:
#             folder = "0d1d_results"
#         scenarios["selected_folder"] = folder

#     output_folder_box.observe(on_folder_change, "value")

#     # scenario name
#     def update_scenario_name_widget(extra_name=""):
#         """Build scenario name from various input parameters (polder name, revision, model type, run type)"""
#         # retrieve revision and model type from the model slug.
#         if revision_dropdown.value is None:
#             model_revision = ""
#         else:
#             model_revision = str.split(revision_dropdown.value)[0]

#         model_type = ""

#         for model_types in MODEL_TYPES:
#             if model_name_dropdown.value is not None:
#                 if model_types in model_name_dropdown.value:
#                     model_type = model_types
#                     scenarios[
#                         "model_type"
#                     ] = model_type  # save to dict for use outside function

#         polder_name = polder_name_widget.value if polder_name_widget.value else "Polder"

#         # Build scenario name
#         scenario_name = (
#             base_scenario_name_str
#             + polder_name
#             + " #"
#             + str(model_revision)
#             + " "
#             + model_type
#             + extra_name
#         )
#         scenario_name = scenario_name.strip()
#         # Update model revision
#         # if model_revision: #Only update if its not empty
#         #    model_revision_widget.value = model_revision
#         # else:
#         #    model_revision_widget.value='enter model slug'

#         # Update widget value
#         scenario_name_widget.value = scenario_name

#     def update_batch_scenario_name_widget(placeholder=""):
#         if revision_dropdown.value is None:
#             model_revision = ""
#         else:
#             model_revision = str.split(revision_dropdown.value)[0]

#         scenario_name = base_scenario_name_str + "{polder} #{revision} {batch_extra_name}".format(
#             polder=polder_name_widget.value,
#             revision=model_revision,
#             # i=len(scenarios["folder"].threedi_results.batch.revisions),
#             batch_extra_name=batch_scenario_name_widget_extra.value,
#         )

#         # scenario_name = base_scenario_name_str + "{polder} #{revision} {groundwater_type} {rain_type} {rain_scenario} ({i}) {batch_extra_name}".format(
#         #     polder=polder_name_widget.value,
#         #     revision=model_revision,
#         #     groundwater_type="GLG",
#         #     rain_type="piek",
#         #     rain_scenario="T1000",
#         #     i="3",
#         #     batch_extra_name=batch_scenario_name_widget_extra.value,
#         # )
#         scenario_name = scenario_name.strip()
#         batch_scenario_name_widget.value = scenario_name

#         # update_API_call_widget() #check if name is valid for use in a scenario name

#     batch_scenario_name_widget_extra.observe(update_batch_scenario_name_widget)

#     #    def on_scenario_name_change(scenario_name):
#     #        update_API_call_widget()
#     #    scenario_name_widget.observe(on_scenario_name_change, names='value')

#     #    def on_organisation_change(organisation):
#     #        #update_API_call_widget()
#     #    organisation_box.observe(on_organisation_change, names='value')

#     # --------------------------------------------------------------------------------------------------
#     # 6. API call
#     # --------------------------------------------------------------------------------------------------

#     def update_API_call_widget_v3(simulation):
#         global sim

#         events = sim.threedi_api.simulations_events(id=simulation.id)

#         rain = events.timeseriesrain[0]
#         rain_offset = rain.offset / 3600
#         rain_duration = rain.duration / 3600
#         rain_intensity = rain.values[0][1] * 1000 * 3600

#         controlstructures = len(events.tablestructurecontrols)
#         laterals = len(events.laterals)
#         model_slug = simulation.slug
#         start_datetime = simulation.start_datetime.strftime("%Y-%m-%d %H:%M")
#         end_datetime = simulation.end_datetime.strftime("%Y-%m-%d %H:%M")
#         organisation_name = simulation.organisation_name
#         simulation_id = simulation.id
#         scenario_name = simulation.name

#         simulation_url = THREEDI_API_HOST + "/simulations/" + str(simulation_id)

#         html_text = "Simulation id: <a href={}>{}</a><br>Scenario name: {}<br>Model slug: {}<br>Organisation name: {}<br>Start time: {}<br>End time: {}<br>Rain event: {}<br>Rain starts after (h): {}<br>Rain duration (h): {}<br>Rain intensity (mm/h): {}<br># Control structures: {}<br># Laterals: {}".format(
#             simulation_url,
#             simulation_id,
#             scenario_name,
#             model_slug,
#             organisation_name,
#             start_datetime,
#             end_datetime,
#             rain,
#             rain_offset,
#             rain_duration,
#             rain_intensity,
#             controlstructures,
#             laterals,
#         )

#         API_call_widget.value = '<p style="line-height:1.4">' + html_text + "</p>"

#     # --------------------------------------------------------------------------------------------------
#     # 7. Start calculation
#     # --------------------------------------------------------------------------------------------------
#     def update_create_simulation_button():
#         """
#         Make sure the following data is supplied:
#             - Repository
#             - Revision
#             - Model name
#             - Sqlite
#         When clicked disable again
#         """
#         global simulation

#         if simulation is None:
#             if (
#                 repository_dropdown.value is not None
#                 and revision_dropdown.value is not None
#                 # and model_name_dropdown.value is not None #FIXME tijdelijk uitgezet voor starten heemskerk
#                 # and sqlite_selection.selected is not None
#             ):
#                 create_simulation_button.disabled = False
#                 create_simulation_button.style.button_color = "lightgreen"
#             else:
#                 create_simulation_button.disabled = True
#                 create_simulation_button.style.button_color = "red"
#         else:
#             create_simulation_button.description = "Change simulation"
#             create_simulation_button.disabled = False
#             create_simulation_button.style.button_color = "lightgreen"

#     def update_start_simulation_button():
#         """
#         Make sure the simulation is created
#         When clicked disable again
#         """
#         if simulation is None:
#             start_button.disabled = True
#             start_button.style.button_color = "red"
#         elif (
#             sim.threedi_api.simulations_status_list(simulation_pk=simulation.id).name
#             == "created"
#         ):
#             start_button.disabled = False
#             start_button.style.button_color = "lightgreen"
#         else:
#             start_button.disabled = True
#             start_button.style.button_color = "lightgreen"
#             start_button.description = sim.threedi_api.simulations_status_list(
#                 simulation_pk=simulation.id
#             ).name

#     def update_start_batch_button():
#         global batch_started

#         if not batch_started:
#             if (
#                 repository_dropdown.value is not None
#                 and revision_dropdown.value is not None
#                 and model_name_glg_dropdown.value is not None
#                 and model_name_ggg_dropdown.value is not None
#                 and model_name_ghg_dropdown.value is not None
#                 and sqlite_selection.selected is not None
#             ):
#                 start_batch_button.disabled = False
#                 start_batch_button.style.button_color = "lightgreen"
#             else:
#                 start_batch_button.disabled = True
#                 start_batch_button.style.button_color = "red"
#         else:
#             start_batch_button.disabled = True
#             start_batch_button.style.button_color = "lightgreen"
#             start_batch_button.description = "Batch started"

#     def create_simulation():
#         """
#         Creates (but does not initialize or start) a simulation
#         """
#         global sim
#         # global threedi_api_client
#         global simulation

#         days_dry_start = rain_event_widget.children[0].value
#         hours_dry_start = int(rain_event_widget.children[1].value)
#         days_rain = rain_event_widget.children[2].value
#         hours_rain = int(rain_event_widget.children[3].value)
#         days_dry_end = rain_event_widget.children[4].value
#         hours_dry_end = int(rain_event_widget.children[5].value)
#         rain_intensity = rain_event_widget.children[6].value

#         scenario_name = scenario_name_widget.value

#         # output_folder = os.path.join(
#         #     str(scenarios["folder"].threedi_results),
#         #     scenarios["selected_folder"],
#         #     scenario_name_widget.value,
#         # )
#         organisation_uuid = organisation_uuid = API_SETTINGS["org_uuid"][
#             organisation_box.value
#         ]
#         basic_processing = True
#         damage_processing = True
#         arrival_processing = False

#         models = get_models()
#         model_id = None
#         if len(models) != 1:
#             print("No, or more than 1 model found")
#         else:
#             model_id = models[0].id

#         sqlite = sqlite_selection.selected

#         output_folder = os.path.join(
#             str(scenarios["folder"].threedi_results),
#             scenarios["selected_folder"],
#             scenario_name_widget.value,
#         ) #FIXME dubbel, staat ook in def start_simulation(wait_to_download=0)

#         simulation = create_threedi_simulation(
#             sim=sim,
#             sqlite_file=sqlite,
#             scenario_name=scenario_name,
#             model_id=model_id,
#             organisation_uuid=organisation_uuid,
#             days_dry_start=days_dry_start,
#             hours_dry_start=hours_dry_start,
#             days_rain=days_rain,
#             hours_rain=hours_rain,
#             days_dry_end=days_dry_end,
#             hours_dry_end=hours_dry_end,
#             rain_intensity=rain_intensity,
#             basic_processing=basic_processing,
#             damage_processing=damage_processing,
#             arrival_processing=arrival_processing,
#             output_folder=output_folder,
#         )
#         update_API_call_widget_v3(simulation)
#         update_available_results()
#         update_create_simulation_button()
#         update_start_simulation_button()
#         update_start_batch_button()

#         disable_input(True)

#     def disable_input(disable=True):

#         widgets = [
#             lizard_apikey_widget,
#             threedi_apikey_widget,
#             login_button,
#             polder_name_widget,
#             polder_name_search_button,
#             repository_dropdown,
#             model_name_dropdown,
#             revision_dropdown,
#             hour_test_button,
#             hyd_test_button,
#             test_1d2d_button,
#             T10_blok_button,
#             T100_blok_button,
#             T1000_blok_button,
#             T10_piek_button,
#             T100_piek_button,
#             T1000_piek_button,
#             output_polder_dropdown,
#             output_folder_box,
#             sqlite_selection,
#             scenario_name_widget,
#             organisation_box,
#         ]

#         for widget in widgets:
#             widget.disabled = disable

#         interactive_widgets = [rain_event_widget]

#         for widget in interactive_widgets:
#             for child in sqlite_selection.children:
#                 if hasattr(child, "disabled"):
#                     child.disabled = disable

#                 if hasattr(child, "children"):
#                     for grandchild in child.children:
#                         grandchild.disabled = disable

#     def start_simulation(wait_to_download=0):
#         """
#         Starts the previously created simulation
#         """
#         output_folder = os.path.join(
#             str(scenarios["folder"].threedi_results),
#             scenarios["selected_folder"],
#             scenario_name_widget.value,
#         )

#         sim.threedi_api.simulations_actions_create(
#             simulation_pk=simulation.id, data={"name": "queue"}
#         )
#         update_start_simulation_button()

#         monitor_simulation()

#         if wait_to_download == 1:
#             revision_number = int(str.split(revision_dropdown.value)[0])
#             wait_to_download_results(
#                 dl,
#                 scenario_name_widget.value,
#                 polder_name_widget.value,
#                 revision_number,
#                 output_folder,
#                 os.path.join(output_folder, "call_log.log"),
#             )

#     def monitor_simulation():
#         """
#         Monitor the progress of the simulation and update the 'start simulation' button to show the progress
#         """

#         def update_start_button():
#             status_name = sim.threedi_api.simulations_status_list(
#                 simulation_pk=simulation.id
#             ).name

#             try:
#                 progress = sim.threedi_api.simulations_progress_list(
#                     simulation.id, async_req=False
#                 ).percentage
#             except:
#                 progress = 0
#             start_button.description = "{} ({}%)".format(status_name, progress)

#             if status_name == "finished":
#                 scheduler.shutdown(wait=False)

#         # print("schedule monitoring task")

#         scheduler.add_job(update_start_button, IntervalTrigger(timezone="Europe/Amsterdam"), seconds=10)
#         scheduler.start()  # Start the scheduled job

#     def start_batch_simulation():
#         """start batch calculation for all climate scenarios using api v3"""
#         global sim

#         def get_all_model_idx() -> dict:
#             """return threedi model ids of the selected glg, ggg and ghg model"""

#             def get_model_idx(name):
#                 if "ggg" in name or "ghg" in name or "glg" in name:
#                     revision_number = name.split("@")[0]
#                     name = name.split("@")[-1]
#                     results = sim.threedi_api.threedimodels_list(
#                         revision__number=revision_number,
#                         name=name,
#                         limit=100,
#                     ).results

#                 else:
#                     revision_number = str.split(revision_dropdown.value)[0]
#                     results = sim.threedi_api.threedimodels_list(
#                         slug__startswith=repository_dropdown.value,
#                         revision__number=revision_number,
#                         name=name,
#                         limit=100,
#                     ).results

#                 if len(results) != 1:
#                     raise Exception(f"model '{name}' is not unique or not found.")
#                 else:
#                     return results[0].id

#             model_idx = {}

#             model_idx["1d2d_glg"] = get_model_idx(model_name_glg_dropdown.value)
#             model_idx["1d2d_ggg"] = get_model_idx(model_name_ggg_dropdown.value)
#             model_idx["1d2d_ghg"] = get_model_idx(model_name_ghg_dropdown.value)

#             # TODO batch sommen werkt nog niet met nieuwe schematisaties
#             # gw = "glg"
#             # if gw not in model_name_glg_dropdown.value:
#             #     raise Exception(f"{gw} Model name should contain {gw}")
#             # gw = "ggg"
#             # if gw not in model_name_ggg_dropdown.value:
#             #     raise Exception(f"{gw} Model name should contain {gw}")
#             # gw = "ghg"
#             # if gw not in model_name_ghg_dropdown.value:
#             #     raise Exception(f"{gw} Model name should contain {gw}")
#             return model_idx

#         model_idx = get_all_model_idx()

#         print("Batch calculation started for {}:".format(polder_name_widget.value))
#         revision_number = str.split(revision_dropdown.value)[0]

#         output_folder = os.path.join(
#             str(scenarios["folder"].threedi_results.batch),
#             batch_scenario_name_widget.value,
#         )

#         apicall_txt = os.path.join(output_folder, "APIcall.txt")  # Path to apicall log
#         # logger_path = os.path.join(output_folder, 'call_log.log') #Path to logger

#         simulations = []
#         all_api_calls = {}

#         # update available results not to start a simulation that already exists
#         update_available_results()

#         i = 0
#         for rain_type in RAIN_TYPES:
#             for groundwater_type in GROUNDWATER:  # these are row values (GLG, GGG, GHG)
#                 selected_scenarios = [
#                     child.value for child in calc_scenarios[groundwater_type].children
#                 ][: len(RAIN_SCENARIOS)]
#                 for index, rain_scenario in enumerate(
#                     RAIN_SCENARIOS
#                 ):  # (T10, T100, T1000)

#                     i += 1
#                     if (
#                         rain_type_widgets[rain_type].value == True
#                     ):  # if the rain type (blok/piek) is selected, check which of the 9 scenarios are selected.
#                         # check if button for rain_scenario(T10) and groundwater (GLG) is selected
#                         if selected_scenarios[index] == True:

#                             # Create Data JSON for API call (initialize variables here)
#                             days_dry_start = RAIN_SETTINGS[rain_type]["days_dry_start"]
#                             hours_dry_start = RAIN_SETTINGS[rain_type][
#                                 "hours_dry_start"
#                             ]
#                             days_rain = RAIN_SETTINGS[rain_type]["days_rain"]
#                             hours_rain = RAIN_SETTINGS[rain_type]["hours_rain"]
#                             days_dry_end = RAIN_SETTINGS[rain_type]["days_dry_end"]
#                             hours_dry_end = RAIN_SETTINGS[rain_type]["hours_dry_end"]
#                             rain_intensity = RAIN_INTENSITY[rain_type][rain_scenario]
#                             organisation_uuid = API_SETTINGS["org_uuid"][
#                                 organisation_box.value
#                             ]

#                             if groundwater_type == "GLG":
#                                 model_id = model_idx["1d2d_glg"]
#                             elif groundwater_type == "GGG":
#                                 model_id = model_idx["1d2d_ggg"]
#                             elif groundwater_type == "GHG":
#                                 model_id = model_idx["1d2d_ghg"]
#                             else:
#                                 model_id = None
#                             # model_slug = model_slugs[[a for a in MODEL_TYPES if groundwater_type.lower() in a][0]] #e.g.: select '1d2d_ggg' for 'GGG'

#                             scenario_name = (
#                                 base_scenario_name_str
#                                 + "{polder} #{revision} {groundwater_type} {rain_type} {rain_scenario} ({i}) {batch_extra_name}".format(
#                                     polder=polder_name_widget.value,
#                                     revision=revision_number,
#                                     groundwater_type=groundwater_type,
#                                     rain_type=rain_type,
#                                     rain_scenario=rain_scenario,
#                                     i=i,
#                                     batch_extra_name=batch_scenario_name_widget_extra.value,
#                                 )
#                             )
#                             scenario_name = scenario_name.strip()

#                             # Remove leading and trailing spaces
#                             # print("Scenario name: {}".scenario_name)
#                             basic_processing = True  # include rasters in results
#                             damage_processing = True
#                             arrival_processing = False

#                             # check if scnario is already available
#                             print(
#                                 "Checking if a scenario with the following name already exists: {}".format(
#                                     scenario_name
#                                 )
#                             )

#                             # available_scenarios = []
#                             # for index,_ in enumerate(scenarios['results']):
#                             #    print(scenarios['results'][index]['name'])
#                             #    available_scenarios.append(scenarios['results'][index]['name'])

#                             # if scenario_name in available_scenarios:
#                             #    print("Already available, not starting")
#                             # else:
#                             #    print("Not yet available, starting")
#                             # return
#                             scenario_names = [
#                                 scenarios["results"][i]["name"]
#                                 for i, _ in enumerate(scenarios["results"])
#                             ]
#                             if scenario_name not in scenario_names:
#                                 #                                 print('Scenario_name {} not in the following list: {}'.format(scenario_name,[scenarios['results'][i]['name'] for i,_ in enumerate(scenarios['results'])]))

#                                 # Create Data dict
#                                 # data = create_3Di_start_API_call_data(days_dry_start, hours_dry_start, days_rain, hours_rain,
#                                 #                                      days_dry_end, hours_dry_end, rain_intensity, organisation_uuid, model_slug, scenario_name, store_results)

#                                 # call start calculation script
#                                 print(
#                                     "++ Creating simulation: {}".format(scenario_name)
#                                 )
#                                 #                                 display(data)

#                                 sqlite_file = sqlite_selection.selected

#                                 while True:
#                                     try:
#                                         simulation = create_threedi_simulation(
#                                             sim=sim,
#                                             sqlite_file=sqlite_file,
#                                             scenario_name=scenario_name,
#                                             model_id=model_id,
#                                             organisation_uuid=organisation_uuid,
#                                             days_dry_start=days_dry_start,
#                                             hours_dry_start=hours_dry_start,
#                                             days_rain=days_rain,
#                                             hours_rain=hours_rain,
#                                             days_dry_end=days_dry_end,
#                                             hours_dry_end=hours_dry_end,
#                                             rain_intensity=rain_intensity,
#                                             basic_processing=basic_processing,
#                                             damage_processing=damage_processing,
#                                             arrival_processing=arrival_processing,
#                                         )
#                                     except ApiException:
#                                         time.sleep(10)
#                                         continue
#                                     break

#                                 print(
#                                     "Created simulation with id: {}".format(
#                                         simulation.id
#                                     )
#                                 )
#                                 simulations.append(simulation)

#                                 all_api_calls[
#                                     "{} General".format(scenario_name)
#                                 ] = simulation

#                                 while True:
#                                     try:
#                                         all_api_calls[
#                                             "{} Events".format(scenario_name)
#                                         ] = sim.threedi_api.simulations_events(
#                                             id=simulation.id
#                                         )
#                                     except ApiException:
#                                         time.sleep(10)
#                                         continue
#                                     break
#                                 # start_3di_calculation(data, json.dumps(data), lizard_apikey_widget.value, threedi_apikey_widget.value, output_folder, apicall_txt, batch=1)

#                             else:
#                                 print(
#                                     "-- Scenario is already available: {}".format(
#                                         scenario_name
#                                     )
#                                 )

#         # Create folder and write all api calls to file
#         print(output_folder)
#         if not os.path.exists(output_folder) and output_folder != "":
#             os.mkdir(output_folder)
#             print("Created folder: " + output_folder.rsplit("/")[-1])

#         # copy qgis project to batch folder
#         # shutil.copy(qgis_file, output_qgis_file)

#         apicall_txt_base = apicall_txt
#         n = 1
#         while os.path.exists(
#             apicall_txt
#         ):  # if the batch apicall already exists, make a new one with another number.
#             apicall_txt = "{}_{}.txt".format(apicall_txt_base.rsplit(".txt")[0], n)
#             n += 1
#         with open(apicall_txt, "w") as outfile:
#             outfile.write(pprint.pformat(all_api_calls))

#         # start the simulation
#         for simulation in simulations:
#             print("Starting all the created simulations")
#             sim.threedi_api.simulations_actions_create(
#                 simulation_pk=simulation.id, data={"name": "queue"}
#             )

#     # --------------------------------------------------------------------------------------------------
#     # 8. Available results
#     # --------------------------------------------------------------------------------------------------
#     def update_available_results():
#         revision_number = int(str.split(revision_dropdown.value)[0])

#         scenarios["results"] = dl.find_scenarios(
#             model_name=polder_name_widget.value,
#             model_revision=revision_number,
#             limit=50,
#         )

#         available_results_text = ""
#         for index, scenario in enumerate(scenarios["results"]):
#             if index != 0:
#                 available_results_text = available_results_text + "<br>"
#             available_results_text = available_results_text + scenario["name"]

#         # Update the widget
#         available_results_widget.value = (
#             '<p style="line-height:1.4">' + available_results_text + "</p>"
#         )

#     # --------------------------------------------------------------------------------------------------
#     # Initialize GUI
#     # --------------------------------------------------------------------------------------------------

#     update_model_repo_link()  # Create a blank link to model repo
#     hyd_test_button.click()  # Apply the settings of a hyd test as default
#     on_folder_change()  # select 'HydToets_data' and put it in memory
#     update_scenario_name_widget()  # Update scenario name and API call
#     update_batch_scenario_name_widget()  # Update batch scenario name

#     ###################################################################################################
#     # Create GUI
#     ###################################################################################################
#     start_calculation_tab = widgets.GridBox(
#         children=[
#             login_label,
#             lizard_apikey_widget,
#             threedi_apikey_widget,
#             login_button,
#             logout_button,  # 1 login
#             select_polder_label,
#             polder_name_label,
#             polder_name_widget,
#             polder_name_search_button,  # 2 select polder
#             get_slug_label,
#             repository_label,
#             repository_dropdown,
#             model_name_label,
#             model_name_dropdown,
#             revision_label,
#             revision_dropdown,  # 3
#             rain_event_label,
#             rain_event_widget,
#             hour_test_button,
#             hyd_test_button,
#             test_1d2d_button,  # 4
#             T10_blok_button,
#             T100_blok_button,
#             T1000_blok_button,
#             T10_piek_button,
#             T100_piek_button,
#             T1000_piek_button,  # 4
#             output_folder_label,
#             output_polder_label,
#             output_polder_dropdown,
#             output_subfolder_label,  # 5
#             output_folder_box,
#             sqlite_selection_label,
#             sqlite_selection,
#             scenario_name_label,
#             scenario_name_widget,
#             organisation_label,
#             organisation_box,  # 5
#             API_call_label,
#             API_call_widget,  # 6
#             start_calculation_label,
#             start_button,
#             create_simulation_button,  # 7
#             available_results_label,
#             available_results_widget,
#         ],  # 8
#         layout=widgets.Layout(
#             width="100%",
#             grid_row_gap="200px 200px 200px 200px",
#             #             grid_template_rows='auto auto auto 50px auto 40px auto 20px 40px',
#             grid_template_rows="auto auto auto",
#             grid_template_columns="12% 8% 10% 10% 18% 10% 10% 10% 10%",
#             grid_template_areas="""
#             'login_label login_label . select_polder_label select_polder_label get_slug_label get_slug_label get_slug_label get_slug_label'
#             'lizard_apikey lizard_apikey lizard_apikey polder_name_label polder_name_widget  repository_label repository_dropdown repository_dropdown repository_dropdown'
#             'threedi_apikey threedi_apikey threedi_apikey . polder_name_search_button revision_label revision_dropdown revision_dropdown revision_dropdown'
#             'login_button logout_button . . . model_name_label model_name_dropdown model_name_dropdown model_name_dropdown'
#             'rain_event_label rain_event_label . . . output_folder_label output_folder_label . .'
#             '. rain_event_widget rain_event_widget rain_event_widget rain_event_widget output_polder_label output_polder_dropdown output_polder_dropdown output_polder_dropdown'
#             'hyd_test_button rain_event_widget rain_event_widget rain_event_widget rain_event_widget output_subfolder_label output_folder_box output_folder_box output_folder_box'
#             'hour_test_button rain_event_widget rain_event_widget rain_event_widget rain_event_widget scenario_name_label scenario_name_widget scenario_name_widget scenario_name_widget'
#             'test_1d2d_button rain_event_widget rain_event_widget rain_event_widget rain_event_widget organisation_label organisation_box organisation_box organisation_box'
#             '. rain_event_widget rain_event_widget rain_event_widget rain_event_widget sqlite_selection_label sqlite_selection_label sqlite_selection_label sqlite_selection_label'
#             'T10_blok_button rain_event_widget rain_event_widget rain_event_widget rain_event_widget sqlite_selection sqlite_selection sqlite_selection sqlite_selection'
#             'T100_blok_button rain_event_widget rain_event_widget rain_event_widget rain_event_widget sqlite_selection sqlite_selection sqlite_selection sqlite_selection'
#             'T1000_blok_button rain_event_widget rain_event_widget rain_event_widget rain_event_widget sqlite_selection sqlite_selection sqlite_selection sqlite_selection'
#             'T10_piek_button rain_event_widget rain_event_widget rain_event_widget rain_event_widget sqlite_selection sqlite_selection sqlite_selection sqlite_selection'
#             'T100_piek_button rain_event_widget rain_event_widget rain_event_widget rain_event_widget sqlite_selection sqlite_selection sqlite_selection sqlite_selection'
#             'T1000_piek_button rain_event_widget rain_event_widget rain_event_widget rain_event_widget sqlite_selection sqlite_selection sqlite_selection sqlite_selection'
#             'API_call_label . . . . . . start_calculation_label start_calculation_label'
#             'API_call_widget API_call_widget API_call_widget API_call_widget API_call_widget API_call_widget API_call_widget create_simulation_button create_simulation_button'
#             'API_call_widget API_call_widget API_call_widget API_call_widget API_call_widget API_call_widget API_call_widget start_button start_button'
#             'available_results_label available_results_label . . . . . . .'
#             'available_results_widget available_results_widget available_results_widget available_results_widget . . . . .'
#             """,
#         ),
#     )

#     start_batch_calculation_tab = widgets.GridBox(
#         children=[
#             login_label,
#             lizard_apikey_widget,
#             threedi_apikey_widget,
#             login_button,
#             logout_button,  # 1 login
#             select_polder_label,
#             polder_name_label,
#             polder_name_widget,
#             polder_name_search_button,  # 2 select polder
#             get_slug_label,
#             repository_label,
#             repository_dropdown,
#             revision_label,
#             revision_dropdown,
#             model_name_glg_label,
#             model_name_ggg_label,
#             model_name_ghg_label,
#             model_name_glg_dropdown,
#             model_name_ggg_dropdown,
#             model_name_ghg_dropdown,  # 3
#             scenario_label,
#             rain_type_box,
#             scenario_box,  # 4
#             output_folder_label,
#             output_polder_label,
#             output_polder_dropdown,
#             output_subfolder_label,  # 5
#             sqlite_selection_label,
#             sqlite_selection,
#             output_folder_box,
#             batch_scenario_name_label,
#             batch_scenario_name_widget_extra,
#             batch_scenario_name_widget,
#             organisation_label,
#             organisation_box,  # 5
#             start_calculation_label,
#             start_batch_button,  # 7
#             available_results_label,
#             available_results_widget,
#         ],  # 8
#         layout=widgets.Layout(
#             width="100%",
#             grid_row_gap="200px 200px 200px 200px",
#             #             grid_template_rows='auto auto auto 50px auto 40px auto 20px 40px',
#             grid_template_rows="auto auto auto",
#             grid_template_columns="12% 8% 10% 10% 18% 10% 10% 10% 10%",
#             grid_template_areas="""

#             'login_label login_label . select_polder_label select_polder_label get_slug_label get_slug_label get_slug_label get_slug_label'
#             'lizard_apikey lizard_apikey lizard_apikey polder_name_label polder_name_widget repository_label repository_dropdown repository_dropdown repository_dropdown'
#             'threedi_apikey threedi_apikey threedi_apikey . polder_name_search_button revision_label revision_dropdown revision_dropdown revision_dropdown'
#             'login_button logout_button . . . model_name_glg_label model_name_glg_dropdown model_name_glg_dropdown model_name_glg_dropdown'
#             '. . . . . model_name_ggg_label model_name_ggg_dropdown model_name_ggg_dropdown model_name_ggg_dropdown'
#             '. . . . . model_name_ghg_label model_name_ghg_dropdown model_name_ghg_dropdown model_name_ghg_dropdown'
#             'scenario_label scenario_label scenario_label .  . output_folder_label output_folder_label . .'
#             '. . scenario_box scenario_box scenario_box output_polder_label output_polder_dropdown output_polder_dropdown output_polder_dropdown'
#             'rain_type_box rain_type_box scenario_box scenario_box scenario_box output_subfolder_label output_folder_box output_folder_box output_folder_box'
#             'rain_type_box rain_type_box scenario_box scenario_box scenario_box . . . .'
#             'rain_type_box rain_type_box scenario_box scenario_box scenario_box batch_scenario_name_label batch_scenario_name_widget_extra batch_scenario_name_widget_extra batch_scenario_name_widget_extra'
#             'rain_type_box rain_type_box scenario_box scenario_box scenario_box batch_scenario_name_label batch_scenario_name_widget batch_scenario_name_widget batch_scenario_name_widget'
#             'rain_type_box rain_type_box scenario_box scenario_box scenario_box organisation_label organisation_box organisation_box organisation_box'
#             '. . . . . sqlite_selection_label sqlite_selection_label sqlite_selection_label sqlite_selection_label'
#             '. . . . . sqlite_selection sqlite_selection sqlite_selection sqlite_selection'
#             '. . . . . sqlite_selection sqlite_selection sqlite_selection sqlite_selection'
#             '. . . . . sqlite_selection sqlite_selection sqlite_selection sqlite_selection'
#             '. . . . . sqlite_selection sqlite_selection sqlite_selection sqlite_selection'
#             '. . . . . . . . .'
#             '. . . . . . start_calculation_label start_calculation_label start_calculation_label'
#             '. . . . . . start_batch_button start_batch_button start_batch_button'
#             'available_results_label available_results_label . . . . . . .'
#             'available_results_widget available_results_widget available_results_widget available_results_widget . . . . .'
#             """,
#         ),
#     )

#     lizard_apikey_widget.value = api_keys["lizard"]
#     threedi_apikey_widget.value = api_keys["threedi"]
#     login_button.click()
#     # polder_name_widget.value= 'Katvoed'

#     update_create_simulation_button()
#     update_start_simulation_button()
#     update_start_batch_button()

#     tab = widgets.Tab(children=[start_calculation_tab, start_batch_calculation_tab])
#     tab.set_title(0, "single calculation")
#     tab.set_title(1, "batch calculation")

#     return tab


# if __name__ == '__main__':
#     data = {'polder_folder': 'E:\\02.modellen\\model_test_v2',
#  'api_keys_path': 'C:\\Users\\wvangerwen\\AppData\\Roaming\\3Di\\QGIS3\\profiles\\default\\python\\plugins\\hhnk_threedi_plugin\\api_key.txt'}
#     start_calculation_tab = start_calculation_gui(data=data); 
#     start_calculation_tab



# %%
