# %%
# system imports
import os
import datetime

from pathlib import Path

# Third-party imports
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import ipywidgets as widgets
from IPython.core.display import HTML
from traitlets import Unicode
from apscheduler.schedulers.blocking import BlockingScheduler

# threedi
# from threedi_scenario_downloader import downloader as dl
from hhnk_threedi_tools.core.api import downloader as dl

# local imports
import hhnk_threedi_tools as htt
from hhnk_threedi_tools import Folders

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


dl.LIZARD_URL = "https://hhnk.lizard.net/api/v3/"
DL_RESULT_LIMIT = 1000
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
        self.batch = self.BatchRainSchemaWidgets()
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

            self.search_button_batch = widgets.Button(
                    description="Search",
                    layout=item_layout(height="30px", grid_area="search_button_batch"),
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

            #Batch requires 3x schema revision and model.
            for gxg in GROUNDWATER:
                setattr(self, f"schema_label_{gxg}", 
                    widgets.Label(
                        f"Schematisation {gxg}:", layout=item_layout(grid_area=f"schema_label_{gxg}")
                        )
                    )
                setattr(self, f"schema_dropdown_{gxg}", 
                    widgets.Dropdown(
                        layout=item_layout(grid_area=f"schema_dropdown_{gxg}")
                        )
                    )
                setattr(self, f"schema_view_{gxg}", 
                    widgets.HTML("",
                        layout=item_layout(grid_area=f"schema_view_{gxg}")
                        )
                    )
                setattr(self, f"revision_dropdown_{gxg}", 
                    widgets.Dropdown(
                        layout=item_layout(grid_area=f"revision_dropdown_{gxg}")
                        )
                    )
                setattr(self, f"threedimodel_dropdown_{gxg}", 
                    widgets.Dropdown(
                        disabled=True,
                        layout=item_layout(grid_area=f"threedimodel_dropdown_{gxg}")
                        )
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

            self.custom_rain_button = widgets.Button(
                description="custom rain",
                layout=item_layout(grid_area="custom_rain_button", justify_self="end"),
            )


            self.custom_rain_label = widgets.HTML("Add extra rain events by seperating them with a semicolon (;). Do this for all input fields below. e.g. rainoffset -> 0; 2*3600", 
                                                    layout=item_layout(grid_area="custom_rain_label"))
            self.simulation_duration_label = widgets.Label("Sim duration [s]",
                    layout=item_layout(grid_area="simulation_duration_label")
                )
            self.simulation_duration_widget = widgets.Text(
                    value="3600",
                    layout=item_layout(grid_area="simulation_duration_widget")
                )
            self.rain_offset_label = widgets.Label("Rain offset [s]",
                    layout=item_layout(grid_area="rain_offset_label")
                )
            self.rain_offset_widget = widgets.Text(
                    value="0",
                    layout=item_layout(grid_area="rain_offset_widget")
                )
            self.rain_duration_label = widgets.Label("Rain duration [s]",
                    layout=item_layout(grid_area="rain_duration_label")
                )
            self.rain_duration_widget = widgets.Text(
                    value="3600",
                    layout=item_layout(grid_area="rain_duration_widget")
                )
            self.rain_intensity_label = widgets.Label("Rain intensity [mm/hour]",
                    layout=item_layout(grid_area="rain_intensity_label")
                )
            self.rain_intensity_widget = widgets.Text(
                    value="100/24",
                    layout=item_layout(grid_area="rain_intensity_widget")
                )


            self.rain_event_plot = widgets.Output()
            self.rain_event_plot.layout = item_layout(grid_area="rain_event_plot")
            
            #TODO create plot of rain event
            # self.rain_figure = go.FigureWidget()
            # self.rain_figure.scatter(x=[0,1], y=[0,0])

            # with self.rain_event_plot:
            #     self.rain_figure
                

        def update_rain(self,
            simulation_duration,
            rain_offset,
            rain_duration,
            rain_intensity,
        ):
            """Update sliders with input values, used for buttons."""
            self.simulation_duration_widget.value = simulation_duration
            self.rain_offset_widget.value = rain_offset
            self.rain_duration_widget.value = rain_duration
            self.rain_intensity_widget.value = rain_intensity


        def update_rain_event(self, rain_type, rain_scenario):
            self.update_rain(
                simulation_duration=RAIN_SETTINGS[rain_type]["simulation_duration"],
                rain_offset=RAIN_SETTINGS[rain_type]["rain_offset"],
                rain_duration=RAIN_SETTINGS[rain_type]["rain_duration"],
                rain_intensity=RAIN_INTENSITY[rain_type][rain_scenario],
            )


        @property
        def rain_settings(self):
            try:
                r_settings = []
                
                #Check if input is valid
                if not len(self.rain_offset_widget.value.split(';'))==len(self.rain_duration_widget.value.split(';'))==len(self.rain_intensity_widget.value.split(';')):
                    raise
                
                for r_o, r_d, r_i in zip(self.rain_offset_widget.value.split(';'), self.rain_duration_widget.value.split(';'), self.rain_intensity_widget.value.split(';')):
                    if not eval(r_o) + eval(r_d) <= eval(self.simulation_duration_widget.value):
                        return f"{self.caller.vars.time_now} ERROR - End of rain longer than sim duration"
                    
                    r_settings.append({
                        "offset": int(eval(r_o)), 
                        "duration": int(eval(r_d)),
                        "value": eval(r_i)/(1000*3600), #mm/hour -> m/s
                        "units": "m/s",
                    })
                return r_settings
                    
            except:
                return f"{self.caller.vars.time_now} ERROR - Rain settings not valid" #Todo add to log panel.
        # def plot_rain_event(self):

        #         # simulation_duration,
        #         # rain_offset,
        #         # rain_duration,
        #         # rain_intensity,
        #     lines = self.w.rain.rain_figure.data
        #     for line in lines:
                           
        #         line.x = [0, rain_offset, rain_offset+rain_duration, simulation_duration]
        #         line.y = [0, rain_intensity, rain_intensity, 0]
        

        @property        
        def gridbox(self):
            return widgets.GridBox(
                children=[
                    self.test_0d1d_button,
                    self.test_1d2d_button,
                    self.test_hour_button,
                    self.T10_blok_button,
                    self.T100_blok_button,
                    self.T1000_blok_button,
                    self.T10_piek_button,
                    self.T100_piek_button,
                    self.T1000_piek_button,
                    self.custom_rain_button,
                    self.simulation_duration_label,               
                    self.simulation_duration_widget,   
                    self.custom_rain_label,            
                    self.rain_offset_label,               
                    self.rain_offset_widget,    
                    self.rain_duration_label,               
                    self.rain_duration_widget,    
                    self.rain_intensity_label,               
                    self.rain_intensity_widget,  
                    self.rain_event_plot,  
                    ],  
                layout=widgets.Layout(
                    width="100%",
                    grid_area="rain_box",
                    grid_row_gap="200px 200px 200px 200px",
                    #             grid_template_rows='auto auto auto 50px auto 40px auto 20px 40px',
                    # grid_template_rows="auto auto auto auto",
                    grid_template_columns="25% 25% 25% 25%",
                    grid_template_areas="""
                    'test_0d1d_button simulation_duration_label simulation_duration_widget simulation_duration_widget'
                    'test_1d2d_button custom_rain_label custom_rain_label custom_rain_label'
                    'hour_test_button rain_offset_label rain_offset_widget rain_offset_widget'
                    'T10_blok_button rain_duration_label rain_duration_widget rain_duration_widget'
                    'T100_blok_button rain_intensity_label rain_intensity_widget rain_intensity_widget'
                    'T1000_blok_button rain_event_plot rain_event_plot rain_event_plot'
                    'T10_piek_button rain_event_plot rain_event_plot rain_event_plot'
                    'T100_piek_button rain_event_plot rain_event_plot rain_event_plot'
                    'T1000_piek_button rain_event_plot rain_event_plot rain_event_plot'
                    'custom_rain_button rain_event_plot rain_event_plot rain_event_plot'
                    """
                    )
                )



    class BatchRainSchemaWidgets():
        def __init__(self):

            #Searching for the schema on 3Di servers.
            self.label = widgets.HTML("<b>4. Select scenarios to be run</b>",
                    layout=item_layout(grid_area="batch_scenario_label"),
                )

            self.scenario_toggle = {}

            # add togglebutton for every scenario (T10, T100, T1000)
            for rs in RAIN_SCENARIOS: 
                self.scenario_toggle[rs] = {}
                # add togglebutton for Gxg
                for gxg in GROUNDWATER:
                    self.scenario_toggle[rs][gxg] = widgets.ToggleButton(
                            value=True, layout=item_layout(), icon="check"
                        )

            row_widget={}
            calc_scenarios={}
            for row in RAIN_SCENARIOS: 
                # loop over groundwater conditions (first entry is for headers)
                row_widget[row] ={}
                for col in GROUNDWATER:
                    row_widget[row][col] = self.scenario_toggle[row][col]

                #rain scenario label
                calc_scenarios[row] = widgets.HBox(
                    # (widgets.HTML("<b>{}</b>".format(row), layout=item_layout(width="100%")),),
                    layout=item_layout(width="100%")
                ) 
                        
                for key in row_widget[row]:
                    calc_scenarios[row].children += (row_widget[row][key],)


                # calc_scenarios[row] = widgets.HBox(
                #     [row_widget[col] for col in row_widget], layout=item_layout()
                # ) 

                # #Add GxG label
                # calc_scenarios[row].children += (
                #     widgets.HTML("<b>{}</b>".format(row), layout=item_layout()),
                #     )

            self.gxg_label_box = widgets.HBox(
                layout=item_layout(grid_area="gxg_label_box", width="100%"),
            )
            for row in GROUNDWATER: 
                self.gxg_label_box.children += (
                    widgets.HTML("<b><center>{}</center></b>".format(row), 
                    layout=item_layout()),
                    )

            self.rain_label_box = widgets.VBox(
                layout=item_layout(grid_area="rain_label_box", width="100%"),
            )
            for row in RAIN_SCENARIOS: 
                self.rain_label_box.children += (
                    widgets.HTML("<b>{}</b>".format(row), layout=item_layout()),
                    )
            

            
            #Buttons for T10,T100,T1000, GxG
            self.scenario_box = widgets.VBox(
                    [calc_scenarios[row] for row in calc_scenarios],
                    layout=item_layout(grid_area="batch_scenario_box", width="100%"),
                )

            # buttons for Piek and Blok
            self.rain_type_widgets = {}
            for rain_type in RAIN_TYPES:
                self.rain_type_widgets[rain_type] = widgets.ToggleButton(
                    value=True, description=rain_type, layout=item_layout(), icon="check"
                )
            self.rain_type_box = widgets.HBox(
                [self.rain_type_widgets[t] for t in self.rain_type_widgets],
                layout=item_layout(grid_area="batch_rain_type_box"),)


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

            self.folder_value = widgets.Text(
                    '',
                    disabled=True,
                    layout=item_layout(grid_area="output_folder_value"),
                )


            #folder_map is subfolder for batch 
            self.subfolder_batch_label = widgets.Label(
                    "Output folder map:", layout=item_layout(grid_area="output_subfolder_batch_label")
                )    

            self.subfolder_batch_value = widgets.Text(
                    '',
                    disabled=False,
                    layout=item_layout(grid_area="output_subfolder_batch_value"),
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

            self.folder_value_batch = widgets.Text(
                                '', 
                                disabled=True,
                                layout=item_layout(grid_area="output_folder_value_batch"),
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
                    "<b>8. Feedback</b>", layout=item_layout(grid_area="feedback_label")
                )

            # API call HTML widget that shows the API call
            self.widget = widgets.Output(layout=item_layout(grid_area="feedback_widget"))


    class StartWidgets():
        def __init__(self):
            self.label = widgets.HTML(
                    "<b>7. Start simulation</b>",
                    layout=item_layout(grid_area="start_label"),
                )

            self.simulation_name_label = widgets.Label("Simulation name:",
                    layout=item_layout(grid_area="simulation_name_label")
                )

            self.simulation_name_widget = widgets.Text(
                    value="{schema_name} #{rev} {model_type}",
                    layout=item_layout(grid_area="simulation_name_widget")
                )

            self.simulation_name_view_widget = widgets.Text(
                    value="",
                    disabled=True,
                    layout=item_layout(grid_area="simulation_name_view_widget")
                )

            self.create_simulation_button = widgets.Button(
                    description="Create simulation",
                    layout=item_layout(height="90%", grid_area="create_simulation_button"),
                )

            self.start_button = widgets.Button(
                    description="Start simulation",
                    disabled=True,
                    layout=item_layout(height="90%", grid_area="start_button"),
                )

            #Batch
            self.simulation_batch_name_widget = widgets.Text(
                    value="{schema_name} #{rev} {rt}_{gxg}_{rs} ({i})",
                    layout=item_layout(grid_area="simulation_batch_name_widget")
                )

            self.check_batch_input_button = widgets.Button(
                    description="Check input",
                    layout=item_layout(height="90%", grid_area="check_batch_input_button"),
                )

            self.start_batch_button = widgets.Button(
                    description="Start batch simulation",
                    disabled=True,
                    layout=item_layout(height="90%", grid_area="start_batch_button"),
                )

class StartCalculationWidgetsInteraction(StartCalculationWidgets):
    def __init__(self, caller):
        super().__init__()
        self.caller = caller

        #Login with API key
        @self.login.button.on_click
        def login(action):
            self.vars.sim = Simulation(api_key=self.vars.api_keys["threedi"]) 
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
        def search(action):
            self.search_models()

            #TODO add check of available calc cores. self.vars.sim.threedi_api.statuses_statistics(simulation__organisation__unique_id='48dac75bef8a42ebbb52e8f89bbdb9f2', simulation__type__live=True)

            self.update_dropdowns(batch=False, schema=True)
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

            self.update_dropdowns(batch=False, revision=True, threedimodel=True)

        self.model.schema_dropdown.observe(on_select_schematisation, names="value")


        #Search model with revision
        def on_select_revision(selected_revision):
            """Update revisions options when repository/schematisation is selected"""
            self.model.threedimodel_dropdown.value = self.vars.threedimodel_dropdown_viewlist[self.vars.revision_dropdown_viewlist.index(selected_revision['new'])]
            self.update_simulation_name_widget()

        self.model.revision_dropdown.observe(on_select_revision, names="value")


        #Select sqlite - deprecated, sqlite is downloaded from api.
        # def on_select_sqlite(selected_sqlite):
        #     self.update_folder(schema_viewname=selected_sqlite["new"])
        
        # self.model.sqlite_dropdown.observe(on_select_sqlite, names="value")



        @self.rain.test_0d1d_button.on_click
        def change_rain(action):
            self.rain.update_rain(
                simulation_duration="8*24*3600",
                rain_offset="1*24*3600",
                rain_duration="5*24*3600",
                rain_intensity="100/24",  # 100mm/day, using impervious surface mapping makes 14.4mm/day and 11.5mm/day
            )
            self._activate_button_color(self.rain.test_0d1d_button)
            self.output.subfolder_box.value =self.output.subfolder_box.options[1]
            self._update_calc_settings_buttons(structure_control=False, laterals=True)
            self.update_simulation_name_widget(model_type = "0d1d_test")


        @self.rain.test_1d2d_button.on_click
        def change_rain(action):
            self.rain.update_rain(
                simulation_duration="15*3600",                
                rain_offset="1*3600",
                rain_duration="2*3600",
                rain_intensity="17.75",
            )
            self._activate_button_color(self.rain.test_1d2d_button)
            self.output.subfolder_box.value =self.output.subfolder_box.options[0]
            self._update_calc_settings_buttons(structure_control=True, laterals=True)
            self.update_simulation_name_widget(model_type = "1d2d_test")


        @self.rain.test_hour_button.on_click
        def change_rain(action):
            self.rain.update_rain(
                simulation_duration="3600",                
                rain_offset="0",
                rain_duration="1*3600",
                rain_intensity="100",
            )

            self._activate_button_color(self.rain.test_hour_button)
            self.output.subfolder_box.value =self.output.subfolder_box.options[0]
            self._update_calc_settings_buttons(structure_control=True, laterals=True)
            self.update_simulation_name_widget(model_type = "1hour_test")

        @self.rain.T10_blok_button.on_click
        def change_rain(action):
            self.rain.update_rain_event(rain_type='blok', rain_scenario='T10')
            self._activate_button_color(self.rain.T10_blok_button)
            self.output.subfolder_box.value =self.output.subfolder_box.options[0]
            self._update_calc_settings_buttons(structure_control=True, laterals=True)
            self.update_simulation_name_widget(model_type="blok_T10")

        @self.rain.T100_blok_button.on_click
        def change_rain(action):
            self.rain.update_rain_event(rain_type='blok', rain_scenario='T100')
            self._activate_button_color(self.rain.T100_blok_button)
            self.output.subfolder_box.value =self.output.subfolder_box.options[0]
            self._update_calc_settings_buttons(structure_control=True, laterals=True)
            self.update_simulation_name_widget(model_type="blok_T100")

        @self.rain.T1000_blok_button.on_click
        def change_rain(action):
            self.rain.update_rain_event(rain_type='blok', rain_scenario='T1000')
            self._activate_button_color(self.rain.T1000_blok_button)
            self.output.subfolder_box.value =self.output.subfolder_box.options[0]
            self._update_calc_settings_buttons(structure_control=True, laterals=True)
            self.update_simulation_name_widget(model_type="blok_T1000")

        @self.rain.T10_piek_button.on_click
        def change_rain(action):
            self.rain.update_rain_event(rain_type='piek', rain_scenario='T10')
            self._activate_button_color(self.rain.T10_piek_button)
            self.output.subfolder_box.value =self.output.subfolder_box.options[0]
            self._update_calc_settings_buttons(structure_control=True, laterals=True)
            self.update_simulation_name_widget(model_type="piek_T10")

        @self.rain.T100_piek_button.on_click
        def change_rain(action):
            self.rain.update_rain_event(rain_type='piek', rain_scenario='T100')
            self._activate_button_color(self.rain.T100_piek_button)
            self.output.subfolder_box.value =self.output.subfolder_box.options[0]
            self._update_calc_settings_buttons(structure_control=True, laterals=True)
            self.update_simulation_name_widget(model_type="piek_T100")

        @self.rain.T1000_piek_button.on_click
        def change_rain(action):
            self.rain.update_rain_event(rain_type='piek', rain_scenario='T1000')
            self._activate_button_color(self.rain.T1000_piek_button)
            self.output.subfolder_box.value =self.output.subfolder_box.options[0]
            self._update_calc_settings_buttons(structure_control=True, laterals=True)
            self.update_simulation_name_widget(model_type="piek_T1000")

        @self.rain.custom_rain_button.on_click
        def change_rain(action):
            self.rain.update_rain(
                simulation_duration="3600*",                
                rain_offset="3600*",
                rain_duration="3600*",
                rain_intensity="",
            )

            self._activate_button_color(self.rain.custom_rain_button)
            self.output.subfolder_box.value =self.output.subfolder_box.options[0]
            self._update_calc_settings_buttons(structure_control=True, laterals=True)
            self.update_simulation_name_widget(model_type="")


        # Observe all calculation settings buttons
        for button in self.calc_settings.children:
            button.observe(self._update_button_icon, "value")


        self.start.simulation_name_widget.observe(lambda change: self.update_simulation_name_widget(model_type=None), 'value', type='change')

        @self.start.create_simulation_button.on_click
        def create_simulation(action):
            sim_create_status, sim_message = self.check_create_simulation()
            if  not sim_create_status:
                self.add_feedback("ERROR", sim_message)
                return 


            if self.sim.simulation_created:
                #Simulation was already created. Button was clicked again, this will clear the simulation.
                self.sim.simulation = None
                self.sim.simulation_created = False
                self.update_create_simulation_button()
                self.update_start_simulation_button(status=0, button=self.start.start_button)

                self.add_feedback("INFO", "Simulation widget reset.")
                return

            self.feedback.widget.clear_output()

            self.vars.output_folder = self.vars.folder.threedi_results.full_path(
                    f"{self.output.subfolder_box.value}{os.sep}{self.start.simulation_name_view_widget.value}"
                )


            self.vars.sqlite_path = self.sim.download_sqlite()

            #Creating will set sim.simulation_created to True.
            self.sim.create(output_folder=self.vars.output_folder,
                        simulation_name=self.start.simulation_name_view_widget.value,
                        model_id=self.selected_threedimodel_id,
                        organisation_uuid=self.selected_organisation_id,
                        sim_duration=eval(self.rain.simulation_duration_widget.value)
                )

            #Load data from sqlite
            self.sim.get_data(rain_data=self.rain.rain_settings)

            use_structure_control=self.calc_settings.structure_control.value,
            use_laterals=self.calc_settings.laterals.value,
            use_basic_processing=self.calc_settings.basic_processing.value,
            use_damage_processing=self.calc_settings.damage_processing.value,
            use_arrival_processing=self.calc_settings.arrival_processing.value,

            #Add data to simulation
            self.sim.add_default_settings()
            self.sim.add_constant_rain()
            self.sim.add_boundaries()

            if use_structure_control:
                self.sim.add_structure_control()
            if use_laterals:
                self.sim.add_laterals()
            if use_basic_processing:
                self.sim.add_basic_post_processing()
            if use_damage_processing:
                self.sim.add_damage_post_processing()
            if use_arrival_processing:
                self.sim.add_arrival_post_processing()


            self.update_simulation_feedback(sim=self.sim)
            self.update_create_simulation_button()
            self.update_start_simulation_button(status=2, button=self.start.start_button)


        @self.start.start_button.on_click
        def start_simulation(action):
            #start simulation
            self.vars.sim.start()

            self.update_start_simulation_button(status=2, button=self.start.start_button)

            with self.feedback.widget:
                print(f"{self.vars.time_now} - Simulation (hopefully) started or queued.\n\
                    (check self.vars.sim.start_feedback)\n\
                    stop broken simulation with self.vars.sim.shutdown(simulation_pk=)")
                display(
                    HTML(
                        "<a href={} target='_blank'>Check progress on 3di</a>".format(
                            "https://3di.live/"
                        )
                    )
                )


        #-- #%%^^%%# --# BATCH interactions #-- #%%^^%%# --# 
        #Search schematisations
        @self.model.search_button_batch.on_click
        def search(action):
            self.search_models()

            self.update_dropdowns(batch=True, schema=True)
            self.update_organisations()


        #Search revisions
        def on_select_schematisation_gxg(selected_schematisation, gxg):
            if self.selected_schema_id_gxg(gxg):
                """Update revisions options when repository/schematisation is selected"""
                revisions = self.sim.threedi_api.schematisations_revisions_list(
                        schematisation_pk=self.selected_schema_id_gxg(gxg), limit=RESULT_LIMIT #selected_schematisation['new'].split(' -')[0]
                    ).results
            else:
                revisions = []

            self.vars.revisions_gxg[gxg] = {}
            for revision in revisions:  
                self.vars.revisions_gxg[gxg][revision.id] = revision #vars.schema_results is empty dict

            if self.selected_schema_id_gxg(gxg):
                threedimodels = self.sim.threedi_api.threedimodels_list(
                        revision__schematisation__id=self.selected_schema_id_gxg(gxg), limit=RESULT_LIMIT
                    ).results
            else:
                threedimodels = []

            self.vars.threedimodels_gxg[gxg] = {}
            for threedimodel in threedimodels:  
                self.vars.threedimodels_gxg[gxg][threedimodel.revision_id] = threedimodel 

            self.update_dropdowns(batch=True, revision=True, threedimodel=True)

            #update views of schematisations
            if getattr(self.model, f"schema_dropdown_{gxg}").value is not None:
                getattr(self.model, f"schema_view_{gxg}").value = getattr(self.model, f"schema_dropdown_{gxg}").value
            else:
                getattr(self.model, f"schema_view_{gxg}").value = ""


        self.model.schema_dropdown_glg.observe(lambda change: on_select_schematisation_gxg(change, "glg"), 'value', type='change')
        self.model.schema_dropdown_ggg.observe(lambda change: on_select_schematisation_gxg(change, "ggg"), 'value', type='change')
        self.model.schema_dropdown_ghg.observe(lambda change: on_select_schematisation_gxg(change, "ghg"), 'value', type='change')


        #Search model with revision
        def on_select_revision_gxg(selected_revision, gxg):
            """Update revisions options when repository/schematisation is selected"""
            getattr(self.model, f"threedimodel_dropdown_{gxg}").value = self.vars.threedimodel_dropdown_viewlist_gxg[gxg][self.vars.revision_dropdown_viewlist_gxg[gxg].index(selected_revision['new'])]

        self.model.revision_dropdown_glg.observe(lambda change: on_select_revision_gxg(change, "glg"), 'value', type='change')
        self.model.revision_dropdown_ggg.observe(lambda change: on_select_revision_gxg(change, "ggg"), 'value', type='change')
        self.model.revision_dropdown_ghg.observe(lambda change: on_select_revision_gxg(change, "ghg"), 'value', type='change')


        # Observe all calculation settings buttons
        for key in self.batch.scenario_toggle:
            for button in self.batch.scenario_toggle[key].values():
                button.observe(self._update_button_icon, "value")

        for button in self.batch.rain_type_widgets.values():
            button.observe(self._update_button_icon, "value")
            

        @self.start.check_batch_input_button.on_click
        def check_batch_input(action):
            # self.feedback.widget.clear_output()
            ready_to_roll = self.check_create_batch_simulation()


        @self.start.start_batch_button.on_click
        def start_batch_calculation(action):
            # self.feedback.widget.clear_output()

            ready_to_roll = self.check_create_batch_simulation()
            if not ready_to_roll:
                self.add_feedback("ERROR", "Check inputs again.")
                return
            
            self.update_start_simulation_button(status=1, button=self.start.start_batch_button)
            self.start.start_batch_button.style.button_color

            self.vars.output_folder_batch = self.output.folder_value_batch.value
            if os.path.exists(str(self.vars.output_folder_batch)) == False:
                os.mkdir(str(self.vars.output_folder_batch))


            batch_scenario_names = self.batch_scenario_names()


            self.add_feedback("INFO", f"Starting simulations.\nOutput wil be stored in {self.vars.output_folder_batch}")
            for rt, gxg, rs, i in self.loop_batch_selection():

                shortname = f"{rt}_{gxg}_{rs}"


                #Skip simulation if it already exists on lizard
                if batch_scenario_names[shortname] in self.vars.available_batch_results:
                    continue

                self.vars.sim_batch[shortname] = Simulation(api_key=self.vars.api_keys["threedi"]) 

                sim = self.vars.sim_batch[shortname]
                sim.simulation = None
                sim.simulation_created = False

                self.vars.sqlite_path_batch[shortname] = sim.download_sqlite()
                
                sim.create(output_folder=self.vars.output_folder_batch,
                            simulation_name=batch_scenario_names[shortname],
                            model_id=self.selected_threedimodel_id_gxg(gxg),
                            organisation_uuid=self.selected_organisation_id,
                            sim_duration=eval(RAIN_SETTINGS[rt]["simulation_duration"])
                    )

                #Load data from sqlite
                sim.get_data(rain_data=[{
                                    "offset": int(eval(RAIN_SETTINGS[rt]["rain_offset"])), 
                                    "duration": int(eval(RAIN_SETTINGS[rt]["rain_duration"])),
                                    "value": eval(RAIN_INTENSITY[rt][rs])/(1000*3600), #mm/hour -> m/s
                                    "units": "m/s",
                                }])

                #Add data to simulation
                sim.add_default_settings()
                sim.add_constant_rain()
                sim.add_boundaries()

                sim.add_structure_control()
                sim.add_laterals()

                #Postprocessing are enabled.
                sim.add_basic_post_processing()
                sim.add_damage_post_processing()
                #sim.add_arrival_post_processing()   -  Gebruiken bij BWN simulaties

                self.update_simulation_feedback(sim=sim)

                sim.start(extra_name=f"_{shortname}")

            with self.feedback.widget:
                print(f"{self.vars.time_now} - All simulations (hopefully) started or queued.\n\
                    stop broken simulation with self.vars.sim.shutdown(simulation_pk=)")
                display(
                    HTML(
                        "<a href={} target='_blank'>Check progress on 3di</a>".format(
                            "https://3di.live/"
                        )
                    )
                )

            self.update_start_simulation_button(status=0, button=self.start.start_batch_button)





    #-- #%%^^%%# --# end of __init__ #-- #%%^^%%# --#
    def check_create_simulation(self):
        """check if we can create a simulation"""
        if "ERROR" in self.rain.rain_settings:
            return False, "Rain settings incorrect"

        if self.start.simulation_name_widget.value == "":
            return False, "Missing simulation name"
        
        #Check if logged into API.
        try:
            if self.sim.logged_in == "Cannot login":
                return False, "API key incorrect"
        except:
            return False, "Missing API key"

        #Check for available results on lizard.
        r = dl.find_scenarios(name__icontains=self.model.schema_name_widget.value, limit=DL_RESULT_LIMIT)
        available_results = []
        if len(r) > 0:
            for i in r:
                available_results.append(i["name"].lower())

        self.vars.available_results = available_results
        if self.start.simulation_name_view_widget.value.lower() in self.vars.available_results:
            return False, "Simulation already available on lizard"

        return True, ""


    def check_create_batch_simulation(self):
        self.feedback.widget.clear_output()
        ready_to_roll = True
           
        #Set output folder

        output_folder = self.output.subfolder_batch_value.value
        if output_folder == "":
            ready_to_roll=False
            self.add_feedback("ERROR", "No output subfolder selected.")
            
        else:
            self.output.folder_value_batch.value = self.vars.folder.threedi_results.batch.full_path(output_folder)
            if os.path.exists(self.vars.folder.threedi_results.batch.full_path(output_folder)):
                self.add_feedback("Warning", "Output folder map already exists!")

        # output_folder = self.start.simulation_batch_name_widget.value.replace("{rt}_{gxg}_{rs}","")
        # output_folder = output_folder.replace("({i})","")
        # output_folder = output_folder.replace("  "," ")
        # output_folder=output_folder.format(schema_name=self.model.schema_name_widget.value,
        #                                     rev="-",
        #                                     model_type=self.vars.model_type,)
        # output_folder = output_folder.rstrip(" ")

        selected_models = []
        for gxg in GROUNDWATER:
            if np.any([x for x in self.selected_batch_scenarios[gxg].values()]):
                if self.selected_schema_id_gxg(gxg) is None:
                    self.add_feedback("ERROR", f"{gxg} has no schematisation selected")
                    ready_to_roll=False
                else:
                    threedimodel_gxg = getattr(self.model, f"threedimodel_dropdown_{gxg}").value
                    schema_name = self.vars.schematisations[int(self.selected_schema_id_gxg(gxg))].name

                    if threedimodel_gxg is None \
                        or threedimodel_gxg  == "Revision has no model":
                        self.add_feedback("ERROR", f"{gxg} scenario selected but missing model.")
                        ready_to_roll=False
                    else:
                        selected_models += [threedimodel_gxg]
                    
                    if not gxg in schema_name:
                        self.add_feedback("ERROR", f"{gxg} not in schema_name_{gxg}") 
                        ready_to_roll=False
                    
                
        if len(set(selected_models)) != len(selected_models):
            self.add_feedback("ERROR", "selected schematisations have the same 3Di model id.\nSelect different schematisations.")
            ready_to_roll=False
        
        #Check for available results on lizard.
        r = dl.find_scenarios(name__icontains=self.model.schema_name_widget.value, limit=DL_RESULT_LIMIT)
        available_results = []
        if len(r) > 0:
            for i in r:
                available_results.append(i['name'].lower())

        self.vars.available_results = available_results


        if ready_to_roll:
            batch_scenario_names = self.batch_scenario_names()

            self.vars.available_batch_results = [b for b in batch_scenario_names.values() if b.lower() in self.vars.available_results]

            self.add_feedback("INFO", f"Ready to start simulations ({len(batch_scenario_names)} total)")
            self.add_feedback("INFO", f"{len(self.vars.available_batch_results)} simulations already have results on lizard and wont be started")

            for key in batch_scenario_names:
                name = batch_scenario_names[key]
                if name not in self.vars.available_batch_results:
                    self.feedback.widget.append_stdout(f"{name}\n")
                else:
                    self.feedback.widget.append_stdout(f"{name} -- already exists\n")

                # with self.feedback.widget:
                #     print(batch_scenario_names[key])
            self.update_start_simulation_button(status=2, button=self.start.start_batch_button)

            if len(batch_scenario_names) == len(self.vars.available_batch_results):
                self.add_feedback("Warning", f"All selected scenarios already exist on lizard") 
                ready_to_roll=False
                self.update_start_simulation_button(status=0, button=self.start.start_batch_button)

            
        else:
            self.update_start_simulation_button(status=0, button=self.start.start_batch_button)

        


        return ready_to_roll


    def search_models(self):
        """search models on 3Di and organisations. Add them to vars"""
        schematisations = self.sim.threedi_api.schematisations_list(
            slug__icontains=self.model.schema_name_widget.value, limit=RESULT_LIMIT
        ).results

        self.vars.schematisations={}
        for result in schematisations:  
            self.vars.schematisations[result.id] = result


        contracts = self.sim.threedi_api.contracts_list().results
        self.vars.contracts = {}
        for con in contracts:
            self.vars.contracts[con.organisation_name] = con


    def update_api_keys(self, api_keys_path):
        self.vars.api_keys = htt.read_api_file(api_keys_path)
        self.login.lizard_apikey_widget.value=self.vars.api_keys["lizard"]
        self.login.threedi_apikey_widget.value=self.vars.api_keys["threedi"]
        self.login.button.click()


    def update_organisations(self):
        self.model.organisation_box.options = self.vars.organisations_viewlist 


    def update_dropdowns(self, batch=False, **kwargs):
        if not batch:
            if 'schema' in kwargs:
                self.model.schema_dropdown.options = self.vars.schema_dropdown_viewlist
            if 'threedimodel' in kwargs: #Needs to be defined before revision because we observe that box.
                self.model.threedimodel_dropdown.options = self.vars.threedimodel_dropdown_viewlist
            if 'revision' in kwargs:
                self.model.revision_dropdown.options = self.vars.revision_dropdown_viewlist
        
        else:
            if 'schema' in kwargs:
                for gxg in GROUNDWATER:
                    getattr(self.model, f"schema_dropdown_{gxg}").options = self.vars.schema_dropdown_viewlist
            if 'threedimodel' in kwargs: #Needs to be defined before revision because we observe that box.
                for gxg in GROUNDWATER:
                    getattr(self.model, f"threedimodel_dropdown_{gxg}").options = self.vars.threedimodel_dropdown_viewlist_gxg[gxg]
            if 'revision' in kwargs:
                for gxg in GROUNDWATER:
                    getattr(self.model, f"revision_dropdown_{gxg}").options = self.vars.revision_dropdown_viewlist_gxg[gxg]
        # if 'sqlite' in kwargs:
        #     self.model.sqlite_dropdown.options = self.vars.sqlite_dropdown_viewlist

        # if 'sqlite' in kwargs:
        #     self.model.sqlite_dropdown.options = self.vars.sqlite_dropdown_viewlist


    def update_folder(self):
        """when main folder changes, we update some values"""

        #Output folder string
        self.output.folder_value.value = self.vars.folder.threedi_results.path
        self.output.folder_value_batch.value = self.vars.folder.threedi_results.batch.path


    def update_simulation_feedback(self, sim):
        """Give feedback of created simulation"""

        self.feedback.widget.append_stdout(f"{self.vars.time_now} - Simulation created")
        with self.feedback.widget:
            display(sim.simulation_info(str_type="html"))


    def add_feedback(self, errortype, message):
        self.feedback.widget.append_stdout(f"{self.vars.time_now} {errortype} - {message}\n")


    def update_create_simulation_button(self):
        """
        Make sure the following data is supplied:s
            - Repository
            - Revision
            - Model name
            - Sqlite
        When clicked disable again
        """
        if not self.sim.simulation_created:
            self.start.create_simulation_button.description = "Create simulation"
            self.start.create_simulation_button.disabled = False
            self.start.create_simulation_button.style.button_color = "lightgreen"

            
            #FIXME no functionality to change this button based on inputs.
            # if (
            #     repository_dropdown.value is not None
            #     and revision_dropdown.value is not None
            # ):
            #     create_simulation_button.disabled = False
            #     create_simulation_button.style.button_color = "lightgreen"
            # else:
                # create_simulation_button.disabled = True
                # create_simulation_button.style.button_color = "red"
        else:
            self.start.create_simulation_button.description = "Change simulation"
            self.start.create_simulation_button.disabled = False
            self.start.create_simulation_button.style.button_color = "lightgreen"


    def update_start_simulation_button(self, status: int, button):
        """
        0 = disable red
        1 = disable orange
        2 = enable green"""
        if status == 0:
            button.disabled=True
            button.style.button_color="red"
        elif status == 1:
            button.disabled=True
            button.style.button_color="orange"
        elif status == 2:
            button.disabled=False
            button.style.button_color="lightgreen"


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


    def update_simulation_name_widget(self, model_type=None):
        schema_name = self.model.schema_name_widget.value #used in eval
        if self.selected_revision:
            rev=f"{self.selected_revision.number}"
        else:
           rev=""

        if model_type is None:
            model_type = self.vars.model_type
        else:
            self.vars.model_type = model_type

        try:
            self.start.simulation_name_view_widget.value = self.start.simulation_name_widget.value.format(
                        schema_name=schema_name,
                        rev=rev,
                        model_type=model_type,
                        )
        except:
            pass


    def batch_scenario_names(self):
        simulation_names = {}
        schema_name = self.model.schema_name_widget.value #used in eval
        
        for rt, gxg, rs, i in self.loop_batch_selection():
                rev = self.selected_revision_gxg(gxg).number #used in eval
                simulation_names[f"{rt}_{gxg}_{rs}"] =self.start.simulation_batch_name_widget.value.format(
                        schema_name=schema_name,
                        rev=rev,
                        rt=rt,
                        gxg=gxg,
                        rs=rs,
                        i=i,
                    )
                                
        return simulation_names


    def loop_batch_selection(self):
        i=0
        for rt in RAIN_TYPES:
            for gxg in GROUNDWATER:
                for rs in RAIN_SCENARIOS:
                    i+=1
                    if self.batch.rain_type_widgets[rt].value: #check piek/block selection
                        if self.batch.scenario_toggle[rs][gxg].value: #check T1xxx/gxg selection
                            yield rt, gxg, rs, i


    @property
    def selected_batch_scenarios(self):
        """return list of selected scenarios"""
        selected_scenarios={}
        for gxg in GROUNDWATER:
            selected_scenarios[gxg]={}
            for rs in RAIN_SCENARIOS:
                selected_scenarios[gxg][rs] = self.batch.scenario_toggle[rs][gxg].value
        return selected_scenarios


    @property
    def selected_schema_id(self):
        """id of selected schematization"""
        try:
            return self.model.schema_dropdown.value.split(' - ')[0]
        except:
            return None


    def selected_schema_id_gxg(self, gxg):
        """id of selected schematization"""
        try:
            return getattr(self.model, f"schema_dropdown_{gxg}").value.split(' - ')[0]
        except:
            return None


    def selected_revision_gxg(self, gxg):
        try:
            return self.vars.revisions_gxg[gxg][int(self.selected_revision_id_gxg(gxg))]
        except:
            return None


    def selected_revision_id_gxg(self, gxg):
        """id of selected revision"""
        try:
            return getattr(self.model, f"revision_dropdown_{gxg}").value.split(' - ')[0]
        except:
            return None


    def selected_threedimodel_id_gxg(self, gxg):
        """id of selected model"""
        try:
            return getattr(self.model, f"threedimodel_dropdown_{gxg}").value
        except:
            return None     


    @property
    def selected_revision_id(self):
        """id of selected revision"""
        try:
            return self.model.revision_dropdown.value.split(' - ')[0]
        except:
            return None


    @property
    def selected_threedimodel_id(self):
        """id of selected model"""
        try:
            return self.model.threedimodel_dropdown.value
        except:
            return None         


    @property
    def selected_revision(self):
        try:
            return self.vars.revisions[int(self.selected_revision_id)]
        except:
            return None


    @property
    def selected_organisation_id(self):
        try:
            return self.vars.contracts[self.model.organisation_box.value.split("  -")[0]].organisation

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

        self.sim = None #Is filled when pressing login #htt.core.api.calculation.Calculation class
        self.sim_batch = {} #Copies of .sim for batch calculations.
        self.schematisations = {} #Is filled when searching for models by name
        self.revisions = {}
        self.revisions_gxg = {"glg":{}, "ggg":{}, "ghg":{}}
        self.threedimodels = {}
        self.threedimodels_gxg = {"glg":{}, "ggg":{}, "ghg":{}}
        self.sqlite_dropdown_options = {}
        self.sqlite_path = None #Sqlite is downloaded and placed here.
        self.sqlite_path_batch = {} #Sqlite is downloaded and placed here.

        self.contracts = {} #holds organisations and available sessions
        self.api_keys = {"lizard":"", "threedi":""}

        self.simulation_model_type = ""
        self.output_folder = None #Filled when create_simulation is clicked.
        self.available_results = None #Filled when create_simulation is clicked
        self.output_folder_batch = None #filled when start batch simulation is clicked.
        
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
    def revision_dropdown_viewlist_gxg(self):
        revision_dict = {}
        for gxg in GROUNDWATER:
            revision_list = []

            for idx in self.revisions_gxg[gxg]:
                rev = self.revisions_gxg[gxg][idx]
                try:
                    commit_date = rev.commit_date.strftime('%y/%m/%d-%H:%M:%S') or None
                except:
                    commit_date=None
                # if rev.is_valid:
                revision_list.append(f"{idx} - #{rev.number} - {commit_date} - valid:{rev.is_valid} - {rev.commit_user}")
            revision_dict[gxg] = revision_list
        return revision_dict


    @property
    def threedimodel_dropdown_viewlist(self):
        threedimodel_list = []
        for rev in self.revisions.keys():
            if rev in self.threedimodels.keys():
                threedimodel_list.append(f"{self.threedimodels[rev].id}")
            else:
                threedimodel_list.append(f"Revision has no model")
        return threedimodel_list
        

    @property
    def threedimodel_dropdown_viewlist_gxg(self):
        threedimodel_dict = {}
        for gxg in GROUNDWATER:
            threedimodel_list = []
            for rev in self.revisions_gxg[gxg].keys():
                if rev in self.threedimodels_gxg[gxg].keys():
                    threedimodel_list.append(f"{self.threedimodels_gxg[gxg][rev].id}")
                else:
                    threedimodel_list.append(f"Revision has no model")
            threedimodel_dict[gxg]=threedimodel_list
        return threedimodel_dict

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
        return [f"{org}  -  ({self.contracts[org].current_sessions}/{self.contracts[org].session_limit})" for org in self.contracts.keys()]

    @property
    def time_now(self):
        return datetime.datetime.now().strftime("%H:%M:%S")


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

        self.w.rain.test_0d1d_button.click()

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
                self.w.rain.gridbox,
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
                self.w.start.simulation_name_label,
                self.w.start.simulation_name_widget,
                self.w.start.simulation_name_view_widget,
                self.w.start.create_simulation_button,
                self.w.start.start_button,
                ],  # 8
            layout=widgets.Layout(
                width="100%",
                grid_row_gap="200px 200px 200px 200px",
                #             grid_template_rows='auto auto auto 50px auto 40px auto 20px 40px',
                grid_template_rows="auto auto auto",
                grid_template_columns="1% 10% 10% 10% 10% 2% 19% 19% 19%",
                grid_template_areas="""
'. login_label login_label model_label model_label . schema_select_label schema_select_label schema_select_label'
'. lizard_apikey_widget lizard_apikey_widget . . . schema_label schema_dropdown schema_dropdown'
'. threedi_apikey_widget threedi_apikey_widget schema_name_widget schema_name_widget . revision_label revision_dropdown revision_dropdown'
'. login_button login_button model_name_search_button model_name_search_button . threedimodel_label threedimodel_dropdown threedimodel_dropdown'
'. . . . . . organisation_label organisation_box organisation_box'
'. rain_label rain_label rain_label rain_label . output_label output_label output_label'
'. rain_box rain_box rain_box rain_box . output_folder_label output_folder_value output_folder_value'
'. rain_box rain_box rain_box rain_box . output_subfolder_label output_subfolder_box output_subfolder_box '
'. rain_box rain_box rain_box rain_box . calc_settings_label calc_settings_label calc_settings_label'
'. rain_box rain_box rain_box rain_box . calc_settings_basic_processing calc_settings_damage_processing calc_settings_arrival_processing'
'. rain_box rain_box rain_box rain_box . calc_settings_structure_control calc_settings_laterals .'
'. rain_box rain_box rain_box rain_box . start_label start_label start_label'
'. rain_box rain_box rain_box rain_box . simulation_name_label simulation_name_widget simulation_name_widget'
'. rain_box rain_box rain_box rain_box . . simulation_name_view_widget simulation_name_view_widget'
'. rain_box rain_box rain_box rain_box . . create_simulation_button create_simulation_button'
'. feedback_label . . . . . start_button start_button'
'. feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget'
'. feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget'
'. feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget'
'. . . . . . . . .'
""",
            ))

        self.start_batch_calculation_tab = widgets.GridBox(
            children=[
                self.w.login.label,
                self.w.login.button,
                self.w.login.lizard_apikey_widget,
                self.w.login.threedi_apikey_widget,  # 1 login
                self.w.model.label,
                self.w.model.schema_name_widget,
                # self.w.model.search_button,
                self.w.model.search_button_batch,
                self.w.model.select_label,
                self.w.model.schema_label,
                self.w.model.schema_label_glg,
                self.w.model.schema_label_ggg,
                self.w.model.schema_label_ghg,
                self.w.model.schema_view_glg,
                self.w.model.schema_view_ggg,
                self.w.model.schema_view_ghg,
                # self.w.model.schema_dropdown,
                self.w.model.schema_dropdown_glg,
                self.w.model.schema_dropdown_ggg,
                self.w.model.schema_dropdown_ghg,
                self.w.model.revision_label,
                # self.w.model.revision_dropdown,
                self.w.model.revision_dropdown_glg,
                self.w.model.revision_dropdown_ggg,
                self.w.model.revision_dropdown_ghg,
                self.w.model.threedimodel_label,
                # self.w.model.threedimodel_dropdown,
                self.w.model.threedimodel_dropdown_glg,
                self.w.model.threedimodel_dropdown_ggg,
                self.w.model.threedimodel_dropdown_ghg,
                self.w.model.organisation_label,
                self.w.model.organisation_box,
                # self.w.model.sqlite_label,
                # self.w.model.sqlite_dropdown,
                # self.w.model.sqlite_chooser,
                # self.w.rain.label,
                # self.w.rain.gridbox,
                self.w.batch.label,
                self.w.batch.gxg_label_box,
                self.w.batch.rain_label_box,
                self.w.batch.scenario_box,
                self.w.batch.rain_type_box,
                self.w.output.label,
                self.w.output.folder_label,
                # self.w.output.folder_value,
                self.w.output.folder_value_batch,
                self.w.output.subfolder_batch_label,
                self.w.output.subfolder_batch_value,
                # self.w.output.subfolder_label,
                # self.w.output.subfolder_box,
                # self.w.calc_settings.label,
                # self.w.calc_settings.basic_processing,
                # self.w.calc_settings.damage_processing,
                # self.w.calc_settings.arrival_processing,
                # self.w.calc_settings.structure_control,
                # self.w.calc_settings.laterals,
                self.w.feedback.label,
                self.w.feedback.widget,
                self.w.start.label,
                self.w.start.simulation_name_label,
                # self.w.start.simulation_name_widget,
                self.w.start.simulation_batch_name_widget,
                # self.w.start.create_simulation_button,
                self.w.start.check_batch_input_button,
                # self.w.start.start_button,
                self.w.start.start_batch_button,
                ],  # 8
            layout=widgets.Layout(
                width="100%",
                grid_row_gap="200px 200px 200px 200px",
                #             grid_template_rows='auto auto auto 50px auto 40px auto 20px 40px',
                grid_template_rows="auto auto auto",
                grid_template_columns="1% 10% 10% 10% 10% 2% 14% 15% 28%",
                grid_template_areas="""
'. login_label login_label model_label model_label . schema_select_label schema_select_label schema_select_label'
'. lizard_apikey_widget lizard_apikey_widget . . . schema_label_glg schema_view_glg schema_view_glg'
'. threedi_apikey_widget threedi_apikey_widget schema_name_widget schema_name_widget . schema_label_ggg schema_view_ggg schema_view_ggg'
'. login_button login_button search_button_batch search_button_batch . schema_label_ghg schema_view_ghg schema_view_ghg'
'. . . . . . organisation_label organisation_box organisation_box'
'. batch_scenario_label batch_scenario_label batch_scenario_label batch_scenario_label . output_label output_label output_label'
'. . batch_rain_type_box batch_rain_type_box batch_rain_type_box . output_folder_label output_folder_value_batch output_folder_value_batch'
'. . gxg_label_box gxg_label_box gxg_label_box . output_subfolder_batch_label output_subfolder_batch_value output_subfolder_batch_value'
'. rain_label_box batch_scenario_box batch_scenario_box batch_scenario_box . . . .'
'. rain_label_box batch_scenario_box batch_scenario_box batch_scenario_box . . . .'
'. rain_label_box batch_scenario_box batch_scenario_box batch_scenario_box . start_label start_label start_label'
'. schema_label schema_dropdown_glg schema_dropdown_ggg schema_dropdown_ghg . simulation_name_label simulation_batch_name_widget simulation_batch_name_widget'
'. revision_label revision_dropdown_glg revision_dropdown_ggg revision_dropdown_ghg . . check_batch_input_button check_batch_input_button'
'. threedimodel_label threedimodel_dropdown_glg threedimodel_dropdown_ggg threedimodel_dropdown_ghg . . start_batch_button start_batch_button'
'. feedback_label . . . . . . .'
'. feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget'
'. feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget'
""",


# '. . . . . . . . . .'
# '. . . . . . . . . .'
# '. . . . . . . . .' 
# """,
        ))
    @property
    def tab(self):
        tab = widgets.Tab(children=[self.start_calculation_tab, self.start_batch_calculation_tab])
        tab.set_title(0, "single calculation")
        tab.set_title(1, "batch calculation")
        return tab

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
    display(self.tab)
    # display(self.start_calculation_tab)

    # display(self.w.batch.scenario_box)

    self.widgets.model.schema_name_widget.value='model_test'
    # self.widgets.model.schema_name_widget.value='katvoed'

# %%

# grid_template_areas="""
# '. login_label login_label model_label model_label schema_select_label schema_select_label schema_select_label schema_select_label'
# '. lizard_apikey_widget lizard_apikey_widget . . schema_label schema_dropdown schema_dropdown schema_dropdown'
# '. threedi_apikey_widget threedi_apikey_widget schema_name_widget schema_name_widget revision_label revision_dropdown revision_dropdown revision_dropdown'
# '. login_button login_button model_name_search_button model_name_search_button threedimodel_label threedimodel_dropdown threedimodel_dropdown threedimodel_dropdown'
# '. . . . . organisation_label organisation_box organisation_box organisation_box'
# '. rain_label rain_label rain_label rain_label output_label output_label output_label output_label'
# '. rain_box rain_box rain_box rain_box output_folder_label output_folder_value output_folder_value output_folder_value'
# '. rain_box rain_box rain_box rain_box output_subfolder_label output_subfolder_box output_subfolder_box output_subfolder_box'
# '. rain_box rain_box rain_box rain_box calc_settings_label calc_settings_label calc_settings_label calc_settings_label'
# '. rain_box rain_box rain_box rain_box . calc_settings_basic_processing calc_settings_damage_processing calc_settings_arrival_processing'
# '. rain_box rain_box rain_box rain_box . calc_settings_structure_control calc_settings_laterals .'
# '. rain_box rain_box rain_box rain_box . start_label start_label start_label'
# '. rain_box rain_box rain_box rain_box . . simulation_name_label simulation_name_widget simulation_name_widget
# '. rain_box rain_box rain_box rain_box . . . create_simulation_button create_simulation_button'
# '. rain_box rain_box rain_box rain_box . . start_button start_button'
# '. feedback_label . . . . . . .'
# '. feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget . . .'
# '. feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget . . .'
# '. feedback_widget feedback_widget feedback_widget feedback_widget feedback_widget . . .'
# '. . . . . . . . .'
# """

# a=[row.split(" ") for row in grid_template_areas.split("\n")]
# for i in a:
#     print(f"{len(i)} {i}")
# [len(i) for i in a]

