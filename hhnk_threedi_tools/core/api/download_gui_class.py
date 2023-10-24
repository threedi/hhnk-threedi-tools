# %%
# system imports
import os
import datetime
import re

# Third-party imports
import pandas as pd
import ipywidgets as widgets

from traitlets import Unicode
# from apscheduler.schedulers.blocking import BlockingScheduler
import requests
from IPython.core.display import HTML
from IPython.display import display
from pathlib import Path

# threedi
from threedi_scenario_downloader import downloader as dl
import hhnk_threedi_tools.core.api.download_functions as download_functions
from hhnk_threedi_tools.core.api.calculation import Simulation

# local imports
import hhnk_research_tools as hrt
from hhnk_threedi_tools import Folders


# Globals
from hhnk_threedi_tools.variables.api_settings import (
    RAIN_TYPES,
    GROUNDWATER,
    RAIN_SCENARIOS,
)


dl.LIZARD_URL = "https://hhnk.lizard.net/api/v4/"
# THREEDI_API_HOST = "https://api.3di.live/v3"
RESULT_LIMIT = 50 #Results on search
CHUNK_SIZE = 1024**2


def item_layout(width="95%", grid_area="", **kwargs):
    return widgets.Layout(
        width=width, grid_area=grid_area, **kwargs
    )  # override the default width of the button to 'auto' to let the button grow


def get_threedi_download_file(download, 
                      output_file,
                      overwrite=False) -> bool:
    """Getting file from threedi download object and write it under given path."""
    if hrt.check_create_new_file(output_file=output_file, overwrite=overwrite):
        r = requests.get(download.get_url, stream=True, timeout=15)
        with open(output_file, "wb") as f:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
        return True
    else:
        return False
    

class dlRaster():
    """Helper for input of download_functions."""
    def __init__(self, scenario_uuid, raster_code, resolution, output_path, button, name, timelist=None, bbox=None):
        #Api variables
        self.scenario_uuid = scenario_uuid
        self.raster_code = raster_code
        self.resolution = resolution
        self.bbox = bbox
        self.timelist = timelist
        self.output_path = output_path

        #local use
        self.button = button
        self.name = name

class dlRasterSettingsV4():
    def __init__(self, projection = "EPSG:28992") -> None:
        self.scenario_uuid_list = []
        self.raster_code_list = []
        self.projection_list = projection
        self.resolution_list = []
        self.bbox_list = []
        self.time_list = []
        self.pathname_list = []

    def add_raster(self, r: dlRaster):
        """Add single raster to the settings"""
        self.scenario_uuid_list.append(r.scenario_uuid)
        self.raster_code_list.append(r.raster_code)
        self.resolution_list.append(r.resolution)
        self.bbox_list.append(r.bbox)
        self.time_list.append(r.timelist)
        self.pathname_list.append(Path(r.output_path).as_posix())

    def print(self):
        print(f"scenario_uuid_list: {self.scenario_uuid_list}")
        print(f"raster_code_list: {self.raster_code_list}")
        print(f"projection_list: {self.projection_list}")
        print(f"resolution_list: {self.resolution_list}")
        print(f"bbox_list: {self.bbox_list}")
        print(f"time_list: {self.time_list}")
        print(f"pathname_list: {self.pathname_list}")


class DownloadWidgets:
    def __init__(self):
        # --------------------------------------------------------------------------------------------------
        # 1. Login with API keys
        # --------------------------------------------------------------------------------------------------
        self.login = self.LoginWidgets()
        self.search = self.SearchWidgets()
        self.select = self.SelectWidgets()
        self.outputtypes = self.OutputTypesWidgets()
        self.output = self.OutputLocationWidgets()
        self.download = self.DownloadWidgets()
        self.download_batch = self.DownloadBatchWidgets()


    class LoginWidgets:
        def __init__(self):
            class ApikeyWidget(widgets.Text):
                _view_name = Unicode("PasswordView").tag(sync=True)

            #Label
            self.label = widgets.HTML(
                '<b>1. Login with API keys\
                    (<a href=https://hhnk.lizard.net/management/personal_api_keys target target="_blank">lizard</a>,\
                    <a href=https://management.3di.live/personal_api_keys target target="_blank">3di</a>) </b>',
                layout=item_layout(grid_area="login_label"),
            )

            # Api key widgets
            self.lizard_apikey_widget = ApikeyWidget(
                description="Lizard key:",
                disabled=True,
                layout=item_layout(grid_area="lizard_apikey_widget"),
            )

            self.threedi_apikey_widget = ApikeyWidget(
                description="Threedi key:",
                disabled=True,
                layout=item_layout(grid_area="threedi_apikey_widget"),
            )

            self.button = widgets.Button(
                description="Login",
                disabled=True,
                layout=item_layout(height="30px", width="95%", grid_area="login_button", justify_self='center'),
            )


    class SearchWidgets:
        def __init__(self):

            #Searching for the schema on 3Di servers.
            self.label = widgets.HTML(
                "<b>2. Search for simulation on lizard</b>",
                layout=item_layout(grid_area="search_label"),
            )

            # Simulation name widget
            self.sim_name_label = widgets.Label(
                "Sim name:", layout=item_layout(grid_area="sim_name_label")
            )
            self.sim_name_widget = widgets.Text(
                layout=item_layout(grid_area="sim_name_widget")
            )

            # Simulation revision widget
            self.sim_rev_label = widgets.Label(
                "Sim rev:", layout=item_layout(grid_area="sim_rev_label")
            )
            self.sim_rev_widget = widgets.Text(
                layout=item_layout(grid_area="sim_rev_widget")
            )

            # Simulation revision widget
            self.limit_label = widgets.Label(
                "Limit:", layout=item_layout(grid_area="search_limit_label")
            )
            self.limit_widget = widgets.IntText(
                value=RESULT_LIMIT,
                layout=item_layout(grid_area="search_limit_widget")
            )

            self.search_button_lizard = widgets.Button(
                description="Search Lizard",
                layout=item_layout(height="100%", grid_area="sim_search_button_lizard"),
            )

            self.search_button_threedi = widgets.Button(
                description="Search 3Di",
                layout=item_layout(height="100%", grid_area="sim_search_button_threedi"),
            )

    class SelectWidgets:
        """Output box with available results for simulation"""
        def __init__(self) -> None:

            self.dl_select_label_text = "<b>3. Select simulation results  ( {} selected )</b>"
            self.dl_select_label = widgets.HTML(
                self.dl_select_label_text.format(0),
                layout=item_layout(grid_area="dl_select_label"),
            )

            # selection box with names of model results
            self.dl_select_box = widgets.SelectMultiple(
                rows=20, layout=item_layout(width="98%", grid_area="dl_select_box")
            )

            # Subset the list in the download selection box
            self.show_0d1d_button = widgets.Button(
                description="Show 0d1d",
                layout=item_layout(grid_area="button_0d1d", justify_self="end"),
            )

            self.filter_button = widgets.Button(
                description="Filter", layout=item_layout(grid_area="filter_button")
            )

            # Create search box
            self.search_results_widget = widgets.Text(
                placeholder="Filter results by starting with: -",
                layout=item_layout(grid_area="search_results"))


    class OutputTypesWidgets:
        def __init__(self):

            self.label = widgets.HTML(
                "<b>4. Select filetypes (dark=selected)</b>",
                layout=item_layout(grid_area="outputtypes_label"),
            )

            # file result buttons ----------------------------------------------------------------------------
            self.file_buttons_label = widgets.HTML(
                "File results", layout=item_layout(grid_area="file_buttons_label")
            )
            self.netcdf_button = widgets.ToggleButton(
                value=True,
                description="raw 3Di output (.nc)",
                layout=item_layout(grid_area="netcdf_button"),
            )
            self.agg_netcdf_button = widgets.ToggleButton(
                value=True,
                description="aggregated 3Di output (.nc)",
                layout=item_layout(grid_area="agg_netcdf_button"),
            )
            self.h5_button = widgets.ToggleButton(
                value=True,
                description="grid administration (.h5)",
                layout=item_layout(grid_area="h5_button"),
            )
            self.log_button = widgets.ToggleButton(
                value=True,
                description="calculation core logging (.txt)",
                layout=item_layout(grid_area="log_button"),
            )

            # Stack the buttons in a vbox for easier plotting
            self.file_buttons = (
                self.netcdf_button,
                self.agg_netcdf_button,
                self.h5_button,
                self.log_button
            )
            self.file_buttons_box = widgets.VBox(
                self.file_buttons, layout=item_layout(grid_area="file_buttons_box")
            )

            # Raster buttons ----------------------------------------------------------------------------------
            self.raster_buttons_label = widgets.HTML(
                "Raster results", layout=item_layout(grid_area="raster_buttons_label")
            )

            # button to select which raster to download
            self.wlvl_button = widgets.ToggleButton(
                value=True,
                description="Water level at selected time",
                layout=item_layout(grid_area="wlvl_button"),
            )
            self.depth_button = widgets.ToggleButton(
                value=True,
                description="Water depth at selected time",
                layout=item_layout(grid_area="depth_button"),
            )
            self.max_wlvl_button = widgets.ToggleButton(
                value=True,
                description="Max water level",
                layout=item_layout(grid_area="max_wlvl_button"),
            )
            self.max_depth_button = widgets.ToggleButton(
                value=True,
                description="Max water depth",
                layout=item_layout(grid_area="max_depth_button"),
            )
            self.total_damage_button = widgets.ToggleButton(
                value=True,
                description="Total damage",
                layout=item_layout(grid_area="total_damage_button"),
            )
            self.depth_damage_button = widgets.ToggleButton(
                value=True,
                description="Depth (damage calc)",
                layout=item_layout(grid_area="depth_damage_button"),
            )

            # Stack the buttons in a vbox for easier plotting
            self.raster_buttons = (
                self.max_wlvl_button,
                self.max_depth_button,
                self.total_damage_button,
                self.wlvl_button,
                self.depth_button,
                self.depth_damage_button,
            )
            self.raster_buttons_box = widgets.VBox(
                self.raster_buttons, layout=item_layout(grid_area="raster_buttons_box")
            )


            # dropdown to pick timepoint to download raster  ---------------------------------------------------
            self.time_pick_label = widgets.Label(
                "Timestep raster:", layout=item_layout(grid_area="time_pick_label")
            )
            self.time_pick_dropdown = widgets.Dropdown(
                layout=item_layout(grid_area="time_pick_dropdown", description_width="initial")
            )

            # dropdown to pick resolution ----------------------------------------------------------------------
            self.resolution_label = widgets.Label(
                "Resolution [m]:", layout=item_layout(grid_area="resolution_label")
            )
            self.resolution_dropdown = widgets.Dropdown(
                layout=item_layout(
                    grid_area="resolution_dropdown", description_width="initial"
                ),
            )

            # Combine time pick and resolution in one box for GUI.
            self.time_resolution_box = widgets.HBox(
                [
                    widgets.VBox([self.time_pick_label, self.time_pick_dropdown], layout=item_layout()),
                    widgets.VBox([self.resolution_label, self.resolution_dropdown], layout=item_layout()),
                ],
                layout=item_layout(grid_area="time_resolution_box"),
            )


    class OutputLocationWidgets():
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


            # Selection box of the folder the output should be put in. (Hyd toets or Extreme)
            self.folder_options = ["0d1d_results", "1d2d_results",  "batch_results"]
            self.subfolder_label = widgets.Label(
                    "Sub folder:", layout=item_layout(grid_area="output_subfolder_label")
                )
            self.subfolder_box = widgets.Select(
                    options=self.folder_options,
                    rows=3,
                    disabled=False,
                    layout=item_layout(grid_area="output_subfolder_box"),
                )



            self.output_select_box = widgets.SelectMultiple(
                rows=13,
                description="",
                disabled=True,
                layout=item_layout(height="95%", grid_area="output_select_box"),
            )


    class DownloadWidgets():
        def __init__(self) -> None:
            self.label = widgets.HTML(
                "<b>6. Download selected</b>",
                layout=item_layout(grid_area="download_button_label"),
            )
            self.custom_extent_button = widgets.ToggleButton(
                value=True,
                description="Use custom extent",
                tooltip="Format should be x1,y1,x2,y2. Value is filled with the dem extent when selecting the option.",
                layout=item_layout(grid_area="custom_extent_button"),
            )
            self.custom_extent_widget = widgets.Text(
                value="x1,y1,x2,y2", 
                tooltip="Format should be x1,y1,x2,y2. Value is filled with the dem extent when selecting the option.",
                layout=item_layout(grid_area="custom_extent_widget")
            )
            self.button = widgets.Button(
                description="Download",
                layout=item_layout(
                    height="50px", grid_area="download_button", align_self="flex-end"
                ),
            )


    class DownloadBatchWidgets():
        def __init__(self) -> None:
                # Download results button
            self.label = widgets.HTML(
                "<b>7. Download klimaatsommen</b>",
                layout=item_layout(grid_area="download_batch_button_label"),
            )
            self.button = widgets.Button(
                description="Download batch",
                layout=item_layout(
                    height="50px", grid_area="download_batch_button", align_self="flex-end"
                ),
            )
            self.button.style.button_color = "lightgreen"

            # Select folder to download batch to
            self.batch_folder_label = widgets.Label(
                "Batch folder naam (maak aan als niet bestaat!):",
                layout=item_layout(grid_area="batch_folder_label"),
            )
            self.batch_folder_dropdown = widgets.Dropdown(
                options="",
                disabled=False,
                layout=item_layout(grid_area="batch_folder_dropdown"),
            )

            # Select DEM file to use as reference resolution and extent for raster download
            self.dem_path_label = widgets.Label(
                "DEM path:", layout=item_layout(grid_area="dem_path_label")
            )
            self.dem_path_dropdown = widgets.Dropdown(
                options="",
                disabled=False,
                layout=item_layout(grid_area="dem_path_dropdown"),
            )


class DownloadWidgetsInteraction(DownloadWidgets):
    def __init__(self, caller):
        super().__init__()
        self.caller = caller

        # --------------------------------------------------------------------------------------------------
        # 1. Login with API key
        # --------------------------------------------------------------------------------------------------
        @self.login.button.on_click
        def login(action): 
            self.vars.sim = Simulation(api_key=self.vars.api_keys["threedi"]) 
            dl.set_api_key(self.vars.api_keys['lizard'])
            try:
                if self.vars.sim.logged_in == "Cannot login":
                    raise
                # Login success
                self.login.button.style.button_color = "lightgreen"
                self.login.button.description = "Logged in"
            except:
                # Login failed
                self.login.button.style.button_color = "red"
                self.login.button.description = "Invalid API key"


        # --------------------------------------------------------------------------------------------------
        # 2. Select polder (and show revision)
        # --------------------------------------------------------------------------------------------------
        @self.search.search_button_lizard.on_click
        def find(action):

            self.search.search_button_lizard.style.button_color = "orange"
            self.search.search_button_lizard.description = "Searching..."
            

            self.vars.scenarios = dl.find_scenarios(
                name__icontains=self.search.sim_name_widget.value,
                model_revision=self.search.sim_rev_widget.value,
                limit=self.search.limit_widget.value,
            )

            # Reset/initialize results
            self.vars.scenario_results = {}
            self.vars.scenario_result_type = "lizard"

            # Update selection box
            self.select.dl_select_box.options = self.vars.scenario_view_names

            self.search.search_button_lizard.style.button_color = "lightgreen"
            self.search.search_button_threedi.style.button_color = None
            self.search.search_button_lizard.description = "Search Lizard"

            
        @self.search.search_button_threedi.on_click
        def find(action):

            self.search.search_button_threedi.style.button_color = "orange"
            self.search.search_button_threedi.description = "Searching..."
            

            self.vars.scenarios = self.vars.sim.threedi_api.simulations_list(
                name__icontains=self.search.sim_name_widget.value,
                limit=self.search.limit_widget.value,
            ).results

            # Reset/initialize results
            self.vars.scenario_results = {}
            self.vars.scenario_result_type = "threedi"

            # Update selection box
            self.select.dl_select_box.options = self.vars.scenario_view_names

            self.search.search_button_threedi.style.button_color = "lightgreen"
            self.search.search_button_lizard.style.button_color = None
            self.search.search_button_threedi.description = "Search 3Di"      


        # --------------------------------------------------------------------------------------------------
        # 3. SelectWidgets
        # --------------------------------------------------------------------------------------------------
        @self.select.show_0d1d_button.on_click
        def show(action):
            self.select.search_results_widget.value = "0d1d"

        @self.select.filter_button.on_click
        def show(action):
            self.select.search_results_widget.value = "-"


        def on_text_change(search_input):
            search_str = search_input["new"]

            if search_str.startswith("-"):
                #exclude search results
                self.select.dl_select_box.options = [
                    a for a in self.vars.scenario_view_names if search_str[1:] not in a
                ]
            else:
                self.select.dl_select_box.options = [
                    a for a in self.vars.scenario_view_names if search_str in a
                ]


        self.select.search_results_widget.observe(on_text_change, names="value")


        def get_scenarios_selected_result(value):
            
            self.select.dl_select_label.value = self.select.dl_select_label_text.format(len(self.select.dl_select_box.value)) #show how many selected
            self.get_scenario_results() #Get available results for selected scenarios.
            self.update_buttons()  # Change button state based on selected scenarios
            self.update_time_pick_dropdown()  # change button state and dropdown based on selected scenarios

            #update output folder if 0d1d selected.
            if all(["0d1d_" in x for x in self.select.dl_select_box.value]):
                self.output.subfolder_box.value =self.output.subfolder_box.options[0]
            else:
                self.output.subfolder_box.value =self.output.subfolder_box.options[1]


        self.select.dl_select_box.observe(get_scenarios_selected_result, names="value")


        # --------------------------------------------------------------------------------------------------
        # 4. Result layers selection
        # --------------------------------------------------------------------------------------------------
        for button in self.outputtypes.file_buttons + self.outputtypes.raster_buttons:
            button.observe(self._update_button_icon, names="value")


        # --------------------------------------------------------------------------------------------------
        # 5. destination folder creation/selection
        # --------------------------------------------------------------------------------------------------
        def update_displays(value):
            #display current available results
            self.output.output_select_box.options = (self.vars.folder.threedi_results[self.output.subfolder_box.value].revisions)

            #Also display the selected result as option
            self.update_output_selectbox(self.select.dl_select_box.value)
            
        self.output.subfolder_box.observe(update_displays, names="value")

        # If a new value is selected in the download selection folder, update the output folder
        self.select.dl_select_box.observe(self.update_output_selectbox, names="value")

        # If batch folder selected, update batch download button
        self.download_batch.batch_folder_dropdown.observe(self.change_dowloadbutton_state, "value")


        # Set resolution options
        def update_resolution_options(value):
            self.outputtypes.resolution_dropdown.options = self.resolution_view_list
            self.outputtypes.resolution_dropdown.value = self.resolution_view_list[
                        self.vars.resolution_list.index(self.selected_dem.metadata.pixel_width)
            ]
        self.download_batch.dem_path_dropdown.observe(update_resolution_options, "value")

        # --------------------------------------------------------------------------------------------------
        # 6. Download
        # --------------------------------------------------------------------------------------------------
        def update_custom_extent_widget(value):
            """Allow for custom extent when button is clicked"""
            if self.download.custom_extent_button.value is True:
                if self.selected_dem is not None:
                    self.download.custom_extent_widget.value = self.selected_dem.metadata.bbox
                self.download.custom_extent_widget.disabled = False
            else:
                self.download.custom_extent_widget.value = "x1, y1, x2, y2"
                self.download.custom_extent_widget.disabled = True
        self.download.custom_extent_button.observe(self._update_button_icon, names="value")
        self.download.custom_extent_button.observe(update_custom_extent_widget, "value")


        @self.download.button.on_click
        def download(action):
            """Download the selected models to the output folders"""
            self.vars.output_df = pd.DataFrame()

            #Temporary disable download button
            self.download.button.style.button_color = "orange"
            self.download.button.description = "Downloading..."
            self.download.button.disabled = True
            
            #Init empty raster settings.
            dl_raster_settings = dlRasterSettingsV4()

            # Start download of selected files (if any are selected) ------------------------------------------------
            self.vars.scenario_raw_download_urls = self.create_scenario_raw_download_urls()
            for scenario_id in self.scenario_selected_ids:
                scenario = self.vars.scenarios[scenario_id]
                scenario_result = self.vars.scenario_results[scenario_id]
                download_urls = self.vars.scenario_raw_download_urls[scenario_id]
                scenario_name = scenario.name

                output_folder = str(self.vars.folder.threedi_results[self.output.subfolder_box.value][scenario_name])

                # De 3Di plugin kan geen '[' en ']' aan.
                output_folder = output_folder.replace("[", "")
                output_folder = output_folder.replace("]", "")


                # Print download URLs
                display(HTML(f"\n<font color='#ff4d4d'>Downloading files for {scenario_name} (uuid={scenario.uuid}, <a href={self.get_scenario_management_link(scenario_id)}>management page</a>):</font>"))
                # for index, key in enumerate(download_urls):
                #     print("{}: {}".format(index + 1, download_urls[key]))
                print(f"\nThey will be placed in:\n{output_folder}\n")

                # Create destination folder
                if not os.path.exists(output_folder) and output_folder != "":
                    os.mkdir(output_folder)
                #             print('Created folder: ' + output_folder.rsplit('/')[-1])

                #add some variables to overview
                for index, key in enumerate(download_urls):
                    self.vars.output_df = self.vars.output_df.append({
                                    "id":scenario_id,
                                    "name": scenario_name,
                                    "uuid": scenario.uuid,
                                    "scenario_download_url": download_urls[key],
                                    "output_folder": output_folder}, 
                                ignore_index=True)

                #Download files
                if self.vars.scenario_result_type == "lizard":
                    download_functions.start_download(
                        download_urls.values(),
                        output_folder,
                        api_key=dl.get_api_key(),
                        automatic_download=1,
                    )

                elif self.vars.scenario_result_type == "threedi":
                    for index, key in enumerate(download_urls):
                        try:
                            if key=="grid-admin":
                                download = self.vars.sim.threedi_api.threedimodels_gridadmin_download(scenario.threedimodel_id)
                                filename = "gridadmin.h5"
                            else:
                                download = self.vars.sim.threedi_api.simulations_results_files_download(id=scenario_result[key].id, simulation_pk=scenario.id)
                                filename = scenario_result[key].file.filename

                            output_file = os.path.join(output_folder, filename)

                            download_succes = get_threedi_download_file(download = download, 
                                                        output_file = output_file,
                                                        overwrite=False)
                            
                            if download_succes:
                                # display(HTML(f'{index}. File created at <a href={output_file}>{output_file}</a>')) #link doesnt work in vs code
                                print(f'{index}. File created at {output_file}')
                            else:
                                print(f"{index}. File {filename} is already on the system")
                        except Exception as e:
                            print(f"{index}. Couldnt download {key} of {scenario.name}. Errormessage;\n {e}")
                    

                # Start download of selected lizard rasters (if any are selected) -----------------------------------------------                    
                if self.download.custom_extent_button.value:

                    #This button makes sure we always get the same bounding box as the dem that is used in the model
                    class dlRasterPreset(dlRaster):
                        def __init__(self, 
                                        scenario_uuid=scenario.uuid, 
                                        resolution=self.selected_resolution,
                                        bbox=self.download.custom_extent_widget.value,
                                        **kwargs):
                            super().__init__(scenario_uuid=scenario_uuid, 
                                             resolution=resolution, 
                                             bbox=bbox, 
                                             **kwargs)
                else:
                    class dlRasterPreset(dlRaster):
                        def __init__(self, 
                                        scenario_uuid=scenario.uuid, 
                                        resolution=self.selected_resolution,
                                        **kwargs):
                            super().__init__(scenario_uuid=scenario_uuid, 
                                             resolution=resolution, 
                                             **kwargs)


                raster_max_wlvl = dlRasterPreset(raster_code="s1-max-dtri",
                                        output_path=os.path.join(output_folder, f"max_wlvl_res{self.selected_resolution_view}m.tif"), 
                                        button=self.outputtypes.max_wlvl_button,
                                        name="max waterlevel",
                )
                raster_max_depth = dlRasterPreset(raster_code="depth-max-dtri",
                                        output_path=os.path.join(output_folder, f"max_depth_res{self.selected_resolution_view}m.tif"), 
                                        button=self.outputtypes.max_depth_button,
                                        name="max waterdepth",
                )
                raster_total_damage = dlRasterPreset(raster_code="total-damage",
                                        output_path=os.path.join(output_folder, f"total_damage_res{self.selected_resolution_view}m.tif"), 
                                        button=self.outputtypes.total_damage_button,
                                        name="total damge",
                )
                raster_wlvl = dlRasterPreset(raster_code="s1-dtri",
                                        timelist=self.selected_time,
                                        output_path=os.path.join(output_folder, f"wlvl_{self.selected_time_view}_res{self.selected_resolution_view}m.tif"), 
                                        button=self.outputtypes.wlvl_button,
                                        name="waterlevel at timestep {time}",
                )
                raster_wdepth = dlRasterPreset(raster_code="depth-dtri",
                                        timelist=self.selected_time,
                                        output_path=os.path.join(output_folder, f"depth_{self.selected_time_view}_res{self.selected_resolution_view}m.tif"), 
                                        button=self.outputtypes.depth_button,
                                        name="waterdepth at timestep {time}",
                )
                raster_depth_dmg = dlRasterPreset(raster_code="dmge-depth",
                                        output_path=os.path.join(output_folder, f"depth_for_lizard_dmg_res{self.selected_resolution_view}m.tif"), 
                                        button=self.outputtypes.depth_damage_button,
                                        name="waterdepth for lizard damage calc",
                )


                for r in [raster_max_wlvl, raster_max_depth, raster_total_damage, raster_wlvl, raster_wdepth, raster_depth_dmg]:
                    if r.button.value == True:
                        if not os.path.exists(r.output_path):
                            dl_raster_settings.add_raster(r)
                        else:
                            print("{} already on system".format(r.output_path.split("/")[-1]))


            if len(dl_raster_settings.scenario_uuid_list)==0:
                print("\nNo rasters will be downloaded")
            else:
                print("\nStarting download of rasters")
                dl_raster_settings.print()
                print(f"Wait until download is finished")
                
                self.vars.dl_raster_settings=dl_raster_settings
                logging_batch_path = os.path.join(
                    output_folder,
                    "download_raster_batch_{}.csv".format(datetime.datetime.now().strftime("%Y-%m-%d %Hh%M")),
                )

                dl.download_raster(
                    scenario=dl_raster_settings.scenario_uuid_list,
                    raster_code=dl_raster_settings.raster_code_list,
                    projection=dl_raster_settings.projection_list,
                    resolution=dl_raster_settings.resolution_list,
                    bbox=dl_raster_settings.bbox_list,
                    time=dl_raster_settings.time_list,
                    pathname=dl_raster_settings.pathname_list,
                    export_task_csv=logging_batch_path,
                )

                print("Download of rasters finished")


            #Re enable download button
            self.download.button.style.button_color = "lightgreen"
            self.download.button.description = "Download"
            self.download.button.disabled = False


        @self.download_batch.button.on_click
        def download_batch(action):
            """Download the selected models to the output folders"""
            # Initialize folders and load directory structure

            self.vars.batch_fd = self.vars.folder.threedi_results.batch[self.download_batch.batch_folder_dropdown.value]

            # batch_fd = create_batch_folders_dict(batch_folder)
            # batch_fd = Folders(batch_folder).
            # Create destination folder

            self.vars.batch_fd.create()
            self.vars.batch_fd.downloads.create()

            #Temporary disable download button
            self.download_batch.button.style.button_color = "orange"
            self.download_batch.button.description = "Downloading..."
            self.download_batch.button.disabled = True


            # Link selected files to scenarios. e.g: BWN Hoekje [#30] GLG blok 10 (10) 	 blok_GLG_T10
            df = pd.DataFrame(self.select.dl_select_box.value, columns=["name"])
            for index, row in df.iterrows():
                scenario_id = self.vars.scenario_view_names.index(row["name"])
                scenario = self.vars.scenarios[scenario_id]

                #overwrite viewname with normal name
                df.loc[index, "name"] = self.vars.scenario_names[scenario_id] 
                name = row["name"]
                for rain_type in RAIN_TYPES:
                    for groundwater in GROUNDWATER:
                        for rain_scenario in RAIN_SCENARIOS:
                            rain_scenario = rain_scenario.strip("T")  # strip 'T' because its not used in older versions.

                            #Scenario should have nameformat piek/blok_gxg_Txx
                            if (
                                (rain_type in name)
                                and (groundwater.lower() in name.lower())
                                and (rain_scenario in name)
                            ):
                                if "0" not in [
                                    name[a.start() + len(rain_scenario)]
                                    for a in re.finditer(rain_scenario, name)
                                    if len(name) > (a.start() + len(rain_scenario))
                                ]:  # filters this: BWN Hoekje [#10] GLG blok 100 (10)
                                    df.loc[index, "dl_name"] = f"{rain_type}_{groundwater}_T{rain_scenario}"
                                    df.loc[index, "uuid"] = scenario.uuid
                df.loc[index, "management_link"] = self.get_scenario_management_link(scenario_id=scenario_id)

            df.set_index("name", inplace=True)
            # display(df)

            if "uuid" not in df.keys():
                print("None of selected scenarios can be linked to one of the 18 scenario's (blok_gxg_T10 not in name)")
            else:
                df.to_csv(str(
                    self.vars.batch_fd.downloads.full_path("download_netcdf_batch_{}.csv".format(datetime.datetime.now().strftime("%Y-%m-%d %Hh%M")))
                    )
                )

                # Get raster size of dem, max depth rasters are downloaded on this resolution.
                # print(self.vars.folder)
                # print(self.vars.folder.model.schema_base.rasters.full_path(self.download_batch.dem_path_dropdown.value))
                dem = self.vars.folder.model.schema_base.rasters.dem


                #Init empty raster settings.
                dl_raster_settings = dlRasterSettingsV4()

                # Start download of selected files (if any are selected) ------------------------------------------------
                self.scenario_raw_download_urls = self.create_scenario_raw_download_urls()
                for name, row in df.iterrows():
                    scenario_id = self.vars.scenario_names.index(name)
                    scenario = self.vars.scenarios[scenario_id]
                    scenario_result = self.vars.scenario_results[scenario_id]
                    download_urls = self.scenario_raw_download_urls[scenario_id]

                    # print(f"\n\033[1m\033[31mDownloading files for {name} (uuid={scenario.uuid}):\033[0m")
                    display(HTML(f"\n<font color='#ff4d4d'>Downloading files for {scenario.name} (uuid={scenario.uuid}, <a href={self.get_scenario_management_link(scenario_id)}>management page</a>):</font>"))


                    #Download netcdf of all results.
                    self.vars.batch_fd.downloads.names #FIXME downloads.names need to be initialized first.
                    output_folder = getattr(self.vars.batch_fd.downloads, row["dl_name"]).netcdf

                    # Create destination folder
                    output_folder.create()
                    output_folder = output_folder.path

                    # Start downloading of the files
                    if self.vars.scenario_result_type == "lizard":
                        download_functions.start_download(
                            download_urls.values(),
                            output_folder,
                            api_key=dl.get_api_key(),
                            automatic_download=1,
                        )

                    elif self.vars.scenario_result_type == "threedi":
                        for index, key in enumerate(download_urls):
                            try:
                                if key=="grid-admin":
                                    download = self.vars.sim.threedi_api.threedimodels_gridadmin_download(scenario.threedimodel_id)
                                    filename = "gridadmin.h5"
                                else:
                                    download = self.vars.sim.threedi_api.simulations_results_files_download(id=scenario_result[key].id, simulation_pk=scenario.id)
                                    filename = scenario_result[key].file.filename

                                output_file = os.path.join(output_folder, filename)

                                download_succes = get_threedi_download_file(download = download, 
                                                            output_file = output_file,
                                                            overwrite=False)
                                
                                if download_succes:
                                    # display(HTML(f'{index}. File created at <a href={output_file}>{output_file}</a>')) #link doesnt work in vs code
                                    print(f'{index}. File created at {output_file}')
                                else:
                                    print(f"{index}. File {filename} is already on the system")
                            except Exception as e:
                                print(f"{index}. Couldnt download {key} of {scenario.name}. Errormessage;\n {e}")


                    if self.download.custom_extent_button.value:
                        #This button makes sure we always get the same bounding box as the dem that is used in the model
                        class dlRasterPreset(dlRaster):
                            def __init__(self, 
                                            scenario_uuid=scenario.uuid, 
                                            resolution=self.selected_resolution,
                                            bbox=self.download.custom_extent_widget.value,
                                            **kwargs):
                                super().__init__(scenario_uuid=scenario_uuid, 
                                                resolution=resolution, 
                                                bbox=bbox, 
                                                **kwargs)
                    else:
                        class dlRasterPreset(dlRaster):
                            def __init__(self, 
                                            scenario_uuid=scenario.uuid, 
                                            resolution=self.selected_resolution,
                                            **kwargs):
                                super().__init__(scenario_uuid=scenario_uuid, 
                                                resolution=resolution, 
                                                **kwargs)


                    wlvl_max = getattr(self.vars.batch_fd.downloads, row["dl_name"]).wlvl_max
                    raster_max_wlvl = dlRasterPreset(raster_code="s1-max-dtri",
                                            output_path=wlvl_max.base, 
                                            button=self.outputtypes.max_wlvl_button,
                                            name="max waterlvl",                                        
                    )
                    depth_max = getattr(self.vars.batch_fd.downloads, row["dl_name"]).depth_max
                    raster_max_depth = dlRasterPreset(raster_code="depth-max-dtri",
                                            output_path=depth_max.base, 
                                            button=self.outputtypes.max_depth_button,
                                            name="max waterdepth",
                    )
                    damage_total = getattr(self.vars.batch_fd.downloads, row["dl_name"]).damage_total
                    raster_total_damage = dlRasterPreset(raster_code="total-damage",
                                            output_path=damage_total.base, 
                                            button=self.outputtypes.total_damage_button,
                                            name="total damge",
                    )
                    

                    for r in [raster_max_wlvl, raster_max_depth, raster_total_damage]:
                        if r.button.value == True:
                            if not os.path.exists(r.output_path):
                                dl_raster_settings.add_raster(r)

                #To vars so we can inspect.
                self.vars.dl_raster_settings = dl_raster_settings

                if len(dl_raster_settings.scenario_uuid_list)==0:
                    print("\nNo rasters will be downloaded")
                else:
                    print("\nStarting download of rasters")
                    dl_raster_settings.print()
                    print(f"Wait until download is finished")

                    logging_batch_path = self.vars.batch_fd.downloads.full_path(
                        "download_raster_batch_{}.csv".format(datetime.datetime.now().strftime("%Y-%m-%d %Hh%M")),
                    )

                    dl.download_raster(
                        scenario=dl_raster_settings.scenario_uuid_list,
                        raster_code=dl_raster_settings.raster_code_list,
                        projection=dl_raster_settings.projection_list,
                        resolution=dl_raster_settings.resolution_list,
                        bbox=dl_raster_settings.bbox_list,
                        pathname=dl_raster_settings.pathname_list,
                        export_task_csv=logging_batch_path,
                    )
                    print("Download of rasters finished")

            self.download_batch.button.style.button_color = "lightgreen"
            self.download_batch.button.description = "Download batch"
            self.download_batch.button.disabled = False


    def update_folder(self):
        """when main folder changes, we update some values"""

        #Output folder string
        self.output.folder_value.value = self.vars.folder.threedi_results.base

        # Set dem options
        self.download_batch.dem_path_dropdown.options = [self.vars.folder.model.schema_base.rasters.dem.path]
        self.download_batch.dem_path_dropdown.value = self.vars.folder.model.schema_base.rasters.dem.path


    # --------------------------------------------------------------------------------------------------
    # 3. SelectWidgets
    # --------------------------------------------------------------------------------------------------
    def get_scenario_results(self):

        def find_scenario_results_lizard(scenario_url):
            #TODO zou in threedi_scenario_downloader moeten staan.
            """results under scenario (raw results / rasters)"""
            payload = {"limit": 20} #Always want to see all available results.

            url = f"{scenario_url}results/"
            r=requests.get(url=url, auth=("__key__", dl.get_api_key()), params=payload)
            r.raise_for_status()
            return r.json()["results"]

        #Search for available results per scenario
        for index, scenario_id in enumerate(self.scenario_selected_ids):
            scenario = self.vars.scenarios[scenario_id]
            
            #Every result will be placed in dict per scenario with key=code (e.g. s1-max-dtri)
            self.vars.scenario_results[scenario_id]= {}

            if self.vars.scenario_result_type == "lizard":
                results = find_scenario_results_lizard(scenario_url=scenario.url)
                #Loop individual results and add to dict
                for result in results:
                    self.vars.scenario_results[scenario_id][result["code"]]=result
                
                #Provide link to management page
                if index==0:
                    self.outputtypes.file_buttons_label.value = f'File results (<a href={self.get_scenario_management_link(scenario_id)} target="_blank">management page</a>)'


            if self.vars.scenario_result_type == "threedi":
                results = self.vars.sim.threedi_api.simulations_results_files_list(simulation_pk=scenario.id).results

                #Add available results to scenario_results. Give them the same keys as the lizard ones
                for result in results:
                    code = None
                    if result.file.filename.startswith("log"):
                        code = "logfiles"
                    if result.file.filename == 'results_3di.nc':
                        code="results-3di"
                    if result.file.filename == "aggregate_results_3di.nc":
                        code="aggregate-results-3di"

                    #Uploaded files only, if they have been removed they get the state 'removed'
                    if result.file.state == "uploaded" and code is not None:
                        self.vars.scenario_results[scenario_id][code]=result
                if results != []:
                    self.vars.scenario_results[scenario_id]["grid-admin"] = True

                #Provide link to management page
                if index==0:
                    self.outputtypes.file_buttons_label.value = f'File results (<a href={self.get_scenario_management_link(scenario_id)} target="_blank">management page</a>)'

        if self.scenario_selected_ids == []:
            self.outputtypes.file_buttons_label.value = f'File results'


    # --------------------------------------------------------------------------------------------------
    # 4. Result layers selection
    # --------------------------------------------------------------------------------------------------
    def update_time_pick_dropdown(self):
        """Update options with time intervals.
        If there are multiple results, all time series will be analyzed. If these are not the same,
        selecting these output rasters is disabled."""

        def retrieve_time_interval(selected_result):
            """retrieve selected time"""
            try:
                Tstart = datetime.datetime.strptime(
                    selected_result["simulation_start"], "%Y-%m-%dT%H:%M:%SZ"
                )
                Tend = datetime.datetime.strptime(selected_result["simulation_end"], "%Y-%m-%dT%H:%M:%SZ")

                dates = pd.date_range(Tstart, Tend, freq="H")
                time_pick_options = [(date.strftime("%Y-%m-%dT%H:%M")) for date in dates]
                return time_pick_options
            except:
                return None

        time_pick_options = []
        for scenario_id in self.scenario_selected_ids:
            time_pick_option = retrieve_time_interval(self.vars.scenarios[scenario_id])
            if time_pick_option is not None:
                time_pick_options.append(time_pick_option)

        if not all(
            [len(x) == len(time_pick_options[0]) for x in time_pick_options]
        ):  # check if all timeseries are equally long
            for button in (self.outputtypes.wlvl_button, self.outputtypes.depth_button):
                self.change_button_state(button, button_disabled=True, button_value=False)
                self.outputtypes.time_pick_dropdown.disabled = True
        elif (
            not len(set([x[0] for x in time_pick_options])) == 1
        ):  # check if the start timestep is the same for all results
            for button in (self.outputtypes.wlvl_button, self.outputtypes.depth_button):
                self.change_button_state(button, button_disabled=True, button_value=False)
                self.outputtypes.time_pick_dropdown.disabled = True
        else:  # If above two conditions are met the buttons may stay enabled.
            self.outputtypes.time_pick_dropdown.disabled = False
            self.outputtypes.time_pick_dropdown.options = time_pick_options[0]


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

        # Depening on the button values, change the download button color
        self.change_dowloadbutton_state()  


    def change_dowloadbutton_state(self, *args):
        """Change color and disabled for download button."""
        #Download button
        if any([a.value for a in self.outputtypes.file_buttons + self.outputtypes.raster_buttons]):
            self.download.button.disabled = False
            self.download.button.style.button_color = "lightgreen"
        else:
            self.download.button.disabled = True
            self.download.button.style.button_color = "red"

        #Download batch button
        if all([
                any([self.outputtypes.netcdf_button.value is True,
                self.outputtypes.agg_netcdf_button.value is True]),
            self.download_batch.batch_folder_dropdown.value is not None
        ]):
            self.download_batch.button.disabled = False
            self.download_batch.button.style.button_color = "lightgreen"
        else:
            self.download_batch.button.disabled = True
            self.download_batch.button.style.button_color = "red"



    def change_button_state(self, button, button_disabled=False, button_value=False):
        """Disable buttons based on input"""
        button.disabled = button_disabled  # Disable button based on input
        button.value = button_value
        self._update_button_icon(button)



    def update_buttons(self):
        """Update the buttons with possible results to download, enabling or disabling them based
        on their occrence in the results"""
        result_codes = []
        for index, scenario_id in enumerate(self.scenario_selected_ids):
            result_codes += self.vars.scenario_results[scenario_id].keys()

        # select results that appear in all selected scenarios. If for instance rasters are not in one of the results
        # all rasters are dropped from the list.
        is_result_in_all_selected = [
            nr_items == len(self.scenario_selected_ids)
            for nr_items in [result_codes.count(x) for x in set(result_codes)]
        ]
        result_codes = [
            x for x, y in zip(set(result_codes), is_result_in_all_selected) if y == True
        ]

        # we now know which results are available for all selected models. Only these buttons will be available for download.
        # Enable or disable buttons based on their availability in the results
        for code in self.button_codes:
            if code in result_codes:
                if code in ["results-3di", "grid-admin", "logfiles", "aggregate-results-3di"]:
                    self.change_button_state(self.button_codes[code], button_disabled=False, button_value=True)
                else:
                    self.change_button_state(self.button_codes[code], button_disabled=False, button_value=False)

            else:
                self.change_button_state(self.button_codes[code], button_disabled=True, button_value=False)
        self.vars.result_codes=result_codes


    # --------------------------------------------------------------------------------------------------
    # 5. destination folder creation/selection
    # --------------------------------------------------------------------------------------------------
    def update_output_selectbox(self, selected_download=""):
        """Add the selected download files to the output display and select them"""
        # Depends on how the function is called. If it was called with observe use the first try, otherwise use the except
        try:
            selected_download_new = selected_download["new"]
        except:
            selected_download_new = selected_download

        
        self.output.output_select_box.options = (
            self.vars.folder.threedi_results[self.output.subfolder_box.value].revisions
        )

        # Batch folder gets only batch_folder options
        self.download_batch.batch_folder_dropdown.options = (
            self.vars.folder.threedi_results["batch_results"].revisions
        )

        # Add the newly selected records to the list
        selected_download_newer=[]
        for new_selected in selected_download_new:
            #New_selected is the viewname. We only want the name without dates and stuff as output folder.
            selected_id = self.vars.scenario_view_names.index(new_selected)
            new_selected = self.vars.scenario_names[selected_id]
            selected_download_newer.append(new_selected)


            if new_selected not in self.output.output_select_box.options:
                self.output.output_select_box.options = self.output.output_select_box.options + (new_selected,)

        # Select these new records.
        self.output.output_select_box.value = selected_download_newer



    def update_api_keys(self, api_keys_path):
        self.vars.api_keys = hrt.read_api_file(api_keys_path)
        self.login.lizard_apikey_widget.value=self.vars.api_keys["lizard"]
        self.login.threedi_apikey_widget.value=self.vars.api_keys["threedi"]
        self.login.button.click()

    @property
    def button_codes(self):
        """Map buttons to scenario result code on lizard"""
        return  {"results-3di": self.outputtypes.netcdf_button,
                        "grid-admin": self.outputtypes.h5_button, 
                        "logfiles": self.outputtypes.log_button,
                        "aggregate-results-3di": self.outputtypes.agg_netcdf_button,
                        "s1-max-dtri": self.outputtypes.max_wlvl_button,
                        "depth-max-dtri": self.outputtypes.max_depth_button,
                        "s1-dtri": self.outputtypes.wlvl_button,
                        "depth-dtri": self.outputtypes.depth_button,
                        "total-damage": self.outputtypes.total_damage_button,
                        "dmge-depth": self.outputtypes.depth_damage_button,
                        }


    @property
    def scenario_selected_ids(self):
        return  [self.vars.scenario_view_names.index(a) for a in self.select.dl_select_box.value]  # id's of selected models to download


    def create_scenario_raw_download_urls(self):
        """Retrieve urls from scenario raw results of selected results"""
        scenario_download_urls = {}
        for scenario_id in self.scenario_selected_ids:
            scenario_result = self.vars.scenario_results[scenario_id]
            download_urls = {}

            for code in ["results-3di", "grid-admin", "logfiles", "aggregate-results-3di"]:
                if self.button_codes[code].value:

                    if self.vars.scenario_result_type == "lizard":
                        download_urls[code] = scenario_result[code]["attachment_url"]
                    elif self.vars.scenario_result_type == "threedi":
                        download_urls[code] = code
            scenario_download_urls[scenario_id] = download_urls
        return scenario_download_urls

    def get_scenario_management_link(self, scenario_id):
        scenario = self.vars.scenarios[scenario_id]
        
        if self.vars.scenario_result_type =="lizard":
            return f"https://hhnk.lizard.net/management/data_management/scenarios/scenarios/{scenario.uuid}"
        elif self.vars.scenario_result_type =="threedi":
            return f"https://management.3di.live/simulations/{scenario.id}"
    

    @property
    def resolution_view_list(self):
        l = []
        if self.selected_dem is not None:
            dem_pixelwidth = self.selected_dem.metadata.pixel_width
        else:
            dem_pixelwidth = 0
        for i in self.vars.resolution_list:
            if i==dem_pixelwidth:
                l.append(f"{i} (dem resolution)")
            else:
                l.append(i)
        return l

    @property
    def selected_resolution(self):
        return self.vars.resolution_list[self.resolution_view_list.index(self.outputtypes.resolution_dropdown.value)]

    @property
    def selected_resolution_view(self):
        return str(self.selected_resolution).replace(".", "_")

    @property
    def selected_dem(self):
        dem_path = self.download_batch.dem_path_dropdown.value
        if dem_path is not None:
            return hrt.Raster(dem_path)
        else:
            return None

    @property
    def selected_time(self):
        return self.outputtypes.time_pick_dropdown.value

    @property
    def selected_time_view(self):
        timestr = self.outputtypes.time_pick_dropdown.value
        if timestr is not None:
            timestr = timestr.replace("-", "_")
            timestr = timestr.replace(":", "_")
        return timestr

    @property
    def vars(self):
        return self.caller.vars


class GuiVariables:
    def __init__(self) -> None:
        self._folder = None

        self.scenario_results = None #Filled when selecting a scenario, reset when clicking search
        self.scenario_result_type = None #Filled when searching (lizard/threedi)

        self.dl_raster_settings = None #filled when clicking download

        self.api_keys = {"lizard":"", "threedi":""}
        self._scenarios = []  #filled when clicking search


    @property
    def folder(self):
        return self._folder

    @folder.setter
    def main_folder(self, main_folder):
        self._folder = Folders(main_folder, create=False)


    @property
    def scenarios(self):
        return self._scenarios
    
    @scenarios.setter
    def scenarios(self, scenarios):
        """scenarios from lizard are of type dict, from threedi its;
        threedi_api_client.openapi.models.simulation.Simulation.
        Turn all results into classes so we can access them equally."""
        self._scenarios = []
        for scenario in scenarios:
            if type(scenario) == dict:
                scenario = hrt.dict_to_class(scenario)
            self._scenarios.append(scenario)


    @property
    def scenario_names(self):
        return [f"{scenario.name}" for scenario in self.scenarios]

    @property
    def scenario_view_names(self):
        return [f"{scenario.created[:10]}   |   {scenario.name}" for scenario in self.scenarios]
    

    @property
    def resolution_list(self):
        return [0.5, 1, 2, 5, 10, 25]
    

    @property
    def time_now(self):
        return datetime.datetime.now().strftime("%H:%M:%S")



class DownloadGui:
    def __init__(self, 
        data=None, 
        lizard_api_key=None, 
        threedi_api_key=None,
        main_folder=None, 
    ):

        self.vars = GuiVariables()
        self.widgets = DownloadWidgetsInteraction(self)

        if data:
            self.w.update_api_keys(api_keys_path=data["api_keys_path"])
            self.vars.main_folder = data["polder_folder"]
        else:
            self.vars.api_keys["lizard"] = lizard_api_key
            self.vars.api_keys["threedi"] = threedi_api_key
            self.vars.main_folder = main_folder

        self.w.update_folder()
        self.w.update_buttons() #disable filetype buttons
        self.w.output.subfolder_box.value = "1d2d_results"
        self.w.search.sim_name_widget.value = ""  #hopefully prevents cursor from going to api key field.
        self.w.download.custom_extent_button.value = False

        if not self.vars.main_folder:
            self.vars.main_folder = os.getcwd()


        # self.scheduler = BlockingScheduler(timezone="Europe/Amsterdam")



        self.download_tab = widgets.GridBox(
            children=[
                self.w.login.label,
                self.w.login.lizard_apikey_widget,
                self.w.login.threedi_apikey_widget,
                self.w.login.button,
                self.w.search.label,
                self.w.search.sim_name_label,
                self.w.search.sim_name_widget,
                self.w.search.sim_rev_label,
                self.w.search.sim_rev_widget,
                self.w.search.limit_label,
                self.w.search.limit_widget,
                self.w.search.search_button_lizard,
                self.w.search.search_button_threedi,
                self.w.select.dl_select_label,
                self.w.select.dl_select_box,
                self.w.select.show_0d1d_button,
                self.w.select.filter_button,
                self.w.select.search_results_widget,
                self.w.outputtypes.label,
                self.w.outputtypes.file_buttons_label,
                self.w.outputtypes.file_buttons_box,
                self.w.outputtypes.raster_buttons_label,
                self.w.outputtypes.raster_buttons_box,
                self.w.outputtypes.time_resolution_box,
                self.w.output.label,
                self.w.output.folder_label,
                self.w.output.folder_value,
                self.w.output.subfolder_label,
                self.w.output.subfolder_box,
                self.w.output.output_select_box,
                self.w.download.label,
                self.w.download.custom_extent_button,
                self.w.download.custom_extent_widget,
                self.w.download.button,
                self.w.download_batch.label,
                self.w.download_batch.button,
                self.w.download_batch.batch_folder_label,
                self.w.download_batch.batch_folder_dropdown,
                self.w.download_batch.dem_path_label,
                self.w.download_batch.dem_path_dropdown,
            ],
            layout=widgets.Layout(
                width="100%",
                grid_row_gap="200px 200px 200px 200px",
                grid_template_rows="auto auto auto auto auto auto auto auto auto auto auto auto",
                grid_template_columns="20% 10% 10% 15% 15% 10% 10% 10%",
                grid_template_areas="""
                    'login_label login_label search_label search_label . . . .'
                    'lizard_apikey_widget lizard_apikey_widget sim_name_label sim_name_widget sim_search_button_lizard sim_search_button_threedi . .'
                    'threedi_apikey_widget threedi_apikey_widget sim_rev_label sim_rev_widget sim_search_button_lizard sim_search_button_threedi . .'
                    'login_button login_button search_limit_label search_limit_widget sim_search_button_lizard sim_search_button_threedi . .'
                    'dl_select_label dl_select_label . outputtypes_label outputtypes_label output_label output_label output_label'
                    'dl_select_box dl_select_box dl_select_box file_buttons_label file_buttons_label output_folder_label output_folder_value output_folder_value'
                    'dl_select_box dl_select_box dl_select_box file_buttons_box file_buttons_box output_subfolder_label output_subfolder_box output_subfolder_box'
                    'dl_select_box dl_select_box dl_select_box file_buttons_box file_buttons_box output_select_box output_select_box output_select_box'
                    'dl_select_box dl_select_box dl_select_box raster_buttons_label raster_buttons_label output_select_box output_select_box output_select_box'
                    'dl_select_box dl_select_box dl_select_box raster_buttons_box raster_buttons_box output_select_box output_select_box output_select_box'
                    'search_results button_0d1d filter_button raster_buttons_box raster_buttons_box  output_select_box output_select_box output_select_box'
                    '. . . time_resolution_box time_resolution_box time_resolution_box download_button_label download_button_label'
                    '. . . time_resolution_box time_resolution_box time_resolution_box download_button download_button'
                    '. . . custom_extent_button custom_extent_widget custom_extent_widget download_batch_button_label download_batch_button_label'
                    'dem_path_label dem_path_label dem_path_label dem_path_label batch_folder_label batch_folder_label download_batch_button download_batch_button'
                    'dem_path_dropdown dem_path_dropdown dem_path_dropdown dem_path_dropdown batch_folder_dropdown batch_folder_dropdown download_batch_button download_batch_button'
                    """,
            ),
        )

    @property
    def tab(self):
        return self.download_tab
        # tab = widgets.Tab(children=[self.download_tab, self.start_batch_calculation_tab])
        # tab.set_title(0, "single calculation")
        # return tab

    @property
    def w(self):
        return self.widgets


if __name__ == "__main__":
        data = {'polder_folder': r'E:\02.modellen\callantsoog',
              'api_keys_path': fr"{os.getenv('APPDATA')}\3Di\QGIS3\profiles\default\python\plugins\hhnk_threedi_plugin\api_key.txt"}
        self = DownloadGui(data=data); 
        
        self.w.search.sim_name_widget.value = "model_test"
        display(self.tab)