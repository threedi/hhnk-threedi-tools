# %%
# system imports
import os
import datetime
import re
from pathlib import Path

# Third-party imports
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import ipywidgets as widgets
from IPython.core.display import HTML
from traitlets import Unicode
# from apscheduler.schedulers.blocking import BlockingScheduler
import requests


# threedi
# from threedi_scenario_downloader import downloader as dl #FIXME local changes, push to threedi_scenario_downloader?
from hhnk_threedi_tools.core.api import downloader as dl
import hhnk_threedi_tools.core.api.download_functions as download_functions

# local imports
import hhnk_threedi_tools as htt
import hhnk_research_tools as hrt
from hhnk_threedi_tools import Folders

from hhnk_threedi_tools.core.api.calculation import Simulation

# Globals
from hhnk_threedi_tools.variables.api_settings import (
    RAIN_TYPES,
    GROUNDWATER,
    RAIN_SCENARIOS,
)


dl.LIZARD_URL = "https://hhnk.lizard.net/api/v3/"
# THREEDI_API_HOST = "https://api.3di.live/v3"
RESULT_LIMIT = 50 #Results on search


def item_layout(width="95%", grid_area="", **kwargs):
    return widgets.Layout(
        width=width, grid_area=grid_area, **kwargs
    )  # override the default width of the button to 'auto' to let the button grow



class dlRaster():
    """Helper for input of download_functions."""
    def __init__(self, uuid, code, resolution, timelist, output_path, button, name, bounds=None, bounds_srs=None):
        self.uuid = uuid
        self.code = code
        self.resolution = resolution
        self.timelist = timelist
        self.output_path = output_path
        self.button = button
        self.name = name
        self.bounds = bounds
        self.bounds_srs = bounds_srs

class dlRasterSettings():
    def __init__(self, target_srs = "EPSG:28992") -> None:
        self.uuid_list = []
        self.code_list = []
        self.target_srs_list = target_srs
        self.resolution_list = []
        self.time_list = []
        self.pathname_list = []
        self.bounds_list = []
        self.bounds_srs_list = []

    def add_raster(self, r: dlRaster):
        self.uuid_list.append(r.uuid)
        self.code_list.append(r.code)
        self.resolution_list.append(r.resolution)
        self.time_list.append(r.timelist)
        self.pathname_list.append(r.output_path)
        self.bounds_list.append(r.bounds)
        self.bounds_srs_list.append(r.bounds_srs)



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
                "<b>1. Login with API keys</b>",
                layout=item_layout(grid_area="login_label"),
            )

            # Api key widgets
            self.lizard_apikey_widget = ApikeyWidget(
                description="Lizard key:",
                disabled=True,
                layout=item_layout(grid_area="lizard_apikey_widget"),
            )

            # self.threedi_apikey_widget = ApikeyWidget(
            #     description="Threedi key:",
            #     layout=item_layout(grid_area="threedi_apikey_widget"),
            # )

            # Login button, after login create threedi api client
            self.get_api_key_widget = widgets.HTML(
                'Get key <a href=https://hhnk.lizard.net/management/personal_api_keys target target="_blank">here</a>',
                layout=item_layout(grid_area="get_api_key_widget"),
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

            self.search_button = widgets.Button(
                description="Search",
                layout=item_layout(height="100%", grid_area="sim_search_button"),
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
                rows=20, layout=item_layout(grid_area="dl_select_box")
            )

            # Subset the list in the download selection box
            self.show_0d1d_button = widgets.Button(
                description="Show 0d1d",
                layout=item_layout(grid_area="button_0d1d", justify_self="end"),
            )

            self.show_all_button = widgets.Button(
                description="Show all", layout=item_layout(grid_area="all_button")
            )

            # Create search box
            self.search_results_widget = widgets.Text(layout=item_layout(grid_area="search_results"))


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
            resolution_options = [0.5, 1, 2, 5, 10, 25]
            self.resolution_label = widgets.Label(
                "Resolution [m]:", layout=item_layout(grid_area="resolution_label")
            )
            self.resolution_dropdown = widgets.Dropdown(
                options=resolution_options,
                value=10,
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
            self.use_dem_label = widgets.Label(
                "DEM extent:", layout=item_layout(grid_area="use_dem_label")
            )
            self.use_dem_button = widgets.ToggleButton(
                value=False,
                description="use",
                layout=item_layout(grid_area="use_dem_button"),
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
                "Naam van batch folder (maak aan als niet bestaat!):",
                layout=item_layout(grid_area="batch_folder_label"),
            )
            self.batch_folder_dropdown = widgets.Dropdown(
                options="",
                disabled=False,
                layout=item_layout(grid_area="batch_folder_dropdown"),
            )

            # Select DEM file to use to determine which resolution to download depth rasters on
            self.dem_path_label = widgets.Label(
                "Locatie DEM:", layout=item_layout(grid_area="dem_path_label")
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
        def set_apikey(value):
            if value["new"] is not None:
                dl.set_api_key(value["new"])
            # try:
            #     # Login success
            #     self.login.button.style.button_color = "lightgreen"
            #     self.login.button.description = "Logged in"
            # except:
            #     # Login failed
            #     self.login.button.style.button_color = "red"
            #     self.login.button.description = "Invalid API key"
        self.login.lizard_apikey_widget.observe(set_apikey, names="value")


        # --------------------------------------------------------------------------------------------------
        # 2. Select polder (and show revision)
        # --------------------------------------------------------------------------------------------------
        @self.search.search_button.on_click
        def find(action):

            self.search.search_button.style.button_color = "orange"
            self.search.search_button.description = "Searching..."
            

            self.vars.scenarios = dl.find_scenarios(
                name__icontains=self.search.sim_name_widget.value,
                model_revision=self.search.sim_rev_widget.value,
                limit=self.search.limit_widget.value,
            )
            # Reset/initialize results
            self.vars.scenario_results = {}

            # Update selection box
            self.select.dl_select_box.options = self.vars.scenario_names

            self.search.search_button.style.button_color = "lightgreen"
            self.search.search_button.description = "Search"

            
            


        # --------------------------------------------------------------------------------------------------
        # 3. SelectWidgets
        # --------------------------------------------------------------------------------------------------
        @self.select.show_0d1d_button.on_click
        def show(action):
            self.select.dl_select_box.options = [a for a in self.vars.scenario_names if "0D1D" in a]


        @self.select.show_all_button.on_click
        def show(action):
            self.select.dl_select_box.options = self.vars.scenario_names


        def on_text_change(search_input):
            self.select.dl_select_box.options = [
                a for a in self.vars.scenario_names if search_input["new"] in a
            ]
        self.select.search_results_widget.observe(on_text_change, names="value")


        def get_scenarios_selected_result(value):
            
            self.select.dl_select_label.value = self.select.dl_select_label_text.format(len(self.select.dl_select_box.value))
            self.get_scenario_results() #Get available results for selected scenarios.
            self.update_buttons()  # Change button state based on selected scenarios
            self.update_time_pick_dropdown()  # change button state and dropdown based on selected scenarios

        self.select.dl_select_box.observe(get_scenarios_selected_result, "value")



        # --------------------------------------------------------------------------------------------------
        # 4. Result layers selection
        # --------------------------------------------------------------------------------------------------
        for button in self.outputtypes.file_buttons + self.outputtypes.raster_buttons:
            button.observe(self._update_button_icon, "value")


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



        # --------------------------------------------------------------------------------------------------
        # 6. Download
        # --------------------------------------------------------------------------------------------------
        self.download.use_dem_button.observe(self._update_button_icon, "value")

        @self.download.button.on_click
        def download(action):
            """Download the selected models to the output folders"""
            
            

            self.vars.output_df = pd.DataFrame()

            #Temporary disable download button
            self.download.button.style.button_color = "orange"
            self.download.button.description = "Downloading..."
            self.download.button.disabled = True
            
            #Init empty raster settings.
            dl_raster_settings = dlRasterSettings()

            # Start download of selected files (if any are selected) ------------------------------------------------
            for scenario_id in self.scenario_selected_ids:
                scenario = self.vars.scenarios[scenario_id]
                download_urls = self.scenario_raw_download_urls[scenario_id]
                scenario_name = scenario["name"]
                # Print download URLs

                print(
                    "\n\033[1m\033[31mDownloading files for {} (uuid={}):\033[0m".format(
                        scenario_name, scenario["uuid"]
                    )
                )
                for index, url in enumerate(download_urls):
                    print("{}: {}".format(index + 1, url))

                # Print destination folder
                output_folder = str(self.vars.folder.threedi_results[self.output.subfolder_box.value][scenario_name])

                # De 3Di plugin kan geen '[' en ']' aan.
                output_folder = output_folder.replace("[", "")
                output_folder = output_folder.replace("]", "")

                print("\nThey will be placed in:\n" + output_folder + "\n")

                # Create destination folder
                if not os.path.exists(output_folder) and output_folder != "":
                    os.mkdir(output_folder)
                #             print('Created folder: ' + output_folder.rsplit('/')[-1])


                #add somee variables to overview
                self.vars.output_df = self.vars.output_df.append({
                                "id":scenario_id,
                                "name": scenario_name,
                                "uuid": scenario["uuid"],
                                "scenario_download_url": download_urls,
                                "output_folder": output_folder}, 
                            ignore_index=True)


                # Start downloading of the files
                download_functions.start_download(
                    download_urls,
                    output_folder,
                    api_key=dl.get_api_key(),
                    automatic_download=1,
                )

                # Start download of selected rasters (if any are selected) -----------------------------------------------
                time = self.outputtypes.time_pick_dropdown.value
                if time is not None:
                    time = time.replace("-", "_")
                    time = time.replace(":", "_")
                    
                res = str(self.outputtypes.resolution_dropdown.value).replace(".", "_")



                if self.download.use_dem_button.value:
                    dem = hrt.Raster(self.vars.folder.model.schema_base.rasters.dem.path)
                    class dlRasterPreset(dlRaster):
                        def __init__(self, 
                                        uuid=scenario["uuid"], 
                                        resolution=self.outputtypes.resolution_dropdown.value,
                                        bounds=dem.metadata["bounds_dl"],
                                        bounds_srs="EPSG:28992",
                                        **kwargs):
                            super().__init__(uuid=uuid, resolution=resolution, bounds=bounds, bounds_srs=bounds_srs, **kwargs)
                else:
                    class dlRasterPreset(dlRaster):
                        def __init__(self, 
                                        uuid=scenario["uuid"], 
                                        resolution=self.outputtypes.resolution_dropdown.value,
                                        **kwargs):
                            super().__init__(uuid=uuid, resolution=resolution, **kwargs)


                raster_max_wlvl = dlRasterPreset(code="s1-max-dtri",
                                        timelist=None,
                                        output_path=os.path.join(output_folder, f"max_wlvl_res{res}m.tif"), 
                                        button=self.outputtypes.max_wlvl_button,
                                        name="max waterlevel",
                )
                raster_max_depth = dlRasterPreset(code="depth-max-dtri",
                                        timelist=None,
                                        output_path=os.path.join(output_folder, f"max_depth_res{res}m.tif"), 
                                        button=self.outputtypes.max_depth_button,
                                        name="max waterdepth",
                )
                raster_total_damage = dlRasterPreset(code="total-damage",
                                        timelist=None,
                                        output_path=os.path.join(output_folder, f"total_damage_res{res}m.tif"), 
                                        button=self.outputtypes.total_damage_button,
                                        name="total damge",
                )
                raster_wlvl = dlRasterPreset(code="s1-dtri",
                                        timelist=time,
                                        output_path=os.path.join(output_folder, f"wlvl_{time}_res{res}m.tif"), 
                                        button=self.outputtypes.wlvl_button,
                                        name="waterlevel at timestep {time}",
                )
                raster_wdepth = dlRasterPreset(code="depth-dtri",
                                        timelist=time,
                                        output_path=os.path.join(output_folder, f"depth_{time}_res{res}m.tif"), 
                                        button=self.outputtypes.depth_button,
                                        name="waterdepth at timestep {time}",
                )
                raster_depth_dmg = dlRasterPreset(code="dmge-depth",
                                        timelist=None,
                                        output_path=os.path.join(output_folder, f"depth_for_lizard_dmg_res{res}m.tif"), 
                                        button=self.outputtypes.depth_damage_button,
                                        name="waterdepth for lizard damage calc",
                )


                for r in [raster_max_wlvl, raster_max_depth, raster_total_damage, raster_wlvl, raster_wdepth, raster_depth_dmg]:
                    if r.button.value == True:
                        if not os.path.exists(r.output_path):
                            dl_raster_settings.add_raster(r)
                        else:
                            print("{} already on system".format(r.output_path.split("/")[-1]))


            if len(dl_raster_settings.uuid_list)==0:
                print("\nNo rasters will be downloaded")
            else:
                print("\nStarting download of rasters")
                print(f"uuid_list: {dl_raster_settings.uuid_list}")
                print(f"code_list: {dl_raster_settings.code_list}")
                print(f"target_srs_list: {dl_raster_settings.target_srs_list}")
                print(f"resolution_list: {dl_raster_settings.resolution_list}")
                print(f"bounds_list: {dl_raster_settings.bounds_list}")
                print(f"bounds_srs_list: {dl_raster_settings.bounds_srs_list}")
                print(f"pathname_list: {dl_raster_settings.pathname_list}")
                print(f"Wait until download is finished")
                
                self.vars.dl_raster_settings=dl_raster_settings
                logging_batch_path = os.path.join(
                    output_folder,
                    "download_raster_batch_{}.csv".format(datetime.datetime.now().strftime("%Y-%m-%d %Hh%M")),
                )

                dl.download_raster(
                    scenario=dl_raster_settings.uuid_list,
                    raster_code=dl_raster_settings.code_list,
                    target_srs=dl_raster_settings.target_srs_list,
                    resolution=dl_raster_settings.resolution_list,
                    bounds=dl_raster_settings.bounds_list,
                    bounds_srs=dl_raster_settings.bounds_srs_list,
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
                name = row["name"]
                for rain_type in RAIN_TYPES:
                    for groundwater in GROUNDWATER:
                        for rain_scenario in RAIN_SCENARIOS:
                            rain_scenario = rain_scenario.strip("T")  # strip 'T' because its not used in older versions.

                            scenario_id = self.vars.scenario_names.index(name)
                            scenario = self.vars.scenarios[scenario_id]

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
                                    df.loc[index, "uuid"] = scenario["uuid"]


            df.set_index("name", inplace=True)
            # display(df)
            df.to_csv(str(
                self.vars.batch_fd.downloads.full_path("download_netcdf_batch_{}.csv".format(datetime.datetime.now().strftime("%Y-%m-%d %Hh%M")))
                )
            )

            # Get raster size of dem, max depth rasters are downloaded on this resolution.
            # print(self.vars.folder)
            # print(self.vars.folder.model.schema_base.rasters.full_path(self.download_batch.dem_path_dropdown.value))
            dem = hrt.Raster(self.vars.folder.model.schema_base.rasters.dem.path)


            #Init empty raster settings.
            dl_raster_settings = dlRasterSettings()

            # Start download of selected files (if any are selected) ------------------------------------------------
            for name, row in df.iterrows():
                scenario_id = self.vars.scenario_names.index(name)
                scenario = self.vars.scenarios[scenario_id]
                download_urls = self.scenario_raw_download_urls[scenario_id]

                print(f"\n\033[1m\033[31mDownloading files for {name} (uuid={scenario['uuid']}):\033[0m")


                #Download netcdf of all results.
                if True:
                    output_folder = getattr(self.vars.batch_fd.downloads, row["dl_name"]).netcdf

                    # Create destination folder
                    output_folder.create()

                    # Start downloading of the files
                    download_functions.start_download(
                        download_urls,
                        output_folder.path,
                        api_key=dl.get_api_key(),
                        automatic_download=1,
                    )

                # Donwload max depth and damage rasters
                class dlRasterPreset(dlRaster):
                    def __init__(self, 
                                    uuid=scenario["uuid"], 
                                    code=None, 
                                    resolution=None, 
                                    timelist=None, 
                                    output_path=None, 
                                    button=None, 
                                    name=None,
                                    bounds=dem.metadata["bounds_dl"],
                                    bounds_srs="EPSG:28992"):
                        super().__init__(uuid, code, resolution, timelist, output_path, button, name, bounds, bounds_srs)

                wlvl_max = getattr(self.vars.batch_fd.downloads, row["dl_name"]).wlvl_max
                raster_max_wlvl = dlRasterPreset(code="s1-max-dtri",
                                        resolution=dem.metadata["pixel_width"],
                                        output_path=wlvl_max.path, 
                                        button=self.outputtypes.max_wlvl_button,
                                        name="max waterlvl",                                        
                )
                depth_max = getattr(self.vars.batch_fd.downloads, row["dl_name"]).depth_max
                raster_max_depth = dlRasterPreset(code="depth-max-dtri",
                                        resolution=dem.metadata["pixel_width"],
                                        output_path=depth_max.path, 
                                        button=self.outputtypes.max_depth_button,
                                        name="max waterdepth",
                )
                damage_total = getattr(self.vars.batch_fd.downloads, row["dl_name"]).damage_total
                raster_total_damage = dlRasterPreset(code="total-damage",
                                        resolution=0.5, #FIXME 0.5 res
                                        output_path=damage_total.path, 
                                        button=self.outputtypes.total_damage_button,
                                        name="total damge",
                )
                

                for r in [raster_max_wlvl, raster_max_depth, raster_total_damage]:
                    if r.button.value == True:
                        if not os.path.exists(r.output_path):
                            dl_raster_settings.add_raster(r)

            #To vars so we can inspect.
            self.vars.dl_raster_settings = dl_raster_settings

            if len(dl_raster_settings.uuid_list)==0:
                print("\nNo rasters will be downloaded")
            else:
                print("\nStarting download of rasters")
                print(f"uuid_list: {dl_raster_settings.uuid_list}")
                print(f"code_list: {dl_raster_settings.code_list}")
                print(f"target_srs_list: {dl_raster_settings.target_srs_list}")
                print(f"resolution_list: {dl_raster_settings.resolution_list}")
                print(f"bounds_list: {dl_raster_settings.bounds_list}")
                print(f"bounds_srs_list: {dl_raster_settings.bounds_srs_list}")
                print(f"pathname_list: {dl_raster_settings.pathname_list}")
                print(f"Wait until download is finished")
                

                logging_batch_path = self.vars.batch_fd.downloads.full_path(
                    "download_raster_batch_{}.csv".format(datetime.datetime.now().strftime("%Y-%m-%d %Hh%M")),
                )

                dl.download_raster(
                    scenario=dl_raster_settings.uuid_list,
                    raster_code=dl_raster_settings.code_list,
                    target_srs=dl_raster_settings.target_srs_list,
                    resolution=dl_raster_settings.resolution_list,
                    bounds=dl_raster_settings.bounds_list,
                    bounds_srs=dl_raster_settings.bounds_srs_list,
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
        self.output.folder_value.value = self.vars.folder.threedi_results.path


    # --------------------------------------------------------------------------------------------------
    # 3. SelectWidgets
    # --------------------------------------------------------------------------------------------------
    def get_scenario_results(self):

        #TODO zou in threedi_scenario_downloader moeten staan.
        def find_scenario_results(scenario_url):
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

            r = find_scenario_results(scenario_url=scenario['url'])
            #Loop individual results and add to dict
            for result in r:
                self.vars.scenario_results[scenario_id][result["code"]]=result

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
                time_pick_options = [(date.strftime("%Y-%m-%dT%H:%M:%S")) for date in dates]
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


        def change_dowloadbutton_state():
            """Change color and disabled for download button."""
            if any([a.value for a in self.outputtypes.file_buttons + self.outputtypes.raster_buttons]):
                self.download.button.disabled = False
                self.download.button.style.button_color = "lightgreen"
            else:
                self.download.button.disabled = True
                self.download.button.style.button_color = "red"

        # Depening on the button values, change the download button color
        change_dowloadbutton_state()  


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
            selected_download_old = selected_download["old"]
        except:
            selected_download_old = ""
            selected_download_new = selected_download

        # Remove the previous selected records from the list
        #         output_select_box.options = [x for x in output_select_box.options if x not in selected_download_old]
        self.output.output_select_box.options = (
            self.vars.folder.threedi_results[self.output.subfolder_box.value].revisions
        )

        # Batch folder gets only batch_folder options
        self.download_batch.batch_folder_dropdown.options = (
            self.vars.folder.threedi_results["batch_results"].revisions
        )

        # Add the newly selected records to the list
        for new_selected in selected_download_new:
            if new_selected not in self.output.output_select_box.options:
                self.output.output_select_box.options = self.output.output_select_box.options + (new_selected,)

        # Select these new records.
        self.output.output_select_box.value = selected_download_new

        # Set Dem path for batch
        # if scenarios["folder"].model.rasters.find_dem() == "":
        #     dem_path_dropdown.options = [
        #         i.split(os.sep)[-1]
        #         for i in scenarios["folder"].model.rasters.find_ext("tif")
        #     ]
        # else:
        self.download_batch.dem_path_dropdown.options = [
            self.vars.folder.model.schema_base.rasters.dem.path
        ]


    def update_api_keys(self, api_keys_path):
        self.vars.api_keys = htt.read_api_file(api_keys_path)
        self.login.lizard_apikey_widget.value=self.vars.api_keys["lizard"]
        # self.login.threedi_apikey_widget.value=self.vars.api_keys["threedi"]
        # self.login.button.click()

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
        return  [self.vars.scenario_names.index(a) for a in self.select.dl_select_box.value]  # id's of selected models to download

    @property
    def scenario_raw_download_urls(self):
        """Retrieve urls from scenario raw results of selected results"""
        scenario_download_urls = {}
        for scenario_id in self.scenario_selected_ids:
            scenario_result = self.vars.scenario_results[scenario_id]
            download_urls = []

            for code in ["results-3di", "grid-admin", "logfiles", "aggregate-results-3di"]:
                if self.button_codes[code].value:
                    download_urls.append(scenario_result[code]["attachment_url"])
            scenario_download_urls[scenario_id] = download_urls
        return scenario_download_urls

    @property
    def vars(self):
        return self.caller.vars


class GuiVariables:
    def __init__(self) -> None:
        self._folder = None

        self.scenarios = None #filled when clicking search
        self.scenario_results = None #Filled when selecting a scenario, reset when clicking search
        self.dl_raster_settings = None #filled when clicking download

        self.api_keys = {"lizard":"", "threedi":""}
        self.scenarios = {}

    @property
    def folder(self):
        return self._folder

    @folder.setter
    def main_folder(self, main_folder):
        self._folder = Folders(main_folder, create=False)


    @property
    def scenario_names(self):
        return [a["name"] for a in self.scenarios]


    @property
    def time_now(self):
        return datetime.datetime.now().strftime("%H:%M:%S")



class DownloadGui:
    def __init__(
        self, data=None, base_scenario_name=None, 
        lizard_api_key=None, main_folder=None, 
    ):

        self.vars = GuiVariables()
        self.widgets = DownloadWidgetsInteraction(self)

        if data:
            self.widgets.update_api_keys(api_keys_path=data["api_keys_path"])
            self.vars.main_folder = data["polder_folder"]
        else:
            self.vars.api_keys["lizard"] = lizard_api_key
            self.vars.main_folder = main_folder

        self.widgets.update_folder()
        self.widgets.update_buttons() #disable filetype buttons
        self.widgets.download.use_dem_button.value = True
        self.widgets.output.subfolder_box.value = "1d2d_results"
        self.widgets.search.sim_name_widget.value = ""  #hopefully prevents cursor from going to api key field.


        if not self.vars.main_folder:
            self.vars.main_folder = os.getcwd()


        # self.scheduler = BlockingScheduler(timezone="Europe/Amsterdam")



        self.download_tab = widgets.GridBox(
            children=[
                self.w.login.label,
                self.w.login.lizard_apikey_widget,
                self.w.login.get_api_key_widget,
                self.w.search.label,
                self.w.search.sim_name_label,
                self.w.search.sim_name_widget,
                self.w.search.sim_rev_label,
                self.w.search.sim_rev_widget,
                self.w.search.limit_label,
                self.w.search.limit_widget,
                self.w.search.search_button,
                self.w.select.dl_select_label,
                self.w.select.dl_select_box,
                self.w.select.show_0d1d_button,
                self.w.select.show_all_button,
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
                self.w.download.use_dem_label,
                self.w.download.use_dem_button,
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
                    'lizard_apikey_widget lizard_apikey_widget sim_name_label sim_name_widget sim_search_button . . .'
                    'get_api_key_widget get_api_key_widget sim_rev_label sim_rev_widget sim_search_button . . .'
                    '. . search_limit_label search_limit_widget sim_search_button . . .'
                    'dl_select_label dl_select_label . outputtypes_label outputtypes_label output_label output_label output_label'
                    'dl_select_box dl_select_box dl_select_box file_buttons_label file_buttons_label output_folder_label output_folder_value output_folder_value'
                    'dl_select_box dl_select_box dl_select_box file_buttons_box file_buttons_box output_subfolder_label output_subfolder_box output_subfolder_box'
                    'dl_select_box dl_select_box dl_select_box file_buttons_box file_buttons_box output_select_box output_select_box output_select_box'
                    'dl_select_box dl_select_box dl_select_box raster_buttons_label raster_buttons_label output_select_box output_select_box output_select_box'
                    'dl_select_box dl_select_box dl_select_box raster_buttons_box raster_buttons_box output_select_box output_select_box output_select_box'
                    'search_results button_0d1d all_button raster_buttons_box raster_buttons_box  output_select_box output_select_box output_select_box'
                    '. . use_dem_label time_resolution_box time_resolution_box time_resolution_box download_button_label download_button_label'
                    '. . use_dem_button time_resolution_box time_resolution_box time_resolution_box download_button download_button'
                    '. . . . . . download_batch_button_label download_batch_button_label'
                    '. . dem_path_label dem_path_label batch_folder_label batch_folder_label download_batch_button download_batch_button'
                    '. . dem_path_dropdown dem_path_dropdown batch_folder_dropdown batch_folder_dropdown download_batch_button download_batch_button'
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
        data = {'polder_folder': 'E:\\02.modellen\\model_test_v2',
    'api_keys_path': 'C:\\Users\\wvangerwen\\AppData\\Roaming\\3Di\\QGIS3\\profiles\\default\\python\\plugins\\hhnk_threedi_plugin\\api_key.txt'}
        self = DownloadGui(data=data); 
        display(self.tab)
 