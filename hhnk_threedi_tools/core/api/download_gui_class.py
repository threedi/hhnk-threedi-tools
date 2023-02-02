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
from threedi_scenario_downloader import downloader as dl
import hhnk_threedi_tools.core.api.download_functions as download_functions

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
THREEDI_API_HOST = "https://api.3di.live/v3"
RESULT_LIMIT = 10


def item_layout(width="95%", grid_area="", **kwargs):
    return widgets.Layout(
        width=width, grid_area=grid_area, **kwargs
    )  # override the default width of the button to 'auto' to let the button grow




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

            self.dl_select_label = widgets.HTML(
                "<b>3. Select simulation result(s)</b>",
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
                "Locatie DEM (voor batch):", layout=item_layout(grid_area="dem_path_label")
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
            

            self.vars.scenario_results = dl.find_scenarios(
                model_name=self.search.sim_name_widget.value,
                model_revision=self.search.sim_rev_widget.value,
                limit=self.search.limit_widget.value,
            )

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
        @self.download.button.on_click
        def download(action):
            """Download the selected models to the output folders"""
            selected_file_results = []  # list of selected files from the model to download
            if self.outputtypes.netcdf_button.value:
                selected_file_results += ["Raw 3Di output"]
            if self.outputtypes.agg_netcdf_button.value:
                selected_file_results += ["aggregated 3Di output"]
            if self.outputtypes.h5_button.value:
                selected_file_results += ["Grid administration"]
            if self.outputtypes.log_button.value:
                selected_file_results += ["Calculation core logging"]

            self.vars.scenario_download_url = download_functions.create_download_url(
                self.vars.scenario_results, self.scenario_selected_ids, selected_file_results
            )  # url to all download links.

            # Log in to 3Di server
            #     headers, username, passw, headers_results = retrieve_username_pw(username_widget.value, password_widget.value)

            uuid_list = []
            code_list = []
            target_srs_list = []
            resolution_list = []
            time_list = []
            pathname_list = []



            self.vars.output_df = pd.DataFrame()

            #Temporary disable download button
            self.download.button.style.button_color = "orange"
            self.download.button.description = "Downloading..."
            self.download.button.disabled = True
            

            # Start download of selected files (if any are selected) ------------------------------------------------
            for name in self.select.dl_select_box.value:
                scenario_id = self.vars.scenario_names.index(name)
                selected_result = self.vars.scenario_results[scenario_id]
                # Print download URLs

                print(
                    "\n\033[1m\033[31mDownloading files for {} (uuid={}):\033[0m".format(
                        name, selected_result["uuid"]
                    )
                )
                for index, url in enumerate(self.vars.scenario_download_url[name]):
                    print("{}: {}".format(index + 1, url))

                # Print destination folder
                output_folder = str(self.vars.folder.threedi_results[self.output.subfolder_box.value][name])

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
                                "name": name,
                                "uuid": selected_result["uuid"],
                                "scenario_download_url": self.vars.scenario_download_url[name],
                                "output_folder": output_folder}, 
                            ignore_index=True)


                # Start downloading of the files
                download_functions.start_download(
                    self.vars.scenario_download_url[name],
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

                # Output files
                logging_batch_path = os.path.join(
                    output_folder,
                    "download_raster_batch_{}.csv".format(datetime.datetime.now().strftime("%Y-%m-%d %Hh%M")),
                )

                class dlRaster():
                    def __init__(self, output_folder, filename, name, button, code, timelist):
                        self.name = name
                        self.output_path = os.path.join(output_folder, filename)
                        self.button = button
                        self.code = code
                        self.timelist = timelist

                raster_max_wlvl = dlRaster(output_folder=output_folder, 
                                        filename=f"max_wlvl_res{res}m.tif", 
                                        name="max waterlevel",
                                        button=self.outputtypes.max_wlvl_button,
                                        code="s1-max-dtri",
                                        timelist=None,
                )
                raster_max_depth = dlRaster(output_folder=output_folder, 
                                        filename=f"max_depth_res{res}m.tif", 
                                        name="max waterdepth",
                                        button=self.outputtypes.max_depth_button,
                                        code="depth-max-dtri",
                                        timelist=None,
                )
                raster_total_damage = dlRaster(output_folder=output_folder, 
                                        filename=f"total_damage_res{res}m.tif", 
                                        name="total damge",
                                        button=self.outputtypes.total_damage_button,
                                        code="total-damage",
                                        timelist=None,
                )
                raster_wlvl = dlRaster(output_folder=output_folder, 
                                        filename= f"wlvl_{time}_res{res}m.tif", 
                                        name=f"waterlevel at timestep {time}",
                                        button=self.outputtypes.wlvl_button,
                                        code="s1-dtri",
                                        timelist=time,
                )
                raster_wdepth = dlRaster(output_folder=output_folder, 
                                        filename= f"depth_{time}_res{res}m.tif", 
                                        name=f"waterdepth at timestep {time}",
                                        button=self.outputtypes.depth_button,
                                        code="depth-dtri",
                                        timelist=time,
                )
                raster_depth_dmg = dlRaster(output_folder=output_folder, 
                                        filename= f"depth_for_lizard_dmg_res{res}m.tif", 
                                        name=f"waterdepth for lizard damage calc",
                                        button=self.outputtypes.depth_damage_button,
                                        code="dmge-depth",
                                        timelist=None,
                )

                for r in [raster_max_wlvl, raster_max_depth, raster_total_damage, raster_wlvl, raster_wdepth, raster_depth_dmg]:
                    if r.button.value == True:
                        if not os.path.exists(r.output_path):
                            # print(f"Preparing download of raster {r.name}")
                            # dl.download_maximum_waterlevel_raster(selected_result['uuid'],"EPSG:28992",resolution_dropdown.value, pathname=max_wlvl_path)
                            uuid_list.append(selected_result["uuid"])
                            code_list.append(r.code)
                            target_srs_list.append("EPSG:28992")
                            resolution_list.append(self.outputtypes.resolution_dropdown.value)
                            time_list.append(r.timelist)
                            pathname_list.append(r.output_path)
                        else:
                            print("{} already on system".format(r.output_path.split("/")[-1]))



            print("\nStarting download of rasters")
            print("uuid_list: {}".format(uuid_list))
            print("code_list: {}".format(code_list))
            print("target_srs_list: {}".format(target_srs_list))
            print("resolution_list: {}".format(resolution_list))
            print("time_list: {}".format(time_list))
            print("pathname_list: {}".format(pathname_list))

            dl.download_raster(
                uuid_list,
                raster_code=code_list,
                target_srs=target_srs_list,
                resolution=resolution_list,
                time=time_list,
                pathname=pathname_list,
                export_task_csv=logging_batch_path,
            )

            #Re enable download button
            self.download.button.style.button_color = "lightgreen"
            self.download.button.description = "Download"
            self.download.button.disabled = False
            print("Download of rasters finished")



    def update_folder(self):
        """when main folder changes, we update some values"""

        #Output folder string
        self.output.folder_value.value = self.vars.folder.threedi_results.path


    # --------------------------------------------------------------------------------------------------
    # 4. Result layers selection
    # --------------------------------------------------------------------------------------------------
    def update_time_pick_dropdown(self):
        """Update options with time intervals.
        If there are multiple results, all time series will be analyzed. If these are not the same,
        selecting these output rasters is disabled."""

        def retrieve_time_interval(selected_result):
            """retrieve selected time"""
            Tstart = datetime.datetime.strptime(
                selected_result["start_time_sim"], "%Y-%m-%dT%H:%M:%SZ"
            )
            Tend = datetime.datetime.strptime(selected_result["end_time_sim"], "%Y-%m-%dT%H:%M:%SZ")

            dates = pd.date_range(Tstart, Tend, freq="H")
            time_pick_options = [(date.strftime("%Y-%m-%dT%H:%M:%S")) for date in dates]
            return time_pick_options

        time_pick_options = []
        for scenario_id in self.scenario_selected_ids:
            time_pick_options.append(
                retrieve_time_interval(self.vars.scenario_results[scenario_id])
            )

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
            result_codes += [
                rset["result_type"]["code"]
                for rset in self.vars.scenario_results[scenario_id]["result_set"]
            ]  # available results

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
        for button in [self.outputtypes.netcdf_button, self.outputtypes.h5_button, self.outputtypes.log_button]:
            if "results-3di" in result_codes:
                self.change_button_state(button, button_disabled=False, button_value=True)
            else:
                self.change_button_state(button, button_disabled=True, button_value=False)

        if "aggregate-results-3di" in result_codes:
            self.change_button_state(
                self.outputtypes.agg_netcdf_button, button_disabled=False, button_value=True
            )
        else:
            self.change_button_state(
                self.outputtypes.agg_netcdf_button, button_disabled=True, button_value=False
            )

        for button in [self.outputtypes.max_wlvl_button, 
                        self.outputtypes.max_depth_button, 
                        self.outputtypes.wlvl_button, 
                        self.outputtypes.depth_button]:
            if "ucr-max-quad" in result_codes:
                self.change_button_state(button, button_disabled=False, button_value=False)
            else:
                self.change_button_state(button, button_disabled=True, button_value=False)

        for button in [self.outputtypes.total_damage_button,
                        self.outputtypes.depth_damage_button]:
            if "total-damage" in result_codes:
                self.change_button_state(button, button_disabled=False, button_value=False)
            else:
                self.change_button_state(button, button_disabled=True, button_value=False)


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

        # Batch folder gets the same options.
        self.download_batch.batch_folder_dropdown.options = (
            self.vars.folder.threedi_results[self.output.subfolder_box.value].revisions
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
    def scenario_selected_ids(self):
        return  [self.vars.scenario_names.index(a) for a in self.select.dl_select_box.value]  # id's of selected models to download

    @property
    def vars(self):
        return self.caller.vars


class GuiVariables:
    def __init__(self) -> None:
        self._folder = None

        self.scenario_results = None #filled when clicking search

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
        return [a["name"] for a in self.scenario_results]


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
        self.widgets.output.subfolder_box.value = "1d2d_results"
        self.widgets.search.sim_name_widget.value = ""  #hopefully prevents cursor from going to api key field.


        if not self.vars.main_folder:
            self.vars.main_folder = os.getcwd()


        self.scheduler = BlockingScheduler(timezone="Europe/Amsterdam")



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
                    '. . . time_resolution_box time_resolution_box time_resolution_box download_button_label download_button_label'
                    '. . . time_resolution_box time_resolution_box time_resolution_box download_button download_button'
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
        data = {'polder_folder': 'E:\\02.modellen\\23_Katvoed',
    'api_keys_path': 'C:\\Users\\wvangerwen\\AppData\\Roaming\\3Di\\QGIS3\\profiles\\default\\python\\plugins\\hhnk_threedi_plugin\\api_key.txt'}
        self = DownloadGui(data=data); 
        display(self.tab)
# %%
