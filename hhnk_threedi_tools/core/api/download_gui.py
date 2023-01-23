# %%
# System imports
import os
import re


# Third-party imports
import pandas as pd
import ipywidgets as widgets
from datetime import datetime
from threedi_scenario_downloader import downloader as dl

# local imports
from hhnk_threedi_tools import Folders
from hhnk_threedi_tools.core.api.download_functions import (
    create_download_url,
    start_download,
)
from hhnk_threedi_tools.core.api.read_api_file import read_api_file

from hhnk_threedi_tools.variables.api_settings import (
    RAIN_TYPES,
    GROUNDWATER,
    RAIN_SCENARIOS,
    RAW_DOWNLOADS,
)

import hhnk_research_tools as hrt

# from functions_nabewerking.create_batch_folders_dict import create_batch_folders_dict


# %%


def download_gui(main_folder=None, lizard_api_key="", data=None):
    dl.LIZARD_URL = "https://hhnk.lizard.net/api/v3/"

    if data:
        main_folder = data["polder_folder"]
        api_keys = read_api_file(data["api_keys_path"])
    else:
        api_keys = {}
        api_keys["lizard"] = lizard_api_key

    def new_get_api_key():
        return api_key_widget.value

    setattr(dl, "get_api_key", new_get_api_key)

    def item_layout(width="95%", grid_area="", **kwargs):
        return widgets.Layout(
            width=width, grid_area=grid_area, **kwargs
        )  # override the default width of the button to 'auto' to let the button grow

    # def item_layout(width='95%', grid_area='',**kwargs):
    #     return widgets.Layout(**kwargs) # override the default width of the button to 'auto' to let the button grow

    ###################################################################################################
    # Login and model selection
    ###################################################################################################
    # initialize functions and dictorionaries that can be called
    scenarios = {}  # This dict is filled certain buttons are pressed
    scenarios["names"] = []  # Names of models that can be downloaded
    scenarios["selected_folder"] = ""  # 03 hyd toets or 05extreme data
    scenarios["folders_dict"] = {}
    scenarios["selected_ids"] = []  # Selected models for downloading
    scenarios["folder"] = []

    if not main_folder:
        main_folder = os.getcwd()

    # Fetch the first folder
    scenarios["folder"] = Folders(main_folder, create=False)

    def update_folders_dict(polder_name):
        # folder = Folders(os.path.join(main_folder, polder_name), create=False)
        output_polder_dropdown.value = polder_name
        # scenarios["folder"] = folder
        # old
        # scenarios['folders_dict'] = Folders(polder_name)
        # output_polder_dropdown.value=scenarios['folders_dict']['polder']['full_naam']

    # --------------------------------------------------------------------------------------------------
    # 1. Login with 3Di account
    # --------------------------------------------------------------------------------------------------
    login_label = widgets.HTML(
        "<b>1. Login with 3Di account</b>", layout=item_layout(grid_area="login_label")
    )

    # API key widget
    api_key_widget = widgets.Text(
        description="API key:", layout=item_layout(width="261px", grid_area="api_key")
    )

    get_api_key_widget = widgets.HTML(
        'Get key <a href=https://hhnk.lizard.net/management/#/personal_api_keys target target="_blank">here</a>',
        layout=item_layout(grid_area="get_api_key"),
    )

    # --------------------------------------------------------------------------------------------------
    # 2. Select polder (and show revision)
    # --------------------------------------------------------------------------------------------------
    select_polder_label = widgets.HTML(
        "<b>2. Select polder and revision</b>",
        layout=item_layout(grid_area="select_polder_label"),
    )

    # Polder name widget
    polder_name_label = widgets.Label(
        "Polder name:", layout=item_layout(grid_area="polder_name_label")
    )
    polder_name_widget = widgets.Text(
        layout=item_layout(grid_area="polder_name_widget")
    )

    # Polder revision widget
    model_revision_label = widgets.Label(
        "Model revision:", layout=item_layout(grid_area="polder_rev_label")
    )
    model_revision_widget = widgets.Text(
        layout=item_layout(grid_area="polder_rev_widget")
    )

    # Connect to results API and create a list of available results for the selected model.
    find_results_button = widgets.Button(
        description="Find results",
        layout=item_layout(height="100%", grid_area="find_results_button"),
    )

    # --------------------------------------------------------------------------------------------------
    # 3. selection box with names of model results
    # --------------------------------------------------------------------------------------------------
    download_selection_label = widgets.HTML(
        "<b>3. Select model result(s)</b>",
        layout=item_layout(grid_area="download_selection_label"),
    )

    # selection box with names of model results
    download_selection_box = widgets.SelectMultiple(
        rows=20, layout=item_layout(grid_area="dl_sel_box")
    )

    # Subset the list in the download selection box
    show_0d1d_button = widgets.Button(
        description="Show 0d1d",
        layout=item_layout(grid_area="button_0d1d", justify_self="end"),
    )

    @show_0d1d_button.on_click
    def show(action):
        download_selection_box.options = [a for a in scenarios["names"] if "0D1D" in a]

    # Subset the list in the download selection box
    show_all_button = widgets.Button(
        description="Show all", layout=item_layout(grid_area="all_button")
    )

    @show_all_button.on_click
    def show(action):
        download_selection_box.options = scenarios["names"]

    # Create search box
    search_results_widget = widgets.Text(layout=item_layout(grid_area="search_results"))

    def on_text_change(search_input):
        download_selection_box.options = [
            a for a in scenarios["names"] if search_input["new"] in a
        ]

    search_results_widget.observe(on_text_change, names="value")

    # --------------------------------------------------------------------------------------------------
    # 4. Result layers selection
    # --------------------------------------------------------------------------------------------------
    # selection box with possible results (i.e. .nc file, .h5 file, log file, agg file)
    download_files_label = widgets.HTML(
        "<b>4. Select files (dark=selected)</b>",
        layout=item_layout(grid_area="dl_files_label"),
    )
    # download_selection_file_box = widgets.SelectMultiple(rows=5, disabled=False,
    #     layout=item_layout(grid_area='dl_sel_file_box'))

    # download_selection_file_box.options = ['raw 3Di output', 'grid administration', 'calculation core logging', 'aggregated 3Di output']

    # download_selection_file_box.options = ['raw 3Di output', 'grid administration', 'calculation core logging', 'aggregated 3Di output']

    # download_selection_file_box.value = download_selection_file_box.options

    # file result buttons ----------------------------------------------------------------------------
    file_buttons_label = widgets.HTML(
        "File results", layout=item_layout(grid_area="file_buttons_label")
    )

    netcdf_button = widgets.ToggleButton(
        value=True,
        description="raw 3Di output (.nc)",
        layout=item_layout(grid_area="netcdf_button"),
    )
    agg_netcdf_button = widgets.ToggleButton(
        value=True,
        description="aggregated 3Di output (.nc)",
        layout=item_layout(grid_area="agg_netcdf_button"),
    )
    h5_button = widgets.ToggleButton(
        value=True,
        description="grid administration (.h5)",
        layout=item_layout(grid_area="h5_button"),
    )
    log_button = widgets.ToggleButton(
        value=True,
        description="calculation core logging (.txt)",
        layout=item_layout(grid_area="log_button"),
    )

    # Stack the buttons in a vbox for easier plotting

    file_buttons = (netcdf_button, agg_netcdf_button, h5_button, log_button)
    file_buttons_box = widgets.VBox(
        file_buttons, layout=item_layout(grid_area="file_buttons_box")
    )

    # Raster buttons ----------------------------------------------------------------------------------
    raster_buttons_label = widgets.HTML(
        "Raster results", layout=item_layout(grid_area="raster_buttons_label")
    )

    # button to select which raster to download
    wlvl_button = widgets.ToggleButton(
        value=True,
        description="Water level at selected time",
        layout=item_layout(grid_area="wlvl_button"),
    )
    depth_button = widgets.ToggleButton(
        value=True,
        description="Water depth at selected time",
        layout=item_layout(grid_area="depth_button"),
    )
    max_wlvl_button = widgets.ToggleButton(
        value=True,
        description="Max water level",
        layout=item_layout(grid_area="max_wlvl_button"),
    )
    max_depth_button = widgets.ToggleButton(
        value=True,
        description="Max water depth",
        layout=item_layout(grid_area="max_depth_button"),
    )
    total_damage_button = widgets.ToggleButton(
        value=True,
        description="Total damage",
        layout=item_layout(grid_area="total_damage_button"),
    )

    # Stack the buttons in a vbox for easier plotting
    raster_buttons = (
        max_wlvl_button,
        max_depth_button,
        total_damage_button,
        wlvl_button,
        depth_button,
    )
    raster_buttons_box = widgets.VBox(
        raster_buttons, layout=item_layout(grid_area="raster_buttons_box")
    )

    # dropdown to pick timepoint to download raster  ---------------------------------------------------
    time_pick_label = widgets.Label(
        "Timestep raster:", layout=item_layout(grid_area="time_pick_label")
    )
    time_pick_dropdown = widgets.Dropdown(
        layout=item_layout(grid_area="time_pick_dropdown", description_width="initial")
    )

    # dropdown to pick resolution ----------------------------------------------------------------------
    resolution_options = [0.5, 1, 2, 5, 10, 25]
    resolution_label = widgets.Label(
        "Resolution [m]:", layout=item_layout(grid_area="resolution_label")
    )
    resolution_dropdown = widgets.Dropdown(
        options=resolution_options,
        value=10,
        layout=item_layout(
            grid_area="resolution_dropdown", description_width="initial"
        ),
    )

    # Combine time pick and resolution in one box for GUI.
    time_resolution_box = widgets.HBox(
        [
            widgets.VBox([time_pick_label, time_pick_dropdown], layout=item_layout()),
            widgets.VBox([resolution_label, resolution_dropdown], layout=item_layout()),
        ],
        layout=item_layout(grid_area="time_resolution_box"),
    )

    # --------------------------------------------------------------------------------------------------
    # 5. destination folder creation/selection
    # --------------------------------------------------------------------------------------------------
    output_label = widgets.HTML(
        "<b>5. Select output folder</b>", layout=item_layout(grid_area="output_label")
    )

    output_polder_dropdown = widgets.Dropdown(
        options=os.listdir(main_folder),
        description="Output folder:",
        layout=item_layout(grid_area="output_polder_dropdown"),
    )

    # Selection box of the folder the output should be put in. (Hyd toets or Extreme)
    output_folder_options = ["0d1d_results", "1d2d_results", "batch_results"]
    # output_folder_label = widgets.Label('Selecteer output folder:', layout=item_layout(grid_area='output_folder_label'))
    output_folder_box = widgets.Select(
        options=output_folder_options,
        rows=3,
        disabled=False,
        layout=item_layout(grid_area="output_folder_box"),
    )

    # selection box with files in ftp input folder
    output_select_label = widgets.Label(
        "Lijst van de ftp input folder:",
        layout=item_layout(grid_area="output_select_label"),
    )
    output_select_box = widgets.SelectMultiple(
        rows=13,
        description="",
        disabled=True,
        layout=item_layout(height="95%", grid_area="output_select_box"),
    )

    # --------------------------------------------------------------------------------------------------
    # 6. Download
    # --------------------------------------------------------------------------------------------------
    # Download results button
    download_button_label = widgets.HTML(
        "<b>6. Download selected</b>",
        layout=item_layout(grid_area="download_button_label"),
    )
    download_button = widgets.Button(
        description="Download",
        layout=item_layout(
            height="50px", grid_area="download_button", align_self="flex-end"
        ),
    )

    # --------------------------------------------------------------------------------------------------
    # 7. Download batch
    # --------------------------------------------------------------------------------------------------
    # Download results button
    download_batch_button_label = widgets.HTML(
        "<b>7. Download klimaatsommen</b>",
        layout=item_layout(grid_area="download_batch_button_label"),
    )
    download_batch_button = widgets.Button(
        description="Download batch",
        layout=item_layout(
            height="50px", grid_area="download_batch_button", align_self="flex-end"
        ),
    )
    download_batch_button.style.button_color = "lightgreen"

    # Select folder to download batch to
    batch_folder_label = widgets.Label(
        "Naam van batch folder (maak aan als niet bestaat!):",
        layout=item_layout(grid_area="batch_folder_label"),
    )
    batch_folder_dropdown = widgets.Dropdown(
        options="",
        disabled=False,
        layout=item_layout(grid_area="batch_folder_dropdown"),
    )

    # Select DEM file to use to determine which resolution to download depth rasters on
    dem_path_label = widgets.Label(
        "Locatie DEM (voor batch):", layout=item_layout(grid_area="dem_path_label")
    )
    dem_path_dropdown = widgets.Dropdown(
        options="",
        disabled=False,
        layout=item_layout(grid_area="dem_path_dropdown"),
    )

    ###################################################################################################
    # Updating and interaction of the widgets
    ###################################################################################################
    # --------------------------------------------------------------------------------------------------
    # 2. Select polder (and show revision)
    # --------------------------------------------------------------------------------------------------
    @find_results_button.on_click
    def find(action):
        try:
            update_folders_dict(polder_name_widget.value)
            output_polder_dropdown.value = scenarios["folder"].name
        except:
            print("Kan opgegeven naam niet koppelen aan een output folder")

        # Connect to API
        #     headers, username, passw, headers_results = retrieve_username_pw(username_widget.value, password_widget.value)
        #     scenarios['results'] = search_model_slug(base_url, headers_results,
        #                                                 polder_name_widget.value, model_revision_widget.value)

        scenarios["results"] = dl.find_scenarios(
            model_name=polder_name_widget.value,
            model_revision=model_revision_widget.value,
            limit=1000,
        )

        # Update selection box
        scenarios["names"] = [a["name"] for a in scenarios["results"]]
        download_selection_box.options = scenarios["names"]

    # --------------------------------------------------------------------------------------------------
    # 3. selection box with names of model results
    # --------------------------------------------------------------------------------------------------

    def update_displays(selected_folder=""):
        """Refresh client side display boxes of files in folders"""
        try:
            selected_folder = selected_folder["new"]
        except:
            pass
        if (selected_folder in "1d2d_results") and selected_folder != "":
            folder = "1d2d_results"
        elif selected_folder in "batch_results":
            folder = "batch_results"
        else:
            folder = "0d1d_results"
        scenarios["selected_folder"] = folder

        output_select_box.options = (
            scenarios["folder"].threedi_results[folder].revisions
        )

        update_output_selectbox(download_selection_box.value)

    # --------------------------------------------------------------------------------------------------
    # 4. Result layers selection
    # --------------------------------------------------------------------------------------------------
    def update_button_icon(value):
        """Add icons to buttons based on their state"""
        try:
            button = value[
                "owner"
            ]  # change the icon of the smae button that was observed.
        except:
            button = value  # if function is not called with observe

        if button.disabled == True:
            button.icon = "minus"  # https://fontawesome.com/icons?d=gallery
        else:
            button.icon = (
                "plus"  # When button is not selected but available remove icon.
            )
            if button.value == True:
                button.icon = "check"  # https://fontawesome.com/icons?d=gallery

        change_dowloadbutton_state()  # Depening on the button values, change the download button color

    def change_button_state(button, button_disabled=False, button_value=False):
        """Disable buttons based on input"""
        button.disabled = button_disabled  # Disable button based on input
        button.value = button_value
        update_button_icon(button)

    def update_buttons():
        """Update the buttons with possible results to download, enabling or disabling them based
        on their occrence in the results"""
        result_codes = []
        for index, scenario_id in enumerate(scenarios["selected_ids"]):
            result_codes += [
                rset["result_type"]["code"]
                for rset in scenarios["results"][scenario_id]["result_set"]
            ]  # available results

        # select results that appear in all selected scenarios. If for instance rasters are not in one of the results
        # all rasters are dropped from the list.
        is_result_in_all_selected = [
            nr_items == len(scenarios["selected_ids"])
            for nr_items in [result_codes.count(x) for x in set(result_codes)]
        ]
        result_codes = [
            x for x, y in zip(set(result_codes), is_result_in_all_selected) if y == True
        ]

        # we now know which results are available for all selected models. Only these buttons will be available for download.
        # Enable or disable buttons based on their availability in the results
        for button in [netcdf_button, h5_button, log_button]:
            if "results-3di" in result_codes:
                change_button_state(button, button_disabled=False, button_value=True)
            else:
                change_button_state(button, button_disabled=True, button_value=False)

        if "aggregate-results-3di" in result_codes:
            change_button_state(
                agg_netcdf_button, button_disabled=False, button_value=True
            )
        else:
            change_button_state(
                agg_netcdf_button, button_disabled=True, button_value=False
            )

        for button in [max_wlvl_button, max_depth_button, wlvl_button, depth_button]:
            if "ucr-max-quad" in result_codes:
                change_button_state(button, button_disabled=False, button_value=False)
            else:
                change_button_state(button, button_disabled=True, button_value=False)

        for button in [total_damage_button]:
            if "total-damage" in result_codes:
                change_button_state(button, button_disabled=False, button_value=False)
            else:
                change_button_state(button, button_disabled=True, button_value=False)

    def retrieve_time_interval(selected_result):
        """retrieve selected time"""
        Tstart = datetime.strptime(
            selected_result["start_time_sim"], "%Y-%m-%dT%H:%M:%SZ"
        )
        Tend = datetime.strptime(selected_result["end_time_sim"], "%Y-%m-%dT%H:%M:%SZ")

        dates = pd.date_range(Tstart, Tend, freq="H")
        time_pick_options = [(date.strftime("%Y-%m-%dT%H:%M:%S")) for date in dates]
        return time_pick_options

    def update_time_pick_dropdown():
        """Update options with time intervals.
        If there are multiple results, all time series will be analyzed. If these are not the same,
        selecting these output rasters is disabled."""
        time_pick_options = []
        for scenario_id in scenarios["selected_ids"]:
            time_pick_options.append(
                retrieve_time_interval(scenarios["results"][scenario_id])
            )

        if not all(
            [len(x) == len(time_pick_options[0]) for x in time_pick_options]
        ):  # check if all timeseries are equally long
            for button in (wlvl_button, depth_button):
                change_button_state(button, button_disabled=True, button_value=False)
                time_pick_dropdown.disabled = True
        elif (
            not len(set([x[0] for x in time_pick_options])) == 1
        ):  # check if the start timestep is the same for all results
            for button in (wlvl_button, depth_button):
                change_button_state(button, button_disabled=True, button_value=False)
                time_pick_dropdown.disabled = True
        else:  # If above two conditions are met the buttons may stay enabled.
            time_pick_dropdown.disabled = False
            time_pick_dropdown.options = time_pick_options[0]

    def get_scenarios_selected_result(value):
        scenarios["selected_ids"] = [
            scenarios["names"].index(a) for a in download_selection_box.value
        ]  # id's of selected models to download

        update_buttons()  # Change button state based on selected scenarios
        update_time_pick_dropdown()  # change button state and dropdown based on selected scenarios

    download_selection_box.observe(get_scenarios_selected_result, "value")

    for button in file_buttons + raster_buttons:
        button.observe(update_button_icon, "value")

    # --------------------------------------------------------------------------------------------------
    # 5. destination folder creation/selection
    # --------------------------------------------------------------------------------------------------
    output_folder_box.observe(
        update_displays, "value"
    )  # Updated folders in selected dir

    # Observer dropdown box, when the value changes, update other stuff as well
    def on_select_change(selected_polder):
        try:
            selected_polder = selected_polder["new"]
        except:
            pass
        # update_folders_dict(selected_polder[3:])
        update_folders_dict(selected_polder)

        update_displays()

    output_polder_dropdown.observe(on_select_change, names="value")

    def update_output_selectbox(selected_download=""):
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
        output_select_box.options = (
            scenarios["folder"].threedi_results[scenarios["selected_folder"]].revisions
        )

        # Batch folder gets the same options.
        batch_folder_dropdown.options = (
            scenarios["folder"].threedi_results[scenarios["selected_folder"]].revisions
        )

        # Add the newly selected records to the list
        for new_selected in selected_download_new:
            if new_selected not in output_select_box.options:
                output_select_box.options = output_select_box.options + (new_selected,)

        # Select these new records.
        output_select_box.value = selected_download_new

        # Set Dem path for batch
        # if scenarios["folder"].model.rasters.find_dem() == "":
        #     dem_path_dropdown.options = [
        #         i.split(os.sep)[-1]
        #         for i in scenarios["folder"].model.rasters.find_ext("tif")
        #     ]
        # else:
        dem_path_dropdown.options = [
            scenarios["folder"].model.schema_base.rasters.dem.path
        ]

    # If a new value is selected in the download selection folder, update the output folder
    download_selection_box.observe(update_output_selectbox, names="value")

    # --------------------------------------------------------------------------------------------------
    # 6. Download
    # --------------------------------------------------------------------------------------------------

    def change_dowloadbutton_state():
        """Change color and disabled for download button."""
        if any([a.value for a in file_buttons + raster_buttons]):
            download_button.disabled = False
            download_button.style.button_color = "lightgreen"
        else:
            download_button.disabled = True
            download_button.style.button_color = "red"

    @download_button.on_click
    def download(action):
        """Download the selected models to the output folders"""
        selected_file_results = []  # list of selected files from the model to download
        if netcdf_button.value:
            selected_file_results += ["Raw 3Di output"]
        if agg_netcdf_button.value:
            selected_file_results += ["aggregated 3Di output"]
        if h5_button.value:
            selected_file_results += ["Grid administration"]
        if log_button.value:
            selected_file_results += ["Calculation core logging"]

        scenarios["download_url"] = create_download_url(
            scenarios["results"], scenarios["selected_ids"], selected_file_results
        )  # url to all download links.

        # Log in to 3Di server
        #     headers, username, passw, headers_results = retrieve_username_pw(username_widget.value, password_widget.value)

        uuid_list = []
        code_list = []
        target_srs_list = []
        resolution_list = []
        time_list = []
        pathname_list = []

        # Start download of selected files (if any are selected) ------------------------------------------------
        for name in download_selection_box.value:
            scenario_id = scenarios["names"].index(name)
            selected_result = scenarios["results"][scenario_id]
            # Print download URLs

            print(
                "\n\033[1m\033[31mDownloading files for {} (uuid={}):\033[0m".format(
                    name, selected_result["uuid"]
                )
            )
            for index, url in enumerate(scenarios["download_url"][name]):
                print("{}: {}".format(index + 1, url))

            # Print destination folder
            output_folder = str(
                scenarios["folder"].threedi_results[scenarios["selected_folder"]][name]
            )
            # if name in scenarios["folders_dict"].keys():
            #     output_folder = scenarios["folders_dict"][scenarios["selected_folder"]][
            #         name
            #     ]["folder"]
            # else:
            #     output_folder = os.path.join(
            #         scenarios["folders_dict"][scenarios["selected_folder"]]["folder"],
            #         name,
            #     )

            # De 3Di plugin kan geen '[' en ']' aan.
            output_folder = output_folder.replace("[", "")
            output_folder = output_folder.replace("]", "")

            print("\nThey will be placed in:\n" + output_folder + "\n")

            # Create destination folder
            if not os.path.exists(output_folder) and output_folder != "":
                os.mkdir(output_folder)
            #             print('Created folder: ' + output_folder.rsplit('/')[-1])

            # Start downloading of the files
            start_download(
                scenarios["download_url"][name],
                output_folder,
                api_key=dl.get_api_key(),
                automatic_download=1,
            )

            # Start download of selected rasters (if any are selected) -----------------------------------------------
            time = time_pick_dropdown.value
            if time is not None:
                time = time.replace("-", "_")
                time = time.replace(":", "_")
                
            res = str(resolution_dropdown.value).replace(".", "_")

            # Output files
            max_wlvl_path = os.path.join(
                output_folder, "max_wlvl_res{}m.tif".format(res)
            )
            max_depth_path = os.path.join(
                output_folder, "max_depth_res{}m.tif".format(res)
            )
            total_damage_path = os.path.join(
                output_folder, "total_damage_res{}m.tif".format(res)
            )
            wlvl_path = os.path.join(
                output_folder, "wlvl_{}_res{}m.tif".format(time, res)
            )
            depth_path = os.path.join(
                output_folder, "max_wlvl_res{}m.tif".format(time, res)
            )
            batch_path = os.path.join(
                output_folder,
                "{}_download_raster_batch.csv".format(
                    datetime.now().strftime("%Y-%m-%d %Hh%M")
                ),
            )

            if max_wlvl_button.value == True:
                if not os.path.exists(max_wlvl_path):
                    print("Preparing download of max waterlevel raster")
                    # dl.download_maximum_waterlevel_raster(selected_result['uuid'],"EPSG:28992",resolution_dropdown.value, pathname=max_wlvl_path)
                    uuid_list.append(selected_result["uuid"])
                    code_list.append("s1-max-dtri")
                    target_srs_list.append("EPSG:28992")
                    resolution_list.append(resolution_dropdown.value)
                    time_list.append(None)
                    pathname_list.append(max_wlvl_path)
                else:
                    print("{} already on system".format(max_wlvl_path.split("/")[-1]))

            if max_depth_button.value == True:
                if not os.path.exists(max_depth_path):
                    print("Preparing download of max waterdepth raster")
                    # dl.download_maximum_waterdepth_raster(selected_result['uuid'],"EPSG:28992",resolution_dropdown.value, pathname=max_depth_path)
                    uuid_list.append(selected_result["uuid"])
                    code_list.append("depth-max-dtri")
                    target_srs_list.append("EPSG:28992")
                    resolution_list.append(resolution_dropdown.value)
                    time_list.append(None)
                    pathname_list.append(max_depth_path)
                else:
                    print("{} already on system".format(max_depth_path.split("/")[-1]))

            if total_damage_button.value == True:
                if not os.path.exists(total_damage_path):
                    print("Preparing download of total damage raster")
                    # dl.download_total_damage_raster(selected_result['uuid'],"EPSG:28992",resolution_dropdown.value, pathname=total_damage_path)
                    uuid_list.append(selected_result["uuid"])
                    code_list.append("total-damage")
                    target_srs_list.append("EPSG:28992")
                    resolution_list.append(resolution_dropdown.value)
                    time_list.append(None)
                    pathname_list.append(total_damage_path)
                else:
                    print(
                        "{} already on system".format(total_damage_path.split("/")[-1])
                    )

            if wlvl_button.value == True:
                if not os.path.exists(wlvl_path):
                    print(
                        "Preparing download of waterlevel raster at timestep {}".format(
                            time
                        )
                    )
                    # dl.download_waterlevel_raster(selected_result['uuid'],"EPSG:28992",resolution_dropdown.value, time_pick_dropdown.value, pathname=wlvl_path)
                    uuid_list.append(selected_result["uuid"])
                    code_list.append("s1-dtri")
                    target_srs_list.append("EPSG:28992")
                    resolution_list.append(resolution_dropdown.value)
                    time_list.append(time_pick_dropdown.value)
                    pathname_list.append(wlvl_path)
                else:
                    print("{} already on system".format(wlvl_path.split("/")[-1]))

            if depth_button.value == True:
                if not os.path.exists(depth_path):
                    print(
                        "Preparing download of waterdepth raster at timestep {}".format(
                            time
                        )
                    )
                    # dl.download_waterdepth_raster(selected_result['uuid'],"EPSG:28992",resolution_dropdown.value, time_pick_dropdown.value, pathname=depth_path)
                    uuid_list.append(selected_result["uuid"])
                    code_list.append("depth-dtri")
                    target_srs_list.append("EPSG:28992")
                    resolution_list.append(resolution_dropdown.value)
                    time_list.append(time_pick_dropdown.value)
                    pathname_list.append(depth_path)
                else:
                    print("{} already on system".format(depth_path.split("/")[-1]))

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
            export_task_csv=batch_path,
        )

    # --------------------------------------------------------------------------------------------------
    # 7. Download batch
    # --------------------------------------------------------------------------------------------------
    @download_batch_button.on_click
    def download_batch(action):
        """Download the selected models to the output folders"""
        # Initialize folders and load directory structure

        uuid_list = []
        code_list = []
        target_srs_list = []
        bounds_list = []
        bounds_srs_list = []
        resolution_list = []
        pathname_list = []

        folder = scenarios["folder"]
        batch_fd = folder.threedi_results.batch[batch_folder_dropdown.value]

        # batch_fd = create_batch_folders_dict(batch_folder)
        # batch_fd = Folders(batch_folder).
        # Create destination folder

        batch_fd.create()
        batch_fd.downloads.create()

        selected_file_results = [
            "Raw 3Di output",
            "Grid administration",
        ]  # list of selected files from the model to download
        scenarios["download_url"] = create_download_url(
            scenarios["results"], scenarios["selected_ids"], selected_file_results
        )  # url to all download links.

        # Link selected files to scenarios. e.g: BWN Hoekje [#30] GLG blok 10 (10) 	 blok_GLG_T10
        df = pd.DataFrame(download_selection_box.value, columns=["name"])
        for index, row in df.iterrows():
            name = row["name"]
            for rain_type in RAIN_TYPES:
                for groundwater in GROUNDWATER:
                    for rain_scenario in RAIN_SCENARIOS:
                        rain_scenario = rain_scenario.strip("T")  # strip 'T' because its not used in older versions.
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
                                df.loc[index, "dl_name"] = "{}_{}_{}".format(
                                    rain_type, groundwater, "T{}".format(rain_scenario)
                                )
                                df.loc[index, "uuid"] = scenarios["results"][
                                    scenarios["names"].index(name)
                                ]["uuid"]

                        # Sommige resultaten zijn aangeroepen met GG ipv GGG in de naam. Onderstaande elif statement om dit te voorkomen
                        elif (
                            (groundwater.lower() == "ggg")
                            and (rain_type in name)
                            and ("GG" in name)
                            and (rain_scenario in name)
                        ):
                            if "0" not in [
                                name[a.start() + len(rain_scenario)]
                                for a in re.finditer(rain_scenario, name)
                                if len(name) > (a.start() + len(rain_scenario))
                            ]:  # filters this: BWN Hoekje [#10] GLG blok 100 (10)
                                df.loc[index, "dl_name"] = "{}_{}_{}".format(
                                    rain_type, groundwater, "T{}".format(rain_scenario)
                                )
                                df.loc[index, "uuid"] = scenarios["results"][
                                    scenarios["names"].index(name)
                                ]["uuid"]
        df.set_index("name", inplace=True)
        # display(df)
        df.to_csv(str(batch_fd.downloads.download_uuid))

        # Get raster size of dem, max depth rasters are downloaded on this resolution.
        print(scenarios["folder"])
        print(folder.model.schema_base.rasters.full_path(dem_path_dropdown.value))
        dem = hrt.Raster(folder.model.schema_base.rasters.dem.path)

        # Start download of selected files (if any are selected) ------------------------------------------------
        for name, row in df.iterrows():
            scenario_id = scenarios["names"].index(name)
            selected_result = scenarios["results"][scenario_id]

            print(
                "\n\033[1m\033[31mDownloading files for {} (uuid={}):\033[0m".format(
                    name, selected_result["uuid"]
                )
            )

            #Download netcdf of 2 GHG_T1000 results.
            if row["dl_name"] in RAW_DOWNLOADS:

                output_folder = getattr(batch_fd.downloads, row["dl_name"]).netcdf

                # FIXME hoort dit niet een niveau hoger te staan?
                # De 3Di plugin kan geen '[' en ']' aan.
                # output_folder = output_folder.replace("[", "")
                # output_folder = output_folder.replace("]", "")

                # Create destination folder
                output_folder.create()

                # Start downloading of the files
                start_download(
                    scenarios["download_url"][name],
                    output_folder.path,
                    api_key=dl.get_api_key(),
                    automatic_download=1,
                )

            # TODO below, make a list of uuid's, bounds, resolution and pathname
            # Donwload max depth and damage rasters
            # TODO max_depth should be defined in folders.py?
            max_depth = getattr(batch_fd.downloads, row["dl_name"]).max_depth

            print(dem)
            if not max_depth.exists:
                print("Preparing download of max waterdepth raster")
                uuid_list.append(selected_result["uuid"])
                code_list.append("depth-max-dtri")
                target_srs_list.append("EPSG:28992")
                bounds_list.append(dem.metadata["bounds_dl"])
                bounds_srs_list.append("EPSG:28992")
                resolution_list.append(dem.metadata["pixel_width"])
                pathname_list.append(max_depth.path)

            else:
                print("{} already on system".format(max_depth.name))

            # total_damage = str(batch_fd.downloads.totaal_total_damage)
            total_damage = getattr(batch_fd.downloads, row["dl_name"]).total_damage

            if not total_damage.exists:
                print("Preparing download of total damage raster")

                uuid_list.append(selected_result["uuid"])
                code_list.append("total-damage")
                target_srs_list.append("EPSG:28992")
                bounds_list.append(dem.metadata["bounds_dl"])
                bounds_srs_list.append("EPSG:28992")
                resolution_list.append(0.5)
                pathname_list.append(total_damage.path)

            else:
                print("{} already on system".format(total_damage.name))

            # TODO: call batch download function
        print("uuid_list: {}".format(uuid_list))
        print("code_list: {}".format(code_list))
        print("target_srs_list: {}".format(target_srs_list))
        print("resolution_list: {}".format(resolution_list))
        # print("pathname_list: {}".format(pathname_list))

        batch_path = batch_fd.full_path(
            f"{datetime.now().strftime('%Y-%m-%d %Hh%M')}_download_raster_batch.csv"
        )

        dl.download_raster(
            uuid_list,
            raster_code=code_list,
            target_srs=target_srs_list,
            resolution=resolution_list,
            bounds=bounds_list,
            bounds_srs=bounds_srs_list,
            pathname=pathname_list,
            export_task_csv=batch_path,
        )

    # --------------------------------------------------------------------------------------------------
    # Initialize GUI
    # --------------------------------------------------------------------------------------------------
    # Reload output folder so it shows correct values first time around
    on_select_change(output_polder_dropdown.value)
    update_buttons()

    api_key_widget.value = api_keys["lizard"]
    api_key_widget.disabled = False

    ###################################################################################################
    # Create GUI
    ###################################################################################################
    download_tab = widgets.GridBox(
        children=[
            login_label,
            api_key_widget,
            get_api_key_widget,  # 1. login
            select_polder_label,
            polder_name_label,
            polder_name_widget,
            model_revision_label,  # 2. polder/revision
            model_revision_widget,
            find_results_button,  # 2
            download_selection_label,
            download_selection_box,
            show_0d1d_button,  # 3. download selection
            show_all_button,
            search_results_widget,
            download_files_label,  # 3
            file_buttons_label,
            file_buttons_box,
            raster_buttons_label,
            raster_buttons_box,  # 4
            time_resolution_box,  # 4
            output_label,
            output_polder_dropdown,
            output_folder_box,
            output_select_box,  # 5. output
            download_button_label,
            download_button,  # 6 download
            batch_folder_label,
            batch_folder_dropdown,
            dem_path_label,
            dem_path_dropdown,
            download_batch_button_label,
            download_batch_button,  # 7 download batch
        ],
        layout=widgets.Layout(
            width="100%",
            grid_row_gap="200px 200px 200px 200px",
            grid_template_rows="auto auto auto auto auto auto auto auto auto auto auto auto",
            grid_template_columns="20% 10% 10% 15% 15% 10% 10% 10%",
            grid_template_areas="""
                'login_label login_label select_polder_label select_polder_label . . . .'
                'api_key api_key polder_name_label polder_name_widget find_results_button . . .'
                'get_api_key get_api_key polder_rev_label polder_rev_widget find_results_button . . .'
                'download_selection_label download_selection_label . dl_files_label dl_files_label output_label output_label .'
                'dl_sel_box dl_sel_box dl_sel_box file_buttons_label . output_polder_dropdown output_polder_dropdown output_polder_dropdown'
                'dl_sel_box dl_sel_box dl_sel_box file_buttons_box file_buttons_box output_folder_box output_folder_box output_folder_box'
                'dl_sel_box dl_sel_box dl_sel_box file_buttons_box file_buttons_box output_select_box output_select_box output_select_box'
                'dl_sel_box dl_sel_box dl_sel_box raster_buttons_label raster_buttons_label output_select_box output_select_box output_select_box'
                'dl_sel_box dl_sel_box dl_sel_box raster_buttons_box raster_buttons_box output_select_box output_select_box output_select_box'
                'search_results button_0d1d all_button raster_buttons_box raster_buttons_box  output_select_box output_select_box output_select_box'
                '. . . time_resolution_box time_resolution_box time_resolution_box download_button_label download_button_label'
                '. . . time_resolution_box time_resolution_box time_resolution_box download_button download_button'
                '. . . . . . download_batch_button_label download_batch_button_label'
                '. . dem_path_label dem_path_label batch_folder_label batch_folder_label download_batch_button download_batch_button'
                '. . dem_path_dropdown dem_path_dropdown batch_folder_dropdown batch_folder_dropdown download_batch_button download_batch_button'
                """,
        ),
    )

    #     username_widget.value='wietse.vangerwen'
    # password_widget.value =''
    polder_name_widget.value='heiloo_geen_gemaal'
    #     model_revision_widget.value='50'
    return download_tab

    # download_gui(); download_tab

    # find_results_button.click()

    # download_selection_box.value = ('BWN Hoekje [#30] GHG blok 1000 (18)',
    #  'BWN Hoekje [#30] GHG blok 100 (17)',
    #  'BWN Hoekje [#30] GHG blok 10 (16)',
    #  'BWN Hoekje [#30] GGG blok 1000 (15)',
    #  'BWN Hoekje [#30] GGG blok 100 (14)',
    #  'BWN Hoekje [#30] GGG blok 10 (13)',
    #  'BWN Hoekje [#30] GLG blok 1000 (12)',
    #  'BWN Hoekje [#30] GLG blok 100 (11)',
    #  'BWN Hoekje [#30] GLG blok 10 (10)',
    #  'BWN Hoekje [#30] GHG piek 1000 (9)',
    #  'BWN Hoekje [#30] GHG piek 100 (8)',
    #  'BWN Hoekje [#30] GHG piek 10 (7)',
    #  'BWN Hoekje [#30] GGG piek 1000 (6)',
    #  'BWN Hoekje [#30] GGG piek 100 (5)',
    #  'BWN Hoekje [#30] GGG piek 10 (4)',
    #  'BWN Hoekje [#30] GLG piek 1000 (3)',
    #  'BWN Hoekje [#30] GLG piek 100 (2)',
    #  'BWN Hoekje [#30] GLG piek 10 (1)')

# %%
if __name__ == '__main__':
    data = {'polder_folder': 'E:\\02.modellen\\HUB - Scenarioberekeningen wateroverlast',
 'api_keys_path': 'C:\\Users\\wvangerwen\\AppData\\Roaming\\3Di\\QGIS3\\profiles\\default\\python\\plugins\\hhnk_threedi_plugin\\api_key.txt'}
    self = download_gui(data=data)
    display(self)
# %%
