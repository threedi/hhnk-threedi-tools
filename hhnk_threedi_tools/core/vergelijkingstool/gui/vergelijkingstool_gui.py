from pathlib import Path
from typing import Any

import fiona
import ipywidgets as widgets
from IPython.display import clear_output, display
from ipywidgets import Button, HBox, Label, Text, VBox

from hhnk_threedi_tools.core.vergelijkingstool import config, main
from hhnk_threedi_tools.core.vergelijkingstool.utils import get_model_info


def run_gui(
    fn_DAMO_selection,
    fn_damo_new,
    fn_hdb_new,
    fn_damo_old,
    fn_hdb_old,
    fn_threedimodel,
    fn_DAMO_comparison_export,
    fn_threedi_comparison_export,
):
    """
    Set of the graphic interface. Include: compare, checkbox and run buttons
    mainly a copy of the previous gui
    """

    output_box = widgets.Output()

    model_base_path_input = Text(
        value="",
        placeholder="Enter base model folder (e.g. E:/models/castricum)",
        description="Model folder:",
        layout=widgets.Layout(width="90%"),
    )

    output_folder_text = Text(
        value="", description="Output folder location:", disable=True, layout=widgets.Layout(width="70%")
    )

    output_name_input = Text(
        value="comparison_output.gpkg",
        description="Output name:",
        layout=widgets.Layout(width="25%"),
    )

    # Select comparation type
    compare_title = Label(value="Which database do you want to compare with?")
    options = ["Compare with Damo", "Compare with 3Di", "Both"]
    compare_buttons = widgets.ToggleButtons(
        options=options,
        value=None,
        layout=widgets.Layout(width="auto"),
    )

    # Select checkboxes to select layer to compare in case it is wanted.
    # selection for damo
    select_layer_damo = widgets.Checkbox(
        value=False,
        description="Do you want to compare specific layer DAMO/DAMO?",
        indent=False,
    )
    # selection for 3di
    select_layer_3di = widgets.Checkbox(
        value=False,
        description="Do you want to compare specific layer 3Di/DAMO?",
        indent=False,
    )

    # Run botton
    run_button = Button(description="Run Comparison", button_style="success")

    # show widgets.
    output_section = VBox([Label("Output configuration"), HBox([output_folder_text, output_name_input])])

    main_box = VBox([model_base_path_input, compare_title, compare_buttons, output_section, output_box])
    display(main_box)

    def update_output_folder(change):
        """Update model path"""
        try:
            model_info = get_model_info(change["new"])  # tu función ya hace esto
            output_folder_text.value = str(model_info.output_folder)
        except Exception as e:
            output_folder_text.value = "model could not be read"
            print(e)

    model_base_path_input.observe(update_output_folder, names="value")

    update_output_folder({"new": model_base_path_input.value})

    # function use to make the gui dinamic.
    def on_compare_change(change: Any) -> None:
        """
        First step. If the user select any of the options. change['new']
        get that the varaiable  of the selection
        """
        with output_box:
            # step1 clear the output_box
            clear_output()

            # change["new"] is the option selected in the widget and it will show the
            # checkbox depending o n the selection.
            if change["new"] == "Compare with Damo":
                display(select_layer_damo)
            elif change["new"] == "Compare with 3Di":
                display(select_layer_3di)
            elif change["new"] == "Both":
                display(VBox([select_layer_damo, select_layer_3di]))
            display(run_button)

    def on_damo_checkbox_change(change: Any) -> None:
        """
        Second shared step. If the user want to select specific layer from DAMO/DAMO
        the user will be able to select those from this widget.
        """
        with output_box:
            clear_output()

            if change["new"]:
                print("Select layers to compare:")

                # deploy layers to be seen in the gui
                layers_damo = fiona.listlayers(fn_damo_new)
                layers_hdb = fiona.listlayers(fn_hdb_new)

                # put the layers list from damo in bottons
                damo_buttons = [
                    widgets.ToggleButton(description=l, layout=widgets.Layout(width="48%")) for l in layers_damo
                ]
                # put the layers list from hdb in bottons
                hdb_buttons = [
                    widgets.ToggleButton(description=l, layout=widgets.Layout(width="48%")) for l in layers_hdb
                ]

                # put the bottons in box for damo and hdb
                half = len(damo_buttons) // 2

                damo_row1 = HBox(damo_buttons[:half], layout=widgets.Layout(flex_flow="row wrap"))
                damo_row2 = HBox(damo_buttons[half:], layout=widgets.Layout(flex_flow="row wrap"))

                half_hdb = len(hdb_buttons) // 2

                hdb_row1 = HBox(hdb_buttons[:half_hdb], layout=widgets.Layout(flex_flow="row wrap"))
                hdb_row2 = HBox(hdb_buttons[half_hdb:], layout=widgets.Layout(flex_flow="row wrap"))

                display(Label(value="DAMO:"))
                display(VBox([damo_row1, damo_row2]))
                display(Label(value="HDB layers:"))
                display(HBox([hdb_row1, hdb_row2]))
                # display the columns in the gui

                display(run_button)

                # this function collect the selected layer or all of them and the
                # run the main when click is done
                def on_run_clicked(_event: Any) -> None:
                    """Handle Run Comparison click: collect selections in damo and hdb and run main.

                    The event parameter is provided by ipywidgets but not used.
                    """
                    base_path = model_base_path_input.value.strip()
                    if not base_path:
                        print("Please enter a valid base model folder path.")
                        return

                    # create model_info object.
                    model_info = get_model_info(base_path)

                    # colect layer to be compapred
                    selected_damo = [b.description for b in damo_buttons if b.value]
                    selected_hdb = [b.description for b in hdb_buttons if b.value]

                    print(f"Selected DAMO layers: {selected_damo}")
                    print(f"Selected HDB layers: {selected_hdb}")
                    print("Running comparison...")

                    # run main
                    main.main(
                        model_info=model_info,
                        fn_DAMO_selection=fn_DAMO_selection,
                        fn_damo_new=fn_damo_new,
                        fn_hdb_new=fn_hdb_new,
                        fn_damo_old=fn_damo_old,
                        fn_hdb_old=fn_hdb_old,
                        fn_threedimodel=fn_threedimodel,
                        fn_DAMO_comparison_export=fn_DAMO_comparison_export,
                        fn_threedi_comparison_export=fn_threedi_comparison_export,
                        compare_with="Compare with Damo",
                        layer_selection=True,
                        layers_input_hdb_selection=selected_hdb,
                        layers_input_damo_selection=selected_damo,
                        threedi_layer_selector=False,
                        threedi_structure_selection=[],
                        damo_structure_selection=[],
                        structure_codes=[],
                    )

                    print("Comparison finished!")

                run_button.on_click(on_run_clicked)
            else:
                print("Using all layers by default.")
                display(run_button)

    def on_3di_checkbox_change(change: Any) -> None:
        """
        Handle the 3Di-structure selection checkbox change.

        When enabled (change["new"] is truthy) this displays widgets to select:
        - 3Di structure layers
        - DAMO/HDB layers
        - Structure codes
        """

        with output_box:
            clear_output()

            if change["new"]:
                print("Select structures to compare:")

                threedi_layers = config.THREEDI_STRUCTURE_LAYERS
                damo_layers = config.DAMO_HDB_STRUCTURE_LAYERS
                codes = config.STRUCTURE_CODES

                threedi_buttons = [
                    widgets.ToggleButton(description=l, layout=widgets.Layout(width="48%")) for l in threedi_layers
                ]
                damo_buttons = [
                    widgets.ToggleButton(description=l, layout=widgets.Layout(width="48%")) for l in damo_layers
                ]
                code_buttons = [widgets.ToggleButton(description=c, layout=widgets.Layout(width="48%")) for c in codes]

                display(Label(value="3Di structure layers:"))
                display(HBox(threedi_buttons))
                display(Label(value="DAMO/HDB layers:"))
                half = len(damo_buttons) // 2
                damo_row1 = HBox(damo_buttons[:half], layout=widgets.Layout(flex_flow="row wrap"))
                damo_row2 = HBox(damo_buttons[half:], layout=widgets.Layout(flex_flow="row wrap"))
                display(VBox([damo_row1, damo_row2]))
                display(Label(value="Structure codes:"))
                display(HBox(code_buttons))
                display(run_button)

                def on_run_clicked(_):
                    selected_3di = [b.description for b in threedi_buttons if b.value]
                    selected_damo = [b.description for b in damo_buttons if b.value]
                    selected_codes = [b.description for b in code_buttons if b.value]

                    print(f"Selected 3Di layers: {selected_3di}")
                    print(f"Selected DAMO layers: {selected_damo}")
                    print(f"Selected structure codes: {selected_codes}")
                    print("Running comparison...")

                    main.main(
                        fn_DAMO_selection,
                        fn_damo_new,
                        fn_hdb_new,
                        fn_damo_old,
                        fn_hdb_old,
                        fn_threedimodel,
                        fn_DAMO_comparison_export,
                        fn_threedi_comparison_export,
                        "Compare with 3Di",
                        layer_selection=False,
                        layers_input_hdb_selection=[],
                        layers_input_damo_selection=[],
                        threedi_layer_selector=True,
                        threedi_structure_selection=selected_3di,
                        damo_structure_selection=selected_damo,
                        structure_codes=selected_codes,
                    )

                    print("3Di comparison finished!")

                run_button.on_click(on_run_clicked)
            else:
                print("Using all structures by default.")
                display(run_button)

    compare_buttons.observe(on_compare_change, names="value")
    select_layer_damo.observe(on_damo_checkbox_change, names="value")
    select_layer_3di.observe(on_3di_checkbox_change, names="value")
