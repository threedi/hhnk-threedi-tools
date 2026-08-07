from ipywidgets import Button, HBox, Label, Text, VBox
from typing import Any
from hhnk_threedi_tools.core.vergelijkingstool.utils import get_model_info

from pathlib import Path

def update_output_folder(change):
        """Update model path"""
        try:
            model_info = get_model_info(change["new"])  
            output_folder_text.value = str(model_info.output_folder)
            
        except Exception as e:
            output_folder_text.value = "model could not be read"
            print(e)

def update_output_file_path(change= None):
        """Update model path"""
        try:
            output_folder = Path(output_folder_text.value)
            output_name_input_text = output_name_input.value
            full_path = str(output_folder / output_name_input_text)
            output_file_path.value = full_path
            
        except Exception as e:
            output_file_path.value = "Path has not been update"
            print(e)

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
