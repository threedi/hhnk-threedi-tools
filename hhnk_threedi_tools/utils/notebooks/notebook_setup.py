import json
import os
import sys


def update_syspath(sys_paths):
    for sys_path in sys_paths:
        if sys_path not in sys.path:
            if os.path.exists(sys_path):
                sys.path.append(sys_path)


def setup_notebook() -> json:
    """Load notebook data and fix syspath.
    Through QGIS its setup to add the plugin extra dependencies folders for
    - hhnk_threedi_plugin
    - threedi_results_analysis

    notebook_data also contains the api_keys_path and polder_folder
    """
    notebook_data = {}
    try:
        with open(os.getcwd() + "/notebook_data.json") as f:
            notebook_data = json.load(f)
        update_syspath(notebook_data["extra_paths"])
    except:
        print("Failed to update path")
    finally:
        return notebook_data
