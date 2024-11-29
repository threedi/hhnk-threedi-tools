import json
import os
import sys


def update_syspath(sys_paths):
    for sys_path in sys_paths:
        print(sys_path)
        if sys_path not in sys.path:
            if os.path.exists(sys_path):
                sys.path.append(sys_path)


def setup_notebook() -> json:
    """Load notebook data and fix syspath."""
    notebook_data = {}
    try:
        with open(os.getcwd() + "/notebook_data.json") as f:
            notebook_data = json.load(f)
        update_syspath(notebook_data["extra_paths"])
    except:
        print("Failed to update path")
    finally:
        return notebook_data
