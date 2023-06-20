import sys
import os
import json

def update_syspath(sys_paths):
    for sys_path in sys_paths:
        if sys_path not in sys.path:
            if os.path.exists(sys_path):
                sys.path.append(sys_path)


def setup_notebook() -> json:
    """Load notebook data and fix syspath."""
    try:
        with open(os.getcwd() + "/notebook_data.json") as f:
            notebook_data = json.load(f)
        update_syspath(notebook_data["extra_paths"]) 
        return notebook_data
    except:
        print("Failed to update path")
        return {}
