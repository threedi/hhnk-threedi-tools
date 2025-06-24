# %%
import os

import numpy as np
from notebook_setup import setup_notebook

from hhnk_threedi_tools import Folders

# %%
notebook_data = setup_notebook()
folder_dir = notebook_data["polder_folder"]

# %% test_purpose to relative path
relative_path = r"..\..\..\tests\data\model_test"
folder_dir = os.path.join(os.getcwd(), relative_path)

# %% Op een specifieke map
folder_dir = r"E:\02.modellen\VNK_zevenhuis"

folder = Folders(folder_dir)

# %%
control = folder.model.schema_base.database.read_table("v2_control_table")
queries = []

for index, row in control.iterrows():
    action_table_string = row["action_table"]
    action_table = []

    measure_list = []
    action_list = []

    action_type = row["action_type"]
    for entry in action_table_string.split("#"):
        try:
            measurement = [float(entry.split(";")[0])]
            measure_list.append(measurement[0])
        except ValueError as e:
            # Problem with action table
            print(f"""Problem with '{entry}' at index {action_table_string.index(entry)} of the action_table_string for
{row}
""")
            raise e

        if action_type in ["set_crest_level", "set_pump_capacity"]:
            action = [float(entry.split(";")[1])]
            action_list.append(action[0])

        order = np.argsort(measure_list)

        measure_order = np.array(measure_list)[order]
        action_order = np.array(action_list)[order]

        action_string = ""
        for nr, (m, a) in enumerate(zip(measure_order, action_order)):
            action_string += f"{m};{a}"
            if nr != len(measure_order) - 1:
                action_string += "#"

    update_str = f"UPDATE v2_control_table SET action_table='{action_string}' WHERE id={row['id']}"

    queries.append(update_str)
# %%
print(f"Updating {len(queries)} table controls for {folder.model.schema_base.database.name}")
for query in queries:
    folder.model.schema_base.database.execute_sql_changes(query=query)
