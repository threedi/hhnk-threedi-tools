# %%


import os
from pathlib import Path

import hhnk_research_tools as hrt
import numpy as np

from hhnk_threedi_tools import Folders

logger = hrt.logging.get_logger(__name__)


def create_sorted_actiontable_queries(database: hrt.Sqlite) -> list[str]:
    """
    Sommige modellen hebben een sturing die niet door de validatie van 3Di komt.
    Hier is ergens een keer de action_table verkeerd gesorteerd.

    Met dit script kan de sortering goed gezet worden in de sqlite.

    Parameters
    ----------
    database : hrt.Sqlite
        path to the database. This can be retrieved from htt.Folders with:
        folder.model.schema_base.database

    Returns
    -------
    queries : list[str]
        list of sql queries of all controls in the provided model. This list should be
    """

    control_df = database.read_table("v2_control_table")
    queries = []

    # Voor elke sturings regel de actiontable sorteren
    for index, row in control_df.iterrows():
        action_table_string = row["action_table"]

        measure_list = []
        action_list = []

        action_type = row["action_type"]
        for entry in action_table_string.split("#"):
            try:
                measurement = [float(entry.split(";")[0])]
                measure_list.append(measurement[0])
            except ValueError as e:
                # Problem with action table
                logger.error(f"""Problem with '{entry}' at index {action_table_string.index(entry)} of the action_table_string for
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
    return queries


def update_sorted_actiontable(database, queries: list[str]):
    logger.info(f"Updating {len(queries)} table controls for {database.name}")
    for query in queries:
        database.execute_sql_changes(query=query)


# %%
if __name__ == "__main__":
    # %% Op een specifieke map
    folder_dir = Path(r"E:\02.modellen\VNK_zevenhuis")

    # %% of in de local folder
    from notebook_setup import setup_notebook

    notebook_data = setup_notebook()
    folder_dir = Path(notebook_data["polder_folder"])

    # %%

    assert folder_dir.exists()
    folder = Folders(folder_dir)

    queries = create_sorted_actiontable_queries(database=folder.model.schema_base.database)
    update_sorted_actiontable(queries=queries)

    # %% test_purpose to relative path
    folder_dir = Path(__file__).parents[3].joinpath("tests", "data", "model_test")
