# %%
import shutil

import hhnk_research_tools as hrt

from hhnk_threedi_tools.utils.notebooks.nr07_sort_control_table_values import (
    create_sorted_actiontable_queries,
    update_sorted_actiontable,
)
from tests.config import FOLDER_TEST, TEMP_DIR

# %%


def test_sort_control_table():
    database_path = shutil.copy(
        FOLDER_TEST.model.schema_base.database.path,
        TEMP_DIR.joinpath(f"bwn_test_sort_control_{hrt.get_uuid()}.sqlite"),
    )

    database = hrt.Sqlite(database_path)

    queries = create_sorted_actiontable_queries(database=database)

    assert queries == [
        "UPDATE v2_control_table SET action_table='-0.85;-0.85#-0.84;-0.95#-0.83;-1.05#-0.82;-1.15#-0.81;-1.25#-0.8;-1.25#-0.79;-1.25#-0.78;-1.25#-0.77;-1.25#-0.76;-1.25' WHERE id=53"
    ]

    update_sorted_actiontable(database=database, queries=queries)


if __name__ == "__main__":
    test_sort_control_table()
