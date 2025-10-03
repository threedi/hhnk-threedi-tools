import os
import time
from dataclasses import dataclass
from pathlib import Path

from hhnk_threedi_tools.core.folders import Folders

path = r"E:\02.modellen\castricum"


@dataclass
class ModelInfo:
    model_name: str
    source_data: Path
    source_data_old: Path
    fn_damo_old: Path
    fn_hdb_old: Path
    fn_damo_new: Path
    fn_hdb_new: Path
    damo_selection: Path
    date_damo_old: str
    date_damo_new: str
    date_hdb_old: str
    date_hdb_new: str
    date_sqlite: str


def get_model_info(path: str) -> ModelInfo:
    folder = Folders(path)
    source_data = Path(folder.source_data.path)
    model_name = folder.name
    fn_threedimodel = folder.model.schema_base.content[0]

    source_data_old = source_data / "vergelijkingsTool" / "old"

    fn_damo_old = source_data_old / "DAMO.gdb"
    fn_hdb_old = source_data_old / "HDB.gdb"
    fn_damo_new = source_data / "DAMO.gpkg"
    fn_hdb_new = source_data / "HDB.gpkg"
    damo_selection = source_data / "polder_polygon.gpkg"

    return ModelInfo(
        model_name=model_name,
        source_data=source_data,
        source_data_old=source_data_old,
        fn_damo_old=fn_damo_old,
        fn_hdb_old=fn_hdb_old,
        fn_damo_new=fn_damo_new,
        fn_hdb_new=fn_hdb_new,
        damo_selection=damo_selection,
        date_damo_old=time.ctime(os.path.getmtime(fn_damo_old)),
        date_damo_new=time.ctime(os.path.getmtime(fn_damo_new)),
        date_hdb_old=time.ctime(os.path.getmtime(fn_hdb_old)),
        date_hdb_new=time.ctime(os.path.getmtime(fn_hdb_new)),
        date_sqlite=time.ctime(os.path.getmtime(fn_threedimodel)),
    )


# Define de Symbology in case we want to make it transparent or not
def symbology_both(opacity):
    if opacity:
        return 0
    else:
        return 128


# source_data_old = os.path.join(source_data, "vergelijkingsTool", "old")
# fn_damo_old = Path(os.path.join(source_data_old, "DAMO.gdb"))
# fn_hdb_old = Path(os.path.join(source_data_old, "HDB.gdb"))

# fn_damo_new = Path(os.path.join(source_data, "DAMO.gpkg"))
# fn_hdb_new = Path(os.path.join(source_data, "HDB.gpkg"))
# damo_selection = Path(os.path.join(source_data, "polder_polygon.gpkg"))

# date_old_damo = date_fn_damo_old
# date_new_damo = date_fn_damo_new
# date_sqlite = date_3di_new
