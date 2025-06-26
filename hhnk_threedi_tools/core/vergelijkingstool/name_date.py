import os
import time
from pathlib import Path

from hhnk_threedi_tools.core.folders import Folders

path = r"E:\02.modellen\castricum"


# Get the model name and the path from the model we want to work with
def name(path):
    folder = Folders(path)
    source_data = folder.source_data.path
    model_name = folder.name
    return (model_name, source_data, folder)


model_name, source_data, folder = name(path)

fn_threedimodel = folder.model.schema_base.content[0]


# get de data from the old and new DAMO/Threedi database
def date(damo_new, damo_old, hdb_new, hdb_old, threedi):
    time_seconds_damo_new = os.path.getmtime(damo_new)
    time_seconds_damo_old = os.path.getmtime(damo_old)
    time_seconds_hdb_new = os.path.getmtime(hdb_new)
    time_seconds_hdb_old = os.path.getmtime(hdb_old)
    time_seconds_sqlite_old = os.path.getmtime(threedi)

    date_damo_new = time.ctime(time_seconds_damo_new)
    date_damo_old = time.ctime(time_seconds_damo_old)
    date_hdb_new = time.ctime(time_seconds_hdb_new)
    date_hdb_old = time.ctime(time_seconds_hdb_old)
    date_3di_new = time.ctime(time_seconds_sqlite_old)
    return (date_damo_new, date_damo_old, date_hdb_new, date_hdb_old, date_3di_new)


# Define de Symbology in case we want to make it transparent or not
def symbology_both(opacity):
    if opacity:
        return 0
    else:
        return 128


source_data_old = os.path.join(source_data, "vergelijkingsTool", "old")
fn_damo_old = Path(os.path.join(source_data_old, "DAMO.gdb"))
fn_hdb_old = Path(os.path.join(source_data_old, "HDB.gdb"))

fn_damo_new = Path(os.path.join(source_data, "DAMO.gpkg"))
fn_hdb_new = Path(os.path.join(source_data, "HDB.gpkg"))
damo_selection = Path(os.path.join(source_data, "polder_polygon.gpkg"))


date_fn_damo_new, date_fn_damo_old, date_hdb_new, date_hdb_old, date_3di_new = date(
    fn_damo_new, fn_damo_old, fn_hdb_new, fn_hdb_old, fn_threedimodel
)

date_old_damo = date_fn_damo_old
date_new_damo = date_fn_damo_new
date_sqlite = date_3di_new
