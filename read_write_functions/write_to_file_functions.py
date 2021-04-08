import os
from pathlib import Path
from ..variables.types import file_types_dict, GPKG, CSV
from ..variables.definitions import GPKG_DRIVER
from ..variables.default_variables import DEF_DELIMITER, DEF_ENCODING

def ensure_file_path(filepath):
    try:
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise e from None

def gdf_write_to_geopackage(gdf, path, filename, driver=GPKG_DRIVER, index=False):
    ext = file_types_dict[GPKG]
    filepath = os.path.join(path, filename + ext)
    try:
        if os.path.exists(filepath):
            os.remove(filepath)
        if not gdf.empty:
            ensure_file_path(filepath)
            gdf.to_file(filepath,
                        layer=filename,
                        driver=driver,
                        index=index)
    # except OSError as e:
    #     pass
    except Exception as e:
        raise e from None

def gdf_write_to_csv(gdf, path, filename, mode='w', cols=None, index=False):
    ext = file_types_dict[CSV]
    filepath = os.path.join(path, filename + ext)
    try:
        ensure_file_path(filename)
        if os.path.exists(filepath):
            os.remove(filepath)
        if not gdf.empty:
            ensure_file_path(filepath)
            gdf.to_csv(filepath,
                       sep=DEF_DELIMITER,
                       encoding=DEF_ENCODING,
                       columns=cols,
                       mode=mode,
                       index=index)
    # except OSError as e:
    #     pass
    except Exception as e:
        raise e from None
