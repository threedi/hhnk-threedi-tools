import os
from pathlib import Path
from ..variables.types import file_types_dict, GPKG, CSV
from ..variables.definitions import GPKG_DRIVER
from ..variables.default_variables import DEF_DELIMITER, DEF_ENCODING

def ensure_file_path(filepath):
    """
    Functions makes sure all folders in a given file path exist. Creates them if they don't.
    """
    try:
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise e from None

def gdf_write_to_geopackage(gdf, path, filename, driver=GPKG_DRIVER, index=False):
    """
    Functions outputs DataFrame of GeoDataFrame to .gpkg document

        gdf_write_to_csv(
            gdf (DataFrame, to be written to .gpkg file)
            path (string, folder to create new file in)
            filename (string, name of file to be created, without extension (.csv)
            driver -> 'GPKG' (driver to be used by .to_file function of gdf)
            index -> False (given as index parameter to .to_file function of gdf (Write row names or not)

    Return value: file path (path + filename + extension) if gdf is not empty, else None
    """
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
            return filepath
        return None
    except Exception as e:
        raise e from None

def gdf_write_to_csv(gdf, path, filename, mode='w', cols=None, index=False):
    """
    Functions outputs DataFrame of GeoDataFrame to .csv document

        gdf_write_to_csv(
            gdf (DataFrame, to be written to .csv file)
            path (string, folder to create new file in)
            filename (string, name of file to be created, without extension (.csv)
            mode -> 'w' (optional specification of write mode)
            cols -> None (specify columns to write to output)
            index -> False (given as index parameter to .to_csv function of gdf (Write row names or not)

    Return value: file path (path + filename + extension) if gdf is not empty, else None
    """
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
            return filepath
        else:
            return None
    except Exception as e:
        raise e from None
