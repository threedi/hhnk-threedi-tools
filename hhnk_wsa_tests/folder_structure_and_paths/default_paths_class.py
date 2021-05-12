import os
from hhnk_threedi_tools.variables.types import file_types_dict, GDB, SHAPE, SQLITE, TIF
from pathlib import Path

# Set names for certain input files
damo = f"DAMO{file_types_dict[GDB]}"
hdb = f"HDB{file_types_dict[GDB]}"
datachecker = f"datachecker_output{file_types_dict[GDB]}"
polder_poly = f"polder_polygon{file_types_dict[SHAPE]}"
channel_from_profiles = f"channel_surface_from_profiles{file_types_dict[SHAPE]}"

def find_dem(base_dir):
    """
    Look for file starting with dem_ and ending with extension .tif in given directory

    Returns path if found, empty string if not found
    """
    if not os.path.exists(base_dir):
        return ""
    else:
        p = Path(base_dir)
        dir_list = [item for item in p.iterdir()
                    if item.suffix == file_types_dict[TIF]
                    and item.stem.startswith('dem_')]
        if len(dir_list) == 1:
            return os.path.join(base_dir, dir_list[0].name)
        else:
            return ""

def find_database(base_dir):
    """
    Tries to find database in given path by looking for extension.

    Returns path if only one file with .sqlite extension is found. Returns empty string
    in other cases (none found or more than one found)
    """
    if not os.path.exists(base_dir):
        return ""
    else:
        sqlite_files = [item for item in os.listdir(base_dir)
                        if item.endswith(file_types_dict[SQLITE])]
        # If one is found, we are going to assume that is the one we want to use
        if len(sqlite_files) == 1:
            return os.path.join(base_dir, sqlite_files[0])
        else:
            # Couldn't detect automatically
            return ""

class RasterPaths:
    def __init__(self, base):
        self.base = os.path.join(base, "rasters")
        self.dem = find_dem(self.base)

class ModelPaths:
    def __init__(self, base):
        # Files
        self.base = os.path.join(base, "02_Model")
        self.database = find_database(self.base)
        # Folders
        self.rasters = RasterPaths(self.base)

class OutputPaths:
    """
    Output paths are only defined up to the foldername
    of the test, because we can internally decide on the
    filenames of logfiles and generated layers (these
    paths are not up to the user)
    """
    def __init__(self, base):
        # Files
        self.base = os.path.join(base, "Output")
        self.sqlite_tests = os.path.join(self.base, 'Sqlite_checks')
        self.bank_levels = os.path.join(self.base, 'Bank_levels')
        self.zero_d_one_d = os.path.join(self.base, '0d1d_tests')
        self.one_d_two_d = os.path.join(self.base, '1d2d_tests')

class ModelbuilderPaths:
    def __init__(self, base):
        # Files
        self.base = os.path.join(base, "modelbuilder_output")
        self.channel_from_profiles = os.path.join(
            self.base, channel_from_profiles)

class SourcePaths:
    """
    Paths to source data (datachecker, DAMO, HDB)
    """
    def __init__(self, base):
        # Files
        self.base = os.path.join(base, "01_Source_data")
        self.damo = os.path.join(self.base, damo)
        self.hdb = os.path.join(self.base, hdb)
        self.datachecker = os.path.join(self.base, datachecker)
        self.polder_polygon = os.path.join(self.base, polder_poly)
        # Folders
        self.modelbuilder = ModelbuilderPaths(self.base)

class ZeroDOneD:
    def __init__(self, base):
        self.base = os.path.join(base, "0d1d_results")

class OneDTwoD:
    def __init__(self, base):
        self.base = os.path.join(base, "1d2d_results")

class ThreediResults:
    """
    Folder in which 3di results are saved
    """
    def __init__(self, base):
        self.base = os.path.join(base, "03_3di_results")
        self.zeroDoneD = ZeroDOneD(self.base)
        self.oneDtwoD = OneDTwoD(self.base)

class DefaultPaths:
    """
    Init function expects a path to the main polder file (for example:
    C:\Poldermodellen\Heiloo

    The rest of the folder is setup up according to the following structure:

    Main Polder Folder
        ├── 01.Source_data
        │ ├── DAMO.gdb
        │ ├── HDB.gdb
        │ ├── datachecker_output.gdb
        │ └── modelbuilder_output
        │     └── preprocessed
        ├── 02.Model
        │ ├── rasters (include DEM)
        │ └── * model(.sqlite) *
        ├── 03.3di_results
        │ ├── 0d1d_results
        │ └── 1d2d_results
        └── Output
            ├── 0d1d_tests
            │   ├── *some revision*
            │     ├── Layers
            │     └── Logs
            ├── 1d2d_tests
            │ └── *some revision*
            │     ├── Layers
            │     └── Logs
            ├── Bank_levels
            │ └── bank_levels
            │     ├── Layers
            │     └── Logs
            └── Sqlite_checks
                ├── Layers
                └── Log
    """
    def __init__(self, base):
        self.base = base
        # Results of tests
        self.output = OutputPaths(self.base)
        # Source files
        self.model = ModelPaths(self.base)
        # Threedi results
        self.threedi_results = ThreediResults(self.base)
        self.source_data = SourcePaths(self.base)
