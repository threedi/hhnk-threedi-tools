# -*- coding: utf-8 -*-
"""
Created on Fri Aug 13 13:18:54 2021

@author: chris.kerklaan

Each class has a file as its attributes and a file as a new class. 
self.base is the directory in which it is located

"""
# First-party imports
import os
from pathlib import Path

# Third-party imports
from threedigrid.admin.gridadmin import GridH5Admin
from threedigrid.admin.gridresultadmin import GridH5ResultAdmin


from hhnk_research_tools.variables import (
    file_types_dict,
    GDB,
    SHAPE,
    SQLITE,
    TIF,
    NC,
    H5,
)

# Local imports
from hhnk_threedi_tools.variables.damo_hdb_datachecker_variables import (
    damo_duiker_sifon_hevel,
    damo_waterdeel,
    datachecker_culvert_layer,
    datachecker_fixed_drainage,
    hdb_sturing_3di,
    waterlevel_val_field,
)
from hhnk_threedi_tools.variables.model_state import (
    hydraulic_test_state,
    one_d_two_d_state,
    undefined_state,
    invalid_path,
)

from hhnk_threedi_tools.core.checks.model_state import (
    detect_model_states,
    get_proposed_adjustments_weir_width,
    get_proposed_updates_manholes,
    get_proposed_adjustments_global_settings,
    get_proposed_adjustments_channels,
)


# Globals
# Set names for certain input files
DAMO = f"DAMO{file_types_dict[GDB]}"
HDB = f"HDB{file_types_dict[GDB]}"
DATACHECKER = f"datachecker_output{file_types_dict[GDB]}"
POLDER_POLY = f"polder_polygon{file_types_dict[SHAPE]}"
CHANNEL_FROM_PROFILES = f"channel_surface_from_profiles{file_types_dict[SHAPE]}"
POLDER_STRUCTURE = """

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
        | |__ batch_results
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


def create_tif_path(folder, filename):
    """
    Takes a folder name (ex: C:../output/Layers) and base filename (ex: raster) as arguments
    and returns full path (ex: C:../output/Layers/raster.tif)
    """
    try:
        full_path = os.path.join(folder, filename + file_types_dict[TIF])
        return full_path
    except Exception as e:
        raise e from None


def get_top_level_directories(folder, condition_test=None):
    """
    Resturns a list of all top level directories, can be filtered with a function (condition_test)
    that returns a bool and takes one argument (directory)
    """
    return [
        item
        for item in (os.path.join(folder, d1) for d1 in os.listdir(folder))
        if os.path.isdir(item)
        and (condition_test(item) if condition_test is not None else True)
    ]


def if_exists(path):
    return path if os.path.exists(path) else None


def add_log_layer_path(files_dict, base_path):
    # Creates log and layer folders in test specific output folder
    # ex: output/sqlite_tests/Logs and output/sqlite_tests/Layers
    return files_dict


class Folder:
    """main folder class to be inherited by all the other classes"""

    def __init__(self, base):
        self.base = base
        self.base_path = Path(base)

    @property
    def content(self):
        return os.listdir(self.base)

    @property
    def path(self):
        return self.base

    @property
    def name(self):
        return self.base_path.stem

    @property
    def folder(self):
        return os.path.basename(self.base)

    def __str__(self):
        return self.base

    def __repr__(self):
        return self.folder + " @ " + self.path


# source files
class ModelbuilderPaths(Folder):
    def __init__(self, base):
        super().__init__(os.path.join(base, "modelbuilder_output"))
        self.channel_from_profiles = os.path.join(self.base, CHANNEL_FROM_PROFILES)


class SourcePaths(Folder):
    """
    Paths to source data (datachecker, DAMO, HDB)
    """

    def __init__(self, base):
        super().__init__(os.path.join(base, "01_Source_data"))

        self.damo = os.path.join(self.base, DAMO)
        self.hdb = os.path.join(self.base, HDB)
        self.datachecker = os.path.join(self.base, DATACHECKER)
        self.polder_polygon = os.path.join(self.base, POLDER_POLY)

        # Folders
        self.modelbuilder = ModelbuilderPaths(self.base)

        # layers
        self.datachecker_fixed_drainage = datachecker_fixed_drainage
        self.datachecker_culvert_layer = datachecker_culvert_layer
        self.hdb_sturing_3di_layer = hdb_sturing_3di
        self.damo_duiker_sifon_layer = damo_duiker_sifon_hevel
        self.damo_waterdeel_layer = damo_waterdeel
        self.init_waterlevel_val_field = waterlevel_val_field
        self.init_water_level_filename = "initieel_water_level"
        self.dewatering_filename = "drooglegging"


# model
class RasterPaths(Folder):
    def __init__(self, base):
        super().__init__(os.path.join(base, "rasters"))
        self.dem = self.find_dem()
        self.paths = [self.base]

    def exists(self):
        return all([os.path.exists(p) for p in self.paths])

    def find_dem(self):
        """
        Look for file starting with dem_ and ending with extension .tif in given directory

        Returns path if found, empty string if not found
        """
        if not os.path.exists(self.base):
            return ""
        else:
            p = Path(self.base)
            dir_list = [
                item
                for item in p.iterdir()
                if item.suffix == file_types_dict[TIF] and item.stem.startswith("dem_")
            ]
            if len(dir_list) == 1:
                return os.path.join(self.base, dir_list[0].name)
            else:
                return ""


class ModelPaths(Folder):
    def __init__(self, base):
        super().__init__(os.path.join(base, "02_Model"))

        self.database = self.find_model()
        # Folders
        self.rasters = RasterPaths(self.base)

    @property
    def state(self):
        return detect_model_states(self.database)

    @property
    def database_path(self):
        return str(self.database)

    def states(self):
        return [hydraulic_test_state, one_d_two_d_state, undefined_state, invalid_path]

    def proposed_adjustments(self, table, to_state):
        """
        returns proposed adjustments for parts of the model
        params:
            table: can either be:
                'global_settings',
                'weirs',
                'manholes',
                'channels'
            to_state:
                'Hydraulische toets'
                '1d2d toets'
                'Niet gedefinieerd/uit modelbuilder'
                'Ongeldig pad/niet geselecteerd'
        """
        if table == "global_settings":
            return get_proposed_adjustments_global_settings(
                self.database_path, to_state
            )

        if table == "weirs":
            return get_proposed_adjustments_weir_width(
                self.database_path, self.state, to_state
            )
        if table == "manholes":
            return get_proposed_updates_manholes(
                self.database_path, self.state, to_state
            )
        if table == "channels":
            return get_proposed_adjustments_channels(
                self.database_path, self.state, to_state
            )

    def find_model(self):
        """
        Tries to find database in given path by looking for extension.

        Returns path if only one file with .sqlite extension is found. Returns empty string
        in other cases (none found or more than one found)
        """
        if not os.path.exists(self.base):
            return ""
        else:
            sqlite_files = [
                item
                for item in os.listdir(self.base)
                if item.endswith(file_types_dict[SQLITE])
            ]
            # If one is found, we are going to assume that is the one we want to use
            if len(sqlite_files) == 1:
                return os.path.join(self.base, sqlite_files[0])
            else:
                # Couldn't detect automatically
                return ""


# threedi results
class ThreediResults(Folder):
    def __init__(self, base):
        super().__init__(base)
        self.grid_path = os.path.join(self.base, "results_3di.nc")
        self.admin_path = os.path.join(self.base, "gridadmin.h5")

    @property
    def grid(self):
        return GridH5ResultAdmin(self.admin_path, self.grid_path)

    @property
    def admin(self):
        return GridH5Admin(self.admin_path)


class ThreediRevisions(Folder):
    def __init__(self, base, folder):
        super().__init__(os.path.join(base, folder))

    def __getitem__(self, revision):
        """revision can be a integer or a path"""
        if type(revision) == int:
            return ThreediResults(os.path.join(self.base, self.revisions[revision]))
        elif os.path.exists(revision):
            return ThreediResults(revision)

    @property
    def revisions(self):
        return self.content


class ZeroDOneD(ThreediRevisions):
    def __init__(self, base):
        super().__init__(base, "0d1d_results")


class OneDTwoD(ThreediRevisions):
    def __init__(self, base):
        super().__init__(base, "1d2d_results")


class ClimateResults(ThreediRevisions):
    def __init__(self, base):
        super().__init__(base, "batch_results")


class ThreediResultsPaths(Folder):
    """
    Folder in which 3di results are saved
    """

    def __init__(self, base):
        super().__init__(os.path.join(base, "03_3di_resultaten"))
        self.zero_d_one_d = ZeroDOneD(self.base)
        self.one_d_two_d = OneDTwoD(self.base)
        self.climate_results = ClimateResults(self.base)

    @property
    def climate(self):
        # makes more sense than climate_results
        return self.climate_results

    @property
    def batch(self):
        # makes more sense than climate_results
        return self.climate_results

    def find_revision(self, results_path, revision_dir):
        return ThreediResults(os.path.join(results_path, revision_dir))

    def find(self, results_path=None, revision_dir=None, revision_path=None):
        """
        Builds a dictionary containing paths to files pertaining to 3di results.
        Note that you must either provide a revision_path or a results_path combined with a revision dir

            build_threedi_source_paths_dict(
                    results_path -> None (full path to main results folder (ex: C:/.../0d1d_tests))
                    revision_dir -> None (name of revision folder (ex: heiloo_#13_1d2d_test))
                    revision_path -> None (full path to revision (ex: C:/.../0d1d_tests/heiloo_#13_1d2d_test))

                    Provide EITHER revision_path or results_path AND revision_dir
                )

        returns dictionary containing paths to .h5 ('h5_file') and .nc files ('nc_file')
        If there are multiple files with those extensions, it will choose the last instance
        """
        try:
            results_dict = {}
            if (not revision_path and not (results_path and revision_dir)) or (
                revision_path and (results_path or revision_dir)
            ):
                raise Exception(
                    "Provide either revision_path or results_path and revision_dir"
                )
            if revision_path is None:
                path = os.path.join(results_path, revision_dir)
            else:
                path = revision_path
            for item in os.listdir(path):
                if item.endswith(file_types_dict[NC]):
                    results_dict["nc_file"] = os.path.join(path, item)
                if item.endswith(file_types_dict[H5]):
                    results_dict["h5_file"] = os.path.join(path, item)

            results_dict["grid"] = ThreediResults
            return results_dict
        except Exception as e:
            raise e from None


# Output folders
class Layers(Folder):
    def __init__(self, base):
        super().__init__(base)


class Logs(Folder):
    def __init__(self, base):
        super().__init__(base)


class OutputFolder(Folder):
    def __init__(self, base):
        super().__init__(base)
        self.layers = Layers(os.path.join(self.base, "Layers"))
        self.logs = Logs(os.path.join(self.base, "Logs"))


class OutputRevisions(Folder):
    def __init__(self, base):
        super().__init__(base)

    def __getitem__(self, revision):
        if type(revision) == int:
            return OutputFolder(os.path.join(self.base, self.revisions[revision]))
        else:
            return OutputFolder(revision)

    @property
    def revisions(self):
        return os.listdir(self.base)


class OutputPaths(Folder):
    """
    Output paths are only defined up to the foldername
    of the test, because we can internally decide on the
    filenames of logfiles and generated layers (these
    paths are not up to the user)
    """

    def __init__(self, base):
        super().__init__(os.path.join(base, "Output"))

        self.sqlite_tests = OutputFolder(os.path.join(self.base, "Sqlite_tests"))
        self.bank_levels = OutputRevisions(os.path.join(self.base, "Bank_levels"))
        self.zero_d_one_d = OutputRevisions(os.path.join(self.base, "0d1d_tests"))
        self.one_d_two_d = OutputRevisions(os.path.join(self.base, "1d2d_tests"))


class Folders(Folder):
    f"""
    Class to read and create folders structures...
    Init function expects a path to the main polder file (for example:
    C:\Poldermodellen\Heiloo

    The rest of the folder is setup up according to the following structure:

    {POLDER_STRUCTURE}

    """

    def __init__(self, base, create=True):
        super().__init__(base)

        # source
        self.source_data = SourcePaths(self.base)

        # model files
        self.model = ModelPaths(self.base)

        # Threedi results
        self.threedi_results = ThreediResultsPaths(self.base)

        # Results of tests
        self.output = OutputPaths(self.base)

        if create and not os.path.exists(self.base):
            self.create_project()

    @classmethod
    def from_model_path(cls, model_path):
        return cls(str(Path(model_path).parents[0].parents[0]))

    @property
    def structure(self):
        return print(POLDER_STRUCTURE)

    def to_dict(self):
        """
        Creates a dictionary containing all folder paths that need to be made when creating
        a new project

        Input: polder (project) path in which to create the structure
        """
        return {
            "model_folder": str(self.model),
            "threedi_results_folder": str(self.threedi_results),
            "threedi_0d1d_results_folder": str(self.threedi_results.zero_d_one_d),
            "threedi_1d2d_results_folder": str(self.threedi_results.one_d_two_d),
            "threedi_climate_results_folder": str(self.threedi_results.climate_results),
            "source_data_folder": str(self.source_data),
            "output_folder": str(self.output),
        }

    def to_file_dict(self):
        """
        Returns dictionary containing paths to source files according to set project structure.

            build_base_paths_dict(
                    polder_path (string: path to project folder (highest level))
                )
        """
        return {
            "datachecker": if_exists(self.source_data.datachecker),
            "damo": if_exists(self.source_data.damo),
            "hdb": if_exists(self.source_data.hdb),
            "polder_shapefile": if_exists(self.source_data.polder_polygon),
            "channels_shapefile": if_exists(
                self.source_data.modelbuilder.channel_from_profiles
            ),
            # Layer names source data
            "damo_duiker_sifon_layer": damo_duiker_sifon_hevel,
            "damo_waterdeel_layer": damo_waterdeel,
            "datachecker_culvert_layer": datachecker_culvert_layer,
            "datachecker_fixed_drainage": datachecker_fixed_drainage,
            "hdb_sturing_3di_layer": hdb_sturing_3di,
            "init_waterlevel_val_field": waterlevel_val_field,
            # model folder
            "model": if_exists(self.model.database),
            "dem": if_exists(self.model.rasters.dem),
            # Threedi
            "0d1d_results_dir": if_exists(str(self.threedi_results.zero_d_one_d)),
            "1d2d_results_dir": if_exists(str(self.threedi_results.one_d_two_d)),
            "climate_results_dir": if_exists(str(self.threedi_results.climate_results)),
            # Default output folders
            "base_output": if_exists(str(self.output)),
            "sqlite_tests_output": if_exists(str(self.output.sqlite_tests)),
            "0d1d_output": if_exists(str(self.output.zero_d_one_d)),
            "bank_levels_output": if_exists(str(self.output.bank_levels)),
            "1d2d_output": if_exists(str(self.output.one_d_two_d)),
            "polder_folder": if_exists(str(self.base)),
        }

    def to_test_file_dict(self, test_type, revision_dir_name=None):
        """
        Creates a dict containing the file paths (without extensions) we will
        use to write the output to files
        types:
        1 -> sqlite tests
        2 -> 0d1d tests
        3 -> bank levels
        4 -> 1d2d tests
        Base folder is the highest folder in the hierarchy specific to
        a types of test (output/sqlite_tests or output/0d1d_tests for example)

        """
        files_dict = {}
        if test_type in (2, 4) and revision_dir_name:
            # If 3di revisions are involved, we add the revisions name to the output path
            # ex: output/0d1d_tests/{polder name}_#{revision number}_{test type}
            output_revision_dir = revision_dir_name.replace(" ", "_")
            files_dict["output"] = os.path.join(self.base, output_revision_dir)
        else:
            files_dict["output"] = self.base

        files_dict["log_path"] = os.path.join(self.base, "Logs")
        files_dict["layer_path"] = os.path.join(self.base, "Layers")

        if test_type == 1:
            files_dict["impervious_surface_filename"] = "ondoorlatend_oppervlak"
            files_dict["profiles_used_filename"] = "gebruikte_profielen"
            files_dict["controlled_structs_filename"] = "gestuurde_kunstwerken"
            files_dict["weir_heights_filename"] = "bodemhoogte_stuw"
            files_dict["geometry_filename"] = "geometrie"
            files_dict["structs_channel_filename"] = "bodemhoogte_kunstwerken"
            files_dict["general_checks_filename"] = "algemene_tests"
            files_dict["isolated_channels_filename"] = "geisoleerde_watergangen"
            files_dict["init_water_level_filename"] = "initieel_water_level"
            files_dict["dewatering_filename"] = "drooglegging"
            files_dict["water_surface_filename"] = "oppervlaktewater"

        if test_type == 2:
            files_dict["zero_d_one_d_filename"] = "0d1d_toetsing"
            files_dict["hyd_test_channels_filename"] = "hydraulische_toets_watergangen"
            files_dict["hyd_test_structs_filename"] = "hydraulische_toets_kunstwerken"

        if test_type == 3:
            files_dict["flow_1d2d_flowlines_filename"] = "stroming_1d2d_flowlines"
            files_dict[
                "flow_1d2d_cross_sections_filename"
            ] = "stroming_1d2d_cross_sections"
            files_dict["flow_1d2d_channels_filename"] = "stroming_1d2d_watergangen"
            files_dict["flow_1d2d_manholes_filename"] = "stroming_1d2d_putten"

        if test_type == 4:
            files_dict["grid_nodes_2d_filename"] = "grid_nodes_2d"
            files_dict["1d2d_all_flowlines_filename"] = "1d2d_alle_stroming"
            # The actual filename depends on the time steps we are looking at in the test
            # Therefore we create a template rather than a set name
            files_dict["water_level_filename_template"] = "waterstand_T{}_uur"
            files_dict["water_depth_filename_template"] = "waterdiepte_T{}_uur"

        return files_dict

    def create_project(self):

        """
        Takes a base path as input (ex: c:/..../project_name) and creates default project structure in it.
        """
        try:
            fdict = self.to_dict()
            for item in fdict.values():
                os.makedirs(item, exist_ok=True)
            expected_source_files = (
                "Expected files are:\n\n"
                "Damo geodatabase (*.gdb) named 'DAMO.gdb'\n"
                "Datachecker geodatabase (*.gdb) named 'datachecker_output.gdb'\n"
                "Hdb geodatabase (*.gdb) named 'HDB.gdb'\n"
                "Folder named 'modelbuilder_output' and polder shapefile "
                "(*.shp and associated file formats)"
            )
            with open(
                os.path.join(fdict["source_data_folder"], "read_me.txt"), mode="w"
            ) as f:
                f.write(expected_source_files)
            expected_model_files = (
                "Expected files are:\n\n"
                "Sqlite database (model): *.sqlite\n"
                "Folder named 'rasters' containing DEM raster (*.tif) and other rasters\n"
            )
            with open(
                os.path.join(fdict["model_folder"], "read_me.txt"), mode="w"
            ) as f:
                f.write(expected_model_files)
            expected_threedi_files = (
                "Expected files are:\n\n"
                "Both sub folders in this folder expect to contain folders corresponding to "
                "3di results from different revisions (e.g. containing *.nc, *.h5 and *.sqlite file)"
            )
            with open(
                os.path.join(fdict["threedi_results_folder"], "read_me.txt"), mode="w"
            ) as f:
                f.write(expected_threedi_files)
            output_folder_explanation = (
                "This folder is the default folder where the HHNK toolbox "
                "saves results of tests. The inner structure of these result folders "
                "is automatically generated"
            )
            with open(
                os.path.join(fdict["output_folder"], "read_me.txt"), mode="w"
            ) as f:
                f.write(output_folder_explanation)
        except Exception as e:
            raise e from None
