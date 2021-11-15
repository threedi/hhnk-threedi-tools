# -*- coding: utf-8 -*-
"""
Created on Fri Aug 13 13:18:54 2021

@author: chris.kerklaan

Each class has a file as its attributes and a file as a new class. 
self.base is the directory in which it is located

"""
# First-party imports
import os
import glob
from pathlib import Path

# Third-party imports
from threedigrid.admin.gridadmin import GridH5Admin
from threedigrid.admin.gridresultadmin import GridH5ResultAdmin

import hhnk_research_tools as hrt

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

from hhnk_threedi_tools.variables.api_settings import (
    RAIN_SCENARIOS,
    GROUNDWATER,
    RAIN_TYPES,
    RAW_DOWNLOADS,
)

import fiona
import geopandas as gpd

# Globals
DAMO = f"DAMO{file_types_dict[GDB]}"
HDB = f"HDB{file_types_dict[GDB]}"
POLDER_POLY = f"polder_polygon{file_types_dict[SHAPE]}"
CHANNEL_FROM_PROFILES = f"channel_surface_from_profiles{file_types_dict[SHAPE]}"

DAMO_DUIKER_SIFON_HEVEL = "DuikerSifonHevel"
DAMO_WATERDEEL = "waterdeel"
DATACHECKER_CULVERT = "culvert"
DATACHECKER_FIXED_DRAINAGE = "fixeddrainagelevelarea"
HDB_STURING_3DI = "Sturing_3Di"
WATERLEVEL_VAL_FIELD = "streefpeil_bwn2"

POLDER_STRUCTURE = """
    Main Polder object
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


class File:
    def __init__(self, file_path):
        self.file_path = file_path
        self.pl = Path(file_path)

    @property
    def exists(self):
        return self.pl.exists()

    @property
    def name(self):
        return self.pl.stem

    @property
    def path(self):
        return self.file_path

    def __str__(self):
        return self.file_path

    def __repr__(self):
        if self.exists:
            exists = "exists"
        else:
            exists = "doesn't exist"

        return f"{self.name} @ {self.file_path} ({exists})"


class FileGDB(File):
    def __init__(self, file_path):
        super().__init__(file_path)
    
    def load(self, layer=None):
        if layer==None:
            layer=input('Select layer:')
        return gpd.read_file(self.file_path, layer=layer)

    def layers(self):
        """Return available layers in file gdb"""
        return fiona.listlayers(self.file_path)


class Raster(File):
    def __init__(self, raster_path):
        super().__init__(raster_path)

    def load(self, return_array=True):
        if self.exists:
            self.array, self.nodata, self.metadata = hrt.load_gdal_raster(
                raster_source=self.file_path, return_array=return_array
            )
            return self.array, self.nodata, self.metadata
        else:
            print("Doesn't exist")


class Folder:
    """Base folder class for creating, deleting and see if folder exists"""

    def __init__(self, base):
        self.base = base
        self.pl = Path(base)  # pathlib path

        self.files = {}
        self.olayers = {}
        self.space = "\t\t\t\t"

    @property
    def structure(self):
        return ""

    @property
    def content(self):
        return os.listdir(self.base)

    @property
    def path(self):
        return self.base

    @property
    def name(self):
        return self.pl.stem

    @property
    def folder(self):
        return os.path.basename(self.base)

    @property
    def exists(self):
        return self.pl.exists()

    @property
    def show(self):
        print(self.__repr__())

    def create(self, parents=True):
        """Create folder, if parents==False path wont be 
        created if parent doesnt exist."""
        if parents==False:
            if not self.pl.parent.exists():
                print(f'{self.path} not created, parent doesnt exist.')
                return
        self.pl.mkdir(parents=parents, exist_ok=True)
    
    def find_ext(self, ext):
        """ finds files with a certain extension"""
        return glob.glob(self.base +f'/*.{ext}')

    def full_path(self, name):
        """returns the full path of a file or a folder when only a name is known"""
        return str(self.pl / name)

    def add_file(self, objectname, filename, ftype="file"):
        if ftype == "file":
            new_file = File(self.full_path(filename))
        elif ftype == "filegdb":
            new_file = FileGDB(self.full_path(filename))
        elif ftype == "raster":
            new_file = Raster(self.full_path(filename))

        self.files[objectname] = new_file
        setattr(self, objectname, new_file)

    def add_layer(self, objectname, layer):
        self.olayers[objectname] = layer
        setattr(self, objectname, layer)

    def __str__(self):
        return self.base

    def __repr__(self):
        return f"""{self.name} @ {self.path}
                    Folders:\t{self.structure}
                    Files:\t{list(self.files.keys())}
                    Layers:\t{list(self.olayers.keys())}
                """


class Folders(Folder):
    __doc__ = f"""
        
        --------------------------------------------------------------------------
        An object to ease the accessibility, creation and checks of folders and
        files in the polder structure.
        
        Usage as follows:
            - Access class with te path to the main folder (e.g., C:\Poldermodellen\Heiloo)
            - Find your way through by using folder.show
            - Check if a file or folder exists using .exists
            - Create a non-existing revision folder using folder.threedi_results.batch['new_folder'].create()
            - Show all (needed) files using .files
            - Show all (needed) layers using .layers
            - Return a path of a file using either str() or .path
        
        Example code:
            folder = Folders('C:/Poldermodellen/Heiloo')
            
            folder.show
           
            Output: 
                Heiloo @ C:/Poldermodellen/Heiloo
                                Folders:	  
                           				Folders
                           				├── source_data
                           				├── model
                           				├── threedi_results
                           				└── output
                           
                                Files:	[]
                                Layers:	[]
        
            
            folder.source_data.show
            
            Output: 
            
                01_Source_data @ C:/Poldermodellen/Heiloo/01_Source_data
                                    Folders:	  
                               				source_data
                               				└── modelbuilder
                               
                                    Files:	['damo', 'hdb', 'datachecker', ...]
                                    Layers:	['datachecker_fixed_drainage', ...]
            
            
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

        if create and not self.exists:
            self.create_project()
            
        

    @property
    def structure(self):
        return f"""  
               {self.space}Folders
               {self.space}├── source_data
               {self.space}├── model
               {self.space}├── threedi_results
               {self.space}└── output
               """

    @classmethod
    def from_model_path(cls, model_path):
        return cls(str(Path(model_path).parents[0].parents[0]))

    @property
    def full_structure(self):
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
            "output_sqlite_tests_folder": str(self.output.sqlite_tests),
            "output_bank_levels_folder": str(self.output.bank_levels),
            "output_zero_d_one_d_folder": str(self.output.zero_d_one_d),
            "output_one_d_two_d_folder": str(self.output.one_d_two_d),
        }

    def to_file_dict(self):
        """
        Returns dictionary containing paths to source files according to set project structure.

            build_base_paths_dict(
                    polder_path (string: path to project folder (highest level))
                )
        """
        return {
            "datachecker": if_exists(self.source_data.datachecker.path),
            "damo": if_exists(self.source_data.damo.path),
            "hdb": if_exists(self.source_data.hdb.path),
            "polder_shapefile": if_exists(self.source_data.polder_polygon.path),
            "channels_shapefile": if_exists(
                self.source_data.modelbuilder.channel_from_profiles.path
            ),
            # Layer names source data
            "damo_duiker_sifon_layer": DAMO_DUIKER_SIFON_HEVEL,
            "damo_waterdeel_layer": DAMO_WATERDEEL,
            "datachecker_culvert_layer": DATACHECKER_CULVERT,
            "datachecker_fixed_drainage": DATACHECKER_FIXED_DRAINAGE,
            "hdb_sturing_3di_layer": HDB_STURING_3DI,
            "init_waterlevel_val_field": WATERLEVEL_VAL_FIELD,
            # model folder
            "model": if_exists(self.model.database.path),
            "dem": if_exists(self.model.rasters.dem.path),
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


class SourcePaths(Folder):
    """
    Paths to source data (datachecker, DAMO, HDB)
    """

    def __init__(self, base):
        super().__init__(os.path.join(base, "01_Source_data"))

        # Folders
        self.modelbuilder = ModelbuilderPaths(self.base)
        self.peilgebieden = PeilgebiedenPaths(self.base)

        # Files
        self.add_file("damo", DAMO, ftype='filegdb')
        self.add_file("hdb", HDB, ftype='filegdb')
        self.add_file("datachecker", 'datachecker_output.gdb', ftype='filegdb')
        self.add_file("polder_polygon", POLDER_POLY)

        # Layers
        self.add_layer("datachecker_fixed_drainage", "fixeddrainagelevelarea")
        self.add_layer("datachecker_culvert", "culvert")
        self.add_layer("hdb_sturing_3di_layer", "Sturing_3Di")
        self.add_layer("damo_duiker_sifon_layer", "DuikerSifonHevel")
        self.add_layer("damo_waterdeel_layer", "waterdeel")
        self.add_layer("init_waterlevel_val_field", "streefpeil_bwn2")
        self.add_layer("init_water_level_filename", "initieel_water_level")
        self.add_layer("dewatering_filename", "drooglegging")

    @property
    def structure(self):
        return f"""  
               {self.space}source_data
               {self.space}└── modelbuilder
               {self.space}└── peilgebieden
               """


class ModelbuilderPaths(Folder):
    def __init__(self, base):
        super().__init__(os.path.join(base, "modelbuilder_output"))
        self.add_file("channel_from_profiles", CHANNEL_FROM_PROFILES)


class PeilgebiedenPaths(Folder):
    #TODO deze map moet een andere naam en plek krijgen. 
    def __init__(self, base):
        super().__init__(os.path.join(base, "peilgebieden"))

        #Find peilgebieden shapefile in folder. 
        if self.exists:
            shape_name =  [x for x in self.content if x.startswith('peilgebieden') and x.endswith('.shp')]
            if len(shape_name)==1:
                self.add_file("peilgebieden", shape_name[0])
            else:
                self.add_file("peilgebieden", 'peilgebieden.shp')


class ModelPaths(Folder):
    def __init__(self, base):
        super().__init__(os.path.join(base, "02_Model"))

        # Folders
        self.rasters = RasterPaths(self.base)

        # File
        if self.model_path() is not None:
            self.add_file("database", self.model_path())

    @property
    def structure(self):
        return f"""  
               {self.space}model
               {self.space}└── rasters
               """

    @property
    def state(self):
        return detect_model_states(self.database.path)

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
    
    @property
    def sqlite_paths(self):
        """  returns all sqlites in folder"""
        return self.find_ext("sqlite")
    
    @property
    def sqlite_names(self):
        """  returns all sqlites in folder"""
        return [Path(sp).stem for sp in self.sqlite_paths]
    
    def model_path(self, idx=0, name=None):
        """ finds a model using an index"""
        if name:
            try:
                idx = self.sqlite_names.index(name)
            except Exception:
                raise ValueError('name of sqlite given, but cannot be found')
        if len(self.sqlite_paths) >= 1:
            return self.sqlite_paths[idx]
        else:
            return None


    def set_database(self, name_or_idx):
        """  set the model database with either an index or a name"""
        if type(name_or_idx) == str:    
            self.add_file("database",  self.model_path(None, name_or_idx))
        else:
            self.add_file("database",  self.model_path(name_or_idx, None))
            

class RasterPaths(Folder):
    def __init__(self, base):
        super().__init__(os.path.join(base, "rasters"))

        # Files
        self.add_file("dem", self.find_dem(), "raster")

    @property
    def structure(self):
        return None

    def find_dem(self):
        """
        Look for file starting with dem_ and ending with extension .tif in given directory

        Returns path if found, empty string if not found
        """
        if not self.exists:
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


class ThreediResultsPaths(Folder):
    """
    Folder in which 3di results are saved
    """

    def __init__(self, base):
        super().__init__(os.path.join(base, "03_3di_resultaten"))

        # Folders
        self.zero_d_one_d = ZeroDOneD(self.base)
        self.one_d_two_d = OneDTwoD(self.base)
        self.climate_results = ClimateResults(self.base)

    @property
    def structure(self):
        return f"""  
               {self.space}threedi_results
               {self.space}├── zero_d_one_d
               {self.space}├── one_d_two_d
               {self.space}└── climate_results or batch or climate
               """

    def __getitem__(self, name):
        if name == "0d1d_results":
            return self.zero_d_one_d
        elif name == "1d2d_results":
            return self.one_d_two_d
        elif name in ["batch_results", "climate_results", "climate"]:
            return self.climate_results

    @property
    def climate(self):
        # makes more sense than climate_results
        return self.climate_results

    @property
    def batch(self):
        # makes more sense than climate_results
        return self.climate_results

    def find_revision(self, results_path, revision_dir):
        return ThreediResult(os.path.join(results_path, revision_dir))

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

            results_dict["grid"] = ThreediResult
            return results_dict
        except Exception as e:
            raise e from None


class ThreediRevisions(Folder):
    def __init__(self, base, folder):
        super().__init__(os.path.join(base, folder))

    def __getitem__(self, revision):
        """revision can be a integer or a path"""
        if type(revision) == int:
            return ThreediResult(self.full_path(self.revisions[revision]))
        elif os.path.exists(revision):
            return ThreediResult(revision)
        elif (self.pl / revision).exists():
            return ThreediResult(self.full_path(revision))
        else:
            print("path not found, create with '.create()'")
            return ThreediResult(self.full_path(revision))

    def revision_structure(self, name):
        spacing = "\n\t\t\t\t\t\t\t"
        structure = f""" {spacing}{name} """
        for i, rev in enumerate(self.revisions):
            if i == len(self.revisions) - 1:
                structure = structure + f"{spacing}└── {rev}"
            else:
                structure = structure + f"{spacing}├── {rev}"

        return structure

    @property
    def revisions(self):
        return self.content


class ZeroDOneD(ThreediRevisions):
    def __init__(self, base):
        super().__init__(base, "0d1d_results")

    @property
    def structure(self):
        return self.revision_structure("zero_d_one_d")


class OneDTwoD(ThreediRevisions):
    def __init__(self, base):
        super().__init__(base, "1d2d_results")

    @property
    def structure(self):
        return self.revision_structure("one_d_two_d")


class ClimateResultsRevisions(Folder):
    def __init__(self, base, folder):
        super().__init__(os.path.join(base, folder))

    def __getitem__(self, revision):
        """revision can be a integer or a path"""
        if type(revision) == int:
            return ClimateResult(self.full_path(self.revisions[revision]))
        elif os.path.exists(revision):
            return ClimateResult(revision)
        elif (self.pl / revision).exists():
            return ClimateResult(self.full_path(revision))
        else:
            print("path not found, create with '.create()'")
            return ClimateResult(self.full_path(revision))

    def revision_structure(self, name):
        spacing = "\n\t\t\t\t\t\t\t"
        structure = f""" {spacing}{name} """
        for i, rev in enumerate(self.revisions):
            if i == len(self.revisions) - 1:
                structure = structure + f"{spacing}└── {rev}"
            else:
                structure = structure + f"{spacing}├── {rev}"

        return structure

    @property
    def revisions(self):
        return self.content


class ClimateResults(ClimateResultsRevisions):
    def __init__(self, base):
        super().__init__(base, "batch_results")
        self.create(parents=False) #create outputfolder if parent exists

    @property
    def structure(self):
        return self.revision_structure("climate_results")


class ClimateResult(Folder):
    """Individual result with download and output folder"""
    def __init__(self, base):
        super().__init__(base)

        self.downloads = ClimateResultDownloads(self.base)
        self.output = ClimateResultOutput(self.base)

    @property
    def structure(self):
        return f"""  
               {self.space}{self.name}
               {self.space}├── downloads
               {self.space}└── output
                """


class ThreediResult(Folder):
    def __init__(self, base):
        super().__init__(base)

        # Files
        self.add_file("grid_path", "results_3di.nc")
        self.add_file("admin_path", "gridadmin.h5")

    @property
    def grid(self):
        return GridH5ResultAdmin(self.admin_path.file_path, self.grid_path.file_path)

    @property
    def admin(self):
        return GridH5Admin(self.admin_path.file_path)


class ClimateResultOutput(Folder):
    def __init__(self, base):
        super().__init__(base + "/02_output_rasters")

        # Folders
        self.temp = ClimateResultOutputTemp(self.base)

        # Files
        self.add_file("maskerkaart", "maskerkaart.shp")
        self.add_file("maskerkaart_diepte_tif", "maskerkaart_diepte.tif", "raster")
        self.add_file("maskerkaart_schade_tif", "maskerkaart_schade.tif", "raster")
        self.add_file("geen_schade_tif", "geen_schade.tif", "raster")
        self.add_file("mask_diepte_plas", "mask_diepte_plas.tif", "raster")
        self.add_file("mask_schade_plas", "mask_schade_plas.tif", "raster")
        self.add_file("mask_diepte_overlast", "mask_diepte_overlast.tif", "raster")
        self.add_file("mask_schade_overlast", "mask_schade_overlast.tif", "raster")
        self.add_file("ruimtekaart", "ruimtekaart.shp")
        self.add_file("schade_peilgebied", "schade_per_peilgebied.shp")
        self.add_file("schade_peilgebied_corr", "schade_per_peilgebied_correctie.shp")
        self.add_file("schade_polder", "schade_per_polder.csv")
        self.add_file("schade_polder_corr", "schade_per_polder_correctie.csv")


        self.set_scenario_files()
        self.create(parents=False) #create outputfolder if parent exists


    def set_scenario_files(self):
        for type_raster, type_raster_name in zip(
            ["depth", "damage"], ["inundatiediepte", "schade"]
        ):
            for masker, masker_name in zip(
                ["totaal", "plas", "overlast"], ["", "_plas", "_overlast"]
            ):
                for return_period in [10,25,100,1000]:
                    self.add_file(
                        objectname=f"{type_raster}_T{return_period}_{masker}",
                        filename=f"{type_raster_name}_T{str(return_period).zfill(4)}{masker_name}.tif",
                        ftype="raster",
                    )

        for masker, masker_name in zip(["totaal", "plas", "overlast"], ["", "_plas", "_overlast"]):
            self.add_file(
                objectname=f"cw_schade_{masker}", 
                filename=f"cw_schade{masker_name}.tif", 
                ftype="raster"
            )

            self.add_file(
                objectname=f"cw_schade_{masker}_corr",
                filename=f"cw_schade{masker_name}_correctie.tif",
                ftype="raster",
            )

    @property
    def structure(self):
        return f"""  
               {self.space}{self.name}
               {self.space}├── temp
                """


class ClimateResultOutputTemp(Folder):
    def __init__(self, base):
        super().__init__(os.path.join(base,"temp"))

        self.add_file("peilgebieden_diepte", "peilgebieden_diepte.tif", "raster")
        self.add_file("peilgebieden_schade", "peilgebieden_schade.tif", "raster")
        self.add_file("peilgebieden", "peilgebieden_clipped.shp")

        self.create(parents=False) #create outputfolder if parent exists



class ClimateResultDownloads(Folder):
    def __init__(self, base):
        super().__init__(os.path.join(base,"01_downloads"))

        # Files
        self.add_file("download_uuid", "download_uuid.csv")
        self.names = GROUNDWATER #Initializes names.setter

        # for name in RAW_DOWNLOADS:
        #     setattr(self, name, ThreediResult(self.full_path(name)))

        for name in self.names:
            setattr(self,name, ClimateResultScenario(self.base, name))


    @property
    def names(self):
        return self._names

    @names.setter
    def names(self, groundwater_types=GROUNDWATER):
        names = []
        for rain_type in RAIN_TYPES:
            for groundwater in groundwater_types:
                for rain_scenario in RAIN_SCENARIOS:
                    names.append(f"{rain_type}_{groundwater}_{rain_scenario}")
        self._names = names
        
    def __repr__(self):
        return f"""{self.name} @ {self.path}
                    Folders:\t{self.structure}
                    Files:\t{list(self.files.keys())}
                    Layers:\t{list(self.olayers.keys())}
                    Groups:\t{list(self.names)}
                """

    # def _set_raster_files(self):
            # for rastertype in raster_types:
            #     self.add_file(
            #         f"{rastertype}_{name}", f"{rastertype}_{name}.tif", "raster"
            #     )


#TODO dit komt nu niet netjes in de print van de class.
class ClimateResultScenario(Folder):
    """Single scenario with multiple results"""
    def __init__(self, base, name):
        super().__init__(base)

        raster_types=["max_depth", "total_damage", "wlvl_max"]
        for rastertype in raster_types:
            self.add_file(rastertype, f"{rastertype}_{name}.tif", ftype='raster')
        self.structure_extra=[]
        #Netcdf for piek_ghg_t1000 and blok_ghg_t1000 for use in ruimtekaart.
        if name in RAW_DOWNLOADS:
            setattr(self, 'netcdf', ThreediResult(self.full_path(name)))
            self.structure_extra = ['netcdf']
    def __repr__(self):
        return f"""{self.name} @ {self.path}
                    Folders:\t{self.structure_extra}
                    Files:\t{list(self.files.keys())}
                    Layers:\t{list(self.olayers.keys())}
                """


class OutputFolder(Folder):
    def __init__(self, base):
        super().__init__(base)
        self.layers = Layers(os.path.join(self.base, "Layers"))
        self.logs = Logs(os.path.join(self.base, "Logs"))

    @property
    def structure(self):
        return f"""  
               {self.space}{self.name}
               {self.space}├── layers
               {self.space}└── logs
               """


class OutputPaths(Folder):
    """
    Output paths are only defined up to the foldername
    of the test, because we can internally decide on the
    filenames of logfiles and generated layers (these
    paths are not up to the user)
    """

    def __init__(self, base):
        super().__init__(os.path.join(base, "Output"))

        self.sqlite_tests = OutputFolder(self.full_path("Sqlite_tests"))
        self.bank_levels = OutputFolder(self.full_path("Bank_levels"))
        self.zero_d_one_d = OutputZeroDoneD(self.full_path("0d1d_tests"))
        self.one_d_two_d = OutputOneDTwoD(self.full_path("1d2d_tests"))

    @property
    def structure(self):
        return f"""  
               {self.space}output
               {self.space}├── sqlite_tests
               {self.space}├── bank_levels
               {self.space}├── zero_d_one_d
               {self.space}└── one_d_two_d
               """


class OutputRevisions(Folder):
    def __init__(self, base):
        super().__init__(base)

    def __getitem__(self, revision):
        if type(revision) == int:
            return OutputFolder(self.full_path(self.revisions[revision]))
        elif os.path.exists(revision):
            return OutputFolder(revision)
        elif (self.pl / revision).exists():
            return OutputFolder(self.full_path(revision))
        else:
            print("path not found, create with '.create()'")
            return OutputFolder(self.full_path(revision))

    def revision_structure(self, name):
        spacing = "\n\t\t\t\t\t\t\t"
        structure = f""" {spacing}{name} """
        for i, rev in enumerate(self.revisions):
            if i == len(self.revisions) - 1:
                structure = structure + f"{spacing}└── {rev}"
            else:
                structure = structure + f"{spacing}├── {rev}"

        return structure

    @property
    def revisions(self):
        return os.listdir(self.base)


class OutputZeroDoneD(OutputRevisions):
    def __init__(self, base):
        super().__init__(base)

    @property
    def structure(self):
        return self.revision_structure("zero_d_one_d")


class OutputOneDTwoD(OutputRevisions):
    def __init__(self, base):
        super().__init__(base)

    @property
    def structure(self):
        return self.revision_structure("one_d_two_d")


class Layers(Folder):
    def __init__(self, base):
        super().__init__(base)


class Logs(Folder):
    def __init__(self, base):
        super().__init__(base)


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
