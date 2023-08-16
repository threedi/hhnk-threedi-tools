# %%
"""
Each class has a file as its attributes and a file as a new class. 
self.base is the directory in which it is located
"""
# First-party imports
import os


import hhnk_research_tools as hrt

from hhnk_research_tools import Folder

from hhnk_research_tools.variables import (
    file_types_dict,
    GDB,
    SHAPE,
)

# Local imports
from hhnk_threedi_tools.core.folder_helpers import ClimateResult

import pandas as pd


# Globals
DAMO = f"DAMO{file_types_dict[GDB]}"
HDB = f"HDB{file_types_dict[GDB]}"
POLDER_POLY = f"polder_polygon{file_types_dict[SHAPE]}"
CHANNEL_FROM_PROFILES = f"channel_surface_from_profiles{file_types_dict[SHAPE]}"








FOLDER_STRUCTURE = """
    Main Folders object
        ├── 01_source_data
        │ ├── DAMO.gpkg
        │ ├── HDB.gpkg
        │ ├── datachecker_output.gpkg
        │ └── modelbuilder_output
        │     └── preprocessed
        ├── 02_schematisation
        │ ├── rasters (include DEM)
        │ └── * model(.sqlite) *
        ├── 03_3di_results
        │ ├── 0d1d_results
        │ └── 1d2d_results
        | |__ batch_results
        └── 04_test_results
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
                           				├── 01_source_data
                           				├── 02_schematisation
                           				├── 03_3di_results
                           				└── 04_test_results
                           
                                Files:	[]
                                Layers:	[]
        
            
            folder.source_data.show
            
            Output: 
            
                01_Source_data @ C:/Poldermodellen/Heiloo/01_Source_data
                                    Folders:	  
                               				source_data
                               				└── modelbuilder
                               
                                    Files:	['damo', 'hdb', 'datachecker', ...]
            
            
        {FOLDER_STRUCTURE}

        """

    def __init__(self, base, create=False):
        super().__init__(base, create=create)

        # source
        self.source_data = SourceDir(self.base, create=create)

        # model files
        self.model = SchemaDirParent(self.base, create=create)

        # Threedi results
        self.threedi_results = ThreediResultsDir(self.base, create=create)

        # Results of tests
        self.output = OutputDirParent(self.base, create=create)


    @property
    def structure(self):
        return f"""  
               {self.space}Folders
               {self.space}├── 01_source_data (.source_data)
               {self.space}├── 02_schematisation (.model)
               {self.space}├── 03_3di_results (.threedi_results)
               {self.space}└── 04_test_results (.output)
               """


    @property
    def full_structure(self):
        return print(FOLDER_STRUCTURE)


    def to_file_dict(self):
        """
        Returns dictionary containing paths to source files according to set project structure.

            build_base_paths_dict(
                    polder_path (string: path to project folder (highest level))
                )
        """
        return {
            "datachecker": self.source_data.datachecker.path_if_exists,
            "damo": self.source_data.damo.path_if_exists,
            "hdb": self.source_data.hdb.path_if_exists,
            "polder_shapefile": self.source_data.polder_polygon.path_if_exists,
            "channels_shapefile": self.source_data.modelbuilder.channel_from_profiles.path_if_exists,
            # model folder
            "model": self.model.schema_base.database.path_if_exists,
            "dem": self.model.schema_base.rasters.dem.path_if_exists,
            # Threedi
            "0d1d_results_dir": self.threedi_results.zero_d_one_d.path_if_exists,
            "1d2d_results_dir": self.threedi_results.one_d_two_d.path_if_exists,
            "climate_results_dir": self.threedi_results.climate_results.path_if_exists,
            # Default output folders
            "base_output": self.output.path_if_exists,
            "sqlite_tests_output": self.output.sqlite_tests.path_if_exists,
            "0d1d_output": self.output.zero_d_one_d.path_if_exists,
            "bank_levels_output": self.output.bank_levels.path_if_exists,
            "1d2d_output": self.output.one_d_two_d.path_if_exists,
            "polder_folder": self.path_if_exists,
        }


    def is_valid(self):
        """Check if folder stucture is available in input folder."""
        SUB_FOLDERS = ["01_source_data", "02_schematisation", "03_3di_results", "04_test_results"]
        return all([self.full_path(i).exists() for i in SUB_FOLDERS])


class SourceDir(Folder):
    """
    Paths to source data (datachecker, DAMO, HDB)
    """

    def __init__(self, base, create):
        super().__init__(os.path.join(base, "01_source_data"), create)

        # Folders
        self.modelbuilder = self.ModelbuilderPaths(self.base, create=create)
        self.peilgebieden = self.PeilgebiedenPaths(self.base, create=create)
        self.wsa_output_administratie = self.WsaOutputAdministratie(self.base, create=create)

        if create:
            self.create_readme()

        # Files
        self.add_file("damo", "DAMO.gpkg")
        self.damo.add_layers(["DuikerSifonHevel", 
                             "waterdeel"])
        
        self.add_file("hdb", "HDB.gpkg")
        self.hdb.add_layer("sturing_3di")

        self.add_file("datachecker", "datachecker_output.gpkg")
        self.datachecker.add_layers(["fixeddrainagelevelarea",
                                    "culvert"])

        self.add_file("polder_polygon", POLDER_POLY)
        self.add_file("panden", "panden.gpkg")


    def create_readme(self):
        readme_txt = (
                "Expected files are:\n\n"
                "Damo geopackage (*.gpkg) named 'DAMO.gpkg'\n"
                "Datachecker geopackage (*.gpkg) named 'datachecker_output.gpkg'\n"
                "Hdb geopackage (*.gpkg) named 'HDB.gpkg'\n"
                "Folder named 'modelbuilder_output' and polder shapefile "
                "(*.shp and associated file formats)"
            )
        with open(
            os.path.join(self.base, "read_me.txt"), mode="w"
        ) as f:
            f.write(readme_txt)

    @property
    def structure(self):
        return f"""  
               {self.space}01_source_data
               {self.space}└── modelbuilder
               {self.space}└── peilgebieden
               {self.space}└── wsa_output_administratie
               
               """
    class WsaOutputAdministratie(Folder):
        def __init__(self, base, create):
            super().__init__(os.path.join(base, "wsa_output_administratie"), create=create)
            self.add_file("opmerkingen", "opmerkingen.shp")


    class ModelbuilderPaths(Folder):
        def __init__(self, base, create):
            super().__init__(os.path.join(base, "modelbuilder_output"), create=create)
            self.add_file("channel_from_profiles", CHANNEL_FROM_PROFILES)


    class PeilgebiedenPaths(Folder):
        # TODO deze map moet een andere naam en plek krijgen.
        def __init__(self, base, create):
            super().__init__(os.path.join(base, "peilgebieden"), create=create)

            # Find peilgebieden shapefile in folder.
            if self.exists():
                shape_name = [
                    x.name
                    for x in self.content
                    if x.stem.startswith("peilgebieden") and x.suffix==".shp"
                ]
                if len(shape_name) == 1:
                    self.add_file("peilgebieden", shape_name[0])
                else:
                    self.add_file("peilgebieden", "peilgebieden.shp")
            self.add_file("geen_schade", "geen_schade.shp")


class SchemaDirParent(Folder):
    """Parent folder with all model (schematisations) in it. These
    all share the same base schematisation, with only differences in
    global settings or other things specific for that model"""

    def __init__(self, base, create):
        super().__init__(os.path.join(base, "02_schematisation"), create)

        self.revisions = self.ModelRevisionsParent(base=self.base, create=create)
        self.schema_base = hrt.ThreediSchematisation(base=self.base, name="00_basis", create=create)
        self.schema_list = ["schema_base"]
        self.add_file("model_sql", "model_sql.json")

        if create:
            self.create_readme()
        
        self.add_file("settings", "model_settings.xlsx")
        self.add_file("settings_default", "model_settings_default.xlsx")


        #Load settings excel
        self.settings_loaded = False
        self.settings_df = None

    def _add_modelpath(self, name):
        setattr(self, f"schema_{name}", hrt.ThreediSchematisation(base=self.base, name=name))
        self.schema_list.append(f"schema_{name}")
        return f"schema_{name}"


    def set_modelsplitter_paths(self):
        """Call this to set the individual schematisations for the splitter."""
        if self.settings.exists():
            if not self.settings_loaded: #only read once. #FIXME test this, might cause issues.
                    self.settings_df = pd.read_excel(self.settings.base, engine="openpyxl")
                    self.settings_df = self.settings_df[self.settings_df['name'].notna()]
                    self.settings_df.set_index("name", drop=False, inplace=True)
                    self.settings_loaded = True

                    for item_name, row in self.settings_df.iterrows():
                        if not pd.isnull(row["name"]):
                            self._add_modelpath(name=item_name)
        else:
            print(f"Tried to load {self.settings.base}, but it doesnt exist.")

       
    def create_readme(self):
        readme_txt = (
                "Expected files are:\n\n"
                "Sqlite database (model): *.sqlite\n"
                "Folder named 'rasters' containing DEM raster (*.tif) and other rasters\n"
            )
        with open(
            os.path.join(self.base, "read_me.txt"), mode="w"
        ) as f:
            f.write(readme_txt)


    def __repr__(self):
        return f"""{self.name} @ {self.base}
                    Folders:\t{self.structure}
                    Files:\t{list(self.files.keys())}
                    Model schemas:\t{self.schema_list}
                """


    class ModelRevisionsParent(Folder):
        """Local revisions directory of base schematisation"""
        def __init__(self, base, create):
            super().__init__(os.path.join(base, "revisions"), create)
            if create:
                self.create()


class ThreediResultsDir(Folder):
    """
    Folder in which 3di results are saved

    to use with list indexing use the following options:
    options = ["01d_results", "1d2d_results", "climate_results", "batch_results]
    .threedi_results[options[x]]
    """

    def __init__(self, base, create):
        super().__init__(os.path.join(base, "03_3di_results"), create)

        # Folders
        self.zero_d_one_d = self.ZeroDOneDDir(self.base, create=create)
        self.one_d_two_d = self.OneDTwoDDir(self.base, create=create)
        self.climate_results = self.ClimateResultsDir(self.base, create=create)

        if create:
            self.create_readme()


    @property
    def structure(self):
        return f"""  
               {self.space}threedi_results
               {self.space}├── zero_d_one_d  ["0d1d_results"]
               {self.space}├── one_d_two_d  ["1d2d_results"]
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
        return hrt.ThreediResult(os.path.join(results_path, revision_dir))


    def create_readme(self):
        readme_txt = (
                "Expected files are:\n\n"
                "The subfolders in this folder expect to contain folders corresponding to "
                "3di results from different revisions (e.g. containing *.nc, *.h5 file)"
            )
        with open(
            os.path.join(self.base, "read_me.txt"), mode="w"
        ) as f:
            f.write(readme_txt)


    class ZeroDOneDDir(hrt.RevisionsDir):
        def __init__(self, base, create):
            super().__init__(base, "0d1d_results", returnclass=hrt.ThreediResult, create=create)

        @property
        def structure(self):
            return self.revision_structure("zero_d_one_d")


    class OneDTwoDDir(hrt.RevisionsDir):
        def __init__(self, base, create):
            super().__init__(base, "1d2d_results", returnclass=hrt.ThreediResult, create=create)

        @property
        def structure(self):
            return self.revision_structure("one_d_two_d")


    class ClimateResultsDir(hrt.RevisionsDir):
        def __init__(self, base, create):
            super().__init__(base, "batch_results", returnclass=ClimateResult, create=create)

        @property
        def structure(self):
            return self.revision_structure("climate_results")


# 1d2d output
class OutputDirParent(Folder):
    """
    Output paths are only defined up to the foldername
    of the test, because we can internally decide on the
    filenames of logfiles and generated layers (these
    paths are not up to the user)
    """

    def __init__(self, base, create):
        super().__init__(os.path.join(base, "04_test_results"), create)

        self.sqlite_tests = self.OutputDirSqlite(self.full_path("sqlite_tests"), create=create)
        self.bank_levels = self.OutputDirBankLevel(self.full_path("bank_levels"), create=create)
        self.zero_d_one_d = self.OutputDir0d1d(base=self.base, name="0d1d_tests", create=create)
        self.one_d_two_d = self.OutputDir1d2d(base=self.base, name="1d2d_tests", create=create)
        self.climate = self.OutputDirClimate(base=self.base, name="climate", create=create)

        if create:
            self.create_readme()

    def __getitem__(self, name):
        if name == "0d1d_results":
            return self.zero_d_one_d
        elif name == "1d2d_results":
            return self.one_d_two_d
        elif name in ["batch_results", "climate_results", "climate"]:
            return self.climate
        elif name == "bank_levels":
            return self.bank_levels
        elif name == "sqlite_tests":
            return self.sqlite_tests


    def create_readme(self):
        readme_txt = (
                "This folder is the default folder where the HHNK plugin "
                "saves results of tests. The inner structure of these result folders "
                "is automatically generated"
            )
        with open(
            os.path.join(self.base, "read_me.txt"), mode="w"
        ) as f:
            f.write(readme_txt)

    @property
    def structure(self):
        return f"""  
               {self.space}output
               {self.space}├── sqlite_tests
               {self.space}├── bank_levels
               {self.space}├── zero_d_one_d
               {self.space}├── one_d_two_d
               {self.space}└── climate
               """


    class OutputDirSqlite(Folder):
        def __init__(self, base, create):
            super().__init__(base, create=create)

            self.add_file("bodemhoogte_kunstwerken", "bodemhoogte_kunstwerken.gpkg")
            self.add_file("bodemhoogte_stuw", "bodemhoogte_stuw.gpkg")
            self.add_file("gebruikte_profielen", "gebruikte_profielen.gpkg")
            self.add_file("geisoleerde_watergangen", "geisoleerde_watergangen.gpkg")
            self.add_file("gestuurde_kunstwerken", "gestuurde_kunstwerken.gpkg")
            self.add_file("drooglegging", "drooglegging.tif")
            self.add_file("geometry_check", "geometry_check.csv")
            self.add_file("general_sqlite_checks", "general_sqlite_checks.csv")
            self.add_file("overlappende_profielen", "overlappende_profielen.gpkg")
            self.add_file("profielen_geen_vertex", "profielen_geen_vertex.gpkg")
            self.add_file("wateroppervlak", "wateroppervlak.gpkg")


            self.add_file("streefpeil", r"/temp/streefpeil.tif")

    class OutputDirBankLevel(Folder):
        def __init__(self, base, create):
            super().__init__(base, create=create)
            # self.layers = Layers(os.path.join(self.base, "Layers"))
            # self.logs = Logs(os.path.join(self.base, "Logs"))

        @property
        def structure(self):
            return f"""  
                {self.space}{self.name}
                {self.space}├── layers
                {self.space}└── logs
                """


    class OutputDir0d1d(hrt.RevisionsDir):
        def __init__(self, base, name, create):
            super().__init__(base, name, returnclass=self.Outputd0d1d_revision, create=create)

        @property
        def structure(self):
            return self.revision_structure("zero_d_one_d")


        class Outputd0d1d_revision(Folder):
            """Outputfolder 0d1d for a specific revision."""

            def __init__(self, base, create=False):
                super().__init__(base, create=create)

                self.add_file("nodes_0d1d_test", "nodes_0d1d_test.gpkg")
                self.add_file(
                    "hydraulische_toets_kunstwerken",
                    "hydraulische_toets_kunstwerken.gpkg",
                )
                self.add_file(
                    "hydraulische_toets_watergangen",
                    "hydraulische_toets_watergangen.gpkg",
                )


    class OutputDir1d2d(hrt.RevisionsDir):
        def __init__(self, base, name, create):
            super().__init__(base, name, returnclass=self.Outputd1d2d_revision, create=create)

        @property
        def structure(self):
            return self.revision_structure("one_d_two_d")

        class Outputd1d2d_revision(Folder):
            """Outputfolder 1d2d for a specific revision."""

            def __init__(self, base, create=True):
                super().__init__(base, create=create)

                self.add_file("grid_nodes_2d", "grid_nodes_2d.gpkg")
                self.add_file("stroming_1d2d_test", "stroming_1d2d_test.gpkg")
                for T in [1, 3, 15]:
                    self.add_file(f"waterstand_T{T}", f"waterstand_T{T}.tif")
                    self.add_file(f"waterdiepte_T{T}", f"waterdiepte_T{T}.tif")



# TODO hoort deze class hier nog? resultaten staan op een andere plek
    class OutputDirClimate(hrt.Folder):
        def __init__(self, base, name, create=True):
            super().__init__(os.path.join(base, name), create=create)
            # if create:
            #     self.create()  # create outputfolder if parent exists

        @property
        def structure(self):
            return self.revision_structure("Climate")



def create_tif_path(folder, filename):
    """
    Takes a folder name (ex: C:../output/Layers) and base filename (ex: raster) as arguments
    and returns full path (ex: C:../output/Layers/raster.tif)
    """
    try:
        full_path = os.path.join(folder, f"{filename}.tif")
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
    if path is None:
        return None
    else:
        return path if os.path.exists(path) else None