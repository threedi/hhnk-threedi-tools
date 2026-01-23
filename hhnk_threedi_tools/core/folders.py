# %%
"""
Each class has a file as its attributes and a file as a new class.
self.base is the directory in which it is located
"""

# First-party imports
import json
import os
import time
from pathlib import Path

import hhnk_research_tools as hrt
import pandas as pd
from hhnk_research_tools import Folder
from hhnk_research_tools.variables import (
    GDB,
    SHAPE,
    file_types_dict,
)

# Local imports
from hhnk_threedi_tools.core.folder_helpers import ClimateResult
from hhnk_threedi_tools.core.schematisation.threedi_schematisation import ThreediSchematisation

# Globals
DAMO = f"DAMO{file_types_dict[GDB]}"
HDB = f"HDB{file_types_dict[GDB]}"
POLDER_POLY = f"polder_polygon{file_types_dict[SHAPE]}"
CHANNEL_FROM_PROFILES = f"channel_surface_from_profiles{file_types_dict[SHAPE]}"

FOLDER_STRUCTURE = """
    Main Folders object
        ├── project.json
        ├── 00_config
        │ ├── conversion
        │     └── conversion_config_hydroobject.json
        │ └── validation
        │     └── validation_rules.json               
        ├── 01_source_data
        │ ├── DAMO.gpkg
        │ ├── HyDAMO.gpkg
        │ ├── HDB.gpkg
        │ ├── datachecker_output.gpkg
        │ ├── rasters
        │     └── dem.tif
        │ └── modelbuilder_output
        │     └── preprocessed
        │ └── hydamo_validation
        │     └── validation_result.gpkg       
        ├── 02_schematisation
        │ ├── rasters (include DEM)
        │ └── * model(.gpkg) *
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
            └── HhnkSchematisationChecks
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
                                    ├── 00_config
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

        # project.json
        self.project_json = self.add_file("project_json", "project.json")

        # config
        self.config = ConfigDir(self.base, create=create)

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
               {self.space}├── project.json (.project_json)
               {self.space}├── 00_config (.config)
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
        Return dictionary containing paths to source files according to set project structure.

            build_base_paths_dict(
                    polder_path (string: path to project folder (highest level))
                )
        """
        return {
            "project_json": self.project_json.path_if_exists,
            # config
            "validation": self.config.validation.path_if_exists,
            "validation_rules": self.config.validation.validation_rules.path_if_exists,
            "conversion": self.config.conversion.path_if_exists,
            "conversion_config_hydroobject": self.config.conversion.conversion_config_hydroobject.path_if_exists,
            # source data
            "datachecker": self.source_data.datachecker.path_if_exists,
            "damo": self.source_data.damo.path_if_exists,
            "rasters": self.source_data.rasters.path_if_exists,
            "dem": self.source_data.rasters.dem.path_if_exists,
            "hydamo": self.source_data.hydamo.path_if_exists,
            "validation_result": self.source_data.hydamo_validation.validation_result.path_if_exists,
            "vergelijkingstool": self.source_data.vergelijkingstool.path_if_exists,
            "hdb": self.source_data.hdb.path_if_exists,
            "polder_shapefile": self.source_data.polder_polygon.path_if_exists,
            "channels_shapefile": self.source_data.modelbuilder.channel_from_profiles.path_if_exists,
            # model folder
            "model": self.model.schema_base.database.path_if_exists,
            "dem": self.model.schema_base.rasters.dem.path_if_exists,  # FIXME DEM path hier?
            # Threedi
            "0d1d_results_dir": self.threedi_results.zero_d_one_d.path_if_exists,
            "1d2d_results_dir": self.threedi_results.one_d_two_d.path_if_exists,
            "climate_results_dir": self.threedi_results.climate_results.path_if_exists,
            # Default output folders
            "base_output": self.output.path_if_exists,
            "HhnkSchemmatisationChecks_output": self.output.hhnk_schematisation_checks.path_if_exists,
            "0d1d_output": self.output.zero_d_one_d.path_if_exists,
            "bank_levels_output": self.output.bank_levels.path_if_exists,
            "1d2d_output": self.output.one_d_two_d.path_if_exists,
            "polder_folder": self.path_if_exists,
        }

    @classmethod
    def is_valid(self, folderpath):
        """Check if folder stucture is available in input folder."""
        SUB_FOLDERS = [
            "00_config",
            "01_source_data",
            "02_schematisation",
            "03_3di_results",
            "04_test_results",
        ]
        return all([Path(folderpath).joinpath(i).exists() for i in SUB_FOLDERS])


class ConfigDir(Folder):
    """Folder with configuration files"""

    def __init__(self, base, create):
        super().__init__(os.path.join(base, "00_config"), create=create)

        # Folders
        self.conversion = self.ConversionDir(self.base, create=create)
        self.validation = self.ValidationDir(self.base, create=create)

        if create:
            self.create_readme()

        # Files
        self.add_file("conversion_config_hydroobject", "conversion_config_hydroobject.json")
        self.add_file("validation_rules", "validation_rules.json")

    @property
    def structure(self):
        return f"""  
            {self.space}00_config
            {self.space}├── conversion
            {self.space}└── validation
            """

    def create_readme(self):
        readme_txt = (
            "Expected files are:\n\n"
            "Json file with conversion rules of hydroobjects named 'conversion_config_hydroobject.json'\n"
            "Json file with validation rules named 'validation_rules.json'\n"
        )
        with open(os.path.join(self.base, "read_me.txt"), mode="w") as f:
            f.write(readme_txt)

    class ConversionDir(Folder):
        def __init__(self, base, create):
            super().__init__(os.path.join(base, "conversion"), create=create)
            self.add_file("conversion_config_hydroobject", "conversion_config_hydroobject.json")

    class ValidationDir(Folder):
        def __init__(self, base, create):
            super().__init__(os.path.join(base, "validation"), create=create)
            self.add_file("validation_rules", "validation_rules.json")


class SourceDir(Folder):
    """Path to source data (datachecker, DAMO, HyDAMO, HDB)"""

    def __init__(self, base, create):
        super().__init__(os.path.join(base, "01_source_data"), create)

        # Folders
        self.modelbuilder = self.ModelbuilderPaths(self.base, create=create)
        self.hydamo_validation = self.HydamoValidationPaths(self.base, create=create)
        self.vergelijkingstool = self.VergelijkingstoolPaths(self.base, create=create)
        self.peilgebieden = self.PeilgebiedenPaths(self.base, create=create)
        self.wsa_output_administratie = self.WsaOutputAdministratie(self.base, create=create)
        self.rasters = self.Rasters(self.base, create=create)

        if create:
            self.create_readme()

        # Files
        self.add_file("damo", "DAMO.gpkg")
        self.damo.add_layers(["DuikerSifonHevel", "waterdeel"])

        self.add_file("dem", "dem.tif")

        self.add_file("hydamo", "HyDAMO.gpkg")

        self.add_file("hdb", "HDB.gpkg")
        self.hdb.add_layer("sturing_kunstwerken")

        self.add_file("datachecker", "datachecker_output.gpkg")
        self.datachecker.add_layers(["fixeddrainagelevelarea", "culvert", "bridge"])

        self.add_file("polder_polygon", POLDER_POLY)
        self.add_file("panden", "panden.gpkg")

    def create_readme(self):
        readme_txt = (
            "Expected files are:\n\n"
            "Damo geopackage (*.gpkg) named 'DAMO.gpkg'\n"
            "Hydamo geopackage (*.gpkg) named 'HyDAMO.gpkg'\n"
            "Datachecker geopackage (*.gpkg) named 'datachecker_output.gpkg'\n"
            "Hdb geopackage (*.gpkg) named 'HDB.gpkg'\n"
            "Folder named 'modelbuilder_output' and polder shapefile "
            "Folder named 'rasters'"
            "(*.shp and associated file formats)"
        )
        with open(os.path.join(self.base, "read_me.txt"), mode="w") as f:
            f.write(readme_txt)

    @property
    def structure(self):
        return f"""  
               {self.space}01_source_data
               {self.space}└── rasters
               {self.space}└── modelbuilder_output
               {self.space}└── hydamo_validation
               {self.space}└── peilgebieden
               {self.space}└── vergelijkingstool
               {self.space}└── wsa_output_administratie
               
               """

    class Rasters(Folder):
        def __init__(self, base, create):
            super().__init__(os.path.join(base, "rasters"), create=create)
            self.add_file("dem", "dem.tif")

    class WsaOutputAdministratie(Folder):
        def __init__(self, base, create):
            super().__init__(os.path.join(base, "wsa_output_administratie"), create=create)
            self.add_file("opmerkingen", "opmerkingen.shp")

    class ModelbuilderPaths(Folder):
        def __init__(self, base, create):
            super().__init__(os.path.join(base, "modelbuilder_output"), create=create)
            self.add_file("channel_from_profiles", CHANNEL_FROM_PROFILES)

    class HydamoValidationPaths(Folder):
        def __init__(self, base, create):
            super().__init__(os.path.join(base, "hydamo_validation"), create=create)
            self.add_file("validation_result", "validation_result.gpkg")

    class VergelijkingstoolPaths(Folder):
        def __init__(self, base, create):
            super().__init__(os.path.join(base, "vergelijkingstool"), create=create)

            self.input_data_old = self.full_path("input_data_old")
            self.input_data_old.mkdir(parents=True, exist_ok=True)
            self.add_file("input_data_old", "DAMO.gpkg")
            self.add_file("input_data_old", "HDB.gpkg")

            self.output = self.full_path("output")
            self.output.mkdir(parents=True, exist_ok=True)
            self.add_file("output", "Threedi_comparison.gpkg")
            self.add_file("output", "DAMO_comparison.gpkg")

    class PeilgebiedenPaths(Folder):
        # TODO deze map moet een andere naam en plek krijgen.
        def __init__(self, base, create):
            super().__init__(os.path.join(base, "peilgebieden"), create=create)

            # Find peilgebieden shapefile in folder.
            if self.exists():
                shape_name = [x.name for x in self.content if x.stem.startswith("peilgebieden") and x.suffix == ".shp"]
                if len(shape_name) == 1:
                    self.add_file("peilgebieden", shape_name[0])
                else:
                    self.add_file("peilgebieden", "peilgebieden.shp")
            self.add_file("geen_schade", "geen_schade.shp")


class SchemaDirParent(Folder):
    """Parent folder with all model (schematisations) in it. These
    all share the same base schematisation, with only differences in
    global settings or other things specific for that model
    """

    def __init__(self, base, create):
        super().__init__(os.path.join(base, "02_schematisation"), create)

        self.revisions = self.ModelRevisionsParent(base=self.base, create=create)
        self.schema_base = ThreediSchematisation(base=self.base, name="00_basis", create=create)
        self.calculation_rasters = self.CalculationRasters(base=self.base, create=create)
        self.schema_list = ["schema_base"]
        self.add_file("model_sql", "model_sql.json")

        if create:
            self.create_readme()

        self.add_file("settings", "model_settings.xlsx")
        self.add_file("settings_default", "model_settings_default.xlsx")

        # Load settings excel
        self.settings_loaded = False
        self.settings_df = None

    def _add_modelpath(self, name):
        setattr(
            self,
            f"schema_{name}",
            ThreediSchematisation(base=self.base, name=name, create=False),
        )
        self.schema_list.append(f"schema_{name}")
        return f"schema_{name}"

    def set_modelsplitter_paths(self):
        """Call this to set the individual schematisations for the splitter."""
        if self.settings.exists():
            if not self.settings_loaded:  # only read once. #FIXME test this, might cause issues.
                self.settings_df = pd.read_excel(self.settings.base, engine="openpyxl")
                self.settings_df = self.settings_df[self.settings_df["name"].notna()]
                self.settings_df.set_index("name", drop=False, inplace=True)
                self.settings_loaded = True

                for item_name, row in self.settings_df.iterrows():
                    if not pd.isna(row["name"]):
                        self._add_modelpath(name=item_name)
        else:
            print(f"Tried to load {self.settings.base}, but it doesnt exist.")

    def create_readme(self):
        readme_txt = (
            "Expected files are:\n\n"
            "Gpkg database (model): *.gpkg\n"
            "Folder named 'rasters' containing DEM raster (*.tif) and other rasters\n"
        )
        with open(os.path.join(self.base, "read_me.txt"), mode="w") as f:
            f.write(readme_txt)

    def __repr__(self):
        return f"""{self.name} @ {self.base}
                    Folders:\t .calculation_rasters
                    Files:\t{list(self.files.keys())}
                    Model schemas:\t{self.schema_list}
                """

    class ModelRevisionsParent(Folder):
        """Local revisions directory of base schematisation"""

        def __init__(self, base, create):
            super().__init__(os.path.join(base, "revisions"), create)

    class CalculationRasters(Folder):
        """sub-folder of SchemaDirParent with rasters required for calculations.

        With these rasters we can do:
            - damage calculations
        """

        def __init__(self, base, create):
            super().__init__(os.path.join(base, "rasters_verwerkt"), create)

            self.add_file("dem", "dem.tif")
            self.add_file("glg", "glg.tif")
            self.add_file("ggg", "ggg.tif")
            self.add_file("ghg", "ghg.tif")
            self.add_file("infiltration", "infiltration.tif")
            self.add_file("friction", "friction.tif")
            self.add_file("landuse", "landuse.tif")
            self.add_file("polder", "polder.tif")
            self.add_file("waterdeel", "waterdeel.tif")

            # self.add_file("dem", "dem_50cm.tif")
            self.add_file("damage_dem", "damage_dem.tif")  # TODO glob maken van beschikbare damage_dems.
            self.add_file("panden", "panden.tif")
            if create:
                self.create_readme()

        def create_readme(self):
            readme_file = self.path.joinpath("README.txt")
            if not readme_file.exists():
                readme_txt = (
                    "Expected files are:\n\n"
                    "dem_50cm.tif -> used to create damage_dem.tif\n"
                    "panden.tif -> used to create damage_dem.tif\n"
                    "damage_dem.tif -> dem_50cm.tif + panden.tif. Used for damage calculations.\n\n"
                    "polder.tif -> Model bounds.\n\n"
                    "waterdeel.tif -> Waterdeel\n\n"
                )
                with open(readme_file, mode="w") as f:
                    f.write(readme_txt)


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
        with open(os.path.join(self.base, "read_me.txt"), mode="w") as f:
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

        self.hhnk_schematisation_checks = self.OutputDirHhnkSchematisationChecks(
            self.full_path("hhnk_schematisation_checks"), create=create
        )
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
        elif name == "hhnk_schematisation_checks":
            return self.hhnk_schematisation_checks

    def create_readme(self):
        readme_txt = (
            "This folder is the default folder where the HHNK plugin "
            "saves results of tests. The inner structure of these result folders "
            "is automatically generated"
        )
        with open(os.path.join(self.base, "read_me.txt"), mode="w") as f:
            f.write(readme_txt)

    @property
    def structure(self):
        return f"""  
               {self.space}output
               {self.space}├── hhnk_schematisation_checks
               {self.space}├── bank_levels
               {self.space}├── zero_d_one_d
               {self.space}├── one_d_two_d
               {self.space}└── climate
               """

    class OutputDirHhnkSchematisationChecks(Folder):
        def __init__(self, base, create):
            super().__init__(base, create=create)

            self.add_file("bodemhoogte_kunstwerken", "bodemhoogte_kunstwerken.gpkg")
            self.add_file("bodemhoogte_stuw", "bodemhoogte_stuw.gpkg")
            self.add_file("gebruikte_profielen", "gebruikte_profielen.gpkg")
            self.add_file("geisoleerde_watergangen", "geisoleerde_watergangen.gpkg")
            self.add_file("gestuurde_kunstwerken", "gestuurde_kunstwerken.gpkg")
            self.add_file("drooglegging", "drooglegging.tif")
            self.add_file("geometry_check", "geometry_check.csv")
            self.add_file("general_hhnk_schematisation_checks", "general_hhnk_schematisation_checks.csv")
            self.add_file("cross_section_duplicates", "cross_section_duplicates.gpkg")
            self.add_file("cross_section_no_vertex", "cross_section_no_vertex.gpkg")
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
                self.add_file("grid_wlvl", "grid_wlvl.gpkg")
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


# A class for project
# TODO remove Project and add functionality to Folders.
class Project:
    def __init__(self, folder: str):
        self.project_folder = folder  # set project folder
        self.folders = Folders(folder, create=True)  # create folders instance for new project folder

        self.json_path = str(self.folders.project_json.path)
        if self.folders.project_json.exists():
            self.load_from_json(self.json_path)  # load variables from json if exists (fixed filename)
        else:
            self.initialise_new_project()  # initialise new project
            self.save_to_json(self.json_path)

    def initialise_new_project(self):
        """Create all Project variables for a new project"""
        self.project_status = 0
        self.project_name = str(Path(self.project_folder).name)

    def update_project_status(self, status):
        self.project_status = status
        self.save_to_json(self.json_path)

    def retrieve_project_status(self):
        return self.project_status

    def save_to_json(self, filepath):
        self.project_date = time.strftime("%Y-%m-%d %H:%M:%S")  # update project date

        def is_json_serializable(value):
            try:
                json.dumps(value)
                return True
            except (TypeError, OverflowError):
                return False

        data = {k: v for k, v in self.__dict__.items() if is_json_serializable(v)}

        filepath = Path(filepath)
        with filepath.open("w") as f:
            json.dump(data, f, indent=2)

    def load_from_json(self, filepath):
        with open(filepath, "r") as f:
            data = json.load(f)
            self.__dict__.update(data)


# %%
if __name__ == "__main__":
    from hhnk_threedi_tools.core.folders import Folders
    from tests.config import FOLDER_TEST

    folder = Folders(FOLDER_TEST)
    folder.model.schema_base.rasters.dem.path
# %%
