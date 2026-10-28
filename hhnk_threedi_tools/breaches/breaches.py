# %%
"""
Each class has a file as its attributes and a file as a new class.
self.base is the directory in which it is located
"""

# First-party imports
import os
from pathlib import Path

from hhnk_research_tools import Folder

FOLDER_STRUCTURE = """
    Main Breaches object
        ├── 01_NetCDF
        │ ├── aggregate_results_3di.nc
        │ ├── gridadmin.h5
        │ ├── gridadmin.sqlite
        │ └── results_3di.nc
        └── 02_WSS
        │ ├── dem_clip.vrt
        │ ├── grid_raw.gpkg
        │ ├── mask_flood.gpkg
        │ └── max_wdepth_orig.tif
        │ ├── new_grid.gpkg
        │ ├── nodeid.tif
        │ └── landuse_2021_clip.vrt  
        ├── 03_SSM
        │ ├── max_flow_velocity.tif
        │ └── max_raste_of_rise.tif
        │ └── max_waterdepth.tif
        │ └── max_waterlevel.tif
        ├── 04_JPEG
        │ ├── overstroming.png
        │ ├── breach.name.png
        │ └── agg.png
        ├── breach_data.csv
        ├── breach_data_agg.csv
        ├── simulation_data.csv     
    """


class BreachFolder(Folder):
    def _find_files(self, filename):
        matches = sorted(Path(self.base).glob(f"*{filename}"))
        return matches[0] if matches else None


class Breaches(BreachFolder):
    __doc__ = f"""
        
        --------------------------------------------------------------------------
        An object to ease the accessibility, creation and checks of folders and
        files in the current structure.
        
        Usage as follows:
            - Access class with te path to the main folder (e.g., E:\03.resultaten\Overstromingsberekeningen primaire doorbraken 2024\output\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_ZUID_T10_T3000\ROR-PRI-UITDAMMERDIJK_8-T100)
            - Find your way through by using folder.show
            - Check if a file or folder exists using .exists
            - Show all (needed) files using .files
            - Show all (needed) layers using .layers
            - Return a path of a file using either str() or .path
        
        Example code:
            folder = Folders('E:\03.resultaten\Overstromingsberekeningen primaire doorbraken 2024\output\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_ZUID_T10_T3000\ROR-PRI-UITDAMMERDIJK_8-T100')
            
            folder.show
           
            Output: 
                ROR-PRI-UITDAMMERDIJK_8-T100 @ y:\03.resultaten\Overstromingsberekeningen primaire doorbraken 2024\output\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_ZUID_T10_T3000\ROR-PRI-UITDAMMERDIJK_8-T100
                                Folders:	  
                                    Folders
                                    ├── 01_NetCDF
                                    ├── 02_WSS
                                    ├── 03_SSM
                                    └── 04_JPEG
                           
                                Files:	[]
                                Layers:	[]
        
            
            folder.source_data.show
            
            
        {FOLDER_STRUCTURE}

        """

    def __init__(self, base, create=True):
        super().__init__(base, create=create)

        # NetCDF
        self.netcdf = NetCDF(self.base, create=create)

        # png file: maps or graphs
        self.jpeg = JPEG(self.base, create=create)

        # damage folder
        self.ssm = SSM(self.base, create=create)

        # water depth and gridraw
        self.wss = WSS(self.base, create=create)

        # water depth and gridraw
        self.csv = csv(self.base)

    @property
    def structure(self):
        return f"""  
               {self.space}Folders
               {self.space}├── 01_NetCDF (.netcdf)
               {self.space}├── 02_WSS (.wss)
               {self.space}├── 03_SSM (.ssm)
               {self.space}└── 04_JPEG (.jpeg)
               """

    def to_file_dict(self):
        """
        Return dictionary containing paths to source files according to set project structure.

            build_base_paths_dict(
                    polder_path (string: path to project folder (highest level))
                )
        """

        return {
            "aggregate_results_3di": self.netcdf.aggregate_results_3di,
            "gridadmin": self.netcdf.gridadmin,
            "results_3di": self.netcdf.results_3di,
            # jpeg
            "graph": self.jpeg.graph,
            "agg_graph": self.jpeg.agg_graph,
            "bar_graph": self.jpeg.bar_graph,
            # wss
            "dem_clip": self.wss.dem_clip,
            "grid_raw": self.wss.grid_raw,
            "mask_flood": self.wss.mask_flood,
            "max_wdepth_orig": self.wss.max_wdepth_orig,
            "new_grid": self.wss.new_grid,
            "max_waterlevel_orig": self.wss.max_waterlevel_orig,
            "landuse_2021_clip": self.wss.landuse_2021_clip,
            "damage_orig_lizard": self.wss.damage_orig_lizard,
            "direct_cost_damage": self.wss.direct_cost_damage,
            "indirect_cost_damage": self.wss.indirect_cost_damage,
            "total_costs_area": self.wss.total_costs_area,
            # ssm
            # "max_flow_velocity_5m": self.ssm.max_flow_velocity_5m.path_if_exists,
            # "max_rate_of_rise_5m": self.ssm.max_rate_of_rise_5m.hhnk_schematisation_checks.path_if_exists,
            # "max_waterdepth_5m": self.ssm.max_waterdepth_5m.path_if_exists,
            # "max_waterlevel_5m": self.ssm.max_waterlevel_5m.path_if_exists,
            # breach data
            "breach_data": self.csv.breach_data,
            "breach_data_agg": self.csv.breach_data_agg,
            "simulation_data": self.csv.simulation_data,
            "ldo_metadata": self.csv.ldo_metadata,
        }

    @classmethod
    def is_valid(self, folderpath):
        """Check if folder stucture is available in input folder."""
        SUB_FOLDERS = ["01_NetCDF", "02_WSS", "03_SSM", "04_JPEG"]
        return all([Path(folderpath).joinpath(i).exists() for i in SUB_FOLDERS])


class csv(BreachFolder):
    def __init__(self, base):

        super().__init__(str(base), create=False)
        self.base_path = Path(base)

        self.breach_data = self._find_files("breach_data.csv")
        self.breach_data_agg = self._find_files("breach_data_agg.csv")
        self.simulation_data = self._find_files("simulation_data.csv")
        self.ldo_metadata = self._find_files(f"{self.base_path.name}.xlsx")

    @property
    def as_dict(self):
        return {
            "breach_data": self.breach_data,
            "breach_data_agg": self.breach_data_agg,
            "simulation_data": self.simulation_data,
            "ldo_metadata": self.ldo_metadata,
        }


class NetCDF(BreachFolder):
    """Path to netcdf data (aggregate_results_3di, gridadmin, results_3di)"""

    def __init__(self, base, create):
        super().__init__(os.path.join(base, "01_NetCDF"), create)
        self.aggregate_results_3di = self._find_files("aggregate_results_3di.nc")
        self.results_3di = self._find_files("results_3di.nc")
        self.gridadmin = self._find_files("gridadmin.h5")
        # Folders

        if create:
            self.create_readme()

        # Files

    def create_readme(self):
        readme_txt = (
            "Expected files are:\n\n"
            "aggregate_results_3di (*.nc) named 'aggregate_results_3di.nc'\n"
            "gridadmin (*.h5) named 'gridadmin.h5'\n"
            "gridadmin (*.sqlite) named 'gridadmin.sqlite'\n"
            "log_files (*..zip) named 'log_files.zip'\n"
        )
        with open(os.path.join(self.base, "read_me.txt"), mode="w") as f:
            f.write(readme_txt)

    @property
    def structure(self):
        return f"""  
               {self.space}01_NetCDF
               {self.space}└── aggregate_results_3di
               {self.space}└── gridadmin
               {self.space}└── results_3di
               
               """


class JPEG(BreachFolder):
    """Parent folder with all the images created for the final product. They included:
    breach graph and it aggregation, and also all the maps that can be generated.
    """

    def __init__(self, base, create):
        super().__init__(os.path.join(base, "04_JPEG"), create)
        self.graph = self._find_files(f"{Path(base).name}.png")
        self.agg_graph = self._find_files(f"{Path(base).name}_agg.png")
        self.bar_graph = self._find_files(f"{Path(base).name}_bar_graph.png")
        if create:
            self.create_readme()

    def create_readme(self):
        readme_txt = (
            "This folder is the default folder where all the images including the waterdepth maps, graph and others"
            "are stored. The inner structure of these result folders "
            "is automatically generated"
        )
        with open(os.path.join(self.base, "read_me.txt"), mode="w") as f:
            f.write(readme_txt)

    def __repr__(self):
        return f"""{self.name} @ {self.base}
                    Folders:\t{self.structure}
                    Files:\t{list(self.files.keys())}
                """


class SSM(BreachFolder):
    """
    Folder in which rasters from lizard can be saved

    """

    def __init__(self, base, create):
        super().__init__(os.path.join(base, "03_SSM"), create)

        # Folders

        if create:
            self.create_readme()

    def create_readme(self):
        readme_txt = (
            "This folder is the default folder where all the files to calculate the waterdepth raster including the waterdepth raster"
            "are stored. The inner structure of these result folders "
            "is automatically generated"
        )
        with open(os.path.join(self.base, "read_me.txt"), mode="w") as f:
            f.write(readme_txt)


class WSS(BreachFolder):
    """
    Folder where the the water depth raster are going to be created. Also all the files used to create it,
    are going to be store in this folder.
    """

    def __init__(self, base, create):
        super().__init__(os.path.join(base, "02_WSS"), create)
        self.dem_clip = self._find_files("dem_clip.vrt")
        self.grid_raw = self._find_files("grid_raw.gpkg")
        self.mask_flood = self._find_files("mask_flood.gpkg")
        self.max_wdepth_orig = self._find_files("max_wdepth_orig.tif")
        self.new_grid = self._find_files("new_grid.gpkg")
        self.landuse_2021_clip = self._find_files("landuse_2021_clip.vrt")
        self.max_waterlevel_orig = self._find_files("max_waterlevel_orig.tif")
        self.damage_orig_lizard = self._find_files("damage_orig_lizard.tif")
        self.direct_cost_damage = self._find_files("direct_cost_damage.tif")
        self.indirect_cost_damage = self._find_files("indirect_cost_damage.tif")
        self.total_costs_area = self._find_files("total_costs_area.csv")

        if create:
            self.create_readme()

    def create_readme(self):
        readme_txt = (
            "This folder is the default folder where the images and maps "
            "are stored. The inner structure of these result folders "
            "is automatically generated"
        )
        with open(os.path.join(self.base, "read_me.txt"), mode="w") as f:
            f.write(readme_txt)


# %%
