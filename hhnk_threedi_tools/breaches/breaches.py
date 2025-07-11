# %%
"""
Each class has a file as its attributes and a file as a new class.
self.base is the directory in which it is located
"""

# First-party imports
from hhnk_research_tools import Folder
from pathlib import Path
import os


FOLDER_STRUCTURE = """
    Main Breaches object
        ├── 01_NetCDF
        │ ├── aggregate_results_3di.nc
        │ ├── gridadmin.h5
        │ ├── gridadmin.sqlite
        │ └── results_3di.nc
        │    
        ├── 02_JPEG
        │ ├── overstroming.png
        │ ├── breach.name.png
        │ └── agg.png
        ├── 03_ssm
        │ ├── max_flow_velocity.tif
        │ └── max_raste_of_rise.tif
        │ └── max_waterdepth.tif
        │ └── max_waterlevel.tif
        └── 04_wss
        │ ├── dem_clip.vrt
        │ ├── grid_raw.gpkg
        │ ├── mask_flood.gpkg
        │ └── max_wdepth_orig.tif
        │ ├── new_grid.gpkg
        │ ├── nodeid.tif
        │ └── landuse_2021_clip.vrt

    """


class Breaches(Folder):
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
                ROR-PRI-UITDAMMERDIJK_8-T100 @ E:\03.resultaten\Overstromingsberekeningen primaire doorbraken 2024\output\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_ZUID_T10_T3000\ROR-PRI-UITDAMMERDIJK_8-T100
                                Folders:	  
                                    Folders
                                    ├── 01_NetCDF
                                    ├── 02_JPEG
                                    ├── 03_SSM
                                    └── 04_WSS
                           
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

    @property
    def structure(self):
        return f"""  
               {self.space}Folders
               {self.space}├── 01_NetCDF (.netcdf)
               {self.space}├── 02_JPEG (.jpeg)
               {self.space}├── 03_SSM (.ssm)
               {self.space}└── 04_WSS (.wss)
               """

    def to_file_dict(self):
        """
        Return dictionary containing paths to source files according to set project structure.

            build_base_paths_dict(
                    polder_path (string: path to project folder (highest level))
                )
        """
        return {
            "aggregate_results_3di": self.netcdf.aggregate_results_3di.path_if_exists,
            "gridadmin": self.netcdf.gridadmin.path_if_exists,
            "gridadmin": self.netcdf.gridadmin.path_if_exists,
            "results_3di": self.netcdf.results_3di.path_if_exists,
            # "channels_shapefile": self.source_data.modelbuilder.channel_from_profiles.path_if_exists,
            # jpeg
            "graph": self.jpeg.graph.path_if_exists,
            "agg_graph": self.jpeg.agg_graph.path_if_exists,
            "overstroming": self.jpeg.overstroming.path_if_exists,
            # wss
            "dem_clip": self.wss.dem_clip.path_if_exists,
            "grid_raw": self.wss.grid_raw.path_if_exists,
            "mask_flood": self.wss.mask_flood.path_if_exists,
            "max_wdepth_orig": self.wss.max_wdepth_orig.path_if_exists,
            "new_grid": self.wss.new_grid.path_if_exists,
            "nodeid": self.wss.nodeid.path_if_exists,
            "landuse_2021_clip": self.wss.landuse_2021_clip.path_if_exists,
            # ssm
            "max_flow_velocity_5m": self.ssm.max_flow_velocity_5m.path_if_exists,
            "max_rate_of_rise_5m": self.ssm.max_rate_of_rise_5m.sqlite_tests.path_if_exists,
            "max_waterdepth_5m": self.ssm.max_waterdepth_5m.path_if_exists,
            "max_waterlevel_5m": self.ssm.max_waterlevel_5m.path_if_exists,
        }

    @classmethod
    def is_valid(self, folderpath):
        """Check if folder stucture is available in input folder."""
        SUB_FOLDERS = ["01_NetCDF", "02_JPEG", "03_SSM", "04_WSS"]
        return all([Path(folderpath).joinpath(i).exists() for i in SUB_FOLDERS])


class NetCDF(Folder):
    """Path to netcdf data (aggregate_results_3di, gridadmin, results_3di)"""

    def __init__(self, base, create):
        super().__init__(os.path.join(base, "01_NetCDF"), create)

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


class JPEG(Folder):
    """Parent folder with all the images created for the final product. They included:
    breach graph and it aggregation, and also all the maps that can be generated.
    """

    def __init__(self, base, create):
        super().__init__(os.path.join(base, "02_JPEG"), create)

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

    def __repr__(self):
        return f"""{self.name} @ {self.base}
                    Folders:\t{self.structure}
                    Files:\t{list(self.files.keys())}
                """


class SSM(Folder):
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
            "Expected files are:tif files"
            "In this folder we are going to store the raster downloaded from lizard "
            "which include: max_flow_velocity, max_rate_rise, max_waterdepth, waterlelvel"
        )
        with open(os.path.join(self.base, "read_me.txt"), mode="w") as f:
            f.write(readme_txt)


class WSS(Folder):
    """
    Folder where the the water depth raster are going to be created. Also all the files used to create it,
    are going to be store in this folder.
    """

    def __init__(self, base, create):
        super().__init__(os.path.join(base, "04_WSS"), create)

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


# %%
