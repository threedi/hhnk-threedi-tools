from dataclasses import dataclass
import hhnk_research_tools as hrt
import os


class SourcePaths:
    def __init__(self,
        dem_path:str,
        glg_path:str,
        ggg_path:str,
        ghg_path:str,
        polder_path:str,
        watervlakken_path:str,
    ):
        """
        Locaties van inputbestanden voor het maken van de rasters
        dem_path (str): raster source van DEM
        glg_path (str): raster source van glg
        ggg_path (str): raster source van ggg
        ghg_path (str): raster source van ghg
        polder_path (str): polder met bounds van de output
        watervlakken_path (str): shapfile met watervlakken van polder
        """
                
        self.dem = hrt.Raster(dem_path)
        self.glg = hrt.Raster(glg_path)
        self.ggg = hrt.Raster(ggg_path)
        self.ghg = hrt.Raster(ghg_path)
        self.polder = hrt.File(polder_path)
        self.watervlakken = hrt.File(watervlakken_path)
        self.verify()

    def verify(self):
        for f in [self.dem, self.glg, self.ggg, 
                self.ghg, self.polder, self.watervlakken]:
            if not f.exists():
                raise Exception(f"{f} not found")
        return True


class FoldersModelbuilder:
    def __init__(self, dst_path:str, source_paths:SourcePaths):
        """Folder structuur om de rasters in op te slaan.
        
        dst_base (str): path to output folder
        source_paths (SourcePaths): bronrasters en shapes
        """
        self.src = source_paths
        self.dst = self.DestPaths(dst_path)


    class DestPaths(hrt.Folder):
        def __init__(self, base):
            super().__init__(base=os.path.join(base, ""))
            self.tmp = self.TempPaths(base)

            self.dem = self.full_path("dem.tif")
            self.glg = self.full_path("glg.tif")
            self.ggg = self.full_path("ggg.tif")
            self.ghg = self.full_path("ghg.tif")
            

        class TempPaths(hrt.Folder):
            def __init__(self, base):
                """temp rasters allemaal met dezelfde extent."""
                super().__init__(base=os.path.join(base, "tmp"))

                self.dem = self.full_path("dem.vrt")
                self.glg = self.full_path("glg.vrt")
                self.ggg = self.full_path("ggg.vrt")
                self.ghg = self.full_path("ghg.vrt")    
                self.polder = self.full_path("polder.tif")
                self.watervlakken = self.full_path("watervlakken.tif")

